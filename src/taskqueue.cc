/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
#include "config.h"

#include "taskqueue.h"
#include "executorpool.h"
#include "executorthread.h"

#include <cmath>

TaskQueue::TaskQueue(ExecutorPool *m, task_type_t t, const char *nm) :
    name(nm), queueType(t), manager(m), sleepers(0)
{
    // EMPTY
}

TaskQueue::~TaskQueue() {
    LOG(EXTENSION_LOG_INFO, "Task Queue killing %s", name.c_str());
}

const std::string TaskQueue::getName() const {
    return (name+taskType2Str(queueType));
}

size_t TaskQueue::getReadyQueueSize() {
    LockHolder lh(mutex);
    return readyQueue.size();
}

size_t TaskQueue::getFutureQueueSize() {
    LockHolder lh(mutex);
    return futureQueue.size();
}

size_t TaskQueue::getPendingQueueSize() {
    LockHolder lh(mutex);
    return pendingQueue.size();
}

ExTask TaskQueue::_popReadyTask(void) {
    ExTask t = readyQueue.top();
    readyQueue.pop();
    manager->lessWork(queueType);
    return t;
}

void TaskQueue::doWake(size_t &numToWake) {
    LockHolder lh(mutex);
    _doWake_UNLOCKED(numToWake);
}

void TaskQueue::_doWake_UNLOCKED(size_t &numToWake) {
    if (sleepers && numToWake)  {
        if (numToWake < sleepers) {
            for (; numToWake; --numToWake) {
                mutex.notify_one(); // cond_signal 1
            }
        } else {
            mutex.notify_all(); // cond_broadcast
            numToWake -= sleepers;
        }
    }
}

bool TaskQueue::_doSleep(ExecutorThread &t,
                         std::unique_lock<std::mutex>& lock) {
    t.updateCurrentTime();
    if (t.getCurTime() < t.getWaketime() && manager->trySleep(queueType)) {
        // Atomically switch from running to sleeping; iff we were previously
        // running.
        executor_state_t expected_state = EXECUTOR_RUNNING;
        if (!t.state.compare_exchange_strong(expected_state,
                                             EXECUTOR_SLEEPING)) {
            return false;
        }
        sleepers++;
        // zzz....
        const auto snooze = t.getWaketime() - t.getCurTime();

        if (snooze > std::chrono::seconds((int)round(MIN_SLEEP_TIME))) {
            mutex.wait_for(lock, MIN_SLEEP_TIME);
        } else {
            mutex.wait_for(lock, snooze);
        }
        // ... woke!
        sleepers--;
        manager->woke();

        // Finished our sleep, atomically switch back to running iff we were
        // previously sleeping.
        expected_state = EXECUTOR_SLEEPING;
        if (!t.state.compare_exchange_strong(expected_state,
                                             EXECUTOR_RUNNING)) {
            return false;
        }
        t.updateCurrentTime();
    }
    t.setWaketime(ProcessClock::time_point(ProcessClock::time_point::max()));
    return true;
}

bool TaskQueue::_fetchNextTask(ExecutorThread &t, bool toSleep) {
    bool ret = false;
    std::unique_lock<std::mutex> lh(mutex);

    if (toSleep && !_doSleep(t, lh)) {
        return ret; // shutting down
    }

    size_t numToWake = _moveReadyTasks(t.getCurTime());

    if (!futureQueue.empty() && t.taskType == queueType &&
        futureQueue.top()->getWaketime() < t.getWaketime()) {
        // record earliest waketime
        t.setWaketime(futureQueue.top()->getWaketime());
    }

    if (!readyQueue.empty() && readyQueue.top()->isdead()) {
        t.setCurrentTask(_popReadyTask()); // clean out dead tasks first
        ret = true;
    } else if (!readyQueue.empty() || !pendingQueue.empty()) {
        // we must consider any pending tasks too. To ensure prioritized run
        // order, the function below will push any pending task back into the
        // readyQueue (sorted by priority)
        _checkPendingQueue();
        ExTask tid = _popReadyTask(); // and pop out the top task
        t.setCurrentTask(tid);
        ret = true;
    } else { // Let the task continue waiting in pendingQueue
        numToWake = numToWake ? numToWake - 1 : 0; // 1 fewer task ready
    }

    _doWake_UNLOCKED(numToWake);
    lh.unlock();

    return ret;
}

bool TaskQueue::fetchNextTask(ExecutorThread &thread, bool toSleep) {
    __system_allocation__;
    bool rv = _fetchNextTask(thread, toSleep);
    return rv;
}

size_t TaskQueue::_moveReadyTasks(const ProcessClock::time_point tv) {
    if (!readyQueue.empty()) {
        return 0;
    }

    size_t numReady = 0;
    while (!futureQueue.empty()) {
        ExTask tid = futureQueue.top();
        if (tid->getWaketime() <= tv) {
            futureQueue.pop();
            readyQueue.push(tid);
            numReady++;
        } else {
            break;
        }
    }

    manager->addWork(numReady, queueType);

    // Current thread will pop one task, so wake up one less thread
    return numReady ? numReady - 1 : 0;
}

void TaskQueue::_checkPendingQueue(void) {
    if (!pendingQueue.empty()) {
        ExTask runnableTask = pendingQueue.front();
        readyQueue.push(runnableTask);
        manager->addWork(1, queueType);
        pendingQueue.pop_front();
    }
}

ProcessClock::time_point TaskQueue::_reschedule(ExTask &task) {
    LockHolder lh(mutex);

    futureQueue.push(task);
    return futureQueue.top()->getWaketime();
}

ProcessClock::time_point TaskQueue::reschedule(ExTask &task) {
    __system_allocation__;
    auto rv = _reschedule(task);
    return rv;
}

void TaskQueue::_schedule(ExTask &task) {
    TaskQueue* sleepQ;
    size_t numToWake = 1;

    {
        LockHolder lh(mutex);

        // If we are rescheduling a previously cancelled task, we should reset
        // the task state to the initial value of running.
        bool changed_state = task->setState(TASK_RUNNING, TASK_DEAD);

        /* This test is to confirm that we are not changing existing
         * behaviour by resetting dead tasks to running when rescheduling an
         * existing task. Will be removed (MB-23797).
         */
        if (changed_state) {
            if (task->getTypeId() != TaskId::ItemPager) {
                throw std::logic_error(
                        "Unexpected task was scheduled while DEAD "
                        "queue:{" + name + "} "
                        "taskId:{" + std::to_string(task->getId()) + "} "
                        "taskName:{" +
                        GlobalTask::getTaskName(task->getTypeId()) + "}");
            }
        }

        futureQueue.push(task);

        LOG(EXTENSION_LOG_DEBUG,
            "%s: Schedule a task \"%.*s\" id %" PRIu64,
            name.c_str(),
            int(task->getDescription().size()),
            task->getDescription().data(),
            uint64_t(task->getId()));

        sleepQ = manager->getSleepQ(queueType);
        _doWake_UNLOCKED(numToWake);
    }
    if (this != sleepQ) {
        sleepQ->doWake(numToWake);
    }
}

void TaskQueue::schedule(ExTask &task) {
    __system_allocation__;
    _schedule(task);
}

void TaskQueue::_wake(ExTask &task) {
    const ProcessClock::time_point now = ProcessClock::now();
    TaskQueue* sleepQ;
    // One task is being made ready regardless of the queue it's in.
    size_t readyCount = 1;
    {
        LockHolder lh(mutex);
        LOG(EXTENSION_LOG_DEBUG,
            "%s: Wake a task \"%.*s\" id %" PRIu64,
            name.c_str(),
            int(task->getDescription().size()),
            task->getDescription().data(),
            uint64_t(task->getId()));

        std::queue<ExTask> notReady;
        // Wake thread-count-serialized tasks too
        for (std::list<ExTask>::iterator it = pendingQueue.begin();
             it != pendingQueue.end();) {
            ExTask tid = *it;
            if (tid->getId() == task->getId() || tid->isdead()) {
                notReady.push(tid);
                it = pendingQueue.erase(it);
            } else {
                it++;
            }
        }

        futureQueue.updateWaketime(task, now);
        task->setState(TASK_RUNNING, TASK_SNOOZED);

        while (!notReady.empty()) {
            ExTask tid = notReady.front();
            if (tid->getWaketime() <= now || tid->isdead()) {
                readyCount++;
            }

            // MB-18453: Only push to the futureQueue
            futureQueue.push(tid);
            notReady.pop();
        }

        _doWake_UNLOCKED(readyCount);
        sleepQ = manager->getSleepQ(queueType);
    }
    if (this != sleepQ) {
        sleepQ->doWake(readyCount);
    }
}

void TaskQueue::wake(ExTask &task) {
    __system_allocation__;
    _wake(task);
}

const std::string TaskQueue::taskType2Str(task_type_t type) {
    switch (type) {
    case WRITER_TASK_IDX:
        return std::string("Writer");
    case READER_TASK_IDX:
        return std::string("Reader");
    case AUXIO_TASK_IDX:
        return std::string("AuxIO");
    case NONIO_TASK_IDX:
        return std::string("NonIO");
    default:
        return std::string("None");
    }
}
