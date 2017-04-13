/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2013 Couchbase, Inc.
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

#include "arenamanager.h"
#include "ep_engine.h"
#include "locks.h"
#include "objectregistry.h"

#include <cstdio>

#if defined(HAVE_JEMALLOC)
#define CHECK_JEMALLOC(retval) 
#else
#define CHECK_JEMALLOC(retval) return retval;
#endif

std::atomic<ArenaManager*> ArenaManager::instance;
std::mutex ArenaManager::initGuard;

bool fDumpArenaStats = false;

template<typename T>
T ArenaManager::getProperty(const char* property) {
    T value;
    size_t size = sizeof(value);
    value = 0;
    alloc_hooks->get_allocator_property(property, &value, &size);
    return value;
}

ArenaManager::ArenaManager(arenaid_t numArenas_) : numArenas(numArenas_) {
}

ArenaManager* ArenaManager::get() {
    auto* tmp = instance.load();
    if (tmp == nullptr) {
        LockHolder lh(initGuard);
        tmp = instance.load();
        if (tmp == nullptr) {
            fDumpArenaStats = getenv("ARENA_STATS") != NULL;
            tmp = new ArenaManager(100);
            instance.store(tmp);
        }
    }
    return tmp;
}

bool ArenaManager::isInitialized() {
    return alloc_hooks != NULL;
}

void ArenaManager::initialize(ALLOCATOR_HOOKS_API* alloc_hooks) {
    LockHolder lh(allocMutex);
    if (this->alloc_hooks != NULL) return;
    // printf("arena.init:%p\n", (void*)alloc_hooks);
    // throw an exception here ??
    if (alloc_hooks == NULL) return;
    if (arenas.empty()) {
        // we should use opt.narenas here. But because of how
        // we create and destroy this object in testcases, we use arenas.narenas 
        size_t size = sizeof(baseSystemArenas);
        alloc_hooks->get_allocator_property("arenas.narenas", &baseSystemArenas, &size);

        auto totalArenas = numArenas + baseSystemArenas;
        arenas.reserve(totalArenas);
        
        for (auto i = 0 ; i < totalArenas ; i++) {
            arenas.push_back(ArenaState());
        }
        
        for (auto i = 0 ; i < totalArenas; i++) {
            // mark all base system arenas as used
            arenas[i].inUse = i < baseSystemArenas;
            arenas[i].createTime = 0;
        }
    }
    
    // this inits the class
    this->alloc_hooks = alloc_hooks;
    
    LOG(EXTENSION_LOG_NOTICE,
        "Arena Manager initialized - arenas.narenas:%d tcache:%s",
        getProperty<unsigned>("arenas.narenas"),
        getProperty<bool>("opt.tcache") ? "enabled" : "disabled"
        );
}

void ArenaManager::destroy() {
    LockHolder lh(allocMutex);
    this->alloc_hooks = NULL;
}

arenaid_t ArenaManager::allocateArena() {
    CHECK_JEMALLOC(0);
    if (!isInitialized()) return 0;

    LockHolder lh(allocMutex);
    for (auto arenaId = baseSystemArenas; arenaId < numArenas; arenaId++) {
        // find the first free arena
        auto& state = arenas[arenaId];
        if (state.inUse) {
            continue;
        } else {
            if (state.createTime == 0) {
                // create new arena & initialize it
                arenaid_t newArenaIdx = 0;
                size_t size = sizeof(newArenaIdx);
                alloc_hooks->get_allocator_property("arenas.extend", &newArenaIdx, &size);
                if (newArenaIdx != arenaId ) {
                    dumpStats();
                    std::stringstream ss;
                    ss << "newly allocated arena id [" << newArenaIdx << "]"
                       << " does not match expected id [" << arenaId << "]";
                    throw std::runtime_error(ss.str());
                }
                state.createTime = gethrtime();
            } else {
                //TODO: reset this arena
            }
            state.inUse = true;
            return arenaId;
        }
    }

    throw std::runtime_error("unable to allocate new arena");
}

// return the arena and mark it as free
bool ArenaManager::deallocateArena(arenaid_t arenaId) {
    if (!isInitialized()) return false;
    switchToSystemArena();
    LockHolder lh(allocMutex);
    if (arenaId >= numArenas) return false;
    auto& state = arenas[arenaId];
    if (0 != getArenaUsage(arenaId)) {
        dumpStats();
        std::stringstream ss;
        ss << " setting arena for use [" << arenaId << "]"
           << " but arena usage is [" << getArenaUsage(arenaId) << "]";
        LOG(EXTENSION_LOG_FATAL,"%s",ss.str().c_str());
        // throw std::runtime_error(ss.str());
    }
    state.inUse = false;
    return true;
}

bool ArenaManager::resetArena(arenaid_t arenaId) {
    char propName[20];
    checked_snprintf(propName,20, "arena.%d.reset", arenaId);
    return alloc_hooks->get_allocator_property(propName, NULL , NULL);
}

bool ArenaManager::switchToBucketArena(EventuallyPersistentEngine *engine) {
    if (!isInitialized() || engine == nullptr) return false;
    return switchToArena(engine->getArena());
}

bool ArenaManager::switchToArena(arenaid_t arenaId_) {
    if (!isInitialized() || arenaId_ >= numArenas) return false;
    arenaid_t arenaId = arenaId_;
    return alloc_hooks->set_allocator_property(NULL, &arenaId, sizeof(arenaId));
}

bool ArenaManager::switchToSystemArena() {
    if (!isInitialized()) return false;
    return switchToArena(0);
}

arenaid_t ArenaManager::getCurrentArena() {
    if (!isInitialized()) return 0;
    arenaid_t arenaId = 0;
    size_t size = sizeof(arenaId);
    alloc_hooks->get_allocator_property(NULL, &arenaId, &size);
    return arenaId;
}

size_t ArenaManager::getAllocationSize(const void* p) {
    if (!isInitialized()) return 0;
    return alloc_hooks->get_allocation_size(p);
}

size_t ArenaManager::getArenaUsage(arenaid_t arenaId) {
    if (!isInitialized()) return 0;
    if (arenaId == 0) {
        // get the current arena
        arenaId = getCurrentArena();
    }
    return alloc_hooks->get_arena_allocation_size(arenaId);
}

void ArenaManager::dumpStats() {
    if (!isInitialized() || !fDumpArenaStats) return;
    auto curArena = getCurrentArena();

    __system_allocation__;

    arenaid_t narenas = getProperty<arenaid_t>("arenas.narenas");
    arenaid_t nSystemArenas = getProperty<arenaid_t>("opt.narenas");

    bool *array = new bool[narenas];
    std::fill_n(array, narenas, 0);
    
    size_t size = narenas * sizeof(bool);
    alloc_hooks->get_allocator_property("arenas.initialized", array, &size);

    std::stringstream ss;
    ss << nSystemArenas << "/" << narenas << " a:t:sz:use = ";
    char* propName = new char[40];
    arenaid_t numThreads = 0;
    size = sizeof(numThreads);
    for (auto arenaId = 0 ; arenaId < narenas ; arenaId++) {
        if (array [arenaId]) {
            if (arenaId == curArena)  ss << "*";
            snprintf(propName,40, "stats.arenas.%du.nthreads", arenaId);
            numThreads = getProperty<arenaid_t>(propName);
            ss << arenaId
               << ":" << numThreads
               << ":" << alloc_hooks->get_arena_allocation_size(arenaId)
                // << ":" << arenas[arenaId].inUse << ":" << arenas[arenaId].createTime
               << " ";
        }
    }
    std::cout << ss.str() << std::endl;
    delete[] array;
    delete[] propName;
}

