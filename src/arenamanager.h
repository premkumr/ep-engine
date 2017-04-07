/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc.
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

#ifndef SRC_ARENAMANAGER_H_
#define SRC_ARENAMANAGER_H_ 1

#include "config.h"

#include <memcached/allocator_hooks.h>
#include <atomic>
#include <vector>
#include <mutex>

class EventuallyPersistentEngine;

using arenaid_t = unsigned int;

class ArenaManager {
    
public:
    void initialize(ALLOCATOR_HOOKS_API* alloc_hooks);
    void destroy();

    static ArenaManager* get();

    arenaid_t getCurrentArena();
    
    // get one arena for use & mark it as used
    arenaid_t allocateArena();

    // return the arena and mark it as free
    bool deallocateArena(arenaid_t arenaId);

    // switch to the specified Arena
    bool switchToArena(arenaid_t arenaId);
    
    // switch to the arena for the bucket
    bool switchToBucketArena(EventuallyPersistentEngine *engine);

    // switch to a non- bucket specific arena
    bool switchToSystemArena();

    bool isInitialized();

    size_t getAllocationSize(const void* p);

    size_t getArenaUsage(arenaid_t arenaId = 0);

    void dumpStats();

protected:
    ArenaManager(arenaid_t numArenas = 10);
    struct ArenaState {
        bool inUse = false;
        hrtime_t createTime = 0;
    };

    ALLOCATOR_HOOKS_API* alloc_hooks = NULL;
    
    // max no.of arenas, typically one per bucket
    arenaid_t numArenas = 20;

    // no.of pre-existing arenas in the system
    arenaid_t baseSystemArenas = 1;
    std::vector<ArenaState> arenas;

    std::mutex allocMutex;
    static std::mutex initGuard;
    static std::atomic<ArenaManager*> instance;

    bool resetArena(arenaid_t arenaId);
    template<typename T>
    T getProperty(const char* property) ;
};

#endif  // SRC_EXECUTORPOOL_H_
