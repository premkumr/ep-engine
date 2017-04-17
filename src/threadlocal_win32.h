/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2014 Couchbase, Inc
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

#ifndef SRC_THREADLOCAL_WIN32_H_
#define SRC_THREADLOCAL_WIN32_H_ 1

#ifndef SRC_THREADLOCAL_H_
#error "Include threadlocal.h instead"
#endif

#include <cstdlib>
#include <sstream>
#include <cstring>
#include <system_error>
#include <iostream>
#include <string>

#include <platform/strerror.h>

/**
 * Container of thread-local data.
 */
template<typename T>
class ThreadLocalWin32 {
    // No use in Win32. Only for compatibility
    ThreadLocalDestructor dtor = NULL;
public:
    ThreadLocalWin32(ThreadLocalDestructor dtor = NULL) {
        tlsIndex = TlsAlloc();
        if (tlsIndex == TLS_OUT_OF_INDEXES) {
            DWORD err = GetLastError();
            std::string msg("Failed to create thread local storage: ");
            msg.append(cb_strerror(err));
            throw std::system_error(err, std::system_category(), msg);
        }
    }

    ~ThreadLocalWin32() {
        if (!TlsFree(tlsIndex)) {
            std::cerr << "~ThreadLocalPosix() TlsFree: "
                      << cb_strerror() << std::endl;
            std::cerr.flush();
        }
    }

    void set(const T &newValue) {
        if (!TlsSetValue(tlsIndex, newValue)) {
            DWORD err = GetLastError();
            std::string msg("Failed to store thread specific value: ");
            msg.append(cb_strerror(err));
            throw std::system_error(err, std::system_category(), msg);
        }
    }

    T get() const {
        return reinterpret_cast<T>(TlsGetValue(tlsIndex));
    }

    void operator =(const T &newValue) {
        set(newValue);
    }

    operator T() const {
        return get();
    }

private:
    DWORD tlsIndex;
};

#endif  // SRC_THREADLOCAL_WIN32_H_
