/*
* This file is open source software, licensed to you under the terms
* of the Apache License, Version 2.0 (the "License"). See the NOTICE file
* distributed with this work for additional information regarding copyright
* ownership. You may not use this file except in compliance with the License.
*
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied. See the License for the
* specific language governing permissions and limitations
* under the License.
*/
/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#ifndef CORE_RWLOCK_HH_
#define CORE_RWLOCK_HH_

#include "semaphore.hh"

class rwlock {
    static const size_t max_ops = std::numeric_limits<size_t>::max();

    semaphore _sem;
public:
    rwlock()
            : _sem(max_ops) {
    }

    future<> read_lock() {
        return _sem.wait();
    }
    void read_unlock() {
        _sem.signal();
    }
    future<> write_lock() {
        return _sem.wait(max_ops);
    }
    void write_unlock() {
        _sem.signal(max_ops);
    }
    bool try_read_lock() {
        return _sem.try_wait();
    }
    bool try_write_lock() {
        return _sem.try_wait(max_ops);
    }
};

#endif /* CORE_RWLOCK_HH_ */
