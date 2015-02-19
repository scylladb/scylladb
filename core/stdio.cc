/*
 * This file is open source software, licensed to you under the terms
 * of the Apache License, Version 2.0 (the "License").  See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership.  You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */


#include "stdio.hh"
#include <memory>
#include <pthread.h>
#include <mutex>
#include <unordered_map>
#include <boost/thread/tss.hpp>
#include <vector>
#include <algorithm>

class spinlock {
    pthread_spinlock_t _l;
public:
    spinlock() { pthread_spin_init(&_l, PTHREAD_PROCESS_PRIVATE); }
    ~spinlock() { pthread_spin_destroy(&_l); }
    void lock() { pthread_spin_lock(&_l); }
    void unlock() { pthread_spin_unlock(&_l); }
};

class smp_file {
    FILE* _out;
    spinlock _lock;
    boost::thread_specific_ptr<std::vector<char>> _buffer;
    static constexpr size_t max = 2000;
public:
    smp_file(FILE* out) : _out(out) {}
    ssize_t write(const char* buffer, size_t size) {
        auto& b = *_buffer;
        b.insert(b.end(), buffer, buffer + size);
        size_t now = 0;
        if (b.size() >= max) {
            now = b.size();
        } else {
            auto lf = std::find(b.rbegin(), b.rend(), '\n');
            if (lf != b.rend()) {
                auto remain = lf - b.rbegin();
                now = b.size() - remain;
            }
        }
        if (now) {
            auto ret = fwrite(b.data(), 1, now, _out);
            b.erase(b.begin(), b.begin() + now);
            return ret;
        } else {
            return 0;
        }
    }
};

static smp_file* to_smp_file(void* cookie) {
    return static_cast<smp_file*>(cookie);
}

FILE*
smp_synchronize_lines(FILE* out) {
    auto p = std::make_unique<smp_file>(out);
    cookie_io_functions_t vtable = {};
    vtable.write = [] (void* ck, const char* b, size_t s) { return to_smp_file(ck)->write(b, s); };
    vtable.close = [] (void* ck) { delete to_smp_file(ck); return 0; };
    auto ret = fopencookie(p.get(), "w", vtable);
    if (!ret) {
        return ret;
    }
    // disable buffering so that writes to ret don't get mixed
    // up but are sent to smp_file immediately instead.
    setvbuf(ret, nullptr, _IONBF, 0);
    p.release();
    return ret;
}



