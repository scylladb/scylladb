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
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#include "posix.hh"
#include "align.hh"
#include <sys/mman.h>

file_desc
file_desc::temporary(sstring directory) {
    // FIXME: add O_TMPFILE support one day
    directory += "/XXXXXX";
    std::vector<char> templat(directory.c_str(), directory.c_str() + directory.size() + 1);
    int fd = ::mkstemp(templat.data());
    throw_system_error_on(fd == -1);
    int r = ::unlink(templat.data());
    throw_system_error_on(r == -1); // leaks created file, but what can we do?
    return file_desc(fd);
}

void mmap_deleter::operator()(void* ptr) const {
    ::munmap(ptr, _size);
}

mmap_area mmap_anonymous(void* addr, size_t length, int prot, int flags) {
    auto ret = ::mmap(addr, length, prot, flags | MAP_ANONYMOUS, -1, 0);
    throw_system_error_on(ret == MAP_FAILED);
    return mmap_area(reinterpret_cast<char*>(ret), mmap_deleter{length});
}

void* posix_thread::start_routine(void* arg) {
    auto pfunc = reinterpret_cast<std::function<void ()>*>(arg);
    try {
        (*pfunc)();
    } catch (...) {
        // We're in POSIX thread callback, so all exceptions need to be handled.
        std::terminate();
    }
    return nullptr;
}

posix_thread::posix_thread(std::function<void ()> func)
    : posix_thread(attr{}, std::move(func)) {
}

posix_thread::posix_thread(attr a, std::function<void ()> func)
    : _func(std::make_unique<std::function<void ()>>(std::move(func))) {
    pthread_attr_t pa;
    auto r = pthread_attr_init(&pa);
    if (r) {
        throw std::system_error(r, std::system_category());
    }
    auto stack_size = a._stack_size.size;
    if (!stack_size) {
        stack_size = 2 << 20;
    }
    // allocate guard area as well
    _stack = mmap_anonymous(nullptr, stack_size + (4 << 20),
            PROT_NONE, MAP_PRIVATE | MAP_NORESERVE);
    auto stack_start = align_up(_stack.get() + 1, 2 << 20);
    mmap_area real_stack = mmap_anonymous(stack_start, stack_size,
            PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_FIXED | MAP_STACK);
    real_stack.release(); // protected by @_stack
    ::madvise(stack_start, stack_size, MADV_HUGEPAGE);
    r = pthread_attr_setstack(&pa, stack_start, stack_size);
    if (r) {
        throw std::system_error(r, std::system_category());
    }
    r = pthread_create(&_pthread, &pa,
                &posix_thread::start_routine, _func.get());
    if (r) {
        throw std::system_error(r, std::system_category());
    }
}

posix_thread::posix_thread(posix_thread&& x)
    : _func(std::move(x._func)), _pthread(x._pthread), _valid(x._valid)
    , _stack(std::move(x._stack)) {
    x._valid = false;
}

posix_thread::~posix_thread() {
    assert(!_valid);
}

void posix_thread::join() {
    assert(_valid);
    pthread_join(_pthread, NULL);
    _valid = false;
}

void pin_this_thread(unsigned cpu_id) {
    cpu_set_t cs;
    CPU_ZERO(&cs);
    CPU_SET(cpu_id, &cs);
    auto r = pthread_setaffinity_np(pthread_self(), sizeof(cs), &cs);
    assert(r == 0);
}

