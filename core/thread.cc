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

#include "thread.hh"
#include "posix.hh"
#include <ucontext.h>

namespace seastar {

thread_local thread_context* g_current_thread;
thread_local jmp_buf g_unthreaded_context;
thread_local jmp_buf* g_current_context;

thread_context::thread_context(std::function<void ()> func)
        : _func(std::move(func)) {
    setup();
}

void
thread_context::setup() {
    // use setcontext() for the initial jump, as it allows us
    // to set up a stack, but continue with longjmp() as it's
    // much faster.
    ucontext_t initial_context;
    auto q = uint64_t(reinterpret_cast<uintptr_t>(this));
    auto main = reinterpret_cast<void (*)()>(&thread_context::s_main);
    auto r = getcontext(&initial_context);
    throw_system_error_on(r == -1);
    initial_context.uc_stack.ss_sp = _stack.get();
    initial_context.uc_stack.ss_size = _stack_size;
    initial_context.uc_link = nullptr;
    makecontext(&initial_context, main, 2, int(q), int(q >> 32));
    auto prev = g_current_context;
    g_current_context = &_context;
    g_current_thread = this;
    if (setjmp(*prev) == 0) {
        setcontext(&initial_context);
    }
}

void
thread_context::switch_in() {
    // FIXME: use setjmp/longjmp after initial_switch_in, much faster
    auto prev = g_current_context;
    g_current_context = &_context;
    g_current_thread = this;
    if (setjmp(*prev) == 0) {
        longjmp(_context, 1);
    }
}

void
thread_context::switch_out() {
    g_current_context = &g_unthreaded_context;
    g_current_thread = nullptr;
    if (setjmp(_context) == 0) {
        longjmp(g_unthreaded_context, 1);
    }
}

void
thread_context::s_main(unsigned int lo, unsigned int hi) {
    uintptr_t q = lo | (uint64_t(hi) << 32);
    reinterpret_cast<thread_context*>(q)->main();
}

void
thread_context::main() {
    try {
        _func();
        _done.set_value();
    } catch (...) {
        _done.set_exception(std::current_exception());
    }
    g_current_context = &g_unthreaded_context;
    g_current_thread = nullptr;
    longjmp(g_unthreaded_context, 1);
}

namespace thread_impl {

thread_context* get() {
    return g_current_thread;
}

void switch_in(thread_context* to) {
    to->switch_in();
}

void switch_out(thread_context* from) {
    from->switch_out();
}

void init() {
    g_current_context = &g_unthreaded_context;
}

}


}
