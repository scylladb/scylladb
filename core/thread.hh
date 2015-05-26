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

#pragma once

#include "future.hh"
#include <memory>
#include <setjmp.h>

namespace seastar {

class thread;
class thread_context;

namespace thread_impl {

thread_context* get();
void switch_in(thread_context* to);
void switch_out(thread_context* from);
void init();

}

extern thread_local jmp_buf g_unthreaded_context;

// Internal class holding thread state.  We can't hold this in
// \c thread itself because \c thread is movable, and we want pointers
// to this state to be captured.
class thread_context {
    static constexpr size_t _stack_size = 128*1024;
    std::unique_ptr<char[]> _stack{new char[_stack_size]};
    std::function<void ()> _func;
    jmp_buf _context;
    promise<> _done;
    bool _joined = false;
private:
    static void s_main(unsigned int lo, unsigned int hi);
    void setup();
    void main();
public:
    thread_context(std::function<void ()> func);
    void switch_in();
    void switch_out();
    friend class thread;
    friend void thread_impl::switch_in(thread_context*);
    friend void thread_impl::switch_out(thread_context*);
};

/// \brief thread - stateful thread of execution
///
/// Threads allow using seastar APIs in a blocking manner,
/// by calling future::get() on a non-ready future.  When
/// this happens, the thread is put to sleep until the future
/// becomes ready.
class thread {
    std::unique_ptr<thread_context> _context;
    static thread_local thread* _current;
public:
    /// \brief Constructs a \c thread object that does not represent a thread
    /// of execution.
    thread() = default;
    /// \brief Constructs a \c thread object that represents a thread of execution
    ///
    /// \param func Callable object to execute in thread.  The callable is
    ///             called immediately.
    template <typename Func>
    thread(Func func);
    /// \brief Moves a thread object.
    thread(thread&& x) noexcept = default;
    /// \brief Move-assigns a thread object.
    thread& operator=(thread&& x) noexcept = default;
    /// \brief Destroys a \c thread object.
    ///
    /// The thread must not represent a running thread of execution (see join()).
    ~thread() { assert(!_context || _context->_joined); }
    /// \brief Waits for thread execution to terminate.
    ///
    /// Waits for thread execution to terminate, and marks the thread object as not
    /// representing a running thread of execution.
    future<> join();
};

template <typename Func>
inline
thread::thread(Func func)
        : _context(std::make_unique<thread_context>(func)) {
}

inline
future<>
thread::join() {
    _context->_joined = true;
    return _context->_done.get_future();
}

}
