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
#include "do_with.hh"
#include "future-util.hh"
#include <memory>
#include <setjmp.h>
#include <type_traits>

/// \defgroup thread-module Seastar threads
///
/// Seastar threads provide an execution environment where blocking
/// is tolerated; you can issue I/O, and wait for it in the same function,
/// rather then establishing a callback to be called with \ref future<>::then().
///
/// Seastar threads are not the same as operating system threads:
///   - seastar threads are cooperative; they are never preempted except
///     at blocking points (see below)
///   - seastar threads always run on the same core they were launched on
///
/// Like other seastar code, seastar threads may not issue blocking system calls.
///
/// A seastar thread blocking point is any function that returns a \ref future<>.
/// you block by calling \ref future<>::get(); this waits for the future to become
/// available, and in the meanwhile, other seastar threads and seastar non-threaded
/// code may execute.
///
/// Example:
/// \code
///    seastar::thread th([] {
///       sleep(5s).get();  // blocking point
///    });
/// \endcode

/// Seastar API namespace
namespace seastar {

/// \addtogroup thread-module
/// @{

class thread;

/// \cond internal
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

/// \endcond


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

/// Executes a callable in a seastar thread.
///
/// Runs a block of code in a threaded context,
/// which allows it to block (using \ref future::get()).  The
/// result of the callable is returned as a future.
///
/// \param func a callable to be executed in a thread
/// \param args a parameter pack to be forwarded to \c func.
/// \return whatever \c func returns, as a future.
template <typename Func, typename... Args>
inline
futurize_t<std::result_of_t<std::decay_t<Func>(std::decay_t<Args>...)>>
async(Func&& func, Args&&... args) {
    using return_type = std::result_of_t<std::decay_t<Func>(std::decay_t<Args>...)>;
    struct work {
        Func func;
        std::tuple<Args...> args;
        promise<return_type> pr;
        thread th;
    };
    return do_with(work{std::forward<Func>(func), std::forward_as_tuple(std::forward<Args>(args)...)}, [] (work& w) {
        auto ret = w.pr.get_future();
        w.th = thread([&w] {
            futurize<return_type>::apply(std::move(w.func), std::move(w.args)).forward_to(std::move(w.pr));
        });
        return w.th.join().then([ret = std::move(ret)] () mutable {
            return std::move(ret);
        });
    });
}

/// @}

}
