/*
 * Copyright (C) 2021 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include <seastar/core/coroutine.hh>
#include "sstables/consumer.hh"

/* To construct an processing_result_generator object, you need a function that is a coroutine (uses co_yield)
 * and has return type of this class. The execution of the coroutine can be then controlled using this class.
 * To execute a fragment of the coroutine that ends at an co_yield, and get the yielded value use generate().
 * Each subsequent generate() call starts after the last co_yield and continues until another one is encountered.
 *
 * Explanation of C++ coroutines below:
 * A "co_yield val" call is equivalent to "co_await yield_value(val)". In yield_value we save the value in
 * "current_value", and it returns a suspend_always object, causing the coroutine to return to the caller after
 * each co_yield.
 * The await_transform() method is deleted, so we don't use co_await directly without yielding any value.
 * The unhandled_exception() method is called when an unhandled exception is detected in the coroutine. We
 * save it to a local variable, and check it in our generate() calls.
 */
class processing_result_generator {
public:
    struct promise_type {
        using handle_type = std::experimental::coroutine_handle<promise_type>;
        processing_result_generator get_return_object() {
            return processing_result_generator{handle_type::from_promise(*this)};
        }
        // the coroutine doesn't start running until the first handle::resume() call
        static std::experimental::suspend_always initial_suspend() noexcept {
            return {};
        }
        static std::experimental::suspend_always final_suspend() noexcept {
            return {};
        }
        std::experimental::suspend_always yield_value(data_consumer::processing_result value) noexcept {
            current_value = std::move(value);
            return {};
        }
        // Disallow co_await in generator coroutines.
        void await_transform() = delete;

        void unhandled_exception() {
            caught_exception = std::current_exception();
        }
        void return_void() noexcept {}

        data_consumer::processing_result current_value;
        std::exception_ptr caught_exception;
    };
private:
    std::experimental::coroutine_handle<promise_type> _coro_handle;
public:
    explicit processing_result_generator(const std::experimental::coroutine_handle<promise_type> coroutine)
        : _coro_handle{coroutine}
    {}

    processing_result_generator() = delete;
    ~processing_result_generator() {
        if (_coro_handle) {
            _coro_handle.destroy();
        }
    }

    processing_result_generator(const processing_result_generator&) = delete;
    processing_result_generator& operator=(const processing_result_generator&) = delete;

    processing_result_generator(processing_result_generator&& other) noexcept
        : _coro_handle{other._coro_handle}
    {
        other._coro_handle = {};
    }
    processing_result_generator& operator=(processing_result_generator&& other) noexcept {
        if (this != &other) {
            if (_coro_handle) {
                _coro_handle.destroy();
            }
            _coro_handle = other._coro_handle;
            other._coro_handle = {};
        }
        return *this;
    }
    data_consumer::processing_result generate() {
        _coro_handle();
        if (_coro_handle.promise().caught_exception) {
            std::rethrow_exception(_coro_handle.promise().caught_exception);
        }
        return _coro_handle.promise().current_value;
    }
};

template<typename... Args>
struct std::experimental::coroutine_traits<processing_result_generator, Args...> {
    using promise_type = processing_result_generator::promise_type;
};
