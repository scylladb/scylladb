/*
 * Copyright (C) 2021 ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <seastar/core/coroutine.hh>
#include "sstables/consumer.hh"

// Clang < 14 only supports the TS
#if __has_include(<coroutine>) && (!defined(__clang__) || __clang_major__ >= 14)
#  define COROUTINE_NS std
#else
#  define COROUTINE_NS std::experimental
#endif

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
    struct promise_type;
    struct read_awaiter {
        data_consumer::read_status _rs;
        read_awaiter(data_consumer::read_status rs) : _rs(rs) {}
        constexpr bool await_ready() const noexcept { return _rs == data_consumer::read_status::ready; }
        constexpr void await_suspend(COROUTINE_NS::coroutine_handle<promise_type>) const noexcept {}
        constexpr void await_resume() const noexcept {}
    };
    struct promise_type {
        using handle_type = COROUTINE_NS::coroutine_handle<promise_type>;
        processing_result_generator get_return_object() {
            return processing_result_generator{handle_type::from_promise(*this)};
        }
        // the coroutine doesn't start running until the first handle::resume() call
        static COROUTINE_NS::suspend_always initial_suspend() noexcept {
            return {};
        }
        static COROUTINE_NS::suspend_always final_suspend() noexcept {
            return {};
        }
        COROUTINE_NS::suspend_always yield_value(data_consumer::processing_result value) noexcept {
            current_value = std::move(value);
            return {};
        }
        read_awaiter yield_value(data_consumer::read_status rs) noexcept {
            if (rs == data_consumer::read_status::waiting) {
                current_value = data_consumer::proceed::yes;
            }
            return read_awaiter(rs);
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
    COROUTINE_NS::coroutine_handle<promise_type> _coro_handle;
public:
    explicit processing_result_generator(const COROUTINE_NS::coroutine_handle<promise_type> coroutine)
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
struct COROUTINE_NS::coroutine_traits<processing_result_generator, Args...> {
    using promise_type = processing_result_generator::promise_type;
};
