/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once
#include <seastar/http/reply.hh>
#include <seastar/util/bool_class.hh>

namespace utils::http {

using retryable = seastar::bool_class<struct is_retryable>;

retryable from_http_code(seastar::http::reply::status_type http_code);

retryable from_system_error(const std::system_error& system_error);

// -----------------------------------------------------------------------------
// TypedHandler: handles a specific exception type Exc and returns R
// -----------------------------------------------------------------------------

template <typename Exc, typename F>
struct TypedHandler {
    static_assert(std::is_base_of_v<std::exception, Exc>, "TypedHandler can only handle std::exception types");
    using return_type = std::invoke_result_t<F, const Exc&>;

    F func;
    [[nodiscard]] bool matches(const std::exception& e) const noexcept { return dynamic_cast<const Exc*>(&e) != nullptr; }
    return_type handle(const std::exception& e) const { return func(static_cast<const Exc&>(e)); }
};

template <typename Exc, typename F>
auto make_handler(F&& f) {
    return TypedHandler<Exc, std::decay_t<F>>{std::forward<F>(f)};
}

// dispatch_exception: unwraps nested exceptions (if any) and applies handlers
// The dispatcher gets as input the exception_ptr to process, a default handler
// to call if no other handler matches, and a variadic list of TypedHandlers.
// All handlers (including the default one) must return the same type R.

template <typename R, typename DefaultHandler, typename... Handlers>
R dispatch_exception(std::exception_ptr eptr, DefaultHandler default_handler, Handlers&&... handlers) {
    using default_handler_return_type = std::invoke_result_t<DefaultHandler, std::exception_ptr, std::string&&>;
    static_assert(std::is_same_v<R, default_handler_return_type>, "Default handler must return the same type R");
    static_assert((std::is_same_v<default_handler_return_type, typename std::decay_t<Handlers>::return_type> && ...),
                  "All handlers must return the same type R");

    std::string original_message;

    while (eptr) {
        try {
            std::rethrow_exception(eptr);
        } catch (const std::exception& e) {
            if (original_message.empty()) {
                original_message = e.what();
            }

            bool matched = false;
            R result;
            ([&] {
                if (!matched && handlers.matches(e)) {
                    result = handlers.handle(e);
                    matched = true;
                }
            }(),...);

            if (matched)
                return result;

            // Try to unwrap nested exception
            try {
                std::rethrow_if_nested(e);
            } catch (...) {
                eptr = std::current_exception();
                continue;
            }
            return default_handler(eptr, std::move(original_message));
        } catch (...) {
            return default_handler(eptr, std::move(original_message));
        }
    }
    return default_handler(eptr, std::move(original_message));
}

} // namespace utils::http
