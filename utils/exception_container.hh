/*
 * Copyright 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <exception>
#include <typeinfo>
#include <type_traits>
#include <memory>
#include <ostream>
#include <variant>
#include <seastar/core/future.hh>
#include <seastar/core/distributed.hh>
#include <seastar/util/log.hh>
#include "utils/variant_element.hh"

namespace utils {

class bad_exception_container_access : public std::exception {
public:
    const char* what() const noexcept override {
        return "bad exception container access";
    }
};

// A variant-like type capable of holding one of the allowed exception types.
// This allows inspecting the exception in the error handling code without
// having to resort to costly rethrowing of std::exception_ptr, as is
// in the case of the usual exception handling.
//
// It's not as ergonomic as using exceptions with seastar, but allows for
// fast inspection and manipulation.
//
// The exception is held behind a std::shared_ptr. In order to minimize use
// of atomic operations, the copy constructor is deleted and copying is only
// possible by using the `clone()` method.
//
// This means that the moved-out exception container becomes "empty" and
// does not contain a valid exception.
template<typename... Exs>
struct exception_container {
private:
    using exception_variant = std::variant<Exs...>;

    // TODO: Idea for a possible improvement: get rid of the variant
    // and just store a pointer to an error allocated on the heap.
    // Keep an integer which identifies the variant.
    // Bonus points: if each error type has a unique, globally-assigned
    // identified integer, then conversion of the exception_container
    // to a container supporting a superset of errors becomes very cheap.
    std::shared_ptr<exception_variant> _eptr;

    // Users should use `clone()` in order to copy the exception container.
    // The copy constructor is made private in order to make copying explicit.
    exception_container(const exception_container&) = default;

    void check_nonempty() const {
        if (empty()) {
            throw bad_exception_container_access();
        }
    }

public:
    // Constructs an exception_container which does not contain any exception.
    exception_container() = default;

    exception_container(exception_container&&) = default;
    exception_container& operator=(exception_container&&) = default;
    exception_container& operator=(const exception_container&) = delete; // Must be explicitly copied with `clone()`

    template<typename Ex>
    requires VariantElement<Ex, exception_variant>
    exception_container(Ex&& ex)
            : _eptr(std::make_shared<exception_variant>(std::forward<Ex>(ex)))
    { }

    inline bool empty() const {
        return __builtin_expect(!_eptr, false);
    }

    inline operator bool() const {
        return !empty();
    }

    // Accepts a visitor.
    // If the container is empty, the visitor is called with
    // a bad_exception_container_access.
    auto accept(auto f) const {
        if (empty()) {
            return f(bad_exception_container_access());
        }
        return std::visit(std::move(f), *_eptr);
    }

    // Explicitly clones the exception container.
    exception_container clone() const noexcept {
        return exception_container(*this);
    }

    // Throws currently held exception as a C++ exception.
    // If the container is empty, it throws bad_exception_container_access.
    [[noreturn]] void throw_me() const {
        check_nonempty();
        std::visit([] (const auto& ex) { throw ex; }, *_eptr);
        std::terminate(); // Should be unreachable
    }

    // Creates an exceptional future from this error.
    // The exception is copied into the new exceptional future.
    // If the container is empty, returns an exceptional future
    // with the bad_exception_container_access exception.
    template<typename T = void>
    seastar::future<T> as_exception_future() const & {
        if (!_eptr) {
            return seastar::make_exception_future<T>(bad_exception_container_access());
        }
        return std::visit([] (const auto& ex) {
            return seastar::make_exception_future<T>(ex);
        }, *_eptr);
    }

    // Transforms this exception future into an exceptional future.
    // The exception is moved out and the container becomes empty.
    // If the container was empty, returns an exceptional future
    // with the bad_exception_container_access exception.
    template<typename T = void>
    seastar::future<T> into_exception_future() && {
        if (!_eptr) {
            return seastar::make_exception_future<T>(bad_exception_container_access());
        }
        auto f = std::visit([] (auto&& ex) {
            return seastar::make_exception_future<T>(std::move(ex));
        }, *_eptr);
        _eptr.reset();
        return f;
    }
};

template<typename T>
struct is_exception_container : std::false_type {};

template<typename... Exs>
struct is_exception_container<exception_container<Exs...>> : std::true_type {};

template<typename T>
concept ExceptionContainer = is_exception_container<T>::value;

}

#if FMT_VERSION < 100000
// fmt v10 introduced formatter for std::exception
template <>
struct fmt::formatter<utils::bad_exception_container_access> : fmt::formatter<string_view> {
    auto format(const utils::bad_exception_container_access& e, fmt::format_context& ctx) const {
        return fmt::format_to(ctx.out(), "{}", e.what());
    }
};
#endif

template <typename... Exs> struct fmt::formatter<utils::exception_container<Exs...>> : fmt::formatter<string_view> {
    auto format(const auto& ec, fmt::format_context& ctx) const {
        auto out = ctx.out();
        ec.accept([&out] (const auto& ex) { out = fmt::format_to(out, "{}", ex); });
        return out;
    }
};
