/*
 * Copyright 2016-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <cstddef>

#if defined(__GLIBCXX__) && (defined(__x86_64__) || defined(__aarch64__))
  #define OPTIMIZED_EXCEPTION_HANDLING_AVAILABLE
#endif

#if !defined(NO_OPTIMIZED_EXCEPTION_HANDLING)
  #if defined(OPTIMIZED_EXCEPTION_HANDLING_AVAILABLE)
    #define USE_OPTIMIZED_EXCEPTION_HANDLING
  #else
    #warning "Fast implementation of some of the exception handling routines is not available for this platform. Expect poor exception handling performance."
  #endif
#endif

#include <seastar/core/sstring.hh>
#include <seastar/core/on_internal_error.hh>
#include <seastar/core/align.hh>

#include <functional>
#include <system_error>
#include <type_traits>

namespace seastar { class logger; }

typedef std::function<bool (const std::system_error &)> system_error_lambda_t;

bool check_exception(system_error_lambda_t f);
bool is_system_error_errno(int err_no);
bool is_timeout_exception(std::exception_ptr e);

class storage_io_error : public std::exception {
private:
    std::error_code _code;
    std::string _what;
public:
    storage_io_error(std::error_code c, std::string s) noexcept
        : _code(std::move(c))
        , _what(std::move(s))
    { }

    storage_io_error(int err, std::string s) noexcept
        : storage_io_error(std::error_code(err, std::system_category()), std::move(s))
    { }

    storage_io_error(std::system_error& e) noexcept
        : storage_io_error(e.code(), std::string("Storage I/O error: ") + std::to_string(e.code().value()) + ": " + e.what())
    { }

    virtual const char* what() const noexcept override {
        return _what.c_str();
    }

    const std::error_code& code() const noexcept { return _code; }
};

// Rethrow exception if not null
//
// Helps with the common coroutine exception-handling idiom:
//
//  std::exception_ptr ex;
//  try {
//      ...
//  } catch (...) {
//      ex = std::current_exception();
//  }
//
//  // release resource(s)
//  maybe_rethrow_exception(std::move(ex));
//
//  return result;
//
inline void maybe_rethrow_exception(std::exception_ptr ex) {
    if (ex) {
        std::rethrow_exception(std::move(ex));
    }
}

namespace utils::internal {

#if defined(OPTIMIZED_EXCEPTION_HANDLING_AVAILABLE)
void* try_catch_dynamic(std::exception_ptr& eptr, const std::type_info* catch_type) noexcept;

template<typename Ex>
class nested_exception : public Ex, public std::nested_exception {
private:
    void set_nested_exception(std::exception_ptr nested_eptr) {
        // Hack: libstdc++'s std::nested_exception has just one field
        // which is a std::exception_ptr. It is initialized
        // to std::current_exception on its construction, but we override
        // it here.
        auto* nex = dynamic_cast<std::nested_exception*>(this);

        // std::nested_exception is virtual without any base classes,
        // so according to the ABI we just need to skip the vtable pointer
        // and align
        auto* nptr = reinterpret_cast<std::exception_ptr*>(
                seastar::align_up(
                        reinterpret_cast<char*>(nex) + sizeof(void*),
                        alignof(std::nested_exception)));
        *nptr = std::move(nested_eptr);
    }

public:
    explicit nested_exception(const Ex& ex, std::exception_ptr&& nested_eptr)
            : Ex(ex) {
        set_nested_exception(std::move(nested_eptr));
    }

    explicit nested_exception(Ex&& ex, std::exception&& nested_eptr)
            : Ex(std::move(ex)) {
        set_nested_exception(std::move(nested_eptr));
    }
};
#endif

} // utils::internal

/// If the exception_ptr holds an exception which would match on a `catch (T&)`
/// clause, returns a pointer to it. Otherwise, returns `nullptr`.
///
/// The exception behind the pointer is valid as long as the exception
/// behind the exception_ptr is valid.
template<typename T>
inline T* try_catch(std::exception_ptr& eptr) noexcept {
    static_assert(!std::is_pointer_v<T>, "catching pointers is not supported");
    static_assert(!std::is_lvalue_reference_v<T> && !std::is_rvalue_reference_v<T>,
            "T must not be a reference");

#if defined(USE_OPTIMIZED_EXCEPTION_HANDLING)
    void* opt_ptr = utils::internal::try_catch_dynamic(eptr, &typeid(std::remove_const_t<T>));
    return reinterpret_cast<T*>(opt_ptr);
#else
    try {
        std::rethrow_exception(eptr);
    } catch (T& t) {
        return &t;
    } catch (...) {
    }
    return nullptr;
#endif
}

/// Analogous to std::throw_with_nested, but instead of capturing the currently
/// thrown exception, takes the exception to be nested inside as an argument,
/// and does not throw the new exception but rather returns it.
template<typename Ex>
inline std::exception_ptr make_nested_exception_ptr(Ex&& ex, std::exception_ptr nested) {
    using ExDecayed = std::decay_t<Ex>;

    static_assert(std::is_copy_constructible_v<ExDecayed> && std::is_move_constructible_v<ExDecayed>,
            "make_nested_exception_ptr argument must be CopyConstructible");

#if defined(USE_OPTIMIZED_EXCEPTION_HANDLING)
    // std::rethrow_with_nested wraps the exception type if and only if
    // it is a non-final non-union class type
    // and is neither std::nested_exception nor derived from it.
    // Ref: https://en.cppreference.com/w/cpp/error/throw_with_nested
    constexpr bool wrap = std::is_class_v<ExDecayed>
            && !std::is_final_v<ExDecayed>
            && !std::is_base_of_v<std::nested_exception, ExDecayed>;

    if constexpr (wrap) {
        return std::make_exception_ptr(utils::internal::nested_exception<ExDecayed>(
                std::forward<Ex>(ex), std::move(nested)));
    } else {
        return std::make_exception_ptr<Ex>(std::forward<Ex>(ex));
    }
#else
    try {
        std::rethrow_exception(std::move(nested));
    } catch (...) {
        try {
            std::throw_with_nested(std::forward<Ex>(ex));
        } catch (...) {
            return std::current_exception();
        }
    }
#endif
}
