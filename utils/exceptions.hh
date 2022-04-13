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
    #warn "Fast implementation of some of the exception handling routines is not available for this platform. Expect poor exception handling performance."
  #endif
#endif

#include <seastar/core/sstring.hh>
#include <seastar/core/on_internal_error.hh>

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
    storage_io_error(std::system_error& e) noexcept
        : _code{e.code()}
        , _what{std::string("Storage I/O error: ") + std::to_string(e.code().value()) + ": " + e.what()}
    { }

    virtual const char* what() const noexcept override {
        return _what.c_str();
    }

    const std::error_code& code() const { return _code; }
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
