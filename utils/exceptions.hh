/*
 * Copyright 2016-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <seastar/core/sstring.hh>
#include <seastar/core/on_internal_error.hh>

#include <functional>
#include <system_error>

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
