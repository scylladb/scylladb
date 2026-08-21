/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#pragma once
#include <seastar/util/log.hh>

// A test log to use in all unit tests, including boost unit
// tests. Built-in boost logging log levels do not allow to filter
// out unimportant messages, which then clutter xunit-format XML
// output, so are not used for anything profuse.

extern seastar::logger testlog;


// RAII helper that sets a logger to the given level for the duration of a scope,
// restoring the previous level on destruction.
class scoped_logger_level {
    seastar::sstring _name;
    seastar::log_level _prev_level;
public:
    scoped_logger_level(seastar::sstring name, seastar::log_level level)
        : _name(std::move(name))
        , _prev_level(seastar::global_logger_registry().get_logger_level(_name))
    {
        seastar::global_logger_registry().set_logger_level(_name, level);
    }
    ~scoped_logger_level() {
        seastar::global_logger_registry().set_logger_level(_name, _prev_level);
    }
    scoped_logger_level(const scoped_logger_level&) = delete;
    scoped_logger_level& operator=(const scoped_logger_level&) = delete;
};
