/*
 * Copyright (C) 2014-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <seastar/util/log.hh>

namespace logging {

//
// Seastar changed the names of some of these types. Maintain the old names here to avoid too much churn.
//

using log_level = seastar::log_level;
using logger = seastar::logger;
using registry = seastar::logger_registry;

inline registry& logger_registry() noexcept {
    return seastar::global_logger_registry();
}

using settings = seastar::logging_settings;

inline void apply_settings(const settings& s) {
    seastar::apply_logging_settings(s);
}

using seastar::pretty_type_name;
using seastar::level_name;

}

template <typename ExceptionType, typename... Args>
void log_and_throw(seastar::logger& logger, seastar::log_level log_level, const char* fmt, Args... args) {
    auto msg = format(fmt, std::forward<Args>(args)...);
    logger.log(log_level, "{}", msg);
    throw ExceptionType(msg);
}

template <typename ExceptionType, typename... Args>
void log_warning_and_throw(seastar::logger& logger, const char* fmt, Args... args) {
    log_and_throw<ExceptionType>(logger, seastar::log_level::warn, fmt, std::forward<Args>(args)...);
}

template <typename ExceptionType, typename... Args>
void log_error_and_throw(seastar::logger& logger, const char* fmt, Args... args) {
    log_and_throw<ExceptionType>(logger, seastar::log_level::error, fmt, std::forward<Args>(args)...);
}
