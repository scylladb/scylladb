/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "test/lib/test_utils.hh"

#include <seastar/core/print.hh>
#include <seastar/util/backtrace.hh>
#include "test/lib/log.hh"

#include "seastarx.hh"
#include <random>

namespace tests {

namespace {

std::string format_msg(std::string_view test_function_name, bool ok, std::experimental::source_location sl, std::string_view msg) {
    return fmt::format("{}(): {} @ {}() {}:{:d}{}{}", test_function_name, ok ? "OK" : "FAIL", sl.function_name(), sl.file_name(), sl.line(), msg.empty() ? "" : ": ", msg);
}

}

bool do_check(bool condition, std::experimental::source_location sl, std::string_view msg) {
    if (condition) {
        testlog.trace("{}", format_msg(__FUNCTION__, condition, sl, msg));
    } else {
        testlog.error("{}", format_msg(__FUNCTION__, condition, sl, msg));
    }
    return condition;
}

void do_require(bool condition, std::experimental::source_location sl, std::string_view msg) {
    if (condition) {
        testlog.trace("{}", format_msg(__FUNCTION__, condition, sl, msg));
    } else {
        auto formatted_msg = format_msg(__FUNCTION__, condition, sl, msg);
        testlog.error("{}", formatted_msg);
        throw_with_backtrace<std::runtime_error>(std::move(formatted_msg));
    }

}

void fail(std::string_view msg, std::experimental::source_location sl) {
    throw_with_backtrace<std::runtime_error>(format_msg(__FUNCTION__, false, sl, msg));
}

}

sstring make_random_string(size_t size) {
    static thread_local std::default_random_engine rng;
    std::uniform_int_distribution<char> dist;
    sstring str = uninitialized_string(size);
    for (auto&& b : str) {
        b = dist(rng);
    }
    return str;
}

sstring make_random_numeric_string(size_t size) {
    static thread_local std::default_random_engine rng;
    std::uniform_int_distribution<char> dist('0', '9');
    sstring str = uninitialized_string(size);
    for (auto&& b : str) {
        b = dist(rng);
    }
    return str;
}
