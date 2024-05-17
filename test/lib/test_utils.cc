/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "test/lib/test_utils.hh"

#include <seastar/util/file.hh>
#include <boost/range/adaptor/map.hpp>
#include <boost/range/algorithm/sort.hpp>
#include <seastar/core/print.hh>
#include <seastar/util/backtrace.hh>
#include "test/lib/log.hh"
#include "test/lib/simple_schema.hh"
#include "utils/to_string.hh"
#include "seastarx.hh"
#include <random>

namespace tests {

namespace {

std::string format_msg(std::string_view test_function_name, bool ok, seastar::compat::source_location sl, std::string_view msg) {
    return fmt::format("{}(): {} @ {}() {}:{:d}{}{}", test_function_name, ok ? "OK" : "FAIL", sl.function_name(), sl.file_name(), sl.line(), msg.empty() ? "" : ": ", msg);
}

}

bool do_check(bool condition, seastar::compat::source_location sl, std::string_view msg) {
    if (condition) {
        testlog.trace("{}", format_msg(__FUNCTION__, condition, sl, msg));
    } else {
        testlog.error("{}", format_msg(__FUNCTION__, condition, sl, msg));
    }
    return condition;
}

void do_require(bool condition, seastar::compat::source_location sl, std::string_view msg) {
    if (condition) {
        testlog.trace("{}", format_msg(__FUNCTION__, condition, sl, msg));
    } else {
        auto formatted_msg = format_msg(__FUNCTION__, condition, sl, msg);
        testlog.error("{}", formatted_msg);
        throw_with_backtrace<std::runtime_error>(std::move(formatted_msg));
    }

}

void fail(std::string_view msg, seastar::compat::source_location sl) {
    throw_with_backtrace<std::runtime_error>(format_msg(__FUNCTION__, false, sl, msg));
}


extern boost::test_tools::assertion_result has_scylla_test_env(boost::unit_test::test_unit_id) {
    if (::getenv("SCYLLA_TEST_ENV")) {
        return true;
    }

    testlog.info("Test environment is not configured. "
        "Check test/pylib/minio_server.py for an example of how to configure the environment for it to run.");
    return false;
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

namespace tests {

future<bool> compare_files(std::string fa, std::string fb) {
    auto cont_a = co_await util::read_entire_file_contiguous(fa);
    auto cont_b = co_await util::read_entire_file_contiguous(fb);
    co_return cont_a == cont_b;
}

future<> touch_file(std::string name) {
    auto f = co_await open_file_dma(name, open_flags::create);
    co_await f.close();
}

std::mutex boost_logger_mutex;

}
