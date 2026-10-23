/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#define BOOST_TEST_MODULE moving_average_rate

#include <boost/test/unit_test.hpp>
#include <chrono>
#include <cmath>
#include <stdexcept>

#include "utils/moving_average_rate.hh"

using namespace std::chrono_literals;

namespace {

using clock_type = std::chrono::steady_clock;
// A 4-minute window divided into 4 one-minute samples keeps the expected
// values easy to compute by hand.
using rate_type = utils::moving_average_rate<utils::units::bytes, uint64_t, clock_type, 4>;
constexpr auto window = 4min;

clock_type::time_point at(std::chrono::seconds s) {
    return clock_type::time_point() + s;
}

// Starts at 0s.
rate_type make_rate() {
    return rate_type(window, at(0s));
}

// Adds `per_second` every second in [from, to).
void run(rate_type& r, uint64_t per_second, std::chrono::seconds from, std::chrono::seconds to) {
    for (auto s = from; s < to; s += 1s) {
        r.add(per_second, at(s));
    }
}

}

BOOST_AUTO_TEST_CASE(test_nothing_added_is_zero) {
    auto r = make_rate();
    auto rate = r.rate(at(10s));
    BOOST_REQUIRE_EQUAL(rate.amount(), 0);
    BOOST_REQUIRE(rate.interval() == 10s);
    BOOST_REQUIRE_EQUAL(rate.per_second(), 0);
}

BOOST_AUTO_TEST_CASE(test_no_time_passed_is_zero) {
    auto r = make_rate();
    r.add(100, at(0s));
    BOOST_REQUIRE_EQUAL(r.rate(at(0s)).per_second(), 0);
    BOOST_REQUIRE_EQUAL(r.total(), 100u);
}

BOOST_AUTO_TEST_CASE(test_constant_rate) {
    auto r = make_rate();
    run(r, 10, 0s, 10min);
    auto rate = r.rate(at(10min));
    BOOST_REQUIRE_EQUAL(rate.per_second(), 10);
    BOOST_REQUIRE_EQUAL(rate.amount(), 2400);
    BOOST_REQUIRE(rate.interval() == window);
}

BOOST_AUTO_TEST_CASE(test_rate_before_window_fills_covers_time_since_start) {
    auto r = make_rate();
    r.add(1000, at(10s));
    auto rate = r.rate(at(20s));
    BOOST_REQUIRE(rate.interval() == 20s);
    BOOST_REQUIRE_EQUAL(rate.per_second(), 50);
    run(r, 10, 20s, 2min);
    BOOST_REQUIRE_EQUAL(r.rate(at(130s)).per_second(), (1000 + 1000) / 130.0);
}

BOOST_AUTO_TEST_CASE(test_window_slides_to_new_rate) {
    auto r = make_rate();
    run(r, 10, 0s, 4min);
    BOOST_REQUIRE_EQUAL(r.rate(at(4min)).per_second(), 10);
    run(r, 20, 4min, 6min);
    // Half the window at 10/s, half at 20/s.
    BOOST_REQUIRE_EQUAL(r.rate(at(6min)).per_second(), 15);
    run(r, 20, 6min, 8min);
    BOOST_REQUIRE_EQUAL(r.rate(at(8min)).per_second(), 20);
}

BOOST_AUTO_TEST_CASE(test_idle_time_counts_without_calls) {
    auto r = make_rate();
    run(r, 10, 0s, 4min);
    // No calls for two minutes: half the window is idle.
    BOOST_REQUIRE_EQUAL(r.rate(at(6min)).per_second(), 5);
    // No calls for another two minutes: the whole window is idle.
    auto rate = r.rate(at(8min));
    BOOST_REQUIRE_EQUAL(rate.per_second(), 0);
    BOOST_REQUIRE(rate.interval() == window);
}

BOOST_AUTO_TEST_CASE(test_long_gap_loses_nothing) {
    auto r = make_rate();
    run(r, 10, 0s, 4min);
    // Much longer than the window without calls, then one addition.
    r.add(600, at(60min));
    BOOST_REQUIRE_EQUAL(r.rate(at(61min)).per_second(), 600 / 240.0);
    BOOST_REQUIRE_EQUAL(r.total(), 2400u + 600u);
}

BOOST_AUTO_TEST_CASE(test_sparse_calls_match_the_window) {
    auto r = make_rate();
    r.add(6000, at(0s));
    BOOST_REQUIRE_EQUAL(r.rate(at(2min)).per_second(), 50);
    // The addition is now older than the window.
    auto rate = r.rate(at(20min));
    BOOST_REQUIRE_EQUAL(rate.per_second(), 0);
    BOOST_REQUIRE(rate.interval() == window);
}

BOOST_AUTO_TEST_CASE(test_lumpy_additions_average_out) {
    auto r = make_rate();
    // One addition of 600 per minute, like a flush, gives 10/s on average.
    for (auto s = 0s; s < 8min; s += 1min) {
        r.add(600, at(s));
    }
    BOOST_REQUIRE_EQUAL(r.rate(at(8min)).per_second(), 10);
}

BOOST_AUTO_TEST_CASE(test_floating_point_quantity) {
    utils::moving_average_rate<utils::units::bytes, double, clock_type, 4> r(window, at(0s));
    for (auto s = 0s; s < 4min; s += 1s) {
        r.add(0.5, at(s));
    }
    BOOST_REQUIRE_CLOSE(r.rate(at(4min)).per_second(), 0.5, 1e-9);
}

// Averages with the same window and start cover the same interval, however
// differently they are fed, so their amounts can be divided by each other.
BOOST_AUTO_TEST_CASE(test_same_window_and_start_cover_the_same_interval) {
    auto a = make_rate();
    auto b = make_rate();
    a.add(100, at(10s));
    // b is first fed much later, and both are fed at times that aren't sample boundaries.
    for (auto s : {150s, 290s, 330s, 395s}) {
        a.add(100, at(s));
        b.add(50, at(s + 7s));
    }
    for (auto now : {170s, 300s, 400s, 900s}) {
        auto rate_a = a.rate(at(now));
        auto rate_b = b.rate(at(now));
        BOOST_REQUIRE(rate_a.interval() == rate_b.interval());
        BOOST_REQUIRE_EQUAL(rate_b.per(rate_a), rate_a.amount() ? rate_b.amount() / rate_a.amount() : 0);
    }
}

// Amounts over different intervals don't relate to each other.
BOOST_AUTO_TEST_CASE(test_per_rejects_different_intervals) {
    utils::rate<utils::units::bytes> a(100, 10s);
    utils::rate<utils::units::bytes> b(100, 20s);
    BOOST_REQUIRE_EQUAL(a.per(utils::rate<utils::units::bytes>(50, 10s)), 2);
    BOOST_REQUIRE_EQUAL(a.per(utils::rate<utils::units::bytes>(0, 10s)), 0);
    BOOST_REQUIRE_THROW(a.per(b), std::runtime_error);
}

// Rates of the same thing add up per second, over the interval both cover.
BOOST_AUTO_TEST_CASE(test_sum) {
    using rate = utils::rate<utils::units::bytes>;
    auto same = rate(100, 10s) + rate(50, 10s);
    BOOST_REQUIRE_EQUAL(same.amount(), 150);
    BOOST_REQUIRE(same.interval() == 10s);
    // 10/s and 30/s.
    auto different = rate(100, 10s) + rate(600, 20s);
    BOOST_REQUIRE_EQUAL(different.per_second(), 40);
    BOOST_REQUIRE(different.interval() == 10s);
    BOOST_REQUIRE((rate(600, 20s) + rate(100, 10s)).interval() == 10s);
    // A rate over an empty interval measured nothing.
    auto with_empty = rate() + rate(100, 10s);
    BOOST_REQUIRE_EQUAL(with_empty.amount(), 100);
    BOOST_REQUIRE(with_empty.interval() == 10s);
    BOOST_REQUIRE((rate(100, 10s) + rate()).interval() == 10s);
}

// How compaction_group derives the flush size for write amplification: a second
// rate accumulates bytes * ln(size), and dividing it by the bytes rate gives the
// byte-weighted mean of ln(size). Both share the window and the start, so they
// cover the same interval and their amounts can be divided.
namespace {

struct bytes_log_size_unit;

struct flush_size_tracker {
    rate_type bytes = make_rate();
    utils::moving_average_rate<bytes_log_size_unit, double, clock_type, 4> bytes_log_size{window, at(0s)};

    void flush(uint64_t size, clock_type::time_point now) {
        bytes.add(size, now);
        bytes_log_size.add(size * std::log(double(size)), now);
    }

    double mean_flush_size(clock_type::time_point now) {
        return std::exp(bytes_log_size.rate(now).per(bytes.rate(now)));
    }
};

constexpr uint64_t MB = 1 << 20;

}

BOOST_AUTO_TEST_CASE(test_mean_flush_size_of_equal_flushes) {
    flush_size_tracker t;
    for (auto s = 0s; s < 4min; s += 10s) {
        t.flush(64 * MB, at(s));
    }
    BOOST_REQUIRE_CLOSE(t.mean_flush_size(at(4min)), 64.0 * MB, 1e-6);
}

BOOST_AUTO_TEST_CASE(test_mean_flush_size_is_weighted_by_bytes) {
    flush_size_tracker t;
    // 10 regular flushes of 1 GB and a storm of 90 flushes of 10 MB. The mean of the
    // sizes would be ~110 MB; weighted by bytes, the small flushes barely count.
    for (int i = 0; i < 10; ++i) {
        t.flush(1024 * MB, at(i * 10s));
    }
    for (int i = 0; i < 90; ++i) {
        t.flush(10 * MB, at(100s + i * 1s));
    }
    auto expected = std::exp((10 * 1024.0 * std::log(1024.0 * MB) + 90 * 10.0 * std::log(10.0 * MB)) / (10 * 1024.0 + 90 * 10.0));
    auto mean = t.mean_flush_size(at(4min));
    BOOST_REQUIRE_CLOSE(mean, expected, 1e-6);
    BOOST_REQUIRE_GT(mean, 600.0 * MB);
}

BOOST_AUTO_TEST_CASE(test_mean_flush_size_follows_the_window) {
    flush_size_tracker t;
    for (auto s = 0s; s < 4min; s += 10s) {
        t.flush(1024 * MB, at(s));
    }
    for (std::chrono::seconds s = 4min; s < 8min; s += 10s) {
        t.flush(64 * MB, at(s));
    }
    // The 1 GB flushes have left the window.
    BOOST_REQUIRE_CLOSE(t.mean_flush_size(at(8min)), 64.0 * MB, 1e-6);
}

BOOST_AUTO_TEST_CASE(test_window) {
    auto r = make_rate();
    BOOST_REQUIRE(r.window() == window);
}

BOOST_AUTO_TEST_CASE(test_window_too_short_is_rejected) {
    BOOST_REQUIRE_THROW(rate_type(clock_type::duration::zero(), at(0s)), std::invalid_argument);
    BOOST_REQUIRE_THROW(rate_type(clock_type::duration(3), at(0s)), std::invalid_argument);
}
