/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */


#define BOOST_TEST_MODULE core

#include <boost/test/unit_test.hpp>
#include "utils/estimated_histogram.hh"
#include "utils/histogram.hh"
#include "utils/histogram_metrics_helper.hh"

template<uint64_t MIN, uint64_t MAX, std::vector<uint64_t>::size_type NUM_BUCKETS>
std::string validate_histogram(utils::approx_exponential_histogram<MIN, MAX, NUM_BUCKETS>& h, const std::vector<uint64_t>& r) {
    size_t i = 0;
    for (; i < r.size(); i++) {
        if (r[i] != h.get(i)) {
            return format("{:d} != {:d}", r[i], h.get(i)) ;
        }
    }
    for (; i < h.size(); i++) {
        if (h.get(i)) {
            return format("{:d} != 0", h.get(i)) ;
        }
    }
    return "";
}

std::string validate_histogram(seastar::metrics::histogram& h, const std::vector<uint64_t>& counts, const std::vector<uint64_t>& limits) {
    size_t i = 0;
    for (; i < counts.size(); i++) {
        if (counts[i] != h.buckets[i].count) {
            return format("Bucket {} limit {}  count {} !=  limit {} count {}", i, limits[i], counts[i], h.buckets[i].upper_bound, h.buckets[i].count);
        }
    }
    for (; i < h.buckets.size(); i++) {
        if (h.buckets[i].count != counts[counts.size() - 1]) {
            return format("Bucket {} limit {}  count {} !=  limit {} count {}", i, limits[i], counts[counts.size() - 1], h.buckets[i].upper_bound, h.buckets[i].count);
        }
    }
    return "";
}

BOOST_AUTO_TEST_CASE(test_histogram_bucket_limits) {
    std::vector<size_t> limits{128, 160, 192, 224, 256, 320, 384, 448, 512, 640, 768, 896, 1024};
    utils::approx_exponential_histogram<128, 1024, 4> hist;
    BOOST_CHECK_EQUAL(hist.NUM_EXP_RANGES, 3);
    BOOST_CHECK_EQUAL(hist.NUM_BUCKETS, 13);
    BOOST_CHECK_EQUAL(hist.PRECISION_BITS, 2);
    BOOST_CHECK_EQUAL(hist.LOWER_BITS_MASK, 3);

    BOOST_CHECK_EQUAL(hist.BASESHIFT, 7);
    for (size_t i = 0; i < limits.size(); i++) {
        BOOST_CHECK_EQUAL(hist.get_bucket_lower_limit(i), limits[i]);
    }
}

BOOST_AUTO_TEST_CASE(test_basic_estimated) {
    utils::approx_exponential_histogram<128, 1024, 4> hist;
    hist.add(1);
    validate_histogram(hist, {1});
    hist.add(100);
    hist.add(128);
    hist.add(129);
    hist.add(159);
    BOOST_CHECK_EQUAL(validate_histogram(hist, {5}), "");
    hist.add(160);
    hist.add(161);
    hist.add(191);
    hist.add(192);
    BOOST_CHECK_EQUAL(validate_histogram(hist, {5, 3, 1}), "");
    hist.add(223);
    BOOST_CHECK_EQUAL(validate_histogram(hist, {5, 3, 2}), "");
    hist.add(224);
    hist.add(225);
    hist.add(255);
    hist.add(253);
    BOOST_CHECK_EQUAL(validate_histogram(hist, {5, 3, 2, 4}), "");
    hist.add(256);
    hist.add(260);
    hist.add(258);
    hist.add(258);
    hist.add(260);
    hist.add(319);
    BOOST_CHECK_EQUAL(validate_histogram(hist, {5, 3, 2, 4, 6}), "");
    hist.add(1023);
    hist.add(1024);
    hist.add(1025);
    BOOST_CHECK_EQUAL(validate_histogram(hist, {5, 3, 2, 4, 6, 0, 0, 0, 0, 0, 0, 1, 2}), "");
    auto res = to_metrics_histogram(hist);
    BOOST_CHECK_EQUAL(res.sample_count, 23);
    BOOST_CHECK_EQUAL(res.sample_sum, 7840);
    BOOST_CHECK_EQUAL(validate_histogram(res, {5, 8, 10, 14, 20, 20, 20, 20, 20, 20, 20, 21}, {160, 192, 224, 256, 320, 384, 448, 512, 640, 768, 896, 1024}), "");
}

BOOST_AUTO_TEST_CASE(test_estimated_statistics) {
    utils::approx_exponential_histogram<128, 1024, 4> hist;
    hist.add(1);
    hist.add(160);
    BOOST_CHECK_EQUAL(hist.min(), 128);
    BOOST_CHECK_EQUAL(hist.max(), 192);
    hist.add(160);
    hist.add(160);
    BOOST_CHECK_EQUAL(hist.mean(), (128 + 160 + 160 + 3 + 160)/4);
    hist *= 0.5;
    BOOST_CHECK_EQUAL(hist.get(1), 1);
}

BOOST_AUTO_TEST_CASE(test_summary_p95_p99) {
    utils::summary_calculator sc;
    std::vector<int> ms{512, 640, 768, 896, 1024, 1280, 1536, 1792, 2048, 2560,
        3072, 3584, 4096, 5120, 6144, 7168, 8192, 10240, 12288, 14336, 16384};
    std::vector<std::chrono::microseconds> durations;
    for (auto i : ms) {
        durations.push_back(std::chrono::microseconds(i + 1));
    }
    for (int i = 0; i < 20; i++) {
        const std::chrono::microseconds& d = durations[i];
        for (int j = 0; j < 5; j++) {
            sc.mark(d);
        }
    }
    sc.update();
    BOOST_CHECK_EQUAL(sc.summary()[0], ms[10]);
    BOOST_CHECK_EQUAL(sc.summary()[1], ms[19]);
    BOOST_CHECK_EQUAL(sc.summary()[2], ms[20]);
}

BOOST_AUTO_TEST_CASE(test_summary_infinite_bucket) {
    utils::summary_calculator sc;
    for (int i = 0; i < 20; i++) {
        sc.mark(std::chrono::seconds(34));
    }
    sc.update();
    BOOST_CHECK_EQUAL(sc.summary()[0], 33554432);
    BOOST_CHECK_EQUAL(sc.summary()[1], 33554432);
    BOOST_CHECK_EQUAL(sc.summary()[2], 33554432);
}

