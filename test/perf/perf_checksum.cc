/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "sstables/checksum_utils.hh"
#include "test/lib/make_random_string.hh"
#include "utils/gz/crc_combine.hh"

#include <seastar/testing/perf_tests.hh>

struct crc_test {
    const sstring data = make_random_string(64*1024);
    const sstring data2 = make_random_string(64*1024);
    const uint32_t sum1 = zlib_crc32_checksummer::checksum(data.data(), data.size());
    const uint32_t sum2 = zlib_crc32_checksummer::checksum(data2.data(), data2.size());
};

PERF_TEST_F(crc_test, perf_deflate_crc32_combine) {
    perf_tests::do_not_optimize(
        libdeflate_crc32_checksummer::checksum_combine(sum1, sum2, data.size()));
}

PERF_TEST_F(crc_test, perf_adler_combine) {
    perf_tests::do_not_optimize(
        adler32_utils::checksum_combine(sum1, sum2, data.size()));
}

PERF_TEST_F(crc_test, perf_zlib_crc32_combine) {
    perf_tests::do_not_optimize(
        zlib_crc32_checksummer::checksum_combine(sum1, sum2, data.size()));
}

PERF_TEST_F(crc_test, perf_fast_crc32_combine) {
    perf_tests::do_not_optimize(
        fast_crc32_combine(sum1, sum2, data.size()));
}

PERF_TEST_F(crc_test, perf_deflate_crc32_checksum) {
    perf_tests::do_not_optimize(
        libdeflate_crc32_checksummer::checksum(data.data(), data.size()));
}

PERF_TEST_F(crc_test, perf_adler_checksum) {
    perf_tests::do_not_optimize(
        adler32_utils::checksum(data.data(), data.size()));
}

PERF_TEST_F(crc_test, perf_zlib_crc32_checksum) {
    perf_tests::do_not_optimize(
        zlib_crc32_checksummer::checksum(data.data(), data.size()));
}
