/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 *
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "sstables/checksum_utils.hh"
#include "test/lib/make_random_string.hh"
#include "utils/gz/crc_combine.hh"

#include "seastar/include/seastar/testing/perf_tests.hh"

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
