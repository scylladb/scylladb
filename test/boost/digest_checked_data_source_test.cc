/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <seastar/core/iostream.hh>

#undef SEASTAR_TESTING_MAIN
#include <seastar/testing/test_case.hh>
#include <seastar/util/memory-data-source.hh>

#include "sstables/checksum_utils.hh"
#include "sstables/digest_checked_data_source.hh"
#include "test/lib/random_utils.hh"

BOOST_AUTO_TEST_SUITE(digest_checked_data_source_test)

static future<> do_test_digest_checked_data_source(std::function<void(std::vector<char>&)> modifier, std::function<future<>(input_stream<char>)> consumer, bool valid) {
    std::vector<char> random_data(std::from_range_t{}, tests::random::get_bytes(2048));
    auto checksum = crc32_utils::checksum(random_data.data(), random_data.size());

    modifier(random_data);

    temporary_buffer<char> buf(random_data.data(), random_data.size());
    auto stream = util::as_input_stream(std::move(buf));

    bool found_mismatch = false;
    auto digest_checked = sstables::make_digest_checked_input_stream(std::move(stream), checksum, [&found_mismatch] (sstring) {
        found_mismatch = true;
    });
    co_await consumer(std::move(digest_checked));

    BOOST_REQUIRE_EQUAL(found_mismatch, !valid);
}

static void noop_modifier(std::vector<char>&) {}

static void corrupt_modifier(std::vector<char>& data) {
    if (data.empty()) {
        data.push_back(1);
    } else {
        data.front() ^= 1;
    }
}

static future<> no_skip_consumer(input_stream<char> input_stream) {
    do {
        co_await input_stream.read();
    } while (!input_stream.eof());
}

static future<> skip_consumer(input_stream<char> input_stream) {
    do {
        auto to_skip = tests::random::get_int(1024);
        co_await input_stream.skip(to_skip);
        co_await input_stream.read();
    } while (!input_stream.eof());
}

SEASTAR_TEST_CASE(test_digest_checked_input_stream_valid_no_skip) {
    co_return co_await do_test_digest_checked_data_source(noop_modifier, no_skip_consumer, true);
}

SEASTAR_TEST_CASE(test_digest_checked_input_stream_valid_skip) {
    co_return co_await do_test_digest_checked_data_source(noop_modifier, skip_consumer, true);
}

SEASTAR_TEST_CASE(test_digest_checked_input_stream_invalid_no_skip) {
    co_return co_await do_test_digest_checked_data_source(corrupt_modifier, no_skip_consumer, false);
}

SEASTAR_TEST_CASE(test_digest_checked_input_stream_invalid_skip) {
    co_return co_await do_test_digest_checked_data_source(corrupt_modifier, skip_consumer, false);
}

BOOST_AUTO_TEST_SUITE_END()