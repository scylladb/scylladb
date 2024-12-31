/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <seastar/core/manual_clock.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/sleep.hh>
#include "test/lib/scylla_test_case.hh"
#include "utils/dict_trainer.hh"
#include "test/lib/random_utils.hh"
#define ZSTD_STATIC_LINKING_ONLY
#include <zstd.h>

using namespace seastar;
using namespace std::chrono_literals;

static auto bytes_to_page(bytes_view b) {
    auto view = std::span(reinterpret_cast<const std::byte*>(b.data()), b.size());
    return utils::dict_sampler::page_type(view.begin(), view.end());
}

// 1. Compute several random page. Each page is incompressible by itself.
// 2. Feed a stream with many copies of each page to the sampler.
// 3. Train a dictionary on the collected sample.
// 4. Check that a message composed of the chosen pages compresses very well.
SEASTAR_THREAD_TEST_CASE(test_zdict_train) {
    // Compute some random pages and concatenate them to form the test message.
    int pagesize = 1024;
    int n_unique_pages = 64;
    auto unique_pages = std::vector<utils::dict_sampler::page_type>();
    auto message = std::vector<std::byte>();
    for (int i = 0; i < n_unique_pages; ++i) {
        unique_pages.push_back(bytes_to_page(tests::random::get_bytes(pagesize)));
        message.insert(message.end(), unique_pages.back().begin(), unique_pages.back().end());
    }

    // Feed the pages to the sampler, many times.
    utils::dict_sampler dt;
    seastar::abort_source as;
    auto fut = dt.sample(utils::dict_sampler::request{
        .min_sampling_duration = seastar::sleep<manual_clock>(1s),
        .min_sampling_bytes = 0,
        .page_size = 1000,
        .sample_size = 1 * 1024 * 1024,
    }, as);
    for (const auto& up : unique_pages) {
        for (int k = 0; k < 1024; ++k) {
            auto p = up;
            memcpy(p.data(), &k, sizeof(k));
            dt.ingest(p);
        }
    }
    seastar::manual_clock::advance(2s);
    auto pages = fut.get();
 
    // Train on the sample.
    auto dict = utils::zdict_train(pages, {.max_dict_size=128000});

    // A reasonable dict should contain the repetitive pages verbatim.
    BOOST_CHECK_GT(dict.size(), n_unique_pages * pagesize);
    // A zstd dictionary should start with ZSTD_MAGIC_DICTIONARY magic.
    BOOST_REQUIRE_EQUAL(ZSTD_MAGIC_DICTIONARY, seastar::read_le<uint32_t>((char*)dict.data()));

    // We passed multiple copies of the same sample to training.
    // A reasonable dictionary based on that will allow us to compress yet another copy of this sample
    // (otherwise uncompressible) perfectly.
    auto cdict = std::unique_ptr<ZSTD_CDict, decltype(&ZSTD_freeCDict)>(ZSTD_createCDict(dict.data(), dict.size(), 3), ZSTD_freeCDict);
    auto cctx = std::unique_ptr<ZSTD_CCtx, decltype(&ZSTD_freeCCtx)>(ZSTD_createCCtx(), ZSTD_freeCCtx);
    std::vector<std::byte> buf1(message.size() * 2);
    std::vector<std::byte> buf2(message.size());
    size_t compressed_size = ZSTD_compress_usingCDict(cctx.get(), buf1.data(), buf1.size(), message.data(), message.size(), cdict.get());
    BOOST_REQUIRE(!ZSTD_isError(compressed_size));
    // The compressed size should be very small -- no actual data, only backreferences to the dict.
    BOOST_REQUIRE_LT(compressed_size, n_unique_pages * 64);
    
    // Sanity check. Check that the compressed data decompresses properly.
    auto ddict = std::unique_ptr<ZSTD_DDict, decltype(&ZSTD_freeDDict)>(ZSTD_createDDict(dict.data(), dict.size()), ZSTD_freeDDict);
    auto dctx = std::unique_ptr<ZSTD_DCtx, decltype(&ZSTD_freeDCtx)>(ZSTD_createDCtx(), ZSTD_freeDCtx);
    size_t decompressed_size = ZSTD_decompress_usingDDict(dctx.get(), buf2.data(), buf2.size(), buf1.data(), compressed_size, ddict.get());
    BOOST_REQUIRE_EQUAL(decompressed_size, message.size());
    BOOST_REQUIRE(std::ranges::equal(message, std::span(buf2).first(decompressed_size)));
}

SEASTAR_THREAD_TEST_CASE(test_zstd_max_dict_size) {
    auto pages = std::vector<utils::dict_sampler::page_type>();
    for (size_t i = 0; i < 1024; ++i) {
        pages.push_back(bytes_to_page(tests::random::get_bytes(1024)));
    }
    auto dict = utils::zdict_train(pages, {.max_dict_size = 1024 * 128});
    BOOST_REQUIRE_EQUAL(dict.size(), 1024 * 128);
    dict = utils::zdict_train(pages, {.max_dict_size = 1024});
    BOOST_REQUIRE_EQUAL(dict.size(), 1024);
}

// Check that `fut` is aborted when `as` is triggered.
static void check_future_is_aborted(future<> fut, abort_source& as) {
    auto ex = std::make_exception_ptr(std::runtime_error("Cancelling!"));
    as.request_abort_ex(ex);
    fut.wait();
    BOOST_REQUIRE(fut.failed());
    fut.get_exception();
    // The below should work in the ideal world, but unfortunately
    // seastar semaphores ignore the aborting exception and propagate
    // their own one...
    // BOOST_REQUIRE(fut.get_exception() == ex);
}

static void test_min_bytes_condition_impl(bool should_abort) {
    utils::dict_sampler dt;
    seastar::abort_source as;
    auto fut = dt.sample(utils::dict_sampler::request{
        .min_sampling_duration = seastar::sleep<manual_clock>(1s),
        .min_sampling_bytes = 8096,
        .page_size = 1024,
        .sample_size = 4096,
    }, as);
    BOOST_REQUIRE(!fut.available());

    manual_clock::advance(2s);
    thread::yield();
    BOOST_REQUIRE(!fut.available());

    dt.ingest(std::array<std::byte, 5000>());
    thread::yield();
    BOOST_REQUIRE(!fut.available());

    if (should_abort) {
        check_future_is_aborted(std::move(fut).discard_result(), as);
        return;
    }

    dt.ingest(std::array<std::byte, 5000>());
    fut.get();
}

// Test that min_sampling_bytes is respected, and can be aborted.
SEASTAR_THREAD_TEST_CASE(test_min_bytes_condition) {
    test_min_bytes_condition_impl(false);
    test_min_bytes_condition_impl(true);
}

static void test_min_duration_condition_impl(bool should_abort) {
    utils::dict_sampler dt;
    seastar::abort_source as;
    auto fut = dt.sample(utils::dict_sampler::request{
        .min_sampling_duration = seastar::sleep_abortable<manual_clock>(1s, as),
        .min_sampling_bytes = 8096,
        .page_size = 1024,
        .sample_size = 4096,
    }, as);

    dt.ingest(std::array<std::byte, 10000>());
    thread::yield();
    BOOST_REQUIRE(!fut.available());

    if (should_abort) {
        check_future_is_aborted(std::move(fut).discard_result(), as);
        return;
    }

    manual_clock::advance(2s);
    thread::yield();
    fut.get();
}

// Test that min_sampling_duration is respected, and can be aborted.
SEASTAR_THREAD_TEST_CASE(test_min_duration_condition) {
    test_min_duration_condition_impl(false);
    test_min_duration_condition_impl(true);
}
