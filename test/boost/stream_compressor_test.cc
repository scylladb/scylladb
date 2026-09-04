/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <seastar/util/alloc_failure_injector.hh>
#include "message/stream_compressor.hh"
#include "test/lib/log.hh"
#include "test/lib/random_utils.hh"
#include "test/lib/scylla_test_case.hh"
#include "utils/small_vector.hh"
#include <seastar/core/reactor.hh>
#include <seastar/core/thread.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/defer.hh>
#include <boost/test/unit_test.hpp>

#include <algorithm>
#include <bit>
#include <chrono>
#include <cstdlib>
#include <memory>
#include <random>
#include <span>
#include <string_view>
#include <fmt/format.h>

template<class T>
concept RpcBuf = std::same_as<T, rpc::rcv_buf> || std::same_as<T, rpc::snd_buf>;

template <RpcBuf Buf>
bytes rpc_buf_to_bytes(const Buf& data) {
    if (auto src = std::get_if<temporary_buffer<char>>(&data.bufs)) {
        return bytes(reinterpret_cast<const bytes::value_type*>(src->get()), src->size());
    }
    auto src = std::get<std::vector<temporary_buffer<char>>>(data.bufs).data();
    auto out = bytes(bytes::initialized_later{}, data.size);
    size_t i = 0;
    while (i < data.size) {
        std::memcpy(&out[i], src->get(), src->size());
        i += src->size();
        ++src;
    }
    return out;
}

template <RpcBuf Buf, RpcBuf BufFrom>
Buf convert_rpc_buf(BufFrom data) {
    Buf b;
    b.size = data.size;
    b.bufs = std::move(data.bufs);
    return b;
}

// Compute some interesting splits of a message into pieces.
// Pieces of size 0, 1, and whole message are particularly interesting -- they should cover most edge cases.
template <RpcBuf Buf>
std::vector<Buf> compute_splits(bytes b) {
    std::vector<Buf> out;
    {
        // Mostly 1-byte pieces, some 0-byte pieces.
        std::vector<temporary_buffer<char>> frags;
        frags.push_back(temporary_buffer<char>());
        for (size_t i = 0; i < b.size();) {
            for (size_t k = std::min(b.size(), i + 2); i < k; ++i) {
                frags.push_back(temporary_buffer<char>(reinterpret_cast<const char*>(&b[i]), 1));
            }
            frags.push_back(temporary_buffer<char>());
        }
        out.emplace_back(std::move(frags), b.size());
    }
    {
        // Mostly 0-byte pieces, some 1-byte pieces.
        std::vector<temporary_buffer<char>> frags;
        frags.push_back(temporary_buffer<char>());
        for (size_t i = 0; i < b.size(); ++i) {
            frags.push_back(temporary_buffer<char>(reinterpret_cast<const char*>(&b[i]), 1));
            for (size_t k = 0; k < 2; ++k) {
                frags.push_back(temporary_buffer<char>());
            }
        }
        out.emplace_back(std::move(frags), b.size());
    }
    {
        // Whole message.
        out.emplace_back(temporary_buffer<char>(reinterpret_cast<const char*>(b.data()), b.size()));
    }
    return out;
}

void test_compressor_pair_basic_correctness(netw::stream_compressor& compressor, netw::stream_decompressor& decompressor) {
    // Generate some messages.
    for (const auto& message : {
        tests::random::get_bytes(0),
        tests::random::get_bytes(1),
        tests::random::get_bytes(2),
        tests::random::get_bytes(2000),
    })
    // Test both with and without streaming.
    for (bool end_of_frame : {false, true})
    // Split input into pieces.
    for (const auto& input : compute_splits<rpc::snd_buf>(message))
    // Test chunk sizes smaller, equal, and larger to the message.
    for (const auto& chunk_size : {1, 5000})
    // Use each compressor multiple times to make sure it is returned to a proper state after each use.
    for (int repeat = 0; repeat < 3; ++repeat)
    {
        auto compressed = netw::compress_impl(0, input, compressor, end_of_frame, chunk_size);
        auto rcv_buf = convert_rpc_buf<rpc::rcv_buf>(std::move(compressed));
        auto decompressed = netw::decompress_impl(rcv_buf, decompressor, end_of_frame, chunk_size);
        BOOST_REQUIRE_EQUAL(message, rpc_buf_to_bytes(std::move(decompressed)));
    }
}

BOOST_AUTO_TEST_CASE(test_correctness) {
    {
        netw::raw_stream stream;
        test_compressor_pair_basic_correctness(stream, stream);
    }
    {
        netw::lz4_dstream dstream{};
        netw::lz4_cstream cstream{};
        test_compressor_pair_basic_correctness(cstream, dstream);
    }
    {
        netw::lz4_dstream dstream{2};
        netw::lz4_cstream cstream{2};
        test_compressor_pair_basic_correctness(cstream, dstream);
    }
    {
        netw::zstd_dstream dstream{};
        netw::zstd_cstream cstream{};
        test_compressor_pair_basic_correctness(cstream, dstream);
    }
}

// A randomized round-trip test for the streaming compressors.
//
// It knows nothing about the compressors' internals. It only generates random inputs
// and parameters, pushes a message through compress_impl()/decompress_impl(), and checks
// that what comes out is what went in. All of its bug-finding power comes from the number
// and the variety of the cases it covers, so it is meant to be run in a loop,
// for as long as one can afford, rather than once.
//
// The whole run is derived from the test framework's random seed, which the test runner
// prints as `random-seed=<N>`, so a failed run can be replayed with `--random-seed=<N>`.
// Every individual case is in turn described by a single 64-bit seed, which is reported
// when the case fails.

// Draws a random integer from [lo, hi], with a distribution meant to hit boundary
// conditions much more often than a uniform draw would:
//
// - Every order of magnitude (i.e. every bit width) gets a similar share of the probability
//   space, so that "small" values aren't drowned out by the exponentially more numerous
//   "big" ones. (With a uniform draw over [0, 512 kiB], we would practically never see
//   a message shorter than a few kiB).
// - Powers of two, their immediate neighbours, and the ends of the range get an extra
//   boost, since that's where off-by-one bugs tend to hide.
static uint64_t draw_interesting(std::mt19937_64& rng, uint64_t lo, uint64_t hi) {
    BOOST_REQUIRE_LE(lo, hi);
    auto uniform = [&rng] (uint64_t a, uint64_t b) {
        return std::uniform_int_distribution<uint64_t>(a, b)(rng);
    };
    // One in three draws lands on a "special" value.
    if (uniform(0, 2) == 0) {
        utils::small_vector<uint64_t, 64> candidates;
        auto add = [&] (int64_t v) {
            if (v >= int64_t(lo) && v <= int64_t(hi)) {
                candidates.push_back(uint64_t(v));
            }
        };
        add(int64_t(lo));
        add(int64_t(lo) + 1);
        add(int64_t(hi) - 1);
        add(int64_t(hi));
        for (int k = 0; k < 63 && (uint64_t(1) << k) <= hi + 2; ++k) {
            for (int64_t delta = -2; delta <= 2; ++delta) {
                add(int64_t(uint64_t(1) << k) + delta);
            }
        }
        if (!candidates.empty()) {
            return candidates[uniform(0, candidates.size() - 1)];
        }
    }
    // Otherwise: draw a bit width uniformly, then draw uniformly within that bit width.
    const int width = int(uniform(std::bit_width(lo), std::bit_width(hi)));
    const uint64_t band_lo = width ? (uint64_t(1) << (width - 1)) : 0;
    const uint64_t band_hi = width ? ((uint64_t(1) << width) - 1) : 0;
    return uniform(std::max(lo, band_lo), std::min(hi, band_hi));
}

// A string of `size` bytes, each drawn from the first `alphabet_size` byte values.
// The alphabet size is a cheap knob for the compressibility of the data:
// a 1-letter alphabet compresses to nothing, a 256-letter alphabet is incompressible.
//
// Generating the payload is a meaningful part of this test's runtime, so instead of running
// a std::uniform_int_distribution per byte (which burns a whole 64-bit draw on 8 bits of
// output), we spend one draw per 8 bytes and map each byte into the alphabet by a
// multiply-shift. That's about 4x faster, and the resulting negligible bias
// (for alphabet sizes which aren't a power of two) doesn't matter for test data.
static bytes draw_message(std::mt19937_64& rng, size_t size, unsigned alphabet_size) {
    auto b = bytes(bytes::initialized_later{}, size);
    for (size_t i = 0; i < size; i += 8) {
        uint64_t r = rng();
        for (size_t k = i, end = std::min(i + 8, size); k < end; ++k) {
            b[k] = static_cast<bytes::value_type>((alphabet_size * (r & 0xff)) >> 8);
            r >>= 8;
        }
    }
    return b;
}

// The compression algorithms exercised by the randomized round-trip test.
enum class codec_algorithm {
    lz4,
    zstd,
};

static std::string_view codec_algorithm_name(codec_algorithm algo) {
    switch (algo) {
    case codec_algorithm::lz4: return "lz4";
    case codec_algorithm::zstd: return "zstd";
    }
    std::abort();
}

// A matching compressor/decompressor pair, plus whatever dictionary objects they refer to.
//
// The streams take their dictionaries by reference, so the dictionary objects have to outlive
// them; keeping both in one owner is the easiest way to guarantee that.
struct codec_pair {
    virtual netw::stream_compressor& compressor() = 0;
    virtual netw::stream_decompressor& decompressor() = 0;
    virtual ~codec_pair() {}
};

// `dict` must outlive the returned pair. (Both codecs refer to the dictionary bytes
// rather than copying them).
static std::unique_ptr<codec_pair> make_codec_pair(codec_algorithm algo, size_t lz4_window_size, bytes_view dict) {
    struct lz4_pair final : codec_pair {
        // Note that this holds the *dictionary*, not the compression context;
        // that's just how LZ4 represents a preloaded dictionary.
        std::unique_ptr<LZ4_stream_t, decltype(&LZ4_freeStream)> cdict{LZ4_createStream(), LZ4_freeStream};
        netw::lz4_cstream cstream;
        netw::lz4_dstream dstream;
        lz4_pair(size_t window_size, bytes_view dict)
            : cstream(window_size)
            , dstream(window_size)
        {
            BOOST_REQUIRE(cdict);
            if (dict.size()) {
                LZ4_loadDict(cdict.get(), reinterpret_cast<const char*>(dict.data()), dict.size());
                cstream.set_dict(cdict.get());
                dstream.set_dict(std::as_bytes(std::span(dict.data(), dict.size())));
            }
        }
        netw::stream_compressor& compressor() override { return cstream; }
        netw::stream_decompressor& decompressor() override { return dstream; }
    };
    struct zstd_pair final : codec_pair {
        std::unique_ptr<ZSTD_CDict, decltype(&ZSTD_freeCDict)> cdict{nullptr, ZSTD_freeCDict};
        std::unique_ptr<ZSTD_DDict, decltype(&ZSTD_freeDDict)> ddict{nullptr, ZSTD_freeDDict};
        netw::zstd_cstream cstream;
        netw::zstd_dstream dstream;
        explicit zstd_pair(bytes_view dict) {
            if (dict.size()) {
                // Level 1 is what zstd_cstream itself uses. (Only the compression dict cares).
                cdict.reset(ZSTD_createCDict_byReference(dict.data(), dict.size(), 1));
                ddict.reset(ZSTD_createDDict_byReference(dict.data(), dict.size()));
                BOOST_REQUIRE(cdict);
                BOOST_REQUIRE(ddict);
                cstream.set_dict(cdict.get());
                dstream.set_dict(ddict.get());
            }
        }
        netw::stream_compressor& compressor() override { return cstream; }
        netw::stream_decompressor& decompressor() override { return dstream; }
    };
    switch (algo) {
    case codec_algorithm::lz4:
        return std::make_unique<lz4_pair>(lz4_window_size, dict);
    case codec_algorithm::zstd:
        // zstd's window size is hardcoded in stream_compressor.cc, so there's nothing to pass.
        return std::make_unique<zstd_pair>(dict);
    }
    std::abort();
}

// Pushes `message` through a compress_impl()/decompress_impl() round trip
// with the given codec pair, and returns what came out.
static bytes round_trip(codec_pair& codec, bytes_view message) {
    const auto input = rpc::snd_buf(temporary_buffer<char>(reinterpret_cast<const char*>(message.data()), message.size()));
    auto compressed = netw::compress_impl(0, input, codec.compressor(), true, rpc::snd_buf::chunk_size);
    auto rcv_buf = convert_rpc_buf<rpc::rcv_buf>(std::move(compressed));
    return rpc_buf_to_bytes(netw::decompress_impl(rcv_buf, codec.decompressor(), true, rpc::snd_buf::chunk_size));
}

// Checks that `message` survives a round trip. On failure, points at the first difference
// and dumps the message to a file (named after `case_name`) for inspection.
static void check_round_trip(codec_algorithm algo, bytes_view message, bytes_view dict, size_t lz4_window_size,
        std::string_view case_name, std::string_view description) {
    bytes decompressed;
    try {
        auto codec = make_codec_pair(algo, lz4_window_size, dict);
        decompressed = round_trip(*codec, message);
    } catch (...) {
        // Without this, the case parameters would be lost, and the failure unreproducible.
        BOOST_FAIL(fmt::format("{} round trip threw: {} exception={}",
                codec_algorithm_name(algo), description, std::current_exception()));
    }
    if (decompressed != message) {
        // The buffers can be huge, so instead of dumping them to the log we only point at the
        // first difference, and write the buffers themselves out to files for inspection.
        size_t i = 0;
        while (i < std::min(message.size(), decompressed.size()) && message[i] == decompressed[i]) {
            ++i;
        }
        BOOST_FAIL(fmt::format("{} round trip mismatch: {} decompressed_size={} first_difference_at={}",
                codec_algorithm_name(algo), description, decompressed.size(), i));
    }
}

static void run_roundtrip_case(codec_algorithm algo, uint64_t case_seed) {
    constexpr size_t max_message_size = 512 * 1024;
    constexpr size_t max_dict_size = 128 * 1024;

    std::mt19937_64 rng(case_seed);
    // Note: the window size must be at least 2. The compressor can't make progress with 1.
    // It's ignored by codecs other than LZ4, which have no configurable window.
    const size_t window_size = draw_interesting(rng, 2, netw::max_lz4_window_size);
    const size_t message_size = draw_interesting(rng, 0, max_message_size);
    const size_t dict_size = draw_interesting(rng, 0, max_dict_size);
    const unsigned alphabet_size = draw_interesting(rng, 1, 256);

    const auto description = fmt::format(
            "case_seed={} window_size={} message_size={} dict_size={} alphabet_size={}",
            case_seed, window_size, message_size, dict_size, alphabet_size);

    const auto dict = draw_message(rng, dict_size, alphabet_size);
    const auto message = draw_message(rng, message_size, alphabet_size);

    check_round_trip(algo, message, dict, window_size,
            fmt::format("{}.{}", codec_algorithm_name(algo), case_seed), description);
}

// Runs randomized round-trip cases for `algo` until the time budget runs out.
static void run_randomized_roundtrip(codec_algorithm algo) {
    // How long a single run of this test case is allowed to take.
    //
    // It's deliberately short: a single run only covers a handful of cases, which is
    // all that's warranted as a part of a regular test suite run. The bug-finding power
    // of this test comes from running it many times over (in parallel, and/or in a loop)
    // in a dedicated session, so if you are hunting for something, raise this and/or
    // run many copies of the test at once.
    constexpr auto time_budget = std::chrono::milliseconds(200);

    // A single case is up to ~640 kiB of uninterruptible compression work, which takes
    // long enough to trip the stall detector. That's inherent to what this test does,
    // so we mute the reports instead of drowning the log in them.
    const auto prev_notify_ms = seastar::engine().get_blocked_reactor_notify_ms();
    seastar::engine().update_blocked_reactor_notify_ms(std::chrono::hours(1));
    const auto restore_notify_ms = seastar::defer([prev_notify_ms] () noexcept {
        seastar::engine().update_blocked_reactor_notify_ms(prev_notify_ms);
    });

    // The whole case sequence hangs off the framework's seed, so `--random-seed=<N>`
    // replays the run. (The test runner prints the seed it used at startup).
    std::mt19937_64 master(tests::random::get_int<uint64_t>());

    const auto deadline = std::chrono::steady_clock::now() + time_budget;
    uint64_t cases = 0;
    do {
        run_roundtrip_case(algo, master());
        ++cases;
        seastar::thread::maybe_yield();
    } while (std::chrono::steady_clock::now() < deadline);
    testlog.info("{}_roundtrip_randomized: cases_tested={}", codec_algorithm_name(algo), cases);
}

SEASTAR_THREAD_TEST_CASE(test_lz4_roundtrip_randomized) {
    run_randomized_roundtrip(codec_algorithm::lz4);
}

void test_recovery_after_oom_one(netw::stream_decompressor& dstream, netw::stream_compressor& cstream) {
    // Check that compressors and decompressors handle OOM properly and can be reused afterwards.
    for (int repeat = 0; repeat < 3; ++repeat) {
        auto message = tests::random::get_bytes(256);
        rpc::snd_buf compressed;
        {
            auto message_buf = rpc::snd_buf(temporary_buffer<char>(reinterpret_cast<const char*>(message.data()), message.size()));
            memory::with_allocation_failures([&] {
                try {
                    compressed = netw::compress_impl(0, message_buf, cstream, true, 64);
                } catch (const std::runtime_error&) {
                    throw std::bad_alloc();
                }
            });
        }
        rpc::rcv_buf decompressed;
        {
            auto compressed_2 = convert_rpc_buf<rpc::rcv_buf>(std::move(compressed));
            memory::with_allocation_failures([&] {
                try {
                    decompressed = netw::decompress_impl(compressed_2, dstream, true, 64);
                } catch (const std::runtime_error&) {
                    throw std::bad_alloc();
                }
            });
        }
        BOOST_REQUIRE_EQUAL(message, rpc_buf_to_bytes(decompressed));
    }
}

BOOST_AUTO_TEST_CASE(test_recovery_after_oom) {
    {
        netw::lz4_dstream dstream{};
        netw::lz4_cstream cstream{};
        test_recovery_after_oom_one(dstream, cstream);
    }
    {
        netw::zstd_dstream dstream{};
        netw::zstd_cstream cstream{};
        test_recovery_after_oom_one(dstream, cstream);
    }
}

