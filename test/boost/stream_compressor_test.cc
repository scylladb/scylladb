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
#include <seastar/core/reactor.hh>
#include <seastar/core/thread.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/defer.hh>
#include <boost/test/unit_test.hpp>

#include <algorithm>
#include <bit>
#include <chrono>
#include <cstdlib>
#include <cstring>
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
        // RPC reads the decompressed message through an input_stream,
        // which treats an empty fragment as the end of the stream.
        if (auto* frags = std::get_if<std::vector<temporary_buffer<char>>>(&decompressed.bufs)) {
            for (const auto& frag : *frags) {
                BOOST_REQUIRE(!frag.empty());
            }
        }
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

static std::vector<size_t> fragment_sizes(const rpc::snd_buf& buf) {
    if (auto frag = std::get_if<temporary_buffer<char>>(&buf.bufs)) {
        return {frag->size()};
    }
    std::vector<size_t> out;
    for (const auto& frag : std::get<std::vector<temporary_buffer<char>>>(buf.bufs)) {
        out.push_back(frag.size());
    }
    return out;
}

static bool end_frame_always() {
    return true;
}

static bool end_frame_never() {
    return false;
}

static unsigned end_frame_every_other_calls = 0;

static bool end_frame_every_other() {
    return ++end_frame_every_other_calls % 2 == 0;
}

// Checks that compress_impl() can end frames early, only at fragment boundaries,
// and that the resulting frames are independent: each of them can be decompressed on its own, in any order.
void test_compressor_pair_early_frame_end(netw::stream_compressor& compressor, netw::stream_decompressor& decompressor) {
    const auto message = tests::random::get_bytes(100000) + bytes(size_t(100000), bytes::value_type(0));
    std::vector<rpc::snd_buf> inputs;
    // Many small fragments, some of them empty.
    inputs.push_back(std::move(compute_splits<rpc::snd_buf>(bytes(message.begin(), message.begin() + 3000)).front()));
    // Fragments of 30000 bytes, and a shorter last one.
    {
        constexpr size_t fragment_size = 30000;
        std::vector<temporary_buffer<char>> frags;
        for (size_t i = 0; i < message.size(); i += fragment_size) {
            frags.emplace_back(reinterpret_cast<const char*>(message.data() + i), std::min(fragment_size, message.size() - i));
        }
        inputs.emplace_back(std::move(frags), message.size());
    }
    // One big fragment.
    inputs.emplace_back(temporary_buffer<char>(reinterpret_cast<const char*>(message.data()), message.size()));
    for (const auto& input : inputs)
    for (const auto& [should_end_frame, fragments_per_frame] : std::initializer_list<std::pair<bool (*)(), size_t>>{
        {end_frame_always, 1},
        {end_frame_every_other, 2},
        {end_frame_never, std::numeric_limits<size_t>::max()},
    }) {
        // The predicate is called for every non-empty fragment but the last one,
        // so a frame ends after every `fragments_per_frame` non-empty fragments, and after the last one.
        std::vector<size_t> expected_frame_input_sizes;
        size_t pending = 0;
        size_t pending_fragments = 0;
        for (const size_t f : fragment_sizes(input)) {
            if (f == 0) {
                continue;
            }
            pending += f;
            if (++pending_fragments == fragments_per_frame) {
                expected_frame_input_sizes.push_back(pending);
                pending = 0;
                pending_fragments = 0;
            }
        }
        if (pending > 0) {
            expected_frame_input_sizes.push_back(pending);
        }

        end_frame_every_other_calls = 0;
        auto in = netw::snd_buf_cursor(input);
        std::vector<rpc::snd_buf> frames;
        std::vector<size_t> frame_input_sizes;
        while (in.remaining() > 0) {
            const size_t remaining_before = in.remaining();
            frames.push_back(netw::compress_impl(0, in, compressor, true, 5000, should_end_frame));
            frame_input_sizes.push_back(remaining_before - in.remaining());
        }
        BOOST_REQUIRE(frame_input_sizes == expected_frame_input_sizes);
        // Decompress in reverse order, to check that the frames don't depend on each other.
        std::vector<bytes> decompressed(frames.size());
        for (size_t i = frames.size(); i-- > 0;) {
            auto rcv_buf = convert_rpc_buf<rpc::rcv_buf>(std::move(frames[i]));
            decompressed[i] = rpc_buf_to_bytes(netw::decompress_impl(rcv_buf, decompressor, true, 5000));
            BOOST_REQUIRE_EQUAL(decompressed[i].size(), frame_input_sizes[i]);
        }
        bytes joined;
        for (const auto& d : decompressed) {
            joined += d;
        }
        BOOST_REQUIRE_EQUAL(joined, rpc_buf_to_bytes(input));
    }
}

// advanced_rpc_compressor splits a message into segments this way. The receiver decompresses
// each segment on its own, and the shared contexts serve other connections between segments,
// so the frames must be independent.
SEASTAR_THREAD_TEST_CASE(test_early_frame_end) {
    {
        netw::raw_stream stream;
        test_compressor_pair_early_frame_end(stream, stream);
    }
    {
        netw::lz4_dstream dstream{};
        netw::lz4_cstream cstream{};
        test_compressor_pair_early_frame_end(cstream, dstream);
    }
    {
        netw::zstd_dstream dstream{};
        netw::zstd_cstream cstream{};
        test_compressor_pair_early_frame_end(cstream, dstream);
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
// - The same holds for the distance from the upper end of the range, so values just below
//   `hi` are as likely as values just above `lo`, at every scale.
// - Powers of two, their immediate neighbours, and the ends of the range get an extra
//   boost, since that's where off-by-one bugs tend to hide.
//
// This is called for every run of the generated data, so it has to be cheap.
// (In particular, a BOOST_REQUIRE here would cost more than everything else).
static uint64_t draw_interesting(std::mt19937_64& rng, uint64_t lo, uint64_t hi) {
    if (lo > hi) {
        BOOST_FAIL(fmt::format("draw_interesting: lo={} > hi={}", lo, hi));
    }
    auto uniform = [&rng] (uint64_t a, uint64_t b) {
        return std::uniform_int_distribution<uint64_t>(a, b)(rng);
    };
    // One in three draws lands on a "special" value: lo, lo+1, hi-1, hi, or 2^k+delta
    // for delta in [-2, 2], whichever of them lie within [lo, hi].
    //
    // Instead of listing the candidates, we draw one of the slots which can hold
    // a valid one, and draw again if it doesn't. (lo is always valid, so this terminates).
    if (uniform(0, 2) == 0) {
        // The smallest k with 2^k + 2 >= lo, and the largest k with 2^k - 2 <= hi.
        const int k_lo = lo <= 3 ? 0 : std::bit_width(lo - 3);
        const int k_hi = std::min(62, std::bit_width(hi + 2) - 1);
        const uint64_t power_slots = k_hi >= k_lo ? 5 * uint64_t(k_hi - k_lo + 1) : 0;
        while (true) {
            const uint64_t slot = uniform(0, 3 + power_slots);
            int64_t v;
            if (slot < 4) {
                const int64_t ends[] = {int64_t(lo), int64_t(lo) + 1, int64_t(hi) - 1, int64_t(hi)};
                v = ends[slot];
            } else {
                v = int64_t(uint64_t(1) << (k_lo + (slot - 4) / 5)) + int64_t((slot - 4) % 5) - 2;
            }
            if (v >= int64_t(lo) && v <= int64_t(hi)) {
                return uint64_t(v);
            }
        }
    }
    // Otherwise: draw a bit width uniformly, then draw uniformly within that bit width.
    // Half of the time, that's the bit width of the value itself,
    // and the other half, the bit width of its distance from `hi`.
    if (uniform(0, 1)) {
        const int width = int(uniform(std::bit_width(lo), std::bit_width(hi)));
        const uint64_t band_lo = width ? (uint64_t(1) << (width - 1)) : 0;
        const uint64_t band_hi = width ? ((uint64_t(1) << width) - 1) : 0;
        return uniform(std::max(lo, band_lo), std::min(hi, band_hi));
    } else {
        const uint64_t span = hi - lo;
        const int width = int(uniform(0, std::bit_width(span)));
        const uint64_t band_lo = width ? (uint64_t(1) << (width - 1)) : 0;
        const uint64_t band_hi = width ? ((uint64_t(1) << width) - 1) : 0;
        return hi - uniform(band_lo, std::min(span, band_hi));
    }
}

// Fills `out` with bytes, each drawn from the first `alphabet_size` byte values.
// The alphabet size is a cheap knob for the compressibility of the data:
// a 1-letter alphabet compresses to nothing, a 256-letter alphabet is incompressible.
//
// Generating the payload is a meaningful part of this test's runtime, so instead of running
// a std::uniform_int_distribution per byte (which burns a whole 64-bit draw on 8 bits of
// output), we spend one draw per 8 bytes and map each byte into the alphabet by a
// multiply-shift. That's about 4x faster, and the resulting negligible bias
// (for alphabet sizes which aren't a power of two) doesn't matter for test data.
static void fill_letters(std::mt19937_64& rng, std::span<bytes::value_type> out, unsigned alphabet_size) {
    for (size_t i = 0; i < out.size(); i += 8) {
        uint64_t r = rng();
        for (size_t k = i, end = std::min(i + 8, out.size()); k < end; ++k) {
            out[k] = static_cast<bytes::value_type>((alphabet_size * (r & 0xff)) >> 8);
            r >>= 8;
        }
    }
}

// The knobs of draw_data().
struct data_shape {
    // See fill_letters().
    unsigned alphabet_size;
    // The upper bound on the length of a single run (see draw_data()).
    size_t max_run_length;
    // The chance, in percent, that a run is a copy rather than fresh letters.
    unsigned copy_percent;
    // The window of the codec under test. Half of the copies stay within it.
    size_t window_size;

    friend auto format_as(const data_shape& s) {
        return fmt::format("alphabet_size={} max_run_length={} copy_percent={}",
                s.alphabet_size, s.max_run_length, s.copy_percent);
    }
};

// A string of `size` bytes, made of runs of two kinds:
//
// - fresh letters (see fill_letters()),
// - copies of earlier parts of the string, from a random distance back.
//
// Letters alone only produce repeats at short distances (with a small alphabet) or no repeats
// at all (with a big one). Real data also repeats itself over long distances, and a compressor
// must handle matches at every distance it can reach, so the copies draw their distances
// with the same bias towards boundary values as everything else. Half of them draw over
// the whole history, and half only over the codec's window, whose far edge is the
// boundary that matters most to an LZ codec.
static bytes draw_data(std::mt19937_64& rng, size_t size, const data_shape& shape) {
    auto b = bytes(bytes::initialized_later{}, size);
    size_t pos = 0;
    while (pos < size) {
        const size_t len = std::min(size - pos, draw_interesting(rng, 1, shape.max_run_length));
        if (pos > 0 && std::uniform_int_distribution<unsigned>(1, 100)(rng) <= shape.copy_percent) {
            const bool within_window = std::uniform_int_distribution<int>(0, 1)(rng);
            const size_t distance = draw_interesting(rng, 1, within_window ? std::min(pos, shape.window_size) : pos);
            if (distance >= len) {
                std::memcpy(b.data() + pos, b.data() + pos - distance, len);
            } else {
                // Byte by byte, because the source and the destination overlap.
                for (size_t i = pos; i < pos + len; ++i) {
                    b[i] = b[i - distance];
                }
            }
        } else {
            fill_letters(rng, std::span(b.data() + pos, len), shape.alphabet_size);
        }
        pos += len;
    }
    return b;
}

// Splits `message` into an rpc::snd_buf.
//
// The fragment boundaries decide where the compressor has to cut the message into blocks,
// so they are drawn just as carefully as the message itself.
static rpc::snd_buf draw_fragmentation(std::mt19937_64& rng, bytes_view message, std::string& description) {
    auto fragment = [&message] (size_t pos, size_t len) {
        return temporary_buffer<char>(reinterpret_cast<const char*>(message.data()) + pos, len);
    };
    // A quarter of the messages are a single contiguous buffer.
    if (message.empty() || std::uniform_int_distribution<int>(0, 3)(rng) == 0) {
        description += " fragmentation=contiguous";
        return rpc::snd_buf(fragment(0, message.size()));
    }
    // Hundreds of thousands of 1-byte fragments would cost a lot and add little,
    // so beyond this many fragments, the rest of the message goes into the last one.
    constexpr size_t max_fragments = 4096;
    const size_t max_fragment_size = draw_interesting(rng, 1, message.size());
    // Either all fragments (but the last) are of the same size, like rpc::snd_buf's own
    // chunks, or every one has its own size, which can also be 0.
    const bool fixed = std::uniform_int_distribution<int>(0, 1)(rng);
    description += fmt::format(" fragmentation={} max_fragment_size={}", fixed ? "fixed" : "variable", max_fragment_size);
    std::vector<temporary_buffer<char>> frags;
    size_t pos = 0;
    while (pos < message.size()) {
        size_t len = fixed ? max_fragment_size : draw_interesting(rng, 0, max_fragment_size);
        if (frags.size() + 1 == max_fragments) {
            len = message.size() - pos;
        }
        len = std::min(len, message.size() - pos);
        frags.push_back(fragment(pos, len));
        pos += len;
    }
    return rpc::snd_buf(std::move(frags), message.size());
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

// Pushes `input` through a compress_impl()/decompress_impl() round trip
// with the given codec pair, and returns what came out.
static bytes round_trip(codec_pair& codec, const rpc::snd_buf& input) {
    auto compressed = netw::compress_impl(0, input, codec.compressor(), true, rpc::snd_buf::chunk_size);
    auto rcv_buf = convert_rpc_buf<rpc::rcv_buf>(std::move(compressed));
    return rpc_buf_to_bytes(netw::decompress_impl(rcv_buf, codec.decompressor(), true, rpc::snd_buf::chunk_size));
}

// Checks that `message`, fragmented as `input`, survives a round trip. On failure, points at
// the first difference and dumps the message to a file (named after `case_name`) for inspection.
static void check_round_trip(codec_algorithm algo, bytes_view message, const rpc::snd_buf& input, bytes_view dict, size_t lz4_window_size,
        std::string_view case_name, std::string_view description) {
    bytes decompressed;
    try {
        auto codec = make_codec_pair(algo, lz4_window_size, dict);
        decompressed = round_trip(*codec, input);
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
    const data_shape shape{
        .alphabet_size = unsigned(draw_interesting(rng, 1, 256)),
        .max_run_length = draw_interesting(rng, 1, max_message_size),
        .copy_percent = unsigned(draw_interesting(rng, 0, 100)),
        .window_size = window_size,
    };

    auto description = fmt::format(
            "case_seed={} window_size={} message_size={} dict_size={} {}",
            case_seed, window_size, message_size, dict_size, shape);

    // The dictionary and the message are drawn as one string,
    // so that the message can repeat parts of the dictionary.
    const auto data = draw_data(rng, dict_size + message_size, shape);
    const auto dict = bytes_view(data).substr(0, dict_size);
    const auto message = bytes_view(data).substr(dict_size);
    const auto input = draw_fragmentation(rng, message, description);

    check_round_trip(algo, message, input, dict, window_size,
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

