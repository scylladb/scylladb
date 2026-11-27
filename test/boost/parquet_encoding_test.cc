/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// Tests of the Parquet encodings and page compression in sstables/parquet.
//
// Every encoding and compression codec is checked by round trips through
// our own code, and in both directions against Arrow C++, the reference
// implementation: Arrow decodes what we encode, and we decode what Arrow
// encodes. Arrow's encoders, decoders, and compression codecs start no
// threads.
//
// The corruption tests feed truncated and bit-flipped input to the
// decoders. Each must either decode or raise the malformed sstable error.
// The sanitizers of the debug mode check that nothing reads out of bounds.
//
// Random inputs come from fixed seeds, so the tests are repeatable.

#include <array>
#include <bit>
#include <cmath>
#include <cstring>
#include <limits>
#include <random>
#include <span>
#include <string>
#include <vector>

#include <lz4.h>
#include <snappy.h>
#define ZSTD_STATIC_LINKING_ONLY
#include <zstd.h>

#include <seastar/core/memory.hh>
#include <seastar/core/preempt.hh>
#include <seastar/core/scheduling.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/with_scheduling_group.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/defer.hh>

// Arrow's headers use std::is_trivial, which C++26 deprecates.
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#include <arrow/util/compression.h>
#pragma GCC diagnostic pop
#include <parquet/column_reader.h>
#include <parquet/column_writer.h>
#include <parquet/encoding.h>
#include <parquet/schema.h>
#include <parquet/types.h>

#include "sstables/exceptions.hh"
#include "sstables/parquet/bit_util.hh"
#include "sstables/parquet/compression.hh"
#include "sstables/parquet/encoding.hh"
#include "sstables/parquet/rle.hh"
#include "test/lib/log.hh"

namespace pq = sstables::parquet;
using sstables::malformed_sstable_exception;

namespace {

constexpr size_t piece = 128 * 1024;

std::string to_string(const bytes_ostream& b) {
    std::string s;
    for (bytes_view frag : b.fragments()) {
        s.append(reinterpret_cast<const char*>(frag.data()), frag.size());
    }
    return s;
}

std::string to_string(pq::byte_array_view v) {
    std::string s;
    for (bytes_view frag : v) {
        s.append(reinterpret_cast<const char*>(frag.data()), frag.size());
    }
    return s;
}

std::string to_string(const fragmented_temporary_buffer& b) {
    return to_string(pq::byte_array_view(b));
}

bytes_ostream to_ostream(std::string_view s) {
    bytes_ostream out;
    out.write(s.data(), s.size());
    return out;
}

// A buffer with contiguous pieces of `piece_size` bytes, to exercise values
// that cross pieces.
fragmented_temporary_buffer to_buffer(std::string_view s, size_t piece_size) {
    return pq::to_fragmented_buffer(to_ostream(s), piece_size);
}

const uint8_t* bytes_of(const std::string& s) {
    return reinterpret_cast<const uint8_t*>(s.data());
}

// Calls `fn` on every truncation and every single bit flip of `input`.
// `fn` must either succeed or raise the malformed sstable error.
template <typename Fn>
void for_each_damage(const std::string& input, Fn&& fn, size_t max_bits = 4096) {
    auto attempt = [&] (const std::string& damaged) {
        try {
            fn(damaged);
        } catch (const malformed_sstable_exception&) {
        }
    };
    for (size_t n = 0; n < input.size(); ++n) {
        attempt(input.substr(0, n));
    }
    const size_t bits = std::min(input.size() * 8, max_bits);
    for (size_t bit = 0; bit < bits; ++bit) {
        auto damaged = input;
        damaged[bit / 8] ^= char(1 << (bit % 8));
        attempt(damaged);
    }
}

// Arrow's type of each of our value types.
template <typename T> struct arrow_type;
template <> struct arrow_type<bool> { using type = ::parquet::BooleanType; };
template <> struct arrow_type<int32_t> { using type = ::parquet::Int32Type; };
template <> struct arrow_type<int64_t> { using type = ::parquet::Int64Type; };
template <> struct arrow_type<float> { using type = ::parquet::FloatType; };
template <> struct arrow_type<double> { using type = ::parquet::DoubleType; };

::parquet::Encoding::type to_arrow(pq::encoding e) {
    return static_cast<::parquet::Encoding::type>(e);
}

std::unique_ptr<::parquet::ColumnDescriptor> make_descriptor(::parquet::Type::type t, int length = -1) {
    auto node = ::parquet::schema::PrimitiveNode::Make("v", ::parquet::Repetition::REQUIRED, t, ::parquet::ConvertedType::NONE, length);
    return std::make_unique<::parquet::ColumnDescriptor>(node, 0, 0);
}

template <typename T>
std::unique_ptr<::parquet::ColumnDescriptor> make_descriptor() {
    return make_descriptor(arrow_type<T>::type::type_num);
}

template <typename T>
bool same_values(const std::vector<T>& a, const std::vector<T>& b) {
    // Floating point values are compared by their bits, so that NaN equals itself.
    return a.size() == b.size() && std::equal(a.begin(), a.end(), b.begin(), [] (T x, T y) {
        return std::memcmp(&x, &y, sizeof(T)) == 0;
    });
}

} // anonymous namespace

// Bit packing

SEASTAR_THREAD_TEST_CASE(test_bit_packing) {
    // The example of Encodings.md: 0 to 7 with bit width 3.
    {
        const uint64_t values[8] = {0, 1, 2, 3, 4, 5, 6, 7};
        uint8_t out[3];
        pq::pack8(values, 3, out);
        BOOST_REQUIRE_EQUAL(out[0], 0b10001000);
        BOOST_REQUIRE_EQUAL(out[1], 0b11000110);
        BOOST_REQUIRE_EQUAL(out[2], 0b11111010);
    }
    std::mt19937_64 rng(1);
    for (unsigned width = 0; width <= 64; ++width) {
        const uint64_t mask = width == 64 ? ~uint64_t(0) : (uint64_t(1) << width) - 1;
        for (int i = 0; i < 100; ++i) {
            uint64_t in[8];
            for (auto& v : in) {
                v = rng() & mask;
            }
            if (i == 0) {
                std::fill_n(in, 8, mask);
            }
            uint8_t packed[64 + 1];
            packed[width] = 0xab;
            pq::pack8(in, width, packed);
            BOOST_REQUIRE_EQUAL(packed[width], 0xab);
            uint64_t out[8];
            pq::unpack8(packed, width, out);
            BOOST_REQUIRE(std::equal(in, in + 8, out));
            if (width <= 32) {
                uint32_t in32[8];
                uint32_t out32[8];
                std::copy_n(in, 8, in32);
                uint8_t packed32[32];
                pq::pack8(in32, width, packed32);
                BOOST_REQUIRE(std::equal(packed, packed + width, packed32));
                pq::unpack8(packed32, width, out32);
                BOOST_REQUIRE(std::equal(in32, in32 + 8, out32));
            }
        }
    }
}

SEASTAR_THREAD_TEST_CASE(test_byte_reader) {
    bytes_ostream b;
    pq::write_uleb128(b, 0);
    pq::write_uleb128(b, 127);
    pq::write_uleb128(b, 128);
    pq::write_uleb128(b, std::numeric_limits<uint64_t>::max());
    pq::write_uleb128(b, pq::zigzag_encode(std::numeric_limits<int64_t>::min()));
    pq::write_le<uint32_t>(b, 0xdeadbeef);
    const auto s = to_string(b);
    BOOST_REQUIRE_EQUAL(s.size(), 1 + 1 + 2 + 10 + 10 + 4);
    for (size_t p : {size_t(1), size_t(3), piece}) {
        auto buf = to_buffer(s, p);
        pq::byte_reader r{pq::byte_array_view(buf)};
        BOOST_REQUIRE_EQUAL(r.read_uleb128(), 0);
        BOOST_REQUIRE_EQUAL(r.read_uleb128(), 127);
        BOOST_REQUIRE_EQUAL(r.read_uleb128(), 128);
        BOOST_REQUIRE_EQUAL(r.read_uleb128(), std::numeric_limits<uint64_t>::max());
        BOOST_REQUIRE_EQUAL(r.read_zigzag(), std::numeric_limits<int64_t>::min());
        BOOST_REQUIRE_EQUAL(r.read_le<uint32_t>(), 0xdeadbeef);
        BOOST_REQUIRE_EQUAL(r.remaining(), 0);
        BOOST_REQUIRE_THROW(r.read_byte(), malformed_sstable_exception);
    }
    // A ULEB128 integer longer than 64 bits.
    for (const std::string& bad : {std::string(10, '\xff') + '\x01', std::string(9, '\xff') + '\x02', std::string(3, '\x80')}) {
        auto buf = to_buffer(bad, piece);
        pq::byte_reader r{pq::byte_array_view(buf)};
        BOOST_REQUIRE_THROW(r.read_uleb128(), malformed_sstable_exception);
    }
}

// The RLE/bit-packing hybrid

namespace {

std::vector<uint32_t> decode_rle(const std::string& data, unsigned width, size_t count, size_t piece_size) {
    auto buf = to_buffer(data, piece_size);
    pq::rle_bp_decoder d(pq::byte_array_view(buf), width);
    std::vector<uint32_t> out(count);
    d.decode(std::span(out));
    return out;
}

// Random sequences with runs of various lengths.
std::vector<uint32_t> random_levels(std::mt19937_64& rng, unsigned width, size_t count) {
    const uint64_t max = width == 32 ? std::numeric_limits<uint32_t>::max() : (uint64_t(1) << width) - 1;
    std::vector<uint32_t> v;
    std::uniform_int_distribution<int> kind(0, 3);
    while (v.size() < count) {
        const uint32_t value = rng() & max;
        size_t len;
        switch (kind(rng)) {
        case 0: len = 1; break;
        case 1: len = 1 + rng() % 7; break;
        case 2: len = 8 + rng() % 20; break;
        default: len = 1 + rng() % 300; break;
        }
        v.insert(v.end(), std::min(len, count - v.size()), value);
    }
    return v;
}

} // anonymous namespace

SEASTAR_THREAD_TEST_CASE(test_rle_hand_written) {
    // Eight distinct values become one bit-packed run.
    {
        pq::rle_bp_encoder e(3);
        for (uint32_t v = 0; v < 8; ++v) {
            e.append(v);
        }
        BOOST_REQUIRE_EQUAL(to_string(e.finish()), std::string("\x03\x88\xc6\xfa"));
    }
    // A long run becomes an RLE run: header 100 << 1, then the value in one byte.
    {
        pq::rle_bp_encoder e(1);
        e.append_run(1, 100);
        BOOST_REQUIRE_EQUAL(to_string(e.finish()), std::string("\xc8\x01\x01", 3));
    }
    // Three literals are completed to a group by the run that follows.
    {
        pq::rle_bp_encoder e(2);
        e.append(1);
        e.append(2);
        e.append(3);
        e.append_run(0, 20);
        // Group: 1, 2, 3, 0, 0, 0, 0, 0 in 2 bits; then an RLE run of 15 zeros.
        BOOST_REQUIRE_EQUAL(to_string(e.finish()), std::string("\x03\x39\x00\x1e\x00", 5));
    }
    // A short tail is padded to a whole group.
    {
        pq::rle_bp_encoder e(4);
        e.append(15);
        BOOST_REQUIRE_EQUAL(to_string(e.finish()), std::string("\x03\x0f\x00\x00\x00", 5));
    }
    // Values without runs are bit-packed, and next_packed() returns all
    // of them. Small and large contiguous pieces take the copying and the
    // in-place unpacking.
    for (size_t p : {size_t(7), piece}) {
        pq::rle_bp_encoder e(3);
        const size_t n = 1000;
        for (size_t i = 0; i < n; ++i) {
            e.append(i % 5);
        }
        auto buf = to_buffer(to_string(e.finish()), p);
        pq::rle_bp_decoder d(pq::byte_array_view(buf), 3);
        std::array<uint8_t, 20> batch;
        for (size_t at = 0; at < n;) {
            const size_t got = d.next_packed(std::span(batch.data(), std::min(batch.size(), n - at)));
            BOOST_REQUIRE_GE(got, 1);
            for (size_t k = 0; k < got; ++k) {
                BOOST_REQUIRE_EQUAL((at + k) % 5, batch[k]);
            }
            at += got;
        }
    }
    // Bit width 0.
    {
        pq::rle_bp_encoder e(0);
        e.append_run(0, 1000);
        auto s = to_string(e.finish());
        BOOST_REQUIRE_EQUAL(s, std::string("\xd0\x0f", 2));
        BOOST_REQUIRE(decode_rle(s, 0, 1000, piece) == std::vector<uint32_t>(1000, 0));
    }
    // An empty sequence.
    {
        pq::rle_bp_encoder e(5);
        BOOST_REQUIRE_EQUAL(e.finish().size(), 0);
    }
}

SEASTAR_THREAD_TEST_CASE(test_rle_round_trip) {
    std::mt19937_64 rng(2);
    for (unsigned width = 0; width <= 32; ++width) {
        for (size_t count : {size_t(0), size_t(1), size_t(7), size_t(8), size_t(9), size_t(1000), size_t(5000)}) {
            const auto values = random_levels(rng, width, count);
            pq::rle_bp_encoder e(width);
            for (size_t i = 0; i < values.size();) {
                // Append runs of equal values with one call, and the rest one by one.
                size_t j = i + 1;
                while (j < values.size() && values[j] == values[i]) {
                    ++j;
                }
                if (rng() % 2) {
                    e.append_run(values[i], j - i);
                } else {
                    for (size_t k = i; k < j; ++k) {
                        e.append(values[k]);
                    }
                }
                i = j;
            }
            BOOST_REQUIRE_EQUAL(e.count(), count);
            const auto estimate = e.estimated_size();
            const auto data = to_string(e.finish());
            BOOST_REQUIRE_GE(estimate, data.size());
            for (size_t p : {size_t(1), size_t(5), piece}) {
                BOOST_REQUIRE(decode_rle(data, width, count, p) == values);
            }
            // next_run() returns runs of equal values that add up to the
            // sequence. The caller bounds each run by the values it expects,
            // because the last bit-packed group is padded.
            auto buf = to_buffer(data, 7);
            pq::rle_bp_decoder d(pq::byte_array_view(buf), width);
            size_t pos = 0;
            while (pos < count) {
                auto r = d.next_run(std::min<uint64_t>(1 + rng() % 50, count - pos));
                BOOST_REQUIRE_GE(r.length, 1);
                BOOST_REQUIRE_LE(pos + r.length, count);
                for (size_t k = 0; k < r.length; ++k) {
                    BOOST_REQUIRE_EQUAL(values[pos + k], r.value);
                }
                pos += r.length;
            }
            // next_packed() returns the bit-packed values in batches, and
            // nothing at an RLE run, which next_run() then returns. Small
            // and large contiguous pieces take the copying and the in-place
            // unpacking.
            if (width <= 8) {
                for (size_t p : {size_t(7), piece}) {
                    auto packed_buf = to_buffer(data, p);
                    pq::rle_bp_decoder pd(pq::byte_array_view(packed_buf), width);
                    size_t at = 0;
                    while (at < count) {
                        std::array<uint8_t, 20> batch;
                        const size_t max = std::min<size_t>(1 + rng() % batch.size(), count - at);
                        const size_t n = pd.next_packed(std::span(batch.data(), max));
                        BOOST_REQUIRE_LE(n, max);
                        for (size_t k = 0; k < n; ++k) {
                            BOOST_REQUIRE_EQUAL(values[at + k], batch[k]);
                        }
                        at += n;
                        if (!n) {
                            auto r = pd.next_run(count - at);
                            BOOST_REQUIRE_GE(r.length, 1);
                            for (size_t k = 0; k < r.length; ++k) {
                                BOOST_REQUIRE_EQUAL(values[at + k], r.value);
                            }
                            at += r.length;
                        }
                    }
                }
            }
            // The estimator is close to the real size.
            pq::rle_size_estimator est;
            for (auto v : values) {
                est.append(v);
            }
            BOOST_REQUIRE_LE(est.estimate(width), data.size() * 2 + 16);
            BOOST_REQUIRE_LE(data.size(), est.estimate(width) * 2 + 16);
        }
    }
}

SEASTAR_THREAD_TEST_CASE(test_rle_long_runs) {
    // A null run is appended in constant time, and the decoder returns it
    // as a whole. Runs longer than 2^31 - 1 are split.
    pq::rle_bp_encoder e(1);
    e.append(1);
    e.append_run(0, 5'000'000'000);
    e.append_run(0, 1);
    e.append(1);
    const auto data = to_string(e.finish());
    BOOST_REQUIRE_LT(data.size(), 32);
    auto buf = to_buffer(data, piece);
    pq::rle_bp_decoder d(pq::byte_array_view(buf), 1);
    BOOST_REQUIRE_EQUAL(d.next(), 1);
    uint64_t zeros = 0;
    while (zeros < 5'000'000'001) {
        auto r = d.next_run(std::numeric_limits<uint64_t>::max());
        BOOST_REQUIRE_EQUAL(r.value, 0);
        zeros += r.length;
    }
    BOOST_REQUIRE_EQUAL(zeros, 5'000'000'001);
    BOOST_REQUIRE_EQUAL(d.next(), 1);
}

SEASTAR_THREAD_TEST_CASE(test_rle_malformed) {
    // A value that does not fit the bit width.
    BOOST_REQUIRE_THROW(decode_rle(std::string("\x10\x05", 2), 2, 8, piece), malformed_sstable_exception);
    // Fewer values than requested.
    BOOST_REQUIRE_THROW(decode_rle(std::string("\x10\x01", 2), 2, 9, piece), malformed_sstable_exception);
    BOOST_REQUIRE_THROW(decode_rle(std::string("\x05\x01", 2), 2, 8, piece), malformed_sstable_exception);
    // Damaged encodings.
    std::mt19937_64 rng(3);
    for (unsigned width : {1u, 3u, 17u, 32u}) {
        const auto values = random_levels(rng, width, 100);
        pq::rle_bp_encoder e(width);
        for (auto v : values) {
            e.append(v);
        }
        for_each_damage(to_string(e.finish()), [&] (const std::string& s) {
            decode_rle(s, width, values.size(), 3);
        });
    }
}

SEASTAR_THREAD_TEST_CASE(test_rle_arrow_levels) {
    std::mt19937_64 rng(4);
    for (int16_t max_level : {1, 2, 3, 7}) {
        const unsigned width = pq::level_bit_width(max_level);
        for (int count : {1, 100, 10000}) {
            std::vector<int16_t> levels;
            for (auto v : random_levels(rng, width, count)) {
                levels.push_back(v % (max_level + 1));
            }
            // Ours to Arrow.
            {
                pq::rle_bp_encoder e(width);
                for (auto l : levels) {
                    e.append(l);
                }
                const auto data = to_string(e.finish());
                ::parquet::LevelDecoder d;
                d.SetDataV2(data.size(), max_level, count, bytes_of(data));
                std::vector<int16_t> out(count);
                BOOST_REQUIRE_EQUAL(d.Decode(count, out.data()), count);
                BOOST_REQUIRE(out == levels);
            }
            // Arrow to ours.
            {
                const int size = ::parquet::LevelEncoder::MaxBufferSize(::parquet::Encoding::RLE, max_level, count);
                std::string buf(size, '\0');
                ::parquet::LevelEncoder e;
                e.Init(::parquet::Encoding::RLE, max_level, count, reinterpret_cast<uint8_t*>(buf.data()), size);
                BOOST_REQUIRE_EQUAL(e.Encode(count, levels.data()), count);
                buf.resize(e.len());
                auto fb = to_buffer(buf, 3);
                pq::rle_bp_decoder d(pq::byte_array_view(fb), width);
                std::vector<int16_t> out(count);
                d.decode(std::span(out));
                BOOST_REQUIRE(out == levels);
            }
        }
    }
}

// Value encodings of fixed-width types

namespace {

// Value generators: each returns `count` values of a different shape.
template <typename T>
std::vector<std::vector<T>> value_sets(std::mt19937_64& rng) {
    std::vector<std::vector<T>> sets;
    auto add = [&] (size_t count, auto&& gen) {
        std::vector<T> v;
        for (size_t i = 0; i < count; ++i) {
            v.push_back(gen(i));
        }
        sets.push_back(std::move(v));
    };
    auto any = [&] (size_t) {
        if constexpr (std::same_as<T, bool>) {
            return bool(rng() & 1);
        } else if constexpr (std::is_integral_v<T>) {
            return T(rng());
        } else {
            // Any bit pattern, including NaNs and infinities.
            using U = std::conditional_t<sizeof(T) == 4, uint32_t, uint64_t>;
            return std::bit_cast<T>(U(rng()));
        }
    };
    add(0, any);
    add(1, any);
    add(1000, any);
    add(129, any);
    add(3000, [&] (size_t i) { return T(i % 2 ? 1 : 0); });
    add(2000, [&] (size_t) { return T(rng() % 4); });
    add(1000, [&] (size_t) { return T(1); });
    add(2000, [&] (size_t i) { return T(i / 300 % 2); });
    if constexpr (std::is_integral_v<T> && !std::same_as<T, bool>) {
        // Extremes, whose deltas overflow and must wrap around.
        constexpr T lo = std::numeric_limits<T>::min();
        constexpr T hi = std::numeric_limits<T>::max();
        add(500, [&] (size_t i) { return i % 2 ? lo : hi; });
        add(500, [&] (size_t i) { return std::array<T, 5>{lo, hi, 0, -1, hi}[(i * 7 + i / 3) % 5]; });
        add(1000, [&] (size_t i) { return T(i * 1000 + rng() % 100); });
        add(1000, [&] (size_t i) { return T(lo + T(i)); });
    }
    if constexpr (std::is_floating_point_v<T>) {
        add(8, [&] (size_t i) {
            return std::array<T, 8>{T(0), T(-0.0), std::numeric_limits<T>::quiet_NaN(), -std::numeric_limits<T>::quiet_NaN(),
                    std::numeric_limits<T>::infinity(), -std::numeric_limits<T>::infinity(), std::numeric_limits<T>::denorm_min(),
                    std::numeric_limits<T>::max()}[i];
        });
    }
    return sets;
}

template <typename T>
std::vector<T> decode_values(pq::encoding e, const std::string& data, size_t count, size_t piece_size,
        const pq::dictionary<T>* dict = nullptr) {
    auto buf = to_buffer(data, piece_size);
    auto d = pq::make_value_decoder<T>(pq::fixed_width_values<T>::type, e, pq::byte_array_view(buf), count, dict);
    // Decode in batches of varying size. The buffer is not a std::vector,
    // because std::vector<bool> has no contiguous storage.
    std::unique_ptr<T[]> buf_out(new T[count + 1]);
    std::span<T> rest(buf_out.get(), count);
    size_t batch = 1;
    while (!rest.empty()) {
        const size_t n = std::min(batch, rest.size());
        d->decode(rest.first(n));
        rest = rest.subspan(n);
        batch = batch * 3 + 1;
    }
    std::vector<T> out(buf_out.get(), buf_out.get() + count);
    BOOST_REQUIRE_EQUAL(d->values_left(), 0);
    return out;
}

template <typename T>
std::vector<T> arrow_decode(pq::encoding e, const std::string& data, size_t count) {
    using DType = typename arrow_type<T>::type;
    auto descr = make_descriptor<T>();
    auto d = ::parquet::MakeTypedDecoder<DType>(to_arrow(e), descr.get());
    d->SetData(count, bytes_of(data), data.size());
    std::unique_ptr<T[]> out(new T[count + 1]);
    BOOST_REQUIRE_EQUAL(d->Decode(out.get(), count), int(count));
    return std::vector<T>(out.get(), out.get() + count);
}

template <typename T>
std::string arrow_encode(pq::encoding e, const std::vector<T>& values) {
    using DType = typename arrow_type<T>::type;
    auto descr = make_descriptor<T>();
    auto enc = ::parquet::MakeTypedEncoder<DType>(to_arrow(e), false, descr.get());
    std::unique_ptr<T[]> tmp(new T[values.size() + 1]);
    std::copy(values.begin(), values.end(), tmp.get());
    enc->Put(tmp.get(), values.size());
    auto buf = enc->FlushValues();
    return std::string(reinterpret_cast<const char*>(buf->data()), buf->size());
}

template <typename T>
void test_fixed_width_encodings() {
    std::mt19937_64 rng(5 + sizeof(T));
    constexpr auto type = pq::fixed_width_values<T>::type;
    for (const auto& values : value_sets<T>(rng)) {
        pq::fixed_width_values<T> buffer;
        for (T v : values) {
            buffer.append(v);
        }
        BOOST_REQUIRE_EQUAL(buffer.size(), values.size());
        for (auto e : pq::writer_encodings(type)) {
            BOOST_TEST_CONTEXT("type " << int(type) << ", encoding " << int(e) << ", " << values.size() << " values") {
                bytes_ostream out;
                buffer.encode(e, out);
                const auto data = to_string(out);
                const auto estimate = buffer.encoded_size(e);
                if (e == pq::encoding::rle) {
                    BOOST_REQUIRE_LE(estimate, data.size() * 2 + 16);
                    BOOST_REQUIRE_LE(data.size(), estimate * 2 + 16);
                } else {
                    BOOST_REQUIRE_EQUAL(estimate, data.size());
                }
                for (size_t p : {size_t(1), size_t(7), piece}) {
                    BOOST_REQUIRE(same_values(decode_values<T>(e, data, values.size(), p), values));
                }
                BOOST_REQUIRE(same_values(arrow_decode<T>(e, data, values.size()), values));
                const auto from_arrow = arrow_encode<T>(e, values);
                BOOST_REQUIRE(same_values(decode_values<T>(e, from_arrow, values.size(), 3), values));
            }
        }
        buffer.clear();
        BOOST_REQUIRE_EQUAL(buffer.size(), 0);
        BOOST_REQUIRE_EQUAL(buffer.encoded_size(pq::encoding::plain), 0);
    }
}

} // anonymous namespace

SEASTAR_THREAD_TEST_CASE(test_boolean_encodings) {
    test_fixed_width_encodings<bool>();
}

SEASTAR_THREAD_TEST_CASE(test_int32_encodings) {
    test_fixed_width_encodings<int32_t>();
}

SEASTAR_THREAD_TEST_CASE(test_int64_encodings) {
    test_fixed_width_encodings<int64_t>();
}

SEASTAR_THREAD_TEST_CASE(test_float_encodings) {
    test_fixed_width_encodings<float>();
}

SEASTAR_THREAD_TEST_CASE(test_double_encodings) {
    test_fixed_width_encodings<double>();
}

SEASTAR_THREAD_TEST_CASE(test_delta_hand_written) {
    // Example 1 of Encodings.md with our block layout: 1, 2, 3, 4, 5.
    pq::fixed_width_values<int32_t> b;
    for (int32_t v : {1, 2, 3, 4, 5}) {
        b.append(v);
    }
    bytes_ostream out;
    b.encode(pq::encoding::delta_binary_packed, out);
    // Header: block size 128, 4 miniblocks, 5 values, first value 1.
    // Block: min delta 1, four bit widths of 0, no miniblock data.
    BOOST_REQUIRE_EQUAL(to_string(out), std::string("\x80\x01\x04\x05\x02\x02\x00\x00\x00\x00", 10));
}

SEASTAR_THREAD_TEST_CASE(test_delta_malformed) {
    auto decode = [] (const std::string& s, size_t count) {
        decode_values<int64_t>(pq::encoding::delta_binary_packed, s, count, piece);
    };
    // A block size that is not a multiple of 128.
    BOOST_REQUIRE_THROW(decode(std::string("\x40\x01\x01\x00", 4), 1), malformed_sstable_exception);
    // A miniblock of 16 values.
    BOOST_REQUIRE_THROW(decode(std::string("\x80\x01\x08\x01\x00", 5), 1), malformed_sstable_exception);
    // A huge block size, which must not allocate.
    BOOST_REQUIRE_THROW(decode(std::string("\x80\x80\x80\x80\x08\x01\x01\x00", 8), 1), malformed_sstable_exception);
    // A value count that differs from the page.
    BOOST_REQUIRE_THROW(decode(std::string("\x80\x01\x04\x02\x00", 5), 1), malformed_sstable_exception);
    // A bit width above 64.
    BOOST_REQUIRE_THROW(decode(std::string("\x80\x01\x04\x02\x00\x00\x41\x00\x00\x00", 10) + std::string(1000, '\0'), 2),
            malformed_sstable_exception);
    // A bit width above 32 for int32.
    BOOST_REQUIRE_THROW(decode_values<int32_t>(pq::encoding::delta_binary_packed,
            std::string("\x80\x01\x04\x02\x00\x00\x21\x00\x00\x00", 10) + std::string(1000, '\0'), 2, piece), malformed_sstable_exception);
    // Unused miniblocks may have any bit width.
    BOOST_REQUIRE((decode_values<int32_t>(pq::encoding::delta_binary_packed,
            std::string("\x80\x01\x04\x02\x00\x02\x00\xff\xff\xff", 10), 2, piece) == std::vector<int32_t>{0, 1}));

    std::mt19937_64 rng(6);
    for (auto e : {pq::encoding::plain, pq::encoding::delta_binary_packed}) {
        pq::fixed_width_values<int64_t> b;
        for (int i = 0; i < 200; ++i) {
            b.append(rng() % 1000);
        }
        bytes_ostream out;
        b.encode(e, out);
        for_each_damage(to_string(out), [&] (const std::string& s) {
            decode_values<int64_t>(e, s, 200, 5);
        });
    }
    for (auto e : {pq::encoding::plain, pq::encoding::rle}) {
        pq::fixed_width_values<bool> b;
        for (int i = 0; i < 300; ++i) {
            b.append(i % 50 < 30 || rng() % 2);
        }
        bytes_ostream out;
        b.encode(e, out);
        for_each_damage(to_string(out), [&] (const std::string& s) {
            decode_values<bool>(e, s, 300, 5);
        });
    }
}

// Byte arrays

namespace {

std::vector<std::string> byte_array_set(std::mt19937_64& rng, size_t count, size_t max_length, std::optional<uint32_t> fixed) {
    std::vector<std::string> v;
    for (size_t i = 0; i < count; ++i) {
        const size_t len = fixed ? *fixed : (rng() % 4 ? rng() % (max_length + 1) : rng() % 4);
        std::string s(len, '\0');
        const char base = 'a' + rng() % 4;
        for (auto& c : s) {
            c = rng() % 8 ? base : char(rng());
        }
        v.push_back(std::move(s));
    }
    return v;
}

std::vector<std::string> decode_byte_arrays(pq::physical_type type, pq::encoding e, const std::string& data, size_t count,
        std::optional<uint32_t> fixed, size_t piece_size, const pq::dictionary<pq::byte_array_view>* dict = nullptr) {
    auto buf = to_buffer(data, piece_size);
    auto d = pq::make_value_decoder<pq::byte_array_view>(type, e, pq::byte_array_view(buf), count, dict, fixed.value_or(0));
    std::vector<pq::byte_array_view> views(count);
    size_t done = 0;
    for (size_t batch = 1; done < count; batch *= 2) {
        const size_t n = std::min(batch, count - done);
        d->decode(std::span(views).subspan(done, n));
        done += n;
    }
    std::vector<std::string> out;
    for (auto& v : views) {
        out.push_back(to_string(v));
    }
    return out;
}

std::vector<std::string> arrow_decode_byte_arrays(pq::encoding e, const std::string& data, size_t count, std::optional<uint32_t> fixed) {
    std::vector<std::string> out;
    if (fixed) {
        auto descr = make_descriptor(::parquet::Type::FIXED_LEN_BYTE_ARRAY, *fixed);
        auto d = ::parquet::MakeTypedDecoder<::parquet::FLBAType>(to_arrow(e), descr.get());
        d->SetData(count, bytes_of(data), data.size());
        std::vector<::parquet::FixedLenByteArray> v(count + 1);
        BOOST_REQUIRE_EQUAL(d->Decode(v.data(), count), int(count));
        for (size_t i = 0; i < count; ++i) {
            out.emplace_back(reinterpret_cast<const char*>(v[i].ptr), *fixed);
        }
    } else {
        auto descr = make_descriptor(::parquet::Type::BYTE_ARRAY);
        auto d = ::parquet::MakeTypedDecoder<::parquet::ByteArrayType>(to_arrow(e), descr.get());
        d->SetData(count, bytes_of(data), data.size());
        std::vector<::parquet::ByteArray> v(count + 1);
        BOOST_REQUIRE_EQUAL(d->Decode(v.data(), count), int(count));
        for (size_t i = 0; i < count; ++i) {
            out.emplace_back(reinterpret_cast<const char*>(v[i].ptr), v[i].len);
        }
    }
    return out;
}

std::string arrow_encode_byte_arrays(pq::encoding e, const std::vector<std::string>& values, std::optional<uint32_t> fixed) {
    std::shared_ptr<::parquet::Buffer> buf;
    if (fixed) {
        auto descr = make_descriptor(::parquet::Type::FIXED_LEN_BYTE_ARRAY, *fixed);
        auto enc = ::parquet::MakeTypedEncoder<::parquet::FLBAType>(to_arrow(e), false, descr.get());
        std::vector<::parquet::FixedLenByteArray> v;
        for (auto& s : values) {
            v.emplace_back(reinterpret_cast<const uint8_t*>(s.data()));
        }
        enc->Put(v.data(), v.size());
        buf = enc->FlushValues();
    } else {
        auto descr = make_descriptor(::parquet::Type::BYTE_ARRAY);
        auto enc = ::parquet::MakeTypedEncoder<::parquet::ByteArrayType>(to_arrow(e), false, descr.get());
        std::vector<::parquet::ByteArray> v;
        for (auto& s : values) {
            v.emplace_back(s.size(), reinterpret_cast<const uint8_t*>(s.data()));
        }
        enc->Put(v.data(), v.size());
        buf = enc->FlushValues();
    }
    return std::string(reinterpret_cast<const char*>(buf->data()), buf->size());
}

} // anonymous namespace

SEASTAR_THREAD_TEST_CASE(test_byte_array_encodings) {
    std::mt19937_64 rng(7);
    struct shape {
        size_t count;
        size_t max_length;
        std::optional<uint32_t> fixed;
    };
    for (auto [count, max_length, fixed] : {
            shape{0, 10, {}}, shape{1, 0, {}}, shape{1, 10, {}}, shape{1000, 30, {}}, shape{300, 3, {}},
            // Values larger than a contiguous piece.
            shape{5, 300 * 1024, {}},
            shape{0, 0, 16}, shape{1, 0, 1}, shape{1000, 0, 16}, shape{3, 0, 200 * 1024}}) {
        const auto values = byte_array_set(rng, count, max_length, fixed);
        pq::byte_array_values buffer(fixed);
        for (size_t i = 0; i < values.size(); ++i) {
            if (i % 2) {
                buffer.append(managed_bytes_view(bytes_view(reinterpret_cast<const int8_t*>(values[i].data()), values[i].size())));
            } else {
                buffer.append(bytes_view(reinterpret_cast<const int8_t*>(values[i].data()), values[i].size()));
            }
        }
        const auto type = buffer.type();
        for (auto e : pq::writer_encodings(type)) {
            BOOST_TEST_CONTEXT("encoding " << int(e) << ", " << count << " values, fixed length " << fixed.value_or(0)) {
                bytes_ostream out;
                buffer.encode(e, out);
                const auto data = to_string(out);
                BOOST_REQUIRE_EQUAL(buffer.encoded_size(e), data.size());
                for (size_t p : {size_t(1), size_t(9), piece}) {
                    if (p < 100 && data.size() > 100'000) {
                        continue;
                    }
                    BOOST_REQUIRE(decode_byte_arrays(type, e, data, count, fixed, p) == values);
                }
                BOOST_REQUIRE(arrow_decode_byte_arrays(e, data, count, fixed) == values);
                const auto from_arrow = arrow_encode_byte_arrays(e, values, fixed);
                BOOST_REQUIRE(decode_byte_arrays(type, e, from_arrow, count, fixed, 11) == values);
            }
        }
    }
}

SEASTAR_THREAD_TEST_CASE(test_byte_array_malformed) {
    // A length beyond the data.
    BOOST_REQUIRE_THROW(decode_byte_arrays(pq::physical_type::byte_array, pq::encoding::plain, std::string("\x05\x00\x00\x00" "abcd", 8), 1, {}, piece),
            malformed_sstable_exception);
    // A huge length.
    BOOST_REQUIRE_THROW(decode_byte_arrays(pq::physical_type::byte_array, pq::encoding::plain, std::string("\xff\xff\xff\xff" "abcd", 8), 1, {}, piece),
            malformed_sstable_exception);
    // A negative length in DELTA_LENGTH_BYTE_ARRAY.
    {
        bytes_ostream out;
        pq::delta_binary_packed_encoder<int32_t> e(out, 1);
        e.put(-1);
        e.finish();
        BOOST_REQUIRE_THROW(decode_byte_arrays(pq::physical_type::byte_array, pq::encoding::delta_length_byte_array, to_string(out), 1, {}, piece),
                malformed_sstable_exception);
    }
    // An invalid fixed length.
    BOOST_REQUIRE_THROW(decode_byte_arrays(pq::physical_type::fixed_len_byte_array, pq::encoding::plain, "abcd", 1, 0, piece),
            malformed_sstable_exception);
    // An encoding that does not apply to the type.
    BOOST_REQUIRE_THROW(decode_byte_arrays(pq::physical_type::fixed_len_byte_array, pq::encoding::delta_length_byte_array, "abcd", 1, 4, piece),
            malformed_sstable_exception);
    BOOST_REQUIRE_THROW(decode_values<int32_t>(pq::encoding::delta_length_byte_array, "abcd", 1, piece), malformed_sstable_exception);
    BOOST_REQUIRE_THROW(decode_values<double>(pq::encoding::delta_binary_packed, "abcd", 1, piece), malformed_sstable_exception);
    // A dictionary-encoded page without a dictionary.
    BOOST_REQUIRE_THROW(decode_values<int32_t>(pq::encoding::rle_dictionary, std::string("\x01\x02\x00", 3), 1, piece), malformed_sstable_exception);

    std::mt19937_64 rng(8);
    const auto values = byte_array_set(rng, 40, 10, {});
    pq::byte_array_values buffer;
    for (auto& v : values) {
        buffer.append(bytes_view(reinterpret_cast<const int8_t*>(v.data()), v.size()));
    }
    for (auto e : pq::writer_encodings(pq::physical_type::byte_array)) {
        bytes_ostream out;
        buffer.encode(e, out);
        for_each_damage(to_string(out), [&] (const std::string& s) {
            decode_byte_arrays(pq::physical_type::byte_array, e, s, values.size(), {}, 4);
        });
    }
}

// Dictionaries

namespace {

template <typename T>
void test_dictionary() {
    using DType = typename arrow_type<T>::type;
    constexpr auto type = pq::fixed_width_values<T>::type;
    std::mt19937_64 rng(9 + sizeof(T));
    for (const auto& values : value_sets<T>(rng)) {
        BOOST_TEST_CONTEXT("type " << int(type) << ", " << values.size() << " values") {
            pq::dictionary_builder<T> builder(1 << 20);
            pq::dictionary_indices indices;
            for (T v : values) {
                auto i = builder.insert(v);
                BOOST_REQUIRE(i);
                indices.append(*i);
            }
            bytes_ostream page;
            builder.write_page(page);
            BOOST_REQUIRE_EQUAL(page.size(), builder.page_size());
            bytes_ostream out;
            indices.encode(builder.size(), out);
            const auto data = to_string(out);
            BOOST_REQUIRE_LE(indices.encoded_size(builder.size()), data.size() * 2 + 16);
            BOOST_REQUIRE_LE(data.size(), indices.encoded_size(builder.size()) * 2 + 16);

            const auto page_data = to_string(page);
            auto dict = pq::dictionary<T>::decode(to_buffer(page_data, 5), builder.size(), type).get();
            BOOST_REQUIRE_EQUAL(dict.size(), builder.size());
            // The dictionary stays valid when it is moved.
            auto moved = std::move(dict);
            for (auto e : {pq::encoding::rle_dictionary, pq::encoding::plain_dictionary}) {
                BOOST_REQUIRE(same_values(decode_values<T>(e, data, values.size(), 3, &moved), values));
            }
            // A copy of the compact form, as the metadata cache makes it, is the same dictionary.
            const auto compact = to_string(moved.compact());
            BOOST_REQUIRE_EQUAL(compact.size(), pq::dictionary_compact_size(type, page_data.size(), builder.size()));
            const auto copy = pq::dictionary<T>::from_compact(fragmented_temporary_buffer(compact.data(), compact.size()), type);
            BOOST_REQUIRE_EQUAL(copy.size(), builder.size());
            BOOST_REQUIRE(same_values(decode_values<T>(pq::encoding::rle_dictionary, data, values.size(), 3, &copy), values));

            // Arrow reads our dictionary and indices.
            auto descr = make_descriptor<T>();
            {
                auto dict_decoder = ::parquet::MakeTypedDecoder<DType>(::parquet::Encoding::PLAIN, descr.get());
                dict_decoder->SetData(builder.size(), bytes_of(page_data), page_data.size());
                auto d = ::parquet::MakeDictDecoder<DType>(descr.get());
                d->SetDict(dict_decoder.get());
                d->SetData(values.size(), bytes_of(data), data.size());
                std::unique_ptr<T[]> out(new T[values.size() + 1]);
                BOOST_REQUIRE_EQUAL(d->Decode(out.get(), values.size()), int(values.size()));
                BOOST_REQUIRE(same_values(std::vector<T>(out.get(), out.get() + values.size()), values));
            }
            // We read Arrow's dictionary and indices.
            {
                auto enc = ::parquet::MakeTypedEncoder<DType>(::parquet::Encoding::PLAIN, true, descr.get());
                auto* dict_enc = dynamic_cast<::parquet::DictEncoder<DType>*>(enc.get());
                BOOST_REQUIRE(dict_enc);
                std::unique_ptr<T[]> tmp(new T[values.size() + 1]);
                std::copy(values.begin(), values.end(), tmp.get());
                dict_enc->Put(tmp.get(), values.size());
                std::string arrow_page(dict_enc->dict_encoded_size(), '\0');
                dict_enc->WriteDict(reinterpret_cast<uint8_t*>(arrow_page.data()));
                const uint32_t entries = dict_enc->num_entries();
                auto idx = dict_enc->FlushValues();
                const std::string arrow_indices(reinterpret_cast<const char*>(idx->data()), idx->size());
                auto arrow_dict = pq::dictionary<T>::decode(to_buffer(arrow_page, 7), entries, type).get();
                const auto decoded = decode_values<T>(pq::encoding::rle_dictionary, arrow_indices, values.size(), 3, &arrow_dict);
                // Arrow's dictionary merges all NaNs, and zero with negative
                // zero, so the values are compared by value here.
                BOOST_REQUIRE(std::ranges::equal(decoded, values, [] (T x, T y) {
                    if constexpr (std::is_floating_point_v<T>) {
                        return x == y || (std::isnan(x) && std::isnan(y));
                    } else {
                        return x == y;
                    }
                }));
            }
        }
    }
}

} // anonymous namespace

SEASTAR_THREAD_TEST_CASE(test_dictionaries) {
    test_dictionary<int32_t>();
    test_dictionary<int64_t>();
    test_dictionary<float>();
    test_dictionary<double>();
}

SEASTAR_THREAD_TEST_CASE(test_byte_array_dictionary) {
    std::mt19937_64 rng(10);
    for (std::optional<uint32_t> fixed : {std::optional<uint32_t>(), std::optional<uint32_t>(16)}) {
        // Few distinct values, some of them larger than a contiguous piece.
        // Each starts with its index, so they differ.
        auto distinct = byte_array_set(rng, 20, 50, fixed);
        for (size_t i = 0; i < distinct.size(); ++i) {
            if (fixed) {
                distinct[i][0] = char(i);
            } else {
                distinct[i] = std::to_string(i) + "." + distinct[i];
            }
        }
        if (!fixed) {
            distinct.push_back(std::string(200 * 1024, 'x'));
            distinct.push_back("");
        }
        std::vector<std::string> values;
        for (int i = 0; i < 2000; ++i) {
            values.push_back(distinct[i % 37 % distinct.size()]);
        }
        pq::byte_array_dictionary_builder builder(1 << 20, fixed);
        pq::dictionary_indices indices;
        for (auto& v : values) {
            auto i = builder.insert(bytes_view(reinterpret_cast<const int8_t*>(v.data()), v.size()));
            BOOST_REQUIRE(i);
            indices.append(*i);
        }
        BOOST_REQUIRE_EQUAL(builder.size(), distinct.size());
        bytes_ostream page;
        builder.write_page(page);
        BOOST_REQUIRE_EQUAL(page.size(), builder.page_size());
        bytes_ostream out;
        indices.encode(builder.size(), out);
        const auto data = to_string(out);
        const auto page_data = to_string(page);
        const auto type = fixed ? pq::physical_type::fixed_len_byte_array : pq::physical_type::byte_array;
        auto dict = pq::dictionary<pq::byte_array_view>::decode(to_buffer(page_data, 1000), builder.size(), type, fixed.value_or(0)).get();
        auto moved = std::move(dict);
        BOOST_REQUIRE(decode_byte_arrays(type, pq::encoding::rle_dictionary, data, values.size(), fixed, 3, &moved) == values);
        // A copy of the compact form, as the metadata cache makes it, is
        // the same dictionary. The large value makes it cross fragments.
        const auto compact = to_string(moved.compact());
        BOOST_REQUIRE_EQUAL(compact.size(), pq::dictionary_compact_size(type, page_data.size(), builder.size()));
        BOOST_REQUIRE(fixed || compact.size() > piece);
        const auto copy = pq::dictionary<pq::byte_array_view>::from_compact(fragmented_temporary_buffer(compact.data(), compact.size()),
                type, fixed.value_or(0));
        BOOST_REQUIRE_EQUAL(copy.size(), builder.size());
        BOOST_REQUIRE(decode_byte_arrays(type, pq::encoding::rle_dictionary, data, values.size(), fixed, 3, &copy) == values);
        for (size_t i = 0; i < copy.size(); ++i) {
            BOOST_REQUIRE_EQUAL(to_string(copy[i]), to_string(moved[i]));
        }

        // Arrow reads our dictionary and indices.
        if (!fixed) {
            auto descr = make_descriptor(::parquet::Type::BYTE_ARRAY);
            auto dict_decoder = ::parquet::MakeTypedDecoder<::parquet::ByteArrayType>(::parquet::Encoding::PLAIN, descr.get());
            dict_decoder->SetData(builder.size(), bytes_of(page_data), page_data.size());
            auto d = ::parquet::MakeDictDecoder<::parquet::ByteArrayType>(descr.get());
            d->SetDict(dict_decoder.get());
            d->SetData(values.size(), bytes_of(data), data.size());
            std::vector<::parquet::ByteArray> v(values.size());
            BOOST_REQUIRE_EQUAL(d->Decode(v.data(), values.size()), int(values.size()));
            for (size_t i = 0; i < values.size(); ++i) {
                BOOST_REQUIRE_EQUAL(std::string(reinterpret_cast<const char*>(v[i].ptr), v[i].len), values[i]);
            }
        }
    }
}

SEASTAR_THREAD_TEST_CASE(test_dictionary_fallback) {
    // A dictionary page limit of 40 bytes holds 10 int32 values.
    pq::dictionary_builder<int32_t> builder(40);
    for (int32_t v = 0; v < 10; ++v) {
        BOOST_REQUIRE(builder.insert(v * 7) == uint32_t(v));
    }
    BOOST_REQUIRE(!builder.insert(1000));
    BOOST_REQUIRE_EQUAL(builder.size(), 10);
    // Existing values are still found.
    BOOST_REQUIRE(builder.insert(21) == 3);
    builder.clear();
    BOOST_REQUIRE(builder.insert(1000) == 0);

    pq::byte_array_dictionary_builder b(13);
    auto bv = [] (const char* s) { return bytes_view(reinterpret_cast<const int8_t*>(s), std::strlen(s)); };
    BOOST_REQUIRE(b.insert(bv("abc")) == 0);
    BOOST_REQUIRE(!b.insert(bv("defg")));
    BOOST_REQUIRE(b.insert(bv("abc")) == 0);
    BOOST_REQUIRE(b.insert(bv("de")) == 1);
    BOOST_REQUIRE_EQUAL(b.page_size(), 3 + 4 + 2 + 4);
    BOOST_REQUIRE(!b.insert(bv("")));

    // Many distinct values exercise the growth of the hash index.
    pq::dictionary_builder<int64_t> big(1 << 20);
    for (int64_t v = 0; v < 50000; ++v) {
        BOOST_REQUIRE(big.insert(v * 1'000'003) == uint32_t(v));
    }
    for (int64_t v = 0; v < 50000; ++v) {
        BOOST_REQUIRE(big.insert(v * 1'000'003) == uint32_t(v));
    }
}

SEASTAR_THREAD_TEST_CASE(test_dictionary_malformed) {
    // More entries than the page can hold. This must fail before allocating.
    BOOST_REQUIRE_THROW(pq::dictionary<int64_t>::decode(to_buffer("12345678", piece), 1'000'000'000, pq::physical_type::int64).get(),
            malformed_sstable_exception);
    BOOST_REQUIRE_THROW(pq::dictionary<pq::byte_array_view>::decode(to_buffer("1234", piece), 4'000'000'000, pq::physical_type::byte_array).get(),
            malformed_sstable_exception);
    // Bytes left after the entries.
    BOOST_REQUIRE_THROW(pq::dictionary<int32_t>::decode(to_buffer("123456", piece), 1, pq::physical_type::int32).get(),
            malformed_sstable_exception);
    BOOST_REQUIRE_THROW(pq::dictionary<pq::byte_array_view>::decode(to_buffer(std::string("\x01\x00\x00\x00" "ab", 6), piece), 1,
            pq::physical_type::byte_array).get(), malformed_sstable_exception);

    auto dict = pq::dictionary<int32_t>::decode(to_buffer(std::string("\x01\x00\x00\x00\x02\x00\x00\x00", 8), piece), 2, pq::physical_type::int32).get();
    // An index out of range: bit width 2, an RLE run of 8 values of 3.
    BOOST_REQUIRE_THROW(decode_values<int32_t>(pq::encoding::rle_dictionary, std::string("\x02\x10\x03", 3), 8, piece, &dict),
            malformed_sstable_exception);
    // A bit width above 32.
    BOOST_REQUIRE_THROW(decode_values<int32_t>(pq::encoding::rle_dictionary, std::string("\x21\x10\x00", 3), 8, piece, &dict),
            malformed_sstable_exception);
    // Damaged indices.
    pq::dictionary_indices indices;
    for (int i = 0; i < 100; ++i) {
        indices.append(i % 5 % 2);
    }
    bytes_ostream out;
    indices.encode(2, out);
    for_each_damage(to_string(out), [&] (const std::string& s) {
        decode_values<int32_t>(pq::encoding::rle_dictionary, s, 100, 3, &dict);
    });
}

// Page compression

namespace {

constexpr pq::compression_codec all_codecs[] = {
    pq::compression_codec::uncompressed,
    pq::compression_codec::snappy,
    pq::compression_codec::gzip,
    pq::compression_codec::lz4_raw,
    pq::compression_codec::zstd,
};

std::optional<::arrow::Compression::type> arrow_codec(pq::compression_codec c) {
    switch (c) {
    case pq::compression_codec::snappy: return ::arrow::Compression::SNAPPY;
    case pq::compression_codec::gzip: return ::arrow::Compression::GZIP;
    // Arrow's LZ4 is the raw block format, which Parquet calls LZ4_RAW.
    case pq::compression_codec::lz4_raw: return ::arrow::Compression::LZ4;
    case pq::compression_codec::zstd: return ::arrow::Compression::ZSTD;
    default: return std::nullopt;
    }
}

std::vector<std::string> compression_inputs(std::mt19937_64& rng) {
    std::vector<std::string> inputs;
    inputs.push_back("");
    inputs.push_back("x");
    inputs.push_back(std::string(1000, 'a'));
    // Compressible pages of one contiguous piece and of several, and
    // around the size of a synchronous call.
    for (size_t size : {size_t(100), size_t(64 * 1024), pq::sync_compress_threshold, pq::sync_compress_threshold + 1,
            pq::sync_decompress_threshold - 1, pq::sync_decompress_threshold, pq::sync_decompress_threshold + 1, size_t(1024 * 1024)}) {
        std::string s(size, '\0');
        for (size_t i = 0; i < size; ++i) {
            s[i] = "abcdefgh"[(i / 7 + rng() % 3) % 8];
        }
        inputs.push_back(std::move(s));
    }
    auto random = [&] (size_t size) {
        std::string s(size, '\0');
        for (auto& c : s) {
            c = char(rng());
        }
        return s;
    };
    // Incompressible pages. The compressed form of the first is larger
    // than a synchronous decompression, and its uncompressed form is not.
    inputs.push_back(random(pq::sync_decompress_threshold - 10));
    inputs.push_back(random(300 * 1024));
    // A long incompressible run between compressible parts, so that LZ4
    // carries literals across many chunks into a match.
    inputs.push_back(std::string(1000, 'a') + random(200 * 1024) + std::string(100 * 1024, 'b') + random(10));
    // Zeros, which compress very well.
    inputs.push_back(std::string(2 * 1024 * 1024, '\0'));
    return inputs;
}

// Runs `fn` and checks that it makes no contiguous allocation above
// 128 KiB, apart from the compression codec contexts, which are allocated
// once.
template <typename Fn>
auto without_large_allocations(Fn&& fn) {
    const auto before = seastar::memory::stats().large_allocations();
    const seastar::memory::scoped_large_allocation_warning_threshold guard((128 << 10) + 1);
    auto r = fn();
    BOOST_REQUIRE_EQUAL(seastar::memory::stats().large_allocations(), before);
    return r;
}

std::string compress(pq::page_compressor& c, const std::string& input) {
    // Write the input in small writes, so that the bytes_ostream has
    // contiguous pieces of many sizes.
    bytes_ostream in;
    for (size_t pos = 0; pos < input.size(); pos += 1000) {
        in.write(input.data() + pos, std::min<size_t>(1000, input.size() - pos));
    }
    bytes_ostream out;
    out.write("prefix", 6);
    without_large_allocations([&] {
        auto f = c.compress(3, in, out);
        // A small call runs synchronously.
        if (input.size() <= pq::sync_compress_threshold) {
            BOOST_REQUIRE(f.available());
        }
        f.get();
        return 0;
    });
    auto s = to_string(out);
    BOOST_REQUIRE_EQUAL(s.substr(0, 6), "prefix");
    return s.substr(6);
}

std::string decompress(pq::page_compressor& c, const std::string& input, size_t size, size_t piece_size) {
    auto buf = to_buffer(input, piece_size);
    auto out = without_large_allocations([&] {
        auto f = c.decompress(3, pq::byte_array_view(buf), size);
        if (input.size() <= pq::sync_decompress_threshold && size <= pq::sync_decompress_threshold) {
            BOOST_REQUIRE(f.available());
        }
        return f.get();
    });
    BOOST_REQUIRE_EQUAL(out.size_bytes(), size);
    for (const auto& p : out) {
        BOOST_REQUIRE_LE(p.size(), piece);
        BOOST_REQUIRE_GT(p.size(), 0);
    }
    return to_string(out);
}

// Makes need_preempt() true while it lives, so that a large call yields
// after every step.
class always_preempt {
    seastar::internal::preemption_monitor _monitor;
    const seastar::internal::preemption_monitor* _old;
public:
    always_preempt() : _old(seastar::internal::get_need_preempt_var()) {
        _monitor.head.store(1);
        _monitor.tail.store(0);
        seastar::internal::set_need_preempt_var(&_monitor);
    }
    ~always_preempt() {
        seastar::internal::set_need_preempt_var(_old);
    }
};

// Waits for `f` while preemption is forced, and returns the number of
// times that the waiting thread ran before `f` resolved. Each time, the
// call made about one step.
template <typename T>
size_t count_yields(future<T>& f) {
    size_t n = 0;
    while (!f.available()) {
        ++n;
        seastar::thread::yield();
    }
    return n;
}

} // anonymous namespace

SEASTAR_THREAD_TEST_CASE(test_compression_round_trip) {
    std::mt19937_64 rng(11);
    const auto inputs = compression_inputs(rng);
    for (auto codec : all_codecs) {
        BOOST_REQUIRE(pq::is_supported(codec));
        // Level 9 makes zstd use a slow strategy, which ends a block at each step.
        for (std::optional<int> level : {std::optional<int>(), std::optional<int>(1), std::optional<int>(9)}) {
            auto c = pq::make_page_compressor(codec, level);
            BOOST_REQUIRE(c->codec() == codec);
            for (const auto& input : inputs) {
                BOOST_TEST_CONTEXT("codec " << int(codec) << ", " << input.size() << " bytes") {
                    const auto compressed = compress(*c, input);
                    if (input.size() >= 1000 && input.find_first_not_of(input[0]) == std::string::npos && codec != pq::compression_codec::uncompressed) {
                        BOOST_REQUIRE_LT(compressed.size(), input.size() / 4);
                    }
                    for (size_t p : {size_t(1000), piece}) {
                        BOOST_REQUIRE(decompress(*c, compressed, input.size(), p) == input);
                    }
                    if (input.size() < 10'000 || (input.size() < 400'000 && input.size() > pq::sync_decompress_threshold)) {
                        BOOST_REQUIRE(decompress(*c, compressed, input.size(), 1) == input);
                    }
                }
            }
        }
    }
    BOOST_REQUIRE(!pq::is_supported(pq::compression_codec::lzo));
    BOOST_REQUIRE_THROW(pq::make_page_compressor(pq::compression_codec::brotli), malformed_sstable_exception);
    BOOST_REQUIRE_THROW(pq::make_page_compressor(pq::compression_codec::lz4), malformed_sstable_exception);
}

// Large calls yield between steps of bounded size. Small calls do not
// yield and take no lock, even while a large call holds a context. A large
// call with zstd or gzip waits for a large call of its own scheduling
// group, and not for one of another group.
SEASTAR_THREAD_TEST_CASE(test_large_calls_yield) {
    std::mt19937_64 rng(12);
    std::string input(8 << 20, 0);
    for (size_t i = 0; i < input.size(); i += 8) {
        // Compressible, but not trivially: small random numbers.
        const uint64_t v = rng() % 1000;
        std::memcpy(input.data() + i, &v, 8);
    }
    const std::string medium = input.substr(0, 512 * 1024);
    const std::string small = input.substr(0, 10000);
    auto other_group = create_scheduling_group("parquet_test", 1000).get();
    auto destroy_group = defer([&] () noexcept { destroy_scheduling_group(other_group).get(); });
    // A large call yields at least once per this many bytes.
    constexpr size_t max_step = 256 * 1024;
    for (auto codec : all_codecs) {
        BOOST_TEST_CONTEXT("codec " << int(codec)) {
            auto c = pq::make_page_compressor(codec);
            const auto compressed = compress(*c, input);
            const auto compressed_medium = compress(*c, medium);
            const auto compressed_small = compress(*c, small);
            {
                const auto in = to_ostream(input);
                bytes_ostream out;
                always_preempt p;
                auto f = c->compress(3, in, out);
                BOOST_REQUIRE(!f.available());
                const size_t yields = count_yields(f);
                f.get();
                testlog.info("codec {}: compression yielded {} times", int(codec), yields);
                BOOST_REQUIRE_GE(yields, input.size() / max_step);
                BOOST_REQUIRE(to_string(out) == compressed);
            }
            auto buf = to_buffer(compressed, piece);
            auto medium_buf = to_buffer(compressed_medium, piece);
            auto small_buf = to_buffer(compressed_small, piece);
            always_preempt p;
            {
                auto f = c->decompress(3, pq::byte_array_view(buf), input.size());
                BOOST_REQUIRE(!f.available());
                const size_t yields = count_yields(f);
                testlog.info("codec {}: decompression yielded {} times", int(codec), yields);
                BOOST_REQUIRE_GE(yields, input.size() / max_step);
                BOOST_REQUIRE(to_string(f.get()) == input);
            }
            auto large = c->decompress(3, pq::byte_array_view(buf), input.size());
            BOOST_REQUIRE(!large.available());
            // A small call while the large one holds a context.
            auto f = c->decompress(3, pq::byte_array_view(small_buf), small.size());
            BOOST_REQUIRE(f.available());
            BOOST_REQUIRE(to_string(f.get()) == small);
            // A large call of another group ends first.
            auto other = with_scheduling_group(other_group, [&] {
                return c->decompress(3, pq::byte_array_view(medium_buf), medium.size());
            });
            count_yields(other);
            BOOST_REQUIRE(!large.available());
            BOOST_REQUIRE(to_string(other.get()) == medium);
            // A large call of the same group. With zstd and gzip, it waits for
            // the first call's context.
            auto same = c->decompress(3, pq::byte_array_view(medium_buf), medium.size());
            count_yields(same);
            BOOST_REQUIRE_EQUAL(large.available(), codec == pq::compression_codec::zstd || codec == pq::compression_codec::gzip);
            BOOST_REQUIRE(to_string(same.get()) == medium);
            count_yields(large);
            BOOST_REQUIRE(to_string(large.get()) == input);
            BOOST_REQUIRE_THROW(c->decompress(3, pq::byte_array_view(buf), input.size() - 1).get(), malformed_sstable_exception);
        }
    }
}

namespace {

uint64_t zstd_window(const std::string& frame) {
    ZSTD_frameHeader h;
    BOOST_REQUIRE_EQUAL(ZSTD_getFrameHeader(&h, frame.data(), frame.size()), 0);
    return h.windowSize;
}

} // anonymous namespace

SEASTAR_THREAD_TEST_CASE(test_compression_arrow) {
    std::mt19937_64 rng(12);
    const auto inputs = compression_inputs(rng);
    for (auto codec : all_codecs) {
        auto ac = arrow_codec(codec);
        if (!ac) {
            continue;
        }
        if (!::arrow::util::Codec::IsAvailable(*ac)) {
            BOOST_TEST_MESSAGE("Arrow lacks codec " << int(codec) << ", skipping");
            continue;
        }
        auto arrow = ::arrow::util::Codec::Create(*ac).ValueOrDie();
        auto c = pq::make_page_compressor(codec);
        for (const auto& input : inputs) {
            BOOST_TEST_CONTEXT("codec " << int(codec) << ", " << input.size() << " bytes") {
                // Arrow decompresses ours.
                const auto ours = compress(*c, input);
                std::string out(input.size(), '\0');
                auto n = arrow->Decompress(ours.size(), bytes_of(ours), out.size(), reinterpret_cast<uint8_t*>(out.data())).ValueOrDie();
                BOOST_REQUIRE_EQUAL(n, int64_t(input.size()));
                BOOST_REQUIRE(out == input);
                // We decompress Arrow's.
                std::string theirs(arrow->MaxCompressedLen(input.size(), bytes_of(input)), '\0');
                n = arrow->Compress(input.size(), bytes_of(input), theirs.size(), reinterpret_cast<uint8_t*>(theirs.data())).ValueOrDie();
                theirs.resize(n);
                if (codec == pq::compression_codec::zstd && zstd_window(theirs) > (1 << 17)) {
                    // Arrow sizes the window by the input. We read only the
                    // window of our own frames (format document, 3.8).
                    BOOST_REQUIRE_THROW(decompress(*c, theirs, input.size(), 4096), malformed_sstable_exception);
                    continue;
                }
                BOOST_REQUIRE(decompress(*c, theirs, input.size(), 4096) == input);
            }
        }
    }
}

SEASTAR_THREAD_TEST_CASE(test_compression_multiple_frames) {
    // Concatenated zstd frames and gzip members decode as one page.
    std::mt19937_64 rng(13);
    for (auto codec : {pq::compression_codec::zstd, pq::compression_codec::gzip}) {
        auto c = pq::make_page_compressor(codec);
        const std::string a(5000, 'a');
        const std::string b = "bbb" + std::string(200 * 1024, 'c');
        const auto both = compress(*c, a) + compress(*c, b);
        BOOST_REQUIRE(decompress(*c, both, a.size() + b.size(), 100) == a + b);
        // Garbage after the last frame.
        BOOST_REQUIRE_THROW(decompress(*c, both + "x", a.size() + b.size(), 100), malformed_sstable_exception);
    }
}

SEASTAR_THREAD_TEST_CASE(test_compression_malformed) {
    std::mt19937_64 rng(14);
    // A page of a synchronous call, and a page of an asynchronous one.
    for (size_t size : {size_t(20000), size_t(300 * 1024)})
    for (auto codec : all_codecs) {
        std::string input(size, '\0');
        for (size_t i = 0; i < input.size(); ++i) {
            input[i] = "abcd"[(i / 5 + rng() % 2) % 4];
        }
        BOOST_TEST_CONTEXT("codec " << int(codec) << ", " << size << " bytes") {
            auto c = pq::make_page_compressor(codec);
            const auto compressed = compress(*c, input);
            // A wrong uncompressed size, smaller or larger.
            BOOST_REQUIRE_THROW(decompress(*c, compressed, input.size() - 1, piece), malformed_sstable_exception);
            BOOST_REQUIRE_THROW(decompress(*c, compressed, input.size() + 1, piece), malformed_sstable_exception);
            // A huge declared size, which must not be allocated.
            BOOST_REQUIRE_THROW(decompress(*c, compressed, size_t(1) << 40, piece), malformed_sstable_exception);
            {
                // Nor does it cause a large allocation of bookkeeping.
                const auto before = seastar::memory::stats();
                const seastar::memory::scoped_large_allocation_warning_threshold guard((128 << 10) + 1);
                BOOST_REQUIRE_THROW(decompress(*c, compressed, (size_t(1) << 31) - 1, piece), malformed_sstable_exception);
                BOOST_REQUIRE_EQUAL(seastar::memory::stats().large_allocations(), before.large_allocations());
            }
            // Empty input.
            if (codec != pq::compression_codec::uncompressed) {
                BOOST_REQUIRE_THROW(decompress(*c, "", 0, piece), malformed_sstable_exception);
            }
            // Truncations and bit flips decode to the declared size or fail.
            // The flips cover the start of the stream, where the headers are.
            for (size_t n = 0; n < compressed.size(); n += 1 + n / 8) {
                try {
                    decompress(*c, compressed.substr(0, n), input.size(), 1000);
                    // Only an uncompressed page, or a stream truncated at a
                    // frame boundary, can decode.
                    BOOST_FAIL("a truncated page decoded");
                } catch (const malformed_sstable_exception&) {
                }
            }
            // For the large page, the flips cover the start, and then
            // random places.
            const size_t start_bits = size < pq::sync_decompress_threshold ? 2000 : 200;
            const size_t bits = size < pq::sync_decompress_threshold ? 2000 : 400;
            for (size_t i = 0; i < std::min<size_t>(compressed.size() * 8, bits); ++i) {
                const size_t bit = i < start_bits ? i : rng() % (compressed.size() * 8);
                auto damaged = compressed;
                damaged[bit / 8] ^= char(1 << (bit % 8));
                try {
                    decompress(*c, damaged, input.size(), 1000);
                } catch (const malformed_sstable_exception&) {
                }
            }
        }
    }
}

namespace {

// Decodes with the LZ4 or snappy library, or returns nothing if it fails.
std::optional<std::string> library_decompress(pq::compression_codec codec, const std::string& in, size_t size) {
    std::string out(size, '\0');
    if (codec == pq::compression_codec::lz4_raw) {
        const int n = LZ4_decompress_safe(in.data(), out.data(), in.size(), out.size());
        if (n < 0 || size_t(n) != size) {
            return std::nullopt;
        }
        return out;
    }
    size_t declared;
    if (!snappy::GetUncompressedLength(in.data(), in.size(), &declared) || declared != size
            || !snappy::RawUncompress(in.data(), in.size(), out.data())) {
        return std::nullopt;
    }
    return out;
}

std::optional<std::string> our_decompress(pq::page_compressor& c, const std::string& in, size_t size, size_t piece_size) {
    try {
        return decompress(c, in, size, piece_size);
    } catch (const malformed_sstable_exception&) {
        return std::nullopt;
    }
}

void write_lz4_length(std::string& out, size_t n) {
    for (; n >= 255; n -= 255) {
        out += char(255);
    }
    out += char(n);
}

// An LZ4 sequence: `literals`, then a match of `match` bytes from
// `offset` back, or no match if `match` is 0.
void lz4_sequence(std::string& out, const std::string& literals, size_t offset = 0, size_t match = 0) {
    const size_t m = match ? match - 4 : 0;
    out += char((std::min<size_t>(literals.size(), 15) << 4) | std::min<size_t>(m, 15));
    if (literals.size() >= 15) {
        write_lz4_length(out, literals.size() - 15);
    }
    out += literals;
    if (match) {
        out += char(offset & 255);
        out += char(offset >> 8);
        if (m >= 15) {
            write_lz4_length(out, m - 15);
        }
    }
}

} // anonymous namespace

// Our decoders of LZ4_RAW and SNAPPY, which decode pages above
// sync_decompress_threshold, agree with the libraries on valid and on damaged input.
SEASTAR_THREAD_TEST_CASE(test_compression_library_decoders) {
    std::mt19937_64 rng(15);
    auto random = [&] (size_t size) {
        std::string s(size, '\0');
        for (auto& c : s) {
            c = char(rng());
        }
        return s;
    };
    for (auto codec : {pq::compression_codec::lz4_raw, pq::compression_codec::snappy}) {
        BOOST_TEST_CONTEXT("codec " << int(codec)) {
            auto c = pq::make_page_compressor(codec);
            // Streams: (compressed, uncompressed).
            std::vector<std::pair<std::string, std::string>> streams;
            // The libraries' own output for whole pages.
            auto add_library = [&] (const std::string& input) {
                std::string out;
                if (codec == pq::compression_codec::lz4_raw) {
                    out.resize(LZ4_compressBound(input.size()));
                    out.resize(LZ4_compress_default(input.data(), out.data(), input.size(), out.size()));
                } else {
                    snappy::Compress(input.data(), input.size(), &out);
                }
                streams.emplace_back(std::move(out), input);
            };
            for (const auto& input : compression_inputs(rng)) {
                add_library(input);
                streams.emplace_back(compress(*c, input), input);
            }
            // Repeats at the largest LZ4 offset, which cross contiguous pieces.
            const auto r = random(65535);
            add_library(r + r + r + r + r);
            if (codec == pq::compression_codec::lz4_raw) {
                // A long literal, and long matches with short periods.
                std::string s;
                const auto lit = random(300 * 1024);
                lz4_sequence(s, lit);
                streams.emplace_back(s, lit);
                s.clear();
                lz4_sequence(s, "ab", 2, 1 << 20);
                lz4_sequence(s, "xyz", 3, 300 * 1024);
                lz4_sequence(s, "12345");
                streams.emplace_back(s, "ab" + std::string(1 << 20, '\0') + "xyz" + std::string(300 * 1024, '\0') + "12345");
                auto& u = streams.back().second;
                for (size_t i = 2; i < 2 + (1 << 20); ++i) {
                    u[i] = u[i - 2];
                }
                for (size_t i = (1 << 20) + 5; i < (1 << 20) + 5 + 300 * 1024; ++i) {
                    u[i] = u[i - 3];
                }
            } else {
                // A literal with a 3-byte length, and copies with 4-byte offsets.
                const auto lit = random(200 * 1024);
                std::string s;
                std::string u = lit;
                const size_t total = lit.size() + 1000 * 64;
                size_t v = total;
                for (; v >= 128; v >>= 7) {
                    s += char((v & 127) | 128);
                }
                s += char(v);
                s += char(62 << 2);
                const size_t len = lit.size() - 1;
                s += char(len & 255);
                s += char((len >> 8) & 255);
                s += char(len >> 16);
                s += lit;
                for (int i = 0; i < 1000; ++i) {
                    const uint32_t offset = 150000 + i;
                    s += char((63 << 2) | 3);
                    s.append(reinterpret_cast<const char*>(&offset), 4);
                    for (int k = 0; k < 64; ++k) {
                        u += u[u.size() - offset];
                    }
                }
                streams.emplace_back(s, u);
            }
            size_t damaged = 0;
            size_t ours_only = 0;
            for (const auto& [compressed, input] : streams) {
                BOOST_TEST_CONTEXT(input.size() << " bytes") {
                    BOOST_REQUIRE(library_decompress(codec, compressed, input.size()) == input);
                    for (size_t p : {size_t(1000), piece}) {
                        BOOST_REQUIRE(decompress(*c, compressed, input.size(), p) == input);
                    }
                    if (input.size() <= pq::sync_decompress_threshold) {
                        continue;
                    }
                    // Damage. When both decoders succeed, they agree. When the
                    // library succeeds, ours does too.
                    for (int i = 0; i < 100; ++i) {
                        auto d = compressed;
                        const size_t pos = i < 50 ? i * d.size() / 50 : rng() % d.size();
                        d[pos] ^= char(1 << (rng() % 8));
                        if (i % 10 == 0) {
                            d.resize(rng() % d.size());
                        }
                        const auto theirs = library_decompress(codec, d, input.size());
                        const auto ours = our_decompress(*c, d, input.size(), 1000);
                        if (theirs) {
                            BOOST_REQUIRE(ours == theirs);
                        } else if (ours) {
                            ++ours_only;
                        }
                        ++damaged;
                    }
                }
            }
            testlog.info("codec {}: {} damaged streams, {} decoded only by our decoder", int(codec), damaged, ours_only);
        }
    }
}
