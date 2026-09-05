/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

// Parquet value encoders.
//
// PLAIN                -- baseline, and the fallback for everything.
// RLE_DICTIONARY       -- low-cardinality columns; the big win on enums and
//                         repeated strings.
// DELTA_BINARY_PACKED  -- integers and timestamps. This is the encoding that
//                         makes the folded __ts column nearly free, which is
//                         the mechanism behind the 26.8x folding result.
// BYTE_STREAM_SPLIT    -- floats and doubles; transposes mantissa bytes so the
//                         block compressor sees runs.

#include "rle_bitpack.hh"

#include <cstdint>
#include <cstring>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace sstables::parquet::format {

inline void put_le(std::vector<uint8_t>& o, const void* p, size_t n) {
    auto q = static_cast<const uint8_t*>(p);
    o.insert(o.end(), q, q + n);
}

// ---------------------------------------------------------------- PLAIN
template <typename T>
inline void encode_plain(std::vector<uint8_t>& out, std::span<const T> vals) {
    for (const T& v : vals) { put_le(out, &v, sizeof(T)); }
}

inline void encode_plain_byte_array(std::vector<uint8_t>& out,
                                    std::span<const std::string> vals) {
    for (const auto& s : vals) {
        uint32_t n = uint32_t(s.size());
        put_le(out, &n, 4);          // 4-byte little-endian length prefix
        out.insert(out.end(), s.begin(), s.end());
    }
}

// ---------------------------------------------------------------- BYTE_STREAM_SPLIT
// For k-byte values, write all byte-0s, then all byte-1s, ... Values themselves
// are unchanged; the point is to give the block compressor uniform streams.
template <typename T>
inline void encode_byte_stream_split(std::vector<uint8_t>& out, std::span<const T> vals) {
    constexpr size_t K = sizeof(T);
    const size_t n = vals.size();
    const size_t base = out.size();
    out.resize(base + n * K);
    for (size_t i = 0; i < n; ++i) {
        uint8_t buf[K];
        std::memcpy(buf, &vals[i], K);
        for (size_t b = 0; b < K; ++b) { out[base + b * n + i] = buf[b]; }
    }
}

// ---------------------------------------------------------------- DELTA_BINARY_PACKED
//
//   header := block_size, miniblocks_per_block, total_count (ULEB128),
//             first_value (zigzag varint)
//   block  := min_delta (zigzag varint), bitwidth[miniblocks], miniblock data
//
// block_size must be a multiple of 128 and miniblock size a multiple of 32.
// We use 128 / 4 -> 32 values per miniblock, which is what parquet-mr defaults to.
class delta_binary_packed_encoder {
    static constexpr size_t BLOCK = 128;
    static constexpr size_t MINIS = 4;
    static constexpr size_t MINI  = BLOCK / MINIS;   // 32

    std::vector<uint8_t>& _out;
    std::vector<int64_t> _deltas;
    int64_t _prev = 0;
    size_t _count = 0;

    static void uvarint(std::vector<uint8_t>& o, uint64_t v) {
        while (v >= 0x80) { o.push_back(uint8_t(v) | 0x80); v >>= 7; }
        o.push_back(uint8_t(v));
    }
    static void zigzag(std::vector<uint8_t>& o, int64_t v) {
        uvarint(o, (uint64_t(v) << 1) ^ uint64_t(v >> 63));
    }
    static uint8_t width_of(uint64_t max) {
        uint8_t w = 0; while (max) { ++w; max >>= 1; } return w;
    }

    // Every step of the delta arithmetic here is done in unsigned space, and
    // deliberately so: the values can span the whole int64 range -- timestamps at
    // both ends of it are a real case, and the losslessness suite generates them --
    // and a signed difference then overflows, which is undefined behaviour rather
    // than the wrap-around the format relies on. Modulo 2**64 throughout round-trips
    // exactly, and the decoder undoes it the same way.
    //
    // If deltas do wrap, `min_delta` stops being a meaningful minimum and the width
    // comes out at 64, so such a block simply does not compress. That is the correct
    // outcome: correctness first, and the case is pathological.
    void flush_block() {
        if (_deltas.empty()) { return; }
        // Pad the block out to BLOCK values so miniblock arithmetic is uniform.
        int64_t min_delta = _deltas[0];
        for (int64_t d : _deltas) { min_delta = std::min(min_delta, d); }
        _deltas.resize(BLOCK, min_delta);

        zigzag(_out, min_delta);
        size_t width_at = _out.size();
        for (size_t m = 0; m < MINIS; ++m) { _out.push_back(0); }

        for (size_t m = 0; m < MINIS; ++m) {
            uint64_t maxv = 0;
            for (size_t i = 0; i < MINI; ++i) {
                maxv = std::max(maxv, uint64_t(_deltas[m * MINI + i]) - uint64_t(min_delta));
            }
            uint8_t w = width_of(maxv);
            _out[width_at + m] = w;
            if (w == 0) { continue; }
            // Miniblock bodies are plain bit-packed (no RLE hybrid header). The accumulator is
            // `bitpack_acc`, not uint64_t: between values it holds up to 7 bits of the previous
            // one, so a w-bit value straddles up to w+6 bit positions -- 71 at w == 64. See the
            // comment on bitpack_acc in rle_bitpack.hh for what the narrow accumulator lost.
            bitpack_acc acc = 0; int bits = 0;
            for (size_t i = 0; i < MINI; ++i) {
                uint64_t v = uint64_t(_deltas[m * MINI + i]) - uint64_t(min_delta);
                acc |= bitpack_acc(w == 64 ? v : (v & ((1ull << w) - 1))) << bits;
                bits += w;
                while (bits >= 8) { _out.push_back(uint8_t(acc)); acc >>= 8; bits -= 8; }
            }
            if (bits > 0) { _out.push_back(uint8_t(acc)); }
        }
        _deltas.clear();
    }

public:
    explicit delta_binary_packed_encoder(std::vector<uint8_t>& out) : _out(out) {}

    void encode(std::span<const int64_t> vals) {
        _count = vals.size();
        uvarint(_out, BLOCK);
        uvarint(_out, MINIS);
        uvarint(_out, _count);
        if (vals.empty()) { zigzag(_out, 0); return; }
        zigzag(_out, vals[0]);
        _prev = vals[0];
        for (size_t i = 1; i < vals.size(); ++i) {
            _deltas.push_back(int64_t(uint64_t(vals[i]) - uint64_t(_prev)));
            _prev = vals[i];
            if (_deltas.size() == BLOCK) { flush_block(); }
        }
        flush_block();
    }
};

inline void encode_delta_binary_packed(std::vector<uint8_t>& out,
                                       std::span<const int64_t> vals) {
    delta_binary_packed_encoder(out).encode(vals);
}

// ---------------------------------------------------------------- DELTA_LENGTH_BYTE_ARRAY
// Lengths as one DELTA_BINARY_PACKED block, then every value's bytes concatenated. Useful on its own
// for values with no shared prefixes but similar lengths, and it is also the second half of
// DELTA_BYTE_ARRAY below.
inline void encode_delta_length_byte_array(std::vector<uint8_t>& out,
                                           std::span<const std::string> vals) {
    std::vector<int64_t> lens;
    lens.reserve(vals.size());
    for (const auto& s : vals) { lens.push_back(int64_t(s.size())); }
    encode_delta_binary_packed(out, lens);
    for (const auto& s : vals) { out.insert(out.end(), s.begin(), s.end()); }
}

// ---------------------------------------------------------------- DELTA_BYTE_ARRAY
// Incremental (front-coded) encoding: each value stores how many leading bytes it shares with the
// value *before it*, then only the remaining suffix.
//
// Layout, per the spec: DELTA_BINARY_PACKED prefix lengths, then a whole
// DELTA_LENGTH_BYTE_ARRAY of the suffixes -- i.e. DELTA_BINARY_PACKED suffix lengths followed by the
// concatenated suffix bytes. Three independent streams, each of which compresses well on its own,
// which is why this beats PLAIN even after zstd.
//
// It wins where adjacent values share a leading run, and in an SSTable that is the common case rather
// than a lucky one: rows arrive in clustering order, so a text clustering key is *sorted* within a row
// group. "st00042"/"st00043" share six of seven bytes; URLs sharing a host share the whole authority;
// timestamps rendered as text share everything but the last digits. Where there is no shared prefix
// the prefix stream is all zeroes, which delta-packs to nothing, so the downside is bounded at
// roughly the length stream -- cheaper than PLAIN's fixed 4 bytes per value.
//
// Deliberately not the default. A dictionary is better whenever values repeat (it stores each
// distinct value once, where this stores every occurrence), and the writer already prefers a
// dictionary when the repeat ratio justifies it. This is for the case a dictionary handles badly:
// many distinct values that happen to be ordered.
inline void encode_delta_byte_array(std::vector<uint8_t>& out,
                                     std::span<const std::string> vals) {
    std::vector<int64_t> prefixes;
    std::vector<std::string> suffixes;
    prefixes.reserve(vals.size());
    suffixes.reserve(vals.size());
    std::string_view prev;
    for (const auto& s : vals) {
        // The shared prefix is capped by both strings' lengths; the spec allows any value the
        // decoder can honour, and the longest common prefix is what makes it worth doing.
        size_t max_share = std::min(prev.size(), s.size());
        size_t k = 0;
        while (k < max_share && prev[k] == s[k]) { ++k; }
        prefixes.push_back(int64_t(k));
        suffixes.emplace_back(s.substr(k));
        prev = std::string_view(s);
    }
    encode_delta_binary_packed(out, prefixes);
    encode_delta_length_byte_array(out, suffixes);
}

// ---------------------------------------------------------------- RLE_DICTIONARY
// Returns the dictionary (as PLAIN-encoded values) and the index stream.
// The index stream is prefixed with a single bit-width byte, per the spec.
struct dict_result {
    std::vector<uint8_t> dictionary_page;   // PLAIN encoded distinct values
    std::vector<uint8_t> index_page;        // bit-width byte + RLE hybrid, whole chunk
    size_t num_distinct = 0;
    // The raw indices, retained so the chunk can be split into several data pages. The
    // dictionary stays per column chunk -- that is what Parquet specifies -- but each data
    // page carries its own RLE stream over a slice of these. Without them the chunk had to
    // be emitted as one page, which meant a point read decoded every row in the row group
    // to return one (design doc 10.4f).
    std::vector<uint64_t> indices;
    uint8_t bit_width = 1;
};

// One data page's worth of dictionary indices: the bit-width byte Parquet requires at the
// head of an RLE_DICTIONARY page body, then the hybrid stream for this slice only.
inline std::vector<uint8_t> encode_dict_index_page(std::span<const uint64_t> idx, uint8_t bw) {
    std::vector<uint8_t> out;
    out.push_back(bw);
    rle_encoder enc(bw);
    enc.encode(idx);
    out.insert(out.end(), enc.bytes().begin(), enc.bytes().end());
    return out;
}

inline dict_result encode_dictionary_byte_array(std::span<const std::string> vals) {
    dict_result r;
    std::unordered_map<std::string_view, uint32_t> seen;
    std::vector<const std::string*> distinct;
    std::vector<uint64_t> idx;
    idx.reserve(vals.size());
    for (const auto& s : vals) {
        auto it = seen.find(s);
        if (it == seen.end()) {
            uint32_t id = uint32_t(distinct.size());
            distinct.push_back(&s);
            seen.emplace(std::string_view(s), id);
            idx.push_back(id);
        } else {
            idx.push_back(it->second);
        }
    }
    r.num_distinct = distinct.size();
    for (auto* s : distinct) {
        uint32_t n = uint32_t(s->size());
        put_le(r.dictionary_page, &n, 4);
        r.dictionary_page.insert(r.dictionary_page.end(), s->begin(), s->end());
    }
    uint8_t bw = bit_width_for(r.num_distinct ? r.num_distinct - 1 : 0);
    // A single-entry dictionary needs zero bits to address, and a zero bit width
    // is what the arithmetic produces. Our own decoder copes, but parquet-cpp
    // rejects the resulting index stream ("Invalid number of indices: 0"), so
    // the file would not be externally readable -- which is the whole point of
    // the format. Cost is one bit per value in a case that compresses away.
    if (bw == 0) { bw = 1; }
    r.bit_width = bw;
    r.index_page = encode_dict_index_page(idx, bw);
    r.indices = std::move(idx);
    return r;
}

// The same, for fixed-width values. A PLAIN dictionary page for a fixed-width type
// is just the distinct values back to back, and the index stream is identical.
//
// Distinctness is decided on the **bit pattern**, not on `==`. For integers the two
// agree; for doubles the bit pattern is the stricter one, which is what a lossless
// format needs -- it keeps -0.0 apart from 0.0 and one NaN apart from another, where
// `==` would fold the first pair together and call the second pair unequal to itself.
template <typename T>
requires (sizeof(T) == 4 || sizeof(T) == 8)
inline dict_result encode_dictionary_fixed(std::span<const T> vals) {
    static_assert(std::is_trivially_copyable_v<T>);
    dict_result r;
    std::unordered_map<uint64_t, uint32_t> seen;
    std::vector<T> distinct;
    std::vector<uint64_t> idx;
    idx.reserve(vals.size());
    for (const T& v : vals) {
        uint64_t key = 0;
        std::memcpy(&key, &v, sizeof(T));
        auto it = seen.find(key);
        if (it == seen.end()) {
            uint32_t id = uint32_t(distinct.size());
            distinct.push_back(v);
            seen.emplace(key, id);
            idx.push_back(id);
        } else {
            idx.push_back(it->second);
        }
    }
    r.num_distinct = distinct.size();
    for (const T& v : distinct) { put_le(r.dictionary_page, &v, sizeof(T)); }
    uint8_t bw = bit_width_for(r.num_distinct ? r.num_distinct - 1 : 0);
    if (bw == 0) { bw = 1; }        // see encode_dictionary_byte_array
    r.bit_width = bw;
    r.index_page = encode_dict_index_page(idx, bw);
    r.indices = std::move(idx);
    return r;
}

// Definition levels for a flat optional column: 1 = present, 0 = null.
// V2 data pages store the level stream without the 4-byte length prefix that
// V1 pages use, which is why the writer only ever emits V2.
inline std::vector<uint8_t> encode_levels_v2(std::span<const uint64_t> levels,
                                             uint8_t max_level) {
    uint8_t bw = bit_width_for(max_level);
    rle_encoder enc(bw);
    enc.encode(levels);
    return enc.bytes();
}

} // namespace sstables::parquet::format
