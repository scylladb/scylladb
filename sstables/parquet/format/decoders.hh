/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

// Value decoders -- the inverse of encoders.hh.
//
// Until now the read side could parse metadata and definition levels but not a
// single value, which meant nothing written could be read back. These close the
// loop for the encodings the writer emits: PLAIN, RLE_DICTIONARY and
// DELTA_BINARY_PACKED, plus BYTE_STREAM_SPLIT for completeness.
//
// Every decoder is bounded: it is told how many values to produce and never
// reads past the buffer it was given, because page bodies can come from
// untrusted files.

#include "rle_bitpack.hh"

#include <cstdint>
#include <cstring>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

namespace sstables::parquet::format {

class decode_error : public std::runtime_error {
public:
    explicit decode_error(const std::string& w) : std::runtime_error("parquet/decode: " + w) {}
};

// Every decoder below takes an optional leading `skip`: produce `count` values starting at the
// `skip`-th one in the stream, rather than at the first. A point read reads one page per leaf and
// wants a few of the values in it, so the values before its window are pure waste -- and for the
// dictionary and BYTE_ARRAY paths that waste is an allocation per value, not just a load. Where
// the encoding allows it (PLAIN, BYTE_STREAM_SPLIT, RLE_DICTIONARY) the skip is arithmetic or a
// run walk; where it cannot (the DELTA family, whose values are defined relative to their
// predecessor) the decoder still decodes from the start and simply does not hand back the head,
// which is what it did before this parameter existed.
//
// `skip == 0` is the whole of the scan path and is bit-for-bit the old behaviour.

// ---------------------------------------------------------------- PLAIN
template <typename T>
inline std::vector<T> decode_plain(std::span<const uint8_t> in, size_t count, size_t skip = 0) {
    if (in.size() / sizeof(T) < skip || (in.size() - skip * sizeof(T)) / sizeof(T) < count) {
        throw decode_error("PLAIN buffer too small for " + std::to_string(count) + " values");
    }
    std::vector<T> out(count);
    // memcpy with a null source or destination is UB even for a zero length, and
    // an all-null column legitimately produces a zero-value page.
    if (count) { std::memcpy(out.data(), in.data() + skip * sizeof(T), count * sizeof(T)); }
    return out;
}

// The length prefixes still have to be walked to find the `skip`-th value, but no string is built
// for the ones passed over.
inline std::vector<std::string> decode_plain_byte_array(std::span<const uint8_t> in, size_t count,
                                                       size_t skip = 0) {
    std::vector<std::string> out;
    out.reserve(count);
    size_t p = 0;
    for (size_t i = 0; i < skip; ++i) {
        if (p + 4 > in.size()) { throw decode_error("truncated BYTE_ARRAY length"); }
        uint32_t n;
        std::memcpy(&n, in.data() + p, 4);
        p += 4;
        if (p + n > in.size()) { throw decode_error("truncated BYTE_ARRAY value"); }
        p += n;
    }
    for (size_t i = 0; i < count; ++i) {
        if (p + 4 > in.size()) { throw decode_error("truncated BYTE_ARRAY length"); }
        uint32_t n;
        std::memcpy(&n, in.data() + p, 4);
        p += 4;
        if (p + n > in.size()) { throw decode_error("truncated BYTE_ARRAY value"); }
        out.emplace_back(reinterpret_cast<const char*>(in.data() + p), n);
        p += n;
    }
    return out;
}

// ---------------------------------------------------------------- BYTE_STREAM_SPLIT
// `total` is the number of values in the whole stream, which is what the stride is made of: the
// b-th byte of value i lives at `b * total + i`. It is therefore *not* derivable from the slice
// being asked for, which is why it is a separate parameter rather than `skip + count`.
template <typename T>
inline std::vector<T> decode_byte_stream_split(std::span<const uint8_t> in, size_t total,
                                              size_t skip = 0, size_t count = size_t(-1)) {
    constexpr size_t K = sizeof(T);
    if (count == size_t(-1)) { count = total - std::min(total, skip); }
    if (in.size() < total * K) { throw decode_error("BYTE_STREAM_SPLIT buffer too small"); }
    if (skip > total || count > total - skip) {
        throw decode_error("BYTE_STREAM_SPLIT range outside the stream");
    }
    std::vector<T> out(count);
    for (size_t i = 0; i < count; ++i) {
        uint8_t buf[K];
        for (size_t b = 0; b < K; ++b) { buf[b] = in[b * total + skip + i]; }
        std::memcpy(&out[i], buf, K);
    }
    return out;
}

// A BYTE_ARRAY dictionary page as a table of views into the page's own bytes.
//
// Materialising it as std::string cost more than everything else in a point read
// put together: a column with a near-unique dictionary made every reader allocate
// one string per distinct value before it could decode a single row. Views make
// the table one allocation and copy only the values actually referenced.
// The caller must keep the decompressed page alive for as long as the views.
inline std::vector<std::string_view> index_plain_byte_array(std::span<const uint8_t> in,
                                                            size_t count) {
    std::vector<std::string_view> out;
    out.reserve(count);
    size_t p = 0;
    for (size_t i = 0; i < count; ++i) {
        if (p + 4 > in.size()) { throw decode_error("truncated BYTE_ARRAY length"); }
        uint32_t n;
        std::memcpy(&n, in.data() + p, 4);
        p += 4;
        if (p + n > in.size()) { throw decode_error("truncated BYTE_ARRAY value"); }
        out.emplace_back(reinterpret_cast<const char*>(in.data() + p), n);
        p += n;
    }
    return out;
}

// RLE_DICTIONARY over a view table: only referenced entries become strings.
inline std::vector<std::string> decode_rle_dictionary_views(std::span<const uint8_t> page,
                                                            std::span<const std::string_view> dict,
                                                            size_t count, size_t skip = 0) {
    if (page.empty()) { throw decode_error("empty dictionary index page"); }
    const uint8_t bw = page[0];
    rle_decoder dec(page.subspan(1), bw);
    if (skip && dec.skip(skip) != skip) { throw decode_error("short dictionary index stream"); }
    auto idx = dec.decode_all(count);
    if (idx.size() != count) { throw decode_error("short dictionary index stream"); }
    std::vector<std::string> out;
    out.reserve(count);
    for (uint64_t i : idx) {
        if (i >= dict.size()) { throw decode_error("dictionary index out of range"); }
        out.emplace_back(dict[size_t(i)]);
    }
    return out;
}

// ---------------------------------------------------------------- RLE_DICTIONARY
// The data page is a single bit-width byte followed by an RLE/bit-packed hybrid
// stream of indices into the dictionary page.
template <typename T>
inline std::vector<T> decode_rle_dictionary(std::span<const uint8_t> page,
                                            const std::vector<T>& dict,
                                            size_t count, size_t skip = 0) {
    if (page.empty()) { throw decode_error("empty dictionary index page"); }
    const uint8_t bw = page[0];
    rle_decoder dec(page.subspan(1), bw);
    if (skip && dec.skip(skip) != skip) { throw decode_error("short dictionary index stream"); }
    auto idx = dec.decode_all(count);
    if (idx.size() != count) { throw decode_error("short dictionary index stream"); }
    std::vector<T> out;
    out.reserve(count);
    for (uint64_t i : idx) {
        if (i >= dict.size()) { throw decode_error("dictionary index out of range"); }
        out.push_back(dict[size_t(i)]);
    }
    return out;
}

// ---------------------------------------------------------------- DELTA_BINARY_PACKED
// `consumed`, when given, receives the number of input bytes this block occupied. DELTA_BYTE_ARRAY
// needs it: its three streams are concatenated with no length prefixes, so the only way to find where
// the suffix data begins is to know where the preceding stream ended.
inline std::vector<int64_t> decode_delta_binary_packed(std::span<const uint8_t> in, size_t count,
                                                      size_t* consumed = nullptr, size_t skip = 0) {
    // Each value is defined against its predecessor, so there is nothing to seek with: the head
    // has to be decoded and is then dropped. `count` below is the number *wanted*, so the loops
    // run to skip + count.
    //
    // Stopping short of the stream is safe here and only here: this decoder's own values are
    // correct from the first one, and a caller that also needs `consumed` -- the two DELTA
    // byte-array encodings, whose streams are concatenated without lengths -- must pass the
    // stream's full count so the block is walked to its end.
    count += skip;
    size_t p = 0;
    auto uvarint = [&] () -> uint64_t {
        uint64_t v = 0; int shift = 0;
        for (int i = 0; i < 10; ++i) {
            if (p >= in.size()) { throw decode_error("truncated varint"); }
            uint8_t b = in[p++];
            v |= uint64_t(b & 0x7F) << shift;
            if (!(b & 0x80)) { return v; }
            shift += 7;
        }
        throw decode_error("varint too long");
    };
    auto zigzag = [&] () -> int64_t {
        uint64_t u = uvarint();
        return int64_t(u >> 1) ^ -int64_t(u & 1);
    };

    const uint64_t block = uvarint();
    const uint64_t minis = uvarint();
    const uint64_t total = uvarint();
    if (block == 0 || minis == 0 || block % minis) { throw decode_error("bad delta header"); }
    const uint64_t mini = block / minis;
    if (mini % 32) { throw decode_error("miniblock size not a multiple of 32"); }

    std::vector<int64_t> out;
    out.reserve(count);
    if (total == 0) {
        // The first value is part of the *header*, not of the data, so it is present even when the
        // block holds no values -- the spec's header is <block size> <miniblocks> <count> <first
        // value> and every writer emits all four. Returning without consuming it left the stream
        // positioned one varint early.
        //
        // Harmless while a delta block was always the whole page body, which is why it survived: the
        // leftover byte was simply never read. DELTA_BYTE_ARRAY is the first encoding here to
        // concatenate two delta blocks with no length prefix between them, so the second block
        // started parsing at the stray varint and failed with "bad delta header".
        zigzag();
        if (consumed) { *consumed = p; }
        return out;
    }

    int64_t prev = zigzag();
    out.push_back(prev);

    while (out.size() < count && out.size() < total) {
        const int64_t min_delta = zigzag();
        if (p + minis > in.size()) { throw decode_error("truncated miniblock widths"); }
        std::vector<uint8_t> widths(in.begin() + long(p), in.begin() + long(p + minis));
        p += minis;

        for (uint64_t m = 0; m < minis && out.size() < count && out.size() < total; ++m) {
            const uint8_t w = widths[m];
            if (w > 64) { throw decode_error("delta bit width > 64"); }
            if (w == 0) {
                for (uint64_t i = 0; i < mini && out.size() < count && out.size() < total; ++i) {
                    prev = int64_t(uint64_t(prev) + uint64_t(min_delta));
                    out.push_back(prev);
                }
                continue;
            }
            const size_t need = size_t(mini) * w / 8;
            if (p + need > in.size()) { throw decode_error("truncated miniblock body"); }
            // Plain bit-packing, LSB first -- no RLE hybrid header here. `bitpack_acc` rather than
            // uint64_t because the refill below overshoots: it stops on a byte boundary at or past
            // `w`, so with up to 7 bits already in flight it can hold w+7 bits at once. A uint64_t
            // dropped the overshooting byte's top bits, which belonged to the *next* value. See the
            // comment on bitpack_acc in rle_bitpack.hh.
            bitpack_acc acc = 0; int bits = 0; size_t q = p;
            for (uint64_t i = 0; i < mini; ++i) {
                while (bits < w) { acc |= bitpack_acc(in[q++]) << bits; bits += 8; }
                const uint64_t v = uint64_t(acc) & ((w == 64) ? ~0ull : ((1ull << w) - 1));
                acc >>= w;
                bits -= w;
                if (out.size() < count && out.size() < total) {
                    // Unsigned throughout, to undo the encoder's wrap exactly rather
                    // than overflow a signed add. See format/encoders.hh.
                    prev = int64_t(uint64_t(prev) + uint64_t(min_delta) + v);
                    out.push_back(prev);
                }
            }
            p += need;
        }
    }
    if (consumed) { *consumed = p; }
    if (skip) { out.erase(out.begin(), out.begin() + long(std::min(skip, out.size()))); }
    return out;
}

// ---------------------------------------------------------------- DELTA_LENGTH_BYTE_ARRAY
// A DELTA_BINARY_PACKED block of lengths, then the values' bytes back to back.
// `total` is the number of values in the stream, and it is the count the length block must be
// decoded with regardless of how few values are wanted: decode_delta_binary_packed() stops as
// soon as it has produced `count` values, so a short count leaves `used` pointing into the middle
// of the length block and the value bytes are then read from the wrong offset. That was not a
// hypothetical -- asking for a slice of a DELTA_BYTE_ARRAY page made the suffix stream parse start
// at a stray varint and fail with "bad delta header" (test_pq_corpus_shaped_schema).
//
// So the head is always decoded here; only the strings are skipped.
inline std::vector<std::string> decode_delta_length_byte_array(std::span<const uint8_t> in,
                                                              size_t total, size_t skip = 0,
                                                              size_t take = size_t(-1)) {
    if (take == size_t(-1)) { take = total - std::min(total, skip); }
    size_t used = 0;
    auto lens = decode_delta_binary_packed(in, total, &used);
    std::vector<std::string> out;
    out.reserve(take);
    size_t p = used;
    for (size_t i = 0; i < lens.size(); ++i) {
        const int64_t L = lens[i];
        if (L < 0) { throw decode_error("negative length in delta_length_byte_array"); }
        if (p + size_t(L) > in.size()) { throw decode_error("truncated delta_length_byte_array"); }
        if (i >= skip && out.size() < take) {
            out.emplace_back(reinterpret_cast<const char*>(in.data() + p), size_t(L));
        }
        p += size_t(L);
    }
    return out;
}

// ---------------------------------------------------------------- DELTA_BYTE_ARRAY
// Prefix lengths, then a whole DELTA_LENGTH_BYTE_ARRAY of suffixes. Each value is the first
// `prefix` bytes of the value before it, followed by its own suffix -- so decoding is strictly
// sequential and a corrupt prefix length is not recoverable, which is why it is range-checked
// against the previous value rather than trusted.
inline std::vector<std::string> decode_delta_byte_array(std::span<const uint8_t> in, size_t total,
                                                       size_t skip = 0, size_t take = size_t(-1)) {
    // Strictly sequential -- value i is a prefix of value i-1 plus its own suffix -- and, like
    // DELTA_LENGTH_BYTE_ARRAY above, its two streams are concatenated with no length prefix, so
    // the prefix block has to be decoded in full for `used` to locate the suffix block. Both
    // reasons say the same thing: `total`, never a short count. Only the strings are skipped.
    if (take == size_t(-1)) { take = total - std::min(total, skip); }
    size_t used = 0;
    auto prefixes = decode_delta_binary_packed(in, total, &used);
    auto suffixes = decode_delta_length_byte_array(in.subspan(used), total);
    if (suffixes.size() != prefixes.size()) {
        throw decode_error("delta_byte_array: prefix and suffix counts differ");
    }
    std::vector<std::string> out;
    out.reserve(prefixes.size());
    std::string prev;
    for (size_t i = 0; i < prefixes.size(); ++i) {
        const int64_t k = prefixes[i];
        if (k < 0 || size_t(k) > prev.size()) {
            throw decode_error("delta_byte_array: prefix length exceeds the previous value");
        }
        std::string v;
        v.reserve(size_t(k) + suffixes[i].size());
        v.assign(prev, 0, size_t(k));
        v.append(suffixes[i]);
        prev = v;
        if (i >= skip && out.size() < take) { out.push_back(std::move(v)); }
    }
    return out;
}

} // namespace sstables::parquet::format
