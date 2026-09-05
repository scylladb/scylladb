/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

// Parquet's RLE / bit-packed hybrid encoding.
//
// This is the most load-bearing encoding in the format: it carries definition
// and repetition levels for every optional or repeated column, and the index
// stream of every dictionary-encoded column. Getting it right (and bounded) is
// most of what a Parquet reader does before it touches user values.
//
// Wire format (parquet-format/Encodings.md):
//
//   rle-bit-packed-hybrid := <length>? <run>*
//   run                   := <bit-packed-run> | <rle-run>
//   bit-packed-run        := <bit-packed-header> <bit-packed-values>
//   bit-packed-header     := varint(<bit-packed-groups-count> << 1 | 1)
//   rle-run               := <rle-header> <repeated-value>
//   rle-header            := varint(<run-length> << 1)
//   repeated-value        := value in ceil(bit-width / 8) bytes, little-endian
//
// Bit-packed values are packed least-significant-bit first, in groups of 8.

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <span>
#include <stdexcept>
#include <string>
#include <vector>

namespace sstables::parquet::format {

// The accumulator every bit-packing loop in this directory shifts values through -- here and in
// encoders.hh / decoders.hh -- and it has to be *wider* than the widest value it packs.
//
// The loops all hold a partial byte between values: after emitting or consuming whole bytes, 0..7
// bits of the previous value are still in flight. So a value of `w` bits enters the accumulator at
// an offset of up to 7 and occupies bit positions up to `w + 6`. With w up to 64 that is 71 bits,
// and a uint64_t accumulator silently discarded everything past bit 63 -- losing the top bits of
// the value on the write side and the top bits of the *next* value on the read side.
//
// This was not theoretical: it corrupted `bigint` and `timestamp` key columns, which are the ones
// DELTA_BINARY_PACKED is applied to (schema_mapping.cc), whenever a miniblock's delta range needed
// more than 57 bits -- which is the normal case for a partition key, where deltas are zero inside a
// partition and an arbitrary 64-bit jump between partitions. See §9.6 of the design doc.
// `unsigned __int128` is a GCC/Clang extension, so -Wpedantic objects to naming it. Silenced at
// the definition rather than by dropping the flag: run_tests.sh compiles these sources standalone
// with -Wall -Wextra -Wpedantic and no -Werror, so the warning was not fatal -- it was just ten
// lines of noise per build in a suite whose value depends on someone reading its output. A pragma
// rather than __extension__ because that script uses g++ while the main build uses clang++.
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wpedantic"
using bitpack_acc = unsigned __int128;
#pragma GCC diagnostic pop

class rle_error : public std::runtime_error {
public:
    explicit rle_error(const std::string& w) : std::runtime_error("parquet/rle: " + w) {}
};

class rle_decoder {
    const uint8_t* _p;
    const uint8_t* _end;
    uint8_t _bit_width;
    uint8_t _byte_width;

    // Current run state.
    uint64_t _rle_count = 0;      // values left in the current RLE run
    uint64_t _rle_value = 0;
    uint64_t _packed_count = 0;   // values left in the current bit-packed run
    uint64_t _group[8]{};
    uint8_t  _group_len = 0;
    uint8_t  _group_pos = 0;

    uint64_t uvarint() {
        uint64_t v = 0; int shift = 0;
        for (int i = 0; i < 10; ++i) {
            if (_p >= _end) { throw rle_error("truncated varint"); }
            uint8_t b = *_p++;
            v |= uint64_t(b & 0x7F) << shift;
            if (!(b & 0x80)) { return v; }
            shift += 7;
        }
        throw rle_error("varint too long");
    }

    // Unpack one group of 8 values of _bit_width bits each.
    void unpack8() {
        size_t need = (size_t(_bit_width) * 8 + 7) / 8;   // == bit_width bytes
        if (size_t(_end - _p) < need) { throw rle_error("truncated bit-packed group"); }
        bitpack_acc acc = 0;
        int acc_bits = 0;
        const uint8_t* q = _p;
        for (int i = 0; i < 8; ++i) {
            while (acc_bits < _bit_width) {
                acc |= bitpack_acc(*q++) << acc_bits;
                acc_bits += 8;
            }
            _group[i] = uint64_t(acc)
                    & (_bit_width == 64 ? ~uint64_t(0) : ((uint64_t(1) << _bit_width) - 1));
            acc >>= _bit_width;
            acc_bits -= _bit_width;
        }
        _p += need;
        _group_len = 8;
        _group_pos = 0;
    }

    void next_run() {
        if (_p >= _end) { throw rle_error("no more runs"); }
        uint64_t hdr = uvarint();
        if (hdr & 1) {
            uint64_t groups = hdr >> 1;
            if (groups == 0) { throw rle_error("bit-packed run with zero groups"); }
            // Bound: a run cannot claim more data than remains.
            if (groups > uint64_t(_end - _p)) { throw rle_error("bit-packed run too long"); }
            _packed_count = groups * 8;
            _group_len = _group_pos = 0;
        } else {
            uint64_t n = hdr >> 1;
            if (n == 0) { throw rle_error("RLE run of length zero"); }
            if (size_t(_end - _p) < _byte_width) { throw rle_error("truncated RLE value"); }
            uint64_t v = 0;
            std::memcpy(&v, _p, _byte_width);   // little-endian on the wire
            _p += _byte_width;
            _rle_count = n;
            _rle_value = v;
        }
    }

public:
    // `bit_width` of 0 means every value is 0 and nothing is stored.
    rle_decoder(std::span<const uint8_t> buf, uint8_t bit_width)
        : _p(buf.data()), _end(buf.data() + buf.size()), _bit_width(bit_width),
          _byte_width(uint8_t((bit_width + 7) / 8)) {
        if (bit_width > 64) { throw rle_error("bit width > 64"); }
    }

    // Decode up to `max` values. Returns how many were produced; a short return
    // means the input is exhausted.
    size_t decode(uint64_t* out, size_t max) {
        size_t n = 0;
        if (_bit_width == 0) {
            for (; n < max; ++n) { out[n] = 0; }
            return n;
        }
        while (n < max) {
            if (_rle_count) {
                size_t take = size_t(std::min<uint64_t>(_rle_count, max - n));
                for (size_t i = 0; i < take; ++i) { out[n + i] = _rle_value; }
                _rle_count -= take; n += take;
            } else if (_packed_count) {
                if (_group_pos >= _group_len) {
                    unpack8();
                }
                size_t avail = size_t(_group_len - _group_pos);
                size_t take = std::min({avail, max - n, size_t(_packed_count)});
                for (size_t i = 0; i < take; ++i) { out[n + i] = _group[_group_pos + i]; }
                _group_pos = uint8_t(_group_pos + take);
                _packed_count -= take; n += take;
            } else {
                if (_p >= _end) { break; }
                next_run();
            }
        }
        return n;
    }

    // Advance past `max` values without materialising them. Returns how many were stepped over;
    // a short return means the input is exhausted, exactly as decode() reports it.
    //
    // Exists because a point read decodes one page per leaf and wants a handful of the values in
    // it. Decoding the ones before the window and throwing them away was the single largest term
    // in the read once the window itself was fixed (design doc 10.28): for a dictionary-encoded
    // column that is a dictionary lookup and -- for BYTE_ARRAY -- a std::string per value, all of
    // it discarded. A whole bit-packed group is eight values in `_bit_width` bytes, so skipping
    // one is a pointer add rather than an unpack; that is what makes this cheaper than decoding
    // into a scratch buffer rather than merely tidier.
    size_t skip(size_t max) {
        if (_bit_width == 0) {
            // decode() would have produced `max` zeroes from nothing, and consumed no input.
            return max;
        }
        size_t n = 0;
        while (n < max) {
            if (_rle_count) {
                const size_t take = size_t(std::min<uint64_t>(_rle_count, max - n));
                _rle_count -= take;
                n += take;
            } else if (_packed_count) {
                if (_group_pos < _group_len) {
                    // A group already in hand, possibly part-consumed: step within it.
                    const size_t avail = size_t(_group_len - _group_pos);
                    const size_t take = std::min({avail, max - n, size_t(_packed_count)});
                    _group_pos = uint8_t(_group_pos + take);
                    _packed_count -= take;
                    n += take;
                    continue;
                }
                // Nothing in hand, so whole groups can be stepped over by moving the read
                // pointer -- eight values for `_bit_width` bytes, with no unpacking at all.
                const size_t whole = size_t(std::min<uint64_t>(_packed_count / 8, (max - n) / 8));
                if (whole) {
                    const size_t need = size_t(_bit_width) * whole;
                    if (size_t(_end - _p) < need) { throw rle_error("truncated bit-packed group"); }
                    _p += need;
                    _packed_count -= whole * 8;
                    n += whole * 8;
                    continue;
                }
                unpack8();      // fewer than eight left to step: go through the group path
            } else {
                if (_p >= _end) { break; }
                next_run();
            }
        }
        return n;
    }

    std::vector<uint64_t> decode_all(size_t count) {
        std::vector<uint64_t> v(count);
        size_t got = decode(v.data(), count);
        v.resize(got);
        return v;
    }
};

// Minimal encoder, used to round-trip the decoder in tests. Emits one
// bit-packed run per 8 values, plus RLE runs for long repeats -- not optimal,
// but spec-conformant, which is what a round-trip test needs.
class rle_encoder {
    std::vector<uint8_t> _out;
    uint8_t _bit_width, _byte_width;

    void put_uvarint(uint64_t v) {
        while (v >= 0x80) { _out.push_back(uint8_t(v) | 0x80); v >>= 7; }
        _out.push_back(uint8_t(v));
    }
    void put_rle(uint64_t value, uint64_t count) {
        put_uvarint(count << 1);
        for (int i = 0; i < _byte_width; ++i) { _out.push_back(uint8_t(value >> (8 * i))); }
    }
    void put_packed(const uint64_t* v, size_t groups) {
        put_uvarint((groups << 1) | 1);
        for (size_t g = 0; g < groups; ++g) {
            bitpack_acc acc = 0; int bits = 0;
            for (int i = 0; i < 8; ++i) {
                acc |= bitpack_acc(v[g * 8 + i]
                                & ((_bit_width == 64) ? ~0ull : ((1ull << _bit_width) - 1))) << bits;
                bits += _bit_width;
                while (bits >= 8) { _out.push_back(uint8_t(acc)); acc >>= 8; bits -= 8; }
            }
            if (bits > 0) { _out.push_back(uint8_t(acc)); }
        }
    }

public:
    explicit rle_encoder(uint8_t bit_width)
        : _bit_width(bit_width), _byte_width(uint8_t((bit_width + 7) / 8)) {}

    // A bit-packed run always encodes a whole number of groups of 8. Padding a
    // run in the middle of the stream would shift every value after it, so the
    // literal buffer is only ever flushed on a multiple of 8 -- except at the
    // very end, where padding is harmless because the reader stops at num_values.
    std::vector<uint64_t> _lit;

    void flush_literals(bool final_flush) {
        if (_lit.empty()) { return; }
        size_t groups = final_flush ? (_lit.size() + 7) / 8 : _lit.size() / 8;
        if (groups == 0) { return; }
        if (final_flush) {
            _lit.resize(groups * 8, 0);          // padding is safe only at the end
            put_packed(_lit.data(), groups);
            _lit.clear();
        } else {
            put_packed(_lit.data(), groups);
            // Carry the sub-group remainder forward; dropping it here would
            // silently lose values.
            _lit.erase(_lit.begin(), _lit.begin() + long(groups * 8));
        }
    }

public:
    void encode(std::span<const uint64_t> vals) {
        if (_bit_width == 0) { return; }
        size_t i = 0, n = vals.size();
        while (i < n) {
            size_t run = 1;
            while (i + run < n && vals[i + run] == vals[i]) { ++run; }

            if (run >= 8) {
                size_t need = (8 - (_lit.size() % 8)) % 8;
                if (need == 0) {
                    flush_literals(false);
                    put_rle(vals[i], run);
                    i += run;
                    continue;
                }
                if (run - need >= 8) {
                    // Top the literal buffer up to a group boundary using values
                    // from the head of the run, then RLE what is left.
                    for (size_t k = 0; k < need; ++k) { _lit.push_back(vals[i]); }
                    i += need; run -= need;
                    flush_literals(false);
                    put_rle(vals[i], run);
                    i += run;
                    continue;
                }
            }
            for (size_t k = 0; k < run; ++k) { _lit.push_back(vals[i + k]); }
            i += run;
            if (_lit.size() >= 512) { flush_literals(false); }
        }
        flush_literals(true);
    }
public:
    const std::vector<uint8_t>& bytes() const { return _out; }
};

inline uint8_t bit_width_for(uint64_t max_value) {
    uint8_t w = 0;
    while (max_value) { ++w; max_value >>= 1; }
    return w;
}

} // namespace sstables::parquet::format
