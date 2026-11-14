/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

// The RLE/bit-packing hybrid encoding of Parquet (Encodings.md, "RLE = 3").
//
// It stores definition and repetition levels, dictionary indices, and the
// RLE encoding of booleans. Data Page V2 stores levels without the 4-byte
// length prefix, and the classes here never write or read that prefix.
// Callers that need it (boolean values, see encoding.hh) add it themselves.
// Dictionary indices are preceded by a byte with the bit width, which the
// callers in encoding.hh also handle.

#include <array>
#include <cstdint>
#include <span>

#include "bytes_ostream.hh"
#include "sstables/parquet/bit_util.hh"

namespace sstables::parquet {

/// The bit width of the levels of a column whose maximum level is `max_level`.
inline unsigned level_bit_width(unsigned max_level) noexcept {
    return bit_width_of(max_level);
}

/// Encodes values of a fixed bit width (0 to 32) with the RLE/bit-packing hybrid.
///
/// append_run() appends any number of equal values in constant time: a run
/// that continues the previous one only grows a counter. The column writer
/// relies on this to record a gap of nulls as one call. A run of at least
/// `min_rle_run` values becomes an RLE run, so a reader can learn the
/// length of a null run without decoding value by value. Shorter runs are
/// bit-packed.
///
/// The encoder buffers its output in its own bytes_ostream, in contiguous
/// pieces of at most 128 KiB.
class rle_bp_encoder {
public:
    /// The shortest run that is stored as an RLE run.
    static constexpr uint64_t min_rle_run = 8;
private:
    bytes_ostream _out;
    unsigned _bit_width;
    // Values that are waiting to fill a group of 8. They precede the pending run.
    std::array<uint32_t, 8> _literals;
    unsigned _literal_count = 0;
    // The pending run of equal values, after the literals.
    uint32_t _run_value = 0;
    uint64_t _run_length = 0;
    // The header byte of the open bit-packed run, or null. The run holds at
    // most 63 groups, so the header always fits in one byte.
    bytes_ostream::value_type* _bp_header = nullptr;
    unsigned _bp_groups = 0;
    uint64_t _count = 0;

    void flush_run();
    void push_literal(uint32_t v);
    void write_group();
    void close_bp_run();
    void write_rle_run(uint32_t v, uint64_t n);
public:
    explicit rle_bp_encoder(unsigned bit_width);

    rle_bp_encoder(rle_bp_encoder&&) noexcept = default;

    unsigned bit_width() const noexcept { return _bit_width; }

    /// Appends one value. The value must fit in the bit width.
    void append(uint32_t v) { append_run(v, 1); }
    /// Appends `n` copies of `v`, in constant time.
    void append_run(uint32_t v, uint64_t n);

    /// The number of values appended since construction or the last finish().
    uint64_t count() const noexcept { return _count; }

    /// An upper bound of the size of the output that finish() would return
    /// now. Constant time.
    uint64_t estimated_size() const noexcept;

    /// Returns the encoded values and resets the encoder for the next page.
    /// The last bit-packed group is padded with zeros. A reader knows the
    /// number of values from the page header and ignores the padding.
    bytes_ostream finish();
};

/// Decodes the RLE/bit-packing hybrid encoding from untrusted input.
///
/// The decoder exposes runs: next_run() returns a run of equal values, so a
/// reader can skip a run of nulls without decoding value by value. Within a
/// bit-packed run, it returns the runs of equal values that it finds there.
///
/// It raises the malformed sstable error if the input ends before the
/// requested values, or if a value does not fit in the bit width.
/// It allocates nothing.
class rle_bp_decoder {
    byte_reader _in;
    unsigned _bit_width;
    unsigned _value_bytes;
    // Values left in the current run.
    uint64_t _left = 0;
    // The current run is an RLE run of _value. Otherwise it is bit-packed:
    // _group holds the unpacked current group, and _group_pos is the
    // position of the next value in it.
    bool _repeated = false;
    uint32_t _value = 0;
    std::array<uint32_t, 8> _group;
    unsigned _group_pos = 8;

    void next_header();
    void load_group();
public:
    rle_bp_decoder() noexcept : _bit_width(0), _value_bytes(0) {}
    rle_bp_decoder(byte_array_view data, unsigned bit_width);

    struct run {
        uint32_t value;
        uint64_t length;
    };

    /// Returns the next run of equal values, of at least 1 and at most
    /// `max` values, and consumes it. `max` must be at least 1.
    /// The decoder does not know the number of values: the last bit-packed
    /// group is padded, so the caller limits `max` to the values it expects.
    run next_run(uint64_t max);

    uint32_t next() { return next_run(1).value; }

    /// If the next values are bit-packed, decodes at most out.size() of
    /// them, which is at least 1, and returns their number. It stays
    /// within one bit-packed run. Returns 0 if the next run is an RLE
    /// run, which next_run() then returns. The bit width must be at most
    /// 8. Like next_run(), it may return the padding of the last group.
    size_t next_packed(std::span<uint8_t> out);

    /// Decodes exactly out.size() values.
    template <typename T>
    void decode(std::span<T> out);

    /// Skips `n` values.
    void skip(uint64_t n);

    /// The input that follows the values decoded so far. Meaningful only at
    /// the end of a run, for example after the last value of a page.
    const byte_reader& input() const noexcept { return _in; }
};

/// Estimates, in constant time per value, the size of the RLE/bit-packing
/// hybrid encoding of a sequence, for a bit width that is chosen later.
///
/// The column writer uses it for booleans and for dictionary indices, whose
/// bit width depends on the final size of the dictionary. The estimate
/// models runs of rle_bp_encoder::min_rle_run or more as RLE runs and
/// everything else as bit-packed. It ignores the alignment of runs to
/// groups of 8, so it is an approximation.
class rle_size_estimator {
    uint64_t _count = 0;
    uint32_t _run_value = 0;
    uint64_t _run_length = 0;
    // Runs that were closed.
    uint64_t _rle_runs = 0;
    uint64_t _rle_header_bytes = 0;
    uint64_t _packed_values = 0;
public:
    void append(uint32_t v) noexcept {
        ++_count;
        if (_run_length && v == _run_value) {
            ++_run_length;
            return;
        }
        close_run();
        _run_value = v;
        _run_length = 1;
    }
    void close_run() noexcept;
    uint64_t count() const noexcept { return _count; }
    uint64_t estimate(unsigned bit_width) const noexcept;
    void clear() noexcept { *this = {}; }
};

} // namespace sstables::parquet
