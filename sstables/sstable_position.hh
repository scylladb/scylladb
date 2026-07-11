/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <compare>
#include <cstdint>
#include <limits>
#include <optional>
#include <fmt/format.h>

#include "utils/assert.hh"

namespace sstables {

class sstable_position_offset;

namespace sstable_position_detail {

// The extent_hint of an encoded logical position or offset. See sstable_position.
constexpr uint32_t logical_marker = std::numeric_limits<uint32_t>::max();

// Kind checks are compiled out of release builds. These operations are on the
// hot path of the sstable reader.
inline void debug_check_position_kind([[maybe_unused]] bool ok) noexcept {
#ifndef SCYLLA_BUILD_MODE_RELEASE
    SCYLLA_ASSERT(ok);
#endif
}

} // namespace sstable_position_detail

// A position in an sstable data file. A logical position is an offset in the
// uncompressed data. A physical position identifies a compression chunk and an
// offset in the uncompressed chunk. Each sstable uses one position type. The
// writer selects this type.
//
// Use the position type of the sstable for all position operations. Do not
// compare or subtract positions of different types.
//
// A physical position at the end of a chunk is written as the start of the next
// chunk: (next chunk position, 0), never (chunk position, uncompressed chunk
// size). Ordering and equality compare the chunk position first and the offset
// within the chunk second. Thus, they are correct only for positions in this
// normal form.
//
// Both position types share one representation, so that the operations don't
// have to branch on the type. A logical position `v` is encoded as the physical
// position (chunk_position=v, extent_hint=logical_marker, offset_within_chunk=0).
// With this encoding, the physical ordering and arithmetic are also correct for
// logical positions. logical_marker is the largest uint32_t value. A physical
// extent_hint is bounded by the size of a chunk, so it never equals the marker.
// The checks that use it run only in debug builds.
class sstable_position {
    // Used by physically indexed compressed sstables.
    struct physical {
        // Start position of the chunk within the file.
        uint64_t chunk_position;
        // A hint of the chunk's extent: its size in Data.db, including the
        // header, the checksum and the footer. The reader uses this value to
        // load the chunk in one I/O operation. The hint has three states:
        //
        // - 0: the extent is not known. The reader gets it from the chunk
        //   header. Positions that don't come from the index (e.g. the
        //   position of the next chunk during a sequential read) can use 0.
        // - From the index: an upper bound of the extent. The index stores the
        //   number of 512-byte blocks that the chunk touches. The blocks are
        //   aligned to offset 0 of Data.db. The hint reaches from
        //   chunk_position to the end of the last block, so it exceeds the
        //   extent by less than 512 bytes. Reads are 512-byte aligned anyway,
        //   so the rounding doesn't make them bigger. Positions from the index
        //   always contain a hint.
        // - After the chunk is loaded: the exact extent.
        //
        // This field does not participate in position ordering.
        //
        // In an encoded logical position, this field is sstable_position_detail::logical_marker
        // instead (see the class comment).
        //
        // This field and offset_within_chunk are bounded by the size of a
        // compression chunk, so 32 bits are enough.
        uint32_t extent_hint;
        // Offset of this position within the chunk after decompression.
        uint32_t offset_within_chunk;
    };

    physical _value;

    explicit sstable_position(physical v) noexcept : _value(v) {}

    bool same_kind(const sstable_position& other) const noexcept {
        return is_physical() == other.is_physical();
    }
public:
    sstable_position(const sstable_position&) = default;
    sstable_position& operator=(const sstable_position&) = default;

    // The ordering is weak: extent_hint does not take part in it, so equal
    // positions can have different hints.
    std::weak_ordering operator<=>(const sstable_position& other) const noexcept {
        sstable_position_detail::debug_check_position_kind(same_kind(other));
        if (auto cmp = _value.chunk_position <=> other._value.chunk_position; cmp != 0) {
            return cmp;
        }
        return _value.offset_within_chunk <=> other._value.offset_within_chunk;
    }
    bool operator==(const sstable_position& other) const noexcept {
        return (*this <=> other) == 0;
    }
    static sstable_position from_logical(uint64_t v) noexcept {
        return sstable_position(physical{v, sstable_position_detail::logical_marker, 0});
    }
    static sstable_position from_physical(uint64_t chunk_position, uint32_t extent_hint, uint32_t offset_within_chunk) noexcept {
        sstable_position_detail::debug_check_position_kind(extent_hint != sstable_position_detail::logical_marker);
        return sstable_position(physical{chunk_position, extent_hint, offset_within_chunk});
    }

    bool is_physical() const noexcept {
        return _value.extent_hint != sstable_position_detail::logical_marker;
    }
    const physical& as_physical() const noexcept {
        sstable_position_detail::debug_check_position_kind(is_physical());
        return _value;
    }
    physical& as_physical() noexcept {
        sstable_position_detail::debug_check_position_kind(is_physical());
        return _value;
    }
    uint64_t to_logical() const noexcept {
        sstable_position_detail::debug_check_position_kind(!is_physical());
        return _value.chunk_position;
    }

    friend sstable_position operator+(sstable_position pos, sstable_position_offset off) noexcept;
    friend sstable_position_offset operator-(sstable_position a, sstable_position b) noexcept;
    friend struct fmt::formatter<sstable_position>;
};

struct disk_read_range {
    sstable_position start;
    sstable_position end;

    disk_read_range(sstable_position start, sstable_position end)
      : start(start)
      , end(end)
    {}
    explicit operator bool() const {
        return start != end;
    }
};

// A position relative to an sstable_position. It usually identifies a row
// relative to the start of its partition.
//
// A logical offset is a plain difference of offsets. In a physical offset, only
// chunk_position is relative. extent_hint and offset_within_chunk are
// copied unchanged from the absolute position (see operator+ and operator-).
//
// The encoding of logical offsets is the same as in sstable_position. The
// physical arithmetic then also computes the logical result.
//
// chunk_position is unsigned, and the arithmetic on it wraps around. A negative
// offset is stored as its two's complement, so adding it to a position still
// gives the correct result.
class sstable_position_offset {
    struct physical {
        uint64_t chunk_position;
        // sstable_position_detail::logical_marker in an encoded logical offset.
        uint32_t extent_hint;
        uint32_t offset_within_chunk;
    };

    physical _value;

    explicit sstable_position_offset(physical v) noexcept : _value(v) {}
public:
    sstable_position_offset(const sstable_position_offset&) = default;
    sstable_position_offset& operator=(const sstable_position_offset&) = default;

    static sstable_position_offset from_logical(uint64_t v) noexcept {
        return sstable_position_offset(physical{v, sstable_position_detail::logical_marker, 0});
    }
    static sstable_position_offset from_physical(uint64_t chunk_position, uint32_t extent_hint, uint32_t offset_within_chunk) noexcept {
        sstable_position_detail::debug_check_position_kind(extent_hint != sstable_position_detail::logical_marker);
        return sstable_position_offset(physical{chunk_position, extent_hint, offset_within_chunk});
    }
    uint64_t to_logical() const noexcept {
        sstable_position_detail::debug_check_position_kind(!holds_physical());
        return _value.chunk_position;
    }
    bool holds_physical() const noexcept {
        return _value.extent_hint != sstable_position_detail::logical_marker;
    }

    const physical& as_physical() const noexcept {
        sstable_position_detail::debug_check_position_kind(holds_physical());
        return _value;
    }
    physical& as_physical() noexcept {
        sstable_position_detail::debug_check_position_kind(holds_physical());
        return _value;
    }

    friend sstable_position operator+(sstable_position pos, sstable_position_offset off) noexcept;
    friend sstable_position_offset operator-(sstable_position a, sstable_position b) noexcept;
    friend struct fmt::formatter<sstable_position_offset>;
};

// For logical positions, these compute the plain sum and difference: the
// offset within chunk is always 0 and the extent_hint is always the marker.
inline sstable_position operator+(sstable_position pos, sstable_position_offset off) noexcept {
    sstable_position_detail::debug_check_position_kind(pos.is_physical() == off.holds_physical());
    // chunk_position is relative to `pos`. This decreases its encoded width.
    // extent_hint and offset_within_chunk are absolute. Relative values
    // for these fields can be negative and do not save space.
    return sstable_position(sstable_position::physical{
        pos._value.chunk_position + off._value.chunk_position,
        off._value.extent_hint,
        off._value.offset_within_chunk,
    });
}

inline sstable_position_offset operator-(sstable_position a, sstable_position b) noexcept {
    sstable_position_detail::debug_check_position_kind(a.same_kind(b));
    return sstable_position_offset(sstable_position_offset::physical{
        a._value.chunk_position - b._value.chunk_position,
        a._value.extent_hint,
        a._value.offset_within_chunk,
    });
}

} // namespace sstables

template <>
struct fmt::formatter<sstables::sstable_position> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    auto format(const sstables::sstable_position& p, fmt::format_context& ctx) const {
        if (!p.is_physical()) {
            return fmt::format_to(ctx.out(), "{}", p._value.chunk_position);
        }
        return fmt::format_to(ctx.out(), "{{chunk_position={}, extent_hint={}, offset_within_chunk={}}}",
            p._value.chunk_position, p._value.extent_hint, p._value.offset_within_chunk);
    }
};

template <>
struct fmt::formatter<sstables::sstable_position_offset> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    auto format(const sstables::sstable_position_offset& o, fmt::format_context& ctx) const {
        if (!o.holds_physical()) {
            return fmt::format_to(ctx.out(), "{}", o._value.chunk_position);
        }
        return fmt::format_to(ctx.out(), "{{chunk_position={}, extent_hint={}, offset_within_chunk={}}}",
            o._value.chunk_position, o._value.extent_hint, o._value.offset_within_chunk);
    }
};
