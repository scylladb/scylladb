/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <compare>
#include <cstdint>
#include <optional>
#include <utility>
#include <variant>
#include <fmt/format.h>

#include "utils/assert.hh"

namespace sstables {

class sstable_position_offset;

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
class sstable_position {
    // Used by uncompressed and logically indexed sstables.
    struct logical {
        int64_t value;
        auto operator<=>(const logical&) const = default;
        bool operator==(const logical&) const = default;
    };

    // Used by physically indexed compressed sstables.
    struct physical {
        // Start position of the chunk within the file.
        int64_t chunk_position;
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
        int64_t extent_hint;
        // Offset of this position within the chunk after decompression.
        int64_t offset_within_chunk;
    };

    std::variant<logical, physical> _value;

    explicit sstable_position(logical v) noexcept : _value(v) {}
    explicit sstable_position(physical v) noexcept : _value(v) {}
public:
    sstable_position(const sstable_position&) = default;
    sstable_position& operator=(const sstable_position&) = default;

    // The ordering is weak: extent_hint does not take part in it, so equal
    // positions can have different hints.
    std::weak_ordering operator<=>(const sstable_position& other) const noexcept {
        SCYLLA_ASSERT(_value.index() == other._value.index());
        if (auto* a = std::get_if<logical>(&_value)) {
            auto* b = std::get_if<logical>(&other._value);
            return a->value <=> b->value;
        }
        auto* a = std::get_if<physical>(&_value);
        auto* b = std::get_if<physical>(&other._value);
        if (auto cmp = a->chunk_position <=> b->chunk_position; cmp != 0) {
            return cmp;
        }
        return a->offset_within_chunk <=> b->offset_within_chunk;
    }
    bool operator==(const sstable_position& other) const noexcept {
        return (*this <=> other) == 0;
    }
    static sstable_position from_logical(int64_t v) noexcept {
        return sstable_position(logical{v});
    }
    static sstable_position from_physical(int64_t chunk_position, int64_t extent_hint, int64_t offset_within_chunk) noexcept {
        return sstable_position(physical{chunk_position, extent_hint, offset_within_chunk});
    }

    bool is_physical() const noexcept {
        return std::holds_alternative<physical>(_value);
    }
    const physical& as_physical() const {
        SCYLLA_ASSERT(std::holds_alternative<physical>(_value));
        return std::get<physical>(_value);
    }
    physical& as_physical() {
        return const_cast<physical&>(std::as_const(*this).as_physical());
    }
    int64_t to_logical() const noexcept {
        SCYLLA_ASSERT(std::holds_alternative<logical>(_value));
        return std::get<logical>(_value).value;
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
class sstable_position_offset {
    struct logical {
        int64_t value;
    };
    struct physical {
        int64_t chunk_position;
        int64_t extent_hint;
        int64_t offset_within_chunk;
    };

    std::variant<logical, physical> _value;

    explicit sstable_position_offset(logical v) noexcept : _value(v) {}
    explicit sstable_position_offset(physical v) noexcept : _value(v) {}
public:
    sstable_position_offset(const sstable_position_offset&) = default;
    sstable_position_offset& operator=(const sstable_position_offset&) = default;

    static sstable_position_offset from_logical(int64_t v) noexcept {
        return sstable_position_offset(logical{v});
    }
    static sstable_position_offset from_physical(int64_t chunk_position, int64_t extent_hint, int64_t offset_within_chunk) noexcept {
        return sstable_position_offset(physical{chunk_position, extent_hint, offset_within_chunk});
    }
    int64_t to_logical() const noexcept {
        SCYLLA_ASSERT(std::holds_alternative<logical>(_value));
        return std::get<logical>(_value).value;
    }
    bool holds_physical() const noexcept {
        return std::holds_alternative<physical>(_value);
    }

    const physical& as_physical() const {
        SCYLLA_ASSERT(std::holds_alternative<physical>(_value));
        return std::get<physical>(_value);
    }
    physical& as_physical() {
        return const_cast<physical&>(std::as_const(*this).as_physical());
    }

    friend sstable_position operator+(sstable_position pos, sstable_position_offset off) noexcept;
    friend sstable_position_offset operator-(sstable_position a, sstable_position b) noexcept;
    friend struct fmt::formatter<sstable_position_offset>;
};

inline sstable_position operator+(sstable_position pos, sstable_position_offset off) noexcept {
    SCYLLA_ASSERT(pos._value.index() == off._value.index());
    if (auto* p = std::get_if<sstable_position::logical>(&pos._value)) {
        auto* o = std::get_if<sstable_position_offset::logical>(&off._value);
        return sstable_position(sstable_position::logical{p->value + o->value});
    }
    auto* p = std::get_if<sstable_position::physical>(&pos._value);
    auto* o = std::get_if<sstable_position_offset::physical>(&off._value);
    // chunk_position is relative to `pos`. This decreases its encoded width.
    // extent_hint and offset_within_chunk are absolute. Relative values
    // for these fields need signed types and do not save space.
    return sstable_position(sstable_position::physical{
        p->chunk_position + o->chunk_position,
        o->extent_hint,
        o->offset_within_chunk,
    });
}

inline sstable_position_offset operator-(sstable_position a, sstable_position b) noexcept {
    SCYLLA_ASSERT(a._value.index() == b._value.index());
    if (auto* la = std::get_if<sstable_position::logical>(&a._value)) {
        auto* lb = std::get_if<sstable_position::logical>(&b._value);
        return sstable_position_offset(sstable_position_offset::logical{la->value - lb->value});
    }
    auto* pa = std::get_if<sstable_position::physical>(&a._value);
    auto* pb = std::get_if<sstable_position::physical>(&b._value);
    return sstable_position_offset(sstable_position_offset::physical{
        pa->chunk_position - pb->chunk_position,
        pa->extent_hint,
        pa->offset_within_chunk,
    });
}

} // namespace sstables

template <>
struct fmt::formatter<sstables::sstable_position> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    auto format(const sstables::sstable_position& p, fmt::format_context& ctx) const {
        if (auto* l = std::get_if<sstables::sstable_position::logical>(&p._value)) {
            return fmt::format_to(ctx.out(), "{}", l->value);
        }
        auto* ph = std::get_if<sstables::sstable_position::physical>(&p._value);
        return fmt::format_to(ctx.out(), "{{chunk_position={}, extent_hint={}, offset_within_chunk={}}}",
            ph->chunk_position, ph->extent_hint, ph->offset_within_chunk);
    }
};

template <>
struct fmt::formatter<sstables::sstable_position_offset> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    auto format(const sstables::sstable_position_offset& o, fmt::format_context& ctx) const {
        if (auto* l = std::get_if<sstables::sstable_position_offset::logical>(&o._value)) {
            return fmt::format_to(ctx.out(), "{}", l->value);
        }
        auto* ph = std::get_if<sstables::sstable_position_offset::physical>(&o._value);
        return fmt::format_to(ctx.out(), "{{chunk_position={}, extent_hint={}, offset_within_chunk={}}}",
            ph->chunk_position, ph->extent_hint, ph->offset_within_chunk);
    }
};
