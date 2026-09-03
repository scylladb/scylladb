/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "dht/token.hh"
#include "mutation/tombstone.hh"
#include "utils/chunked_vector.hh"

#include <algorithm>
#include <compare>
#include <initializer_list>
#include <ranges>

class mutation;
class mutation_partition;

// A tombstone which deletes all partitions whose token falls into the
// token interval (start, end].
//
// The interval is open on the left and closed on the right so that a single
// token_range_tombstone can exactly describe a vnode or a tablet, whose ranges
// have the same shape. The whole ring is described by
// (dht::token::minimum(), dht::token::maximum()].
//
// Note that unlike a range_tombstone, which lives inside a single partition and
// deletes a range of clustering keys, a token_range_tombstone lives above the
// partition level and deletes whole partitions. It commutes with everything
// else in mutation algebra: applying it to a partition whose token it covers is
// the same as applying a partition tombstone.
class token_range_tombstone {
    // Exclusive.
    dht::token _start;
    // Inclusive.
    dht::token _end;
    ::tombstone _tomb;
public:
    token_range_tombstone() = default;
    token_range_tombstone(dht::token start, dht::token end, ::tombstone tomb) noexcept
        : _start(std::move(start))
        , _end(std::move(end))
        , _tomb(tomb)
    { }

    // Returns a tombstone covering the whole ring.
    static token_range_tombstone full_ring(::tombstone tomb) noexcept {
        return token_range_tombstone(dht::token::minimum(), dht::token::maximum(), tomb);
    }

    // The first token which is *not* covered, all covered tokens are greater than it.
    const dht::token& start_exclusive() const noexcept { return _start; }
    // The last token which is covered.
    const dht::token& end_inclusive() const noexcept { return _end; }
    const ::tombstone& tomb() const noexcept { return _tomb; }

    void set_start_exclusive(dht::token t) noexcept { _start = std::move(t); }
    void set_end_inclusive(dht::token t) noexcept { _end = std::move(t); }
    void set_tombstone(::tombstone t) noexcept { _tomb = t; }

    // True iff the tombstone has no effect, either because it covers no token
    // or because it carries no deletion.
    bool empty() const noexcept { return !(_start < _end) || !_tomb; }

    bool contains(const dht::token& t) const noexcept {
        return _start < t && t <= _end;
    }

    // True iff the two tombstones cover a common token.
    bool overlaps(const token_range_tombstone& other) const noexcept {
        return _start < other._end && other._start < _end;
    }

    // True iff the two tombstones cover a common token or are contiguous,
    // so that their union is a single token range.
    bool overlaps_or_touches(const token_range_tombstone& other) const noexcept {
        return _start <= other._end && other._start <= _end;
    }

    size_t memory_usage() const noexcept { return sizeof(token_range_tombstone); }

    std::strong_ordering operator<=>(const token_range_tombstone&) const noexcept = default;
    bool operator==(const token_range_tombstone&) const noexcept = default;
};

template <>
struct fmt::formatter<token_range_tombstone> : fmt::formatter<string_view> {
    auto format(const token_range_tombstone& rt, fmt::format_context& ctx) const {
        return fmt::format_to(ctx.out(), "{{token_range_tombstone: ({}, {}], {}}}",
                              rt.start_exclusive(), rt.end_inclusive(), rt.tomb());
    }
};

// An ordered collection of token_range_tombstone:s.
//
// Maintains the following invariants:
//   - entries are sorted by their start token,
//   - entries are non-empty,
//   - entries neither overlap nor touch, unless they carry different tombstones.
//
// Because tombstone::apply() picks the newer of two tombstones, apply() on this
// list is commutative, associative and idempotent, which makes the list a part
// of mutation algebra: it can be merged in any order, any number of times, and
// the result of a read does not depend on how the writes were grouped.
class token_range_tombstone_list final {
public:
    using container_type = utils::chunked_vector<token_range_tombstone>;
    using iterator = container_type::iterator;
    using const_iterator = container_type::const_iterator;
private:
    container_type _tombstones;
public:
    token_range_tombstone_list() = default;
    token_range_tombstone_list(std::initializer_list<token_range_tombstone>);

    auto begin() const noexcept { return _tombstones.begin(); }
    auto end() const noexcept { return _tombstones.end(); }
    auto begin() noexcept { return _tombstones.begin(); }
    auto end() noexcept { return _tombstones.end(); }
    size_t size() const noexcept { return _tombstones.size(); }
    bool empty() const noexcept { return _tombstones.empty(); }
    void clear() noexcept { _tombstones.clear(); }

    // Merges the given tombstone into this list. Where the two overlap the
    // newer tombstone wins. Applying an empty tombstone is a no-op.
    void apply(const token_range_tombstone& rt);
    void apply(dht::token start, dht::token end, tombstone tomb) {
        apply(token_range_tombstone(std::move(start), std::move(end), tomb));
    }
    // Merges another list into this one. Equivalent to applying each of its
    // entries in turn, but cheaper.
    void apply(const token_range_tombstone_list& list);

    // Returns the tombstone covering the given token, or an empty tombstone if
    // the token is not covered by this list.
    tombstone search(const dht::token& t) const noexcept;

    // Returns the newest tombstone in the list.
    tombstone max_tombstone() const noexcept;

    // Applies the tombstone covering the mutation's token, if any, to the
    // mutation's partition. This is the point at which a token range tombstone
    // becomes an ordinary partition tombstone.
    void apply_to(mutation& m) const;

    // Returns the subset of this list restricted to the token range (start, end].
    token_range_tombstone_list slice(const dht::token& start, const dht::token& end) const;

    // Erases the tombstones for which the predicate returns true.
    template <typename Pred>
    requires std::is_invocable_r_v<bool, Pred, const token_range_tombstone&>
    void erase_where(Pred filter) {
        _tombstones.erase(std::remove_if(_tombstones.begin(), _tombstones.end(), filter), _tombstones.end());
    }

    // Erases tombstones which were deleted before the given time point, and are
    // therefore past their grace period and can no longer resurrect anything.
    void purge(gc_clock::time_point before) {
        erase_where([before] (const token_range_tombstone& rt) {
            return rt.tomb().deletion_time < before;
        });
    }

    bool equal(const token_range_tombstone_list& other) const noexcept {
        return std::ranges::equal(_tombstones, other._tombstones);
    }
    bool operator==(const token_range_tombstone_list& other) const noexcept { return equal(other); }

    size_t external_memory_usage() const noexcept { return _tombstones.external_memory_usage(); }
    size_t memory_usage() const noexcept { return sizeof(*this) + external_memory_usage(); }
};

template <>
struct fmt::formatter<token_range_tombstone_list> : fmt::formatter<string_view> {
    auto format(const token_range_tombstone_list& list, fmt::format_context& ctx) const {
        return fmt::format_to(ctx.out(), "{{{}}}", fmt::join(list, ", "));
    }
};
