/*
 * Copyright (C) 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <seastar/util/bool_class.hh>

#include "mutation/tombstone.hh"
#include "schema/schema_fwd.hh"
#include "dht/i_partitioner_fwd.hh"

using is_shadowable = bool_class<struct is_shadowable_tag>;

// Determines whether tombstone may be GC-ed.
using can_gc_fn = std::function<bool(tombstone, is_shadowable)>;

extern can_gc_fn always_gc;
extern can_gc_fn never_gc;

// For the purposes of overlap with live data, a tombstone is purgeable if:
//      tombstone.timestamp ∈ (-inf, max_purgeable._timestamp)
//
// The above overlap check can be omitted iff:
//      tombstone.deletion_time ∈ (-inf, max_purgeable._expiry_threshold.value_or(gc_clock::time_point::min()))
//
// So in other words, a tombstone is purgeable iff:
//      tombstone.deletion_time < max_purgeable._expiry_threshold.value_or(gc_clock::time_point::min()) || tombstone.timestamp < max_purgeable._timestamp
//
// See can_purge() for more details.
class max_purgeable {
public:
    enum class timestamp_source {
        none,
        memtable_possibly_shadowing_data,
        other_sstables_possibly_shadowing_data
    };

    using expiry_threshold_opt = std::optional<gc_clock::time_point>;

private:
    api::timestamp_type _timestamp { api::missing_timestamp };
    expiry_threshold_opt _expiry_threshold;
    timestamp_source _source { timestamp_source::none };

public:
    max_purgeable() = default;
    explicit max_purgeable(api::timestamp_type timestamp, timestamp_source source = timestamp_source::none)
        : _timestamp(timestamp), _source(source)
    { }
    explicit max_purgeable(api::timestamp_type timestamp, expiry_threshold_opt expiry_threshold, timestamp_source source = timestamp_source::none)
        : _timestamp(timestamp), _expiry_threshold(expiry_threshold), _source(source)
    { }

    operator bool() const { return _timestamp != api::missing_timestamp; }
    bool operator==(const max_purgeable&) const = default;
    bool operator!=(const max_purgeable&) const = default;

    api::timestamp_type timestamp() const noexcept { return _timestamp; }
    expiry_threshold_opt expiry_threshold() const noexcept { return _expiry_threshold; }
    timestamp_source source() const noexcept { return _source; }

    max_purgeable& combine(max_purgeable other);

    struct can_purge_result {
        bool can_purge { true };
        timestamp_source timestamp_source { timestamp_source::none };

        // can purge?
        operator bool() const noexcept {
            return can_purge;
        }
        bool operator!() const noexcept {
            return !can_purge;
        }
    };

    // Determines whether the tombstone can be purged.
    //
    // If available, the expiry threshold is used to maybe elide the overlap
    // check against the min live timestamp. The overlap check elision is
    // possible if the tombstone's deletion time is < than the expiry threshold
    // or in other words: the tombstone was already expired when the data
    // source(s) represented by this max_purgeable were created. Consequently,
    // all writes in these data sources arrived *after* the tombstone was already
    // expired and hence it is not relevant to these writes, even if they
    // otherwise overlap with the tombstone's timestamp.
    //
    // The overlap check elision is an optimization, checking whether a tombstone
    // can be purged by just looking at the timestamps is still correct (but
    // stricter).
    can_purge_result can_purge(tombstone) const;
};

template <>
struct fmt::formatter<max_purgeable::timestamp_source> : fmt::formatter<string_view> {
    auto format(max_purgeable::timestamp_source, fmt::format_context& ctx) const -> decltype(ctx.out());
};

template <>
struct fmt::formatter<max_purgeable> : fmt::formatter<string_view> {
    auto format(max_purgeable, fmt::format_context& ctx) const -> decltype(ctx.out());
};

using max_purgeable_fn = std::function<max_purgeable(const dht::decorated_key&, is_shadowable)>;

extern max_purgeable_fn can_always_purge;
extern max_purgeable_fn can_never_purge;

class atomic_cell;
class row_marker;
struct collection_mutation_description;

class compaction_garbage_collector {
public:
    virtual ~compaction_garbage_collector() = default;
    virtual void collect(column_id id, atomic_cell) = 0;
    virtual void collect(column_id id, collection_mutation_description) = 0;
    virtual void collect(row_marker) = 0;
};
