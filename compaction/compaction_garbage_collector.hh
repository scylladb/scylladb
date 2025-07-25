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
//      tombstone.timestamp ∈ (max_purgeable._timestamp, +inf)
//
// The above overlap check can be omitted iff:
//      tombstone.deletion_time ∈ (-inf, max_purgeable._expiry_threshold.value_or(gc_clock::time_point::min()))
//
// So in other words, a tombstone is purgeable iff:
//      tombstone.deletion_time < max_purgeable._expiry_threshold.value_or(gc_clock::time_point::min()) || tombstone.timestamp > max_purgeable._timestamp
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

    api::timestamp_type timestamp() const noexcept { return _timestamp; }
    timestamp_source source() const noexcept { return _source; }
    expiry_threshold_opt expiry_threshold() const noexcept { return _expiry_threshold; }

    static expiry_threshold_opt combine_expiry_tresholds(expiry_threshold_opt a, expiry_threshold_opt b);

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
    // Uses the expiry treshold if available in conjuction to the provided expiry
    // time to avoid the overlap check if possible, increasing the chance of purging.
    // NOTE: checking for purge by
    //      tombstone.timestamp() >= max_purgeable.timestamp()
    // is still correct, but is a stricter check: for all cases where can_purge()
    // returns false, the above will also evaluate to false.
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
