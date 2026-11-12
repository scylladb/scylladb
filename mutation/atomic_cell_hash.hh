/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

// Not part of atomic_cell.hh to avoid cyclic dependency between types.hh and atomic_cell.hh

#include "types/types.hh"
#include "atomic_cell.hh"
#include "atomic_cell_or_collection.hh"
#include "utils/hashing.hh"
#include "counters.hh"

template<>
struct appending_hash<collection_mutation_view> {
    // If max_ts is set, also stores what cell.last_update() returns, saving a second walk.
    template<typename Hasher>
    void operator()(Hasher& h, collection_mutation_view cell, const column_definition& cdef, api::timestamp_type* max_ts = nullptr) const {
        const auto tomb = cell.tomb();
        ::feed_hash(h, tomb);
        api::timestamp_type max = tomb.timestamp;
        cell.for_each_element([&] (managed_bytes_view key, atomic_cell_view value) {
            if (!feed_contiguous(h, key, value.serialize(), max)) {
                ::feed_hash(h, key);
                ::feed_hash(h, value, cdef);
                max = std::max(max, value.timestamp());
            }
            return true;
        });
        if (max_ts) {
            *max_ts = max;
        }
    }

private:
    // Feeds what feed_hash(key) and appending_hash<atomic_cell_view> feed, reading
    // the fields in place: the fragmented accessors dominate hashing large collections.
    template<typename Hasher>
    static bool feed_contiguous(Hasher& h, managed_bytes_view key, managed_bytes_view cell, api::timestamp_type& max) {
        using act = atomic_cell_type;
        const auto k = key.current_fragment();
        const auto c = cell.current_fragment();
        if (k.size() != key.size_bytes() || c.size() != cell.size_bytes() || c.empty()) {
            return false;
        }
        const auto p = reinterpret_cast<const char*>(c.data());
        const auto flags = p[0];
        const bool live = flags & act::LIVE_FLAG;
        const bool expiring = live && (flags & act::EXPIRY_FLAG);
        const size_t fixed = !live ? act::dead_serialized_size() : expiring ? act::live_expiring_serialized_size(0) : act::live_serialized_size(0);
        // Truncated cells take the checked path, which throws.
        if ((flags & act::COUNTER_UPDATE_FLAG) || c.size() < fixed) {
            return false;
        }
        auto time_at = [p] (unsigned offset) {
            return gc_clock::time_point(gc_clock::duration(seastar::read_be<int64_t>(p + offset)));
        };
        ::feed_hash(h, k);
        ::feed_hash(h, live);
        const auto ts = seastar::read_be<api::timestamp_type>(p + act::timestamp_offset);
        ::feed_hash(h, ts);
        max = std::max(max, ts);
        if (live) {
            if (expiring) {
                ::feed_hash(h, time_at(act::expiry_offset));
                ::feed_hash(h, gc_clock::duration(seastar::read_be<int32_t>(p + act::ttl_offset)));
            }
            ::feed_hash(h, c.substr(fixed));
        } else {
            ::feed_hash(h, time_at(act::deletion_time_offset));
        }
        return true;
    }
};

template<>
struct appending_hash<atomic_cell_view> {
    template<typename Hasher>
    void operator()(Hasher& h, atomic_cell_view cell, const column_definition& cdef) const {
        feed_hash(h, cell.is_live());
        feed_hash(h, cell.timestamp());
        if (cell.is_live()) {
            if (cdef.is_counter()) {
                ::feed_hash(h, counter_cell_view(cell));
                return;
            }
            if (cell.is_live_and_has_ttl()) {
                feed_hash(h, cell.expiry());
                feed_hash(h, cell.ttl());
            }
            feed_hash(h, cell.value());
        } else {
            feed_hash(h, cell.deletion_time());
        }
    }
};

template<>
struct appending_hash<atomic_cell> {
    template<typename Hasher>
    void operator()(Hasher& h, const atomic_cell& cell, const column_definition& cdef) const {
        feed_hash(h, static_cast<atomic_cell_view>(cell), cdef);
    }
};

template<>
struct appending_hash<collection_mutation> {
    template<typename Hasher>
    void operator()(Hasher& h, const collection_mutation& cm, const column_definition& cdef) const {
        feed_hash(h, static_cast<collection_mutation_view>(cm), cdef);
    }
};

template<>
struct appending_hash<atomic_cell_or_collection> {
    template<typename Hasher>
    void operator()(Hasher& h, const atomic_cell_or_collection& c, const column_definition& cdef) const {
        if (cdef.is_atomic()) {
            feed_hash(h, c.as_atomic_cell(cdef), cdef);
        } else {
            feed_hash(h, c.as_collection_mutation(), cdef);
        }
    }
};
