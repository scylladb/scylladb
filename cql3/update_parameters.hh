/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.1 and Apache-2.0)
 */

#pragma once

#include <unordered_map>

#include "gc_clock.hh"
#include "mutation/timestamp.hh"
#include "schema/schema_fwd.hh"
#include "mutation/atomic_cell.hh"
#include "mutation/tombstone.hh"
#include "exceptions/exceptions.hh"
#include "cql3/query_options.hh"
#include "cql3/selection/selection.hh"
#include "query/query-request.hh"
#include "query/query-result.hh"
#include "utils/hash.hh"

namespace cql3 {

/**
 * A simple container that simplify passing parameters for collections methods.
 */
class update_parameters final {
public:
    // Option set for partition_slice to be used when fetching prefetch_data
    static constexpr query::partition_slice::option_set options = query::partition_slice::option_set::of<
        query::partition_slice::option::send_partition_key,
        query::partition_slice::option::send_clustering_key,
        query::partition_slice::option::collections_as_maps>();

    // Holder for data for
    // 1) CQL list updates which depend on current state of the list
    // 2) cells needed to check conditions of a CAS statement,
    // 3) rows of CAS result set.
    struct prefetch_data {
        using key = std::pair<partition_key, clustering_key>;
        struct key_hash {
            partition_key::hashing pk_hash;
            clustering_key::hashing ck_hash;

            key_hash(const schema& s)
                : pk_hash(s)
                , ck_hash(s)
            { }
            size_t operator()(const key& k) const {
                return utils::hash_combine(pk_hash(k.first), ck_hash(k.second));
            }
        };
        struct key_equal {
            partition_key::equality pk_eq;
            clustering_key::equality ck_eq;

            key_equal(const schema& s)
                : pk_eq(s)
                , ck_eq(s)
            { }
            bool operator()(const key& k1, const key& k2) const {
                return pk_eq(k1.first, k2.first) && ck_eq(k1.second, k2.second);
            }
        };
    public:
        struct row {
            bool has_static;
            // indexes are determined by prefetch_data::selection
            std::vector<managed_bytes_opt> cells;
            // Return true if this row has at least one static column set.
            bool has_static_columns(const schema& schema) const {
                return has_static;
            }
        };
        // Rows are only ever looked up by their exact full primary key
        // (see find_row()), the container is never iterated, so an unordered
        // map is enough. Note that the CAS result set is ordered by iterating
        // the statements of the CAS request, not this map.
        std::unordered_map<key, row, key_hash, key_equal> rows;
        schema_ptr schema;
        shared_ptr<cql3::selection::selection> selection;
    public:
        prefetch_data(schema_ptr schema);
        // Find a row object for either static or regular subset of cells, depending
        // on whether clustering key is empty or not.
        // A particular cell within the row can then be found using a column id.
        const row* find_row(const partition_key& pkey, const clustering_key& ckey) const;
    };
    // Note: value (mutation) only required to contain the rows we are interested in
private:
    const std::optional<gc_clock::duration> _ttl;
    // For operations that require a read-before-write, stores prefetched cell values.
    // For CAS statements, stores values of conditioned columns.
    // Is a reference to an outside prefetch_data container since a CAS BATCH statement
    // prefetches all rows at once, for all its nested modification statements.
    const prefetch_data& _prefetched;
public:
    const api::timestamp_type _timestamp;
    const gc_clock::time_point _local_deletion_time;
    const schema_ptr _schema;
    const query_options& _options;

    update_parameters(const schema_ptr schema_, const query_options& options,
            api::timestamp_type timestamp, std::optional<gc_clock::duration> ttl, const prefetch_data& prefetched)
        : _ttl(ttl)
        , _prefetched(prefetched)
        , _timestamp(timestamp)
        , _local_deletion_time(gc_clock::now())
        , _schema(std::move(schema_))
        , _options(options)
    {
        // We use MIN_VALUE internally to mean the absence of of timestamp (in Selection, in sstable stats, ...), so exclude
        // it to avoid potential confusion.
        if (timestamp < api::min_timestamp || timestamp > api::max_timestamp) {
            throw exceptions::invalid_request_exception(format("Out of bound timestamp, must be in [{:d}, {:d}]",
                    api::min_timestamp, api::max_timestamp));
        }
    }

    atomic_cell make_dead_cell() const {
        return atomic_cell::make_dead(_timestamp, _local_deletion_time);
    }

    atomic_cell make_cell(const abstract_type& type, const raw_value_view& value, atomic_cell::collection_member cm = atomic_cell::collection_member::no) const {
        auto ttl = this->ttl();

        return value.with_value([&] (const FragmentedView auto& v) {
            if (ttl.count() > 0) {
                return atomic_cell::make_live(type, _timestamp, v, _local_deletion_time + ttl, ttl, cm);
            } else {
                return atomic_cell::make_live(type, _timestamp, v, cm);
            }
        });
    };

    atomic_cell make_cell(const abstract_type& type, const managed_bytes_view& value, atomic_cell::collection_member cm = atomic_cell::collection_member::no) const {
        auto ttl = this->ttl();

        if (ttl.count() > 0) {
            return atomic_cell::make_live(type, _timestamp, value, _local_deletion_time + ttl, ttl, cm);
        } else {
            return atomic_cell::make_live(type, _timestamp, value, cm);
        }
    };

    atomic_cell make_counter_update_cell(int64_t delta) const {
        return atomic_cell::make_live_counter_update(_timestamp, delta);
    }

    tombstone make_tombstone() const {
        return {_timestamp, _local_deletion_time};
    }

    tombstone make_tombstone_just_before() const {
        return {_timestamp - 1, _local_deletion_time};
    }

    gc_clock::duration ttl() const {
        return _ttl.value_or(_schema->default_time_to_live());
    }

    gc_clock::time_point expiry() const {
        return ttl() + _local_deletion_time;
    }

    api::timestamp_type timestamp() const {
        return _timestamp;
    }

    std::optional<std::vector<std::pair<data_value, data_value>>>
    get_prefetched_list(const partition_key& pkey, const clustering_key& ckey, const column_definition& column) const;

    // Evaluates expr against the prefetched row identified by (pkey, ckey).
    // For efficiency, call this only when the expression references regular or
    // static columns; for expressions that don't, call evaluate() directly.
    cql3::raw_value evaluate_on_prefetched_row(
        const expr::expression& expr, const partition_key& pkey, const clustering_key& ckey) const;

    static prefetch_data build_prefetch_data(schema_ptr schema, const query::result& query_result,
            const query::partition_slice& slice);
};

}
