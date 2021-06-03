/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "gc_clock.hh"
#include "timestamp.hh"
#include "schema_fwd.hh"
#include "atomic_cell.hh"
#include "tombstone.hh"
#include "exceptions/exceptions.hh"
#include "cql3/query_options.hh"
#include "query-request.hh"
#include "query-result.hh"

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
        using key_view = std::pair<const partition_key&, const clustering_key&>;
        struct key_less {
            partition_key::tri_compare pk_cmp;
            clustering_key::tri_compare ck_cmp;

            key_less(const schema& s)
                : pk_cmp(s)
                , ck_cmp(s)
            { }
            std::strong_ordering tri_compare(const partition_key& pk1, const clustering_key& ck1,
                    const partition_key& pk2, const clustering_key& ck2) const {

                std::strong_ordering rc = pk_cmp(pk1, pk2);
                return rc != 0 ? rc : ck_cmp(ck1, ck2) <=> 0;
            }
            // Allow mixing std::pair<partition_key, clustering_key> and
            // std::pair<const partition_key&, const clustering_key&> during lookup
            template <typename Pair1, typename Pair2>
            bool operator()(const Pair1& k1, const Pair2& k2) const {
                return  tri_compare(k1.first, k1.second, k2.first, k2.second) < 0;
            }
        };
    public:
        struct row {
            // Order CAS columns by ordinal column id.
            std::map<ordinal_column_id, data_value> cells;
            // Return true if this row has at least one static column set.
            bool has_static_columns(const schema& schema) const {
                if (!schema.has_static_columns()) {
                    return false;
                }
                // Static columns use a continuous range of ids so to efficiently check
                // if a row has a static cell, we can look up the first cell with id >=
                // first static column id and check if it's static.
                auto it = cells.lower_bound(schema.static_begin()->ordinal_id);
                if (it == cells.end() || !schema.column_at(it->first).is_static()) {
                    return false;
                }
                return true;
            }
        };
        // Use an ordered map since CAS result set must be naturally ordered
        // when returned to the client.
        std::map<key, row, key_less> rows;
        schema_ptr schema;
    public:
        prefetch_data(schema_ptr schema);
        // Find a row object for either static or regular subset of cells, depending
        // on whether clustering key is empty or not.
        // A particular cell within the row can then be found using a column id.
        const row* find_row(const partition_key& pkey, const clustering_key& ckey) const;
    };
    // Note: value (mutation) only required to contain the rows we are interested in
private:
    const gc_clock::duration _ttl;
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
            api::timestamp_type timestamp, gc_clock::duration ttl, const prefetch_data& prefetched)
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
        auto ttl = _ttl;

        if (ttl.count() <= 0) {
            ttl = _schema->default_time_to_live();
        }

        return value.with_value([&] (const FragmentedView auto& v) {
            if (ttl.count() > 0) {
                return atomic_cell::make_live(type, _timestamp, v, _local_deletion_time + ttl, ttl, cm);
            } else {
                return atomic_cell::make_live(type, _timestamp, v, cm);
            }
        });
    };

    atomic_cell make_cell(const abstract_type& type, const managed_bytes_view& value, atomic_cell::collection_member cm = atomic_cell::collection_member::no) const {
        auto ttl = _ttl;

        if (ttl.count() <= 0) {
            ttl = _schema->default_time_to_live();
        }

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
        return _ttl.count() > 0 ? _ttl : _schema->default_time_to_live();
    }

    gc_clock::time_point expiry() const {
        return ttl() + _local_deletion_time;
    }

    api::timestamp_type timestamp() const {
        return _timestamp;
    }

    const std::vector<std::pair<data_value, data_value>>*
    get_prefetched_list(const partition_key& pkey, const clustering_key& ckey, const column_definition& column) const;

    static prefetch_data build_prefetch_data(schema_ptr schema, const query::result& query_result,
            const query::partition_slice& slice);
};

}
