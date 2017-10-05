/*
 * Copyright (C) 2017 ScyllaDB
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

#include "schema.hh"
#include "schema_registry.hh"
#include "keys.hh"
#include "mutation_fragment.hh"
#include "mutation.hh"
#include "schema_builder.hh"
#include "sstable_utils.hh"

// Helper for working with the following table:
//
//   CREATE TABLE ks.cf (pk text, ck text, v text, s1 text static, PRIMARY KEY (pk, ck));
//
class simple_schema {
    friend class global_simple_schema;

    schema_ptr _s;
    api::timestamp_type _timestamp = api::min_timestamp;
    const column_definition& _v_def;

    simple_schema(schema_ptr s, api::timestamp_type timestamp)
        : _s(s)
        , _timestamp(timestamp)
        , _v_def(*_s->get_column_definition(to_bytes("v"))) {
    }
public:
    api::timestamp_type current_timestamp() {
        return _timestamp;
    }
    api::timestamp_type new_timestamp() {
        return _timestamp++;
    }
    tombstone new_tombstone() {
        return {new_timestamp(), gc_clock::now()};
    }
public:
    using with_static = bool_class<class static_tag>;
    simple_schema(with_static ws = with_static::yes)
        : _s(schema_builder("ks", "cf")
            .with_column("pk", utf8_type, column_kind::partition_key)
            .with_column("ck", utf8_type, column_kind::clustering_key)
            .with_column("s1", utf8_type, ws ? column_kind::static_column : column_kind::regular_column)
            .with_column("v", utf8_type)
            .build())
        , _v_def(*_s->get_column_definition(to_bytes("v")))
    { }

    sstring cql() const {
        return "CREATE TABLE ks.cf (pk text, ck text, v text, s1 text static, PRIMARY KEY (pk, ck))";
    }

    clustering_key make_ckey(sstring ck) {
        return clustering_key::from_single_value(*_s, data_value(ck).serialize());
    }

    query::clustering_range make_ckey_range(uint32_t start_inclusive, uint32_t end_inclusive) {
        return query::clustering_range::make({make_ckey(start_inclusive)}, {make_ckey(end_inclusive)});
    }

    // Make a clustering_key which is n-th in some arbitrary sequence of keys
    clustering_key make_ckey(uint32_t n) {
        return make_ckey(sprint("ck%010d", n));
    }

    // Make a partition key which is n-th in some arbitrary sequence of keys.
    // There is no particular order for the keys, they're not in ring order.
    dht::decorated_key make_pkey(uint32_t n) {
        return make_pkey(sprint("pk%010d", n));
    }

    dht::decorated_key make_pkey(sstring pk) {
        auto key = partition_key::from_single_value(*_s, data_value(pk).serialize());
        return dht::global_partitioner().decorate_key(*_s, key);
    }

    api::timestamp_type add_row(mutation& m, const clustering_key& key, const sstring& v, api::timestamp_type t = api::missing_timestamp) {
        if (t == api::missing_timestamp) {
            t = new_timestamp();
        }
        m.set_clustered_cell(key, _v_def, atomic_cell::make_live(*_v_def.type, t, data_value(v).serialize()));
        return t;
    }

    std::pair<sstring, api::timestamp_type> get_value(const clustering_row& row) {
        auto cell = row.cells().find_cell(_v_def.id);
        if (!cell) {
            throw std::runtime_error("cell not found");
        }
        atomic_cell_view ac = cell->as_atomic_cell(_v_def);
        if (!ac.is_live()) {
            throw std::runtime_error("cell is dead");
        }
        return std::make_pair(value_cast<sstring>(utf8_type->deserialize(ac.value().linearize())), ac.timestamp());
    }

    mutation_fragment make_row(const clustering_key& key, sstring v) {
        auto row = clustering_row(key);
        row.cells().apply(*_s->get_column_definition(to_bytes(sstring("v"))),
            atomic_cell::make_live(*_v_def.type, new_timestamp(), data_value(v).serialize()));
        return mutation_fragment(std::move(row));
    }

    mutation_fragment make_row_from_serialized_value(const clustering_key& key, bytes_view v) {
        auto row = clustering_row(key);
        row.cells().apply(_v_def, atomic_cell::make_live(*_v_def.type, new_timestamp(), v));
        return mutation_fragment(std::move(row));
    }

    api::timestamp_type add_static_row(mutation& m, sstring s1, api::timestamp_type t = api::missing_timestamp) {
        if (t == api::missing_timestamp) {
            t = new_timestamp();
        }
        m.set_static_cell(to_bytes("s1"), data_value(s1), t);

        return t;
    }

    range_tombstone delete_range(mutation& m, const query::clustering_range& range) {
        auto rt = make_range_tombstone(range);
        m.partition().apply_delete(*_s, rt);
        return rt;
    }

    range_tombstone make_range_tombstone(const query::clustering_range& range, tombstone t = {}) {
        auto bv_range = bound_view::from_range(range);
        if (!t) {
            t = tombstone(new_timestamp(), gc_clock::now());
        }
        range_tombstone rt(bv_range.first, bv_range.second, t);
        return rt;
    }

    mutation new_mutation(sstring pk) {
        return mutation(_s, make_pkey(pk));
    }

    schema_ptr schema() {
        return _s;
    }

    const schema_ptr schema() const {
        return _s;
    }

    // Creates a sequence of keys in ring order
    std::vector<dht::decorated_key> make_pkeys(int n) {
        auto local_keys = make_local_keys(n, _s);
        return boost::copy_range<std::vector<dht::decorated_key>>(local_keys | boost::adaptors::transformed([this] (sstring& key) {
            return make_pkey(std::move(key));
        }));
    }

    dht::decorated_key make_pkey() {
        return make_pkey(make_local_key(_s));
    }

    static std::vector<dht::ring_position> to_ring_positions(const std::vector<dht::decorated_key>& keys) {
        return boost::copy_range<std::vector<dht::ring_position>>(keys | boost::adaptors::transformed([] (const dht::decorated_key& key) {
            return dht::ring_position(key);
        }));
    }

    // Returns n clustering keys in their natural order
    std::vector<clustering_key> make_ckeys(int n) {
        std::vector<clustering_key> keys;
        for (int i = 0; i < n; ++i) {
            keys.push_back(make_ckey(i));
        }
        return keys;
    }
};

// Allows a simple_schema to be transferred to another shard.
// Must be used in `cql_test_env`.
class global_simple_schema {
    global_schema_ptr _gs;
    api::timestamp_type _timestamp;
public:

    global_simple_schema(simple_schema& s)
        : _gs(s.schema())
        , _timestamp(s.current_timestamp()) {
    }

    simple_schema get() const {
        return simple_schema(_gs.get(), _timestamp);
    }
};
