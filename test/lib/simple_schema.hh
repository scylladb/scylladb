/*
 * Copyright (C) 2017-present ScyllaDB
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
#include "reader_permit.hh"

// Helper for working with the following table:
//
//   CREATE TABLE ks.cf (pk text, ck text, v text, s1 text static, PRIMARY KEY (pk, ck));
//
class simple_schema {
    friend class global_simple_schema;

    schema_ptr _s;
    api::timestamp_type _timestamp = api::min_timestamp;
    const column_definition* _v_def = nullptr;
    table_schema_version _v_def_version;

    simple_schema(schema_ptr s, api::timestamp_type timestamp)
        : _s(s)
        , _timestamp(timestamp)
    {}
private:
    const column_definition& get_v_def(const schema& s) {
        if (_v_def_version == s.version() && _v_def) {
            return *_v_def;
        }
        _v_def = s.get_column_definition(to_bytes("v"));
        _v_def_version = s.version();
        return *_v_def;
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
    { }

    sstring cql() const {
        return "CREATE TABLE ks.cf (pk text, ck text, v text, s1 text static, PRIMARY KEY (pk, ck))";
    }

    clustering_key make_ckey(sstring ck) {
        return clustering_key::from_single_value(*_s, serialized(ck));
    }

    query::clustering_range make_ckey_range(uint32_t start_inclusive, uint32_t end_inclusive) {
        return query::clustering_range::make({make_ckey(start_inclusive)}, {make_ckey(end_inclusive)});
    }

    // Make a clustering_key which is n-th in some arbitrary sequence of keys
    clustering_key make_ckey(uint32_t n) {
        return make_ckey(format("ck{:010d}", n));
    }

    // Make a partition key which is n-th in some arbitrary sequence of keys.
    // There is no particular order for the keys, they're not in ring order.
    dht::decorated_key make_pkey(uint32_t n) {
        return make_pkey(format("pk{:010d}", n));
    }

    dht::decorated_key make_pkey(sstring pk) {
        auto key = partition_key::from_single_value(*_s, serialized(pk));
        return dht::decorate_key(*_s, key);
    }

    api::timestamp_type add_row(mutation& m, const clustering_key& key, const sstring& v, api::timestamp_type t = api::missing_timestamp) {
        if (t == api::missing_timestamp) {
            t = new_timestamp();
        }
        const column_definition& v_def = get_v_def(*_s);
        m.set_clustered_cell(key, v_def, atomic_cell::make_live(*v_def.type, t, serialized(v)));
        return t;
    }

    std::pair<sstring, api::timestamp_type> get_value(const schema& s, const clustering_row& row) {
        const column_definition& v_def = get_v_def(s);
        auto cell = row.cells().find_cell(v_def.id);
        if (!cell) {
            throw std::runtime_error("cell not found");
        }
        atomic_cell_view ac = cell->as_atomic_cell(v_def);
        if (!ac.is_live()) {
            throw std::runtime_error("cell is dead");
        }
        return std::make_pair(value_cast<sstring>(utf8_type->deserialize(ac.value().linearize())), ac.timestamp());
    }

    mutation_fragment make_row(reader_permit permit, const clustering_key& key, sstring v) {
        auto row = clustering_row(key);
        const column_definition& v_def = get_v_def(*_s);
        row.cells().apply(v_def, atomic_cell::make_live(*v_def.type, new_timestamp(), serialized(v)));
        return mutation_fragment(*_s, std::move(permit), std::move(row));
    }

    mutation_fragment make_row_from_serialized_value(reader_permit permit, const clustering_key& key, bytes_view v) {
        auto row = clustering_row(key);
        const column_definition& v_def = get_v_def(*_s);
        row.cells().apply(v_def, atomic_cell::make_live(*v_def.type, new_timestamp(), v));
        return mutation_fragment(*_s, std::move(permit), std::move(row));
    }

    mutation_fragment make_static_row(reader_permit permit, sstring v) {
        static_row row;
        const column_definition& s_def = *_s->get_column_definition(to_bytes("s1"));
        row.cells().apply(s_def, atomic_cell::make_live(*s_def.type, new_timestamp(), serialized(v)));
        return mutation_fragment(*_s, std::move(permit), std::move(row));
    }

    void set_schema(schema_ptr s) {
        _s = s;
    }

    api::timestamp_type add_static_row(mutation& m, sstring s1, api::timestamp_type t = api::missing_timestamp) {
        if (t == api::missing_timestamp) {
            t = new_timestamp();
        }
        m.set_static_cell(to_bytes("s1"), data_value(s1), t);

        return t;
    }

    range_tombstone delete_range(mutation& m, const query::clustering_range& range, tombstone t = {}) {
        auto rt = make_range_tombstone(range, t);
        m.partition().apply_delete(*_s, rt);
        return rt;
    }

    range_tombstone make_range_tombstone(const query::clustering_range& range, tombstone t = {}) {
        auto bv_range = bound_view::from_range(range);
        if (!t) {
            t = new_tombstone();
        }
        range_tombstone rt(bv_range.first, bv_range.second, t);
        return rt;
    }
    range_tombstone make_range_tombstone(const query::clustering_range& range, gc_clock::time_point deletion_time) {
        return make_range_tombstone(range, tombstone(new_timestamp(), deletion_time));
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
