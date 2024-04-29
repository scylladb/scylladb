/*
 * Copyright (C) 2017-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <map>

#include "schema/schema.hh"
#include "schema/schema_registry.hh"
#include "keys.hh"
#include "mutation/mutation_fragment.hh"
#include "mutation/mutation.hh"
#include "schema/schema_builder.hh"
#include "reader_permit.hh"
#include "types/map.hh"
#include "test/lib/key_utils.hh"
#include "mutation/atomic_cell_or_collection.hh"

// Helper for working with the following table:
//
//   CREATE TABLE ks.cf (pk text, ck text, v text, s1 text static, PRIMARY KEY (pk, ck));
//
// or
//
//   CREATE TABLE ${ks}.${cf} (pk text, ck text, v text, s1 text static, PRIMARY KEY (pk, ck));
//
// where ks and cf are specified by the constructor.
class simple_schema {
public:
    using with_static = bool_class<class static_tag>;
    using with_collection = bool_class<class collection_tag>;
private:
    friend class global_simple_schema;

    schema_ptr _s = nullptr;
    api::timestamp_type _timestamp = api::min_timestamp;
    const column_definition* _v_def = nullptr;
    table_schema_version _v_def_version;
    with_static _ws;
    with_collection _wc;

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
    static auto get_collection_type() {
        return map_type_impl::get_instance(utf8_type, utf8_type, true);
    }
    simple_schema(std::string_view ks_name, std::string_view cf_name,
                  with_static ws, with_collection wc)
        : _ws(ws)
        , _wc(wc)
    {
        auto sb = schema_builder(ks_name, cf_name)
            .with_column("pk", utf8_type, column_kind::partition_key)
            .with_column("ck", utf8_type, column_kind::clustering_key)
            .with_column("s1", utf8_type, ws ? column_kind::static_column : column_kind::regular_column)
            .with_column("v", utf8_type);
        if (wc) {
            sb.with_column("c1", get_collection_type());
        }
        _s = sb.build();
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
    simple_schema(with_static ws = with_static::yes, with_collection wc = with_collection::no)
        : simple_schema("ks", "cf", ws, wc) {}

    simple_schema(std::string_view ks_name, std::string_view cf_name)
        : simple_schema(ks_name, cf_name, with_static::yes, with_collection::no) {}

    sstring cql() const {
        return format("CREATE TABLE {}.{} (pk text, ck text, v text, s1 text{}{}, PRIMARY KEY (pk, ck))",
                      _s->keypace_name(), _s->element_name(),
                      _ws ? " static" : "", _wc ? ", c1 map<text, text>" : "");
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

    api::timestamp_type add_row_with_dead_cell(mutation& m, const clustering_key& key,
            api::timestamp_type t = api::missing_timestamp, gc_clock::time_point deletion_time = gc_clock::now()) {
        if (t == api::missing_timestamp) {
            t = new_timestamp();
        }
        const column_definition& v_def = get_v_def(*_s);
        m.set_clustered_cell(key, v_def, atomic_cell::make_dead(t, deletion_time));
        return t;
    }

    api::timestamp_type add_row_with_collection(mutation& m, const clustering_key& ck, const std::map<bytes, bytes>& kv_map, api::timestamp_type t = api::missing_timestamp) {
        if (t == api::missing_timestamp) {
            t = new_timestamp();
        }

        collection_mutation_description cmd;
        for (const auto& [k, v] : kv_map) {
            cmd.cells.emplace_back(k, atomic_cell::make_live(*bytes_type, t, v, atomic_cell::collection_member::yes));
        }

        const auto map_type = get_collection_type();
        auto serialized_map = cmd.serialize(*map_type);
        const column_definition& c1_def = *_s->get_column_definition(to_bytes("c1"));
        m.set_clustered_cell(ck, c1_def, atomic_cell_or_collection(std::move(serialized_map)));
        return t;
    }

    api::timestamp_type set_cell(row& r, const sstring& v, api::timestamp_type t = api::missing_timestamp) {
        if (t == api::missing_timestamp) {
            t = new_timestamp();
        }
        const column_definition& v_def = get_v_def(*_s);
        r.apply_monotonically(v_def, atomic_cell::make_live(*v_def.type, t, serialized(v)));
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

    mutation_fragment_v2 make_row_v2(reader_permit permit, const clustering_key& key, sstring v) {
        auto row = clustering_row(key);
        const column_definition& v_def = get_v_def(*_s);
        row.cells().apply(v_def, atomic_cell::make_live(*v_def.type, new_timestamp(), serialized(v)));
        return mutation_fragment_v2(*_s, std::move(permit), std::move(row));
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

    mutation_fragment_v2 make_static_row_v2(reader_permit permit, sstring v) {
        static_row row;
        const column_definition& s_def = *_s->get_column_definition(to_bytes("s1"));
        row.cells().apply(s_def, atomic_cell::make_live(*s_def.type, new_timestamp(), serialized(v)));
        return mutation_fragment_v2(*_s, std::move(permit), std::move(row));
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
        return tests::generate_partition_keys(n, _s);
    }

    dht::decorated_key make_pkey() {
        return tests::generate_partition_key(_s);
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
