/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#ifndef DATABASE_HH_
#define DATABASE_HH_

#include "dht/i_partitioner.hh"
#include "core/sstring.hh"
#include "core/shared_ptr.hh"
#include "net/byteorder.hh"
#include "utils/UUID.hh"
#include "db_clock.hh"
#include "gc_clock.hh"
#include <functional>
#include <boost/any.hpp>
#include <cstdint>
#include <boost/variant.hpp>
#include <unordered_map>
#include <map>
#include <set>
#include <vector>
#include <iostream>
#include <boost/functional/hash.hpp>
#include <experimental/optional>
#include <string.h>
#include "types.hh"
#include "tuple.hh"
#include "core/future.hh"
#include "cql3/column_specification.hh"
#include <limits>
#include <cstddef>
#include "schema.hh"
#include "timestamp.hh"
#include "tombstone.hh"
#include "atomic_cell.hh"
#include "bytes.hh"

using partition_key_type = tuple_type<>;
using clustering_key_type = tuple_type<>;
using clustering_prefix_type = tuple_prefix;
using partition_key = bytes;
using clustering_key = bytes;
using clustering_prefix = clustering_prefix_type::value_type;

using row = std::map<column_id, atomic_cell_or_collection>;

struct deletable_row final {
    tombstone t;
    row cells;
};

using row_tombstone_set = std::map<bytes, tombstone, serialized_compare>;

class mutation_partition final {
private:
    tombstone _tombstone;
    row _static_row;
    std::map<clustering_key, deletable_row, key_compare> _rows;
    row_tombstone_set _row_tombstones;
public:
    mutation_partition(schema_ptr s)
        : _rows(key_compare(s->clustering_key_type))
        , _row_tombstones(serialized_compare(s->clustering_key_prefix_type))
    { }
    void apply(tombstone t) { _tombstone.apply(t); }
    void apply_delete(schema_ptr schema, const clustering_prefix& prefix, tombstone t);
    void apply_row_tombstone(schema_ptr schema, bytes prefix, tombstone t) {
        apply_row_tombstone(schema, {std::move(prefix), std::move(t)});
    }
    void apply_row_tombstone(schema_ptr schema, std::pair<bytes, tombstone> row_tombstone);
    void apply(schema_ptr schema, const mutation_partition& p);
    const row_tombstone_set& row_tombstones() const { return _row_tombstones; }
    row& static_row() { return _static_row; }
    row& clustered_row(const clustering_key& key) { return _rows[key].cells; }
    row& clustered_row(clustering_key&& key) { return _rows[std::move(key)].cells; }
    row* find_row(const clustering_key& key);
    tombstone tombstone_for_row(schema_ptr schema, const clustering_key& key);
    friend std::ostream& operator<<(std::ostream& os, const mutation_partition& mp);
};

class mutation final {
public:
    schema_ptr schema;
    partition_key key;
    mutation_partition p;
public:
    mutation(partition_key key_, schema_ptr schema_)
        : schema(std::move(schema_))
        , key(std::move(key_))
        , p(schema)
    { }

    mutation(mutation&&) = default;
    mutation(const mutation&) = default;

    void set_static_cell(const column_definition& def, atomic_cell_or_collection value) {
        update_column(p.static_row(), def, std::move(value));
    }

    void set_clustered_cell(const clustering_prefix& prefix, const column_definition& def, atomic_cell_or_collection value) {
        auto& row = p.clustered_row(serialize_value(*schema->clustering_key_type, prefix));
        update_column(row, def, std::move(value));
    }

    void set_clustered_cell(const clustering_key& key, const column_definition& def, atomic_cell_or_collection value) {
        auto& row = p.clustered_row(key);
        update_column(row, def, std::move(value));
    }
private:
    static void update_column(row& row, const column_definition& def, atomic_cell_or_collection&& value) {
        // our mutations are not yet immutable
        auto id = def.id;
        auto i = row.lower_bound(id);
        if (i == row.end() || i->first != id) {
            row.emplace_hint(i, id, std::move(value));
        } else {
            merge_column(def, i->second, value);
        }
    }
    friend std::ostream& operator<<(std::ostream& os, const mutation& m);
};

struct column_family {
    column_family(schema_ptr schema);
    mutation_partition& find_or_create_partition(const bytes& key);
    row& find_or_create_row(const bytes& partition_key, const bytes& clustering_key);
    mutation_partition* find_partition(const bytes& key);
    row* find_row(const bytes& partition_key, const bytes& clustering_key);
    schema_ptr _schema;
    // partition key -> partition
    std::map<bytes, mutation_partition, key_compare> partitions;
    void apply(const mutation& m);
};

class keyspace {
public:
    std::unordered_map<sstring, column_family> column_families;
    static future<keyspace> populate(sstring datadir);
    schema_ptr find_schema(const sstring& cf_name);
    column_family* find_column_family(const sstring& cf_name);
};

// Policy for distributed<database>:
//   broadcast metadata writes
//   local metadata reads
//   use shard_of() for data

class database {
public:
    std::unordered_map<sstring, keyspace> keyspaces;
    future<> init_from_data_directory(sstring datadir);
    static future<database> populate(sstring datadir);
    keyspace* find_keyspace(const sstring& name);
    future<> stop() { return make_ready_future<>(); }
    void assign(database&& db) {
        *this = std::move(db);
    }
    unsigned shard_of(const dht::token& t);
};

// FIXME: stub
class secondary_index_manager {};

#endif /* DATABASE_HH_ */
