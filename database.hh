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
#include "query.hh"
#include "keys.hh"
#include <boost/intrusive/set.hpp>

using row = std::map<column_id, atomic_cell_or_collection>;

struct deletable_row final {
    tombstone t;
    row cells;

    void apply(tombstone t_) {
        t.apply(t_);
    }
};

class row_tombstones_entry : public boost::intrusive::set_base_hook<> {
    clustering_key::prefix::one _prefix;
    tombstone _t;
public:
    row_tombstones_entry(clustering_key::prefix::one&& prefix, tombstone t)
        : _prefix(std::move(prefix))
        , _t(std::move(t))
    { }
    clustering_key::prefix::one& prefix() {
        return _prefix;
    }
    const clustering_key::prefix::one& prefix() const {
        return _prefix;
    }
    tombstone& t() {
        return _t;
    }
    const tombstone& t() const {
        return _t;
    }
    void apply(tombstone t) {
        _t.apply(t);
    }
    struct compare {
        clustering_key::prefix::one::less_compare _c;
        compare(const schema& s) : _c(s) {}
        bool operator()(const row_tombstones_entry& e1, const row_tombstones_entry& e2) const {
            return _c(e1._prefix, e2._prefix);
        }
        bool operator()(const clustering_key::prefix::one& prefix, const row_tombstones_entry& e) const {
            return _c(prefix, e._prefix);
        }
        bool operator()(const row_tombstones_entry& e, const clustering_key::prefix::one& prefix) const {
            return _c(e._prefix, prefix);
        }
    };
    template <typename Comparator>
    struct delegating_compare {
        Comparator _c;
        delegating_compare(Comparator&& c) : _c(std::move(c)) {}
        template <typename Comparable>
        bool operator()(const Comparable& prefix, const row_tombstones_entry& e) const {
            return _c(prefix, e._prefix);
        }
        template <typename Comparable>
        bool operator()(const row_tombstones_entry& e, const Comparable& prefix) const {
            return _c(e._prefix, prefix);
        }
    };
    template <typename Comparator>
    static auto key_comparator(Comparator&& c) {
        return delegating_compare<Comparator>(std::move(c));
    }
};

class rows_entry : public boost::intrusive::set_base_hook<> {
    clustering_key::one _key;
    deletable_row _row;
public:
    rows_entry(clustering_key::one&& key)
        : _key(std::move(key))
    { }
    rows_entry(const clustering_key::one& key)
        : _key(key)
    { }
    rows_entry(const rows_entry& e)
        : _key(e._key)
        , _row(e._row)
    { }
    clustering_key::one& key() {
        return _key;
    }
    const clustering_key::one& key() const {
        return _key;
    }
    deletable_row& row() {
        return _row;
    }
    const deletable_row& row() const {
        return _row;
    }
    void apply(tombstone t) {
        _row.apply(t);
    }
    struct compare {
        clustering_key::one::less_compare _c;
        compare(const schema& s) : _c(s) {}
        bool operator()(const rows_entry& e1, const rows_entry& e2) const {
            return _c(e1._key, e2._key);
        }
        bool operator()(const clustering_key::one& key, const rows_entry& e) const {
            return _c(key, e._key);
        }
        bool operator()(const rows_entry& e, const clustering_key::one& key) const {
            return _c(e._key, key);
        }
    };
    struct compare_prefix {
        clustering_key::one::less_compare_with_prefix _c;
        compare_prefix(const schema& s) : _c(s) {}
        bool operator()(const clustering_key::prefix::one& prefix, const rows_entry& e) const {
            return _c(prefix, e._key);
        }
        bool operator()(const rows_entry& e, const clustering_key::prefix::one& prefix) const {
            return _c(e._key, prefix);
        }
    };
};

class mutation_partition final {
private:
    tombstone _tombstone;
    row _static_row;
    boost::intrusive::set<rows_entry, boost::intrusive::compare<rows_entry::compare>> _rows;
    // Contains only strict prefixes so that we don't have to lookup full keys
    // in both _row_tombstones and _rows.
    boost::intrusive::set<row_tombstones_entry, boost::intrusive::compare<row_tombstones_entry::compare>> _row_tombstones;
public:
    mutation_partition(schema_ptr s)
        : _rows(rows_entry::compare(*s))
        , _row_tombstones(row_tombstones_entry::compare(*s))
    { }
    mutation_partition(mutation_partition&&) = default;
    ~mutation_partition();
    void apply(tombstone t) { _tombstone.apply(t); }
    void apply_delete(schema_ptr schema, const clustering_prefix& prefix, tombstone t);
    void apply_delete(schema_ptr schema, clustering_key::one&& key, tombstone t);
    // prefix must not be full
    void apply_row_tombstone(schema_ptr schema, clustering_key::prefix::one prefix, tombstone t);
    void apply(schema_ptr schema, const mutation_partition& p);
    row& static_row() { return _static_row; }
    row& clustered_row(const clustering_key::one& key);
    row* find_row(const clustering_key::one& key);
    row* find_row(schema_ptr schema, const clustering_key::prefix::one& key);
    rows_entry* find_entry(schema_ptr schema, const clustering_key::prefix::one& key);
    tombstone tombstone_for_row(schema_ptr schema, const clustering_key::one& key);
    tombstone tombstone_for_row(schema_ptr schema, const clustering_key::prefix::one& key);
    friend std::ostream& operator<<(std::ostream& os, const mutation_partition& mp);
};

class mutation final {
public:
    schema_ptr schema;
    partition_key::one key;
    mutation_partition p;
public:
    mutation(partition_key::one key_, schema_ptr schema_)
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
        auto& row = p.clustered_row(clustering_key::one::from_clustering_prefix(*schema, prefix));
        update_column(row, def, std::move(value));
    }

    void set_clustered_cell(const clustering_key::one& key, const column_definition& def, atomic_cell_or_collection value) {
        auto& row = p.clustered_row(key);
        update_column(row, def, std::move(value));
    }
    void set_cell(const clustering_prefix& prefix, const column_definition& def, atomic_cell_or_collection value) {
        if (def.is_static()) {
            set_static_cell(def, std::move(value));
        } else if (def.is_regular()) {
            set_clustered_cell(prefix, def, std::move(value));
        } else {
            throw std::runtime_error("attemting to store into a key cell");
        }
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
    column_family(column_family&&) = default;
    mutation_partition& find_or_create_partition(const partition_key::one& key);
    row& find_or_create_row(const partition_key::one& partition_key, const clustering_key::one& clustering_key);
    mutation_partition* find_partition(const partition_key::one& key);
    row* find_row(const partition_key::one& partition_key, const clustering_key::one& clustering_key);
    schema_ptr _schema;
    // partition key -> partition
    std::map<partition_key::one, mutation_partition, partition_key::one::less_compare> partitions;
    void apply(const mutation& m);
    // Returns at most "cmd.limit" rows
    future<lw_shared_ptr<query::result>> query(const query::read_command& cmd);
private:
    // Returns at most "limit" rows
    query::result::partition get_partition_slice(mutation_partition& partition,
        const query::partition_slice& slice, uint32_t limit);
};

class keyspace {
public:
    std::unordered_map<sstring, column_family> column_families;
    future<> populate(sstring datadir);
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
    future<> populate(sstring datadir);
    keyspace* find_keyspace(const sstring& name);
    schema_ptr find_schema(const sstring& ks_name, const sstring& cf_name);
    future<> stop() { return make_ready_future<>(); }
    void assign(database&& db) {
        *this = std::move(db);
    }
    unsigned shard_of(const dht::token& t);
    future<lw_shared_ptr<query::result>> query(const query::read_command& cmd);
};

// FIXME: stub
class secondary_index_manager {};

#endif /* DATABASE_HH_ */
