/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#ifndef DATABASE_HH_
#define DATABASE_HH_

#include "dht/i_partitioner.hh"
#include "config/ks_meta_data.hh"
#include "locator/abstract_replication_strategy.hh"
#include "core/sstring.hh"
#include "core/shared_ptr.hh"
#include "net/byteorder.hh"
#include "utils/UUID.hh"
#include "utils/hash.hh"
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
#include <boost/range/iterator_range.hpp>
#include "sstables/sstables.hh"

using row = std::map<column_id, atomic_cell_or_collection>;

struct deletable_row final {
    tombstone t;
    api::timestamp_type created_at = api::missing_timestamp;
    row cells;

    void apply(tombstone t_) {
        t.apply(t_);
    }
};

class row_tombstones_entry : public boost::intrusive::set_base_hook<> {
    clustering_key_prefix _prefix;
    tombstone _t;
public:
    row_tombstones_entry(clustering_key_prefix&& prefix, tombstone t)
        : _prefix(std::move(prefix))
        , _t(std::move(t))
    { }
    clustering_key_prefix& prefix() {
        return _prefix;
    }
    const clustering_key_prefix& prefix() const {
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
        clustering_key_prefix::less_compare _c;
        compare(const schema& s) : _c(s) {}
        bool operator()(const row_tombstones_entry& e1, const row_tombstones_entry& e2) const {
            return _c(e1._prefix, e2._prefix);
        }
        bool operator()(const clustering_key_prefix& prefix, const row_tombstones_entry& e) const {
            return _c(prefix, e._prefix);
        }
        bool operator()(const row_tombstones_entry& e, const clustering_key_prefix& prefix) const {
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
    clustering_key _key;
    deletable_row _row;
public:
    rows_entry(clustering_key&& key)
        : _key(std::move(key))
    { }
    rows_entry(const clustering_key& key)
        : _key(key)
    { }
    rows_entry(const rows_entry& e)
        : _key(e._key)
        , _row(e._row)
    { }
    clustering_key& key() {
        return _key;
    }
    const clustering_key& key() const {
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
        clustering_key::less_compare _c;
        compare(const schema& s) : _c(s) {}
        bool operator()(const rows_entry& e1, const rows_entry& e2) const {
            return _c(e1._key, e2._key);
        }
        bool operator()(const clustering_key& key, const rows_entry& e) const {
            return _c(key, e._key);
        }
        bool operator()(const rows_entry& e, const clustering_key& key) const {
            return _c(e._key, key);
        }
    };
    template <typename Comparator>
    struct delegating_compare {
        Comparator _c;
        delegating_compare(Comparator&& c) : _c(std::move(c)) {}
        template <typename Comparable>
        bool operator()(const Comparable& v, const rows_entry& e) const {
            return _c(v, e._key);
        }
        template <typename Comparable>
        bool operator()(const rows_entry& e, const Comparable& v) const {
            return _c(e._key, v);
        }
    };
    template <typename Comparator>
    static auto key_comparator(Comparator&& c) {
        return delegating_compare<Comparator>(std::move(c));
    }
};

namespace db {
template<typename T>
class serializer;
}

class mutation_partition final {
    using rows_type = boost::intrusive::set<rows_entry, boost::intrusive::compare<rows_entry::compare>>;
private:
    tombstone _tombstone;
    row _static_row;
    rows_type _rows;
    // Contains only strict prefixes so that we don't have to lookup full keys
    // in both _row_tombstones and _rows.
    boost::intrusive::set<row_tombstones_entry, boost::intrusive::compare<row_tombstones_entry::compare>> _row_tombstones;

    template<typename T>
    friend class db::serializer;
public:
    mutation_partition(schema_ptr s)
        : _rows(rows_entry::compare(*s))
        , _row_tombstones(row_tombstones_entry::compare(*s))
    { }
    mutation_partition(mutation_partition&&) = default;
    ~mutation_partition();
    tombstone tombstone_for_static_row() const { return _tombstone; }
    void apply(tombstone t) { _tombstone.apply(t); }
    void apply_delete(schema_ptr schema, const exploded_clustering_prefix& prefix, tombstone t);
    void apply_delete(schema_ptr schema, clustering_key&& key, tombstone t);
    // prefix must not be full
    void apply_row_tombstone(schema_ptr schema, clustering_key_prefix prefix, tombstone t);
    void apply(schema_ptr schema, const mutation_partition& p);
    row& static_row() { return _static_row; }
    deletable_row& clustered_row(const clustering_key& key);
    row* find_row(const clustering_key& key);
    rows_entry* find_entry(schema_ptr schema, const clustering_key_prefix& key);
    tombstone range_tombstone_for_row(const schema& schema, const clustering_key& key);
    tombstone tombstone_for_row(const schema& schema, const clustering_key& key);
    tombstone tombstone_for_row(const schema& schema, const rows_entry& e);
    friend std::ostream& operator<<(std::ostream& os, const mutation_partition& mp);
    boost::iterator_range<rows_type::iterator> range(const schema& schema, const query::range<clustering_key_prefix>& r);
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
    void set_static_cell(const column_definition& def, atomic_cell_or_collection value);
    void set_clustered_cell(const exploded_clustering_prefix& prefix, const column_definition& def, atomic_cell_or_collection value);
    void set_clustered_cell(const clustering_key& key, const column_definition& def, atomic_cell_or_collection value);
    void set_cell(const exploded_clustering_prefix& prefix, const bytes& name, const boost::any& value, api::timestamp_type timestamp, ttl_opt ttl = {});
    void set_cell(const exploded_clustering_prefix& prefix, const column_definition& def, atomic_cell_or_collection value);
    std::experimental::optional<atomic_cell_or_collection> get_cell(const clustering_key& rkey, const column_definition& def);
private:
    static void update_column(row& row, const column_definition& def, atomic_cell_or_collection&& value);
    friend std::ostream& operator<<(std::ostream& os, const mutation& m);
};

struct column_family {
    column_family(schema_ptr schema);
    column_family(column_family&&) = default;
    mutation_partition& find_or_create_partition(const partition_key& key);
    row& find_or_create_row(const partition_key& partition_key, const clustering_key& clustering_key);
    mutation_partition* find_partition(const partition_key& key);
    row* find_row(const partition_key& partition_key, const clustering_key& clustering_key);
    schema_ptr _schema;
    // partition key -> partition
    std::map<partition_key, mutation_partition, partition_key::less_compare> partitions;
    void apply(const mutation& m);
    // Returns at most "cmd.limit" rows
    future<lw_shared_ptr<query::result>> query(const query::read_command& cmd);

    future<> populate(sstring datadir);
private:
    // generation -> sstable. Ordered by key so we can easily get the most recent.
    std::map<unsigned long, std::unique_ptr<sstables::sstable>> _sstables;
    future<> probe_file(sstring sstdir, sstring fname);
    // Returns at most "limit" rows
    query::result::partition get_partition_slice(mutation_partition& partition,
        const query::partition_slice& slice, uint32_t limit);
};

class keyspace {
    std::unique_ptr<locator::abstract_replication_strategy> _replication_strategy;
public:
    void create_replication_strategy(config::ks_meta_data& ksm);
    locator::abstract_replication_strategy& get_replication_strategy();
};

class no_such_keyspace : public std::runtime_error {
public:
    using runtime_error::runtime_error;
};

class no_such_column_family : public std::runtime_error {
public:
    using runtime_error::runtime_error;
};

// Policy for distributed<database>:
//   broadcast metadata writes
//   local metadata reads
//   use shard_of() for data

class database {
    std::unordered_map<sstring, keyspace> _keyspaces;
    std::unordered_map<utils::UUID, column_family> _column_families;
    std::unordered_map<std::pair<sstring, sstring>, utils::UUID, utils::tuple_hash> _ks_cf_to_uuid;
public:
    database();

    future<> init_from_data_directory(sstring datadir);
    future<> populate(sstring datadir);

    keyspace& add_keyspace(sstring name, keyspace k);
    /** Adds cf with auto-generated UUID. */
    void add_column_family(column_family&&);
    void add_column_family(const utils::UUID&, column_family&&);

    /* throws std::out_of_range if missing */
    const utils::UUID& find_uuid(const sstring& ks, const sstring& cf) const throw (std::out_of_range);
    const utils::UUID& find_uuid(const schema_ptr&) const throw (std::out_of_range);

    /* below, find* throws no_such_<type> on fail */
    keyspace& find_or_create_keyspace(const sstring& name);
    keyspace& find_keyspace(const sstring& name) throw (no_such_keyspace);
    const keyspace& find_keyspace(const sstring& name) const throw (no_such_keyspace);
    bool has_keyspace(const sstring& name) const;
    column_family& find_column_family(const sstring& ks, const sstring& name) throw (no_such_column_family);
    const column_family& find_column_family(const sstring& ks, const sstring& name) const throw (no_such_column_family);
    column_family& find_column_family(const utils::UUID&) throw (no_such_column_family);
    const column_family& find_column_family(const utils::UUID&) const throw (no_such_column_family);
    column_family& find_column_family(const schema_ptr&) throw (no_such_column_family);
    const column_family& find_column_family(const schema_ptr&) const throw (no_such_column_family);
    schema_ptr find_schema(const sstring& ks_name, const sstring& cf_name) const throw (no_such_column_family);
    schema_ptr find_schema(const utils::UUID&) const throw (no_such_column_family);
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
