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
#include <cstdint>
#include <unordered_map>
#include <map>
#include <set>
#include <iostream>
#include <boost/functional/hash.hpp>
#include <experimental/optional>
#include <string.h>
#include "types.hh"
#include "compound.hh"
#include "core/future.hh"
#include "cql3/column_specification.hh"
#include <limits>
#include <cstddef>
#include "schema.hh"
#include "timestamp.hh"
#include "tombstone.hh"
#include "atomic_cell.hh"
#include "query-request.hh"
#include "query-result.hh"
#include "keys.hh"
#include "mutation.hh"

class frozen_mutation;

namespace sstables {

class sstable;

}

namespace db {
template<typename T>
class serializer;

class commitlog;
class config;
}

class memtable {
public:
    using partitions_type = std::map<dht::decorated_key, mutation_partition, dht::decorated_key::less_comparator>;
private:
    schema_ptr _schema;
    partitions_type partitions;
public:
    using const_mutation_partition_ptr = std::unique_ptr<const mutation_partition>;
public:
    explicit memtable(schema_ptr schema);
    schema_ptr schema() const { return _schema; }
    mutation_partition& find_or_create_partition(const dht::decorated_key& key);
    mutation_partition& find_or_create_partition_slow(partition_key_view key);
    const_mutation_partition_ptr find_partition(const dht::decorated_key& key) const;
    void apply(const mutation& m);
    void apply(const frozen_mutation& m);
    const partitions_type& all_partitions() const;
};

class column_family {
    schema_ptr _schema;
    std::vector<memtable> _memtables;
    // generation -> sstable. Ordered by key so we can easily get the most recent.
    std::map<unsigned long, std::unique_ptr<sstables::sstable>> _sstables;
private:
    memtable& active_memtable() { return _memtables.back(); }
    struct merge_comparator;
public:
    // Queries can be satisfied from multiple data sources, so they are returned
    // as temporaries.
    //
    // FIXME: in case a query is satisfied from a single memtable, avoid a copy
    using const_mutation_partition_ptr = std::unique_ptr<const mutation_partition>;
    using const_row_ptr = std::unique_ptr<const row>;
public:
    column_family(schema_ptr schema);
    column_family(column_family&&) = default;
    ~column_family();
    schema_ptr schema() const { return _schema; }
    mutation_partition& find_or_create_partition(const dht::decorated_key& key);
    mutation_partition& find_or_create_partition_slow(const partition_key& key);
    const_mutation_partition_ptr find_partition(const dht::decorated_key& key) const;
    const_mutation_partition_ptr find_partition_slow(const partition_key& key) const;
    row& find_or_create_row_slow(const partition_key& partition_key, const clustering_key& clustering_key);
    const_row_ptr find_row(const dht::decorated_key& partition_key, const clustering_key& clustering_key) const;
    void apply(const frozen_mutation& m);
    void apply(const mutation& m);
    // Returns at most "cmd.limit" rows
    future<lw_shared_ptr<query::result>> query(const query::read_command& cmd) const;

    future<> populate(sstring datadir);
    void seal_active_memtable();
    const std::vector<memtable>& testonly_all_memtables() const;
private:
    // Iterate over all partitions.  Protocol is the same as std::all_of(),
    // so that iteration can be stopped by returning false.
    // Func signature: bool (const decorated_key& dk, const mutation_partition& mp)
    template <typename Func>
    bool for_all_partitions(Func&& func) const;
    future<> probe_file(sstring sstdir, sstring fname);
public:
    // Iterate over all partitions.  Protocol is the same as std::all_of(),
    // so that iteration can be stopped by returning false.
    bool for_all_partitions_slow(std::function<bool (const dht::decorated_key&, const mutation_partition&)> func) const;

    friend std::ostream& operator<<(std::ostream& out, const column_family& cf);
};

class user_types_metadata {
    std::unordered_map<bytes, user_type> _user_types;
public:
    user_type get_type(bytes name) const {
        return _user_types.at(name);
    }
    const std::unordered_map<bytes, user_type>& get_all_types() const {
        return _user_types;
    }
    void add_type(user_type type) {
        auto i = _user_types.find(type->_name);
        assert(i == _user_types.end() || type->is_compatible_with(*i->second));
        _user_types[type->_name] = std::move(type);
    }
    void remove_type(user_type type) {
        _user_types.erase(type->_name);
    }
};

class keyspace {
    std::unique_ptr<locator::abstract_replication_strategy> _replication_strategy;
public:
    user_types_metadata _user_types;
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
    std::unique_ptr<db::commitlog> _commitlog;
    std::unique_ptr<db::config> _cfg;

    future<> init_commitlog();
    future<> apply_in_memory(const frozen_mutation&);
    future<> populate(sstring datadir);
public:
    database();
    database(const db::config&);
    database(database&&) = default;
    ~database();

    db::commitlog* commitlog() const {
        return _commitlog.get();
    }

    future<> init_from_data_directory();

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
    void update_keyspace(const sstring& name);
    void drop_keyspace(const sstring& name);
    column_family& find_column_family(const sstring& ks, const sstring& name) throw (no_such_column_family);
    const column_family& find_column_family(const sstring& ks, const sstring& name) const throw (no_such_column_family);
    column_family& find_column_family(const utils::UUID&) throw (no_such_column_family);
    const column_family& find_column_family(const utils::UUID&) const throw (no_such_column_family);
    column_family& find_column_family(const schema_ptr&) throw (no_such_column_family);
    const column_family& find_column_family(const schema_ptr&) const throw (no_such_column_family);
    schema_ptr find_schema(const sstring& ks_name, const sstring& cf_name) const throw (no_such_column_family);
    schema_ptr find_schema(const utils::UUID&) const throw (no_such_column_family);
    future<> stop();
    unsigned shard_of(const dht::token& t);
    unsigned shard_of(const mutation& m);
    unsigned shard_of(const frozen_mutation& m);
    future<lw_shared_ptr<query::result>> query(const query::read_command& cmd);
    future<> apply(const frozen_mutation&);
    friend std::ostream& operator<<(std::ostream& out, const database& db);
};

// FIXME: stub
class secondary_index_manager {};

inline
mutation_partition&
column_family::find_or_create_partition(const dht::decorated_key& key) {
    return active_memtable().find_or_create_partition(key);
}

inline
void
column_family::apply(const mutation& m) {
    return active_memtable().apply(m);
}

inline
void
column_family::apply(const frozen_mutation& m) {
    return active_memtable().apply(m);
}

#endif /* DATABASE_HH_ */
