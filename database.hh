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
#include "tuple.hh"
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

namespace sstables {

class sstable;

}

struct column_family {
    column_family(schema_ptr schema);
    column_family(column_family&&) = default;
    ~column_family();
    mutation_partition& find_or_create_partition(const dht::decorated_key& key);
    mutation_partition& find_or_create_partition_slow(const partition_key& key);
    mutation_partition* find_partition(const dht::decorated_key& key);
    mutation_partition* find_partition_slow(const partition_key& key);
    row& find_or_create_row_slow(const partition_key& partition_key, const clustering_key& clustering_key);
    row* find_row(const dht::decorated_key& partition_key, const clustering_key& clustering_key);
    schema_ptr _schema;
    // partition key -> partition
    std::map<dht::decorated_key, mutation_partition> partitions;
    void apply(const mutation& m);
    // Returns at most "cmd.limit" rows
    future<lw_shared_ptr<query::result>> query(const query::read_command& cmd);

    future<> populate(sstring datadir);
private:
    // generation -> sstable. Ordered by key so we can easily get the most recent.
    std::map<unsigned long, std::unique_ptr<sstables::sstable>> _sstables;
    future<> probe_file(sstring sstdir, sstring fname);
    // Returns at most "limit" rows. The limit must be greater than 0.
    void get_partition_slice(mutation_partition& partition, const query::partition_slice& slice,
        uint32_t limit, query::result::partition_writer&);
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
    future<> stop();
    void assign(database&& db);
    unsigned shard_of(const dht::token& t);
    unsigned shard_of(const mutation& m);
    future<lw_shared_ptr<query::result>> query(const query::read_command& cmd);
    friend std::ostream& operator<<(std::ostream& out, const database& db);
};

// FIXME: stub
class secondary_index_manager {};

#endif /* DATABASE_HH_ */
