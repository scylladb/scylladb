/*
 * Copyright 2015 Cloudius Systems
 */

#pragma once

#include <map>
#include <memory>
#include "database_fwd.hh"
#include "dht/i_partitioner.hh"
#include "schema.hh"
#include "mutation_reader.hh"
#include "db/commitlog/replay_position.hh"

class frozen_mutation;

class memtable {
public:
    using partitions_type = std::map<dht::decorated_key, mutation_partition, dht::decorated_key::less_comparator>;
private:
    schema_ptr _schema;
    partitions_type partitions;
    db::replay_position _replay_position;

    void update(const db::replay_position&);
public:
    using const_mutation_partition_ptr = std::unique_ptr<const mutation_partition>;
public:
    explicit memtable(schema_ptr schema);
    schema_ptr schema() const { return _schema; }
    mutation_partition& find_or_create_partition(const dht::decorated_key& key);
    mutation_partition& find_or_create_partition_slow(partition_key_view key);
    row& find_or_create_row_slow(const partition_key& partition_key, const clustering_key& clustering_key);
    const_mutation_partition_ptr find_partition(const dht::decorated_key& key) const;
    void apply(const mutation& m, const db::replay_position& = db::replay_position());
    void apply(const frozen_mutation& m, const db::replay_position& = db::replay_position());
    const partitions_type& all_partitions() const;
    bool empty() const { return partitions.empty(); }
    const db::replay_position& replay_position() const {
        return _replay_position;
    }
    mutation_reader make_reader() const;
};
