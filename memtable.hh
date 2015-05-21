/*
 * Copyright 2015 Cloudius Systems
 */

#pragma once

#include <map>
#include <memory>
#include "database_fwd.hh"
#include "dht/i_partitioner.hh"
#include "schema.hh"

class frozen_mutation;

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
    row& find_or_create_row_slow(const partition_key& partition_key, const clustering_key& clustering_key);
    const_mutation_partition_ptr find_partition(const dht::decorated_key& key) const;
    void apply(const mutation& m);
    void apply(const frozen_mutation& m);
    const partitions_type& all_partitions() const;
    bool empty() const { return partitions.empty(); }
};
