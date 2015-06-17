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

// Managed by lw_shared_ptr<>.
class memtable final : public enable_lw_shared_from_this<memtable> {
public:
    using partitions_type = std::map<dht::decorated_key, mutation_partition, dht::decorated_key::less_comparator>;
private:
    schema_ptr _schema;
    partitions_type partitions;
    db::replay_position _replay_position;

    void update(const db::replay_position&);
private:
    boost::iterator_range<partitions_type::const_iterator> slice(const query::partition_range& r) const;
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
public:
    const partitions_type& all_partitions() const;

    // Creates a reader of data in this memtable for given partition range.
    //
    // Live readers share ownership of the memtable instance, so caller
    // doesn't need to ensure that memtable remains live.
    //
    // The 'range' parameter must be live as long as the reader is being used
    mutation_reader make_reader(const query::partition_range& range = query::full_partition_range) const;

    bool empty() const { return partitions.empty(); }
    const db::replay_position& replay_position() const {
        return _replay_position;
    }
};
