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


namespace bi = boost::intrusive;

class partition_entry : public bi::set_base_hook<> {
    dht::decorated_key _key;
    mutation_partition _p;
public:
    friend class memtable;

    partition_entry(dht::decorated_key key, mutation_partition p)
        : _key(std::move(key))
        , _p(std::move(p))
    { }

    const dht::decorated_key& key() const { return _key; }
    const mutation_partition& partition() const { return _p; }
    mutation_partition& partition() { return _p; }

    struct compare {
        dht::decorated_key::less_comparator _c;

        compare(schema_ptr s)
            : _c(std::move(s))
        {}

        bool operator()(const dht::decorated_key& k1, const partition_entry& k2) const {
            return _c(k1, k2._key);
        }

        bool operator()(const partition_entry& k1, const partition_entry& k2) const {
            return _c(k1._key, k2._key);
        }

        bool operator()(const partition_entry& k1, const dht::decorated_key& k2) const {
            return _c(k1._key, k2);
        }

        bool operator()(const partition_entry& k1, const dht::ring_position& k2) const {
            return _c(k1._key, k2);
        }

        bool operator()(const dht::ring_position& k1, const partition_entry& k2) const {
            return _c(k1, k2._key);
        }
    };
};

// Managed by lw_shared_ptr<>.
class memtable final : public enable_lw_shared_from_this<memtable> {
public:
    using partitions_type = bi::set<partition_entry, bi::compare<partition_entry::compare>>;
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

    mutation_source as_data_source();

    bool empty() const { return partitions.empty(); }
    const db::replay_position& replay_position() const {
        return _replay_position;
    }
};
