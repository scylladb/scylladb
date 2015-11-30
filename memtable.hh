/*
 * Copyright 2015 Cloudius Systems
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include <map>
#include <memory>
#include "database_fwd.hh"
#include "dht/i_partitioner.hh"
#include "schema.hh"
#include "mutation_reader.hh"
#include "db/commitlog/replay_position.hh"
#include "utils/logalloc.hh"
#include "sstables/sstables.hh"

class frozen_mutation;


namespace bi = boost::intrusive;

class partition_entry {
    bi::set_member_hook<> _link;
    dht::decorated_key _key;
    mutation_partition _p;
public:
    friend class memtable;

    partition_entry(dht::decorated_key key, mutation_partition p)
        : _key(std::move(key))
        , _p(std::move(p))
    { }

    partition_entry(partition_entry&& o) noexcept;

    const dht::decorated_key& key() const { return _key; }
    dht::decorated_key& key() { return _key; }
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
    using partitions_type = bi::set<partition_entry,
        bi::member_hook<partition_entry, bi::set_member_hook<>, &partition_entry::_link>,
        bi::compare<partition_entry::compare>>;
private:
    schema_ptr _schema;
    mutable logalloc::region _region;
    logalloc::allocating_section _allocating_section;
    partitions_type partitions;
    db::replay_position _replay_position;
    lw_shared_ptr<sstables::sstable> _sstable;
    void update(const db::replay_position&);
    friend class row_cache;
private:
    boost::iterator_range<partitions_type::const_iterator> slice(const query::partition_range& r) const;
    mutation_partition& find_or_create_partition(const dht::decorated_key& key);
    mutation_partition& find_or_create_partition_slow(partition_key_view key);
public:
    explicit memtable(schema_ptr schema, logalloc::region_group* dirty_memory_region_group = nullptr);
    ~memtable();
    schema_ptr schema() const { return _schema; }
    future<> apply(const memtable&);
    void apply(const mutation& m, const db::replay_position& = db::replay_position());
    void apply(const frozen_mutation& m, const db::replay_position& = db::replay_position());
    const logalloc::region& region() const {
        return _region;
    }
public:
    size_t partition_count() const;
    logalloc::occupancy_stats occupancy() const;

    // Creates a reader of data in this memtable for given partition range.
    //
    // Live readers share ownership of the memtable instance, so caller
    // doesn't need to ensure that memtable remains live.
    //
    // The 'range' parameter must be live as long as the reader is being used
    mutation_reader make_reader(const query::partition_range& range = query::full_partition_range) const;

    mutation_source as_data_source();
    key_source as_key_source();

    bool empty() const { return partitions.empty(); }
    void mark_flushed(lw_shared_ptr<sstables::sstable> sst);
    bool is_flushed() const;

    const db::replay_position& replay_position() const {
        return _replay_position;
    }

    friend class scanning_reader;
};
