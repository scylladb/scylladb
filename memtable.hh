/*
 * Copyright (C) 2015 ScyllaDB
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
#include "partition_version.hh"

class frozen_mutation;


namespace bi = boost::intrusive;

class memtable_entry {
    bi::set_member_hook<> _link;
    schema_ptr _schema;
    dht::decorated_key _key;
    partition_entry _pe;
public:
    friend class memtable;

    memtable_entry(schema_ptr s, dht::decorated_key key, mutation_partition p)
        : _schema(std::move(s))
        , _key(std::move(key))
        , _pe(std::move(p))
    { }

    memtable_entry(memtable_entry&& o) noexcept;

    const dht::decorated_key& key() const { return _key; }
    dht::decorated_key& key() { return _key; }
    const partition_entry& partition() const { return _pe; }
    partition_entry& partition() { return _pe; }
    const schema_ptr& schema() const { return _schema; }
    schema_ptr& schema() { return _schema; }
    streamed_mutation read(lw_shared_ptr<memtable> mtbl, const schema_ptr&, const query::partition_slice&);

    size_t memory_usage_without_rows() const {
        return _key.key().memory_usage();
    }

    struct compare {
        dht::decorated_key::less_comparator _c;

        compare(schema_ptr s)
            : _c(std::move(s))
        {}

        bool operator()(const dht::decorated_key& k1, const memtable_entry& k2) const {
            return _c(k1, k2._key);
        }

        bool operator()(const memtable_entry& k1, const memtable_entry& k2) const {
            return _c(k1._key, k2._key);
        }

        bool operator()(const memtable_entry& k1, const dht::decorated_key& k2) const {
            return _c(k1._key, k2);
        }

        bool operator()(const memtable_entry& k1, const dht::ring_position& k2) const {
            return _c(k1._key, k2);
        }

        bool operator()(const dht::ring_position& k1, const memtable_entry& k2) const {
            return _c(k1, k2._key);
        }
    };
};

// Managed by lw_shared_ptr<>.
class memtable final : public enable_lw_shared_from_this<memtable>, private logalloc::region {
public:
    using partitions_type = bi::set<memtable_entry,
        bi::member_hook<memtable_entry, bi::set_member_hook<>, &memtable_entry::_link>,
        bi::compare<memtable_entry::compare>>;
private:
    schema_ptr _schema;
    logalloc::allocating_section _read_section;
    logalloc::allocating_section _allocating_section;
    partitions_type partitions;
    db::replay_position _replay_position;
    lw_shared_ptr<sstables::sstable> _sstable;
    void update(const db::replay_position&);
    friend class row_cache;
    friend class memtable_entry;
private:
    boost::iterator_range<partitions_type::const_iterator> slice(const query::partition_range& r) const;
    partition_entry& find_or_create_partition(const dht::decorated_key& key);
    partition_entry& find_or_create_partition_slow(partition_key_view key);
    void upgrade_entry(memtable_entry&);
public:
    explicit memtable(schema_ptr schema, logalloc::region_group* dirty_memory_region_group = nullptr);
    ~memtable();
    schema_ptr schema() const { return _schema; }
    void set_schema(schema_ptr) noexcept;
    future<> apply(memtable&);
    // Applies mutation to this memtable.
    // The mutation is upgraded to current schema.
    void apply(const mutation& m, const db::replay_position& = db::replay_position());
    // The mutation is upgraded to current schema.
    void apply(const frozen_mutation& m, const schema_ptr& m_schema, const db::replay_position& = db::replay_position());

    static memtable& from_region(logalloc::region& r) {
        return static_cast<memtable&>(r);
    }

    const logalloc::region& region() const {
        return *this;
    }

    logalloc::region_group* region_group() {
        return group();
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
    //
    // Mutations returned by the reader will all have given schema.
    mutation_reader make_reader(schema_ptr,
                                const query::partition_range& range = query::full_partition_range,
                                const query::partition_slice& slice = query::full_slice,
                                const io_priority_class& pc = default_priority_class());


    mutation_reader make_flush_reader(schema_ptr, const io_priority_class& pc);

    mutation_source as_data_source();

    bool empty() const { return partitions.empty(); }
    void mark_flushed(lw_shared_ptr<sstables::sstable> sst);
    bool is_flushed() const;

    const db::replay_position& replay_position() const {
        return _replay_position;
    }

    friend class iterator_reader;
};
