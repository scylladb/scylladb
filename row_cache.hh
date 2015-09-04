/*
 * Copyright 2015 Cloudius Systems
 */

#pragma once

#include <boost/intrusive/list.hpp>
#include <boost/intrusive/set.hpp>

#include "core/memory.hh"

#include "mutation_reader.hh"
#include "mutation_partition.hh"
#include "utils/logalloc.hh"

namespace scollectd {

struct registrations;

}

namespace bi = boost::intrusive;

// Intrusive set entry which holds partition data.
//
// TODO: Make memtables use this format too.
class cache_entry {
    // We need auto_unlink<> option on the _cache_link because when entry is
    // evicted from cache via LRU we don't have a reference to the container
    // and don't want to store it with each entry. As for the _lru_link, we
    // have a global LRU, so technically we could not use auto_unlink<> on
    // _lru_link, but it's convenient to do so too. We may also want to have
    // multiple eviction spaces in the future and thus multiple LRUs.
    using lru_link_type = bi::list_member_hook<bi::link_mode<bi::auto_unlink>>;
    using cache_link_type = bi::set_member_hook<bi::link_mode<bi::auto_unlink>>;

    dht::decorated_key _key;
    mutation_partition _p;
    lru_link_type _lru_link;
    cache_link_type _cache_link;
public:
    friend class row_cache;
    friend class cache_tracker;

    cache_entry(dht::decorated_key key, mutation_partition p)
        : _key(std::move(key))
        , _p(std::move(p))
    { }

    cache_entry(cache_entry&&) noexcept;

    const dht::decorated_key& key() const { return _key; }
    const mutation_partition& partition() const { return _p; }
    mutation_partition& partition() { return _p; }

    struct compare {
        dht::decorated_key::less_comparator _c;

        compare(schema_ptr s)
            : _c(std::move(s))
        {}

        bool operator()(const dht::decorated_key& k1, const cache_entry& k2) const {
            return _c(k1, k2._key);
        }

        bool operator()(const cache_entry& k1, const cache_entry& k2) const {
            return _c(k1._key, k2._key);
        }

        bool operator()(const cache_entry& k1, const dht::decorated_key& k2) const {
            return _c(k1._key, k2);
        }
    };
};

// Tracks accesses and performs eviction of cache entries.
class cache_tracker final {
public:
    using lru_type = bi::list<cache_entry,
        bi::member_hook<cache_entry, cache_entry::lru_link_type, &cache_entry::_lru_link>,
        bi::constant_time_size<false>>; // we need this to have bi::auto_unlink on hooks.
private:
    uint64_t _hits = 0;
    uint64_t _misses = 0;
    std::unique_ptr<scollectd::registrations> _collectd_registrations;
    logalloc::region _region;
    lru_type _lru;
private:
    void setup_collectd();
public:
    cache_tracker();
    ~cache_tracker();
    void clear();
    void touch(cache_entry&);
    void insert(cache_entry&);
    allocation_strategy& allocator();
    logalloc::region& region();
    const logalloc::region& region() const;
};

// Returns a reference to shard-wide cache_tracker.
cache_tracker& global_cache_tracker();

//
// A data source which wraps another data source such that data obtained from the underlying data source
// is cached in-memory in order to serve queries faster.
//
// To query the underlying data source through cache, use make_reader().
//
// Cache populates itself automatically during misses.
//
// Cache needs to be maintained externally so that it remains consistent with the underlying data source.
// Any incremental change to the underlying data source should result in update() being called on cache.
//
class row_cache final {
public:
    using partitions_type = bi::set<cache_entry,
        bi::member_hook<cache_entry, cache_entry::cache_link_type, &cache_entry::_cache_link>,
        bi::constant_time_size<false>, // we need this to have bi::auto_unlink on hooks
        bi::compare<cache_entry::compare>>;
    friend class populating_reader;
public:
    struct stats {
        uint64_t hits;
        uint64_t misses;
    };
private:
    cache_tracker& _tracker;
    stats _stats{};
    schema_ptr _schema;
    partitions_type _partitions; // Cached partitions are complete.
    mutation_source _underlying;
    logalloc::allocating_section _update_section;
    logalloc::allocating_section _read_section;
public:
    ~row_cache();
    row_cache(schema_ptr, mutation_source underlying, cache_tracker&);
    row_cache(row_cache&&) = default;
    row_cache(const row_cache&) = delete;
    row_cache& operator=(row_cache&&) = default;
public:
    mutation_reader make_reader(const query::partition_range&);
    const stats& stats() const { return _stats; }
public:
    // Populate cache from given mutation. The mutation must contain all
    // information there is for its partition in the underlying data sources.
    void populate(const mutation& m);

    // Synchronizes cache with the underlying data source from a memtable which
    // has just been flushed to the underlying data source.
    // The memtable can be queried during the process, but must not be written.
    // After the update is complete, memtable is empty.
    future<> update(memtable&, partition_presence_checker underlying_negative);

    // Moves given partition to the front of LRU if present in cache.
    void touch(const dht::decorated_key&);

    auto num_entries() const {
        return _partitions.size();
    }
    const cache_tracker& get_cache_tracker() const {
        return _tracker;
    }
};
