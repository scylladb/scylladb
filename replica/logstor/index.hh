/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#pragma once

#include "dht/decorated_key.hh"
#include "dht/ring_position.hh"
#include <seastar/coroutine/maybe_yield.hh>
#include "types.hh"
#include "utils/bptree.hh"
#include "utils/double-decker.hh"
#include "utils/on_internal_error.hh"
#include "utils/phased_barrier.hh"
#include <utility>
#include "replica/logstor/cache.hh"

namespace replica::logstor {

extern seastar::logger logstor_logger;

// One entry in the primary index B+tree.
//
// In addition to the on-disk location (index_entry), each entry may hold a
// pointer to a cached_mutation_entry that lives in the logstor_cache_tracker's
// LSA region.  When the cache evicts the entry under memory pressure it zeroes
// _cached_entry via the back-pointer stored inside cached_mutation_entry.
class primary_index_entry {
    dht::decorated_key _key;
    index_entry _e;
    // Non-owning slot pointing into the shared cache region.
    // Empty when no cached mutation exists for this key.
    mutable cached_entry_slot _cached_entry;
    struct {
        bool _head : 1;
        bool _tail : 1;
        bool _train : 1;
    } _flags{};
public:
    friend class cache_tracker;

    primary_index_entry(dht::decorated_key key, index_entry e)
        : _key(std::move(key))
        , _e(std::move(e))
    { }

    ~primary_index_entry() {
        if (_cached_entry) {
            on_internal_error(logstor_logger, "primary_index_entry destroyed while having a cache entry");
        }
    }

    primary_index_entry(primary_index_entry&& other) noexcept
        : _key(std::move(other._key))
        , _e(std::move(other._e))
        , _cached_entry(std::move(other._cached_entry))
        , _flags(other._flags)
    {}

    primary_index_entry& operator=(primary_index_entry&& other) noexcept {
        if (this == &other) {
            return *this;
        }
        if (_cached_entry) {
            on_internal_error(logstor_logger, "primary_index_entry move-assignment overwrote a live cached entry");
        }
        _key = std::move(other._key);
        _e = std::move(other._e);
        _cached_entry = std::move(other._cached_entry);
        _flags = other._flags;
        return *this;
    }

    size_t memory_usage() const noexcept {
        return sizeof(primary_index_entry) + _key.external_memory_usage();
    }

    bool is_head() const noexcept { return _flags._head; }
    void set_head(bool v) noexcept { _flags._head = v; }
    bool is_tail() const noexcept { return _flags._tail; }
    void set_tail(bool v) noexcept { _flags._tail = v; }
    bool with_train() const noexcept { return _flags._train; }
    void set_train(bool v) noexcept { _flags._train = v; }

    const dht::decorated_key& key() const noexcept { return _key; }
    const index_entry& entry() const noexcept { return _e; }

    friend class primary_index;

    friend dht::ring_position_view ring_position_view_to_compare(const primary_index_entry& e) { return e._key; }
};

class primary_index final {
public:
    using partitions_type = double_decker<int64_t, primary_index_entry,
                            dht::raw_token_less_comparator, dht::ring_position_comparator,
                            16, bplus::key_search::linear>;
private:
    partitions_type _partitions;
    schema_ptr _schema;
    size_t _key_count = 0;
    size_t _memory_usage = 0;

    mutable utils::phased_barrier _reads_phaser{"logstor_primary_index"};

    // Non-owning pointer to the cache tracker; null when the cache is not set up.
    // Mutable so that logically-const methods (lookup_cache, populate_cache) can
    // call non-const methods on the tracker (touch, insert) for LRU accounting.
    mutable cache_tracker* _cache_tracker = nullptr;

    void on_entry_added(const primary_index_entry& e) noexcept {
        _memory_usage += e.memory_usage();
        ++_key_count;
    }

    void on_entry_removed(const primary_index_entry& e) noexcept {
        _memory_usage -= e.memory_usage();
        --_key_count;
    }

    // Update counters and remove an entry from the B+tree.
    void erase_entry(partitions_type::iterator it) noexcept {
        on_entry_removed(*it);
        if (_cache_tracker) {
            _cache_tracker->evict(*it);
        }
        it.erase(dht::raw_token_less_comparator{});
    }

public:
    explicit primary_index(schema_ptr schema)
        : _partitions(dht::raw_token_less_comparator{})
        , _schema(std::move(schema))
        {}

    void set_schema(schema_ptr s) {
        _schema = std::move(s);
    }

    void set_cache_tracker(cache_tracker* ct) noexcept {
        _cache_tracker = ct;
    }

    cache_tracker* cache_tracker() const noexcept {
        return _cache_tracker;
    }

    future<> drain_cache() {
        if (_cache_tracker) {
            for (auto& pie : _partitions) {
                _cache_tracker->evict(pie);
                co_await coroutine::maybe_yield();
            }
        }
    }

    future<> clear() {
        co_await drain_cache();

        _partitions.clear();
        _key_count = 0;
        _memory_usage = 0;
    }

    utils::phased_barrier::operation start_read() const {
        return _reads_phaser.start();
    }

    future<> await_pending_reads() {
        return _reads_phaser.advance_and_await();
    }

    std::optional<index_entry> get(const primary_index_key& key) const {
        auto it = _partitions.find(key.dk, dht::ring_position_comparator(*_schema));
        if (it != _partitions.end()) {
            return it->_e;
        }
        return std::nullopt;
    }

    bool is_record_alive(const primary_index_key& key, log_location location) {
        auto it = _partitions.find(key.dk, dht::ring_position_comparator(*_schema));
        if (it != _partitions.end()) {
            return it->_e.location == location;
        } else {
            return false;
        }
    }

    bool update_record_location(const primary_index_key& key, log_location old_location, log_location new_location) {
        auto it = _partitions.find(key.dk, dht::ring_position_comparator(*_schema));
        if (it != _partitions.end()) {
            if (it->_e.location == old_location) {
                it->_e.location = new_location;
                // The cached mutation is still valid (same data, new location on
                // disk after compaction moved it) — do not evict the cache here.
                return true;
            }
        }
        return false;
    }

    using entry_cmp_fn = std::function<std::strong_ordering(const index_entry&, const index_entry&)>;

    static std::strong_ordering default_entry_cmp(const index_entry& a, const index_entry& b) noexcept {
        return a.timestamp <=> b.timestamp;
    }

    std::pair<bool, std::optional<index_entry>> insert(const primary_index_key& key, index_entry new_entry, entry_cmp_fn cmp = default_entry_cmp) {
        partitions_type::bound_hint hint;
        auto i = _partitions.lower_bound(key.dk, dht::ring_position_comparator(*_schema), hint);
        if (hint.match) {
            if (cmp(i->_e, new_entry) <= 0) {
                // Overwriting with newer data: evict stale cached mutation.
                if (_cache_tracker) {
                    _cache_tracker->evict(*i);
                }
                auto old_entry = i->_e;
                i->_e = std::move(new_entry);
                return {true, std::make_optional(old_entry)};
            } else {
                return {false, std::make_optional(i->_e)};
            }
        } else {
            auto it = _partitions.emplace_before(i, key.dk.token().raw(), hint, key.dk, std::move(new_entry));
            on_entry_added(*it);
            return {true, std::nullopt};
        }
    }

    bool erase(const primary_index_key& key, log_location loc) {
        auto it = _partitions.find(key.dk, dht::ring_position_comparator(*_schema));
        if (it != _partitions.end() && it->_e.location == loc) {
            erase_entry(it);
            return true;
        }
        return false;
    }

    future<> erase(const dht::partition_range& pr) {
        dht::ring_position_comparator cmp(*_schema);
        auto it = _partitions.lower_bound(dht::ring_position_view::for_range_start(pr), cmp);
        auto end_it = _partitions.lower_bound(dht::ring_position_view::for_range_end(pr), cmp);
        while (it != end_it) {
            auto prev = it;
            ++it;
            erase_entry(prev);
            co_await coroutine::maybe_yield();
        }
    }

    auto begin() const noexcept { return _partitions.begin(); }
    auto end() const noexcept { return _partitions.end(); }

    bool empty() const noexcept { return _partitions.empty(); }
    size_t get_key_count() const noexcept { return _key_count; }
    size_t get_memory_usage() const noexcept { return _memory_usage; }

    partitions_type::const_iterator find(const dht::decorated_key& key) const {
        return _partitions.find(key, dht::ring_position_comparator(*_schema));
    }

    // First entry with key >= pos (for positioning at range start)
    partitions_type::const_iterator lower_bound(const dht::ring_position_view& pos) const {
        return _partitions.lower_bound(pos, dht::ring_position_comparator(*_schema));
    }

    // First entry with key strictly > key (for advancing past a key after a yield)
    partitions_type::const_iterator upper_bound(const dht::decorated_key& key) const {
        return _partitions.upper_bound(key, dht::ring_position_comparator(*_schema));
    }

};

}
