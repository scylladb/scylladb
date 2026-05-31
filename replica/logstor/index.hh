/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#pragma once

#include "dht/decorated_key.hh"
#include "dht/i_partitioner.hh"
#include <seastar/coroutine/maybe_yield.hh>
#include "dht/token.hh"
#include "types.hh"
#include "utils/bptree.hh"
#include "utils/double-decker.hh"
#include "utils/on_internal_error.hh"
#include "utils/phased_barrier.hh"
#include <utility>
#include "replica/logstor/cache.hh"

namespace replica::logstor {

extern seastar::logger logstor_logger;

struct primary_index_key_cmp {
    std::strong_ordering operator()(const primary_index_key& lhs, const primary_index_key& rhs) const noexcept {
        return lhs <=> rhs;
    }

    std::strong_ordering operator()(int64_t lhs, const primary_index_key& rhs) const noexcept {
        return dht::tri_compare_raw(lhs, rhs.token().raw());
    }

    std::strong_ordering operator()(const primary_index_key& lhs, int64_t rhs) const noexcept {
        return dht::tri_compare_raw(lhs.token().raw(), rhs);
    }

    std::strong_ordering operator()(const primary_index_entry& lhs, const primary_index_entry& rhs) const noexcept;
    std::strong_ordering operator()(const primary_index_entry& lhs, const primary_index_key& rhs) const noexcept;
    std::strong_ordering operator()(const primary_index_key& lhs, const primary_index_entry& rhs) const noexcept;
    std::strong_ordering operator()(const primary_index_entry& lhs, int64_t rhs) const noexcept;
    std::strong_ordering operator()(int64_t lhs, const primary_index_entry& rhs) const noexcept;
};

// One entry in the primary index B+tree.
//
// In addition to the on-disk location (index_entry), each entry may hold a
// pointer to a cached_mutation_entry that lives in the logstor_cache_tracker's
// LSA region.  When the cache evicts the entry under memory pressure it zeroes
// _cached_entry via the back-pointer stored inside cached_mutation_entry.
class primary_index_entry {
    primary_index_key _key;
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

    primary_index_entry(primary_index_key key, index_entry e)
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
        return sizeof(primary_index_entry);
    }

    bool is_head() const noexcept { return _flags._head; }
    void set_head(bool v) noexcept { _flags._head = v; }
    bool is_tail() const noexcept { return _flags._tail; }
    void set_tail(bool v) noexcept { _flags._tail = v; }
    bool with_train() const noexcept { return _flags._train; }
    void set_train(bool v) noexcept { _flags._train = v; }

    const primary_index_key& key() const noexcept { return _key; }
    const index_entry& entry() const noexcept { return _e; }

    friend class primary_index;

};

inline std::strong_ordering primary_index_key_cmp::operator()(const primary_index_entry& lhs, const primary_index_entry& rhs) const noexcept {
    return lhs.key() <=> rhs.key();
}

inline std::strong_ordering primary_index_key_cmp::operator()(const primary_index_entry& lhs, const primary_index_key& rhs) const noexcept {
    return lhs.key() <=> rhs;
}

inline std::strong_ordering primary_index_key_cmp::operator()(const primary_index_key& lhs, const primary_index_entry& rhs) const noexcept {
    return lhs <=> rhs.key();
}

inline std::strong_ordering primary_index_key_cmp::operator()(const primary_index_entry& lhs, int64_t rhs) const noexcept {
    return (*this)(lhs.key(), rhs);
}

inline std::strong_ordering primary_index_key_cmp::operator()(int64_t lhs, const primary_index_entry& rhs) const noexcept {
    return (*this)(lhs, rhs.key());
}

class primary_index final {
public:
    using partitions_type = double_decker<int64_t, primary_index_entry,
                            dht::raw_token_less_comparator, primary_index_key_cmp,
                            16, bplus::key_search::linear>;

    using iterator = typename partitions_type::iterator;
    using const_iterator = typename partitions_type::const_iterator;

    class token_entries {
        const_iterator _begin;
        const_iterator _end;

    public:
        token_entries(const_iterator begin, const_iterator end) noexcept
            : _begin(begin)
            , _end(end) {
        }

        dht::token token() const noexcept {
            return _begin->key().token();
        }

        const_iterator begin() const noexcept {
            return _begin;
        }

        const_iterator end() const noexcept {
            return _end;
        }
    };

    class token_range_scan {
        const primary_index* _index;
        const_iterator _begin;
        const_iterator _end;

    public:
        class iterator {
            const primary_index* _index;
            const_iterator _current;
            const_iterator _end;

            token_entries current_token_entries() const {
                return token_entries(_current, _index->upper_bound(_current->key().token()));
            }

        public:
            using value_type = token_entries;
            using difference_type = std::ptrdiff_t;

            iterator(const primary_index* index, const_iterator current, const_iterator end) noexcept
                : _index(index)
                , _current(current)
                , _end(end) {
            }

            value_type operator*() const {
                return current_token_entries();
            }

            iterator& operator++() {
                _current = current_token_entries().end();
                return *this;
            }

            bool operator==(const iterator& other) const noexcept = default;
        };

        token_range_scan(const primary_index& index, const_iterator begin, const_iterator end) noexcept
            : _index(&index)
            , _begin(begin)
            , _end(end) {
        }

        iterator begin() const noexcept {
            return iterator(_index, _begin, _end);
        }

        iterator end() const noexcept {
            return iterator(_index, _end, _end);
        }
    };

private:
    partitions_type _partitions;
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

    auto make_entry_disposer() noexcept {
        return [this] (primary_index_entry* e) noexcept {
            if (_cache_tracker) {
                _cache_tracker->evict(*e);
            }
            on_entry_removed(*e);
        };
    }

    future<> erase_range_gently(partitions_type::iterator begin, const partitions_type::iterator end) {
        static constexpr size_t chunk_size = 1024;
        auto dispose = make_entry_disposer();
        while (begin != end) {
            auto chunk_end = begin;
            for (size_t i = 0; i < chunk_size && chunk_end != end; ++i, ++chunk_end);
            begin = _partitions.erase_and_dispose(begin, chunk_end, dispose);
            co_await coroutine::maybe_yield();
        }
    }

    auto find_key(this auto& self, const primary_index_key& key) {
        return self._partitions.find(key, primary_index_key_cmp{});
    }

    auto position_at_range_start(this auto& self, const dht::token_range& tr) {
        return tr.start()
                ? (tr.start()->is_inclusive()
                    ? self._partitions.lower_bound(tr.start()->value().raw(), primary_index_key_cmp{})
                    : self._partitions.upper_bound(tr.start()->value().raw(), primary_index_key_cmp{}))
                : self._partitions.begin();
    }

    auto position_at_range_end(this auto& self, const dht::token_range& tr) {
        return tr.end()
                ? (tr.end()->is_inclusive()
                    ? self._partitions.upper_bound(tr.end()->value().raw(), primary_index_key_cmp{})
                    : self._partitions.lower_bound(tr.end()->value().raw(), primary_index_key_cmp{}))
                : self._partitions.end();
    }

public:
    explicit primary_index(schema_ptr schema)
        : _partitions(dht::raw_token_less_comparator{})
        {}

    void set_schema(schema_ptr s) {
        (void)s;
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

    utils::phased_barrier::operation start_read() const {
        return _reads_phaser.start();
    }

    future<> await_pending_reads() {
        return _reads_phaser.advance_and_await();
    }

    const_iterator find(const primary_index_key& key) const {
        return find_key(key);
    }

    const_iterator begin() const noexcept {
        return _partitions.begin();
    }

    const_iterator end() const noexcept {
        return _partitions.end();
    }

    const_iterator upper_bound(dht::token token) const {
        return _partitions.upper_bound(token.raw(), primary_index_key_cmp{});
    }

    token_range_scan scan(const dht::token_range& tr) const {
        return token_range_scan(*this, position_at_range_start(tr), position_at_range_end(tr));
    }

    std::optional<index_entry> get(const primary_index_key& key) const {
        auto it = find_key(key);
        if (it != _partitions.end()) {
            return it->_e;
        }
        return std::nullopt;
    }

    bool is_record_alive(const primary_index_key& key, log_location location) const {
        auto it = find_key(key);
        if (it != _partitions.end()) {
            return it->_e.location == location;
        } else {
            return false;
        }
    }

    bool update_record_location(const primary_index_key& key, log_location old_location, log_location new_location) {
        auto it = find_key(key);
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
        auto i = _partitions.lower_bound(key, primary_index_key_cmp{}, hint);
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
            auto it = _partitions.emplace_before(i, key.token().raw(), hint, key, std::move(new_entry));
            on_entry_added(*it);
            return {true, std::nullopt};
        }
    }

    bool erase(const primary_index_key& key, log_location loc) {
        auto it = find_key(key);
        if (it != _partitions.end() && it->_e.location == loc) {
            it.erase_and_dispose(dht::raw_token_less_comparator{}, make_entry_disposer());
            return true;
        }
        return false;
    }

    future<> erase(dht::token_range tr) {
        co_await erase_range_gently(position_at_range_start(tr), position_at_range_end(tr));
    }

    future<> clear() {
        co_await erase_range_gently(_partitions.begin(), _partitions.end());

        if (_key_count != 0 || _memory_usage != 0) {
            on_internal_error(logstor_logger, format("primary_index::clear ended with key_count {} and memory_usage {}", _key_count, _memory_usage));
        }
    }

    bool empty() const noexcept { return _partitions.empty(); }
    size_t get_key_count() const noexcept { return _key_count; }
    size_t get_memory_usage() const noexcept { return _memory_usage; }

};

}
