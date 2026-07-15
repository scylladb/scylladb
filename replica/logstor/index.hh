/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#pragma once

#include "dht/decorated_key.hh"
#include "dht/ring_position.hh"
#include <functional>
#include <seastar/coroutine/maybe_yield.hh>
#include <optional>
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
        return sizeof(primary_index_entry) + _key.dk.external_memory_usage();
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

inline dht::ring_position_view ring_position_view_to_compare(const primary_index_key& key) {
    return key.dk;
}

struct primary_index_key_cmp {

    dht::ring_position_comparator cmp;

    primary_index_key_cmp(const schema& s) : cmp(s) {}

    std::strong_ordering operator()(const primary_index_key& lhs, const primary_index_key& rhs) const noexcept {
        return cmp(lhs.dk, rhs.dk);
    }
    std::strong_ordering operator()(int64_t lhs, const primary_index_key& rhs) const noexcept {
        return dht::tri_compare_raw(lhs, rhs.token().raw());
    }
    std::strong_ordering operator()(const primary_index_key& lhs, int64_t rhs) const noexcept {
        return dht::tri_compare_raw(lhs.token().raw(), rhs);
    }
    std::strong_ordering operator()(const primary_index_entry& lhs, const primary_index_entry& rhs) const noexcept {
        return cmp(lhs.key().dk, rhs.key().dk);
    }
    std::strong_ordering operator()(const primary_index_entry& lhs, const primary_index_key& rhs) const noexcept {
        return cmp(lhs.key().dk, rhs.dk);
    }
    std::strong_ordering operator()(const primary_index_key& lhs, const primary_index_entry& rhs) const noexcept {
        return cmp(lhs.dk, rhs.key().dk);
    }
    std::strong_ordering operator()(const primary_index_entry& lhs, int64_t rhs) const noexcept {
        return (*this)(lhs.key(), rhs);
    }
    std::strong_ordering operator()(int64_t lhs, const primary_index_entry& rhs) const noexcept {
        return (*this)(lhs, rhs.key());
    }
    std::strong_ordering operator()(const primary_index_entry& lhs, const dht::ring_position_view& rhs) const noexcept {
        return cmp(lhs.key().dk, rhs);
    }
    std::strong_ordering operator()(const dht::ring_position_view& lhs, const primary_index_entry& rhs) const noexcept {
        return cmp(lhs, rhs.key().dk);
    }
};

class primary_index final {
public:
    using partitions_type = double_decker<int64_t, primary_index_entry,
                            dht::raw_token_less_comparator, primary_index_key_cmp,
                            16, bplus::key_search::linear>;

    using entry_reference = std::reference_wrapper<const primary_index_entry>;

    struct read_lookup_result {
        index_entry entry;
        std::optional<mutation_partition> cached_mutation;
    };

private:
    using iterator = typename partitions_type::iterator;
    using const_iterator = typename partitions_type::const_iterator;

public:

    // Stateful cursor over a token range in the primary index.
    //
    // This type intentionally does not expose index iterators. Call `next_batch()` repeatedly
    // to consume the scan in token order. The scan remembers the last fully returned token and,
    // on the next call, re-seeks from that resume point instead of keeping raw iterators alive.
    //
    // Use it like this:
    //   - construct it with `index.scan()` or `index.scan(range)`;
    //   - call `next_batch(max_entries)` with the maximum number of entries you want to handle
    //     in one batch, knowing that the scan will never split a token run;
    //   - consume every entry in the returned batch synchronously;
    //   - if you yield, call `next_batch()` again after resuming.
    //
    // The scan is not a snapshot. Later batches observe whatever is currently in the index.
    class token_range_scan {
        const primary_index* _index;
        dht::token_range _range;
        std::optional<dht::token> _last_token;
        bool _exhausted = false;

    public:
        // A contiguous chunk of index entries returned by `next_batch()`.
        //
        // `entries` are references to entries currently stored in the index, so they are only
        // meant for immediate consumption. Do not keep them across yields or index mutations.
        // The scan only preserves token position, not entry handles.
        //
        // `first_token` and `last_token` are useful for tracing and resume bookkeeping.
        // `entry_count` is the number of entries in `entries`.
        // `exhausted` tells whether this batch reached the end of the requested range.
        //
        // Each batch contains all entries for every token it includes. If adding the next token
        // would exceed `max_entries`, the scan stops before that token instead of breaking the
        // token group in half (except if the first token itself exceeds `max_entries`, in which
        // case the batch will contain only that token).
        struct batch {
            utils::small_vector<entry_reference, 16> entries;
            dht::token first_token;
            dht::token last_token;
            bool exhausted;
            size_t entry_count;
        };

        token_range_scan(const primary_index& index, dht::token_range range) noexcept
            : _index(&index)
            , _range(std::move(range)) {
        }

        dht::token_range unread_range() const {
            if (!_last_token) {
                return _range;
            }
            return dht::token_range(
                    dht::token_range::bound(*_last_token, false),
                    _range.end());
        }

        bool exhausted() const noexcept {
            return _exhausted;
        }

        void reset() noexcept {
            _last_token.reset();
            _exhausted = false;
        }

        std::optional<batch> next_batch(size_t max_entries) {
            if (_exhausted) {
                return std::nullopt;
            }

            auto current = _index->position_at_range_start(unread_range());
            auto end = _index->position_at_range_end(_range);
            if (current == end) {
                _exhausted = true;
                _last_token.reset();
                return std::nullopt;
            }

            batch next{
                .first_token = current->key().token(),
                .last_token = current->key().token(),
                .exhausted = false,
                .entry_count = 0,
            };

            while (current != end) {
                auto token = current->key().token();
                auto token_end = _index->upper_bound(token);
                if (_range.end() && _range.end()->value() == token) {
                    token_end = end;
                }

                auto token_entry_count = size_t(std::distance(current, token_end));
                if (next.entry_count != 0 && next.entry_count + token_entry_count > max_entries) {
                    break;
                }

                next.last_token = token;
                next.entries.reserve(next.entries.size() + token_entry_count);
                for (; current != token_end; ++current) {
                    next.entries.push_back(std::cref(*current));
                }
                next.entry_count += token_entry_count;
            }

            next.exhausted = current == end;
            if (next.exhausted) {
                _exhausted = true;
                _last_token.reset();
            } else {
                _last_token = next.last_token;
            }

            return next;
        }
    };

private:
    partitions_type _partitions;
    schema_ptr _schema;
    size_t _key_count = 0;
    size_t _memory_usage = 0;

    mutable utils::phased_barrier _reads_phaser{"logstor_primary_index"};

    // Non-owning pointer to the cache tracker; null when the cache is not set up.
    // Mutable so that logically-const methods can call non-const methods on the
    // tracker (touch, insert) for LRU accounting.
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

    future<> erase_range_gently(partitions_type::iterator begin, auto&& end_fn) {
        static constexpr size_t chunk_size = 1024;
        auto dispose = make_entry_disposer();
        while (true) {
            auto end = end_fn();

            auto chunk_end = begin;
            for (size_t i = 0; i < chunk_size && chunk_end != end; ++i, ++chunk_end);

            if (chunk_end == end) {
                _partitions.erase_and_dispose(begin, chunk_end, dispose);
                co_return;
            }

            auto next_key = chunk_end->key();
            _partitions.erase_and_dispose(begin, chunk_end, dispose);
            co_await coroutine::maybe_yield();
            begin = _partitions.lower_bound(next_key, primary_index_key_cmp(*_schema));
        }
    }

    auto find_key(this auto& self, const primary_index_key& key) {
        return self._partitions.find(key, primary_index_key_cmp{*self._schema});
    }

    auto position_at_range_start(this auto& self, const dht::token_range& tr) -> decltype(self._partitions.begin()) {
        return tr.start()
                ? (tr.start()->is_inclusive()
                    ? self._partitions.lower_bound(tr.start()->value().raw(), primary_index_key_cmp{*self._schema})
                    : self._partitions.upper_bound(tr.start()->value().raw(), primary_index_key_cmp{*self._schema}))
                : self._partitions.begin();
    }

    auto position_at_range_end(this auto& self, const dht::token_range& tr) -> decltype(self._partitions.end()) {
        return tr.end()
                ? (tr.end()->is_inclusive()
                    ? self._partitions.upper_bound(tr.end()->value().raw(), primary_index_key_cmp{*self._schema})
                    : self._partitions.lower_bound(tr.end()->value().raw(), primary_index_key_cmp{*self._schema}))
                : self._partitions.end();
    }

    const_iterator upper_bound(dht::token token) const {
        return _partitions.upper_bound(token.raw(), primary_index_key_cmp{*_schema});
    }

public:
    explicit primary_index(schema_ptr schema, cache_tracker* ct)
        : _partitions(dht::raw_token_less_comparator{})
        , _schema(std::move(schema))
        , _cache_tracker(ct)
        {}

    void set_schema(schema_ptr s) {
        _schema = std::move(s);
    }

    future<> drain_cache() {
        if (_cache_tracker) {
            auto scan = this->scan();
            while (auto batch = scan.next_batch(1024)) {
                for (const auto& pie : batch->entries) {
                    _cache_tracker->evict(pie.get());
                }
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

    token_range_scan scan(const dht::token_range& tr = dht::token_range()) const {
        return token_range_scan(*this, tr);
    }

    std::optional<index_entry> get(const primary_index_key& key) const {
        auto it = find_key(key);
        if (it != _partitions.end()) {
            return it->_e;
        }
        return std::nullopt;
    }

    std::optional<read_lookup_result> lookup_for_read(const primary_index_key& key, schema_ptr target_schema, bool cache_enabled) const {
        auto it = find_key(key);
        if (it == _partitions.end()) {
            return std::nullopt;
        }

        read_lookup_result result{.entry = it->entry()};
        if (cache_enabled && _cache_tracker) {
            result.cached_mutation = _cache_tracker->lookup(*it, std::move(target_schema));
        }
        return result;
    }

    bool populate_cache(const primary_index_key& key, const mutation& m) const {
        auto it = find_key(key);
        if (it == _partitions.end() || !_cache_tracker) {
            return false;
        }

        _cache_tracker->populate(*it, m);
        return true;
    }

    bool populate_cache(const primary_index_key& key, log_location read_location, const mutation& m) const {
        auto it = find_key(key);
        if (it == _partitions.end() || !_cache_tracker || it->entry().location != read_location) {
            return false;
        }

        _cache_tracker->populate(*it, m);
        return true;
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
        auto i = _partitions.lower_bound(key, primary_index_key_cmp(*_schema), hint);
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
        co_await erase_range_gently(
                position_at_range_start(tr),
                [this, &tr] { return position_at_range_end(tr); }
            );
    }

    future<> clear() {
        co_await erase_range_gently(
                _partitions.begin(),
                [this] { return _partitions.end(); }
            );

        if (_key_count != 0 || _memory_usage != 0) {
            on_internal_error(logstor_logger, format("primary_index::clear ended with key_count {} and memory_usage {}", _key_count, _memory_usage));
        }
    }

    bool empty() const noexcept { return _partitions.empty(); }
    size_t get_key_count() const noexcept { return _key_count; }
    size_t get_memory_usage() const noexcept { return _memory_usage; }

};

}
