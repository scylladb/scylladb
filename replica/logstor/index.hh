/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#pragma once

#include "dht/i_partitioner.hh"
#include <functional>
#include <seastar/coroutine/maybe_yield.hh>
#include <optional>
#include "dht/token.hh"
#include "types.hh"
#include "utils/bptree.hh"
#include "utils/counting_allocation_strategy.hh"
#include "utils/double-decker.hh"
#include "utils/on_internal_error.hh"
#include "utils/phased_barrier.hh"
#include "utils/small_vector.hh"
#include <utility>
#include <variant>
#include <seastar/util/bool_class.hh>
#include "replica/logstor/cache.hh"

namespace replica::logstor {

extern seastar::logger logstor_logger;

// One entry in the primary index B+tree.
//
// In addition to the on-disk location (index_entry), each entry can have a
// cached partition: a cached_partition_entry in the LSA region of the shared
// cache tracker. _cached_entry links the entry to it. It is a cached_entry_slot,
// paired with the _slot_link of the cached_partition_entry. When the cache
// evicts the cached partition, it destroys the cached_partition_entry. This
// breaks the pair, and _cached_entry becomes empty.
class primary_index_entry {
    primary_index_key _key;
    index_entry _e;
    // Non-owning slot pointing into the shared cache region.
    // Empty when no cached partition exists for this key.
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
    std::strong_ordering operator()(const primary_index_entry& lhs, const primary_index_entry& rhs) const noexcept {
        return lhs.key() <=> rhs.key();
    }
    std::strong_ordering operator()(const primary_index_entry& lhs, const primary_index_key& rhs) const noexcept {
        return lhs.key() <=> rhs;
    }
    std::strong_ordering operator()(const primary_index_key& lhs, const primary_index_entry& rhs) const noexcept {
        return lhs <=> rhs.key();
    }
    std::strong_ordering operator()(const primary_index_entry& lhs, int64_t rhs) const noexcept {
        return (*this)(lhs.key(), rhs);
    }
    std::strong_ordering operator()(int64_t lhs, const primary_index_entry& rhs) const noexcept {
        return (*this)(lhs, rhs.key());
    }
};

// The segments of the locations that came with this guard stay allocated while the guard exists.
// Compaction and the separator move the records out of a segment and update the index. Then they
// wait for all guards that exist, and then they free the segment. Only the index makes a guard. It
// gives one with each location from a lookup or a scan. segment_manager::read() requires one.
class index_read_guard {
    utils::phased_barrier::operation _op;

    explicit index_read_guard(utils::phased_barrier::operation op) noexcept : _op(std::move(op)) {}

    friend class primary_index;
};

// Whether a read looks the cache up, and admits what it reads from a segment. A reader decides it
// once, from its slice: a slice with the bypass_cache option does neither.
using use_cache = bool_class<class use_cache_tag>;

class primary_index final {
public:
    using partitions_type = double_decker<int64_t, primary_index_entry,
                            dht::raw_token_less_comparator, primary_index_key_cmp,
                            16, bplus::key_search::linear>;

    using entry_reference = std::reference_wrapper<const primary_index_entry>;

    // A record location and the guard that keeps the location readable.
    struct guarded_location {
        record_location location;
        index_read_guard guard;
    };

    // The result of a point lookup: the cached partition with its key, or the location of the
    // record if the cache does not have it.
    using read_lookup_result = std::variant<keyed_single_row_partition, guarded_location>;

    struct dump_lookup_result {
        index_entry entry;
        std::optional<mutation> cached_mutation;
    };

    enum class insert_result {
        // The index now holds the new entry, either under a new key or over an older entry.
        inserted,
        // The index already holds a newer entry for this key; nothing changed.
        superseded,
    };

    struct insert_outcome {
        insert_result result;
        // The entry the index held for this key before the call, if it held one.
        std::optional<index_entry> previous_entry;

        bool inserted() const noexcept { return result == insert_result::inserted; }
    };

private:
    using iterator = typename partitions_type::iterator;
    using const_iterator = typename partitions_type::const_iterator;

public:

    // Stateful cursor over a token range in the primary index.
    //
    // This type intentionally does not expose index iterators. Call `next_batch()` repeatedly
    // to consume the scan in primary_index_key order: first by token, then by the key hash
    // order for keys that share the same token. The scan remembers the last fully returned
    // token and, on the next call, re-seeks from that resume point instead of keeping raw
    // iterators alive.
    //
    // Use it like this:
    //   - construct it with `index.scan()` or `index.scan(range)`;
    //   - call `next_batch(max_entries)` with the maximum number of entries you want to handle
    //     in one batch, knowing that the scan will never split a token run;
    //   - consume every entry in the returned batch synchronously; the batch is not usable after
    //     a yield.
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
        // The scan only saves token position, not entry handles.
        //
        // `guard` keeps the locations of the entries readable. Hold it until all reads of the
        // records are complete.
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
            index_read_guard guard;
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
                .guard = _index->start_read(),
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
    counting_allocation_strategy _tree_allocator{standard_allocator(), logstor_logger};
    partitions_type _partitions;
    size_t _key_count = 0;
    size_t _entries_memory_usage = 0;
    // Bytes of the records this index points at.
    uint64_t _live_record_bytes = 0;

    mutable utils::phased_barrier _reads_phaser{"logstor_primary_index"};

    // Non-owning pointer to the cache tracker; null when the cache is not set up.
    // Mutable so that logically-const methods can call non-const methods on the
    // tracker (touch, insert) for LRU accounting.
    mutable cache_tracker* _cache_tracker = nullptr;

    // Currently we support a single subscriber for space accounting which is the segment manager.
    space_accounting_subscriber& _space_accounting;

    void on_index_entry_inserted(const primary_index_entry& e) noexcept {
        _entries_memory_usage += e.memory_usage();
        ++_key_count;
    }

    void on_index_entry_erased(const primary_index_entry& e) noexcept {
        _entries_memory_usage -= e.memory_usage();
        --_key_count;
    }

    void space_accounting_on_record_added(record_location location) noexcept {
        _space_accounting.on_add_record(location);
        _live_record_bytes += location.size;
    }

    void space_accounting_on_record_freed(record_location location) noexcept {
        if (location.size > _live_record_bytes) {
            on_fatal_internal_error(logstor_logger, format("freeing a record of {} bytes from an index holding {} live record bytes",
                    location.size, _live_record_bytes));
        }
        _space_accounting.on_free_record(location);
        _live_record_bytes -= location.size;
    }

    auto make_entry_disposer() noexcept {
        return [this] (primary_index_entry* e) noexcept {
            with_allocator(standard_allocator(), [this, e] () noexcept {
                if (_cache_tracker) {
                    try {
                        _cache_tracker->evict(*e);
                    } catch (...) {}
                }
                space_accounting_on_record_freed(e->_e.location);
            });
            on_index_entry_erased(*e);
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
                with_allocator(_tree_allocator, [&] { _partitions.erase_and_dispose(begin, chunk_end, dispose); });
                co_return;
            }

            auto next_key = chunk_end->key();
            with_allocator(_tree_allocator, [&] { _partitions.erase_and_dispose(begin, chunk_end, dispose); });
            co_await coroutine::maybe_yield();
            begin = _partitions.lower_bound(next_key, primary_index_key_cmp{});
        }
    }

    auto find_key(this auto& self, const primary_index_key& key) {
        return self._partitions.find(key, primary_index_key_cmp{});
    }

    auto position_at_range_start(this auto& self, const dht::token_range& tr) -> decltype(self._partitions.begin()) {
        return tr.start()
                ? (tr.start()->is_inclusive()
                    ? self._partitions.lower_bound(tr.start()->value().raw(), primary_index_key_cmp{})
                    : self._partitions.upper_bound(tr.start()->value().raw(), primary_index_key_cmp{}))
                : self._partitions.begin();
    }

    auto position_at_range_end(this auto& self, const dht::token_range& tr) -> decltype(self._partitions.end()) {
        return tr.end()
                ? (tr.end()->is_inclusive()
                    ? self._partitions.upper_bound(tr.end()->value().raw(), primary_index_key_cmp{})
                    : self._partitions.lower_bound(tr.end()->value().raw(), primary_index_key_cmp{}))
                : self._partitions.end();
    }

    index_read_guard start_read() const {
        return index_read_guard(_reads_phaser.start());
    }

    const_iterator upper_bound(dht::token token) const {
        return _partitions.upper_bound(token.raw(), primary_index_key_cmp{});
    }

public:
    explicit primary_index(space_accounting_subscriber& space_accounting, cache_tracker* ct)
        : _partitions(dht::raw_token_less_comparator{})
        , _cache_tracker(ct)
        , _space_accounting(space_accounting)
        {}

    ~primary_index() {
        with_allocator(_tree_allocator, [this] { _partitions.clear(); });
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

    future<> await_pending_reads() {
        return _reads_phaser.advance_and_await();
    }

    token_range_scan scan(const dht::token_range& tr = dht::token_range()) const {
        return token_range_scan(*this, tr);
    }

    // Returns the entry of a key without a guard. Do not read from it.
    std::optional<index_entry> get(const primary_index_key& key) const {
        auto it = find_key(key);
        if (it != _partitions.end()) {
            return it->_e;
        }
        return std::nullopt;
    }

    // Looks up a key for MUTATION_FRAGMENTS(): returns its index entry and, when the key
    // is cached, a snapshot of the cached mutation. Unlike the read path this neither
    // touches the cache's LRU nor counts a cache hit or miss, so that inspecting a table
    // does not perturb its cache.
    std::optional<dump_lookup_result> lookup_for_dump(const primary_index_key& key, schema_ptr target_schema) const {
        auto it = find_key(key);
        if (it == _partitions.end()) {
            return std::nullopt;
        }

        dump_lookup_result result{.entry = it->entry()};
        if (_cache_tracker) {
            result.cached_mutation = _cache_tracker->peek(*it, std::move(target_schema));
        }
        return result;
    }

    // The cached partition of an entry the caller holds from scan(), with its key, or nullopt when
    // the entry holds nothing, the cache is not enabled or the read does not use it. Counts a hit
    // or a miss and touches the LRU, like a point read does. The entry must belong to this index.
    std::optional<keyed_single_row_partition> lookup_cached(const primary_index_entry& e, const schema& target_schema, use_cache uses_cache) const {
        if (!_cache_tracker || !uses_cache) {
            return std::nullopt;
        }
        return _cache_tracker->read_partition(e, target_schema);
    }

    // Finds a key for a point read. Returns nullopt if the index does not have the key. If the
    // read uses the cache and the cache has the key, returns the cached partition with its key. If
    // not, returns the location of the record with a guard. The entry has only the hash of the key,
    // so the caller compares the key of a cached partition with its own.
    std::optional<read_lookup_result> lookup_for_read(const primary_index_key& key, const schema& target_schema, use_cache uses_cache) const {
        auto it = find_key(key);
        if (it == _partitions.end()) {
            return std::nullopt;
        }

        if (_cache_tracker && uses_cache) {
            if (auto cached = _cache_tracker->read_partition(*it, target_schema)) {
                return std::move(*cached);
            }
        }
        return guarded_location{.location = it->entry().location, .guard = start_read()};
    }

    bool populate_cache(const primary_index_key& key, record_location read_location, const schema_ptr& schema, const partition_key& pk,
            const single_row_partition& partition) const {
        if (!_cache_tracker) {
            return false;
        }

        auto it = find_key(key);
        if (it == _partitions.end() || it->entry().location != read_location) {
            return false;
        }

        _cache_tracker->populate(*it, schema, pk, partition);
        return true;
    }

    bool is_record_alive(const primary_index_key& key, record_location location) const {
        auto it = find_key(key);
        if (it != _partitions.end()) {
            return it->_e.location == location;
        } else {
            return false;
        }
    }

    bool update_record_location(const primary_index_key& key, record_location old_location, record_location new_location) {
        auto it = find_key(key);
        if (it != _partitions.end()) {
            if (it->_e.location == old_location) {
                it->_e.location = new_location;
                space_accounting_on_record_freed(old_location);
                space_accounting_on_record_added(new_location);
                // The cached partition is still valid (same data, new location on
                // disk after compaction moved it) — do not evict the cache here.
                return true;
            }
        }
        return false;
    }

    // Which of two entries of the same key is the newer one. Every write of an existing key asks
    // this, so the default is a stateless functor and the comparator a template parameter: the
    // compare is then inlined, rather than a std::function built per insert to call it through.
    // Only recovery has a comparator of its own, and it captures state, which entry_cmp_fn is for.
    struct default_entry_cmp {
        std::strong_ordering operator()(const index_entry& a, const index_entry& b) const noexcept {
            return a.timestamp <=> b.timestamp;
        }
    };

    using entry_cmp_fn = std::function<std::strong_ordering(const index_entry&, const index_entry&)>;

    template <typename EntryCmp = default_entry_cmp>
    insert_outcome insert(const primary_index_key& key, index_entry new_entry, const EntryCmp& cmp = {}) {
        partitions_type::bound_hint hint;
        auto i = _partitions.lower_bound(key, primary_index_key_cmp{}, hint);
        if (hint.match) {
            if (cmp(i->_e, new_entry) <= 0) {
                // Overwriting with newer data: evict the stale cached partition.
                if (_cache_tracker) {
                    _cache_tracker->evict(*i);
                }
                auto old_entry = i->_e;
                i->_e = std::move(new_entry);
                space_accounting_on_record_freed(old_entry.location);
                space_accounting_on_record_added(i->_e.location);
                return {insert_result::inserted, std::make_optional(old_entry)};
            } else {
                return {insert_result::superseded, std::make_optional(i->_e)};
            }
        }

        auto it = with_allocator(_tree_allocator, [&] {
            return _partitions.emplace_before(i, key.token().raw(), hint, key, std::move(new_entry));
        });
        space_accounting_on_record_added(it->_e.location);
        on_index_entry_inserted(*it);
        return {insert_result::inserted, std::nullopt};
    }

    bool erase(const primary_index_key& key, record_location loc) {
        auto it = find_key(key);
        if (it != _partitions.end() && it->_e.location == loc) {
            with_allocator(_tree_allocator, [&] {
                it.erase_and_dispose(dht::raw_token_less_comparator{}, make_entry_disposer());
            });
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

        if (_key_count != 0 || _entries_memory_usage != 0 || _live_record_bytes != 0) {
            on_internal_error(logstor_logger, format("primary_index::clear ended with key_count {}, entries memory usage {}, memory usage {} and live_record_bytes {}",
                    _key_count, _entries_memory_usage, _tree_allocator.allocated_memory(), _live_record_bytes));
        }
    }

    bool empty() const noexcept { return _partitions.empty(); }
    size_t get_key_count() const noexcept { return _key_count; }
    // The entries alone, without the B+tree.
    size_t get_entries_memory_usage() const noexcept { return _entries_memory_usage; }
    // Everything the index holds, the B+tree's nodes and buckets included.
    size_t get_memory_usage() const noexcept { return _tree_allocator.allocated_memory(); }
    uint64_t get_live_record_bytes() const noexcept { return _live_record_bytes; }

    future<uint64_t> count_keys_in_token_range(dht::token_range tr) const {
        static constexpr size_t batch_size = 1024;
        uint64_t count = 0;
        auto s = scan(tr);
        // Keep only the count of a batch, so that its read guard is released before the yield and
        // does not make a segment free wait.
        while (auto n = s.next_batch(batch_size).transform([] (const auto& b) { return b.entry_count; })) {
            count += *n;
            co_await coroutine::maybe_yield();
        }
        co_return count;
    }

};

// Thrown when the record found at the indexed location holds a different key than the one that was looked up.
// This can happen if the hashes of the keys collide, or on corruption.
class key_mismatch_error : public std::runtime_error {
public:
    struct cache_location {};
    using location_variant = std::variant<record_location, cache_location>;

    key_mismatch_error(const partition_key& expected, const partition_key& actual, const location_variant& location)
        : std::runtime_error(format_message(expected, actual, location))
    { }

private:
    static std::string format_message(const partition_key& expected, const partition_key& actual, const location_variant& location) {
        return std::visit([&](auto&& loc) {
            using T = std::decay_t<decltype(loc)>;
            if constexpr (std::is_same_v<T, record_location>) {
                return format("logstor: key mismatch reading log entry at {}: expected {}, got {}",
                    loc, expected, actual);
            } else {
                return format("logstor: key mismatch reading from cache: expected {}, got {}", expected, actual);
            }
        }, location);
    }
};

}
