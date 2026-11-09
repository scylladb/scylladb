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
    primary_index_entry(primary_index_key key, index_entry e)
        : _key(std::move(key))
        , _e(std::move(e))
    { }

    ~primary_index_entry() {
        if (_cached_entry) {
            on_fatal_internal_error(logstor_logger, "primary_index_entry destroyed while having a cache entry");
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
            on_fatal_internal_error(logstor_logger, "primary_index_entry move-assignment overwrote a live cached entry");
        }
        _key = std::move(other._key);
        _e = std::move(other._e);
        _cached_entry = std::move(other._cached_entry);
        _flags = other._flags;
        return *this;
    }

    bool is_head() const noexcept { return _flags._head; }
    void set_head(bool v) noexcept { _flags._head = v; }
    bool is_tail() const noexcept { return _flags._tail; }
    void set_tail(bool v) noexcept { _flags._tail = v; }
    bool with_train() const noexcept { return _flags._train; }
    void set_train(bool v) noexcept { _flags._train = v; }

    const primary_index_key& key() const noexcept { return _key; }
    const index_entry& entry() const noexcept { return _e; }

    friend class cache_tracker;
    friend class primary_index;
};

struct primary_index_key_cmp {
    std::strong_ordering operator()(const primary_index_key& lhs, const primary_index_key& rhs) const noexcept {
        return lhs <=> rhs;
    }
    std::strong_ordering operator()(int64_t lhs, const primary_index_key& rhs) const noexcept {
        return dht::tri_compare_raw(lhs, rhs.raw_token().value);
    }
    std::strong_ordering operator()(const primary_index_key& lhs, int64_t rhs) const noexcept {
        return dht::tri_compare_raw(lhs.raw_token().value, rhs);
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

// The counts of one primary index at the time of a call. The sizes are in bytes.
struct primary_index_usage {
    // The number of keys that the index holds.
    size_t key_count{0};
    // The memory of the entries alone, without the nodes of the B+tree.
    size_t entries_memory_usage{0};
    // The memory of the B+tree, with its nodes and the entries in them.
    size_t memory_usage{0};
    // The sum of the sizes of the records that the index points at.
    uint64_t live_record_bytes{0};
};

// The primary_index class maps each key of one table on one shard to the location and the
// timestamp of the newest record of the key, and it keeps the entries in a B+tree in index key
// order. An entry can also link to the cached partition of its key in the logstor cache.
//
// The index keeps two rules:
// - Every change of a location goes to the space accounting, so that the live record bytes of
//   each segment follow the index.
// - A location from a lookup or a scan stays readable while the read guard that came with the
//   location exists.
class primary_index final {
    using tree_type = double_decker<int64_t, primary_index_entry,
                            dht::raw_token_less_comparator, primary_index_key_cmp,
                            16, bplus::key_search::linear>;
    using iterator = typename tree_type::iterator;

    counting_allocation_strategy _tree_allocator{standard_allocator(), logstor_logger};
    tree_type _entries;
    size_t _key_count = 0;
    uint64_t _live_record_bytes = 0;

    // Each read guard holds an operation of this phaser, so that await_pending_reads() can wait
    // for the guards that exist.
    mutable utils::phased_barrier _reads_phaser{"logstor_primary_index"};

    // Non-owning pointer to the cache tracker, which is null when the cache is not set up.
    cache_tracker* _cache_tracker = nullptr;
    // The caching option of the table, which controls only lookups and populates. Evicts depend
    // only on `_cache_tracker`, so a partition that stays cached while this is false stays correct.
    bool _cache_enabled = false;
    // Set by the first populate, so that drain_cache() does not scan an index that never had a
    // cached partition. When it is true, `_cache_tracker` is set.
    mutable bool _cache_used = false;

    // Gets the record added, record freed and record moved events of the index. The segment
    // manager is the only subscriber.
    space_accounting_subscriber& _space_accounting;

    // Returns a reference to the entry of the key in the tree, or nullopt if the index does not
    // have the key. The reference is valid until the next insert into or erase from the tree.
    auto find_entry(this auto& self, const primary_index_key& key) -> std::optional<decltype(*self._entries.begin())> {
        auto it = self._entries.find(key, primary_index_key_cmp{});
        if (it == self._entries.end()) {
            return std::nullopt;
        }
        return *it;
    }

    // Returns a reference to the entry of the key only if the entry still holds `location`, or
    // nullopt. The reference has the same lifetime as the reference that find_entry() returns.
    auto find_entry_at(this auto& self, const primary_index_key& key, record_location location) -> decltype(self.find_entry(key)) {
        auto e = self.find_entry(key);
        if (!e || e->_e.location != location) {
            return std::nullopt;
        }
        return e;
    }

    bool reads_cache() const noexcept {
        return _cache_tracker && _cache_enabled;
    }

    index_read_guard start_read() const {
        return index_read_guard(_reads_phaser.start());
    }

public:
    explicit primary_index(space_accounting_subscriber& space_accounting, cache_tracker* ct, bool cache_enabled)
        : _entries(dht::raw_token_less_comparator{})
        , _cache_tracker(ct)
        , _cache_enabled(cache_enabled)
        , _space_accounting(space_accounting)
        {}

    ~primary_index() {
        with_allocator(_tree_allocator, [this] { _entries.clear(); });
    }

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
        tree_type::bound_hint hint;
        auto i = _entries.lower_bound(key, primary_index_key_cmp{}, hint);
        if (hint.match) {
            if (cmp(i->_e, new_entry) <= 0) {
                // Overwriting with newer data: evict the stale cached partition.
                if (i->_cached_entry) {
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
            return _entries.emplace_before(i, key.raw_token().value, hint, key, std::move(new_entry));
        });
        space_accounting_on_record_added(it->_e.location);
        ++_key_count;
        return {insert_result::inserted, std::nullopt};
    }

    bool update_record_location(const primary_index_key& key, record_location old_location, record_location new_location) {
        auto e = find_entry_at(key, old_location);
        if (!e) {
            return false;
        }
        e->_e.location = new_location;
        space_accounting_on_record_moved(old_location, new_location);
        // The cached partition is still valid (same data, new location on
        // disk after compaction moved it) — do not evict the cache here.
        return true;
    }

    bool erase(const primary_index_key& key, record_location loc) {
        auto e = find_entry_at(key, loc);
        if (!e) {
            return false;
        }
        with_allocator(_tree_allocator, [&] {
            iterator(&*e).erase_and_dispose(dht::raw_token_less_comparator{}, [this] (primary_index_entry* erased) noexcept { dispose_entry(erased); });
        });
        return true;
    }

    // Erases the entries of the token range a page at a time, with a yield between the pages. Each
    // page seeks both of its ends, so that no iterator lives from one page to the next.
    future<> erase(dht::token_range tr) {
        static constexpr size_t page_size = default_scan_batch_size;
        auto dispose = [this] (primary_index_entry* e) noexcept { dispose_entry(e); };
        std::optional<primary_index_key> next_key;
        while (true) {
            const bool done = with_allocator(_tree_allocator, [&] {
                auto begin = next_key ? _entries.lower_bound(*next_key, primary_index_key_cmp{}) : position_at_range_start(tr);
                auto end = position_at_range_end(tr);

                auto page_end = begin;
                for (size_t i = 0; i < page_size && page_end != end; ++i, ++page_end);

                const bool last_page = page_end == end;
                if (!last_page) {
                    next_key = page_end->key();
                }
                _entries.erase_and_dispose(begin, page_end, dispose);
                return last_page;
            });
            if (done) {
                co_return;
            }
            co_await coroutine::maybe_yield();
        }
    }

    future<> clear() {
        return erase(dht::token_range());
    }

    // A record location and the guard that keeps the location readable.
    struct guarded_location {
        record_location location;
        index_read_guard guard;
    };

    // The result of a point lookup: the cached partition with its key, or the location of the
    // record if the cache does not have it.
    using read_lookup_result = std::variant<keyed_single_row_partition, guarded_location>;

    // Finds a key for a point read. Returns nullopt if the index does not have the key. If the
    // read uses the cache and the cache has the key, returns the cached partition with its key. If
    // not, returns the location of the record with a guard. The entry has only the hash of the key,
    // so the caller compares the key of a cached partition with its own.
    std::optional<read_lookup_result> lookup_for_read(const primary_index_key& key, const schema& target_schema, use_cache uses_cache) const {
        return find_entry(key).transform([&] (const primary_index_entry& e) -> read_lookup_result {
            if (reads_cache() && uses_cache) {
                if (auto cached = _cache_tracker->read_partition(e, target_schema)) {
                    return std::move(*cached);
                }
            }
            return guarded_location{.location = e.entry().location, .guard = start_read()};
        });
    }

    struct dump_lookup_result {
        index_entry entry;
        std::optional<mutation> cached_mutation;
    };

    // Looks up a key for MUTATION_FRAGMENTS(): returns its index entry and, when the key
    // is cached, a snapshot of the cached mutation. Unlike the read path this neither
    // touches the cache's LRU nor counts a cache hit or miss, so that inspecting a table
    // does not perturb its cache.
    std::optional<dump_lookup_result> lookup_for_dump(const primary_index_key& key, schema_ptr target_schema) const {
        return find_entry(key).transform([&] (const primary_index_entry& e) {
            return dump_lookup_result{
                .entry = e.entry(),
                .cached_mutation = _cache_tracker ? _cache_tracker->peek(e, std::move(target_schema)) : std::nullopt,
            };
        });
    }

    bool is_record_alive(const primary_index_key& key, record_location location) const {
        return find_entry_at(key, location).has_value();
    }

    bool populate_cache(const primary_index_key& key, record_location read_location, const schema_ptr& schema, const partition_key& pk,
            const single_row_partition& partition) const {
        if (!reads_cache()) {
            return false;
        }

        auto e = find_entry_at(key, read_location);
        if (!e) {
            return false;
        }
        _cache_used = true;
        _cache_tracker->populate(*e, schema, pk, partition);
        return true;
    }

    future<> await_pending_reads() {
        return _reads_phaser.advance_and_await();
    }

    // The number of entries in one batch of a scan whose caller has no reason to choose another
    // number, and in one page of a range erase. A batch or a page runs without a yield, so this
    // number also limits the time of one batch or page.
    static constexpr size_t default_scan_batch_size = 1024;

    class token_range_scan;

    // A scanned_entry gives the visit of a scan access to one entry of the index. The entry is
    // valid only inside the visit, so a scanned_entry cannot be copied or moved, and only the scan
    // can create one. It returns the key and the index entry as copies, and it reads the cached
    // partition through the index that holds the entry.
    class scanned_entry {
        const primary_index& _index;
        const primary_index_entry& _e;

        scanned_entry(const primary_index& index, const primary_index_entry& e) noexcept : _index(index), _e(e) {}

        friend class token_range_scan;
        friend class primary_index;
    public:
        scanned_entry(const scanned_entry&) = delete;
        scanned_entry& operator=(const scanned_entry&) = delete;

        primary_index_key key() const noexcept { return _e.key(); }
        index_entry entry() const noexcept { return _e.entry(); }

        // Returns the cached partition of the entry, with its key. Returns nullopt when the cache
        // does not hold the entry or when the index does not read the cache. Counts a hit or a miss
        // and touches the LRU, like a point read does.
        std::optional<keyed_single_row_partition> read_cached(const schema& target_schema) const {
            if (!_index.reads_cache()) {
                return std::nullopt;
            }
            return _index._cache_tracker->read_partition(_e, target_schema);
        }
    };

    // Stateful cursor over a token range in the primary index.
    //
    // This type intentionally does not expose index iterators. Call `next_batch()` repeatedly
    // to consume the scan in primary_index_key order: first by token, then by the key hash
    // order for keys that share the same token. The scan remembers the last fully visited
    // token and, on the next call, re-seeks from that resume point instead of keeping raw
    // iterators alive.
    //
    // Use it like this:
    //   - construct it with `index.scan()` or `index.scan(range)`;
    //   - call `next_batch(max_entries, visit)` with the maximum number of entries you want to
    //     handle in one batch, knowing that the scan will never split a token run, and a `visit`
    //     that takes what it needs from each entry.
    //
    // The scan is not a snapshot. Later batches observe whatever is currently in the index.
    class token_range_scan {
        const primary_index* _index;
        dht::token_range _range;
        std::optional<dht::token> _last_token;
        bool _exhausted = false;

        dht::token_range unread_range() const {
            if (!_last_token) {
                return _range;
            }
            return dht::token_range(
                    dht::token_range::bound(*_last_token, false),
                    _range.end());
        }

    public:
        // A batch describes the entries that one call of `next_batch()` visited.
        //
        // `guard` keeps the locations of the visited entries readable. Hold it until all reads
        // of the records are complete.
        //
        // Each batch contains all entries for every token it includes. If adding the next token
        // would exceed `max_entries`, the scan stops before that token instead of breaking the
        // token group in half (except if the first token itself exceeds `max_entries`, in which
        // case the batch will contain only that token).
        struct batch {
            index_read_guard guard;
            dht::token first_token;
            dht::token last_token;
        };

        token_range_scan(const primary_index& index, dht::token_range range) noexcept
            : _index(&index)
            , _range(std::move(range)) {
        }

        // Visits the entries of the next batch in index order, with `visit(const scanned_entry&)`,
        // and returns what the batch covered, or nullopt at the end of the range. An entry is valid
        // inside `visit` and not after it, so `visit` copies what it needs. `visit` must not yield
        // and must not change the index.
        template <typename Visit>
        requires std::invocable<Visit, const scanned_entry&>
                && std::is_void_v<std::invoke_result_t<Visit, const scanned_entry&>>
        std::optional<batch> next_batch(size_t max_entries, Visit&& visit) {
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
            };

            size_t entry_count = 0;
            while (current != end) {
                auto token = current->key().token();
                auto token_end = current;
                size_t token_entry_count = 0;
                while (token_end != end && token_end->key().token() == token) {
                    ++token_end;
                    ++token_entry_count;
                }
                if (entry_count != 0 && entry_count + token_entry_count > max_entries) {
                    break;
                }

                next.last_token = token;
                for (; current != token_end; ++current) {
                    visit(scanned_entry(*_index, *current));
                }
                entry_count += token_entry_count;
            }

            if (current == end) {
                _exhausted = true;
                _last_token.reset();
            } else {
                _last_token = next.last_token;
            }

            return next;
        }
    };

    token_range_scan scan(const dht::token_range& tr = dht::token_range()) const {
        return token_range_scan(*this, tr);
    }

    void set_cache_enabled(bool enabled) noexcept {
        _cache_enabled = enabled;
    }

    future<> drain_cache() {
        if (_cache_used) {
            auto scan = this->scan();
            while (scan.next_batch(default_scan_batch_size, [this] (const scanned_entry& e) { _cache_tracker->evict(e._e); })) {
                co_await coroutine::maybe_yield();
            }
        }
    }

    // Returns what the index holds now.
    primary_index_usage get_usage() const noexcept {
        return {
            .key_count = _key_count,
            .entries_memory_usage = _key_count * sizeof(primary_index_entry),
            .memory_usage = _tree_allocator.allocated_memory(),
            .live_record_bytes = _live_record_bytes,
        };
    }

private:
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

    void space_accounting_on_record_moved(record_location from, record_location to) noexcept {
        if (from.size > _live_record_bytes) {
            on_fatal_internal_error(logstor_logger, fmt::format("moving a record of {} bytes in an index holding {} live record bytes",
                    from.size, _live_record_bytes));
        }
        _space_accounting.on_move_record(from, to);
        _live_record_bytes += to.size;
        _live_record_bytes -= from.size;
    }

    // Evicts the cached partition of an entry that the tree erases, and frees its record in the
    // accounting.
    void dispose_entry(primary_index_entry* e) noexcept {
        if (e->_cached_entry) {
            _cache_tracker->evict(*e);
        }
        space_accounting_on_record_freed(e->_e.location);
        --_key_count;
    }

    auto position_at_range_start(this auto& self, const dht::token_range& tr) -> decltype(self._entries.begin()) {
        return tr.start()
                ? (tr.start()->is_inclusive()
                    ? self._entries.lower_bound(tr.start()->value().raw(), primary_index_key_cmp{})
                    : self._entries.upper_bound(tr.start()->value().raw(), primary_index_key_cmp{}))
                : self._entries.begin();
    }

    auto position_at_range_end(this auto& self, const dht::token_range& tr) -> decltype(self._entries.end()) {
        return tr.end()
                ? (tr.end()->is_inclusive()
                    ? self._entries.upper_bound(tr.end()->value().raw(), primary_index_key_cmp{})
                    : self._entries.lower_bound(tr.end()->value().raw(), primary_index_key_cmp{}))
                : self._entries.end();
    }
};

}
