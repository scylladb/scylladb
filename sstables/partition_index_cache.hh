/*
 * Copyright (C) 2021 ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "index_entry.hh"
#include <seastar/core/future.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include "utils/assert.hh"
#include "utils/bptree.hh"
#include "utils/lru.hh"
#include "utils/lsa/weak_ptr.hh"
#include "sstables/partition_index_cache_stats.hh"

namespace sstables {

// Associative cache of summary index -> partition_index_page
// Entries stay around as long as there is any live external reference (entry_ptr) to them.
// Supports asynchronous insertion, ensures that only one entry will be loaded.
// Entries without a live entry_ptr are linked in the LRU.
// The instance must be destroyed only after all live_ptr:s are gone.
class partition_index_cache {
public:
    using key_type = uint64_t;
private:
    // Allocated inside LSA
    class entry final : public index_evictable, public lsa::weakly_referencable<entry> {
    public:
        partition_index_cache* _parent;
        key_type _key;
        std::variant<lw_shared_ptr<shared_promise<>>, partition_index_page> _page;
        size_t _size_in_allocator = 0;
    public:
        entry(partition_index_cache* parent, key_type key)
                : _parent(parent)
                , _key(key)
                , _page(make_lw_shared<shared_promise<>>())
        { }

        void set_page(partition_index_page&& page) noexcept {
            with_allocator(_parent->_region.allocator(), [&] {
                _size_in_allocator = sizeof(entry) + page.external_memory_usage();
            });
            _page = std::move(page);
        }

        entry(entry&&) noexcept = default;

        ~entry() {
            if (is_referenced()) {
                // Live entry_ptr should keep the entry alive, except when the entry failed on loading.
                // In that case, entry_ptr holders are not supposed to use the pointer, so it's safe
                // to nullify those entry_ptrs.
                SCYLLA_ASSERT(!ready());
            }
        }

        void on_evicted() noexcept override;

        // Returns the amount of memory owned by this entry.
        // Always returns the same value for a given state of _page.
        size_t size_in_allocator() const { return _size_in_allocator; }

        lw_shared_ptr<shared_promise<>> promise() { return std::get<lw_shared_ptr<shared_promise<>>>(_page); }
        bool ready() const { return std::holds_alternative<partition_index_page>(_page); }
        partition_index_page& page() { return std::get<partition_index_page>(_page); }
        const partition_index_page& page() const { return std::get<partition_index_page>(_page); }
        key_type key() const { return _key; }
    };
public:
    struct key_less_comparator {
        bool operator()(key_type lhs, key_type rhs) const noexcept {
            return lhs < rhs;
        }
    };

    // A shared pointer to cached partition_index_page.
    //
    // Prevents page from being evicted.
    // Never invalidated.
    // Can be accessed and destroyed in the standard allocator context.
    //
    // The partition_index_page reference obtained by dereferencing this pointer
    // is invalidated when the owning LSA region invalidates references.
    class entry_ptr {
        // *_ref is kept alive by the means of unlinking from LRU.
        lsa::weak_ptr<entry> _ref;
    private:
        friend class partition_index_cache;
        entry& get_entry() { return *_ref; }
    public:
        using element_type = partition_index_page;
        entry_ptr() = default;
        explicit entry_ptr(lsa::weak_ptr<entry> ref)
            : _ref(std::move(ref))
        {
            if (_ref->is_linked()) {
                _ref->_parent->_lru.remove(*_ref);
            }
        }
        ~entry_ptr() { *this = nullptr; }
        entry_ptr(entry_ptr&&) noexcept = default;
        entry_ptr(const entry_ptr&) noexcept = default;
        entry_ptr& operator=(std::nullptr_t) noexcept {
            if (_ref) {
                if (_ref.unique() && _ref->ready()) {
                    _ref->_parent->_lru.add(*_ref);
                }
                _ref = nullptr;
            }
            return *this;
        }
        entry_ptr& operator=(entry_ptr&& o) noexcept {
            if (this != &o) {
                *this = nullptr;
                _ref = std::move(o._ref);
            }
            return *this;
        }
        entry_ptr& operator=(const entry_ptr& o) noexcept {
            if (this != &o) {
                *this = nullptr;
                _ref = o._ref;
            }
            return *this;
        }
        explicit operator bool() const noexcept { return bool(_ref); }
        const element_type& operator*() const noexcept { return _ref->page(); }
        const element_type* operator->() const noexcept { return &_ref->page(); }
        element_type& operator*() noexcept { return _ref->page(); }
        element_type* operator->() noexcept { return &_ref->page(); }
    };

    // Creates a shared pointer to cp.
    // Invalidates cp.
    entry_ptr share(entry& cp) {
        auto wptr = cp.weak_from_this(); // may throw
        return entry_ptr(std::move(wptr));
    }

private:
    using cache_type = bplus::tree<key_type, entry, key_less_comparator, 8, bplus::key_search::linear>;
    cache_type _cache;
    logalloc::region& _region;
    logalloc::allocating_section _as;
    lru& _lru;
    partition_index_cache_stats& _stats;
public:

    // Create a cache with a given LRU attached.
    partition_index_cache(lru& lru_, logalloc::region& r, partition_index_cache_stats& stats)
            : _cache(key_less_comparator())
            , _region(r)
            , _lru(lru_)
            , _stats(stats)
    { }

    ~partition_index_cache() {
        with_allocator(_region.allocator(), [&] {
            _cache.clear_and_dispose([this] (entry* e) noexcept {
                _lru.remove(*e);
                on_evicted(*e);
            });
        });
    }

    partition_index_cache(partition_index_cache&&) = delete;
    partition_index_cache(const partition_index_cache&) = delete;

    // Returns a future which resolves with a shared pointer to index_list for given key.
    // Always returns a valid pointer if succeeds. The pointer is never invalidated externally.
    //
    // If entry is missing, the loader is invoked. If list is already loading, this invocation
    // will wait for prior loading to complete and use its result when it's done.
    //
    // The loader object does not survive deferring, so the caller must deal with its liveness.
    //
    // The returned future must be waited on before destroying this instance.
    template<typename Loader>
    future<entry_ptr> get_or_load(const key_type& key, Loader&& loader) {
        auto i = _cache.lower_bound(key);
        if (i != _cache.end() && i->_key == key) {
            entry& cp = *i;
            auto ptr = share(cp);
            if (cp.ready()) {
                ++_stats.hits;
                return make_ready_future<entry_ptr>(std::move(ptr));
            } else {
                ++_stats.blocks;
                return ptr.get_entry().promise()->get_shared_future().then([ptr] () mutable {
                    return std::move(ptr);
                });
            }
        }

        ++_stats.misses;
        ++_stats.blocks;

        entry_ptr ptr = _as(_region, [&] {
            return with_allocator(_region.allocator(), [&] {
                auto it_and_flag = _cache.emplace(key, this, key);
                entry &cp = *it_and_flag.first;
                SCYLLA_ASSERT(it_and_flag.second);
                try {
                    return share(cp);
                } catch (...) {
                    _cache.erase(key);
                    throw;
                }
            });
        });

        // No exceptions before then_wrapped() is installed so that ptr will be eventually populated.

        return futurize_invoke(loader, key).then_wrapped([this, key, ptr = std::move(ptr)] (auto&& f) mutable {
            entry& e = ptr.get_entry();
            try {
                partition_index_page&& page = f.get();
                e.promise()->set_value();
                e.set_page(std::move(page));
                _stats.used_bytes += e.size_in_allocator();
                ++_stats.populations;
                return ptr;
            } catch (...) {
                e.promise()->set_exception(std::current_exception());
                ptr = {};
                with_allocator(_region.allocator(), [&] {
                    _cache.erase(key);
                });
                throw;
            }
        });
    }

    void on_evicted(entry& p) {
        _stats.used_bytes -= p.size_in_allocator();
        ++_stats.evictions;
    }

    // Evicts all unreferenced entries.
    future<> evict_gently() {
        auto i = _cache.begin();
        std::optional<partition_index_page> partial_page;
        auto clear_on_exception = defer([&] {
            with_allocator(_region.allocator(), [&] {
                partial_page.reset();
            });
        });
        while (partial_page || i != _cache.end()) {
            if (partial_page) {
                auto preempted = with_allocator(_region.allocator(), [&] {
                    while (!partial_page->empty()) {
                        partial_page->clear_one_entry();
                        if (need_preempt()) {
                            return true;
                        }
                    }
                    partial_page.reset();
                    return false;
                });
                if (preempted) {
                    auto key = (i != _cache.end()) ? std::optional(i->key()) : std::nullopt;
                    co_await coroutine::maybe_yield();
                    i = key ? _cache.lower_bound(*key) : _cache.end();
                }
            } else {
                with_allocator(_region.allocator(), [&] {
                    if (i->is_referenced()) {
                        ++i;
                    } else {
                        _lru.remove(*i);
                        on_evicted(*i);
                        if (i->ready()) {
                            partial_page = std::move(i->page());
                        }
                        i = i.erase(key_less_comparator());
                    }
                });
            }
        }
    }
};

inline
void partition_index_cache::entry::on_evicted() noexcept {
    _parent->on_evicted(*this);
    cache_type::iterator it(this);
    it.erase(key_less_comparator());
}

}
