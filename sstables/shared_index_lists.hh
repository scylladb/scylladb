/*
 * Copyright (C) 2017 ScyllaDB
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

#include <unordered_map>
#include <vector>
#include <seastar/core/shared_future.hh>
#include <seastar/core/future.hh>

namespace sstables {

using index_list = std::vector<index_entry>;

// Associative cache of summary index -> index_list
// Entries stay around as long as there is any live external reference (list_ptr) to them.
// Supports asynchronous insertion, ensures that only one entry will be loaded.
class shared_index_lists {
public:
    using key_type = uint64_t;
    struct stats {
        uint64_t hits = 0; // Number of times entry was found ready
        uint64_t misses = 0; // Number of times entry was not found
        uint64_t blocks = 0; // Number of times entry was not ready (>= misses)
    };
private:
    class entry : public enable_lw_shared_from_this<entry> {
    public:
        key_type key;
        index_list list;
        shared_promise<> loaded;
        shared_index_lists& parent;

        entry(shared_index_lists& parent, key_type key)
            : key(key), parent(parent)
        { }
        ~entry() {
            parent._lists.erase(key);
        }
        bool operator==(const entry& e) const { return key == e.key; }
        bool operator!=(const entry& e) const { return key != e.key; }
    };
    std::unordered_map<key_type, entry*> _lists;
    static thread_local stats _shard_stats;
public:
    // Pointer to index_list
    class list_ptr {
        lw_shared_ptr<entry> _e;
    public:
        using element_type = index_list;
        list_ptr() = default;
        explicit list_ptr(lw_shared_ptr<entry> e) : _e(std::move(e)) {}
        explicit operator bool() const { return static_cast<bool>(_e); }
        index_list& operator*() { return _e->list; }
        const index_list& operator*() const { return _e->list; }
        index_list* operator->() { return &_e->list; }
        const index_list* operator->() const { return &_e->list; }

        index_list release() {
            auto res = _e.owned() ? index_list(std::move(_e->list)) : index_list(_e->list);
            _e = {};
            return std::move(res);
        }
    };

    shared_index_lists() = default;
    shared_index_lists(shared_index_lists&&) = delete;
    shared_index_lists(const shared_index_lists&) = delete;

    // Returns a future which resolves with a shared pointer to index_list for given key.
    // Always returns a valid pointer if succeeds. The pointer is never invalidated externally.
    //
    // If entry is missing, the loader is invoked. If list is already loading, this invocation
    // will wait for prior loading to complete and use its result when it's done.
    //
    // The loader object does not survive deferring, so the caller must deal with its liveness.
    template<typename Loader>
    future<list_ptr> get_or_load(key_type key, Loader&& loader) {
        auto i = _lists.find(key);
        lw_shared_ptr<entry> e;
      auto f = [&] {
        if (i != _lists.end()) {
            e = i->second->shared_from_this();
            return e->loaded.get_shared_future();
        } else {
            ++_shard_stats.misses;
            e = make_lw_shared<entry>(*this, key);
            auto f = e->loaded.get_shared_future();
            auto res = _lists.emplace(key, e.get());
            assert(res.second);
            futurize_apply(loader, key).then_wrapped([e](future<index_list>&& f) mutable {
                if (f.failed()) {
                    e->loaded.set_exception(f.get_exception());
                } else {
                    e->list = f.get0();
                    e->loaded.set_value();
                }
            });
            return f;
        }
      }();
        if (!f.available()) {
            ++_shard_stats.blocks;
            return f.then([e]() mutable {
                return list_ptr(std::move(e));
            });
        } else if (f.failed()) {
            return make_exception_future<list_ptr>(std::move(f).get_exception());
        } else {
            ++_shard_stats.hits;
            return make_ready_future<list_ptr>(list_ptr(std::move(e)));
        }
    }

    static const stats& shard_stats() { return _shard_stats; }
};

}
