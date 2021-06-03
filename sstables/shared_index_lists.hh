/*
 * Copyright (C) 2017-present ScyllaDB
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

#include "index_entry.hh"
#include <vector>
#include <seastar/core/future.hh>
#include "utils/loading_shared_values.hh"
#include "utils/chunked_vector.hh"

namespace sstables {

using index_list = utils::chunked_vector<index_entry>;

// Associative cache of summary index -> index_list
// Entries stay around as long as there is any live external reference (list_ptr) to them.
// Supports asynchronous insertion, ensures that only one entry will be loaded.
class shared_index_lists {
public:
    using key_type = uint64_t;
    static thread_local struct stats {
        uint64_t hits = 0; // Number of times entry was found ready
        uint64_t misses = 0; // Number of times entry was not found
        uint64_t blocks = 0; // Number of times entry was not ready (>= misses)
    } _shard_stats;

    struct stats_updater {
        static void inc_hits() noexcept { ++_shard_stats.hits; }
        static void inc_misses() noexcept { ++_shard_stats.misses; }
        static void inc_blocks() noexcept { ++_shard_stats.blocks; }
        static void inc_evictions() noexcept {}
    };

    using loading_shared_lists_type = utils::loading_shared_values<key_type, index_list, std::hash<key_type>, std::equal_to<key_type>, stats_updater>;
    // Pointer to index_list
    using list_ptr = loading_shared_lists_type::entry_ptr;
private:

    loading_shared_lists_type _lists;
public:

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
    future<list_ptr> get_or_load(const key_type& key, Loader&& loader) {
        return _lists.get_or_load(key, std::forward<Loader>(loader));
    }

    static const stats& shard_stats() { return _shard_stats; }
};

}
