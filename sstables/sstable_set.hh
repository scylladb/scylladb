/*
 * Copyright (C) 2016 ScyllaDB
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

#include "sstables.hh"
#include "query-request.hh" // for partition_range; FIXME: move it out of there
#include <seastar/core/shared_ptr.hh>
#include <vector>

namespace sstables {

class sstable_set_impl;
class incremental_selector_impl;

class sstable_set {
    std::unique_ptr<sstable_set_impl> _impl;
    // used to support column_family::get_sstable(), which wants to return an sstable_list
    // that has a reference somewhere
    lw_shared_ptr<sstable_list> _all;
public:
    ~sstable_set();
    sstable_set(std::unique_ptr<sstable_set_impl> impl, lw_shared_ptr<sstable_list> all);
    sstable_set(const sstable_set&);
    sstable_set(sstable_set&&) noexcept;
    sstable_set& operator=(const sstable_set&);
    sstable_set& operator=(sstable_set&&) noexcept;
    std::vector<shared_sstable> select(const query::partition_range& range) const;
    lw_shared_ptr<sstable_list> all() const { return _all; }
    void insert(shared_sstable sst);
    void erase(shared_sstable sst);

    // Used to incrementally select sstables from sstable set using tokens.
    // sstable set must be alive and cannot be modified while incremental
    // selector is used.
    class incremental_selector {
        std::unique_ptr<incremental_selector_impl> _impl;
        mutable stdx::optional<dht::token_range> _current_token_range;
        mutable std::vector<shared_sstable> _current_sstables;
    public:
        ~incremental_selector();
        incremental_selector(std::unique_ptr<incremental_selector_impl> impl);
        incremental_selector(incremental_selector&&) noexcept;
        const std::vector<shared_sstable>& select(const dht::token& t) const;
    };
    incremental_selector make_incremental_selector() const;
};

}
