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

#include "shared_sstable.hh"
#include "query-request.hh" // for partition_range; FIXME: move it out of there
#include <seastar/core/shared_ptr.hh>
#include <vector>

namespace sstables {

class sstable_set_impl;
class incremental_selector_impl;

class sstable_set {
    std::unique_ptr<sstable_set_impl> _impl;
    schema_ptr _schema;
    // used to support column_family::get_sstable(), which wants to return an sstable_list
    // that has a reference somewhere
    lw_shared_ptr<sstable_list> _all;
public:
    ~sstable_set();
    sstable_set(std::unique_ptr<sstable_set_impl> impl, schema_ptr s, lw_shared_ptr<sstable_list> all);
    sstable_set(const sstable_set&);
    sstable_set(sstable_set&&) noexcept;
    sstable_set& operator=(const sstable_set&);
    sstable_set& operator=(sstable_set&&) noexcept;
    std::vector<shared_sstable> select(const dht::partition_range& range) const;
    lw_shared_ptr<sstable_list> all() const { return _all; }
    void insert(shared_sstable sst);
    void erase(shared_sstable sst);

    // Used to incrementally select sstables from sstable set using ring-position.
    // sstable set must be alive and cannot be modified while incremental
    // selector is used.
    class incremental_selector {
        std::unique_ptr<incremental_selector_impl> _impl;
        dht::ring_position_comparator _cmp;
        mutable std::optional<dht::partition_range> _current_range;
        mutable std::optional<nonwrapping_range<dht::ring_position_view>> _current_range_view;
        mutable std::vector<shared_sstable> _current_sstables;
        mutable dht::ring_position_view _current_next_position = dht::ring_position_view::min();
    public:
        ~incremental_selector();
        incremental_selector(std::unique_ptr<incremental_selector_impl> impl, const schema& s);
        incremental_selector(incremental_selector&&) noexcept;

        struct selection {
            const std::vector<shared_sstable>& sstables;
            dht::ring_position_view next_position;
        };

        // Return the sstables that intersect with `pos` and the next
        // position where the intersecting sstables change.
        // To walk through the token range incrementally call `select()`
        // with `dht::ring_position_view::min()` and then pass back the
        // returned `next_position` on each next call until
        // `next_position` becomes `dht::ring_position::max()`.
        //
        // Successive calls to `select()' have to pass weakly monotonic
        // positions (incrementability).
        //
        // NOTE: both `selection.sstables` and `selection.next_position`
        // are only guaranteed to be valid until the next call to
        // `select()`.
        selection select(const dht::ring_position_view& pos) const;
    };
    incremental_selector make_incremental_selector() const;
};

}
