/*
 * Copyright (C) 2016-present ScyllaDB
 *
 * Modified by ScyllaDB
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

#include "schema_fwd.hh"
#include "query-request.hh"

namespace query {

class clustering_key_filter_ranges {
    clustering_row_ranges _storage;
    std::reference_wrapper<const clustering_row_ranges> _ref;
public:
    clustering_key_filter_ranges(const clustering_row_ranges& ranges) : _ref(ranges) { }
    struct reversed { };
    clustering_key_filter_ranges(reversed, const clustering_row_ranges& ranges)
        : _storage(ranges.rbegin(), ranges.rend()), _ref(_storage) { }

    clustering_key_filter_ranges(clustering_key_filter_ranges&& other) noexcept
        : _storage(std::move(other._storage))
        , _ref(&other._ref.get() == &other._storage ? _storage : other._ref.get())
    { }

    clustering_key_filter_ranges& operator=(clustering_key_filter_ranges&& other) noexcept {
        if (this != &other) {
            _storage = std::move(other._storage);
            _ref = (&other._ref.get() == &other._storage) ? _storage : other._ref.get();
        }
        return *this;
    }

    auto begin() const { return _ref.get().begin(); }
    auto end() const { return _ref.get().end(); }
    bool empty() const { return _ref.get().empty(); }
    size_t size() const { return _ref.get().size(); }
    const clustering_row_ranges& ranges() const { return _ref; }

    static clustering_key_filter_ranges get_ranges(const schema& schema, const query::partition_slice& slice, const partition_key& key) {
        const query::clustering_row_ranges& ranges = slice.row_ranges(schema, key);
        if (slice.options.contains(query::partition_slice::option::reversed)) {
            return clustering_key_filter_ranges(clustering_key_filter_ranges::reversed{}, ranges);
        }
        return clustering_key_filter_ranges(ranges);
    }
};

}
