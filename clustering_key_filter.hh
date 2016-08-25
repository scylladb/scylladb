/*
 * Copyright (C) 2016 ScyllaDB
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

#include "core/shared_ptr.hh"
#include "schema.hh"
#include "query-request.hh"

namespace query {

class clustering_key_filter_ranges {
    clustering_row_ranges _storage;
    const clustering_row_ranges& _ref;
public:
    clustering_key_filter_ranges(const clustering_row_ranges& ranges) : _ref(ranges) { }
    struct reversed { };
    clustering_key_filter_ranges(reversed, const clustering_row_ranges& ranges)
        : _storage(ranges.rbegin(), ranges.rend()), _ref(_storage) { }

    clustering_key_filter_ranges(clustering_key_filter_ranges&& other) noexcept
        : _storage(std::move(other._storage))
        , _ref(&other._ref == &other._storage ? _storage : other._ref)
    { }

    clustering_key_filter_ranges& operator=(clustering_key_filter_ranges&& other) noexcept {
        if (this != &other) {
            this->~clustering_key_filter_ranges();
            new (this) clustering_key_filter_ranges(std::move(other));
        }
        return *this;
    }

    auto begin() const { return _ref.begin(); }
    auto end() const { return _ref.end(); }
    bool empty() const { return _ref.empty(); }
    size_t size() const { return _ref.size(); }
};

// A factory for clustering key filter which can be reused for multiple clustering keys.
class clustering_key_filter_factory {
public:
    virtual clustering_key_filter_ranges get_ranges(const partition_key&) = 0;

    virtual ~clustering_key_filter_factory() = default;
};

class clustering_key_filtering_context {
private:
    shared_ptr<clustering_key_filter_factory> _factory;
    clustering_key_filtering_context() {};
    clustering_key_filtering_context(shared_ptr<clustering_key_filter_factory> factory) : _factory(factory) {}
public:
    clustering_key_filter_ranges get_ranges(const partition_key& key) const;

    static const clustering_key_filtering_context create(schema_ptr, const partition_slice&);

    static clustering_key_filtering_context create_no_filtering();
};

extern const clustering_key_filtering_context no_clustering_key_filtering;

}
