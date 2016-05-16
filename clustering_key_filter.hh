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
#include <functional>
#include <vector>

#include "core/shared_ptr.hh"
#include "database_fwd.hh"
#include "schema.hh"

template<typename T> class range;

namespace query {

class partition_slice;

// A predicate that tells if a clustering key should be accepted.
using clustering_key_filter = std::function<bool(const clustering_key&)>;

// A factory for clustering key filter which can be reused for multiple clustering keys.
class clustering_key_filter_factory {
public:
    // Create a clustering key filter that can be used for multiple clustering keys with no restrictions.
    virtual clustering_key_filter get_filter(const partition_key&) = 0;
    // Create a clustering key filter that can be used for multiple clustering keys but they have to be sorted.
    virtual clustering_key_filter get_filter_for_sorted(const partition_key&) = 0;
    virtual const std::vector<range<clustering_key_prefix>>& get_ranges(const partition_key&) = 0;
    virtual ~clustering_key_filter_factory() = default;
};

class clustering_key_filtering_context {
private:
    shared_ptr<clustering_key_filter_factory> _factory;
    clustering_key_filtering_context() {};
    clustering_key_filtering_context(shared_ptr<clustering_key_filter_factory> factory) : _factory(factory) {}
public:
    // Create a clustering key filter that can be used for multiple clustering keys with no restrictions.
    clustering_key_filter get_filter(const partition_key& key) const {
        return _factory ? _factory->get_filter(key) : [] (const clustering_key&) { return true; };
    }
    // Create a clustering key filter that can be used for multiple clustering keys but they have to be sorted.
    clustering_key_filter get_filter_for_sorted(const partition_key& key) const {
        return _factory ? _factory->get_filter_for_sorted(key) : [] (const clustering_key&) { return true; };
    }
    const std::vector<range<clustering_key_prefix>>& get_ranges(const partition_key& key) const;

    static const clustering_key_filtering_context create(schema_ptr, const partition_slice&);

    static clustering_key_filtering_context create_no_filtering();
};

extern const clustering_key_filtering_context no_clustering_key_filtering;

}
