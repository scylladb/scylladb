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
#include "range.hh"

namespace query {

class partition_slice;

// A factory for clustering key filter which can be reused for multiple clustering keys.
class clustering_key_filter_factory {
public:
    virtual const std::vector<nonwrapping_range<clustering_key_prefix>>& get_ranges(const partition_key&) = 0;

    virtual ~clustering_key_filter_factory() = default;
};

class clustering_key_filtering_context {
private:
    shared_ptr<clustering_key_filter_factory> _factory;
    clustering_key_filtering_context() {};
    clustering_key_filtering_context(shared_ptr<clustering_key_filter_factory> factory) : _factory(factory) {}
public:
    const std::vector<nonwrapping_range<clustering_key_prefix>>& get_ranges(const partition_key& key) const;

    static const clustering_key_filtering_context create(schema_ptr, const partition_slice&);

    static clustering_key_filtering_context create_no_filtering();
};

extern const clustering_key_filtering_context no_clustering_key_filtering;

}
