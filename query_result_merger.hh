/*
 * Copyright (C) 2015-present ScyllaDB
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

#include <seastar/core/distributed.hh>
#include "query-result.hh"

namespace query {

// Merges non-overlapping results into one
// Implements @Reducer concept from distributed.hh
class result_merger {
    std::vector<foreign_ptr<lw_shared_ptr<query::result>>> _partial;
    const uint64_t _max_rows;
    const uint32_t _max_partitions;
public:
    explicit result_merger(uint64_t max_rows, uint32_t max_partitions)
            : _max_rows(max_rows)
            , _max_partitions(max_partitions)
    { }

    void reserve(size_t size) {
        _partial.reserve(size);
    }

    void operator()(foreign_ptr<lw_shared_ptr<query::result>> r) {
        if (!_partial.empty() && _partial.back()->is_short_read()) {
            return;
        }
        _partial.emplace_back(std::move(r));
    }

    // FIXME: Eventually we should return a composite_query_result here
    // which holds the vector of query results and which can be quickly turned
    // into packet fragments by the transport layer without copying the data.
    foreign_ptr<lw_shared_ptr<query::result>> get();
};

}
