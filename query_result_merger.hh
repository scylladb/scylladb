/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
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
