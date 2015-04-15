/*
 * Copyright 2015 Cloudius Systems
 */

#pragma once

#include "core/distributed.hh"
#include "query-result.hh"

namespace query {

// Merges non-overlapping results into one
// Implements @Reducer concept from distributed.hh
class result_merger {
    std::vector<foreign_ptr<lw_shared_ptr<query::result>>> _partial;
public:
    void reserve(size_t size) {
        _partial.reserve(size);
    }

    void operator()(foreign_ptr<lw_shared_ptr<query::result>> r) {
        _partial.emplace_back(std::move(r));
    }

    // FIXME: Eventually we should return a composite_query_result here
    // which holds the vector of query results and which can be quickly turned
    // into packet fragments by the transport layer without copying the data.
    foreign_ptr<lw_shared_ptr<query::result>> get() && {
        auto merged = make_lw_shared<query::result>();

        size_t total_size = 0;
        for (auto&& r : _partial) {
            total_size += r->_w.size();
        }

        bytes_ostream w;
        w.reserve(total_size);

        for (auto&& r : _partial) {
            w.append(r->_w);
        }

        return make_foreign(make_lw_shared<query::result>(std::move(w)));
    }
};

}
