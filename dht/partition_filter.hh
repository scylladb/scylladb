/*
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include "dht/i_partitioner_fwd.hh"
#include "dht/token.hh"

namespace dht {

class incremental_owned_ranges_checker {
    const dht::token_range_vector& _sorted_owned_ranges;
    mutable dht::token_range_vector::const_iterator _it;
public:
    incremental_owned_ranges_checker(const dht::token_range_vector& sorted_owned_ranges)
            : _sorted_owned_ranges(sorted_owned_ranges)
            , _it(_sorted_owned_ranges.begin()) {
    }

    // Must be called with increasing token values.
    bool belongs_to_current_node(const dht::token& t) const noexcept {
        // While token T is after a range Rn, advance the iterator.
        // iterator will be stopped at a range which either overlaps with T (if T belongs to node),
        // or at a range which is after T (if T doesn't belong to this node).
        //
        // As the name "incremental" suggests, the search is expected to
        // terminate quickly (usually after 0 or 1 iterations) and so linear
        // search is best.
        while (_it != _sorted_owned_ranges.end() && _it->after(t, dht::token_comparator())) {
            _it++;
        }

        return _it != _sorted_owned_ranges.end() && _it->contains(t, dht::token_comparator());
    }

    const dht::token_range* next_owned_range() const noexcept {
        if (_it == _sorted_owned_ranges.end()) {
            return nullptr;
        }
        return &*_it++;
    }

};

} // dht
