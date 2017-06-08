/*
 * Copyright (C) 2017 ScyllaDB
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

#include <stack>

#include "dht/i_partitioner.hh"

// range_splitter(r, N, K) is a helper for splitting a given token_range r of
// estimated size N into many small ranges of size K, and later iterating
// over those small ranges once with the has_next() and next() methods.
// This implementation assumes only the availability of a range::midpoint()
// operation, and as result creates ranges with size between K/2 and K.
// Moreover, it has memory requirement log(N). With more general arithmetic
// support over tokens, we could get exactly K and O(1) memory.
class range_splitter {
    std::stack<std::pair<::dht::token_range, float>> _stack;
    uint64_t _desired;
public:
    range_splitter(::dht::token_range r, uint64_t N, uint64_t K) {
        _stack.push({r, N});
        _desired = K;
    }
    bool has_next() const {
        return !_stack.empty();
    }
    ::dht::token_range next() {
        // If the head range's estimated size is small enough, return it.
        // Otherwise split it to two halves, push the second half on the
        // stack, and repeat with the first half. May need to do this more
        // than once (up to log(N/K) times) until we have one range small
        // enough to return.
        assert(!_stack.empty());
        auto range = _stack.top().first;
        auto size = _stack.top().second;
        _stack.pop();
        while (size > _desired) {
            // The use of minimum_token() here twice is not a typo - because wrap-
            // around token ranges are supported by midpoint(), the beyond-maximum
            // token can also be represented by minimum_token().
            auto midpoint = dht::global_partitioner().midpoint(
                    range.start() ? range.start()->value() : dht::minimum_token(),
                    range.end() ? range.end()->value() : dht::minimum_token());
            // This shouldn't happen, but if the range included just one token, we
            // can't split further (split() may actually fail with assertion failure)
            if ((range.start() && midpoint == range.start()->value()) ||
                (range.end() && midpoint == range.end()->value())) {
                return range;
            }
            auto halves = range.split(midpoint, dht::token_comparator());
            _stack.push({halves.second, size / 2.0});
            range = halves.first;
            size /= 2.0;
        }
        return range;
    }
};
