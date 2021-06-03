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

#include "i_partitioner.hh"
#include "range.hh"

#include <vector>

namespace dht {


// Utilities for sharding ring partition_range:s

// A ring_position range's data is divided into sub-ranges, where each sub-range's data
// is owned by a single shard. Note that multiple non-overlapping sub-ranges may map to a
// single shard, and some shards may not receive any sub-range.
//
// This module provides utilities for determining the sub-ranges to shard mapping. The utilities
// generate optimal mappings: each range that you get is the largest possible, so you
// get the minimum number of ranges possible. You can get many ranges, so operate on them
// one (or a few) at a time, rather than accumulating them.

// A mapping between a partition_range and a shard. All positions within `ring_range` are
// owned by `shard`.
//
// The classes that return ring_position_range_and_shard make `ring_range` as large as
// possible (maximizing the number of tokens), so the total number of such ranges is minimized.
// Successive ranges therefore always have a different `shard` than the previous return.
// (classes that return ring_position_range_and_shard_and_element can have the same `shard`
// in successive returns, if `element` is different).
struct ring_position_range_and_shard {
    dht::partition_range ring_range;
    unsigned shard;
};

// Incrementally divides a `partition_range` into sub-ranges wholly owned by a single shard.
class ring_position_range_sharder {
    const sharder& _sharder;
    dht::partition_range _range;
    bool _done = false;
public:
    // Initializes the ring_position_range_sharder with a given range to subdivide.
    ring_position_range_sharder(const sharder& sharder, nonwrapping_range<ring_position> rrp)
            : _sharder(sharder), _range(std::move(rrp)) {}
    // Fetches the next range-shard mapping. When the input range is exhausted, std::nullopt is
    // returned. The returned ranges are contiguous and non-overlapping, and together span the
    // entire input range.
    std::optional<ring_position_range_and_shard> next(const schema& s);
};

// A mapping between a partition_range and a shard (like ring_position_range_and_shard) extended
// by having a reference to input range index. See ring_position_range_vector_sharder for use.
//
// The classes that return ring_position_range_and_shard_and_element make `ring_range` as large as
// possible (maximizing the number of tokens), so the total number of such ranges is minimized.
// Successive ranges therefore always have a different `shard` than the previous return.
// (classes that return ring_position_range_and_shard_and_element can have the same `shard`
// in successive returns, if `element` is different).
struct ring_position_range_and_shard_and_element : ring_position_range_and_shard {
    ring_position_range_and_shard_and_element(ring_position_range_and_shard&& rpras, unsigned element)
            : ring_position_range_and_shard(std::move(rpras)), element(element) {
    }
    unsigned element;
};

// Incrementally divides several non-overlapping `partition_range`:s into sub-ranges wholly owned by
// a single shard.
//
// Similar to ring_position_range_sharder, but instead of stopping when the input range is exhauseted,
// moves on to the next input range (input ranges are supplied in a vector).
//
// This has two use cases:

// 1. vnodes. A vnode cannot be described by a single range, since
//    one vnode wraps around from the largest token back to the smallest token. Hence it must be
//    described as a vector of two ranges, (largest_token, +inf) and (-inf, smallest_token].
// 2. sstable shard mappings. An sstable has metadata describing which ranges it owns, and this is
//    used to see what shards these ranges map to (and therefore to see if the sstable is shared or
//    not, and which shards share it).

class ring_position_range_vector_sharder {
    using vec_type = dht::partition_range_vector;
    vec_type _ranges;
    const sharder& _sharder;
    vec_type::iterator _current_range;
    std::optional<ring_position_range_sharder> _current_sharder;
private:
    void next_range() {
        if (_current_range != _ranges.end()) {
            _current_sharder.emplace(_sharder, std::move(*_current_range++));
        }
    }
public:
    // Initializes the `ring_position_range_vector_sharder` with the ranges to be processesd.
    // Input ranges should be non-overlapping (although nothing bad will happen if they do
    // overlap).
    ring_position_range_vector_sharder(const sharder& sharder, dht::partition_range_vector ranges);
    // Fetches the next range-shard mapping. When the input range is exhausted, std::nullopt is
    // returned. Within an input range, results are contiguous and non-overlapping (but since input
    // ranges usually are discontiguous, overall the results are not contiguous). Together, the results
    // span the input ranges.
    //
    // The result is augmented with an `element` field which indicates the index from the input vector
    // that the result belongs to.
    //
    // Results are returned sorted by index within the vector first, then within each vector item
    std::optional<ring_position_range_and_shard_and_element> next(const schema& s);
};

// Incrementally divides a `partition_range` into sub-ranges wholly owned by a single shard.
// Unlike ring_position_range_sharder, it only returns result for a shard number provided by the caller.
class selective_token_range_sharder {
    const sharder& _sharder;
    dht::token_range _range;
    shard_id _shard;
    bool _done = false;
    shard_id _next_shard;
    dht::token _start_token;
    std::optional<range_bound<dht::token>> _start_boundary;
public:
    // Initializes the selective_token_range_sharder with a token range and shard_id of interest.
    selective_token_range_sharder(const sharder& sharder, dht::token_range range, shard_id shard)
            : _sharder(sharder)
            , _range(std::move(range))
            , _shard(shard)
            , _next_shard(_shard + 1 == _sharder.shard_count() ? 0 : _shard + 1)
            , _start_token(_range.start() ? _range.start()->value() : minimum_token())
            , _start_boundary(_sharder.shard_of(_start_token) == shard ?
                _range.start() : range_bound<dht::token>(_sharder.token_for_next_shard(_start_token, shard))) {
    }
    // Returns the next token_range that is both wholly contained within the input range and also
    // wholly owned by the input shard_id. When the input range is exhausted, std::nullopt is returned.
    // Note if the range does not intersect the shard at all, std::nullopt will be returned immediately.
    std::optional<dht::token_range> next();
};

} // dht
