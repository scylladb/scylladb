/*
 * Copyright (C) 2015 ScyllaDB
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

struct ring_position_range_and_shard {
    dht::partition_range ring_range;
    unsigned shard;
};

class ring_position_range_sharder {
    const sharding_info& _sharding_info;
    dht::partition_range _range;
    bool _done = false;
public:
    ring_position_range_sharder(const sharding_info& sharding_info, nonwrapping_range<ring_position> rrp)
            : _sharding_info(sharding_info), _range(std::move(rrp)) {}
    std::optional<ring_position_range_and_shard> next(const schema& s);
};

struct ring_position_range_and_shard_and_element : ring_position_range_and_shard {
    ring_position_range_and_shard_and_element(ring_position_range_and_shard&& rpras, unsigned element)
            : ring_position_range_and_shard(std::move(rpras)), element(element) {
    }
    unsigned element;
};

class ring_position_range_vector_sharder {
    using vec_type = dht::partition_range_vector;
    vec_type _ranges;
    const sharding_info& _sharding_info;
    vec_type::iterator _current_range;
    std::optional<ring_position_range_sharder> _current_sharder;
private:
    void next_range() {
        if (_current_range != _ranges.end()) {
            _current_sharder.emplace(_sharding_info, std::move(*_current_range++));
        }
    }
public:
    ring_position_range_vector_sharder(const sharding_info& si, dht::partition_range_vector ranges);
    // results are returned sorted by index within the vector first, then within each vector item
    std::optional<ring_position_range_and_shard_and_element> next(const schema& s);
};

class selective_token_range_sharder {
    const i_partitioner& _partitioner;
    dht::token_range _range;
    shard_id _shard;
    bool _done = false;
    shard_id _next_shard;
    dht::token _start_token;
    std::optional<range_bound<dht::token>> _start_boundary;
public:
    selective_token_range_sharder(const i_partitioner& partitioner, dht::token_range range, shard_id shard)
            : _partitioner(partitioner)
            , _range(std::move(range))
            , _shard(shard)
            , _next_shard(_shard + 1 == _partitioner.shard_count() ? 0 : _shard + 1)
            , _start_token(_range.start() ? _range.start()->value() : minimum_token())
            , _start_boundary(_partitioner.shard_of(_start_token) == shard ?
                _range.start() : range_bound<dht::token>(_partitioner.token_for_next_shard(_start_token, shard))) {
    }
    std::optional<dht::token_range> next();
};

} // dht
