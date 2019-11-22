/*
 * Copyright (C) 2019 ScyllaDB
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

#include <boost/type.hpp>

#include "keys.hh"
#include "dht/i_partitioner.hh"

#include "cdc/generation.hh"

namespace cdc {

stream_id::stream_id()
    : _first(-1), _second(-1) {}

stream_id::stream_id(int64_t first, int64_t second)
    : _first(first)
    , _second(second) {}

bool stream_id::is_set() const {
    return _first != -1 || _second != -1;
}

bool stream_id::operator==(const stream_id& o) const {
    return _first == o._first && _second == o._second;
}

int64_t stream_id::first() const {
    return _first;
}

int64_t stream_id::second() const {
    return _second;
}

partition_key stream_id::to_partition_key(const schema& log_schema) const {
    return partition_key::from_exploded(log_schema,
            { long_type->decompose(_first), long_type->decompose(_second) });
}


bool token_range_description::operator==(const token_range_description& o) const {
    return token_range_end == o.token_range_end && streams == o.streams
        && sharding_ignore_msb == o.sharding_ignore_msb;
}

topology_description::topology_description(std::vector<token_range_description> entries)
    : _entries(std::move(entries)) {}

bool topology_description::operator==(const topology_description& o) const {
    return _entries == o._entries;
}

const std::vector<token_range_description>& topology_description::entries() const {
    return _entries;
}

} // namespace cdc
