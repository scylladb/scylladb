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


/* This module contains classes and functions used to manage CDC generations:
 * sets of CDC stream identifiers used by the cluster to choose partition keys for CDC log writes.
 * Each CDC generation begins operating at a specific time point, called the generation's timestamp
 * (`cdc_streams_timpestamp` or `streams_timestamp` in the code).
 * The generation is used by all nodes in the cluster to pick CDC streams until superseded by a new generation.
 *
 * Functions from this module are used by the node joining procedure to introduce new CDC generations to the cluster
 * (which is necessary due to new tokens being inserted into the token ring), or during rolling upgrade
 * if CDC is enabled for the first time.
 */

#pragma once

#include <vector>

#include "database_fwd.hh"
#include "dht/token.hh"

namespace cdc {

class stream_id final {
    bytes _value;
public:
    stream_id() = default;
    stream_id(int64_t, int64_t);
    stream_id(bytes);
    bool is_set() const;
    bool operator==(const stream_id&) const;
    bool operator<(const stream_id&) const;

    int64_t first() const;
    int64_t second() const;

    const bytes& to_bytes() const;

    partition_key to_partition_key(const schema& log_schema) const;
    static int64_t token_from_bytes(bytes_view);
};

/* Describes a mapping of tokens to CDC streams in a token range.
 *
 * The range ends with `token_range_end`. A vector of `token_range_description`s defines the ranges entirely
 * (the end of the `i`th range is the beginning of the `i+1 % size()`th range). Ranges are left-opened, right-closed.
 *
 * Tokens in the range ending with `token_range_end` are mapped to streams in the `streams` vector as follows:
 * token `T` is mapped to `streams[j]` if and only if the used partitioner maps `T` to the `j`th shard,
 * assuming that the partitioner is configured for `streams.size()` shards and (partitioner's) `sharding_ignore_msb`
 * equals to the given `sharding_ignore_msb`.
*/
struct token_range_description {
    dht::token token_range_end;
    std::vector<stream_id> streams;
    uint8_t sharding_ignore_msb;

    bool operator==(const token_range_description&) const;
};


/* Describes a mapping of tokens to CDC streams in a whole token ring.
 *
 * Division of the ring to token ranges is defined in terms of `token_range_end`s
 * in the `_entries` vector. See the comment above `token_range_description` for explanation.
 */
class topology_description {
    std::vector<token_range_description> _entries;
public:
    topology_description(std::vector<token_range_description> entries);
    bool operator==(const topology_description&) const;

    const std::vector<token_range_description>& entries() const;
};

} // namespace cdc
