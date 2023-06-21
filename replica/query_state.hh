/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "query-request.hh"
#include "query-result.hh"
#include "query-result-writer.hh"

namespace replica {

struct query_state {
    explicit query_state(schema_ptr s,
                         const query::read_command& cmd,
                         query::result_options opts,
                         const dht::partition_range_vector& ranges,
                         query::result_memory_accounter memory_accounter)
            : schema(std::move(s))
            , cmd(cmd)
            , builder(cmd.slice, opts, std::move(memory_accounter), cmd.tombstone_limit)
            , limit(cmd.get_row_limit())
            , partition_limit(cmd.partition_limit)
            , current_partition_range(ranges.begin())
            , range_end(ranges.end()){
    }
    schema_ptr schema;
    const query::read_command& cmd;
    query::result::builder builder;
    uint64_t limit;
    uint32_t partition_limit;
    bool range_empty = false;   // Avoid ubsan false-positive when moving after construction
    dht::partition_range_vector::const_iterator current_partition_range;
    dht::partition_range_vector::const_iterator range_end;
    uint64_t remaining_rows() const {
        return limit - builder.row_count();
    }
    uint32_t remaining_partitions() const {
        return partition_limit - builder.partition_count();
    }
    bool done() const {
        return !remaining_rows() || !remaining_partitions() || current_partition_range == range_end || builder.is_short_read();
    }
};

} // namespace replica
