/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "bytes_fwd.hh"
#include "dht/i_partitioner_fwd.hh"
#include "query/query-request.hh"

#include <seastar/core/sstring.hh>

#include <optional>
#include <unordered_map>
#include <vector>

namespace cql3 {
class query_options;
}

namespace cql3::statements {

class modification_statement;

/*
 * What a modification - an INSERT, an UPDATE or a DELETE - addresses, once the
 * values bound to it are known: the rows it writes, and the JSON document it
 * writes them from.
 *
 * None of this can be known at prepare time. Every field is evaluated from
 * query_options, so the spec is built per execution.
 */
struct modification_spec {
    using json_cache_opt = std::optional<std::unordered_map<seastar::sstring, bytes_opt>>;

    // The parsed document of an INSERT JSON; empty for every other modification.
    json_cache_opt json_cache;
    // The partitions the modification writes.
    std::vector<dht::partition_range> keys;
    // The rows it writes within them.
    std::vector<query::clustering_range> ranges;

    modification_spec(const modification_statement& stmt, const query_options& options);
};

}
