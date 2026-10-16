/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#include "replica/logstor/single_row_partition.hh"
#include <seastar/core/format.hh>
#include <seastar/core/on_internal_error.hh>
#include <seastar/util/log.hh>
#include "keys/keys.hh"
#include "mutation/mutation.hh"
#include "mutation/mutation_partition.hh"
#include "schema/schema.hh"

namespace replica::logstor {

extern seastar::logger logstor_logger;

void check_partition_shape(const mutation_partition& p, const schema& s, const partition_key& key) {
    if (!p.row_tombstones().empty() || !p.static_row().empty()) [[unlikely]] {
        on_internal_error(logstor_logger, format("logstor: partition of {} has range tombstones or a static row", key.with_schema(s)));
    }
    const auto& rows = p.clustered_rows();
    if (!rows.empty()) {
        auto it = rows.begin();
        if (it->dummy()) [[unlikely]] {
            on_internal_error(logstor_logger, format("logstor: partition of {} has a dummy row", key.with_schema(s)));
        }
        if (std::next(it) != rows.end()) [[unlikely]] {
            on_internal_error(logstor_logger, format("logstor: partition of {} has more than one row", key.with_schema(s)));
        }
    }
}

single_row_partition to_single_row_partition(mutation_partition&& p, const schema& s, const partition_key& key) {
    check_partition_shape(p, s, key);
    single_row_partition partition{.partition_tombstone = p.partition_tombstone()};
    auto& rows = p.mutable_clustered_rows();
    if (!rows.empty()) {
        // Moved out of its entry, which stays in the tree with the key and goes with the partition:
        // what mutation::consume() does with a row.
        partition.row.emplace(std::move(rows.begin()->row()));
    }
    return partition;
}

mutation to_mutation(schema_ptr s, dht::decorated_key dk, single_row_partition&& partition) {
    mutation m(std::move(s), std::move(dk));
    m.partition().apply(partition.partition_tombstone);
    if (partition.row) {
        auto& dr = m.partition().clustered_row(*m.schema(), clustering_key::make_empty());
        dr.apply(*m.schema(), std::move(*partition.row));
    }
    return m;
}

} // namespace replica::logstor
