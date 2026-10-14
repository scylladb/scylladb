/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#pragma once

#include <optional>
#include "dht/decorated_key.hh"
#include "mutation/mutation_partition.hh"
#include "mutation/tombstone.hh"
#include "schema/schema_fwd.hh"

class mutation;

namespace replica::logstor {

// A logstor partition once it leaves the cache or the record decoder, in the form a reader pushes
// into its buffer as fragments. A logstor table has no clustering columns, so its partition is at
// most one row, with no range tombstones and no static row: the row is absent when the partition is
// only a tombstone, and there is never more than one. The row is held without its clustering key,
// which is always the empty one; a reader adds it when it pushes the row. The partition key is not
// in it either; whoever holds a single_row_partition holds the key beside it.
struct single_row_partition {
    tombstone partition_tombstone;
    std::optional<deletable_row> row;
};

// What a logstor schema guarantees of a partition, checked rather than fallen back from, since a
// reader built on single_row_partition has nothing to fall back to: no range tombstones, no static
// row, and at most one row, which the record decoder never makes a dummy of. An internal error
// otherwise, naming the partition by `key`.
void check_partition_shape(const mutation_partition&, const schema&, const partition_key& key);

// The partition of a consumed mutation: the row is moved out of it, no cell is copied.
single_row_partition to_single_row_partition(mutation&&);

// The mutation that a single_row_partition describes.
mutation to_mutation(schema_ptr, dht::decorated_key, single_row_partition&&);

} // namespace replica::logstor
