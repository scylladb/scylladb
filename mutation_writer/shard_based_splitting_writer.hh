/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "readers/mutation_reader.hh"
#include "dht/i_partitioner_fwd.hh"

namespace mutation_writer {

// Splits a producer's data by shard, e.g. for resharding. `sharder` decides ownership
// per-partition; it must outlive the returned future.
future<> segregate_by_shard(mutation_reader producer, const dht::sharder& sharder, mutation_reader_consumer consumer);

} // namespace mutation_writer
