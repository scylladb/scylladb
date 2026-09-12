/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "readers/mutation_reader.hh"
#include "dht/i_partitioner_fwd.hh"

namespace mutation_writer {

// Given a producer that may contain data for all shards, consume it in a per-shard
// manner. This is useful, for instance, in the resharding process where a user changes
// the amount of CPU assigned to Scylla and we have to rewrite the SSTables to their new
// owners, or when tablet sstables loaded from a foreign shard need to be split by their
// current owning shard.
//
// `sharder` determines the owning shard of each partition. Its lifetime must extend
// until the returned future resolves.
future<> segregate_by_shard(mutation_reader producer, const dht::sharder& sharder, mutation_reader_consumer consumer);

} // namespace mutation_writer
