/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "schema/schema_fwd.hh"
#include "readers/mutation_reader.hh"
#include "dht/i_partitioner_fwd.hh"
#include "utils/phased_barrier.hh"

namespace mutation_writer {

/// Distribute a mutation stream across shards according to the provided sharder.
/// The function returns a future that is ready when all shards are done consuming.
///
/// The provided "consumer" function is called on the destination shard to create a consumer
/// on that shard. It takes a mutation_reader that will contain only the mutations that
/// should be applied on that shard.
/// The consumer function should return a future that is ready when the shard is done consuming.
///
/// Mutations are distributed to shards according to the sharder::shard_for_writes().
/// During intra-node migration of a tablet shard_for_writes() may return two shards for tokens of that tablet,
/// in which case that part of the stream will be duplicated to two shards.
///
/// The caller is responsible for ensuring that effective_replication_map_ptr used to obtain the sharder
/// is alive until the returned future is ready. Alternatively, if auto_refreshing_sharder is used,
/// the topology_guard associated with operation must be used and checked by the consumer.
future<uint64_t> distribute_reader_and_consume_on_shards(schema_ptr s,
    const dht::sharder& sharder,
    mutation_reader producer,
    std::function<future<> (mutation_reader)> consumer,
    utils::phased_barrier::operation&& op = {});

} // namespace mutation_writer
