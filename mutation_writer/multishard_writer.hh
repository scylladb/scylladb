/*
 * Copyright (C) 2018-present ScyllaDB
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

#include "schema_fwd.hh"
#include "flat_mutation_reader.hh"
#include "dht/i_partitioner.hh"
#include "utils/phased_barrier.hh"

namespace mutation_writer {

// Helper to use multishard_writer to distribute mutation_fragments from the
// producer to the correct shard and consume with the consumer.
// It returns number of partitions consumed.
future<uint64_t> distribute_reader_and_consume_on_shards(schema_ptr s,
    flat_mutation_reader producer,
    std::function<future<> (flat_mutation_reader)> consumer,
    utils::phased_barrier::operation&& op = {});

} // namespace mutation_writer
