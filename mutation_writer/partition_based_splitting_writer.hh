/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <seastar/util/noncopyable_function.hh>
#include "readers/mutation_reader.hh"

namespace mutation_writer {

struct segregate_config {
    // Maximum amount of memory to be used by the in-memory segregation
    // (sorting) structures. Partitions can be split across partitions
    size_t max_memory;
};

// Given a producer that may contain partitions in the wrong order, or even
// contain partitions multiple times, separate them such that each output
// stream keeps the partition ordering guarantee. In other words, repair
// a stream that violates the ordering requirements by splitting it into output
// streams that honor it.
// This is useful for scrub compaction to split sstables containing out-of-order
// and/or duplicate partitions into sstables that honor the partition ordering.
future<> segregate_by_partition(mutation_reader producer, segregate_config cfg, reader_consumer_v2 consumer);

} // namespace mutation_writer
