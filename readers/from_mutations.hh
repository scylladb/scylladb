/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once
#include "schema_fwd.hh"
#include "dht/i_partitioner_fwd.hh"
#include <vector>
#include "mutation_fragment_fwd.hh"
#include "readers/flat_mutation_reader_fwd.hh"

class flat_mutation_reader;
class reader_permit;
class mutation;

namespace query {
    class partition_slice;
    extern const dht::partition_range full_partition_range;
}

// All mutations should have the same schema.
flat_mutation_reader make_flat_mutation_reader_from_mutations(
    schema_ptr schema,
    reader_permit permit,
    std::vector<mutation>,
    const dht::partition_range& pr = query::full_partition_range,
    streamed_mutation::forwarding fwd = streamed_mutation::forwarding::no);

// All mutations should have the same schema.
flat_mutation_reader make_flat_mutation_reader_from_mutations(schema_ptr schema, reader_permit permit, std::vector<mutation> ms, streamed_mutation::forwarding fwd);

// All mutations should have the same schema.
flat_mutation_reader
make_flat_mutation_reader_from_mutations(schema_ptr schema,
                                    reader_permit permit,
                                    std::vector<mutation> ms,
                                    const query::partition_slice& slice,
                                    streamed_mutation::forwarding fwd = streamed_mutation::forwarding::no);

// All mutations should have the same schema.
flat_mutation_reader
make_flat_mutation_reader_from_mutations(schema_ptr schema,
                                    reader_permit permit,
                                    std::vector<mutation> ms,
                                    const dht::partition_range& pr,
                                    const query::partition_slice& slice,
                                    streamed_mutation::forwarding fwd = streamed_mutation::forwarding::no);

