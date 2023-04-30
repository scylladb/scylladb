/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once
#include "schema/schema_fwd.hh"
#include <vector>
#include "dht/i_partitioner_fwd.hh"
#include "readers/flat_mutation_reader_fwd.hh"
#include "readers/flat_mutation_reader_v2.hh"

class reader_permit;
class mutation;

namespace query {
    class partition_slice;
    extern const dht::partition_range full_partition_range;
}

// Reader optimized for a single mutation.
flat_mutation_reader_v2
make_flat_mutation_reader_from_mutations_v2(
    schema_ptr schema,
    reader_permit permit,
    mutation m,
    streamed_mutation::forwarding fwd = streamed_mutation::forwarding::no,
    bool reversed = false,
    mutation_fragment_stream_validation_level validation_level = mutation_fragment_stream_validation_level::none);

// Reader optimized for a single mutation.
flat_mutation_reader_v2
make_flat_mutation_reader_from_mutations_v2(
    schema_ptr schema,
    reader_permit permit,
    mutation m,
    const query::partition_slice& slice,
    streamed_mutation::forwarding fwd = streamed_mutation::forwarding::no,
    mutation_fragment_stream_validation_level validation_level = mutation_fragment_stream_validation_level::none);

// All mutations should have the same schema.
flat_mutation_reader_v2 make_flat_mutation_reader_from_mutations_v2(
    schema_ptr schema,
    reader_permit permit,
    std::vector<mutation>,
    const dht::partition_range& pr,
    streamed_mutation::forwarding fwd = streamed_mutation::forwarding::no,
    mutation_fragment_stream_validation_level validation_level = mutation_fragment_stream_validation_level::none);

// All mutations should have the same schema.
inline flat_mutation_reader_v2 make_flat_mutation_reader_from_mutations_v2(
    schema_ptr schema,
    reader_permit permit,
    std::vector<mutation> ms,
    streamed_mutation::forwarding fwd = streamed_mutation::forwarding::no,
    mutation_fragment_stream_validation_level validation_level = mutation_fragment_stream_validation_level::none) {
    if (ms.size() == 1) {
        constexpr bool reversed = false;
        return make_flat_mutation_reader_from_mutations_v2(std::move(schema), std::move(permit), std::move(ms.back()), fwd, reversed, validation_level);
    }
    return make_flat_mutation_reader_from_mutations_v2(std::move(schema), std::move(permit), std::move(ms), query::full_partition_range, fwd, validation_level);
}

// All mutations should have the same schema.
flat_mutation_reader_v2
make_flat_mutation_reader_from_mutations_v2(
    schema_ptr schema,
    reader_permit permit,
    std::vector<mutation> ms,
    const dht::partition_range& pr,
    const query::partition_slice& slice,
    streamed_mutation::forwarding fwd = streamed_mutation::forwarding::no,
    mutation_fragment_stream_validation_level validation_level = mutation_fragment_stream_validation_level::none);

// All mutations should have the same schema.
inline flat_mutation_reader_v2
make_flat_mutation_reader_from_mutations_v2(
    schema_ptr schema,
    reader_permit permit,
    std::vector<mutation> ms,
    const query::partition_slice& slice,
    streamed_mutation::forwarding fwd = streamed_mutation::forwarding::no,
    mutation_fragment_stream_validation_level validation_level = mutation_fragment_stream_validation_level::none) {
    if (ms.size() == 1) {
        return make_flat_mutation_reader_from_mutations_v2(std::move(schema), std::move(permit), std::move(ms.back()), slice, fwd, validation_level);
    }
    return make_flat_mutation_reader_from_mutations_v2(std::move(schema), std::move(permit), std::move(ms), query::full_partition_range, slice, fwd, validation_level);
}
