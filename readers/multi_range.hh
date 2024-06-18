/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once
#include "schema/schema_fwd.hh"
#include "dht/i_partitioner_fwd.hh"
#include <functional>
#include <optional>
#include "readers/mutation_reader_fwd.hh"
#include "readers/mutation_reader.hh"
#include "tracing/trace_state.hh"

using namespace seastar;

class mutation_reader;
class reader_permit;
class mutation_source;

namespace query {
    class partition_slice;
}


// Make a reader that enables the wrapped reader to work with multiple ranges.
///
/// \param ranges An range vector that has to contain strictly monotonic
///     partition ranges, such that successively calling
///     `mutation_reader::fast_forward_to()` with each one is valid.
///     An range vector range with 0 or 1 elements is also valid.
/// \param fwd_mr It is only respected when `ranges` contains 0 or 1 partition
///     ranges. Otherwise the reader is created with
///     mutation_reader::forwarding::yes.
mutation_reader
make_flat_multi_range_reader(
        schema_ptr s, reader_permit permit, mutation_source source, const dht::partition_range_vector& ranges,
        const query::partition_slice& slice,
        tracing::trace_state_ptr trace_state = nullptr,
        mutation_reader::forwarding fwd_mr = mutation_reader::forwarding::yes);

/// Make a reader that enables the wrapped reader to work with multiple ranges.
///
/// Generator overload. The ranges returned by the generator have to satisfy the
/// same requirements as the `ranges` param of the vector overload.
mutation_reader
make_flat_multi_range_reader(
        schema_ptr s,
        reader_permit permit,
        mutation_source source,
        std::function<std::optional<dht::partition_range>()> generator,
        const query::partition_slice& slice,
        tracing::trace_state_ptr trace_state = nullptr,
        mutation_reader::forwarding fwd_mr = mutation_reader::forwarding::yes);
