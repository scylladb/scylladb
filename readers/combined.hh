/*
 * Copyright (C) 2012-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "schema/schema_fwd.hh"
#include "dht/ring_position.hh"
#include "readers/mutation_reader_fwd.hh"
#include "readers/mutation_reader.hh"

class reader_permit;

class reader_selector {
protected:
    schema_ptr _s;
    dht::ring_position_view _selector_position;
public:
    reader_selector(schema_ptr s, dht::ring_position_view rpv) noexcept : _s(std::move(s)), _selector_position(std::move(rpv)) {}

    virtual ~reader_selector() = default;
    // Call only if has_new_readers() returned true.
    virtual std::vector<mutation_reader> create_new_readers(const std::optional<dht::ring_position_view>& pos) = 0;
    virtual std::vector<mutation_reader> fast_forward_to(const dht::partition_range& pr) = 0;

    // Can be false-positive but never false-negative!
    bool has_new_readers(const std::optional<dht::ring_position_view>& pos) const noexcept {
        dht::ring_position_comparator cmp(*_s);
        return !_selector_position.is_max() && (!pos || cmp(*pos, _selector_position) >= 0);
    }
};

// Creates a mutation reader which combines data return by supplied readers.
// Returns mutation of the same schema only when all readers return mutations
// of the same schema.
mutation_reader make_combined_reader(schema_ptr schema,
        reader_permit permit,
        std::vector<mutation_reader>,
        streamed_mutation::forwarding fwd_sm = streamed_mutation::forwarding::no,
        mutation_reader::forwarding fwd_mr = mutation_reader::forwarding::yes);
mutation_reader make_combined_reader(schema_ptr schema,
        reader_permit permit,
        std::unique_ptr<reader_selector>,
        streamed_mutation::forwarding,
        mutation_reader::forwarding);
mutation_reader make_combined_reader(schema_ptr schema,
        reader_permit permit,
        mutation_reader&& a,
        mutation_reader&& b,
        streamed_mutation::forwarding fwd_sm = streamed_mutation::forwarding::no,
        mutation_reader::forwarding fwd_mr = mutation_reader::forwarding::yes);
