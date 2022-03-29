/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <seastar/core/future.hh>
#include "readers/flat_mutation_reader.hh"

/*
 * A helper class that wraps another flat_mutation_reader
 * and splits range tombstones across rows before pushing them
 * to the buffer.
 *
 * Used for bringing mutation stream read from old SSTables formats (ka/la)
 * to the uniformed representation with that of a new format (mc).
 */
class normalizing_reader : public flat_mutation_reader::impl {
    flat_mutation_reader _rd;
    range_tombstone_stream _range_tombstones;
public:
    normalizing_reader(flat_mutation_reader rd);

    virtual future<> fill_buffer() override;

    virtual future<> next_partition() override;

    virtual future<> fast_forward_to(const dht::partition_range& pr) override;

    virtual future<> fast_forward_to(position_range pr) override;

    virtual future<> close() noexcept override;
};

// Creates a mutation_reader wrapper which creates a new stream of mutations
// with some mutations removed from the original stream.
// MutationFilter is a callable which decides which mutations are dropped. It
// accepts mutation const& and returns a bool. The mutation stays in the
// stream if and only if the filter returns true.
flat_mutation_reader make_normalizing_reader(flat_mutation_reader rd);

