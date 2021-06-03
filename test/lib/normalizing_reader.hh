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

#include "mutation_reader.hh"
#include <seastar/core/future.hh>
#include "flat_mutation_reader.hh"

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

    virtual future<> fill_buffer(db::timeout_clock::time_point timeout) override;

    virtual future<> next_partition() override;

    virtual future<> fast_forward_to(const dht::partition_range& pr, db::timeout_clock::time_point timeout) override;

    virtual future<> fast_forward_to(position_range pr, db::timeout_clock::time_point timeout) override;

    virtual future<> close() noexcept override;
};

// Creates a mutation_reader wrapper which creates a new stream of mutations
// with some mutations removed from the original stream.
// MutationFilter is a callable which decides which mutations are dropped. It
// accepts mutation const& and returns a bool. The mutation stays in the
// stream if and only if the filter returns true.
flat_mutation_reader make_normalizing_reader(flat_mutation_reader rd);

