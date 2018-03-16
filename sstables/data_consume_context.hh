/*
 * Copyright (C) 2018 ScyllaDB
 *
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

#include <memory>

#include <seastar/core/future.hh>
#include <seastar/util/optimized_optional.hh>

#include "shared_sstable.hh"

namespace sstables {

class data_consume_rows_context;

class reader_position_tracker;

// data_consume_context is an object returned by sstable::data_consume_rows()
// which allows knowing when the consumer stops reading, and starting it again
// (e.g., when the consumer wants to stop after every sstable row).
//
// The read() method initiates reading into the consumer, and continues to
// read and feed data into the consumer until one of the consumer's callbacks
// requests to stop,  or until we reach the end of the data range originally
// requested. read() returns a future which completes when reading stopped.
// If we're at the end-of-file, the read may complete without reading anything
// so it's the consumer class's task to check if anything was consumed.
// Note:
// The caller MUST ensure that between calling read() on this object,
// and the time the returned future is completed, the object lives on.
// Moreover, the sstable object used for the sstable::data_consume_rows()
// call which created this data_consume_context, must also be kept alive.
//
// data_consume_rows() and data_consume_rows_at_once() both can read just a
// single row or many rows. The difference is that data_consume_rows_at_once()
// is optimized to reading one or few rows (reading it all into memory), while
// data_consume_rows() uses a read buffer, so not all the rows need to fit
// memory in the same time (they are delivered to the consumer one by one).
class data_consume_context {
    shared_sstable _sst;
    std::unique_ptr<data_consume_rows_context> _ctx;

    // This object can only be constructed by sstable::data_consume_rows()
    data_consume_context(shared_sstable, row_consumer &consumer, input_stream<char> &&input, uint64_t start,
                         uint64_t maxlen);

    friend class sstable;

    data_consume_context();

    explicit operator bool() const noexcept;

    friend class optimized_optional<data_consume_context>;

public:
    future<> read();

    future<> fast_forward_to(uint64_t begin, uint64_t end);

    future<> skip_to(indexable_element, uint64_t begin);

    const reader_position_tracker &reader_position() const;

    bool eof() const;

    ~data_consume_context();

    data_consume_context(data_consume_context &&) noexcept;

    data_consume_context &operator=(data_consume_context &&) noexcept;
};

using data_consume_context_opt = optimized_optional<data_consume_context>;

}