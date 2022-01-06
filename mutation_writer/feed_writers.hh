/*
 * Copyright (C) 2020-present ScyllaDB
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

#include "flat_mutation_reader.hh"
#include "mutation_reader.hh"
#include "seastar/core/coroutine.hh"

namespace mutation_writer {

class bucket_writer {
    schema_ptr _schema;
    queue_reader_handle _handle;
    future<> _consume_fut;

private:
    bucket_writer(schema_ptr schema, std::pair<flat_mutation_reader, queue_reader_handle> queue_reader, reader_consumer& consumer);

public:
    bucket_writer(schema_ptr schema, reader_permit permit, reader_consumer& consumer);

    future<> consume(mutation_fragment mf);

    void consume_end_of_stream();

    void abort(std::exception_ptr ep) noexcept;

    future<> close() noexcept;
};

template <typename Writer>
requires MutationFragmentConsumer<Writer, future<>>
future<> feed_writer(flat_mutation_reader&& rd_ref, Writer wr) {
    // Only move in reader if stack was successfully allocated, so caller can close reader otherwise.
    auto rd = std::move(rd_ref);
    std::exception_ptr ex;
    try {
        while (!rd.is_end_of_stream() || !rd.is_buffer_empty()) {
            co_await rd.fill_buffer();
            while (!rd.is_buffer_empty()) {
                co_await rd.pop_mutation_fragment().consume(wr);
            }
        }
    } catch (...) {
        ex = std::current_exception();
    }

    co_await rd.close();

    try {
        if (ex) {
            wr.abort(ex);
        } else {
            wr.consume_end_of_stream();
        }
    } catch (...) {
        if (!ex) {
            ex = std::current_exception();
        }
    }

    try {
        co_await wr.close();
    } catch (...) {
        if (!ex) {
            ex = std::current_exception();
        }
    }
    if (ex) {
        std::rethrow_exception(ex);
    }
}

class bucket_writer_v2 {
    schema_ptr _schema;
    queue_reader_handle_v2 _handle;
    future<> _consume_fut;

private:
    bucket_writer_v2(schema_ptr schema, std::pair<flat_mutation_reader_v2, queue_reader_handle_v2> queue_reader_v2, reader_consumer_v2& consumer);

public:
    bucket_writer_v2(schema_ptr schema, reader_permit permit, reader_consumer_v2& consumer);

    future<> consume(mutation_fragment_v2 mf);

    void consume_end_of_stream();

    void abort(std::exception_ptr ep) noexcept;

    future<> close() noexcept;
};

template <typename Writer>
requires MutationFragmentConsumerV2<Writer, future<>>
future<> feed_writer(flat_mutation_reader_v2&& rd_ref, Writer wr) {
    // Only move in reader if stack was successfully allocated, so caller can close reader otherwise.
    auto rd = std::move(rd_ref);
    std::exception_ptr ex;
    try {
        while (!rd.is_end_of_stream() || !rd.is_buffer_empty()) {
            co_await rd.fill_buffer();
            while (!rd.is_buffer_empty()) {
                co_await rd.pop_mutation_fragment().consume(wr);
            }
        }
    } catch (...) {
        ex = std::current_exception();
    }

    co_await rd.close();

    try {
        if (ex) {
            wr.abort(ex);
        } else {
            wr.consume_end_of_stream();
        }
    } catch (...) {
        if (!ex) {
            ex = std::current_exception();
        }
    }

    try {
        co_await wr.close();
    } catch (...) {
        if (!ex) {
            ex = std::current_exception();
        }
    }
    if (ex) {
        std::rethrow_exception(ex);
    }
}

}
