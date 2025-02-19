/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <seastar/coroutine/as_future.hh>

#include "readers/queue.hh"

namespace mutation_writer {

class bucket_writer_v2 {
    schema_ptr _schema;
    queue_reader_handle_v2 _handle;
    future<> _consume_fut;

private:
    bucket_writer_v2(schema_ptr schema, std::pair<mutation_reader, queue_reader_handle_v2> queue_reader_v2, reader_consumer_v2& consumer);

public:
    bucket_writer_v2(schema_ptr schema, reader_permit permit, reader_consumer_v2& consumer);

    future<> consume(mutation_fragment_v2 mf);

    void consume_end_of_stream();

    void abort(std::exception_ptr ep) noexcept;

    future<> close() noexcept;
};

template <typename Writer>
requires MutationFragmentConsumerV2<Writer, future<>>
future<> feed_writer(mutation_reader&& rd_ref, Writer wr) {
    // Only move in reader if stack was successfully allocated, so caller can close reader otherwise.
    auto rd = std::move(rd_ref);
    std::exception_ptr ex;
    try {
        while (!rd.is_end_of_stream()) {
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

    auto f = co_await coroutine::as_future(wr.close());
    if (f.failed()) [[unlikely]] {
        // Need to consume the failed future exception even if not used
        auto close_ex = f.get_exception();
        if (!ex) {
            ex = std::move(close_ex);
        }
    }
    if (ex) [[unlikely]] {
        std::rethrow_exception(ex);
    }
}

}
