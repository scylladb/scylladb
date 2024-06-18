/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "feed_writers.hh"

namespace mutation_writer {

bucket_writer_v2::bucket_writer_v2(schema_ptr schema, std::pair<mutation_reader, queue_reader_handle_v2> queue_reader, reader_consumer_v2& consumer)
    : _schema(schema)
    , _handle(std::move(queue_reader.second))
    , _consume_fut(consumer(std::move(queue_reader.first)))
{ }

bucket_writer_v2::bucket_writer_v2(schema_ptr schema, reader_permit permit, reader_consumer_v2& consumer)
    : bucket_writer_v2(schema, make_queue_reader_v2(schema, std::move(permit)), consumer)
{ }

future<> bucket_writer_v2::consume(mutation_fragment_v2 mf) {
    if (_handle.is_terminated()) {
        // When the handle is terminated, it was aborted
        // or associated reader was closed prematurely.
        // In this case return _consume_fut that will propagate
        // the root-cause error.
        auto ex = _handle.get_exception();
        if (!ex) {
            // shouldn't really happen
            ex = make_exception_ptr(std::runtime_error("queue_reader_handle_v2 is terminated"));
        }
        return std::exchange(_consume_fut, make_exception_future<>(ex)).then([ex = std::move(ex)] () mutable {
            return make_exception_future<>(std::move(ex));
        });
    }
    return _handle.push(std::move(mf));
}

void bucket_writer_v2::consume_end_of_stream() {
    _handle.push_end_of_stream();
}

void bucket_writer_v2::abort(std::exception_ptr ep) noexcept {
    _handle.abort(std::move(ep));
}

future<> bucket_writer_v2::close() noexcept {
    return std::move(_consume_fut);
}

} // mutation_writer
