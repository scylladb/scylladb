/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "feed_writers.hh"

namespace mutation_writer {

bucket_writer::bucket_writer(schema_ptr schema, std::pair<mutation_reader, queue_reader_handle> queue_reader, mutation_reader_consumer& consumer)
    : _schema(schema)
    , _handle(std::move(queue_reader.second))
    , _consume_fut(consumer(std::move(queue_reader.first)))
{ }

bucket_writer::bucket_writer(schema_ptr schema, reader_permit permit, mutation_reader_consumer& consumer)
    : bucket_writer(schema, make_queue_reader(schema, std::move(permit)), consumer)
{ }

future<> bucket_writer::consume(mutation_fragment_v2 mf) {
    if (_handle.is_terminated()) {
        // When the handle is terminated, it was aborted
        // or associated reader was closed prematurely.
        // In this case return _consume_fut that will propagate
        // the root-cause error.
        auto ex = _handle.get_exception();
        if (!ex) {
            // shouldn't really happen
            ex = make_exception_ptr(std::runtime_error("queue_reader_handle is terminated"));
        }
        return std::exchange(_consume_fut, make_exception_future<>(ex)).then([ex = std::move(ex)] () mutable {
            return make_exception_future<>(std::move(ex));
        });
    }
    return _handle.push(std::move(mf));
}

void bucket_writer::consume_end_of_stream() {
    _handle.push_end_of_stream();
}

void bucket_writer::abort(std::exception_ptr ep) noexcept {
    _handle.abort(std::move(ep));
}

future<> bucket_writer::close() noexcept {
    return std::move(_consume_fut);
}

} // mutation_writer
