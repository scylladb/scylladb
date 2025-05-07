/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "upload_sinks.hh"
#include "utils/log.hh"

namespace s3 {

extern logging::logger s3l;

client::upload_sink_base::upload_sink_base(shared_ptr<client> cln, sstring object_name, std::optional<tag> tag, seastar::abort_source* as)
    : multipart_upload(std::move(cln), std::move(object_name), std::move(tag), as) {
}

future<> client::upload_sink_base::put(net::packet) {
    throw_with_backtrace<std::runtime_error>("s3 put(net::packet) unsupported");
}

future<> client::upload_sink_base::close() {
    if (upload_started()) {
        s3l.warn("closing incomplete multipart upload -> aborting");
        // If we got here, we need to pick up any background activity as it may
        // still trying to handle successful request and 'this' should remain alive
        //
        // The gate might have been closed by finalize_upload() so need to avoid
        // double close
        if (!_bg_flushes.is_closed()) {
            co_await _bg_flushes.close();
        }
        co_await abort_upload();
    } else {
        s3l.trace("closing multipart upload");
    }
}

size_t client::upload_sink_base::buffer_size() const noexcept {
    return 128 * 1024;
}


future<> client::upload_sink::maybe_flush() {
    if (_bufs.size() >= aws_minimum_part_size) {
        co_await upload_part(std::move(_bufs));
    }
}

client::upload_sink::upload_sink(shared_ptr<client> cln, sstring object_name, std::optional<tag> tag, seastar::abort_source* as)
    : upload_sink_base(std::move(cln), std::move(object_name), std::move(tag), as) {
}

future<> client::upload_sink::put(temporary_buffer<char> buf) {
    _bufs.put(std::move(buf));
    return maybe_flush();
}

future<> client::upload_sink::put(std::vector<temporary_buffer<char>> data) {
    for (auto&& buf : data) {
        _bufs.put(std::move(buf));
    }
    return maybe_flush();
}

future<> client::upload_sink::flush() {
    if (_bufs.size() != 0) {
        // This is handy for small objects that are uploaded via the sink. It makes
        // upload happen in one REST call, instead of three (create + PUT + wrap-up)
        if (!upload_started()) {
            s3l.trace("Sink fallback to plain PUT for {}", _object_name);
            co_return co_await _client->put_object(_object_name, std::move(_bufs));
        }

        co_await upload_part(std::move(_bufs));
    }
    if (upload_started()) {
        std::exception_ptr ex;
        try {
            co_await finalize_upload();
        } catch (...) {
            ex = std::current_exception();
        }
        if (ex) {
            co_await abort_upload();
            std::rethrow_exception(ex);
        }
    }
}


future<> client::upload_jumbo_sink::maybe_flush() {
    if (_current->parts_count() >= _maximum_parts_in_piece) {
        auto next = std::make_unique<upload_sink>(_client, format("{}_{}", _object_name, parts_count() + 1), piece_tag);
        co_await upload_part(std::exchange(_current, std::move(next)));
        s3l.trace("Initiated {} piece (upload_id {})", parts_count(), _upload_id);
    }
}

client::upload_jumbo_sink::upload_jumbo_sink(shared_ptr<client> cln,
                                             sstring object_name,
                                             std::optional<unsigned> max_parts_per_piece,
                                             seastar::abort_source* as)
    : upload_sink_base(std::move(cln), std::move(object_name), std::nullopt, as)
    , _maximum_parts_in_piece(max_parts_per_piece.value_or(aws_maximum_parts_in_piece))
    , _current(std::make_unique<upload_sink>(_client, format("{}_{}", _object_name, parts_count()), piece_tag)) {
}

future<> client::upload_jumbo_sink::put(temporary_buffer<char> buf) {
    co_await _current->put(std::move(buf));
    co_await maybe_flush();
}

future<> client::upload_jumbo_sink::put(std::vector<temporary_buffer<char>> data) {
    co_await _current->put(std::move(data));
    co_await maybe_flush();
}

future<> client::upload_jumbo_sink::flush() {
    if (_current) {
        co_await upload_part(std::exchange(_current, nullptr));
    }
    if (upload_started()) {
        std::exception_ptr ex;
        try {
            co_await finalize_upload();
        } catch (...) {
            ex = std::current_exception();
        }
        if (ex) {
            co_await abort_upload();
            std::rethrow_exception(ex);
        }
    }
}

future<> client::upload_jumbo_sink::close() {
    if (_current) {
        co_await _current->close();
        _current.reset();
    }
    co_await upload_sink_base::close();
}

} // s3