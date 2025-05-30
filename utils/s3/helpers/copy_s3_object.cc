/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "copy_s3_object.hh"
#include "utils/log.hh"
#include <seastar/http/request.hh>
#include <seastar/util/short_streams.hh>

namespace s3 {

extern logging::logger s3l;

client::copy_s3_object::copy_s3_object(
    shared_ptr<client> cln, sstring source_object, sstring target_object, size_t part_size, std::optional<tag> tag, abort_source* as)
    : multipart_upload(std::move(cln), std::move(target_object), std::move(tag), as), _max_copy_part_size(part_size), _source_object(std::move(source_object)) {
    assert(_max_copy_part_size > 0 && _max_copy_part_size <= _default_copy_part_size);
}
client::copy_s3_object::copy_s3_object(shared_ptr<client> cln, sstring source_object, sstring target_object, std::optional<tag> tag, abort_source* as)
    : copy_s3_object(std::move(cln), std::move(source_object), std::move(target_object), _default_copy_part_size, std::move(tag), as) {
}
future<> client::copy_s3_object::copy() {
    auto source_size = co_await _client->get_object_size(_source_object);
    if (source_size <= _max_copy_part_size) {
        co_await copy_put();
    } else {
        co_await copy_multipart(source_size);
    }
}
future<> client::copy_s3_object::copy_put() {
    auto req = http::request::make("PUT", _client->_host, _object_name);
    if (_tag) {
        req._headers["x-amz-tagging"] = seastar::format("{}={}", _tag->key, _tag->value);
    }
    req._headers["x-amz-copy-source"] = _source_object;

    co_await _client->make_request(std::move(req), ignore_reply, http::reply::status_type::ok, _as);
}
future<> client::copy_s3_object::copy_multipart(size_t source_size) {
    co_await start_upload();
    auto part_size = _max_copy_part_size;
    std::exception_ptr ex;

    try {
        for (size_t offset = 0; offset < source_size; offset += part_size) {
            part_size = std::min(source_size - offset, part_size);
            co_await copy_part(offset, part_size);
        }
        // Here we are going to finalize the upload and close the _bg_flushes, in case an exception is thrown the
        // gate will be closed and the upload will be aborted. See below.
        co_await finalize_upload();
    } catch (...) {
        ex = std::current_exception();
    }

    if (ex) {
        if (!_bg_flushes.is_closed()) {
            co_await _bg_flushes.close();
        }
        co_await abort_upload();
        std::rethrow_exception(ex);
    }
}
future<> client::copy_s3_object::copy_part(size_t offset, size_t part_size) {
    unsigned part_number = _part_etags.size();
    _part_etags.emplace_back();
    auto req = http::request::make("PUT", _client->_host, _object_name);
    req._headers["x-amz-copy-source"] = _source_object;
    auto range = format("bytes={}-{}", offset, offset + part_size - 1);
    s3l.trace("PUT part {}, Upload range: {}, Upload ID:", part_number, range, _upload_id);

    req._headers["x-amz-copy-source-range"] = range;
    req.query_parameters.emplace("partNumber", to_sstring(part_number + 1));
    req.query_parameters.emplace("uploadId", _upload_id);

    // upload the parts in the background for better throughput
    auto gh = _bg_flushes.hold();
    // Ignoring the result of make_request() because we don't want to block and it is safe since we have a gate we are going to wait on and all argument are
    // captured by value or moved into the fiber
    std::ignore =
        _client
            ->make_request(
                std::move(req),
                [this, part_number, start = s3_clock::now()](group_client& gc, const http::reply& reply, input_stream<char>&& in) -> future<> {
                    return util::read_entire_stream_contiguous(in).then([this, part_number](auto body) mutable {
                        auto etag = parse_multipart_copy_upload_etag(body);
                        if (etag.empty()) {
                            return make_exception_future<>(std::runtime_error("Cannot parse ETag"));
                        }
                        s3l.trace("Part data -> etag = {} (upload id {})", part_number, etag, _upload_id);
                        _part_etags[part_number] = std::move(etag);
                        return make_ready_future<>();
                    });
                },
                http::reply::status_type::ok,
                _as)
            .handle_exception([this, part_number](auto ex) { s3l.warn("Failed to upload part {}, upload id {}. Reason: {}", part_number, _upload_id, ex); })
            .finally([gh = std::move(gh)] {});

    co_return;
}

} // s3