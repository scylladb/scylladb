/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "multipart_upload.hh"

#include "upload_sinks.hh"
#include "utils/log.hh"
#include "utils/s3/aws_error.hh"
#include "utils/s3/client.hh"
#include <seastar/core/future.hh>
#include <seastar/http/exception.hh>
#include <seastar/http/reply.hh>
#include <seastar/http/request.hh>
#include <seastar/util/lazy.hh>
#include <seastar/util/short_streams.hh>

namespace s3 {
extern logging::logger s3l;

future<> client::multipart_upload::start_upload() {
    s3l.trace("POST uploads {} (tag {})", _object_name, seastar::value_of([this] { return _tag ? _tag->key + "=" + _tag->value : "none"; }));
    auto rep = http::request::make("POST", _client->_host, _object_name);
    rep.query_parameters["uploads"] = "";
    if (_tag) {
        rep._headers["x-amz-tagging"] = seastar::format("{}={}", _tag->key, _tag->value);
    }
    co_await _client->make_request(std::move(rep), [this] (const http::reply& rep, input_stream<char>&& in_) -> future<> {
        auto in = std::move(in_);
        auto body = co_await util::read_entire_stream_contiguous(in);
        _upload_id = parse_multipart_upload_id(body);
        if (_upload_id.empty()) {
            co_await coroutine::return_exception(std::runtime_error("cannot initiate upload"));
        }
        s3l.trace("created uploads for {} -> id = {}", _object_name, _upload_id);
    }, http::reply::status_type::ok, _as);
}

future<> client::multipart_upload::upload_part(memory_data_sink_buffers bufs) {
    if (!upload_started()) {
        co_await start_upload();
    }

    auto claim = co_await _client->claim_memory(bufs.size());

    unsigned part_number = _part_etags.size();
    _part_etags.emplace_back();
    s3l.trace("PUT part {} {} bytes in {} buffers (upload id {})", part_number, bufs.size(), bufs.buffers().size(), _upload_id);
    auto req = http::request::make("PUT", _client->_host, _object_name);
    auto size = bufs.size();
    req._headers["Content-Length"] = seastar::format("{}", size);
    req.query_parameters["partNumber"] = seastar::format("{}", part_number + 1);
    req.query_parameters["uploadId"] = _upload_id;
    req.write_body("bin", size, [this, part_number, bufs = std::move(bufs), p = std::move(claim)] (output_stream<char>&& out_) mutable -> future<> {
        auto out = std::move(out_);
        std::exception_ptr ex;
        s3l.trace("upload {} part data (upload id {})", part_number, _upload_id);
        try {
            for (auto&& buf : bufs.buffers()) {
                co_await out.write(buf.get(), buf.size());
            }
            co_await out.flush();
        } catch (...) {
            ex = std::current_exception();
        }
        co_await out.close();
        if (ex) {
            co_await coroutine::return_exception_ptr(std::move(ex));
        }
        // note: At this point the buffers are sent, but the response is not yet
        // received. However, claim is released and next part may start uploading
    });

    // Do upload in the background so that several parts could go in parallel.
    // The gate lets the finalize_upload() wait in any background activity
    // before checking the progress.
    //
    // Upload parallelizm is managed per-sched-group -- client maintains a set
    // of http clients each with its own max-connections. When upload happens it
    // will naturally be limited with the relevant http client's connections
    // limit not affecting other groups' requests concurrency
    //
    // In case part upload goes wrong and doesn't happen, the _part_etags[part]
    // is not set, so the finalize_upload() sees it and aborts the whole thing.
    auto gh = _bg_flushes.hold();
    (void)_client->make_request(std::move(req), [this, size, part_number, start = s3_clock::now()] (group_client& gc, const http::reply& rep, input_stream<char>&& in_) mutable -> future<> {
        auto etag = rep.get_header("ETag");
        s3l.trace("uploaded {} part data -> etag = {} (upload id {})", part_number, etag, _upload_id);
        _part_etags[part_number] = std::move(etag);
        gc.write_stats.update(size, s3_clock::now() - start);
        return make_ready_future<>();
    }, http::reply::status_type::ok, _as).handle_exception([this, part_number] (auto ex) {
        // ... the exact exception only remains in logs
        s3l.warn("couldn't upload part {}: {} (upload id {})", part_number, ex, _upload_id);
    }).finally([gh = std::move(gh)] {});
}

future<> client::multipart_upload::upload_part(std::unique_ptr<upload_sink> piece_ptr) {
    if (!upload_started()) {
        co_await start_upload();
    }

    auto& piece = *piece_ptr;
    unsigned part_number = _part_etags.size();
    _part_etags.emplace_back();
    s3l.trace("PUT part {} from {} (upload id {})", part_number, piece._object_name, _upload_id);
    auto req = http::request::make("PUT", _client->_host, _object_name);
    req.query_parameters["partNumber"] = format("{}", part_number + 1);
    req.query_parameters["uploadId"] = _upload_id;
    req._headers["x-amz-copy-source"] = piece._object_name;

    // See comment in upload_part(memory_data_sink_buffers) overload regarding the
    // _bg_flushes usage and _part_etags assignments
    //
    // Before the piece's object can be copied into the target one, it should be
    // flushed and closed. After the object is copied, it can be removed. If copy
    // goes wrong, the object should be removed anyway.
    auto gh = _bg_flushes.hold();
    (void)piece.flush().then([&piece] () {
        return piece.close();
    }).then([this, part_number, req = std::move(req)] () mutable {
        return _client->make_request(std::move(req), [this, part_number] (const http::reply& rep, input_stream<char>&& in_) mutable -> future<> {
            return do_with(std::move(in_), [this, part_number] (auto& in) mutable {
                return util::read_entire_stream_contiguous(in).then([this, part_number] (auto body) mutable {
                    auto etag = parse_multipart_copy_upload_etag(body);
                    if (etag.empty()) {
                        return make_exception_future<>(std::runtime_error("cannot copy part upload"));
                    }
                    s3l.trace("copy-uploaded {} part data -> etag = {} (upload id {})", part_number, etag, _upload_id);
                    _part_etags[part_number] = std::move(etag);
                    return make_ready_future<>();
                });
            });
        }, http::reply::status_type::ok, _as).handle_exception([this, part_number] (auto ex) {
            // ... the exact exception only remains in logs
            s3l.warn("couldn't copy-upload part {}: {} (upload id {})", part_number, ex, _upload_id);
        });
    }).then_wrapped([this, &piece] (auto f) {
        if (f.failed()) {
            s3l.warn("couldn't flush piece {}: {} (upload id {})", piece._object_name, f.get_exception(), _upload_id);
        }
        return _client->delete_object(piece._object_name).handle_exception([&piece] (auto ex) {
            s3l.warn("failed to remove copy-upload piece {}", piece._object_name);
        });
    }).finally([gh = std::move(gh), piece_ptr = std::move(piece_ptr)] {});
}

future<> client::multipart_upload::abort_upload() {
    s3l.trace("DELETE upload {}", _upload_id);
    auto req = http::request::make("DELETE", _client->_host, _object_name);
    req.query_parameters["uploadId"] = std::exchange(_upload_id, ""); // now upload_started() returns false
    co_await _client->make_request(std::move(req), ignore_reply, http::reply::status_type::no_content)
        .handle_exception([this](const std::exception_ptr& ex) -> future<> {
            // Here we discard whatever exception is thrown when aborting multipart upload since we don't care about cleanly aborting it since there are other
            // means to clean up dangling parts, for example `rclone cleanup` or S3 bucket's Lifecycle Management Policy
            s3l.warn("Failed to abort multipart upload. Object: '{}'. Reason: {})", _object_name, ex);
            co_return;
        });
}

bool client::multipart_upload::upload_started() const noexcept {
    return !_upload_id.empty();
}

client::multipart_upload::multipart_upload(seastar::shared_ptr<client> cln, seastar::sstring object_name, std::optional<tag> tag, seastar::abort_source* as)
    : _client(std::move(cln)), _object_name(std::move(object_name)), _bg_flushes("s3::client::multipart_upload::bg_flushes"), _tag(std::move(tag)), _as(as) {
}

future<> client::multipart_upload::finalize_upload() {
    s3l.trace("wait for {} parts to complete (upload id {})", _part_etags.size(), _upload_id);
    co_await _bg_flushes.close();

    unsigned parts_xml_len = prepare_multipart_upload_parts(_part_etags);
    if (parts_xml_len == 0) {
        co_await coroutine::return_exception(std::runtime_error("Failed to parse ETag list. Aborting multipart upload."));
    }

    s3l.trace("POST upload completion {} parts (upload id {})", _part_etags.size(), _upload_id);
    auto req = http::request::make("POST", _client->_host, _object_name);
    req.query_parameters["uploadId"] = _upload_id;
    req.write_body("xml", parts_xml_len, [this] (output_stream<char>&& out) -> future<> {
        return dump_multipart_upload_parts(std::move(out), _part_etags);
    });
    // If this request fails, finalize_upload() throws, the upload should then
    // be aborted in .close() method
    co_await _client->make_request(std::move(req), [](const http::reply& rep, input_stream<char>&& in) -> future<> {
        auto payload = std::move(in);
        auto status_class = http::reply::classify_status(rep._status);
        std::optional<aws::aws_error> possible_error = aws::aws_error::parse(co_await util::read_entire_stream_contiguous(payload));
        if (possible_error) {
            co_await coroutine::return_exception(aws::aws_exception(std::move(possible_error.value())));
        }

        if (status_class != http::reply::status_class::informational && status_class != http::reply::status_class::success) {
            co_await coroutine::return_exception(aws::aws_exception(aws::aws_error::from_http_code(rep._status)));
        }

        if (rep._status != http::reply::status_type::ok) {
            co_await coroutine::return_exception(httpd::unexpected_status_error(rep._status));
        }
        // If we reach this point it means the request succeeded. However, the body payload was already consumed, so no response handler was invoked. At
        // this point it is ok since we are not interested in parsing this particular response
    }, http::reply::status_type::ok);
    _upload_id = ""; // now upload_started() returns false
}
} // s3