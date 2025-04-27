/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "do_upload_file.hh"

#include "utils/div_ceil.hh"
#include "utils/log.hh"
#include <seastar/http/request.hh>
#include <seastar/core/on_internal_error.hh>

namespace s3 {

extern logging::logger s3l;

file_input_stream_options client::do_upload_file::input_stream_options() {
    // optimized for throughput
    return {
        .buffer_size = 128_KiB,
        .read_ahead = 4,
    };
}

future<> client::do_upload_file::copy_to(input_stream<char> input, output_stream<char> output, size_t unit_size, upload_progress& progress) {
    std::exception_ptr ex;
    try {
        for (;;) {
            auto buf = co_await input.read_up_to(unit_size);
            if (buf.empty()) {
                break;
            }
            const size_t buf_size = buf.size();
            co_await output.write(std::move(buf));
            progress.uploaded += buf_size;
        }
        co_await output.flush();
    } catch (...) {
        ex = std::current_exception();
    }
    co_await output.close();
    co_await input.close();
    if (ex) {
        co_await coroutine::return_exception_ptr(std::move(ex));
    }
}

future<> client::do_upload_file::upload_part(file f, uint64_t offset, uint64_t part_size) {
    // upload a part in a multipart upload, see
    // https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPart.html
    auto mem_units = co_await _client->claim_memory(_transmit_size);

    unsigned part_number = _part_etags.size();
    _part_etags.emplace_back();
    auto req = http::request::make("PUT", _client->_host, _object_name);
    req._headers["Content-Length"] = to_sstring(part_size);
    req.query_parameters.emplace("partNumber", to_sstring(part_number + 1));
    req.query_parameters.emplace("uploadId", _upload_id);
    s3l.trace("PUT part {}, {} bytes (upload id {})", part_number, part_size, _upload_id);
    req.write_body(
        "bin", part_size, [f = std::move(f), mem_units = std::move(mem_units), offset, part_size, &progress = _progress](output_stream<char>&& out_) {
            auto input = make_file_input_stream(f, offset, part_size, input_stream_options());
            auto output = std::move(out_);
            return copy_to(std::move(input), std::move(output), _transmit_size, progress);
        });
    // upload the parts in the background for better throughput
    auto gh = _bg_flushes.hold();
    std::ignore = _client
                      ->make_request(
                          std::move(req),
                          [this, part_size, part_number, start = s3_clock::now()](
                              group_client& gc, const http::reply& reply, input_stream<char>&& in_) mutable -> future<> {
                              auto etag = reply.get_header("ETag");
                              s3l.trace("uploaded {} part data -> etag = {} (upload id {})", part_number, etag, _upload_id);
                              _part_etags[part_number] = std::move(etag);
                              gc.write_stats.update(part_size, s3_clock::now() - start);
                              return make_ready_future();
                          },
                          http::reply::status_type::ok,
                          _as)
                      .handle_exception([this, part_number](auto ex) { s3l.warn("couldn't upload part {}: {} (upload id {})", part_number, ex, _upload_id); })
                      .finally([gh = std::move(gh)] {});
}

std::pair<unsigned, size_t> client::do_upload_file::calc_part_size(size_t total_size, size_t part_size) {
    if (part_size > 0) {
        if (part_size < aws_minimum_part_size) {
            on_internal_error(s3l, fmt::format("part_size too large: {} < {}", part_size, aws_minimum_part_size));
        }
        const size_t num_parts = div_ceil(total_size, part_size);
        if (num_parts > aws_maximum_parts_in_piece) {
            on_internal_error(s3l, fmt::format("too many parts: {} > {}", num_parts, aws_maximum_parts_in_piece));
        }
        return {num_parts, part_size};
    }
    // if part_size is 0, this means the caller leaves it to us to decide
    // the part_size. to be more reliance, say, we don't have to re-upload
    // a giant chunk of buffer if a certain part fails to upload, we prefer
    // small parts, let's make it a multiple of MiB.
    part_size = div_ceil(total_size / aws_maximum_parts_in_piece, 1_MiB);
    // The default part size for multipart upload is set to 50MiB.
    // This value was determined empirically by running `perf_s3_client` with various part sizes to find the optimal one.
    static constexpr size_t default_part_size = 50_MiB;

    part_size = std::max(part_size, default_part_size);
    return {div_ceil(total_size, part_size), part_size};
}

future<> client::do_upload_file::multi_part_upload(file&& f, uint64_t total_size, size_t part_size) {
    co_await start_upload();

    std::exception_ptr ex;
    try {
        for (size_t offset = 0; offset < total_size; offset += part_size) {
            part_size = std::min(total_size - offset, part_size);
            s3l.trace("upload_part: {}~{}/{}", offset, part_size, total_size);
            co_await upload_part(file{f}, offset, part_size);
        }

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

future<> client::do_upload_file::put_object(file&& f, uint64_t len) {
    // https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html
    s3l.trace("PUT {} ({})", _object_name, _path.native());
    auto mem_units = co_await _client->claim_memory(_transmit_size);

    auto req = http::request::make("PUT", _client->_host, _object_name);
    if (_tag) {
        req._headers["x-amz-tagging"] = seastar::format("{}={}", _tag->key, _tag->value);
    }
    req.write_body("bin", len, [f = std::move(f), &progress = _progress](output_stream<char>&& out_) {
        auto input = make_file_input_stream(f, input_stream_options());
        auto output = std::move(out_);
        return copy_to(std::move(input), std::move(output), _transmit_size, progress);
    });
    co_await _client->make_request(
        std::move(req),
        [len, start = s3_clock::now()](group_client& gc, const auto& rep, auto&& in) {
            gc.write_stats.update(len, s3_clock::now() - start);
            return ignore_reply(rep, std::move(in));
        },
        http::reply::status_type::ok,
        _as);
}

client::do_upload_file::do_upload_file(shared_ptr<client> cln,
                                       std::filesystem::path path,
                                       sstring object_name,
                                       std::optional<tag> tag,
                                       size_t part_size,
                                       upload_progress& up,
                                       seastar::abort_source* as)
    : multipart_upload(std::move(cln), std::move(object_name), std::move(tag), as), _path{std::move(path)}, _part_size(part_size), _progress(up) {
}

future<> client::do_upload_file::upload() {
    auto f = co_await open_file_dma(_path.native(), open_flags::ro);
    const auto stat = co_await f.stat();
    const uint64_t file_size = stat.st_size;
    _progress.total += file_size;
    // use multipart upload when possible in order to transmit parts in
    // parallel to improve throughput
    if (file_size > aws_minimum_part_size) {
        auto [num_parts, part_size] = calc_part_size(file_size, _part_size);
        _part_etags.reserve(num_parts);
        co_await multi_part_upload(std::move(f), file_size, part_size);
    } else {
        // single part upload
        co_await put_object(std::move(f), file_size);
    }
}
} // s3