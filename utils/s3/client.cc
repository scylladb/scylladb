/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <seastar/core/coroutine.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/util/short_streams.hh>
#include <seastar/http/request.hh>
#include "utils/s3/client.hh"
#include "utils/memory_data_sink.hh"
#include "utils/chunked_vector.hh"
#include "log.hh"

namespace s3 {

static logging::logger s3l("s3");

future<> ignore_reply(const http::reply& rep, input_stream<char>&& in_) {
    auto in = std::move(in_);
    co_await util::skip_entire_stream(in);
}

client::client(socket_address addr, private_tag)
        : _addr(std::move(addr))
        , _host(to_sstring(_addr))
        , _http(_addr)
{
}

shared_ptr<client> client::make(socket_address addr) {
    return seastar::make_shared<client>(std::move(addr), private_tag{});
}

future<uint64_t> client::get_object_size(sstring object_name) {
    s3l.trace("HEAD {}", object_name);
    auto req = http::request::make("HEAD", _host, object_name);
    uint64_t len = 0;
    co_await _http.make_request(std::move(req), [&len] (const http::reply& rep, input_stream<char>&& in_) mutable -> future<> {
        len = rep.content_length;
        return make_ready_future<>(); // it's HEAD with no body
    });
    co_return len;
}

future<temporary_buffer<char>> client::get_object_contiguous(sstring object_name, std::optional<range> range) {
    auto req = http::request::make("GET", _host, object_name);
    http::reply::status_type expected = http::reply::status_type::ok;
    if (range) {
        auto range_header = format("bytes={}-{}", range->off, range->off + range->len - 1);
        s3l.trace("GET {} contiguous range='{}'", object_name, range_header);
        req._headers["Range"] = std::move(range_header);
        expected = http::reply::status_type::partial_content;
    } else {
        s3l.trace("GET {} contiguous", object_name);
    }

    size_t off = 0;
    std::optional<temporary_buffer<char>> ret;
    co_await _http.make_request(std::move(req), [&off, &ret, &object_name] (const http::reply& rep, input_stream<char>&& in_) mutable -> future<> {
        auto in = std::move(in_);
        ret = temporary_buffer<char>(rep.content_length);
        s3l.trace("Consume {} bytes for {}", ret->size(), object_name);
        co_await in.consume([&off, &ret] (temporary_buffer<char> buf) mutable {
            if (buf.empty()) {
                return make_ready_future<consumption_result<char>>(stop_consuming(std::move(buf)));
            }

            size_t to_copy = std::min(ret->size() - off, buf.size());
            if (to_copy > 0) {
                std::copy_n(buf.get(), to_copy, ret->get_write() + off);
                off += to_copy;
            }
            return make_ready_future<consumption_result<char>>(continue_consuming());
        });
    }, expected);
    ret->trim(off);
    s3l.trace("Consumed {} bytes of {}", off, object_name);
    co_return std::move(*ret);
}

future<> client::put_object(sstring object_name, temporary_buffer<char> buf) {
    s3l.trace("PUT {}", object_name);
    auto req = http::request::make("PUT", _host, object_name);
    auto len = buf.size();
    req.write_body("bin", len, [buf = std::move(buf)] (output_stream<char>&& out_) mutable -> future<> {
        auto out = std::move(out_);
        std::exception_ptr ex;
        try {
            co_await out.write(buf.get(), buf.size());
            co_await out.flush();
        } catch (...) {
            ex = std::current_exception();
        }
        co_await out.close();
        if (ex) {
            co_await coroutine::return_exception_ptr(std::move(ex));
        }
    });
    co_await _http.make_request(std::move(req), ignore_reply);
}

future<> client::put_object(sstring object_name, ::memory_data_sink_buffers bufs) {
    s3l.trace("PUT {} (buffers)", object_name);
    auto req = http::request::make("PUT", _host, object_name);
    auto len = bufs.size();
    req.write_body("bin", len, [bufs = std::move(bufs)] (output_stream<char>&& out_) mutable -> future<> {
        auto out = std::move(out_);
        std::exception_ptr ex;
        try {
            for (const auto& buf : bufs.buffers()) {
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
    });
    co_await _http.make_request(std::move(req), ignore_reply);
}

future<> client::delete_object(sstring object_name) {
    s3l.trace("DELETE {}", object_name);
    auto req = http::request::make("DELETE", _host, object_name);
    co_await _http.make_request(std::move(req), ignore_reply, http::reply::status_type::no_content);
}

future<> client::close() {
    co_await _http.close();
}

} // s3 namespace
