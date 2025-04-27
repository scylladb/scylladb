/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "download_source.hh"

#include "utils/log.hh"
#include <seastar/http/exception.hh>
#include <seastar/http/request.hh>

namespace s3 {

extern logging::logger s3l;

client::download_source::download_source(shared_ptr<client> cln, sstring object_name, std::optional<range> range, seastar::abort_source* as)
    : _client(std::move(cln))
    , _object_name(std::move(object_name))
    , _as(as)
    , _range(range.value_or(s3::range{0, std::numeric_limits<uint64_t>::max()}))
    , _bg("s3::client::download_source") {
}

future<temporary_buffer<char>> client::download_source::get() {
    while (true) {
        if (_body.has_value()) {
            try {
                auto buf = co_await _body->b.read_up_to(_range.len);
                _range.off += buf.size();
                _range.len -= buf.size();
                s3l.trace("GET {} got the {}-bytes buffer", _object_name, buf.size());
                if (buf.empty()) {
                    _body->done.set_value();
                    _body.reset();
                }
                co_return std::move(buf);
            } catch (...) {
                s3l.trace("GET {} error reading body, completing it and re-trying", _object_name);
                _body->done.set_exception(std::current_exception());
                _body.reset();
            }
        }

        auto xb = co_await request_body();
        _body.emplace(std::move(xb));
    }
}

future<> client::download_source::close() {
    if (_body.has_value()) {
        _body->done.set_value();
        _body.reset();
    }
    return _bg.close();
}

auto client::download_source::request_body() -> future<external_body> {
    auto req = http::request::make("GET", _client->_host, _object_name);
    auto range_header = format_range_header(_range);
    s3l.trace("GET {} download range {}:{}", _object_name, _range.off, _range.len);
    req._headers["Range"] = std::move(range_header);

    auto bp = std::make_unique<std::optional<promise<external_body>>>(std::in_place);
    auto& p = *bp;
    future<external_body> f = p->get_future();

    (void)_client
        ->make_request(
            std::move(req),
            [this, &p](group_client& gc, const http::reply& rep, input_stream<char>&& in_) mutable -> future<> {
                s3l.trace("GET {} got the body ({} {} bytes)", _object_name, rep._status, rep.content_length);
                if (rep._status != http::reply::status_type::partial_content && rep._status != http::reply::status_type::ok) {
                    co_await coroutine::return_exception(httpd::unexpected_status_error(rep._status));
                }

                auto in = std::move(in_);
                external_body xb(in);
                auto f = xb.done.get_future();
                p->set_value(std::move(xb));
                p.reset();
                co_await std::move(f);
            },
            {},
            _as)
        .handle_exception([&p](auto ex) {
            if (p.has_value()) {
                p->set_exception(std::move(ex));
            }
        })
        .finally([bp = std::move(bp), h = _bg.hold()] {});

    return f;
}

} // s3