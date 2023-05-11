/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <memory>
#include <rapidxml.h>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/classification.hpp>
#include <boost/range/adaptor/map.hpp>
#include <seastar/core/coroutine.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/shared_future.hh>
#include <seastar/coroutine/all.hh>
#include <seastar/net/dns.hh>
#include <seastar/net/tls.hh>
#include <seastar/util/short_streams.hh>
#include <seastar/http/request.hh>
#include "utils/s3/client.hh"
#include "utils/memory_data_sink.hh"
#include "utils/chunked_vector.hh"
#include "utils/aws_sigv4.hh"
#include "db_clock.hh"
#include "log.hh"

namespace utils {

inline size_t iovec_len(const std::vector<iovec>& iov)
{
    size_t ret = 0;
    for (auto&& e : iov) {
        ret += e.iov_len;
    }
    return ret;
}

}

namespace s3 {

static logging::logger s3l("s3");

future<> ignore_reply(const http::reply& rep, input_stream<char>&& in_) {
    auto in = std::move(in_);
    co_await util::skip_entire_stream(in);
}

class dns_connection_factory : public http::experimental::connection_factory {
protected:
    std::string _host;
    int _port;
    struct state {
        bool initialized = false;
        socket_address addr;
        ::shared_ptr<tls::certificate_credentials> creds;
    };
    lw_shared_ptr<state> _state;
    shared_future<> _done;

    future<> initialize(bool use_https) {
        auto state = _state;

        co_await coroutine::all(
            [state, host = _host, port = _port] () -> future<> {
                auto hent = co_await net::dns::get_host_by_name(host, net::inet_address::family::INET);
                state->addr = socket_address(hent.addr_list.front(), port);
            },
            [state, use_https] () -> future<> {
                if (use_https) {
                    tls::credentials_builder cbuild;
                    co_await cbuild.set_system_trust();
                    state->creds = cbuild.build_certificate_credentials();
                }
            }
        );

        state->initialized = true;
        s3l.debug("Initialized factory, address={} tls={}", state->addr, state->creds == nullptr ? "no" : "yes");
    }

public:
    dns_connection_factory(std::string host, int port, bool use_https)
        : _host(std::move(host))
        , _port(port)
        , _state(make_lw_shared<state>())
        , _done(initialize(use_https))
    {
    }

    virtual future<connected_socket> make() override {
        if (!_state->initialized) {
            s3l.debug("Waiting for factory to initialize");
            co_await _done.get_future();
        }

        if (_state->creds) {
            s3l.debug("Making new HTTPS connection addr={} host={}", _state->addr, _host);
            co_return co_await tls::connect(_state->creds, _state->addr, _host);
        } else {
            s3l.debug("Making new HTTP connection");
            co_return co_await seastar::connect(_state->addr, {}, transport::TCP);
        }
    }
};

client::client(std::string host, endpoint_config_ptr cfg, global_factory gf, private_tag)
        : _host(std::move(host))
        , _cfg(std::move(cfg))
        , _http(std::make_unique<dns_connection_factory>(_host, _cfg->port, _cfg->use_https))
        , _gf(std::move(gf))
{
}

void client::update_config(endpoint_config_ptr cfg) {
    if (_cfg->port != cfg->port || _cfg->use_https != cfg->use_https) {
        throw std::runtime_error("Updating port and/or https usage is not possible");
    }
    _cfg = std::move(cfg);
}

shared_ptr<client> client::make(std::string endpoint, endpoint_config_ptr cfg, global_factory gf) {
    return seastar::make_shared<client>(std::move(endpoint), std::move(cfg), std::move(gf), private_tag{});
}

void client::authorize(http::request& req) {
    if (!_cfg->aws) {
        return;
    }

    auto time_point_str = utils::aws::format_time_point(db_clock::now());
    auto time_point_st = time_point_str.substr(0, 8);
    req._headers["x-amz-date"] = time_point_str;
    req._headers["x-amz-content-sha256"] = "UNSIGNED-PAYLOAD";
    std::map<std::string_view, std::string_view> signed_headers;
    sstring signed_headers_list = "";
    // AWS requires all x-... and Host: headers to be signed
    signed_headers["host"] = req._headers["Host"];
    for (const auto& h : req._headers) {
        if (h.first[0] == 'x' && h.first[1] == '-') {
            signed_headers[h.first] = h.second;
        }
    }
    unsigned header_nr = signed_headers.size();
    for (const auto& h : signed_headers) {
        signed_headers_list += format("{}{}", h.first, header_nr == 1 ? "" : ";");
        header_nr--;
    }
    sstring query_string = "";
    std::map<std::string_view, std::string_view> query_parameters;
    for (const auto& q : req.query_parameters) {
        query_parameters[q.first] = q.second;
    }
    unsigned query_nr = query_parameters.size();
    for (const auto& q : query_parameters) {
        query_string += format("{}={}{}", q.first, q.second, query_nr == 1 ? "" : "&");
        query_nr--;
    }
    auto sig = utils::aws::get_signature(_cfg->aws->key, _cfg->aws->secret, _host, req._url, req._method,
        utils::aws::omit_datestamp_expiration_check,
        signed_headers_list, signed_headers,
        utils::aws::unsigned_content,
        _cfg->aws->region, "s3", query_string);
    req._headers["Authorization"] = format("AWS4-HMAC-SHA256 Credential={}/{}/{}/s3/aws4_request,SignedHeaders={},Signature={}", _cfg->aws->key, time_point_st, _cfg->aws->region, signed_headers_list, sig);
}

future<> client::get_object_header(sstring object_name, http::experimental::client::reply_handler handler) {
    s3l.trace("HEAD {}", object_name);
    auto req = http::request::make("HEAD", _host, object_name);
    authorize(req);
    return _http.make_request(std::move(req), std::move(handler));
}

future<uint64_t> client::get_object_size(sstring object_name) {
    uint64_t len = 0;
    co_await get_object_header(std::move(object_name), [&len] (const http::reply& rep, input_stream<char>&& in_) mutable -> future<> {
        len = rep.content_length;
        return make_ready_future<>(); // it's HEAD with no body
    });
    co_return len;
}

// TODO: possibly move this to seastar's http subsystem.
static std::time_t parse_http_last_modified_time(const sstring& object_name, sstring last_modified) {
    std::tm tm = {0};

    // format conforms to HTTP-date, defined in the specification (RFC 7231).
    if (strptime(last_modified.c_str(), "%a, %d %b %Y %H:%M:%S %Z", &tm) == nullptr) {
        s3l.warn("Unable to parse {} as Last-Modified for {}", last_modified, object_name);
    } else {
        s3l.trace("Successfully parsed {} as Last-modified for {}", last_modified, object_name);
    }
    return std::mktime(&tm);
}

future<client::stats> client::get_object_stats(sstring object_name) {
    struct stats st{};
    co_await get_object_header(object_name, [&] (const http::reply& rep, input_stream<char>&& in_) mutable -> future<> {
        st.size = rep.content_length;
        st.last_modified = parse_http_last_modified_time(object_name, rep.get_header("Last-Modified"));
        return make_ready_future<>();
    });
    co_return st;
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
    authorize(req);
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
    authorize(req);
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
    authorize(req);
    co_await _http.make_request(std::move(req), ignore_reply);
}

future<> client::delete_object(sstring object_name) {
    s3l.trace("DELETE {}", object_name);
    auto req = http::request::make("DELETE", _host, object_name);
    authorize(req);
    co_await _http.make_request(std::move(req), ignore_reply, http::reply::status_type::no_content);
}

class client::upload_sink : public data_sink_impl {
    // "Each part must be at least 5 MB in size, except the last part."
    // https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPart.html
    static constexpr size_t minimum_part_size = 5 << 20;
    static constexpr int flush_concurrency = 3;

    shared_ptr<client> _client;
    http::experimental::client& _http;
    sstring _object_name;
    memory_data_sink_buffers _bufs;
    sstring _upload_id;
    utils::chunked_vector<sstring> _part_etags;
    semaphore _flush_sem{flush_concurrency};

    future<> start_upload();
    future<> finalize_upload();
    future<> maybe_flush();
    future<> do_flush();
    future<> upload_part(unsigned part_number, memory_data_sink_buffers bufs);
    future<> abort_upload();

    bool upload_started() const noexcept {
        return !_upload_id.empty();
    }

public:
    upload_sink(shared_ptr<client> cln, sstring object_name)
        : _client(std::move(cln))
        , _http(_client->_http)
        , _object_name(std::move(object_name))
    {
    }

    virtual future<> put(net::packet) override {
        throw_with_backtrace<std::runtime_error>("s3 put(net::packet) unsupported");
    }

    virtual future<> put(temporary_buffer<char> buf) override {
        _bufs.put(std::move(buf));
        return maybe_flush();
    }

    virtual future<> put(std::vector<temporary_buffer<char>> data) override {
        for (auto&& buf : data) {
            _bufs.put(std::move(buf));
        }
        return maybe_flush();
    }

    virtual future<> flush() override {
        return finalize_upload();
    }

    virtual future<> close() override;

    virtual size_t buffer_size() const noexcept override {
        return 128 * 1024;
    }
};

future<> client::upload_sink::maybe_flush() {
    if (_bufs.size() >= minimum_part_size) {
        co_await do_flush();
    }
}

future<> client::upload_sink::do_flush() {
    if (!upload_started()) {
        co_await start_upload();
    }
    auto pn = _part_etags.size();
    _part_etags.emplace_back();
    co_await upload_part(pn, std::move(_bufs));
}

sstring parse_multipart_upload_id(sstring& body) {
    auto doc = std::make_unique<rapidxml::xml_document<>>();
    try {
        doc->parse<0>(body.data());
    } catch (const rapidxml::parse_error& e) {
        s3l.warn("cannot parse initiate multipart upload response: {}", e.what());
        // The caller is supposed to check the upload-id to be empty
        // and handle the error the way it prefers
        return "";
    }
    auto root_node = doc->first_node("InitiateMultipartUploadResult");
    auto uploadid_node = root_node->first_node("UploadId");
    return uploadid_node->value();
}

static constexpr std::string_view multipart_upload_complete_header =
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\r\n"
        "<CompleteMultipartUpload xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">";

static constexpr std::string_view multipart_upload_complete_entry =
        "<Part><ETag>{}</ETag><PartNumber>{}</PartNumber></Part>";

static constexpr std::string_view multipart_upload_complete_trailer =
        "</CompleteMultipartUpload>";

unsigned prepare_multipart_upload_parts(const utils::chunked_vector<sstring>& etags) {
    unsigned ret = multipart_upload_complete_header.size();

    unsigned nr = 1;
    for (auto& etag : etags) {
        if (etag.empty()) {
            // 0 here means some part failed to upload, see comment in upload_part()
            // Caller checks it an aborts the multipart upload altogether
            return 0;
        }
        // length of the format string - four braces + length of the etag + length of the number
        ret += multipart_upload_complete_entry.size() - 4 + etag.size() + format("{}", nr).size();
        nr++;
    }
    ret += multipart_upload_complete_trailer.size();
    return ret;
}

future<> dump_multipart_upload_parts(output_stream<char> out, const utils::chunked_vector<sstring>& etags) {
    std::exception_ptr ex;
    try {
        co_await out.write(multipart_upload_complete_header.data(), multipart_upload_complete_header.size());

        unsigned nr = 1;
        for (auto& etag : etags) {
            assert(!etag.empty());
            co_await out.write(format(multipart_upload_complete_entry.data(), etag, nr));
            nr++;
        }
        co_await out.write(multipart_upload_complete_trailer.data(), multipart_upload_complete_trailer.size());
        co_await out.flush();
    } catch (...) {
        ex = std::current_exception();
    }
    co_await out.close();
    if (ex) {
        co_await coroutine::return_exception_ptr(std::move(ex));
    }
}

future<> client::upload_sink::start_upload() {
    s3l.trace("POST uploads {}", _object_name);
    auto rep = http::request::make("POST", _client->_host, _object_name);
    rep.query_parameters["uploads"] = "";
    _client->authorize(rep);
    co_await _http.make_request(std::move(rep), [this] (const http::reply& rep, input_stream<char>&& in_) -> future<> {
        auto in = std::move(in_);
        auto body = co_await util::read_entire_stream_contiguous(in);
        _upload_id = parse_multipart_upload_id(body);
        if (_upload_id.empty()) {
            co_await coroutine::return_exception(std::runtime_error("cannot initiate upload"));
        }
        s3l.trace("created uploads for {} -> id = {}", _object_name, _upload_id);
    });
}

future<> client::upload_sink::upload_part(unsigned part_number, memory_data_sink_buffers bufs) {
    s3l.trace("PUT part {} {} bytes in {} buffers (upload id {})", part_number, bufs.size(), bufs.buffers().size(), _upload_id);
    auto req = http::request::make("PUT", _client->_host, _object_name);
    req._headers["Content-Length"] = format("{}", bufs.size());
    req.query_parameters["partNumber"] = format("{}", part_number + 1);
    req.query_parameters["uploadId"] = _upload_id;
    req.write_body("bin", bufs.size(), [this, part_number, bufs = std::move(bufs)] (output_stream<char>&& out_) mutable -> future<> {
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
    });

    // Do upload in the background so that several parts could go in parallel.
    // The semaphore is used for two things -- control the concurrency and let
    // the finalize_upload() wait in any background activity before checking
    // the progress.
    //
    // In case part upload goes wrong and doesn't happen, the _part_etags[part]
    // is not set, so the finalize_upload() sees it and aborts the whole thing.
    _client->authorize(req);
    auto units = co_await get_units(_flush_sem, 1);
    (void)_http.make_request(std::move(req), [this, part_number] (const http::reply& rep, input_stream<char>&& in_) mutable -> future<> {
        auto etag = rep.get_header("ETag");
        s3l.trace("uploaded {} part data -> etag = {} (upload id {})", part_number, etag, _upload_id);
        _part_etags[part_number] = std::move(etag);
        return make_ready_future<>();
    }).handle_exception([this, part_number] (auto ex) {
        // ... the exact exception only remains in logs
        s3l.warn("couldn't upload part {}: {} (upload id {})", part_number, ex, _upload_id);
    }).finally([units = std::move(units)] {});
}

future<> client::upload_sink::abort_upload() {
    s3l.trace("DELETE upload {}", _upload_id);
    auto req = http::request::make("DELETE", _client->_host, _object_name);
    req.query_parameters["uploadId"] = std::exchange(_upload_id, ""); // now upload_started() returns false
    _client->authorize(req);
    co_await _http.make_request(std::move(req), ignore_reply, http::reply::status_type::no_content);
}

future<> client::upload_sink::finalize_upload() {
    if (_bufs.size() == 0) {
        co_return;
    }

    co_await do_flush();

    s3l.trace("wait for {} parts to complete (upload id {})", _part_etags.size(), _upload_id);
    co_await _flush_sem.wait(flush_concurrency);

    unsigned parts_xml_len = prepare_multipart_upload_parts(_part_etags);
    if (parts_xml_len == 0) {
        co_await abort_upload();
        co_await coroutine::return_exception(std::runtime_error("couldn't upload parts"));
    }

    s3l.trace("POST upload completion {} parts (upload id {})", _part_etags.size(), _upload_id);
    auto req = http::request::make("POST", _client->_host, _object_name);
    req.query_parameters["uploadId"] = std::exchange(_upload_id, ""); // now upload_started() returns false
    req.write_body("xml", parts_xml_len, [this] (output_stream<char>&& out) -> future<> {
        return dump_multipart_upload_parts(std::move(out), _part_etags);
    });
    _client->authorize(req);
    co_await _http.make_request(std::move(req), ignore_reply);
}

future<> client::upload_sink::close() {
    if (upload_started()) {
        s3l.warn("closing incomplete multipart upload -> aborting");
        co_await abort_upload();
    } else {
        s3l.trace("closing multipart upload");
    }
}

data_sink client::make_upload_sink(sstring object_name) {
    return data_sink(std::make_unique<upload_sink>(shared_from_this(), std::move(object_name)));
}

class client::readable_file : public file_impl {
    shared_ptr<client> _client;
    http::experimental::client& _http;
    sstring _object_name;

    [[noreturn]] void unsupported() {
        throw_with_backtrace<std::logic_error>("unsupported operation on s3 readable file");
    }

public:
    readable_file(shared_ptr<client> cln, sstring object_name)
        : _client(std::move(cln))
        , _http(_client->_http)
        , _object_name(std::move(object_name))
    {
    }

    virtual future<size_t> write_dma(uint64_t pos, const void* buffer, size_t len, const io_priority_class& pc) override { unsupported(); }
    virtual future<size_t> write_dma(uint64_t pos, std::vector<iovec> iov, const io_priority_class& pc) override { unsupported(); }
    virtual future<> truncate(uint64_t length) override { unsupported(); }
    virtual subscription<directory_entry> list_directory(std::function<future<> (directory_entry de)> next) override { unsupported(); }

    virtual future<> flush(void) override { return make_ready_future<>(); }
    virtual future<> allocate(uint64_t position, uint64_t length) override { return make_ready_future<>(); }
    virtual future<> discard(uint64_t offset, uint64_t length) override { return make_ready_future<>(); }

    class readable_file_handle_impl final : public file_handle_impl {
        client::handle _h;
        sstring _object_name;

    public:
        readable_file_handle_impl(client::handle h, sstring object_name)
                : _h(std::move(h))
                , _object_name(std::move(object_name))
        {}

        virtual std::unique_ptr<file_handle_impl> clone() const override {
            return std::make_unique<readable_file_handle_impl>(_h, _object_name);
        }

        virtual shared_ptr<file_impl> to_file() && override {
            return make_shared<readable_file>(std::move(_h).to_client(), std::move(_object_name));
        }
    };

    virtual std::unique_ptr<file_handle_impl> dup() override {
        return std::make_unique<readable_file_handle_impl>(client::handle(*_client), _object_name);
    }

    virtual future<uint64_t> size(void) override {
        return _client->get_object_size(_object_name);
    }

    virtual future<struct stat> stat(void) override {
        auto object_stats = co_await _client->get_object_stats(_object_name);
        struct stat ret {};
        ret.st_nlink = 1;
        ret.st_mode = S_IFREG | S_IRUSR | S_IRGRP | S_IROTH;
        ret.st_size = object_stats.size;
        ret.st_blksize = 1 << 10; // huh?
        ret.st_blocks = object_stats.size >> 9;
        // objects are immutable on S3, therefore we can use Last-Modified to set both st_mtime and st_ctime
        ret.st_mtime = object_stats.last_modified;
        ret.st_ctime = object_stats.last_modified;
        co_return ret;
    }

    virtual future<size_t> read_dma(uint64_t pos, void* buffer, size_t len, const io_priority_class& pc) override {
        auto buf = co_await _client->get_object_contiguous(_object_name, range{ pos, len });
        std::copy_n(buf.get(), buf.size(), reinterpret_cast<uint8_t*>(buffer));
        co_return buf.size();
    }

    virtual future<size_t> read_dma(uint64_t pos, std::vector<iovec> iov, const io_priority_class& pc) override {
        auto buf = co_await _client->get_object_contiguous(_object_name, range{ pos, utils::iovec_len(iov) });
        uint64_t off = 0;
        for (auto& v : iov) {
            auto sz = std::min(v.iov_len, buf.size() - off);
            if (sz == 0) {
                break;
            }
            std::copy_n(buf.get() + off, sz, reinterpret_cast<uint8_t*>(v.iov_base));
            off += sz;
        }
        co_return off;
    }

    virtual future<temporary_buffer<uint8_t>> dma_read_bulk(uint64_t offset, size_t range_size, const io_priority_class& pc) override {
        auto buf = co_await _client->get_object_contiguous(_object_name, range{ offset, range_size });
        co_return temporary_buffer<uint8_t>(reinterpret_cast<uint8_t*>(buf.get_write()), buf.size(), buf.release());
    }

    virtual future<> close() override {
        return make_ready_future<>();
    }
};

file client::make_readable_file(sstring object_name) {
    return file(make_shared<readable_file>(shared_from_this(), std::move(object_name)));
}

future<> client::close() {
    co_await _http.close();
}

} // s3 namespace
