/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <initializer_list>
#include <memory>
#include <stdexcept>
#include <rapidxml.h>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/classification.hpp>
#include <boost/range/adaptor/map.hpp>
#include <seastar/core/coroutine.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/shared_future.hh>
#include <seastar/coroutine/all.hh>
#include <seastar/coroutine/parallel_for_each.hh>
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

template <>
struct fmt::formatter<s3::tag> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    auto format(const s3::tag& tag, fmt::format_context& ctx) const {
        return fmt::format_to(ctx.out(), "<Tag><Key>{}</Key><Value>{}</Value></Tag>",
                              tag.key, tag.value);
    }
};

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
    logging::logger& _logger;
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
        _logger.debug("Initialized factory, address={} tls={}", state->addr, state->creds == nullptr ? "no" : "yes");
    }

public:
    dns_connection_factory(std::string host, int port, bool use_https, logging::logger& logger)
        : _host(std::move(host))
        , _port(port)
        , _logger(logger)
        , _state(make_lw_shared<state>())
        , _done(initialize(use_https))
    {
    }

    virtual future<connected_socket> make() override {
        if (!_state->initialized) {
            _logger.debug("Waiting for factory to initialize");
            co_await _done.get_future();
        }

        if (_state->creds) {
            _logger.debug("Making new HTTPS connection addr={} host={}", _state->addr, _host);
            co_return co_await tls::connect(_state->creds, _state->addr, tls::tls_options{.server_name = _host});
        } else {
            _logger.debug("Making new HTTP connection");
            co_return co_await seastar::connect(_state->addr, {}, transport::TCP);
        }
    }
};

client::client(std::string host, endpoint_config_ptr cfg, global_factory gf, private_tag)
        : _host(std::move(host))
        , _cfg(std::move(cfg))
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

future<> client::make_request(http::request req, http::experimental::client::reply_handler handle, http::reply::status_type expected) {
    authorize(req);
    auto sg = current_scheduling_group();
    auto it = _https.find(sg);
    if (it == _https.end()) [[unlikely]] {
        auto factory = std::make_unique<dns_connection_factory>(_host, _cfg->port, _cfg->use_https, s3l);
        // Limit the maximum number of connections this group's http client
        // may have proportional to its shares. Shares are typically in the
        // range of 100...1000, thus resulting in 1..10 connections
        auto max_connections = std::max((unsigned)(sg.get_shares() / 100), 1u);
        it = _https.emplace(std::piecewise_construct,
            std::forward_as_tuple(sg),
            std::forward_as_tuple(std::move(factory), max_connections)
        ).first;
    }
    return it->second.make_request(std::move(req), std::move(handle), expected);
}

future<> client::get_object_header(sstring object_name, http::experimental::client::reply_handler handler) {
    s3l.trace("HEAD {}", object_name);
    auto req = http::request::make("HEAD", _host, object_name);
    return make_request(std::move(req), std::move(handler));
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

static rapidxml::xml_node<>* first_node_of(rapidxml::xml_node<>* root,
                                           std::initializer_list<std::string_view> names) {
    assert(root);
    auto* node = root;
    for (auto name : names) {
        node = node->first_node(name.data(), name.size());
        if (!node) {
            throw std::runtime_error(fmt::format("'{}' is not found", name));
        }
    }
    return node;
}

static tag_set parse_tagging(sstring& body) {
    auto doc = std::make_unique<rapidxml::xml_document<>>();
    try {
        doc->parse<0>(body.data());
    } catch (const rapidxml::parse_error& e) {
        s3l.warn("cannnot parse tagging response: {}", e.what());
        throw std::runtime_error("cannot parse tagging response");
    }
    tag_set tags;
    auto tagset_node = first_node_of(doc.get(), {"Tagging", "TagSet"});
    for (auto tag_node = tagset_node->first_node("Tag"); tag_node; tag_node = tag_node->next_sibling()) {
        auto key = tag_node->first_node("Key")->value();
        auto value = tag_node->first_node("Value")->value();
        tags.emplace_back(tag{key, value});
    }
    return tags;
}

future<tag_set> client::get_object_tagging(sstring object_name) {
    // see https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectTagging.html
    auto req = http::request::make("GET", _host, object_name);
    req.query_parameters["tagging"] = "";
    s3l.trace("GET {} tagging", object_name);
    tag_set tags;
    co_await make_request(std::move(req),
                          [&tags] (const http::reply& reply, input_stream<char>&& in) mutable -> future<> {
        auto& retval = tags;
        auto input = std::move(in);
        auto body = co_await util::read_entire_stream_contiguous(input);
        retval = parse_tagging(body);
    });
    co_return tags;
}

static auto dump_tagging(const tag_set& tags) {
    // print the tags as an XML as defined by the API definition.
    fmt::memory_buffer body;
    fmt::format_to(fmt::appender(body), "<Tagging><TagSet>{}</TagSet></Tagging>", fmt::join(tags, ""));
    return body;
}

future<> client::put_object_tagging(sstring object_name, tag_set tagging) {
    // see https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObjectTagging.html
    auto req = http::request::make("PUT", _host, object_name);
    req.query_parameters["tagging"] = "";
    s3l.trace("PUT {} tagging", object_name);
    auto body = dump_tagging(tagging);
    size_t body_size = body.size();
    req.write_body("xml", body_size, [body=std::move(body)] (output_stream<char>&& out) -> future<> {
        auto output = std::move(out);
        std::exception_ptr ex;
        try {
            co_await output.write(body.data(), body.size());
            co_await output.flush();
        } catch (...) {
            ex = std::current_exception();
        }
        co_await output.close();
        if (ex) {
            co_await coroutine::return_exception_ptr(std::move(ex));
        }
    });
    co_await make_request(std::move(req));
}

future<> client::delete_object_tagging(sstring object_name) {
    // see https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjectTagging.html
    auto req = http::request::make("DELETE", _host, object_name);
    req.query_parameters["tagging"] = "";
    s3l.trace("DELETE {} tagging", object_name);
    co_await make_request(std::move(req), ignore_reply, http::reply::status_type::no_content);
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
    co_await make_request(std::move(req), [&off, &ret, &object_name] (const http::reply& rep, input_stream<char>&& in_) mutable -> future<> {
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
    co_await make_request(std::move(req));
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
    co_await make_request(std::move(req));
}

future<> client::delete_object(sstring object_name) {
    s3l.trace("DELETE {}", object_name);
    auto req = http::request::make("DELETE", _host, object_name);
    co_await make_request(std::move(req), ignore_reply, http::reply::status_type::no_content);
}

class client::upload_sink_base : public data_sink_impl {
protected:
    shared_ptr<client> _client;
    sstring _object_name;
    sstring _upload_id;
    utils::chunked_vector<sstring> _part_etags;
    gate _bg_flushes;

    future<> start_upload();
    future<> finalize_upload();
    future<> upload_part(memory_data_sink_buffers bufs);
    future<> upload_part(std::unique_ptr<upload_sink> source);
    future<> abort_upload();

    bool upload_started() const noexcept {
        return !_upload_id.empty();
    }

public:
    upload_sink_base(shared_ptr<client> cln, sstring object_name)
        : _client(std::move(cln))
        , _object_name(std::move(object_name))
    {
    }

    virtual future<> put(net::packet) override {
        throw_with_backtrace<std::runtime_error>("s3 put(net::packet) unsupported");
    }

    virtual future<> close() override;

    virtual size_t buffer_size() const noexcept override {
        return 128 * 1024;
    }

    unsigned parts_count() const noexcept { return _part_etags.size(); }
};

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

sstring parse_multipart_copy_upload_etag(sstring& body) {
    rapidxml::xml_document<> doc;
    try {
        doc.parse<0>(body.data());
    } catch (const rapidxml::parse_error& e) {
        s3l.warn("cannot parse multipart copy upload response: {}", e.what());
        // The caller is supposed to check the etag to be empty
        // and handle the error the way it prefers
        return "";
    }
    auto root_node = doc.first_node("CopyPartResult");
    auto etag_node = root_node->first_node("ETag");
    return etag_node->value();
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

future<> client::upload_sink_base::start_upload() {
    s3l.trace("POST uploads {}", _object_name);
    auto rep = http::request::make("POST", _client->_host, _object_name);
    rep.query_parameters["uploads"] = "";
    co_await _client->make_request(std::move(rep), [this] (const http::reply& rep, input_stream<char>&& in_) -> future<> {
        auto in = std::move(in_);
        auto body = co_await util::read_entire_stream_contiguous(in);
        _upload_id = parse_multipart_upload_id(body);
        if (_upload_id.empty()) {
            co_await coroutine::return_exception(std::runtime_error("cannot initiate upload"));
        }
        s3l.trace("created uploads for {} -> id = {}", _object_name, _upload_id);
    });
}

future<> client::upload_sink_base::upload_part(memory_data_sink_buffers bufs) {
    if (!upload_started()) {
        co_await start_upload();
    }

    unsigned part_number = _part_etags.size();
    _part_etags.emplace_back();
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
    (void)_client->make_request(std::move(req), [this, part_number] (const http::reply& rep, input_stream<char>&& in_) mutable -> future<> {
        auto etag = rep.get_header("ETag");
        s3l.trace("uploaded {} part data -> etag = {} (upload id {})", part_number, etag, _upload_id);
        _part_etags[part_number] = std::move(etag);
        return make_ready_future<>();
    }).handle_exception([this, part_number] (auto ex) {
        // ... the exact exception only remains in logs
        s3l.warn("couldn't upload part {}: {} (upload id {})", part_number, ex, _upload_id);
    }).finally([gh = std::move(gh)] {});
}

future<> client::upload_sink_base::abort_upload() {
    s3l.trace("DELETE upload {}", _upload_id);
    auto req = http::request::make("DELETE", _client->_host, _object_name);
    req.query_parameters["uploadId"] = std::exchange(_upload_id, ""); // now upload_started() returns false
    co_await _client->make_request(std::move(req), ignore_reply, http::reply::status_type::no_content);
}

future<> client::upload_sink_base::finalize_upload() {
    s3l.trace("wait for {} parts to complete (upload id {})", _part_etags.size(), _upload_id);
    co_await _bg_flushes.close();

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
    co_await _client->make_request(std::move(req));
}

future<> client::upload_sink_base::close() {
    if (upload_started()) {
        s3l.warn("closing incomplete multipart upload -> aborting");
        // If we got here, we need to pick up any background activity as it may
        // still trying to handle successful request and 'this' should remain alive
        //
        // The gate is not closed by finalize_upload() (i.e. -- no double-close),
        // because otherwise the upload_started() would return false
        co_await _bg_flushes.close();
        co_await abort_upload();
    } else {
        s3l.trace("closing multipart upload");
    }
}

class client::upload_sink final : public client::upload_sink_base {
    // "Each part must be at least 5 MB in size, except the last part."
    // https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPart.html
    static constexpr size_t minimum_part_size = 5 << 20;

    memory_data_sink_buffers _bufs;
    future<> maybe_flush() {
        if (_bufs.size() >= minimum_part_size) {
            co_await upload_part(std::move(_bufs));
        }
    }

public:
    upload_sink(shared_ptr<client> cln, sstring object_name)
        : upload_sink_base(std::move(cln), std::move(object_name))
    {}

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
        if (_bufs.size() != 0) {
            co_await upload_part(std::move(_bufs));
        }
        if (upload_started()) {
            co_await finalize_upload();
        }
    }
};

future<> client::upload_sink_base::upload_part(std::unique_ptr<upload_sink> piece_ptr) {
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
        }).handle_exception([this, part_number] (auto ex) {
            // ... the exact exception only remains in logs
            s3l.warn("couldn't copy-upload part {}: {} (upload id {})", part_number, ex, _upload_id);
        });
    }).finally([this, &piece] {
        return _client->delete_object(piece._object_name).handle_exception([&piece] (auto ex) {
            s3l.warn("failed to remove copy-upload piece {}", piece._object_name);
        });
    }).finally([gh = std::move(gh), piece_ptr = std::move(piece_ptr)] {});
}

class client::upload_jumbo_sink final : public upload_sink_base {
    // "Part numbers can be any number from 1 to 10,000, inclusive."
    // https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPart.html
    static constexpr unsigned aws_maximum_parts_in_piece = 10000;

    const unsigned _maximum_parts_in_piece;
    std::unique_ptr<upload_sink> _current;

    future<> maybe_flush() {
        if (_current->parts_count() >= _maximum_parts_in_piece) {
            auto next = std::make_unique<upload_sink>(_client, format("{}_{}", _object_name, parts_count() + 1));
            co_await upload_part(std::exchange(_current, std::move(next)));
            s3l.trace("Initiated {} piece (upload_id {})", parts_count(), _upload_id);
        }
    }

public:
    upload_jumbo_sink(shared_ptr<client> cln, sstring object_name, std::optional<unsigned> max_parts_per_piece)
        : upload_sink_base(std::move(cln), std::move(object_name))
        , _maximum_parts_in_piece(max_parts_per_piece.value_or(aws_maximum_parts_in_piece))
        , _current(std::make_unique<upload_sink>(_client, format("{}_{}", _object_name, parts_count())))
    {}

    virtual future<> put(temporary_buffer<char> buf) override {
        co_await _current->put(std::move(buf));
        co_await maybe_flush();
    }

    virtual future<> put(std::vector<temporary_buffer<char>> data) override {
        co_await _current->put(std::move(data));
        co_await maybe_flush();
    }

    virtual future<> flush() override {
        if (_current) {
            co_await upload_part(std::exchange(_current, nullptr));
        }
        if (upload_started()) {
            co_await finalize_upload();
        }
    }

    virtual future<> close() override {
        if (_current) {
            co_await _current->close();
            _current.reset();
        }
        co_await upload_sink_base::close();
    }
};

data_sink client::make_upload_sink(sstring object_name) {
    return data_sink(std::make_unique<upload_sink>(shared_from_this(), std::move(object_name)));
}

data_sink client::make_upload_jumbo_sink(sstring object_name, std::optional<unsigned> max_parts_per_piece) {
    return data_sink(std::make_unique<upload_jumbo_sink>(shared_from_this(), std::move(object_name), max_parts_per_piece));
}

class client::readable_file : public file_impl {
    shared_ptr<client> _client;
    sstring _object_name;

    [[noreturn]] void unsupported() {
        throw_with_backtrace<std::logic_error>("unsupported operation on s3 readable file");
    }

public:
    readable_file(shared_ptr<client> cln, sstring object_name)
        : _client(std::move(cln))
        , _object_name(std::move(object_name))
    {
    }

    virtual future<size_t> write_dma(uint64_t pos, const void* buffer, size_t len, io_intent*) override { unsupported(); }
    virtual future<size_t> write_dma(uint64_t pos, std::vector<iovec> iov, io_intent*) override { unsupported(); }
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

    virtual future<size_t> read_dma(uint64_t pos, void* buffer, size_t len, io_intent*) override {
        auto buf = co_await _client->get_object_contiguous(_object_name, range{ pos, len });
        std::copy_n(buf.get(), buf.size(), reinterpret_cast<uint8_t*>(buffer));
        co_return buf.size();
    }

    virtual future<size_t> read_dma(uint64_t pos, std::vector<iovec> iov, io_intent*) override {
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

    virtual future<temporary_buffer<uint8_t>> dma_read_bulk(uint64_t offset, size_t range_size, io_intent*) override {
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
    co_await coroutine::parallel_for_each(_https, [] (auto& it) -> future<> {
        co_await it.second.close();
    });
}

} // s3 namespace
