/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <fmt/format.h>
#include <exception>
#include <initializer_list>
#include <memory>
#include <stdexcept>
#if __has_include(<rapidxml.h>)
#include <rapidxml.h>
#else
#include <rapidxml/rapidxml.hpp>
#endif
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/classification.hpp>
#include <boost/range/adaptor/map.hpp>
#include <seastar/core/coroutine.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/on_internal_error.hh>
#include <seastar/core/pipe.hh>
#include <seastar/core/units.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/coroutine/exception.hh>
#include <seastar/coroutine/parallel_for_each.hh>
#include <seastar/util/short_streams.hh>
#include <seastar/util/lazy.hh>
#include <seastar/http/request.hh>
#include <seastar/http/exception.hh>
#include "utils/assert.hh"
#include "utils/s3/client.hh"
#include "utils/div_ceil.hh"
#include "utils/http.hh"
#include "utils/memory_data_sink.hh"
#include "utils/chunked_vector.hh"
#include "utils/aws_sigv4.hh"
#include "utils/exceptions.hh"
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
// "Each part must be at least 5 MB in size, except the last part."
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPart.html
static constexpr size_t aws_minimum_part_size = 5_MiB;
// "Part numbers can be any number from 1 to 10,000, inclusive."
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPart.html
static constexpr unsigned aws_maximum_parts_in_piece = 10'000;

future<> ignore_reply(const http::reply& rep, input_stream<char>&& in_) {
    auto in = std::move(in_);
    co_await util::skip_entire_stream(in);
}

client::client(std::string host, endpoint_config_ptr cfg, semaphore& mem, global_factory gf, private_tag)
        : _host(std::move(host))
        , _cfg(std::move(cfg))
        , _gf(std::move(gf))
        , _memory(mem)
{
}

void client::update_config(endpoint_config_ptr cfg) {
    if (_cfg->port != cfg->port || _cfg->use_https != cfg->use_https) {
        throw std::runtime_error("Updating port and/or https usage is not possible");
    }
    _cfg = std::move(cfg);
}

shared_ptr<client> client::make(std::string endpoint, endpoint_config_ptr cfg, semaphore& mem, global_factory gf) {
    return seastar::make_shared<client>(std::move(endpoint), std::move(cfg), mem, std::move(gf), private_tag{});
}

void client::authorize(http::request& req) {
    if (!_cfg->aws) {
        return;
    }

    auto time_point_str = utils::aws::format_time_point(db_clock::now());
    auto time_point_st = time_point_str.substr(0, 8);
    req._headers["x-amz-date"] = time_point_str;
    req._headers["x-amz-content-sha256"] = "UNSIGNED-PAYLOAD";
    if (!_cfg->aws->session_token.empty()) {
        req._headers["x-amz-security-token"] = _cfg->aws->session_token;
    }
    std::map<std::string_view, std::string_view> signed_headers;
    sstring signed_headers_list = "";
    // AWS requires all x-... and Host: headers to be signed
    signed_headers["host"] = req._headers["Host"];
    for (const auto& [name, value] : req._headers) {
        if (name.starts_with("x-")) {
            signed_headers[name] = value;
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
        query_string += format("{}={}{}", http::internal::url_encode(q.first), http::internal::url_encode(q.second), query_nr == 1 ? "" : "&");
        query_nr--;
    }
    auto sig = utils::aws::get_signature(
        _cfg->aws->access_key_id, _cfg->aws->secret_access_key,
        _host, req._url, req._method,
        utils::aws::omit_datestamp_expiration_check,
        signed_headers_list, signed_headers,
        utils::aws::unsigned_content,
        _cfg->aws->region, "s3", query_string);
    req._headers["Authorization"] = format("AWS4-HMAC-SHA256 Credential={}/{}/{}/s3/aws4_request,SignedHeaders={},Signature={}", _cfg->aws->access_key_id, time_point_st, _cfg->aws->region, signed_headers_list, sig);
}

future<semaphore_units<>> client::claim_memory(size_t size) {
    return get_units(_memory, size);
}

client::group_client::group_client(std::unique_ptr<http::experimental::connection_factory> f, unsigned max_conn)
        : http(std::move(f), max_conn, http::experimental::client::retry_requests::yes)
{
}

void client::group_client::register_metrics(std::string class_name, std::string host) {
    namespace sm = seastar::metrics;
    auto ep_label = sm::label("endpoint")(host);
    auto sg_label = sm::label("class")(class_name);
    metrics.add_group("s3", {
        sm::make_gauge("nr_connections", [this] { return http.connections_nr(); },
                sm::description("Total number of connections"), {ep_label, sg_label}),
        sm::make_gauge("nr_active_connections", [this] { return http.connections_nr() - http.idle_connections_nr(); },
                sm::description("Total number of connections with running requests"), {ep_label, sg_label}),
        sm::make_counter("total_new_connections", [this] { return http.total_new_connections_nr(); },
                sm::description("Total number of new connections created so far"), {ep_label, sg_label}),
        sm::make_counter("total_read_requests", [this] { return read_stats.ops; },
                sm::description("Total number of object read requests"), {ep_label, sg_label}),
        sm::make_counter("total_write_requests", [this] { return write_stats.ops; },
                sm::description("Total number of object write requests"), {ep_label, sg_label}),
        sm::make_counter("total_read_bytes", [this] { return read_stats.bytes; },
                sm::description("Total number of bytes read from objects"), {ep_label, sg_label}),
        sm::make_counter("total_write_bytes", [this] { return write_stats.bytes; },
                sm::description("Total number of bytes written to objects"), {ep_label, sg_label}),
        sm::make_counter("total_read_latency_sec", [this] { return read_stats.duration.count(); },
                sm::description("Total time spent reading data from objects"), {ep_label, sg_label}),
        sm::make_counter("total_write_latency_sec", [this] { return write_stats.duration.count(); },
                sm::description("Total time spend writing data to objects"), {ep_label, sg_label}),
    });
}

client::group_client& client::find_or_create_client() {
    auto sg = current_scheduling_group();
    auto it = _https.find(sg);
    if (it == _https.end()) [[unlikely]] {
        auto factory = std::make_unique<utils::http::dns_connection_factory>(_host, _cfg->port, _cfg->use_https, s3l);
        // Limit the maximum number of connections this group's http client
        // may have proportional to its shares. Shares are typically in the
        // range of 100...1000, thus resulting in 1..10 connections
        auto max_connections = std::max((unsigned)(sg.get_shares() / 100), 1u);
        it = _https.emplace(std::piecewise_construct,
            std::forward_as_tuple(sg),
            std::forward_as_tuple(std::move(factory), max_connections)
        ).first;

        it->second.register_metrics(sg.name(), _host);
    }
    return it->second;
}

inline bool is_redirect_status(http::reply::status_type st) {
    auto st_i = static_cast<int>(st);
    return st_i >= 300 && st_i < 400;
}

future<> map_s3_client_exception(std::exception_ptr ex) {
    seastar::memory::scoped_critical_alloc_section alloc;

    try {
        std::rethrow_exception(std::move(ex));
    } catch (const httpd::unexpected_status_error& e) {
        auto status = e.status();

        if (is_redirect_status(status) || status == http::reply::status_type::not_found) {
            return make_exception_future<>(storage_io_error(ENOENT, format("S3 object doesn't exist ({})", status)));
        }
        if (status == http::reply::status_type::forbidden || status == http::reply::status_type::unauthorized) {
            return make_exception_future<>(storage_io_error(EACCES, format("S3 access denied ({})", status)));
        }

        return make_exception_future<>(storage_io_error(EIO, format("S3 request failed with ({})", status)));
    } catch (...) {
        auto e = std::current_exception();
        return make_exception_future<>(storage_io_error(EIO, format("S3 error ({})", e)));
    }

}

future<> client::make_request(http::request req, http::experimental::client::reply_handler handle, http::reply::status_type expected) {
    authorize(req);
    auto& gc = find_or_create_client();
    return gc.http.make_request(std::move(req), std::move(handle), expected).handle_exception([] (auto ex) {
        return map_s3_client_exception(std::move(ex));
    });
}

future<> client::make_request(http::request req, reply_handler_ext handle_ex, http::reply::status_type expected) {
    authorize(req);
    auto& gc = find_or_create_client();
    auto handle = [&gc, handle = std::move(handle_ex)] (const http::reply& rep, input_stream<char>&& in) {
        return handle(gc, rep, std::move(in));
    };
    return gc.http.make_request(std::move(req), std::move(handle), expected).handle_exception([] (auto ex) {
        return map_s3_client_exception(std::move(ex));
    });
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

future<stats> client::get_object_stats(sstring object_name) {
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
    SCYLLA_ASSERT(root);
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
        s3l.warn("cannot parse tagging response: {}", e.what());
        throw std::runtime_error("cannot parse tagging response");
    }
    tag_set tags;
    auto tagset_node = first_node_of(doc.get(), {"Tagging", "TagSet"});
    for (auto tag_node = tagset_node->first_node("Tag"); tag_node; tag_node = tag_node->next_sibling()) {
        // See https://docs.aws.amazon.com/AmazonS3/latest/API/API_Tag.html,
        // both "Key" and "Value" are required, but we still need to check them.
        auto key = tag_node->first_node("Key");
        if (!key) {
            throw std::runtime_error("'Key' missing in 'Tag'");
        }
        auto value = tag_node->first_node("Value");
        if (!value) {
            throw std::runtime_error("'Value' missing in 'Tag'");
        }
        tags.emplace_back(tag{key->value(), value->value()});
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
        if (range->len == 0) {
            co_return temporary_buffer<char>();
        }
        auto end_bytes = range->off + range->len - 1;
        if (end_bytes < range->off) {
            throw std::overflow_error("End of the range exceeds 64-bits");
        }
        auto range_header = format("bytes={}-{}", range->off, end_bytes);
        s3l.trace("GET {} contiguous range='{}'", object_name, range_header);
        req._headers["Range"] = std::move(range_header);
        expected = http::reply::status_type::partial_content;
    } else {
        s3l.trace("GET {} contiguous", object_name);
    }

    size_t off = 0;
    std::optional<temporary_buffer<char>> ret;
    co_await make_request(std::move(req), [&off, &ret, &object_name, start = s3_clock::now()] (group_client& gc, const http::reply& rep, input_stream<char>&& in_) mutable -> future<> {
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
        }).then([&gc, &off, start] {
            gc.read_stats.update(off, s3_clock::now() - start);
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
    req.write_body("bin", len, [buf = std::move(buf)] (output_stream<char>&& out_) -> future<> {
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
    co_await make_request(std::move(req), [len, start = s3_clock::now()] (group_client& gc, const auto& rep, auto&& in) {
        gc.write_stats.update(len, s3_clock::now() - start);
        return ignore_reply(rep, std::move(in));
    });
}

future<> client::put_object(sstring object_name, ::memory_data_sink_buffers bufs) {
    s3l.trace("PUT {} (buffers)", object_name);
    auto req = http::request::make("PUT", _host, object_name);
    auto len = bufs.size();
    req.write_body("bin", len, [bufs = std::move(bufs)] (output_stream<char>&& out_) -> future<> {
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
    co_await make_request(std::move(req), [len, start = s3_clock::now()] (group_client& gc, const auto& rep, auto&& in) {
        gc.write_stats.update(len, s3_clock::now() - start);
        return ignore_reply(rep, std::move(in));
    });
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
    std::optional<tag> _tag;

    future<> start_upload();
    future<> finalize_upload();
    future<> upload_part(memory_data_sink_buffers bufs);
    future<> upload_part(std::unique_ptr<upload_sink> source);
    future<> abort_upload();

    bool upload_started() const noexcept {
        return !_upload_id.empty();
    }

public:
    upload_sink_base(shared_ptr<client> cln, sstring object_name, std::optional<tag> tag)
        : _client(std::move(cln))
        , _object_name(std::move(object_name))
        , _tag(std::move(tag))
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
    auto doc = std::make_unique<rapidxml::xml_document<>>();
    try {
        doc->parse<0>(body.data());
    } catch (const rapidxml::parse_error& e) {
        s3l.warn("cannot parse multipart copy upload response: {}", e.what());
        // The caller is supposed to check the etag to be empty
        // and handle the error the way it prefers
        return "";
    }
    auto root_node = doc->first_node("CopyPartResult");
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
            SCYLLA_ASSERT(!etag.empty());
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
    s3l.trace("POST uploads {} (tag {})", _object_name, seastar::value_of([this] { return _tag ? _tag->key + "=" + _tag->value : "none"; }));
    auto rep = http::request::make("POST", _client->_host, _object_name);
    rep.query_parameters["uploads"] = "";
    if (_tag) {
        rep._headers["x-amz-tagging"] = format("{}={}", _tag->key, _tag->value);
    }
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

    auto claim = co_await _client->claim_memory(bufs.size());

    unsigned part_number = _part_etags.size();
    _part_etags.emplace_back();
    s3l.trace("PUT part {} {} bytes in {} buffers (upload id {})", part_number, bufs.size(), bufs.buffers().size(), _upload_id);
    auto req = http::request::make("PUT", _client->_host, _object_name);
    auto size = bufs.size();
    req._headers["Content-Length"] = format("{}", size);
    req.query_parameters["partNumber"] = format("{}", part_number + 1);
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
    req.query_parameters["uploadId"] = _upload_id;
    req.write_body("xml", parts_xml_len, [this] (output_stream<char>&& out) -> future<> {
        return dump_multipart_upload_parts(std::move(out), _part_etags);
    });
    // If this request fails, finalize_upload() throws, the upload should then
    // be aborted in .close() method
    co_await _client->make_request(std::move(req));
    _upload_id = ""; // now upload_started() returns false
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

class client::upload_sink final : public client::upload_sink_base {
    memory_data_sink_buffers _bufs;
    future<> maybe_flush() {
        if (_bufs.size() >= aws_minimum_part_size) {
            co_await upload_part(std::move(_bufs));
        }
    }

public:
    upload_sink(shared_ptr<client> cln, sstring object_name, std::optional<tag> tag = {})
        : upload_sink_base(std::move(cln), std::move(object_name), std::move(tag))
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
            // This is handy for small objects that are uploaded via the sink. It makes
            // upload happen in one REST call, instead of three (create + PUT + wrap-up)
            if (!upload_started()) {
                s3l.trace("Sink fallback to plain PUT for {}", _object_name);
                co_return co_await _client->put_object(_object_name, std::move(_bufs));
            }

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
    }).then_wrapped([this, &piece] (auto f) {
        if (f.failed()) {
            s3l.warn("couldn't flush piece {}: {} (upload id {})", piece._object_name, f.get_exception(), _upload_id);
        }
        return _client->delete_object(piece._object_name).handle_exception([&piece] (auto ex) {
            s3l.warn("failed to remove copy-upload piece {}", piece._object_name);
        });
    }).finally([gh = std::move(gh), piece_ptr = std::move(piece_ptr)] {});
}

class client::upload_jumbo_sink final : public upload_sink_base {
    static constexpr tag piece_tag = { .key = "kind", .value = "piece" };

    const unsigned _maximum_parts_in_piece;
    std::unique_ptr<upload_sink> _current;

    future<> maybe_flush() {
        if (_current->parts_count() >= _maximum_parts_in_piece) {
            auto next = std::make_unique<upload_sink>(_client, format("{}_{}", _object_name, parts_count() + 1), piece_tag);
            co_await upload_part(std::exchange(_current, std::move(next)));
            s3l.trace("Initiated {} piece (upload_id {})", parts_count(), _upload_id);
        }
    }

public:
    upload_jumbo_sink(shared_ptr<client> cln, sstring object_name, std::optional<unsigned> max_parts_per_piece)
        : upload_sink_base(std::move(cln), std::move(object_name), std::nullopt)
        , _maximum_parts_in_piece(max_parts_per_piece.value_or(aws_maximum_parts_in_piece))
        , _current(std::make_unique<upload_sink>(_client, format("{}_{}", _object_name, parts_count()), piece_tag))
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

// unlike upload_sink and upload_jumbo_sink, do_upload_file reads from the
// specified file, and sends the data read from disk right away to the wire,
// without accumulating them first.
class client::do_upload_file {
    shared_ptr<client> _client;
    const std::filesystem::path _path;
    sstring _object_name;
    sstring _upload_id;
    utils::chunked_vector<sstring> _part_etags;
    std::optional<tag> _tag;
    size_t _part_size;
    gate _bg_uploads;

    // each time, we read up to transmit size from disk.
    // this is also an option which limits the number of multipart upload tasks.
    //
    // connected_socket::output() uses 8 KiB for its buffer_size, and
    // file_input_stream_options.buffer_size is also 8 KiB, taking the
    // read-ahead into consideration, for maximizing the throughput,
    // we use 64K buffer size.
    static constexpr size_t _transmit_size = 64_KiB;

    static file_input_stream_options input_stream_options() {
        // optimized for throughput
        return {
            .buffer_size = 128_KiB,
            .read_ahead = 4,
        };
    }

    future<> create_multipart_upload() {
        // https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateMultipartUpload.html
        auto req = http::request::make("POST", _client->_host, _object_name);
        req.query_parameters["uploads"] = "";
        if (_tag) {
            req._headers["x-amz-tagging"] = seastar::format("{}={}", _tag->key, _tag->value);
        }
        co_await _client->make_request(std::move(req), [this] (const http::reply& rep, input_stream<char>&& in_) -> future<> {
            auto in = std::move(in_);
            auto body = co_await util::read_entire_stream_contiguous(in);
            _upload_id = parse_multipart_upload_id(body);
            if (_upload_id.empty()) {
                co_await coroutine::return_exception(std::runtime_error("cannot initiate upload"));
            }
            s3l.trace("created multipart upload for {} -> id = {}", _object_name, _upload_id);
        });
    }

    future<> complete_multipart_upload() {
        // https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html
        unsigned parts_xml_len = prepare_multipart_upload_parts(_part_etags);
        auto req = http::request::make("POST", _client->_host, _object_name);
        req.query_parameters["uploadId"] = _upload_id;
        req.write_body("xml", parts_xml_len, [this] (output_stream<char>&& out) -> future<> {
            return dump_multipart_upload_parts(std::move(out), _part_etags);
        });
        // If this request fails, complete_multipart_upload() throws, the upload should then
        // be aborted in .close() method
        co_await _client->make_request(std::move(req));
        _upload_id = ""; // now upload_started() returns false
    }

    future<> abort_multipart_upload() {
        // https://docs.aws.amazon.com/AmazonS3/latest/API/API_AbortMultipartUpload.html
        s3l.trace("abort multipart upload {}", _upload_id);
        auto req = http::request::make("DELETE", _client->_host, _object_name);
        req.query_parameters["uploadId"] = std::exchange(_upload_id, "");
        co_await _client->make_request(std::move(req), ignore_reply, http::reply::status_type::no_content);
    }

    // transmit data from input to output in chunks sized up to unit_size
    static future<> copy_to(input_stream<char> input,
                            output_stream<char> output,
                            size_t unit_size) {
        std::exception_ptr ex;
        try {
            for (;;) {
                auto buf = co_await input.read_up_to(unit_size);
                if (buf.empty()) {
                    break;
                }
                co_await output.write(buf.get(), buf.size());
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

    future<> upload_part(file f, uint64_t offset, uint64_t part_size) {
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
        req.write_body("bin", part_size, [f=std::move(f), mem_units=std::move(mem_units), offset, part_size] (output_stream<char>&& out_) mutable {
            auto input = make_file_input_stream(std::move(f), offset, part_size, input_stream_options());
            auto output = std::move(out_);
            return copy_to(std::move(input), std::move(output), _transmit_size);
        });
        // upload the parts in the background for better throughput
        auto gh = _bg_uploads.hold();
        std::ignore = _client->make_request(std::move(req), [this, part_size, part_number, start = s3_clock::now()] (group_client& gc, const http::reply& reply, input_stream<char>&& in_) mutable -> future<> {
            auto etag = reply.get_header("ETag");
            s3l.trace("uploaded {} part data -> etag = {} (upload id {})", part_number, etag, _upload_id);
            _part_etags[part_number] = std::move(etag);
            gc.write_stats.update(part_size, s3_clock::now() - start);
            return make_ready_future();
        }).handle_exception([this, part_number] (auto ex) {
            s3l.warn("couldn't upload part {}: {} (upload id {})", part_number, ex, _upload_id);
        }).finally([gh = std::move(gh)] {});
    }

    // returns pair<num_of_parts, part_size>
    static std::pair<unsigned, size_t> calc_part_size(size_t total_size, size_t part_size) {
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
        part_size = std::max(part_size, aws_minimum_part_size);
        return {div_ceil(total_size, part_size), part_size};
    }

    future<> multi_part_upload(file&& f, uint64_t total_size, size_t part_size) {
        co_await create_multipart_upload();

        for (size_t offset = 0; offset < total_size; offset += part_size) {
            part_size = std::min(total_size - offset, part_size);
            s3l.trace("upload_part: {}~{}/{}", offset, part_size, total_size);
            co_await upload_part(file{f}, offset, part_size);
        }

        co_await _bg_uploads.close();
        std::exception_ptr ex;
        try {
            co_await complete_multipart_upload();
        } catch (...) {
            ex = std::current_exception();
        }
        if (ex) {
            co_await abort_multipart_upload();
            std::rethrow_exception(ex);
        }
    }

    future<> put_object(file&& f, uint64_t len) {
        // https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html
        s3l.trace("PUT {} ({})", _object_name, _path.native());
        auto mem_units = co_await _client->claim_memory(_transmit_size);

        auto req = http::request::make("PUT", _client->_host, _object_name);
        if (_tag) {
            req._headers["x-amz-tagging"] = seastar::format("{}={}", _tag->key, _tag->value);
        }
        req.write_body("bin", len, [f = std::move(f)] (output_stream<char>&& out_) mutable {
            auto input = make_file_input_stream(std::move(f), input_stream_options());
            auto output = std::move(out_);
            return copy_to(std::move(input), std::move(output), _transmit_size);
        });
        co_await _client->make_request(std::move(req), [len, start = s3_clock::now()] (group_client& gc, const auto& rep, auto&& in) {
            gc.write_stats.update(len, s3_clock::now() - start);
            return ignore_reply(rep, std::move(in));
        });
    }

public:
    do_upload_file(shared_ptr<client> cln,
                   std::filesystem::path path,
                   sstring object_name,
                   std::optional<tag> tag,
                   size_t part_size)
        : _client{std::move(cln)}
        , _path{std::move(path)}
        , _object_name{std::move(object_name)}
        , _tag{std::move(tag)}
        , _part_size{part_size} {
    }

    future<> upload() {
        auto f = co_await open_file_dma(_path.native(), open_flags::ro);
        const auto stat = co_await f.stat();
        const uint64_t file_size = stat.st_size;
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
};

future<> client::upload_file(std::filesystem::path path,
                              sstring object_name,
                              std::optional<tag> tag,
                              std::optional<size_t> part_size) {
    do_upload_file do_upload{shared_from_this(),
                             std::move(path),
                             std::move(object_name),
                             std::move(tag),
                             part_size.value_or(0)};
    co_await do_upload.upload();
}

class client::readable_file : public file_impl {
    shared_ptr<client> _client;
    sstring _object_name;
    std::optional<stats> _stats;

    [[noreturn]] void unsupported() {
        throw_with_backtrace<std::logic_error>("unsupported operation on s3 readable file");
    }

    future<> maybe_update_stats() {
        if (_stats) {
            return make_ready_future<>();
        }

        return _client->get_object_stats(_object_name).then([this] (auto st) {
            _stats = std::move(st);
            return make_ready_future<>();
        });
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
        co_await maybe_update_stats();
        struct stat ret {};
        ret.st_nlink = 1;
        ret.st_mode = S_IFREG | S_IRUSR | S_IRGRP | S_IROTH;
        ret.st_size = _stats->size;
        ret.st_blksize = 1 << 10; // huh?
        ret.st_blocks = _stats->size >> 9;
        // objects are immutable on S3, therefore we can use Last-Modified to set both st_mtime and st_ctime
        ret.st_mtime = _stats->last_modified;
        ret.st_ctime = _stats->last_modified;
        co_return ret;
    }

    virtual future<size_t> read_dma(uint64_t pos, void* buffer, size_t len, io_intent*) override {
        co_await maybe_update_stats();
        if (pos >= _stats->size) {
            co_return 0;
        }

        auto buf = co_await _client->get_object_contiguous(_object_name, range{ pos, len });
        std::copy_n(buf.get(), buf.size(), reinterpret_cast<uint8_t*>(buffer));
        co_return buf.size();
    }

    virtual future<size_t> read_dma(uint64_t pos, std::vector<iovec> iov, io_intent*) override {
        co_await maybe_update_stats();
        if (pos >= _stats->size) {
            co_return 0;
        }

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
        co_await maybe_update_stats();
        if (offset >= _stats->size) {
            co_return temporary_buffer<uint8_t>();
        }

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
        co_await it.second.http.close();
    });
}

client::bucket_lister::bucket_lister(shared_ptr<client> client, sstring bucket, sstring prefix, size_t objects_per_page, size_t entries_batch)
    : bucket_lister(std::move(client), std::move(bucket), std::move(prefix),
            [] (const fs::path& parent_dir, const directory_entry& entry) { return true; },
            objects_per_page, entries_batch)
{}

client::bucket_lister::bucket_lister(shared_ptr<client> client, sstring bucket, sstring prefix, lister::filter_type filter, size_t objects_per_page, size_t entries_batch)
    : _client(std::move(client))
    , _bucket(std::move(bucket))
    , _prefix(std::move(prefix))
    , _max_keys(format("{}", objects_per_page))
    , _filter(std::move(filter))
    , _queue(entries_batch)
{
}

static std::pair<std::vector<sstring>, sstring> parse_list_of_objects(sstring body) {
    auto doc = std::make_unique<rapidxml::xml_document<>>();
    try {
        doc->parse<0>(body.data());
    } catch (const rapidxml::parse_error& e) {
        s3l.warn("cannot parse list-objects-v2 response: {}", e.what());
        throw std::runtime_error("cannot parse objects list response");
    }

    std::vector<sstring> names;
    auto root_node = doc->first_node("ListBucketResult");
    for (auto contents = root_node->first_node("Contents"); contents; contents = contents->next_sibling()) {
        auto key = contents->first_node("Key");
        names.push_back(key->value());
    }

    sstring continuation_token;
    auto is_truncated = root_node->first_node("IsTruncated");
    if (is_truncated && std::string_view(is_truncated->value()) == "true") {
        auto continuation = root_node->first_node("NextContinuationToken");
        if (!continuation) {
            throw std::runtime_error("no continuation token in truncated list of objects");
        }
        continuation_token = continuation->value();
    }

    return {std::move(names), std::move(continuation_token)};
}

future<> client::bucket_lister::start_listing() {
    // This is the implementation of paged ListObjectsV2 API call
    // https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html
    sstring continuation_token;
    do {
        s3l.trace("GET /?list-type=2 (prefix={})", _prefix);
        auto req = http::request::make("GET", _client->_host, format("/{}", _bucket));
        req.query_parameters.emplace("list-type", "2");
        req.query_parameters.emplace("max-keys", _max_keys);
        if (!continuation_token.empty()) {
            req.query_parameters.emplace("continuation-token", std::exchange(continuation_token, ""));
        }
        if (!_prefix.empty()) {
            req.query_parameters.emplace("prefix", _prefix);
        }

        std::vector<sstring> names;
        try {
            co_await _client->make_request(std::move(req),
                [&names, &continuation_token] (const http::reply& reply, input_stream<char>&& in) mutable -> future<> {
                    auto input = std::move(in);
                    auto body = co_await util::read_entire_stream_contiguous(input);
                    auto list = parse_list_of_objects(std::move(body));
                    names = std::move(list.first);
                    continuation_token = std::move(list.second);
                }
            );
        } catch (...) {
            _queue.abort(std::current_exception());
            co_return;
        }

        fs::path dir(_prefix);
        for (auto&& o : names) {
            directory_entry ent{o.substr(_prefix.size())};
            if (!_filter(dir, ent)) {
                continue;
            }
            co_await _queue.push_eventually(std::move(ent));
        }
    } while (!continuation_token.empty());
    co_await _queue.push_eventually(std::nullopt);
}

future<std::optional<directory_entry>> client::bucket_lister::get() {
    if (!_opt_done_fut) {
        _opt_done_fut = start_listing();
    }

    std::exception_ptr ex;
    try {
        auto ret = co_await _queue.pop_eventually();
        if (ret) {
            co_return ret;
        }
    } catch (...) {
        ex = std::current_exception();
    }
    co_await close();
    if (ex) {
        co_return coroutine::exception(std::move(ex));
    }
    co_return std::nullopt;
}

future<> client::bucket_lister::close() noexcept {
    if (_opt_done_fut) {
        _queue.abort(std::make_exception_ptr(broken_pipe_exception()));
        try {
            co_await std::exchange(_opt_done_fut, std::make_optional<future<>>(make_ready_future<>())).value();
        } catch (...) {
            // ignore all errors
        }
    }
}

} // s3 namespace
