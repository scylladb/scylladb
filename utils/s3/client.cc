/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
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
#include <seastar/core/coroutine.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/metrics.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/units.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/coroutine/exception.hh>
#include <seastar/coroutine/parallel_for_each.hh>
#include <seastar/util/short_streams.hh>
#include <seastar/http/request.hh>
#include <seastar/http/exception.hh>
#include "db/config.hh"
#include "utils/assert.hh"
#include "utils/s3/aws_error.hh"
#include "utils/s3/s3_retry_strategy.hh"
#include "utils/s3/client.hh"
#include "utils/s3/credentials_providers/environment_aws_credentials_provider.hh"
#include "utils/s3/credentials_providers/instance_profile_credentials_provider.hh"
#include "utils/s3/credentials_providers/sts_assume_role_credentials_provider.hh"
#include "helpers/copy_s3_object.hh"
#include "helpers/upload_sinks.hh"
#include "helpers/download_source.hh"
#include "helpers/do_upload_file.hh"
#include "helpers/readable_file.hh"
#include "utils/http.hh"
#include "utils/memory_data_sink.hh"
#include "utils/aws_sigv4.hh"
#include "db_clock.hh"
#include "utils/log.hh"

using namespace std::chrono_literals;

namespace s3 {

logging::logger s3l("s3");

client::client(std::string host, endpoint_config_ptr cfg, semaphore& mem, global_factory gf, private_tag, std::unique_ptr<aws::retry_strategy> rs)
        : _host(std::move(host))
        , _cfg(std::move(cfg))
        , _creds_sem(1)
        , _creds_invalidation_timer([this] {
            std::ignore = [this]() -> future<> {
                auto units = co_await get_units(_creds_sem, 1);
                s3l.info("Credentials update attempt in background failed. Outdated credentials will be discarded, triggering synchronous re-obtainment"
                         " attempts for future requests.");
                _credentials = {};
            }();
        })
        , _creds_update_timer([this] {
            std::ignore = [this]() -> future<> {
                auto units = co_await get_units(_creds_sem, 1);
                s3l.info("Update creds in the background");
                try {
                    co_await update_credentials_and_rearm();
                } catch (...) {
                    _credentials = {};
                }
            }();
        })
        , _gf(std::move(gf))
        , _memory(mem)
        , _retry_strategy(std::move(rs)) {
    _creds_provider_chain
        .add_credentials_provider(std::make_unique<aws::environment_aws_credentials_provider>())
        .add_credentials_provider(std::make_unique<aws::instance_profile_credentials_provider>())
        .add_credentials_provider(std::make_unique<aws::sts_assume_role_credentials_provider>(_cfg->region, _cfg->role_arn));

    _creds_update_timer.arm(lowres_clock::now());
    if (!_retry_strategy) {
        _retry_strategy = std::make_unique<aws::s3_retry_strategy>([this]() -> future<> {
            auto units = co_await get_units(_creds_sem, 1);
            co_await update_credentials_and_rearm();
        });
    }
}

future<> client::update_config(endpoint_config_ptr cfg) {
    if (_cfg->port != cfg->port || _cfg->use_https != cfg->use_https) {
        throw std::runtime_error("Updating port and/or https usage is not possible");
    }
    _cfg = std::move(cfg);
    auto units = co_await get_units(_creds_sem, 1);
    _creds_provider_chain.invalidate_credentials();
    _credentials = {};
    _creds_update_timer.rearm(lowres_clock::now());
}

shared_ptr<client> client::make(std::string endpoint, endpoint_config_ptr cfg, semaphore& mem, global_factory gf) {
    return seastar::make_shared<client>(std::move(endpoint), std::move(cfg), mem, std::move(gf), private_tag{});
}

future<> client::update_credentials_and_rearm() {
    _credentials = co_await _creds_provider_chain.get_aws_credentials();
    _creds_invalidation_timer.rearm(_credentials.expires_at);
    _creds_update_timer.rearm(_credentials.expires_at - 1h);
}

future<> client::authorize(http::request& req) {
    if (!_credentials) [[unlikely]] {
        auto units = co_await get_units(_creds_sem, 1);
        if (!_credentials) {
            s3l.info("Update creds synchronously");
            co_await update_credentials_and_rearm();
        }
    }

    auto time_point_str = utils::aws::format_time_point(db_clock::now());
    auto time_point_st = time_point_str.substr(0, 8);
    req._headers["x-amz-date"] = time_point_str;
    req._headers["x-amz-content-sha256"] = "UNSIGNED-PAYLOAD";
    if (!_credentials.session_token.empty()) {
        req._headers["x-amz-security-token"] = _credentials.session_token;
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
        signed_headers_list += seastar::format("{}{}", h.first, header_nr == 1 ? "" : ";");
        header_nr--;
    }
    sstring query_string = "";
    std::map<std::string_view, std::string_view> query_parameters;
    for (const auto& q : req.query_parameters) {
        query_parameters[q.first] = q.second;
    }
    unsigned query_nr = query_parameters.size();
    for (const auto& q : query_parameters) {
        query_string += seastar::format("{}={}{}", http::internal::url_encode(q.first), http::internal::url_encode(q.second), query_nr == 1 ? "" : "&");
        query_nr--;
    }
    auto sig = utils::aws::get_signature(
        _credentials.access_key_id, _credentials.secret_access_key,
        _host, req._url, req._method,
        utils::aws::omit_datestamp_expiration_check,
        signed_headers_list, signed_headers,
        utils::aws::unsigned_content,
        _cfg->region, "s3", query_string);
    req._headers["Authorization"] = seastar::format("AWS4-HMAC-SHA256 Credential={}/{}/{}/s3/aws4_request,SignedHeaders={},Signature={}", _credentials.access_key_id, time_point_st, _cfg->region, signed_headers_list, sig);
}

future<semaphore_units<>> client::claim_memory(size_t size) {
    return get_units(_memory, size);
}

client::group_client::group_client(std::unique_ptr<http::experimental::connection_factory> f, unsigned max_conn, const aws::retry_strategy& retry_strategy)
    : retryable_client(std::move(f), max_conn, map_s3_client_exception, http::experimental::client::retry_requests::yes, retry_strategy) {
}

void client::group_client::register_metrics(std::string class_name, std::string host) {
    namespace sm = seastar::metrics;
    auto ep_label = sm::label("endpoint")(host);
    auto sg_label = sm::label("class")(class_name);
    metrics.add_group("s3", {
        sm::make_gauge("nr_connections", [this] { return retryable_client.get_http_client().connections_nr(); },
                sm::description("Total number of connections"), {ep_label, sg_label}),
        sm::make_gauge("nr_active_connections", [this] { return retryable_client.get_http_client().connections_nr() - retryable_client.get_http_client().idle_connections_nr(); },
                sm::description("Total number of connections with running requests"), {ep_label, sg_label}),
        sm::make_counter("total_new_connections", [this] { return retryable_client.get_http_client().total_new_connections_nr(); },
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
        unsigned max_connections = _cfg->max_connections.has_value() ? *_cfg->max_connections : std::max((unsigned)(sg.get_shares() / 100), 1u);
        it = _https.emplace(std::piecewise_construct,
            std::forward_as_tuple(sg),
            std::forward_as_tuple(std::move(factory), max_connections, *_retry_strategy)
        ).first;

        it->second.register_metrics(sg.name(), _host);
    }
    return it->second;
}

future<> client::make_request(http::request req, http::experimental::client::reply_handler handle, std::optional<http::reply::status_type> expected, seastar::abort_source* as) {
    co_await authorize(req);
    auto& gc = find_or_create_client();
    co_await gc.retryable_client.make_request(std::move(req), std::move(handle), expected, as);
}

future<> client::make_request(http::request req, reply_handler_ext handle_ex, std::optional<http::reply::status_type> expected, seastar::abort_source* as) {
    co_await authorize(req);
    auto& gc = find_or_create_client();
    auto handle = [&gc, handle = std::move(handle_ex)] (const http::reply& rep, input_stream<char>&& in) {
        return handle(gc, rep, std::move(in));
    };
    co_await gc.retryable_client.make_request(std::move(req), std::move(handle), expected, as);
}

future<> client::get_object_header(sstring object_name, http::experimental::client::reply_handler handler, seastar::abort_source* as) {
    s3l.trace("HEAD {}", object_name);
    auto req = http::request::make("HEAD", _host, object_name);
    return make_request(std::move(req), std::move(handler), http::reply::status_type::ok, as);
}

future<uint64_t> client::get_object_size(sstring object_name, seastar::abort_source* as) {
    uint64_t len = 0;
    co_await get_object_header(std::move(object_name), [&len] (const http::reply& rep, input_stream<char>&& in_) mutable -> future<> {
        len = rep.content_length;
        return make_ready_future<>(); // it's HEAD with no body
    }, as);
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

future<stats> client::get_object_stats(sstring object_name, seastar::abort_source* as) {
    struct stats st{};
    co_await get_object_header(object_name, [&] (const http::reply& rep, input_stream<char>&& in_) mutable -> future<> {
        st.size = rep.content_length;
        st.last_modified = parse_http_last_modified_time(object_name, rep.get_header("Last-Modified"));
        return make_ready_future<>();
    }, as);
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

future<tag_set> client::get_object_tagging(sstring object_name, seastar::abort_source* as) {
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
    }, http::reply::status_type::ok, as);
    co_return tags;
}

static auto dump_tagging(const tag_set& tags) {
    // print the tags as an XML as defined by the API definition.
    fmt::memory_buffer body;
    fmt::format_to(fmt::appender(body), "<Tagging><TagSet>{}</TagSet></Tagging>", fmt::join(tags, ""));
    return body;
}

future<> client::put_object_tagging(sstring object_name, tag_set tagging, seastar::abort_source* as) {
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
    co_await make_request(std::move(req), ignore_reply, http::reply::status_type::ok, as);
}

future<> client::delete_object_tagging(sstring object_name, seastar::abort_source* as) {
    // see https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjectTagging.html
    auto req = http::request::make("DELETE", _host, object_name);
    req.query_parameters["tagging"] = "";
    s3l.trace("DELETE {} tagging", object_name);
    co_await make_request(std::move(req), ignore_reply, http::reply::status_type::no_content, as);
}

future<temporary_buffer<char>> client::get_object_contiguous(sstring object_name, std::optional<range> range, seastar::abort_source* as) {
    auto req = http::request::make("GET", _host, object_name);
    http::reply::status_type expected = http::reply::status_type::ok;
    if (range) {
        if (range->len == 0) {
            co_return temporary_buffer<char>();
        }
        auto range_header = format_range_header(*range);
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
        off = 0;
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
    }, expected, as);
    ret->trim(off);
    s3l.trace("Consumed {} bytes of {}", off, object_name);
    co_return std::move(*ret);
}

future<> client::put_object(sstring object_name, temporary_buffer<char> buf, seastar::abort_source* as) {
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
    }, http::reply::status_type::ok, as);
}

future<> client::put_object(sstring object_name, ::memory_data_sink_buffers bufs, seastar::abort_source* as) {
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
    }, http::reply::status_type::ok, as);
}

future<> client::delete_object(sstring object_name, seastar::abort_source* as) {
    s3l.trace("DELETE {}", object_name);
    auto req = http::request::make("DELETE", _host, object_name);
    co_await make_request(std::move(req), ignore_reply, http::reply::status_type::no_content, as);
}

future<> client::copy_object(sstring source_object, sstring target_object, std::optional<size_t> part_size, std::optional<tag> tag, seastar::abort_source*) {
    if (!part_size)
        co_return co_await copy_s3_object(shared_from_this(), std::move(source_object), std::move(target_object), tag, nullptr).copy();
    co_return co_await copy_s3_object(shared_from_this(), std::move(source_object), std::move(target_object), part_size.value(), tag, nullptr).copy();
}

data_sink client::make_upload_sink(sstring object_name, seastar::abort_source* as) {
    return data_sink(std::make_unique<upload_sink>(shared_from_this(), std::move(object_name), std::nullopt, as));
}

data_sink client::make_upload_jumbo_sink(sstring object_name, std::optional<unsigned> max_parts_per_piece, seastar::abort_source* as) {
    return data_sink(std::make_unique<upload_jumbo_sink>(shared_from_this(), std::move(object_name), max_parts_per_piece, as));
}

data_source client::make_download_source(sstring object_name, std::optional<range> range, seastar::abort_source* as) {
    return data_source(std::make_unique<download_source>(shared_from_this(), std::move(object_name), range, as));
}

future<> client::upload_file(std::filesystem::path path,
                              sstring object_name,
                              upload_progress& up,
                              seastar::abort_source* as) {
    do_upload_file do_upload{shared_from_this(),
                             std::move(path),
                             std::move(object_name),
                             {}, 0, up, as};
    co_await do_upload.upload();
}

future<> client::upload_file(std::filesystem::path path,
                              sstring object_name,
                              std::optional<tag> tag,
                              std::optional<size_t> part_size,
                              seastar::abort_source* as
                              ) {
    upload_progress noop;
    do_upload_file do_upload{shared_from_this(),
                             std::move(path),
                             std::move(object_name),
                             std::move(tag),
                             part_size.value_or(0),
                             noop,
                             as};
    co_await do_upload.upload();
}

file client::make_readable_file(sstring object_name, seastar::abort_source* as) {
    return file(make_shared<readable_file>(shared_from_this(), std::move(object_name), as));
}

future<> client::close() {
    {
        auto units = co_await get_units(_creds_sem, 1);
        _creds_invalidation_timer.cancel();
        _creds_update_timer.cancel();
    }
    co_await coroutine::parallel_for_each(_https, [] (auto& it) -> future<> {
        co_await it.second.retryable_client.close();
    });
}

} // s3 namespace
