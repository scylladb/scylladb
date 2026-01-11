/*
 * Copyright 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "alternator/server.hh"
#include "gms/application_state.hh"
#include "utils/log.hh"
#include <fmt/ranges.h>
#include <seastar/http/function_handlers.hh>
#include <seastar/http/short_streams.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include <seastar/util/defer.hh>
#include <seastar/util/short_streams.hh>
#include "seastarx.hh"
#include "error.hh"
#include "service/client_state.hh"
#include "service/qos/service_level_controller.hh"
#include "utils/assert.hh"
#include "timeout_config.hh"
#include "utils/rjson.hh"
#include "auth.hh"
#include <cctype>
#include <string_view>
#include <utility>
#include "service/storage_proxy.hh"
#include "gms/gossiper.hh"
#include "utils/overloaded_functor.hh"
#include "utils/aws_sigv4.hh"
#include "client_data.hh"
#include "utils/updateable_value.hh"
#include <zlib.h>
#include "alternator/http_compression.hh"

static logging::logger slogger("alternator-server");

using namespace httpd;
using request = http::request;
using reply = http::reply;

namespace alternator {

inline std::vector<std::string_view> split(std::string_view text, char separator) {
    std::vector<std::string_view> tokens;
    if (text == "") {
        return tokens;
    }

    while (true) {
        auto pos = text.find_first_of(separator);
        if (pos != std::string_view::npos) {
            tokens.emplace_back(text.data(), pos);
            text.remove_prefix(pos + 1);
        } else {
            tokens.emplace_back(text);
            break;
        }
    }
    return tokens;
}

// Handle CORS (Cross-origin resource sharing) in the HTTP request:
// If the request has the "Origin" header specifying where the script which
// makes this request comes from, we need to reply with the header
// "Access-Control-Allow-Origin: *" saying that this (and any) origin is fine.
// Additionally, if preflight==true (i.e., this is an OPTIONS request),
// the script can also "request" in headers that the server allows it to use
// some HTTP methods and headers in the followup request, and the server
// should respond by "allowing" them in the response headers.
// We also add the header "Access-Control-Expose-Headers" to let the script
// access additional headers in the response.
// This handle_CORS() should be used when handling any HTTP method - both the
// usual GET and POST, and also the "preflight" OPTIONS method.
static void handle_CORS(const request& req, reply& rep, bool preflight) {
    if (!req.get_header("origin").empty()) {
        rep.add_header("Access-Control-Allow-Origin", "*");
        // This is the list that DynamoDB returns for expose headers. I am
        // not sure why not just return "*" here, what's the risk?
        rep.add_header("Access-Control-Expose-Headers", "x-amzn-RequestId,x-amzn-ErrorType,x-amzn-ErrorMessage,Date");
        if (preflight) {
            sstring s = req.get_header("Access-Control-Request-Headers");
            if (!s.empty()) {
                rep.add_header("Access-Control-Allow-Headers", std::move(s));
            }
            s = req.get_header("Access-Control-Request-Method");
            if (!s.empty()) {
                rep.add_header("Access-Control-Allow-Methods", std::move(s));
            }
            // Our CORS response never change anyway, let the browser cache it
            // for two hours (Chrome's maximum):
            rep.add_header("Access-Control-Max-Age", "7200");
        }
    }
}

// DynamoDB HTTP error responses are structured as follows
// https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Programming.Errors.html
// Our handlers throw an exception to report an error. If the exception
// is of type alternator::api_error, it unwrapped and properly reported to
// the user directly. Other exceptions are unexpected, and reported as
// Internal Server Error.
class api_handler : public handler_base {
    // Although the the DynamoDB API responses are JSON, additional
    // conventions apply to these responses. For this reason, DynamoDB uses
    // the content type "application/x-amz-json-1.0" instead of the standard
    // "application/json". Some other AWS services use later versions instead
    // of "1.0", but DynamoDB currently uses "1.0". Note that this content
    // type applies to all replies, both success and error.
    static constexpr const char* REPLY_CONTENT_TYPE = "application/x-amz-json-1.0";
public:
    api_handler(const std::function<future<executor::request_return_type>(std::unique_ptr<request> req)>& _handle,
                const db::config& config) : _response_compressor(config), _f_handle(
         [this, _handle](std::unique_ptr<request> req, std::unique_ptr<reply> rep) {
         sstring accept_encoding = _response_compressor.get_accepted_encoding(*req);
         return seastar::futurize_invoke(_handle, std::move(req)).then_wrapped(
            [this, rep = std::move(rep), accept_encoding=std::move(accept_encoding)](future<executor::request_return_type> resf) mutable {
             if (resf.failed()) {
                 // Exceptions of type api_error are wrapped as JSON and
                 // returned to the client as expected. Other types of
                 // exceptions are unexpected, and returned to the user
                 // as an internal server error:
                 try {
                     resf.get();
                 } catch (api_error &ae) {
                     generate_error_reply(*rep, ae);
                 } catch (rjson::error & re) {
                     generate_error_reply(*rep,
                             api_error::validation(re.what()));
                 } catch (...) {
                     generate_error_reply(*rep,
                             api_error::internal(format("Internal server error: {}", std::current_exception())));
                 }
                 return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
             }
             auto res = resf.get();
             return std::visit(overloaded_functor {
                [&] (std::string&& str) {
                    return _response_compressor.generate_reply(std::move(rep), std::move(accept_encoding),
                                                               REPLY_CONTENT_TYPE, std::move(str));
                },
                [&] (executor::body_writer&& body_writer) {
                    return _response_compressor.generate_reply(std::move(rep), std::move(accept_encoding),
                                                               REPLY_CONTENT_TYPE, std::move(body_writer));
                },
                [&] (const api_error& err) {
                    generate_error_reply(*rep, err);
                    return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
                }
             }, std::move(res));
         });
    }) { }

    api_handler(const api_handler&) = default;
    future<std::unique_ptr<reply>> handle(const sstring& path,
            std::unique_ptr<request> req, std::unique_ptr<reply> rep) override {
        handle_CORS(*req, *rep, false);
        return _f_handle(std::move(req), std::move(rep)).then(
                [](std::unique_ptr<reply> rep) {
                    rep->done();
                    return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
                });
    }

protected:
    void generate_error_reply(reply& rep, const api_error& err) {
        rjson::value results = rjson::empty_object();
        if (!err._extra_fields.IsNull() && err._extra_fields.IsObject()) {
            results = rjson::copy(err._extra_fields);
        }
        rjson::add(results, "__type", rjson::from_string("com.amazonaws.dynamodb.v20120810#" + err._type));
        rjson::add(results, "message", err._msg);
        rep._content = rjson::print(std::move(results));
        rep._status = err._http_code;
        rep.set_content_type(REPLY_CONTENT_TYPE);
        slogger.trace("api_handler error case: {}", rep._content);
    }

    response_compressor _response_compressor;
    future_handler_function _f_handle;
};

class gated_handler : public handler_base {
    seastar::gate& _gate;
public:
    gated_handler(seastar::gate& gate) : _gate(gate) {}
    virtual future<std::unique_ptr<reply>> do_handle(const sstring& path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) = 0;
    virtual future<std::unique_ptr<reply>> handle(const sstring& path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) final override {
        return with_gate(_gate, [this, &path, req = std::move(req), rep = std::move(rep)] () mutable {
            return do_handle(path, std::move(req), std::move(rep));
        });
    }
};

class health_handler : public gated_handler {
public:
    health_handler(seastar::gate& pending_requests) : gated_handler(pending_requests) {}
protected:
    virtual future<std::unique_ptr<reply>> do_handle(const sstring& path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) override {
        handle_CORS(*req, *rep, false);
        rep->set_status(reply::status_type::ok);
        rep->write_body("txt", format("healthy: {}", req->get_header("Host")));
        return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
    }
};

class local_nodelist_handler : public gated_handler {
    service::storage_proxy& _proxy;
    gms::gossiper& _gossiper;
public:
    local_nodelist_handler(seastar::gate& pending_requests, service::storage_proxy& proxy, gms::gossiper& gossiper)
        : gated_handler(pending_requests)
        , _proxy(proxy)
        , _gossiper(gossiper) {}
protected:
    virtual future<std::unique_ptr<reply>> do_handle(const sstring& path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) override {
        rjson::value results = rjson::empty_array();
        // It's very easy to get a list of all live nodes on the cluster,
        // using _gossiper().get_live_members(). But getting
        // just the list of live nodes in this DC needs more elaborate code:
        auto& topology = _proxy.get_token_metadata_ptr()->get_topology();
        // /localnodes lists nodes in a single DC. By default the DC of this
        // server is used, but it can be overridden by a "dc" query option.
        // If the DC does not exist, we return an empty list - not an error.
        sstring query_dc = req->get_query_param("dc");
        sstring local_dc = query_dc.empty() ? topology.get_datacenter() : query_dc;
        std::unordered_set<locator::host_id> local_dc_nodes;
        const auto& endpoints = topology.get_datacenter_endpoints();
        auto dc_it = endpoints.find(local_dc);
        if (dc_it != endpoints.end()) {
            local_dc_nodes = dc_it->second;
        }
        // By default, /localnodes lists the nodes of all racks in the given
        // DC, unless a single rack is selected by the "rack" query option.
        // If the rack does not exist, we return an empty list - not an error.
        sstring query_rack = req->get_query_param("rack");
        for (auto& id : local_dc_nodes) {
            if (!query_rack.empty()) {
                auto rack = _gossiper.get_application_state_value(id, gms::application_state::RACK);
                if (rack != query_rack) {
                    continue;
                }
            }
            // Note that it's not enough for the node to be is_alive() - a
            // node joining the cluster is also "alive" but not responsive to
            // requests. We alive *and* normal. See #19694, #21538.
            if (_gossiper.is_alive(id) && _gossiper.is_normal(id)) {
                // Use the gossiped broadcast_rpc_address if available instead
                // of the internal IP address "ip". See discussion in #18711.
                rjson::push_back(results, rjson::from_string(_gossiper.get_rpc_address(id)));
            }
        }
        rep->set_status(reply::status_type::ok);
        rep->set_content_type("json");
        rep->_content = rjson::print(results);
        return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
    }
};

// The CORS (Cross-origin resource sharing) protocol can send an OPTIONS
// request before ("pre-flight") the main request. The response to this
// request can be empty, but needs to have the right headers (which we
// fill with handle_CORS())
class options_handler : public gated_handler {
public:
    options_handler(seastar::gate& pending_requests) : gated_handler(pending_requests) {}
protected:
    virtual future<std::unique_ptr<reply>> do_handle(const sstring& path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) override {
        handle_CORS(*req, *rep, true);
        rep->set_status(reply::status_type::ok);
        rep->write_body("txt", sstring(""));
        return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
    }
};

// This function increments the authentication_failures counter, and may also
// log a warn-level message and/or throw an exception, depending on what
// enforce_authorization and warn_authorization are set to.
// The username and client address are only used for logging purposes -
// they are not included in the error message returned to the client, since
// the client knows who it is.
// Note that if enforce_authorization is false, this function will return
// without throwing. So a caller that doesn't want to continue after an
// authentication_error must explicitly return after calling this function.
template<typename Exception>
static void authentication_error(alternator::stats& stats, bool enforce_authorization, bool warn_authorization, Exception&& e, std::string_view user, gms::inet_address client_address) {
    stats.authentication_failures++;
    if (enforce_authorization) {
        if (warn_authorization) {
            slogger.warn("alternator_warn_authorization=true: {} for user {}, client address {}", e.what(), user, client_address);
        }
        throw std::move(e);
    } else {
        if (warn_authorization) {
            slogger.warn("If you set alternator_enforce_authorization=true the following will be enforced: {} for user {}, client address {}", e.what(), user, client_address);
        }
    }
}

future<std::string> server::verify_signature(const request& req, const chunked_content& content) {
    if (!_enforce_authorization.get() && !_warn_authorization.get()) {
        slogger.debug("Skipping authorization");
        return make_ready_future<std::string>();
    }
    auto host_it = req._headers.find("Host");
    if (host_it == req._headers.end()) {
        authentication_error(_executor._stats, _enforce_authorization.get(), _warn_authorization.get(),
            api_error::invalid_signature("Host header is mandatory for signature verification"), 
            "", req.get_client_address());
        return make_ready_future<std::string>();
    }
    auto authorization_it = req._headers.find("Authorization");
    if (authorization_it == req._headers.end()) {
        authentication_error(_executor._stats, _enforce_authorization.get(), _warn_authorization.get(),
            api_error::missing_authentication_token("Authorization header is mandatory for signature verification"),
            "", req.get_client_address());
        return make_ready_future<std::string>();
    }
    std::string host = host_it->second;
    std::string_view authorization_header = authorization_it->second;
    auto pos = authorization_header.find_first_of(' ');
    if (pos == std::string_view::npos || authorization_header.substr(0, pos) != "AWS4-HMAC-SHA256") {
        authentication_error(_executor._stats, _enforce_authorization.get(), _warn_authorization.get(),
            api_error::invalid_signature(fmt::format("Authorization header must use AWS4-HMAC-SHA256 algorithm: {}", authorization_header)),
            "", req.get_client_address());
        return make_ready_future<std::string>();
    }
    authorization_header.remove_prefix(pos+1);
    std::string credential;
    std::string user_signature;
    std::string signed_headers_str;
    std::vector<std::string_view> signed_headers;
    do {
        // Either one of a comma or space can mark the end of an entry
        pos = authorization_header.find_first_of(" ,");
        std::string_view entry = authorization_header.substr(0, pos);
        if (pos != std::string_view::npos) {
            authorization_header.remove_prefix(pos + 1);
        }
        if (entry.empty()) {
            continue;
        }
        std::vector<std::string_view> entry_split = split(entry, '=');
        if (entry_split.size() != 2) {
            continue;
        }
        std::string_view auth_value = entry_split[1];
        if (entry_split[0] == "Credential") {
            credential = std::string(auth_value);
        } else if (entry_split[0] == "Signature") {
            user_signature = std::string(auth_value);
        } else if (entry_split[0] == "SignedHeaders") {
            signed_headers_str = std::string(auth_value);
            signed_headers = split(auth_value, ';');
            std::sort(signed_headers.begin(), signed_headers.end());
        }
    } while (pos != std::string_view::npos);

    std::vector<std::string_view> credential_split = split(credential, '/');
    if (credential_split.size() != 5) {
        authentication_error(_executor._stats, _enforce_authorization.get(), _warn_authorization.get(),
            api_error::validation(fmt::format("Incorrect credential information format: {}", credential)), "", req.get_client_address());
        return make_ready_future<std::string>();
    }
    std::string user(credential_split[0]);
    std::string datestamp(credential_split[1]);
    std::string region(credential_split[2]);
    std::string service(credential_split[3]);

    std::map<std::string_view, std::string_view> signed_headers_map;
    for (const auto& header : signed_headers) {
        signed_headers_map.emplace(header, std::string_view());
    }
    std::vector<std::string> modified_values;
    for (auto& header : req._headers) {
        std::string header_str;
        header_str.resize(header.first.size());
        std::transform(header.first.begin(), header.first.end(), header_str.begin(), ::tolower);
        auto it = signed_headers_map.find(header_str);
        if (it != signed_headers_map.end()) {
            // replace multiple spaces in the header value header.second with
            // a single space, as required by AWS SigV4 header canonization.
            // If we modify the value, we need to save it in modified_values
            // to keep it alive.
            std::string value;
            value.reserve(header.second.size());
            bool prev_space = false;
            bool modified = false;
            for (char ch : header.second) {
                if (ch == ' ') {
                    if (!prev_space) {
                        value += ch;
                        prev_space = true;
                    } else {
                        modified = true; // skip a space
                    }
                } else {
                    value += ch;
                    prev_space = false;
                }
            }
            if (modified) {
                modified_values.emplace_back(std::move(value));
                it->second = std::string_view(modified_values.back());
            } else {
                it->second = std::string_view(header.second);
            }
        }
    }

    auto cache_getter = [&proxy = _proxy, &as = _auth_service] (std::string username) {
        return get_key_from_roles(proxy, as, std::move(username));
    };
    return _key_cache.get_ptr(user, cache_getter).then_wrapped([this, &req, &content,
                                                    user = std::move(user),
                                                    host = std::move(host),
                                                    datestamp = std::move(datestamp),
                                                    signed_headers_str = std::move(signed_headers_str),
                                                    signed_headers_map = std::move(signed_headers_map),
                                                    modified_values = std::move(modified_values),
                                                    region = std::move(region),
                                                    service = std::move(service),
                                                    user_signature = std::move(user_signature)] (future<key_cache::value_ptr> key_ptr_fut) {
        key_cache::value_ptr key_ptr(nullptr);
        try {
            key_ptr = key_ptr_fut.get();
        } catch (const api_error& e) {
            authentication_error(_executor._stats, _enforce_authorization.get(), _warn_authorization.get(),
                e, user, req.get_client_address());
            return std::string();
        }
        std::string signature;
        try {
            signature = utils::aws::get_signature(user, *key_ptr, std::string_view(host), "/", req._method,
                datestamp, signed_headers_str, signed_headers_map, &content, region, service, "");
        } catch (const std::exception& e) {
            authentication_error(_executor._stats, _enforce_authorization.get(), _warn_authorization.get(),
                api_error::invalid_signature(fmt::format("invalid signature: {}", e.what())),
                user, req.get_client_address());
            return std::string();
        }

        if (signature != std::string_view(user_signature)) {
            _key_cache.remove(user);
            authentication_error(_executor._stats, _enforce_authorization.get(), _warn_authorization.get(),
                api_error::unrecognized_client("wrong signature"),
                user, req.get_client_address());
            return std::string();
        }
        return user;
    });
}

static tracing::trace_state_ptr create_tracing_session(tracing::tracing& tracing_instance) {
    tracing::trace_state_props_set props;
    props.set<tracing::trace_state_props::full_tracing>();
    props.set_if<tracing::trace_state_props::log_slow_query>(tracing_instance.slow_query_tracing_enabled());
    return tracing_instance.create_session(tracing::trace_type::QUERY, props);
}

// A helper class to represent a potentially truncated view of a chunked_content.
// If the content is short enough and single chunked, it just holds a view into the content.
// Otherwise it will be copied into an internal buffer, possibly truncated (depending on maximum allowed size passed in),
// and the view will point into that buffer.
// `as_view()` method will return the view.
// `take_as_sstring()` will either move out the internal buffer (if any), or create a new sstring from the view.
// You should consider `as_view()` valid as long both the original chunked_content and the truncated_content object are alive.
class truncated_content {
    std::string_view _view;
    sstring _content_maybe;

    void copy_from_content(const chunked_content& content) {
        size_t offset = 0;
        for(auto &tmp : content) {
            size_t to_copy = std::min(tmp.size(), _content_maybe.size() - offset);
            std::copy(tmp.get(), tmp.get() + to_copy, _content_maybe.data() + offset);
            offset += to_copy;
            if (offset >= _content_maybe.size()) {
                break;
            }
        }
    }
public:
    truncated_content(const chunked_content& content, size_t max_len = std::numeric_limits<size_t>::max()) {
        if (content.empty()) return;
        if (content.size() == 1 && content.begin()->size() <= max_len) {
            _view = std::string_view(content.begin()->get(), content.begin()->size());
            return;
        }

        constexpr std::string_view truncated_text = "<truncated>";
        size_t content_size = 0;
        for(auto &tmp : content) {
            content_size += tmp.size();
        }
        if (content_size <= max_len) {
            _content_maybe = sstring{ sstring::initialized_later{}, content_size };
            copy_from_content(content);
        }
        else {
            _content_maybe = sstring{ sstring::initialized_later{}, max_len + truncated_text.size() };
            copy_from_content(content);
            std::copy(truncated_text.begin(), truncated_text.end(), _content_maybe.data() + _content_maybe.size() - truncated_text.size());
        }
        _view = std::string_view(_content_maybe);
    }

    std::string_view as_view() const { return _view; }
    sstring take_as_sstring() && {
        if (_content_maybe.empty() && !_view.empty()) {
            return sstring{_view};
        }
        return std::move(_content_maybe);
    }
};

// `truncated_content_view` will produce an object representing a view to a passed content
// possibly truncated at some length. The value returned is used in two ways:
// - to print it in logs (use `as_view()` method for this)
// - to pass it to tracing object, where it will be stored and used later
//   (use `take_as_sstring()` method as this produces a copy in form of a sstring)
// `truncated_content` delays constructing `sstring` object until it's actually needed.
// `truncated_content` is valid as long as passed `content` is alive.
// if the content is truncated, `<truncated>` will be appended at the maximum size limit
// and total size will be `max_users_query_size_in_trace_output() + strlen("<truncated>")`.
static truncated_content truncated_content_view(const chunked_content& content, size_t max_size) {
    return truncated_content{content, max_size};
}

static tracing::trace_state_ptr maybe_trace_query(service::client_state& client_state, std::string_view username, std::string_view op, const chunked_content& query, size_t max_users_query_size_in_trace_output) {
    tracing::trace_state_ptr trace_state;
    tracing::tracing& tracing_instance = tracing::tracing::get_local_tracing_instance();
    if (tracing_instance.trace_next_query() || tracing_instance.slow_query_tracing_enabled()) {
        trace_state = create_tracing_session(tracing_instance);
        tracing::add_session_param(trace_state, "alternator_op", op);
        tracing::add_query(trace_state, truncated_content_view(query, max_users_query_size_in_trace_output).take_as_sstring());
        tracing::begin(trace_state, seastar::format("Alternator {}", op), client_state.get_client_address());
        if (!username.empty()) {
            tracing::set_username(trace_state, auth::authenticated_user(username));
        }
    }
    return trace_state;
}

// This read_entire_stream() is similar to Seastar's read_entire_stream()
// which reads the given content_stream until its end into non-contiguous
// memory. The difference is that this implementation takes an extra length
// limit, and throws an error if we read more than this limit.
// This length-limited variant would not have been needed if Seastar's HTTP
// server's set_content_length_limit() worked in every case, but unfortunately
// it does not - it only works if the request has a Content-Length header (see
// issue #8196). In contrast this function can limit the request's length no
// matter how it's encoded. We need this limit to protect Alternator from
// oversized requests that can deplete memory.
static future<chunked_content>
read_entire_stream(input_stream<char>& inp, size_t length_limit) {
    chunked_content ret;
    // We try to read length_limit + 1 bytes, so that we can throw an
    // exception if we managed to read more than length_limit.
    ssize_t remain = length_limit + 1;
    do {
        temporary_buffer<char> buf = co_await inp.read_up_to(remain);
        if (buf.empty()) {
            break;
        }
        remain -= buf.size();
        ret.push_back(std::move(buf));
    } while (remain > 0);
    // If we read the full length_limit + 1 bytes, we went over the limit:
    if (remain <= 0) {
        // By throwing here an error, we may send a reply (the error message)
        // without having read the full request body. Seastar's httpd will
        // realize that we have not read the entire content stream, and
        // correctly mark the connection unreusable, i.e., close it.
        // This means we are currently exposed to issue #12166 caused by
        // Seastar issue 1325), where the client may get an RST instead of
        // a FIN, and may rarely get a "Connection reset by peer" before
        // reading the error we send.
        throw api_error::payload_too_large(fmt::format("Request content length limit of {} bytes exceeded", length_limit));
    }
    co_return ret;
}

// safe_gzip_stream is an exception-safe wrapper for zlib's z_stream.
// The "z_stream" struct is used by zlib to hold state while decompressing a
// stream of data. It allocates memory which must be freed with inflateEnd(),
// which the destructor of this class does.
class safe_gzip_zstream {
    z_stream _zs;
public:
    safe_gzip_zstream() {
        memset(&_zs, 0, sizeof(_zs));
        // The strange 16 + WMAX_BITS tells zlib to expect and decode
        // a gzip header, not a zlib header.
        if (inflateInit2(&_zs, 16 + MAX_WBITS) != Z_OK) {
            // Should only happen if memory allocation fails
            throw std::bad_alloc();
        }
    }
    ~safe_gzip_zstream() {
        inflateEnd(&_zs);
    }
    z_stream* operator->() {
        return &_zs;
    }
    z_stream* get() {
        return &_zs;
    }
    void reset() {
        inflateReset(&_zs);
    }
};

// ungzip() takes a chunked_content with a gzip-compressed request body,
// uncompresses it, and returns the uncompressed content as a chunked_content.
// If the uncompressed content exceeds length_limit, an error is thrown.
static future<chunked_content>
ungzip(chunked_content&& compressed_body, size_t length_limit) {
    chunked_content ret;
    // output_buf can be any size - when uncompressing input_buf, it doesn't
    // need to fit in a single output_buf, we'll use multiple output_buf for
    // a single input_buf if needed.
    constexpr size_t OUTPUT_BUF_SIZE = 4096;
    temporary_buffer<char> output_buf;
    safe_gzip_zstream strm;
    bool complete_stream = false; // empty input is not a valid gzip
    size_t total_out_bytes = 0;
    for (const temporary_buffer<char>& input_buf : compressed_body) {
        if (input_buf.empty()) {
            continue;
        }
        complete_stream = false;
        strm->next_in = (Bytef*) input_buf.get();
        strm->avail_in = (uInt) input_buf.size();
        do {
            co_await coroutine::maybe_yield();
            if (output_buf.empty()) {
                output_buf = temporary_buffer<char>(OUTPUT_BUF_SIZE);
            }
            strm->next_out = (Bytef*) output_buf.get();
            strm->avail_out = OUTPUT_BUF_SIZE;
            int e = inflate(strm.get(), Z_NO_FLUSH);
            size_t out_bytes = OUTPUT_BUF_SIZE - strm->avail_out;
            if (out_bytes > 0) {
                // If output_buf is nearly full, we save it as-is in ret. But
                // if it only has little data, better copy to a small buffer.
                if (out_bytes > OUTPUT_BUF_SIZE/2) {
                    ret.push_back(std::move(output_buf).prefix(out_bytes));
                    // output_buf is now empty. if this loop finds more input,
                    // we'll allocate a new output buffer.
                } else {
                    ret.push_back(temporary_buffer<char>(output_buf.get(), out_bytes));
                }
                total_out_bytes += out_bytes;
                if (total_out_bytes > length_limit) {
                    throw api_error::payload_too_large(fmt::format("Request content length limit of {} bytes exceeded", length_limit));
                }
            }
            if (e == Z_STREAM_END) {
                // There may be more input after the first gzip stream - in
                // either this input_buf or the next one. The additional input
                // should be a second concatenated gzip. We need to allow that
                // by resetting the gzip stream and continuing the input loop
                // until there's no more input.
                strm.reset();
                if (strm->avail_in == 0) {
                    complete_stream = true;
                    break;
                }
            } else if (e != Z_OK && e != Z_BUF_ERROR) {
                // DynamoDB returns an InternalServerError when given a bad
                // gzip request body. See test test_broken_gzip_content
                throw api_error::internal("Error during gzip decompression of request body");
            }
        } while (strm->avail_in > 0 || strm->avail_out == 0);
    }
    if (!complete_stream) {
        // The gzip stream was not properly finished with Z_STREAM_END
        throw api_error::internal("Truncated gzip in request body");
    }
    co_return ret;
}

future<executor::request_return_type> server::handle_api_request(std::unique_ptr<request> req) {
    _executor._stats.total_operations++;
    sstring target = req->get_header("X-Amz-Target");
    // target is DynamoDB API version followed by a dot '.' and operation type (e.g. CreateTable)
    auto dot = target.find('.');
    std::string_view op = (dot == sstring::npos) ? std::string_view() : std::string_view(target).substr(dot+1);
    if (req->content_length > request_content_length_limit) {
        // If we have a Content-Length header and know the request will be too
        // long, we don't need to wait for read_entire_stream() below to
        // discover it. And we definitely mustn't try to get_units() below for
        // for such a size.
        co_return api_error::payload_too_large(fmt::format("Request content length limit of {} bytes exceeded", request_content_length_limit));
    }
    // JSON parsing can allocate up to roughly 2x the size of the raw
    // document, + a couple of bytes for maintenance.
    // If the Content-Length of the request is not available, we assume
    // the largest possible request (request_content_length_limit, i.e., 16 MB)
    // and after reading the request we return_units() the excess.
    size_t mem_estimate = (req->content_length ? req->content_length : request_content_length_limit) * 2 + 8000;
    auto units_fut = get_units(*_memory_limiter, mem_estimate);
    if (_memory_limiter->waiters()) {
        ++_executor._stats.requests_blocked_memory;
    }
    auto units = co_await std::move(units_fut);
    SCYLLA_ASSERT(req->content_stream);
    chunked_content content = co_await read_entire_stream(*req->content_stream, request_content_length_limit);
    // If the request had no Content-Length, we reserved too many units
    // so need to return some
    if (req->content_length == 0) {
        size_t content_length = 0;
        for (const auto& chunk : content) {
            content_length += chunk.size();
        }
        size_t new_mem_estimate = content_length * 2 + 8000;
        units.return_units(mem_estimate - new_mem_estimate);
    }
    auto username = co_await verify_signature(*req, content);
    // If the request is compressed, uncompress it now, after we checked
    // the signature (the signature is computed on the compressed content).
    // We apply the request_content_length_limit again to the uncompressed
    // content - we don't want to allow a tiny compressed request to
    // expand to a huge uncompressed request.
    sstring content_encoding = req->get_header("Content-Encoding");
    if (content_encoding == "gzip") {
        content = co_await ungzip(std::move(content), request_content_length_limit);
    } else if (!content_encoding.empty()) {
        // DynamoDB returns a 500 error for unsupported Content-Encoding.
        // I'm not sure if this is the best error code, but let's do it too.
        // See the test test_garbage_content_encoding confirming this case.
        co_return api_error::internal("Unsupported Content-Encoding");
    }

    // As long as the system_clients_entry object is alive, this request will
    // be visible in the "system.clients" virtual table. When requested, this
    // entry will be formatted by server::ongoing_request::make_client_data().
    auto user_agent_header = co_await _connection_options_keys_and_values.get_or_load(req->get_header("User-Agent"), [] (const client_options_cache_key_type&) {
        return make_ready_future<options_cache_value_type>(options_cache_value_type{});
    });

    auto system_clients_entry = _ongoing_requests.emplace(
        req->get_client_address(), std::move(user_agent_header),
        username, current_scheduling_group(),
        req->get_protocol_name() == "https");

    if (slogger.is_enabled(log_level::trace)) {
        slogger.trace("Request: {} {} {}", op, truncated_content_view(content, _max_users_query_size_in_trace_output).as_view(), req->_headers);
    }
    auto callback_it = _callbacks.find(op);
    if (callback_it == _callbacks.end()) {
        _executor._stats.unsupported_operations++;
        co_return api_error::unknown_operation(fmt::format("Unsupported operation {}", op));
    }
    if (_pending_requests.get_count() >= _max_concurrent_requests) {
        _executor._stats.requests_shed++;
        co_return api_error::request_limit_exceeded(format("too many in-flight requests (configured via max_concurrent_requests_per_shard): {}", _pending_requests.get_count()));
    }
    _pending_requests.enter();
    auto leave = defer([this] () noexcept { _pending_requests.leave(); });
    executor::client_state client_state(service::client_state::external_tag(),
        _auth_service, &_sl_controller, _timeout_config.current_values(), req->get_client_address());
    if (!username.empty()) {
        client_state.set_login(auth::authenticated_user(username));
    }
    co_await client_state.maybe_update_per_service_level_params();

    tracing::trace_state_ptr trace_state = maybe_trace_query(client_state, username, op, content, _max_users_query_size_in_trace_output.get());
    tracing::trace(trace_state, "{}", op);

    auto user = client_state.user();
    auto f = [this, content = std::move(content), &callback = callback_it->second,
            client_state = std::move(client_state), trace_state = std::move(trace_state),
            units = std::move(units), req = std::move(req)] () mutable -> future<executor::request_return_type> {
                rjson::value json_request = co_await _json_parser.parse(std::move(content));
                if (!json_request.IsObject()) {
                    co_return api_error::validation("Request content must be an object");
                }
                co_return co_await callback(_executor, client_state, trace_state,
                    make_service_permit(std::move(units)), std::move(json_request), std::move(req));
    };
    co_return co_await _sl_controller.with_user_service_level(user, std::ref(f));
}

void server::set_routes(routes& r) {
    api_handler* req_handler = new api_handler([this] (std::unique_ptr<request> req) mutable {
        return handle_api_request(std::move(req));
    }, _proxy.data_dictionary().get_config());

    r.put(operation_type::POST, "/", req_handler);
    r.put(operation_type::GET, "/", new health_handler(_pending_requests));
    // The "/localnodes" request is a new Alternator feature, not supported by
    // DynamoDB and not required for DynamoDB compatibility. It allows a
    // client to enquire - using a trivial HTTP request without requiring
    // authentication - the list of all live nodes in the same data center of
    // the Alternator cluster. The client can use this list to balance its
    // request load to all the nodes in the same geographical region.
    // Note that this API exposes - openly without authentication - the
    // information on the cluster's members inside one data center. We do not
    // consider this to be a security risk, because an attacker can already
    // scan an entire subnet for nodes responding to the health request,
    // or even just scan for open ports.
    r.put(operation_type::GET, "/localnodes", new local_nodelist_handler(_pending_requests, _proxy, _gossiper));
    r.put(operation_type::OPTIONS, "/", new options_handler(_pending_requests));
}

//FIXME: A way to immediately invalidate the cache should be considered,
// e.g. when the system table which stores the keys is changed.
// For now, this propagation may take up to 1 minute.
server::server(executor& exec, service::storage_proxy& proxy, gms::gossiper& gossiper, auth::service& auth_service, qos::service_level_controller& sl_controller)
        : _http_server("http-alternator")
        , _https_server("https-alternator")
        , _executor(exec)
        , _proxy(proxy)
        , _gossiper(gossiper)
        , _auth_service(auth_service)
        , _sl_controller(sl_controller)
        , _key_cache(1024, 1min, slogger)
        , _max_users_query_size_in_trace_output(1024)
        , _enabled_servers{}
        , _pending_requests("alternator::server::pending_requests")
        , _timeout_config(_proxy.data_dictionary().get_config())
      , _callbacks{
        {"CreateTable", [] (executor& e, executor::client_state& client_state, tracing::trace_state_ptr trace_state, service_permit permit, rjson::value json_request, std::unique_ptr<request> req) {
            return e.create_table(client_state, std::move(trace_state), std::move(permit), std::move(json_request));
        }},
        {"DescribeTable", [] (executor& e, executor::client_state& client_state, tracing::trace_state_ptr trace_state, service_permit permit, rjson::value json_request, std::unique_ptr<request> req) {
            return e.describe_table(client_state, std::move(trace_state), std::move(permit), std::move(json_request));
        }},
        {"DeleteTable", [] (executor& e, executor::client_state& client_state, tracing::trace_state_ptr trace_state, service_permit permit, rjson::value json_request, std::unique_ptr<request> req) {
            return e.delete_table(client_state, std::move(trace_state), std::move(permit), std::move(json_request));
        }},
        {"UpdateTable", [] (executor& e, executor::client_state& client_state, tracing::trace_state_ptr trace_state, service_permit permit, rjson::value json_request, std::unique_ptr<request> req) {
            return e.update_table(client_state, std::move(trace_state), std::move(permit), std::move(json_request));
        }},
        {"PutItem", [] (executor& e, executor::client_state& client_state, tracing::trace_state_ptr trace_state, service_permit permit, rjson::value json_request, std::unique_ptr<request> req) {
            return e.put_item(client_state, std::move(trace_state), std::move(permit), std::move(json_request));
        }},
        {"UpdateItem", [] (executor& e, executor::client_state& client_state, tracing::trace_state_ptr trace_state, service_permit permit, rjson::value json_request, std::unique_ptr<request> req) {
            return e.update_item(client_state, std::move(trace_state), std::move(permit), std::move(json_request));
        }},
        {"GetItem", [] (executor& e, executor::client_state& client_state, tracing::trace_state_ptr trace_state, service_permit permit, rjson::value json_request, std::unique_ptr<request> req) {
            return e.get_item(client_state, std::move(trace_state), std::move(permit), std::move(json_request));
        }},
        {"DeleteItem", [] (executor& e, executor::client_state& client_state, tracing::trace_state_ptr trace_state, service_permit permit, rjson::value json_request, std::unique_ptr<request> req) {
            return e.delete_item(client_state, std::move(trace_state), std::move(permit), std::move(json_request));
        }},
        {"ListTables", [] (executor& e, executor::client_state& client_state, tracing::trace_state_ptr trace_state, service_permit permit, rjson::value json_request, std::unique_ptr<request> req) {
            return e.list_tables(client_state, std::move(permit), std::move(json_request));
        }},
        {"Scan", [] (executor& e, executor::client_state& client_state, tracing::trace_state_ptr trace_state, service_permit permit, rjson::value json_request, std::unique_ptr<request> req) {
            return e.scan(client_state, std::move(trace_state), std::move(permit), std::move(json_request));
        }},
        {"DescribeEndpoints", [] (executor& e, executor::client_state& client_state, tracing::trace_state_ptr trace_state, service_permit permit, rjson::value json_request, std::unique_ptr<request> req) {
            return e.describe_endpoints(client_state, std::move(permit), std::move(json_request), req->get_header("Host"));
        }},
        {"BatchWriteItem", [] (executor& e, executor::client_state& client_state, tracing::trace_state_ptr trace_state, service_permit permit, rjson::value json_request, std::unique_ptr<request> req) {
            return e.batch_write_item(client_state, std::move(trace_state), std::move(permit), std::move(json_request));
        }},
        {"BatchGetItem", [] (executor& e, executor::client_state& client_state, tracing::trace_state_ptr trace_state, service_permit permit, rjson::value json_request, std::unique_ptr<request> req) {
            return e.batch_get_item(client_state, std::move(trace_state), std::move(permit), std::move(json_request));
        }},
        {"Query", [] (executor& e, executor::client_state& client_state, tracing::trace_state_ptr trace_state, service_permit permit, rjson::value json_request, std::unique_ptr<request> req) {
            return e.query(client_state, std::move(trace_state), std::move(permit), std::move(json_request));
        }},
        {"TagResource", [] (executor& e, executor::client_state& client_state, tracing::trace_state_ptr trace_state, service_permit permit, rjson::value json_request, std::unique_ptr<request> req) {
            return e.tag_resource(client_state, std::move(permit), std::move(json_request));
        }},
        {"UntagResource", [] (executor& e, executor::client_state& client_state, tracing::trace_state_ptr trace_state, service_permit permit, rjson::value json_request, std::unique_ptr<request> req) {
            return e.untag_resource(client_state, std::move(permit), std::move(json_request));
        }},
        {"ListTagsOfResource", [] (executor& e, executor::client_state& client_state, tracing::trace_state_ptr trace_state, service_permit permit, rjson::value json_request, std::unique_ptr<request> req) {
            return e.list_tags_of_resource(client_state, std::move(permit), std::move(json_request));
        }},
        {"UpdateTimeToLive", [] (executor& e, executor::client_state& client_state, tracing::trace_state_ptr trace_state, service_permit permit, rjson::value json_request, std::unique_ptr<request> req) {
            return e.update_time_to_live(client_state, std::move(permit), std::move(json_request));
        }},
        {"DescribeTimeToLive", [] (executor& e, executor::client_state& client_state, tracing::trace_state_ptr trace_state, service_permit permit, rjson::value json_request, std::unique_ptr<request> req) {
            return e.describe_time_to_live(client_state, std::move(permit), std::move(json_request));
        }},
        {"ListStreams", [] (executor& e, executor::client_state& client_state, tracing::trace_state_ptr trace_state, service_permit permit, rjson::value json_request, std::unique_ptr<request> req) {
            return e.list_streams(client_state, std::move(permit), std::move(json_request));
        }},
        {"DescribeStream", [] (executor& e, executor::client_state& client_state, tracing::trace_state_ptr trace_state, service_permit permit, rjson::value json_request, std::unique_ptr<request> req) {
            return e.describe_stream(client_state, std::move(permit), std::move(json_request));
        }},
        {"GetShardIterator", [] (executor& e, executor::client_state& client_state, tracing::trace_state_ptr trace_state, service_permit permit, rjson::value json_request, std::unique_ptr<request> req) {
            return e.get_shard_iterator(client_state, std::move(permit), std::move(json_request));
        }},
        {"GetRecords", [] (executor& e, executor::client_state& client_state, tracing::trace_state_ptr trace_state, service_permit permit, rjson::value json_request, std::unique_ptr<request> req) {
            return e.get_records(client_state, std::move(trace_state), std::move(permit), std::move(json_request));
        }},
        {"DescribeContinuousBackups", [] (executor& e, executor::client_state& client_state, tracing::trace_state_ptr trace_state, service_permit permit, rjson::value json_request, std::unique_ptr<request> req) {
            return e.describe_continuous_backups(client_state, std::move(permit), std::move(json_request));
        }},
    } {
}

future<> server::init(net::inet_address addr, std::optional<uint16_t> port, std::optional<uint16_t> https_port, std::optional<tls::credentials_builder> creds,
        utils::updateable_value<bool> enforce_authorization, utils::updateable_value<bool> warn_authorization, utils::updateable_value<uint64_t> max_users_query_size_in_trace_output,
        semaphore* memory_limiter, utils::updateable_value<uint32_t> max_concurrent_requests) {
    _memory_limiter = memory_limiter;
    _enforce_authorization = std::move(enforce_authorization);
    _warn_authorization = std::move(warn_authorization);
    _max_concurrent_requests = std::move(max_concurrent_requests);
    _max_users_query_size_in_trace_output = std::move(max_users_query_size_in_trace_output);
    if (!port && !https_port) {
        return make_exception_future<>(std::runtime_error("Either regular port or TLS port"
                " must be specified in order to init an alternator HTTP server instance"));
    }
    return seastar::async([this, addr, port, https_port, creds] {
        _executor.start().get();

        if (port) {
            set_routes(_http_server._routes);
            _http_server.set_content_streaming(true);
            _http_server.listen(socket_address{addr, *port}).get();
            _enabled_servers.push_back(std::ref(_http_server));
        }
        if (https_port) {
            set_routes(_https_server._routes);
            _https_server.set_content_streaming(true);

            if (this_shard_id() == 0) {
                _credentials = creds->build_reloadable_server_credentials([this](const tls::credentials_builder& b, const std::unordered_set<sstring>& files, std::exception_ptr ep) -> future<> {
                    if (ep) {
                        slogger.warn("Exception loading {}: {}", files, ep);
                    } else {
                        co_await container().invoke_on_others([&b](server& s) {
                            if (s._credentials) {
                                b.rebuild(*s._credentials);
                            }
                        });
                        slogger.info("Reloaded {}", files);
                    }
                }).get();
            } else {
                _credentials = creds->build_server_credentials();
            }
            _https_server.listen(socket_address{addr, *https_port}, _credentials).get();
            _enabled_servers.push_back(std::ref(_https_server));
        }
    });
}

future<> server::stop() {
    return parallel_for_each(_enabled_servers, [] (http_server& server) {
        return server.stop();
    }).then([this] {
        return _pending_requests.close();
    }).then([this] {
        return _json_parser.stop();
    });
}

server::json_parser::json_parser() : _run_parse_json_thread(async([this] {
        while (true) {
            _document_waiting.wait().get();
            if (_as.abort_requested()) {
                return;
            }
            try {
                _parsed_document = rjson::parse_yieldable(std::move(_raw_document));
                _current_exception = nullptr;
            } catch (...) {
                _current_exception = std::current_exception();
            }
            _document_parsed.signal();
        }
    })) {
}

future<rjson::value> server::json_parser::parse(chunked_content&& content) {
    if (content.size() < yieldable_parsing_threshold) {
        return make_ready_future<rjson::value>(rjson::parse(std::move(content)));
    }
    return with_semaphore(_parsing_sem, 1, [this, content = std::move(content)] () mutable {
        _raw_document = std::move(content);
        _document_waiting.signal();
        return _document_parsed.wait().then([this] {
            if (_current_exception) {
                return make_exception_future<rjson::value>(_current_exception);
            }
            return make_ready_future<rjson::value>(std::move(_parsed_document));
        });
    });
}

future<> server::json_parser::stop() {
    _as.request_abort();
    _document_waiting.signal();
    _document_parsed.broken();
    return std::move(_run_parse_json_thread);
}

// Convert an entry in the server's list of ongoing Alternator requests
// (_ongoing_requests) into a client_data object. This client_data object
// will then be used to produce a row for the "system.clients" virtual table.
client_data server::ongoing_request::make_client_data() const {
    client_data cd;
    cd.ct = client_type::alternator;
    cd.ip = _client_address.addr();
    cd.port = _client_address.port();
    cd.shard_id = this_shard_id();
    cd.connection_stage = client_connection_stage::established;
    cd.username = _username;
    cd.scheduling_group_name = _scheduling_group.name();
    cd.ssl_enabled = _is_https;
    // For now, we save the full User-Agent header as the "driver name"
    // and keep "driver_version" unset.
    cd.driver_name = _user_agent;
    // Leave "protocol_version" unset, it has no meaning in Alternator.
    // Leave "hostname", "ssl_protocol" and "ssl_cipher_suite" unset.
    // As reported in issue #9216, we never set these fields in CQL
    // either (see cql_server::connection::make_client_data()).
    return cd;
}

future<utils::chunked_vector<foreign_ptr<std::unique_ptr<client_data>>>> server::get_client_data() {
    utils::chunked_vector<foreign_ptr<std::unique_ptr<client_data>>> ret;
    co_await _ongoing_requests.for_each_gently([&ret] (const ongoing_request& r) {
        ret.emplace_back(make_foreign(std::make_unique<client_data>(r.make_client_data())));
    });
    co_return ret;
}

const char* api_error::what() const noexcept {
    if (_what_string.empty()) {
        _what_string = fmt::format("{} {}: {}", std::to_underlying(_http_code), _type, _msg);
    }
    return _what_string.c_str();
}

}

