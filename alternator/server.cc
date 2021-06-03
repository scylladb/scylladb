/*
 * Copyright 2019-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "alternator/server.hh"
#include "log.hh"
#include <seastar/http/function_handlers.hh>
#include <seastar/http/short_streams.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/json/json_elements.hh>
#include "seastarx.hh"
#include "error.hh"
#include "utils/rjson.hh"
#include "auth.hh"
#include <cctype>
#include "cql3/query_processor.hh"
#include "service/storage_service.hh"
#include "utils/overloaded_functor.hh"

static logging::logger slogger("alternator-server");

using namespace httpd;

namespace alternator {

static constexpr auto TARGET = "X-Amz-Target";

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
public:
    api_handler(const std::function<future<executor::request_return_type>(std::unique_ptr<request> req)>& _handle) : _f_handle(
         [this, _handle](std::unique_ptr<request> req, std::unique_ptr<reply> rep) {
         return seastar::futurize_invoke(_handle, std::move(req)).then_wrapped([this, rep = std::move(rep)](future<executor::request_return_type> resf) mutable {
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
             auto res = resf.get0();
             std::visit(overloaded_functor {
                 [&] (const json::json_return_type& json_return_value) {
                     slogger.trace("api_handler success case");
                     if (json_return_value._body_writer) {
                         rep->write_body("json", std::move(json_return_value._body_writer));
                     } else {
                         rep->_content += json_return_value._res;
                     }
                 },
                 [&] (const api_error& err) {
                     generate_error_reply(*rep, err);
                 }
             }, res);

             return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
         });
    }), _type("json") { }

    api_handler(const api_handler&) = default;
    future<std::unique_ptr<reply>> handle(const sstring& path,
            std::unique_ptr<request> req, std::unique_ptr<reply> rep) override {
        handle_CORS(*req, *rep, false);
        return _f_handle(std::move(req), std::move(rep)).then(
                [this](std::unique_ptr<reply> rep) {
                    rep->done(_type);
                    return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
                });
    }

protected:
    void generate_error_reply(reply& rep, const api_error& err) {
        rep._content += "{\"__type\":\"com.amazonaws.dynamodb.v20120810#" + err._type + "\"," +
                "\"message\":\"" + err._msg + "\"}";
        rep._status = err._http_code;
        slogger.trace("api_handler error case: {}", rep._content);
    }

    future_handler_function _f_handle;
    sstring _type;
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
public:
    local_nodelist_handler(seastar::gate& pending_requests) : gated_handler(pending_requests) {}
protected:
    virtual future<std::unique_ptr<reply>> do_handle(const sstring& path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) override {
        rjson::value results = rjson::empty_array();
        // It's very easy to get a list of all live nodes on the cluster,
        // using gms::get_local_gossiper().get_live_members(). But getting
        // just the list of live nodes in this DC needs more elaborate code:
        sstring local_dc = locator::i_endpoint_snitch::get_local_snitch_ptr()->get_datacenter(
                utils::fb_utilities::get_broadcast_address());
        std::unordered_set<gms::inet_address> local_dc_nodes =
                service::get_local_storage_service().get_token_metadata().
                get_topology().get_datacenter_endpoints().at(local_dc);
        for (auto& ip : local_dc_nodes) {
            if (gms::get_local_gossiper().is_alive(ip)) {
                rjson::push_back(results, rjson::from_string(ip.to_sstring()));
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

future<std::string> server::verify_signature(const request& req, const chunked_content& content) {
    if (!_enforce_authorization) {
        slogger.debug("Skipping authorization");
        return make_ready_future<std::string>("<unauthenticated request>");
    }
    auto host_it = req._headers.find("Host");
    if (host_it == req._headers.end()) {
        throw api_error::invalid_signature("Host header is mandatory for signature verification");
    }
    auto authorization_it = req._headers.find("Authorization");
    if (authorization_it == req._headers.end()) {
        throw api_error::missing_authentication_token("Authorization header is mandatory for signature verification");
    }
    std::string host = host_it->second;
    std::vector<std::string_view> credentials_raw = split(authorization_it->second, ' ');
    std::string credential;
    std::string user_signature;
    std::string signed_headers_str;
    std::vector<std::string_view> signed_headers;
    for (std::string_view entry : credentials_raw) {
        std::vector<std::string_view> entry_split = split(entry, '=');
        if (entry_split.size() != 2) {
            if (entry != "AWS4-HMAC-SHA256") {
                throw api_error::invalid_signature(format("Only AWS4-HMAC-SHA256 algorithm is supported. Found: {}", entry));
            }
            continue;
        }
        std::string_view auth_value = entry_split[1];
        // Commas appear as an additional (quite redundant) delimiter
        if (auth_value.back() == ',') {
            auth_value.remove_suffix(1);
        }
        if (entry_split[0] == "Credential") {
            credential = std::string(auth_value);
        } else if (entry_split[0] == "Signature") {
            user_signature = std::string(auth_value);
        } else if (entry_split[0] == "SignedHeaders") {
            signed_headers_str = std::string(auth_value);
            signed_headers = split(auth_value, ';');
            std::sort(signed_headers.begin(), signed_headers.end());
        }
    }
    std::vector<std::string_view> credential_split = split(credential, '/');
    if (credential_split.size() != 5) {
        throw api_error::validation(format("Incorrect credential information format: {}", credential));
    }
    std::string user(credential_split[0]);
    std::string datestamp(credential_split[1]);
    std::string region(credential_split[2]);
    std::string service(credential_split[3]);

    std::map<std::string_view, std::string_view> signed_headers_map;
    for (const auto& header : signed_headers) {
        signed_headers_map.emplace(header, std::string_view());
    }
    for (auto& header : req._headers) {
        std::string header_str;
        header_str.resize(header.first.size());
        std::transform(header.first.begin(), header.first.end(), header_str.begin(), ::tolower);
        auto it = signed_headers_map.find(header_str);
        if (it != signed_headers_map.end()) {
            it->second = std::string_view(header.second);
        }
    }

    auto cache_getter = [&qp = _qp] (std::string username) {
        return get_key_from_roles(qp, std::move(username));
    };
    return _key_cache.get_ptr(user, cache_getter).then([this, &req, &content,
                                                    user = std::move(user),
                                                    host = std::move(host),
                                                    datestamp = std::move(datestamp),
                                                    signed_headers_str = std::move(signed_headers_str),
                                                    signed_headers_map = std::move(signed_headers_map),
                                                    region = std::move(region),
                                                    service = std::move(service),
                                                    user_signature = std::move(user_signature)] (key_cache::value_ptr key_ptr) {
        std::string signature = get_signature(user, *key_ptr, std::string_view(host), req._method,
                datestamp, signed_headers_str, signed_headers_map, content, region, service, "");

        if (signature != std::string_view(user_signature)) {
            _key_cache.remove(user);
            throw api_error::unrecognized_client("The security token included in the request is invalid.");
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

// truncated_content_view() prints a potentially long chunked_content for
// debugging purposes. In the common case when the content is not excessively
// long, it just returns a view into the given content, without any copying.
// But when the content is very long, it is truncated after some arbitrary
// max_len (or one chunk, whichever comes first), with "<truncated>" added at
// the end. To do this modification to the string, we need to create a new
// std::string, so the caller must pass us a reference to one, "buf", where
// we can store the content. The returned view is only alive for as long this
// buf is kept alive.
static std::string_view truncated_content_view(const chunked_content& content, std::string& buf) {
    constexpr size_t max_len = 1024;
    if (content.empty()) {
        return std::string_view();
    } else if (content.size() == 1 && content.begin()->size() <= max_len) {
        return std::string_view(content.begin()->get(), content.begin()->size());
    } else {
        buf = std::string(content.begin()->get(), std::min(content.begin()->size(), max_len)) + "<truncated>";
        return std::string_view(buf);
    }
}

static tracing::trace_state_ptr maybe_trace_query(service::client_state& client_state, std::string_view username, sstring_view op, const chunked_content& query) {
    tracing::trace_state_ptr trace_state;
    tracing::tracing& tracing_instance = tracing::tracing::get_local_tracing_instance();
    if (tracing_instance.trace_next_query() || tracing_instance.slow_query_tracing_enabled()) {
        trace_state = create_tracing_session(tracing_instance);
        std::string buf;
        tracing::add_session_param(trace_state, "alternator_op", op);
        tracing::add_query(trace_state, truncated_content_view(query, buf));
        tracing::begin(trace_state, format("Alternator {}", op), client_state.get_client_address());
        tracing::set_username(trace_state, auth::authenticated_user(username));
    }
    return trace_state;
}

future<executor::request_return_type> server::handle_api_request(std::unique_ptr<request> req) {
    _executor._stats.total_operations++;
    sstring target = req->get_header(TARGET);
    std::vector<std::string_view> split_target = split(target, '.');
    //NOTICE(sarna): Target consists of Dynamo API version followed by a dot '.' and operation type (e.g. CreateTable)
    std::string op = split_target.empty() ? std::string() : std::string(split_target.back());
    // JSON parsing can allocate up to roughly 2x the size of the raw
    // document, + a couple of bytes for maintenance.
    // TODO: consider the case where req->content_length is missing. Maybe
    // we need to take the content_length_limit and return some of the units
    // when we finish read_content_and_verify_signature?
    size_t mem_estimate = req->content_length * 2 + 8000;
    auto units_fut = get_units(*_memory_limiter, mem_estimate);
    if (_memory_limiter->waiters()) {
        ++_executor._stats.requests_blocked_memory;
    }
    auto units = co_await std::move(units_fut);
    assert(req->content_stream);
    chunked_content content = co_await httpd::read_entire_stream(*req->content_stream);
    auto username = co_await verify_signature(*req, content);

    if (slogger.is_enabled(log_level::trace)) {
        std::string buf;
        slogger.trace("Request: {} {} {}", op, truncated_content_view(content, buf), req->_headers);
    }
    auto callback_it = _callbacks.find(op);
    if (callback_it == _callbacks.end()) {
        _executor._stats.unsupported_operations++;
        co_return api_error::unknown_operation(format("Unsupported operation {}", op));
    }
    if (_pending_requests.get_count() >= _max_concurrent_requests) {
        _executor._stats.requests_shed++;
        co_return api_error::request_limit_exceeded(format("too many in-flight requests (configured via max_concurrent_requests_per_shard): {}", _pending_requests.get_count()));
    }
    _pending_requests.enter();
    auto leave = defer([this] { _pending_requests.leave(); });
    //FIXME: Client state can provide more context, e.g. client's endpoint address
    // We use unique_ptr because client_state cannot be moved or copied
    executor::client_state client_state{executor::client_state::internal_tag()};
    tracing::trace_state_ptr trace_state = maybe_trace_query(client_state, username, op, content);
    tracing::trace(trace_state, op);
    rjson::value json_request = co_await _json_parser.parse(std::move(content));
    co_return co_await callback_it->second(_executor, client_state, trace_state,
            make_service_permit(std::move(units)), std::move(json_request), std::move(req));
}

void server::set_routes(routes& r) {
    api_handler* req_handler = new api_handler([this] (std::unique_ptr<request> req) mutable {
        return handle_api_request(std::move(req));
    });

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
    r.put(operation_type::GET, "/localnodes", new local_nodelist_handler(_pending_requests));
    r.put(operation_type::OPTIONS, "/", new options_handler(_pending_requests));
}

//FIXME: A way to immediately invalidate the cache should be considered,
// e.g. when the system table which stores the keys is changed.
// For now, this propagation may take up to 1 minute.
server::server(executor& exec, cql3::query_processor& qp)
        : _http_server("http-alternator")
        , _https_server("https-alternator")
        , _executor(exec)
        , _qp(qp)
        , _key_cache(1024, 1min, slogger)
        , _enforce_authorization(false)
        , _enabled_servers{}
        , _pending_requests{}
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
    } {
}

future<> server::init(net::inet_address addr, std::optional<uint16_t> port, std::optional<uint16_t> https_port, std::optional<tls::credentials_builder> creds,
        bool enforce_authorization, semaphore* memory_limiter, utils::updateable_value<uint32_t> max_concurrent_requests) {
    _memory_limiter = memory_limiter;
    _enforce_authorization = enforce_authorization;
    _max_concurrent_requests = std::move(max_concurrent_requests);
    if (!port && !https_port) {
        return make_exception_future<>(std::runtime_error("Either regular port or TLS port"
                " must be specified in order to init an alternator HTTP server instance"));
    }
    return seastar::async([this, addr, port, https_port, creds] {
        try {
            _executor.start().get();

            if (port) {
                set_routes(_http_server._routes);
                _http_server.set_content_length_limit(server::content_length_limit);
                _http_server.set_content_streaming(true);
                _http_server.listen(socket_address{addr, *port}).get();
                _enabled_servers.push_back(std::ref(_http_server));
            }
            if (https_port) {
                set_routes(_https_server._routes);
                _https_server.set_content_length_limit(server::content_length_limit);
                _https_server.set_content_streaming(true);
                _https_server.set_tls_credentials(creds->build_reloadable_server_credentials([](const std::unordered_set<sstring>& files, std::exception_ptr ep) {
                    if (ep) {
                        slogger.warn("Exception loading {}: {}", files, ep);
                    } else {
                        slogger.info("Reloaded {}", files);
                    }
                }).get0());
                _https_server.listen(socket_address{addr, *https_port}).get();
                _enabled_servers.push_back(std::ref(_https_server));
            }
        } catch (...) {
            slogger.error("Failed to set up Alternator HTTP server on {} port {}, TLS port {}: {}",
                    addr, port ? std::to_string(*port) : "OFF", https_port ? std::to_string(*https_port) : "OFF", std::current_exception());
            std::throw_with_nested(std::runtime_error(
                    format("Failed to set up Alternator HTTP server on {} port {}, TLS port {}",
                            addr, port ? std::to_string(*port) : "OFF", https_port ? std::to_string(*https_port) : "OFF")));
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

}

