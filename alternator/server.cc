/*
 * Copyright 2019 ScyllaDB
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
#include <seastar/json/json_elements.hh>
#include <seastarx.hh>
#include "error.hh"
#include "rjson.hh"
#include "auth.hh"
#include <cctype>

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

// DynamoDB HTTP error responses are structured as follows
// https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Programming.Errors.html
// Our handlers throw an exception to report an error. If the exception
// is of type alternator::api_error, it unwrapped and properly reported to
// the user directly. Other exceptions are unexpected, and reported as
// Internal Server Error.
class api_handler : public handler_base {
public:
    api_handler(const future_json_function& _handle) : _f_handle(
         [_handle](std::unique_ptr<request> req, std::unique_ptr<reply> rep) {
         return seastar::futurize_apply(_handle, std::move(req)).then_wrapped([rep = std::move(rep)](future<json::json_return_type> resf) mutable {
             if (resf.failed()) {
                 // Exceptions of type api_error are wrapped as JSON and
                 // returned to the client as expected. Other types of
                 // exceptions are unexpected, and returned to the user
                 // as an internal server error:
                 api_error ret;
                 try {
                     resf.get();
                 } catch (api_error &ae) {
                     ret = ae;
                 } catch (rjson::error & re) {
                     ret = api_error("ValidationException", re.what());
                 } catch (...) {
                     ret = api_error(
                             "Internal Server Error",
                             format("Internal server error: {}", std::current_exception()),
                             reply::status_type::internal_server_error);
                 }
                 // FIXME: what is this version number?
                 rep->_content += "{\"__type\":\"com.amazonaws.dynamodb.v20120810#" + ret._type + "\"," +
                         "\"message\":\"" + ret._msg + "\"}";
                 rep->_status = ret._http_code;
                 slogger.trace("api_handler error case: {}", rep->_content);
                 return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
             }
             slogger.trace("api_handler success case");
             auto res = resf.get0();
             if (res._body_writer) {
                 rep->write_body("json", std::move(res._body_writer));
             } else {
                 rep->_content += res._res;
             }
             return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
         });
    }), _type("json") { }

    api_handler(const api_handler&) = default;
    future<std::unique_ptr<reply>> handle(const sstring& path,
            std::unique_ptr<request> req, std::unique_ptr<reply> rep) override {
        return _f_handle(std::move(req), std::move(rep)).then(
                [this](std::unique_ptr<reply> rep) {
                    rep->done(_type);
                    return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
                });
    }

protected:
    future_handler_function _f_handle;
    sstring _type;
};

static void verify_signature(request& req) {
    auto host_it = req._headers.find("Host");
    if (host_it == req._headers.end()) {
        throw api_error("InvalidSignatureException", "Host header is mandatory for signature verification");
    }
    auto authorization_it = req._headers.find("Authorization");
    if (host_it == req._headers.end()) {
        throw api_error("InvalidSignatureException", "Authorization header is mandatory for signature verification");
    }
    std::string_view host = host_it->second;
    std::vector<std::string_view> credentials_raw = split(authorization_it->second, ' ');
    std::string credential;
    std::string user_signature;
    std::string signed_headers_str;
    std::vector<std::string_view> signed_headers;
    for (std::string_view entry : credentials_raw) {
        std::vector<std::string_view> entry_split = split(entry, '=');
        if (entry_split.size() != 2) {
            if (entry != "AWS4-HMAC-SHA256") {
                throw api_error("InvalidSignatureException", format("Only AWS4-HMAC-SHA256 algorithm is supported. Found: {}", entry));
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
        throw api_error("ValidationException", format("Incorrect credential information format: {}", credential));
    }
    std::string_view user(credential_split[0]);
    //FIXME: use the datestamp to check if the authorization signature has not expired,
    // the default expiration period seems to be 5min.
    std::string_view datestamp(credential_split[1]);
    std::string_view region(credential_split[2]);
    std::string_view service(credential_split[3]);

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

    //FIXME: We need a proper way of providing the secret access key
    std::string secret_access_key = "whatever";
    std::string signature = get_signature(user, secret_access_key, std::string_view(host), req._method,
            signed_headers_str, signed_headers_map, req.content, region, service, "");

    if (signature != std::string_view(user_signature)) {
        throw api_error("UnrecognizedClientException", "The security token included in the request is invalid.");
    }
}

void server::set_routes(routes& r) {
    using alternator_callback = std::function<future<json::json_return_type>(executor&, executor::client_state&, std::unique_ptr<request>)>;
    std::unordered_map<std::string_view, alternator_callback> routes{
        {"CreateTable", [] (executor& e, executor::client_state& client_state, std::unique_ptr<request> req) {
            return e.maybe_create_keyspace().then([&e, &client_state, req = std::move(req)] { return e.create_table(client_state, req->content); }); }
        },
        {"DescribeTable", [] (executor& e, executor::client_state& client_state, std::unique_ptr<request> req) { return e.describe_table(client_state, req->content); }},
        {"DeleteTable", [] (executor& e, executor::client_state& client_state, std::unique_ptr<request> req) { return e.delete_table(client_state, req->content); }},
        {"PutItem", [] (executor& e, executor::client_state& client_state, std::unique_ptr<request> req) { return e.put_item(client_state, req->content); }},
        {"UpdateItem", [] (executor& e, executor::client_state& client_state, std::unique_ptr<request> req) { return e.update_item(client_state, req->content); }},
        {"GetItem", [] (executor& e, executor::client_state& client_state, std::unique_ptr<request> req) { return e.get_item(client_state, req->content); }},
        {"DeleteItem", [] (executor& e, executor::client_state& client_state, std::unique_ptr<request> req) { return e.delete_item(client_state, req->content); }},
        {"ListTables", [] (executor& e, executor::client_state& client_state, std::unique_ptr<request> req) { return e.list_tables(client_state, req->content); }},
        {"Scan", [] (executor& e, executor::client_state& client_state, std::unique_ptr<request> req) { return e.scan(client_state, req->content); }},
        {"DescribeEndpoints", [] (executor& e, executor::client_state& client_state, std::unique_ptr<request> req) { return e.describe_endpoints(client_state, req->content, req->get_header("Host")); }},
        {"BatchWriteItem", [] (executor& e, executor::client_state& client_state, std::unique_ptr<request> req) { return e.batch_write_item(client_state, req->content); }},
        {"BatchGetItem", [] (executor& e, executor::client_state& client_state, std::unique_ptr<request> req) { return e.batch_get_item(client_state, req->content); }},
        {"Query", [] (executor& e, executor::client_state& client_state, std::unique_ptr<request> req) { return e.query(client_state, req->content); }},
    };

    api_handler* handler = new api_handler([this, routes = std::move(routes)](std::unique_ptr<request> req) -> future<json::json_return_type> {
        _executor.local()._stats.total_operations++;
        sstring target = req->get_header(TARGET);
        std::vector<std::string_view> split_target = split(target, '.');
        //NOTICE(sarna): Target consists of Dynamo API version followed by a dot '.' and operation type (e.g. CreateTable)
        std::string op = split_target.empty() ? std::string() : std::string(split_target.back());
        slogger.trace("Request: {} {}", op, req->content);
        if (_enforce_authorization) {
            verify_signature(*req);
        } else {
            slogger.debug("Skipping authorization");
        }
        auto callback_it = routes.find(op);
        if (callback_it == routes.end()) {
            _executor.local()._stats.unsupported_operations++;
            throw api_error("UnknownOperationException",
                    format("Unsupported operation {}", op));
        }
        //FIXME: Client state can provide more context, e.g. client's endpoint address
        // We use unique_ptr because client_state cannot be moved or copied
        return do_with(std::make_unique<executor::client_state>(executor::client_state::internal_tag()), [this, callback_it = std::move(callback_it), op = std::move(op), req = std::move(req)] (std::unique_ptr<executor::client_state>& client_state) mutable {
            client_state->set_raw_keyspace(executor::KEYSPACE_NAME);
            executor::maybe_trace_query(*client_state, op, req->content);
            tracing::trace(client_state->get_trace_state(), op);
            return callback_it->second(_executor.local(), *client_state, std::move(req));
        });
    });

    r.add(operation_type::POST, url("/"), handler);
}

future<> server::init(net::inet_address addr, std::optional<uint16_t> port, std::optional<uint16_t> https_port, std::optional<tls::credentials_builder> creds, bool enforce_authorization) {
    _enforce_authorization = enforce_authorization;
    if (!port && !https_port) {
        return make_exception_future<>(std::runtime_error("Either regular port or TLS port"
                " must be specified in order to init an alternator HTTP server instance"));
    }
    return seastar::async([this, addr, port, https_port, creds] {
        try {
            _executor.invoke_on_all([] (executor& e) {
                return e.start();
            }).get();

            if (port) {
                _control.start().get();
                _control.set_routes(std::bind(&server::set_routes, this, std::placeholders::_1)).get();
                _control.listen(socket_address{addr, *port}).get();
                slogger.info("Alternator HTTP server listening on {} port {}", addr, *port);
            }
            if (https_port) {
                _https_control.start().get();
                _https_control.set_routes(std::bind(&server::set_routes, this, std::placeholders::_1)).get();
                _https_control.server().invoke_on_all([creds] (http_server& serv) {
                    return serv.set_tls_credentials(creds->build_server_credentials());
                }).get();

                _https_control.listen(socket_address{addr, *https_port}).get();
                slogger.info("Alternator HTTPS server listening on {} port {}", addr, *https_port);
            }
        } catch (...) {
            slogger.warn("Failed to set up Alternator HTTP server on {} port {}, TLS port {}: {}",
                    addr, port ? std::to_string(*port) : "OFF", https_port ? std::to_string(*https_port) : "OFF", std::current_exception());
            throw;
        }
    });
}

}

