/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <ranges>
#include <sstream>

#include <seastar/core/seastar.hh>
#include <seastar/core/units.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include <seastar/coroutine/switch_to.hh>
#include <seastar/http/exception.hh>
#include <seastar/http/httpd.hh>
#include <seastar/util/defer.hh>
#include <seastar/util/log.hh>

#include "auth/service.hh"
#include "client_data.hh"
#include "cql3/query_processor.hh"
#include "cql3/column_identifier.hh"
#include "cql3/column_specification.hh"
#include "cql3/query_result_printer.hh"
#include "db/marshal/type_parser.hh"
#include "gms/gossiper.hh"
#include "db/config.hh"
#include "service/client_state.hh"
#include "service/qos/service_level_controller.hh"
#include "service/storage_proxy.hh"
#include "tools/webshell/webshell.hh"
#include "transport/messages/result_message.hh"
#include "utils/base64.hh"
#include "utils/rjson.hh"
#include "utils/scoped_item_list.hh"

using namespace httpd;
using request = http::request;
using reply = http::reply;

namespace rjs = rjson::schema;

namespace tools::webshell {

static logger wslog("webshell");

namespace {

// The largest request body accepted. The body holds a single query, along with
// its options, so this is a generous bound. It is also the content length limit
// of the HTTP server, but that is enforced only for requests that declare a
// content length: a chunked body is bounded by read_request_body().
constexpr size_t max_request_body_size = 1*MB;
// The largest request line and headers accepted, which the HTTP server buffers
// in memory before the handler runs.
constexpr size_t max_request_line_and_headers_size = 16*KB;

constexpr int32_t default_page_size = 100;
constexpr int32_t max_page_size = 1000;

// Requests are processed one-by-one on each shard, up to this many
// concurrently. Further requests queue up, but only up to max_waiting_requests,
// beyond that they are rejected with 429 Too Many Requests.
constexpr uint64_t max_concurrent_requests = 16;
constexpr uint64_t max_waiting_requests = 16;

// Every response of every endpoint, except the files of the web interface, is a
// JSON object. Errors carry their message in the "error" member.
//
// A request which fails authentication is answered with 401, and a
// WWW-Authenticate header, as the standard requires. The challenge is
// deliberately not "Basic", although Basic credentials are what the server
// accepts: a browser answers a Basic challenge by asking for credentials with
// its own dialog, then attaches them to every later request to the same
// server, including the ones a malicious page of another origin makes it send.
// Not asking keeps the credentials where the web interface puts them: in the
// Authorization header of its own requests.
//
// A request which has to be executed on another node is answered with 501, and
// names that node in the "target_node" member, so the client can retry there.

// The node a query has to be executed on.
struct target_node {
    locator::host_id host_id;
    // Unknown if the node is not in the address map (e.g. it left the cluster).
    std::optional<gms::inet_address> address;
};

std::unique_ptr<reply> make_error_reply(reply::status_type status, std::string_view message, const std::optional<target_node>& target = {}) {
    auto rep = std::make_unique<reply>();
    rep->set_status(status);
    if (status == reply::status_type::unauthorized) {
        rep->add_header("WWW-Authenticate", "ScyllaDB-Webshell realm=\"ScyllaDB Web Shell\"");
    }
    if (status == reply::status_type::too_many_requests) {
        rep->add_header("Retry-After", "1");
    }

    std::ostringstream body;
    rjson::streaming_writer writer(body);
    writer.StartObject();
    writer.Key("error");
    writer.String(message);
    if (target) {
        writer.Key("target_node");
        writer.StartObject();
        writer.Key("host_id");
        writer.String(fmt::to_string(target->host_id));
        if (target->address) {
            writer.Key("address");
            writer.String(fmt::to_string(*target->address));
        }
        writer.EndObject();
    }
    writer.EndObject();
    rep->write_body("json", sstring(body.view()));
    return rep;
}

// Thrown to fail a request with the given status and message, which become the
// status code and the "error" member of the response.
class webshell_error : public base_exception {
    std::optional<target_node> _target;
public:
    webshell_error(reply::status_type status, const std::string& message, std::optional<target_node> target = {})
        : base_exception(message, status), _target(std::move(target))
    { }
    const std::optional<target_node>& target() const noexcept { return _target; }
};

// A request, as reported in the system.clients virtual table: from its arrival,
// through waiting for its turn and processing it, to having written its response.
struct ongoing_request {
    socket_address client_address;
    // "webshell: <User-Agent header>", reported as the driver name. The prefix
    // makes it obvious which entries are of the Web Shell, whatever the client
    // claims to be.
    client_options_cache_entry_type user_agent;
    bool is_https;
    // established: waiting for its turn, authenticating: being authenticated,
    // ready: being processed.
    client_connection_stage stage = client_connection_stage::established;
    std::optional<sstring> username;
    scheduling_group sg = current_scheduling_group();

    client_data make_client_data() const {
        client_data cd;
        cd.ip = client_address.addr();
        cd.port = client_address.port();
        cd.ct = client_type::webshell;
        cd.connection_stage = stage;
        cd.shard_id = this_shard_id();
        cd.driver_name = user_agent;
        cd.ssl_enabled = is_https;
        cd.username = username;
        cd.scheduling_group_name = sg.name();
        return cd;
    }
};

class request_control {
    named_gate _gate;
    named_semaphore _semaphore;
    client_options_cache_type _user_agents;
    utils::scoped_item_list<ongoing_request> _ongoing_requests;

public:
    using units = semaphore_units<named_semaphore_exception_factory>;

    // A request counts against the concurrency limit, and is reported in
    // system.clients, for as long as its permit is alive, which can include
    // writing its response body.
    class permit {
        units _units;
        utils::scoped_item_list<ongoing_request>::handle _entry;
    public:
        permit(units u, utils::scoped_item_list<ongoing_request>::handle entry)
            : _units(std::move(u)), _entry(std::move(entry))
        { }
        ongoing_request& request() { return _entry.get(); }
    };

    explicit request_control(sstring name)
        : _gate(name)
        , _semaphore(max_concurrent_requests, named_semaphore_exception_factory{.name = name})
    {}

    bool too_many_waiters() const {
        // The request asking is not waiting yet. It would have to wait if there
        // are waiters already (they are served first), or there are no units.
        const bool would_wait = _semaphore.waiters() > 0 || _semaphore.available_units() <= 0;
        return would_wait && _semaphore.waiters() >= max_waiting_requests;
    }

    // Invoke func with the permit of the request, once it is its turn. The
    // request is reported in system.clients while it waits for its turn too.
    template <typename Func>
    future<std::unique_ptr<reply>> run(const request& req, Func func) {
        auto holder = _gate.hold();
        auto user_agent = co_await _user_agents.get_or_load(seastar::format("webshell: {}", req.get_header("User-Agent")), [] (const client_options_cache_key_type&) {
            return make_ready_future<options_cache_value_type>(options_cache_value_type{});
        });
        auto entry = _ongoing_requests.emplace(req.get_client_address(), std::move(user_agent), req.get_protocol_name() == "https");
        auto u = co_await get_units(_semaphore, 1);
        co_return co_await func(permit(std::move(u), std::move(entry)));
    }

    // The requests of this shard, waiting or being processed, for system.clients.
    future<utils::chunked_vector<foreign_ptr<std::unique_ptr<client_data>>>> get_client_data() {
        utils::chunked_vector<foreign_ptr<std::unique_ptr<client_data>>> ret;
        // The server can be stopped while this runs, and stop() waits for the gate.
        auto holder = _gate.try_hold();
        if (!holder) {
            co_return ret;
        }
        co_await _ongoing_requests.for_each_gently([&ret] (const ongoing_request& r) {
            ret.emplace_back(make_foreign(std::make_unique<client_data>(r.make_client_data())));
        });
        co_return ret;
    }

    future<> stop() noexcept {
        _semaphore.broken();
        return _gate.close();
    }
};

// Base class for all handlers, subjecting each request to concurrency control.
class gated_handler : public handler_base {
    const char* _name;
    request_control& _request_control;
public:
    using request_permit = request_control::permit;

    gated_handler(const char* name, request_control& request_control)
        : _name(name), _request_control(request_control)
    {}

    // The handler may pass permit on to the body writer of its reply, to keep
    // counting against the concurrency limit while the body is written.
    virtual future<std::unique_ptr<reply>> do_handle(const sstring& path, std::unique_ptr<request> req, std::unique_ptr<reply> rep, request_permit permit) = 0;

    virtual future<std::unique_ptr<reply>> handle(const sstring& path_, std::unique_ptr<request> req, std::unique_ptr<reply> rep) final override {
        const auto path = path_;
        const auto method = req->_method;
        wslog.trace("handler {}: start request {} {}", _name, method, path);

        if (_request_control.too_many_waiters()) {
            wslog.debug("handler {}: rejecting {} {}: too many requests", _name, method, path);
            throw webshell_error(reply::status_type::too_many_requests, "Too many requests, try again later");
        }

        try {
            auto& req_ref = *req;
            auto ret = co_await _request_control.run(req_ref, [this, &path, req = std::move(req), rep = std::move(rep)] (request_permit permit) mutable {
                permit.request().stage = client_connection_stage::ready;
                return do_handle(path, std::move(req), std::move(rep), std::move(permit));
            });
            wslog.trace("handler {}: finish request {} {} {}", _name, method, path, ret->_status);
            co_return ret;
        } catch (gate_closed_exception&) {
            throw webshell_error(reply::status_type::service_unavailable, "Server shutting down");
        } catch (broken_semaphore&) {
            throw webshell_error(reply::status_type::service_unavailable, "Server shutting down");
        }
    }
};

// Answers the requests which name no endpoint.
class not_found_handler : public gated_handler {
public:
    explicit not_found_handler(request_control& request_control)
        : gated_handler("not-found", request_control)
    {}

protected:
    virtual future<std::unique_ptr<reply>> do_handle(const sstring& path, std::unique_ptr<request> req, std::unique_ptr<reply> rep, request_permit) override {
        throw webshell_error(reply::status_type::not_found, seastar::format("No such endpoint: {} {}", req->_method, path));
    }
};

// Answers a request that names an endpoint with the wrong method.
class method_not_allowed_handler : public gated_handler {
    const char* _allowed_method;
public:
    method_not_allowed_handler(request_control& request_control, const char* allowed_method)
        : gated_handler("method-not-allowed", request_control), _allowed_method(allowed_method)
    {}

protected:
    virtual future<std::unique_ptr<reply>> do_handle(const sstring& path, std::unique_ptr<request> req, std::unique_ptr<reply> rep, request_permit) override {
        auto error_rep = make_error_reply(reply::status_type::method_not_allowed,
                seastar::format("{} is not allowed on {}, use {}", req->_method, path, _allowed_method));
        error_rep->add_header("Allow", _allowed_method);
        co_return error_rep;
    }
};

} // anonymous namespace

class server : public peering_sharded_service<server> {
public:
    // A single resolved socket the server is to listen on.
    struct listener {
        socket_address addr;
        bool proxy_protocol = false;
        // Index into the credentials vector passed to apply_listeners(), or
        // nullopt for an unencrypted listener.
        std::optional<size_t> credentials_idx;
    };

    // A listener apply_listeners() could not open, and why.
    struct listener_failure {
        socket_address addr;
        std::exception_ptr error;
    };

private:
    cql3::query_processor& _qp;
    auth::service& _auth_service;
    qos::service_level_controller& _sl_controller;
    updateable_timeout_config& _timeout_config;
    gms::gossiper& _gossiper;

    httpd::http_server _http_server;
    httpd::http_server _https_server;
    request_control _request_control;

    // Server credentials, one per distinct set of TLS options passed to
    // apply_listeners(). Indexed by listener::credentials_idx.
    std::vector<::shared_ptr<seastar::tls::server_credentials>> _credentials;
    // What each entry of _credentials was built from. Only ever appended to, so
    // that an index handed out earlier keeps meaning the same credentials - the
    // reload callback rebuilds _credentials[i] by index on the other shards.
    std::vector<std::unordered_map<sstring, sstring>> _credentials_options;
    // Held by certificate reloads, which can fire while the server is being
    // stopped, see add_credentials().
    named_gate _credentials_reload_gate{"webshell-credentials-reload"};

    // Each listener being served, by the address it is bound to, with the handle
    // on its life: aborting the source ends that listener and drains it, and the
    // future is its end of that.
    struct serving_listener {
        listener config;
        std::unique_ptr<abort_source> stop;
        future<> served = make_ready_future<>();
    };
    std::unordered_map<socket_address, serving_listener> _listeners;

private:
    void set_routes(routes& r);
    future<> serve(const listener& l);
    future<> stop_serving(serving_listener& l);
    future<> add_credentials(tls::credentials_builder& builder);

public:
    server(cql3::query_processor& qp, auth::service& auth_service, qos::service_level_controller& sl_controller,
            updateable_timeout_config& timeout_config, gms::gossiper& gossiper);

    cql3::query_processor& qp() { return _qp; }
    auth::service& auth_service() { return _auth_service; }
    qos::service_level_controller& sl_controller() { return _sl_controller; }
    const updateable_timeout_config& timeout_config() const { return _timeout_config; }
    const gms::gossiper& gossiper() const { return _gossiper; }

    // Gets the server ready to serve, without listening anywhere yet:
    // apply_listeners() opens the listeners.
    future<> init();

    // Serve `listeners` from now on instead of whatever is being served: open
    // the ones that are new, and stop the ones that are gone, draining their
    // connections. The listeners that appear in both are left alone.
    //
    // `credentials` is indexed by listener::credentials_idx, and
    // `credentials_options` says what each of them was built from, so that a
    // listener asking for encryption it already has keeps the credentials it is
    // using - including the reloading that watches their files.
    //
    // Best effort: returns the listeners that could not be opened on this
    // shard, the others are open regardless.
    future<std::vector<listener_failure>> apply_listeners(std::vector<listener> listeners,
            std::vector<tls::credentials_builder> credentials,
            std::vector<std::unordered_map<sstring, sstring>> credentials_options);

    // Stop serving the listeners on the given addresses, if they are being served.
    future<> stop_listening(const std::vector<socket_address>& addrs);

    // The requests of this shard, waiting or being processed, for system.clients.
    future<utils::chunked_vector<foreign_ptr<std::unique_ptr<client_data>>>> get_client_data() {
        return _request_control.get_client_data();
    }

    future<> stop();
};

namespace {

// Read the request body, at most max_request_body_size bytes of it.
//
// The HTTP server enforces its content length limit only on requests that
// declare a Content-Length. A chunked body declares none, so it is bounded here.
future<sstring> read_request_body(input_stream<char>& in) {
    std::vector<temporary_buffer<char>> bufs;
    size_t size = 0;
    while (auto buf = co_await in.read()) {
        size += buf.size();
        if (size > max_request_body_size) {
            throw webshell_error(reply::status_type::payload_too_large,
                    seastar::format("Request body too large, max size is {} bytes", max_request_body_size));
        }
        bufs.push_back(std::move(buf));
    }
    auto body = uninitialized_string(size);
    auto it = body.begin();
    for (const auto& buf : bufs) {
        it = std::copy(buf.begin(), buf.end(), it);
    }
    co_return body;
}

struct basic_credentials {
    sstring username;
    sstring password;
};

// Parse the value of an "Authorization: Basic <base64(username:password)>" header.
basic_credentials parse_basic_authorization(std::string_view header) {
    const auto sep = header.find(' ');
    // The scheme is case-insensitive.
    if (sep == std::string_view::npos || !std::ranges::equal(header.substr(0, sep), std::string_view("basic"), [] (char a, char b) {
                return std::tolower(static_cast<unsigned char>(a)) == b;
            })) {
        throw webshell_error(reply::status_type::unauthorized, "Unsupported Authorization header, expected: Basic <credentials>");
    }
    auto token = header.substr(sep + 1);
    while (!token.empty() && token.front() == ' ') {
        token.remove_prefix(1);
    }
    bytes decoded;
    try {
        decoded = base64_decode(token);
    } catch (...) {
        throw webshell_error(reply::status_type::unauthorized, "Malformed Authorization header: credentials are not valid base64");
    }
    const auto user_pass = std::string_view(reinterpret_cast<const char*>(decoded.data()), decoded.size());
    const auto colon = user_pass.find(':');
    if (colon == std::string_view::npos) {
        throw webshell_error(reply::status_type::unauthorized, "Malformed Authorization header: credentials are not in the username:password format");
    }
    return basic_credentials{sstring(user_pass.substr(0, colon)), sstring(user_pass.substr(colon + 1))};
}

// Authenticate the user making the request.
//
// Every request is authenticated on its own. The client is authenticated by its TLS client certificate, if it presented one which the
// authenticator can map to a role, or else by the username and password in the
// Authorization header, with the Basic scheme.
//
// Returns std::nullopt if the authenticator doesn't require authentication:
// the client is anonymous then.
future<std::optional<auth::authenticated_user>> authenticate(auth::service& auth_service, const request& req) {
    auto& authenticator = auth_service.underlying_authenticator();
    if (!authenticator.require_authentication()) {
        co_return std::nullopt;
    }

    // request::tls_dn is set if the client connected over TLS and presented a
    // certificate, request::tls_san points to the certificate's subject
    // alternative names. Both are owned by the connection and are valid for as
    // long as the request is being handled.
    if (req.tls_dn) {
        try {
            auto cert_role = co_await authenticator.authenticate([req = &req] () -> future<std::optional<auth::certificate_info>> {
                co_return auth::certificate_info{req->tls_dn->subject, [req] () -> future<std::string> {
                    co_return req->tls_san ? fmt::format("{}", fmt::join(*req->tls_san, ",")) : std::string();
                }};
            });
            if (cert_role) {
                co_return auth::authenticated_user(*cert_role);
            }
        } catch (exceptions::authentication_exception& e) {
            throw webshell_error(reply::status_type::unauthorized, e.what());
        }
    }

    const auto header = req.get_header("Authorization");
    if (header.empty()) {
        throw webshell_error(reply::status_type::unauthorized,
                "Authentication required: provide credentials in the Authorization header (Basic scheme), or a TLS client certificate");
    }
    const auto credentials = parse_basic_authorization(header);

    // SASL PLAIN: authzid, authcid and password, separated by NUL bytes.
    bytes_ostream buf;
    buf.write(credentials.username.data(), credentials.username.size());
    buf.write("\0", 1);
    buf.write(credentials.username.data(), credentials.username.size());
    buf.write("\0", 1);
    buf.write(credentials.password.data(), credentials.password.size());

    auto sasl_challenge = authenticator.new_sasl_challenge();
    try {
        sasl_challenge->evaluate_response(buf.linearize());
        if (!sasl_challenge->is_complete()) {
            throw webshell_error(reply::status_type::not_implemented,
                    "The configured authenticator uses multi-stage SASL authentication, which the Web Shell doesn't support");
        }
        co_return co_await sasl_challenge->get_authenticated_user();
    } catch (exceptions::authentication_exception& e) {
        throw webshell_error(reply::status_type::unauthorized, e.what());
    }
}

// The consistency levels accepted by the "consistency" request member.
// Keys are lower-case, the value is lower-cased before looking it up.
const std::unordered_map<std::string_view, db::consistency_level>& consistency_levels() {
    static const std::unordered_map<std::string_view, db::consistency_level> str_to_cl {
        {"any", db::consistency_level::ANY},
        {"one", db::consistency_level::ONE},
        {"two", db::consistency_level::TWO},
        {"three", db::consistency_level::THREE},
        {"quorum", db::consistency_level::QUORUM},
        {"all", db::consistency_level::ALL},
        {"local_quorum", db::consistency_level::LOCAL_QUORUM},
        {"each_quorum", db::consistency_level::EACH_QUORUM},
        {"serial", db::consistency_level::SERIAL},
        {"local_serial", db::consistency_level::LOCAL_SERIAL},
        {"local_one", db::consistency_level::LOCAL_ONE},
    };
    return str_to_cl;
}

// The consistency levels accepted by the "serial_consistency" request member.
const std::unordered_map<std::string_view, db::consistency_level>& serial_consistency_levels() {
    static const std::unordered_map<std::string_view, db::consistency_level> str_to_cl {
        {"serial", db::consistency_level::SERIAL},
        {"local_serial", db::consistency_level::LOCAL_SERIAL},
    };
    return str_to_cl;
}

db::consistency_level parse_consistency_level(std::string_view cl_str, const std::unordered_map<std::string_view, db::consistency_level>& str_to_cl, std::string_view member) {
    const auto cl_lower = cl_str | std::views::transform([] (unsigned char c) { return char(std::tolower(c)); }) | std::ranges::to<std::string>();
    if (auto it = str_to_cl.find(cl_lower); it != str_to_cl.end()) {
        return it->second;
    }
    auto all_cls = std::views::values(str_to_cl) | std::ranges::to<std::vector<db::consistency_level>>();
    std::ranges::sort(all_cls);
    throw webshell_error(reply::status_type::bad_request, seastar::format("Invalid {}: {}, expected one of: {}", member, cl_str, fmt::join(all_cls, ", ")));
}

struct query_request {
    sstring query;
    std::optional<sstring> keyspace;
    db::consistency_level consistency = db::consistency_level::ONE;
    db::consistency_level serial_consistency = db::consistency_level::SERIAL;
    int32_t page_size = default_page_size;
    bytes_opt paging_state;
    std::optional<std::chrono::milliseconds> timeout;
    bool tracing = false;
};

query_request parse_query_request(std::string_view body) {
    static const auto request_schema = rjs::schema(rjs::object({
        {"query", rjs::scalar::string()},
        {"keyspace", rjs::optional(rjs::scalar::string())},
        {"consistency", rjs::optional(rjs::scalar::string())},
        {"serial_consistency", rjs::optional(rjs::scalar::string())},
        {"page_size", rjs::optional(rjs::scalar::integer())},
        {"paging_state", rjs::optional(rjs::scalar::string())},
        {"timeout_ms", rjs::optional(rjs::scalar::integer())},
        {"tracing", rjs::optional(rjs::scalar::boolean())},
    }));

    auto json = rjson::parse_and_validate(body, request_schema);
    if (!json) {
        throw webshell_error(reply::status_type::bad_request, seastar::format("Invalid request: {}", json.error()));
    }

    auto get = [&json] (const char* name) -> const rjson::value* {
        auto it = json->FindMember(name);
        return it == json->MemberEnd() || it->value.IsNull() ? nullptr : &it->value;
    };

    query_request req;
    req.query = rjson::to_sstring(*get("query"));
    if (auto v = get("keyspace")) {
        req.keyspace = rjson::to_sstring(*v);
    }
    if (auto v = get("consistency")) {
        req.consistency = parse_consistency_level(rjson::to_string_view(*v), consistency_levels(), "consistency");
    }
    if (auto v = get("serial_consistency")) {
        req.serial_consistency = parse_consistency_level(rjson::to_string_view(*v), serial_consistency_levels(), "serial_consistency");
    }
    if (auto v = get("page_size")) {
        // Paging cannot be turned off: a query without a LIMIT could produce a
        // response of unbounded size.
        req.page_size = v->GetInt();
        if (req.page_size < 1 || req.page_size > max_page_size) {
            throw webshell_error(reply::status_type::bad_request, seastar::format("Invalid page_size: {}, expected a value in the interval [1, {}]", req.page_size, max_page_size));
        }
    }
    if (auto v = get("paging_state")) {
        try {
            req.paging_state = base64_decode(rjson::to_string_view(*v));
        } catch (...) {
            throw webshell_error(reply::status_type::bad_request, "Invalid paging_state: not valid base64");
        }
    }
    if (auto v = get("timeout_ms")) {
        if (v->GetInt() < 1) {
            throw webshell_error(reply::status_type::bad_request, seastar::format("Invalid timeout_ms: {}, expected a positive value", v->GetInt()));
        }
        req.timeout = std::chrono::milliseconds(v->GetInt());
    }
    if (auto v = get("tracing")) {
        req.tracing = v->GetBool();
    }
    return req;
}

// The timeouts of the query: the configured ones, lowered to the requested
// timeout, if there is one. A request cannot raise the timeouts above the ones
// the operator configured.
timeout_config make_timeout_config(timeout_config cfg, std::optional<std::chrono::milliseconds> timeout) {
    if (!timeout) {
        return cfg;
    }
    const auto t = std::chrono::duration_cast<timeout_config::duration_t>(*timeout);
    for (auto member : {&timeout_config::read_timeout, &timeout_config::write_timeout, &timeout_config::range_read_timeout,
            &timeout_config::counter_write_timeout, &timeout_config::truncate_timeout, &timeout_config::cas_timeout, &timeout_config::other_timeout}) {
        cfg.*member = std::min(cfg.*member, t);
    }
    return cfg;
}

using result_message = cql_transport::messages::result_message;

// The result of a query, possibly owned by another shard than the one writing
// the response: a query can be bounced to another shard.
struct query_result {
    foreign_ptr<shared_ptr<result_message>> message;
    std::optional<utils::UUID> trace_session_id;
};

std::string_view to_string(cql_transport::event::schema_change::change_type change) {
    using change_type = cql_transport::event::schema_change::change_type;
    switch (change) {
        case change_type::CREATED: return "CREATED";
        case change_type::UPDATED: return "UPDATED";
        case change_type::DROPPED: return "DROPPED";
    }
    std::abort();
}

std::string_view to_string(cql_transport::event::schema_change::target_type target) {
    using target_type = cql_transport::event::schema_change::target_type;
    switch (target) {
        case target_type::KEYSPACE: return "KEYSPACE";
        case target_type::TABLE: return "TABLE";
        case target_type::TYPE: return "TYPE";
        case target_type::FUNCTION: return "FUNCTION";
        case target_type::AGGREGATE: return "AGGREGATE";
    }
    std::abort();
}

// What it takes to write the response of a query, in a form which can be passed
// to another shard: only plain values, nothing reference counted. A query can be
// bounced to another shard, which then owns its result message, made of shared
// pointers which can only be touched on that shard.
//
// The rows are not copied: they are plain and immutable memory, which the shard
// writing the response reads in place, while the result message, which owns
// them, is kept alive by its foreign_ptr.
struct result_summary {
    struct column {
        sstring keyspace;
        sstring table;
        sstring name;
        // The (marshal) name of the type, see abstract_type::name().
        sstring type;
    };
    struct schema_change_info {
        std::string_view change;
        std::string_view target;
        sstring keyspace;
        std::vector<sstring> arguments;
    };

    std::vector<sstring> warnings;
    // The keyspace USE switched to.
    std::optional<sstring> set_keyspace;
    std::optional<schema_change_info> schema_change;
    std::vector<column> columns;
    std::optional<sstring> paging_state;
    // Null for statements returning no rows.
    const cql3::result_set* result_set = nullptr;
};

// Has to run on the shard owning the result message.
result_summary summarize_query_result(const result_message& msg) {
    struct visitor : public result_message::visitor {
        const result_message::rows* rows = nullptr;
        const result_message::schema_change* schema_change = nullptr;
        const result_message::set_keyspace* set_keyspace = nullptr;

        virtual void visit(const result_message::void_message&) override { }
        virtual void visit(const result_message::set_keyspace& msg) override { set_keyspace = &msg; }
        virtual void visit(const result_message::prepared::cql&) override { throw std::runtime_error("unexpected prepared result message"); }
        virtual void visit(const result_message::schema_change& msg) override { schema_change = &msg; }
        virtual void visit(const result_message::rows& msg) override { rows = &msg; }
        virtual void visit(const result_message::bounce&) override { throw std::runtime_error("unexpected bounce result message"); }
        virtual void visit(const result_message::exception&) override { throw std::runtime_error("unexpected exception result message"); }
    } v;
    msg.accept(v);

    result_summary summary;
    summary.warnings = msg.warnings();
    if (v.set_keyspace) {
        summary.set_keyspace = v.set_keyspace->get_keyspace();
    }
    if (v.schema_change) {
        const auto& change = *v.schema_change->get_change();
        summary.schema_change.emplace(to_string(change.change), to_string(change.target), change.keyspace, change.arguments);
    }
    if (v.rows) {
        const auto& result = v.rows->rs();
        const auto& metadata = result.get_metadata();
        for (const auto& col : metadata.get_names()) {
            summary.columns.push_back({col->ks_name, col->cf_name, col->name->text(), col->type->name()});
        }
        if (metadata.flags().contains<cql3::metadata::flag::HAS_MORE_PAGES>()) {
            summary.paging_state = base64_encode(*metadata.paging_state()->serialize());
        }
        // Materializes the rows, if they are not materialized yet: from here on,
        // they are only read.
        summary.result_set = &result.result_set();
    }
    return summary;
}

// Write the response body of a successful query, a JSON object:
//
//   {
//     "trace_session_id": "<uuid>",               // if the query was traced
//     "warnings": ["<warning>", ...],             // if there are any
//     "set_keyspace": "<keyspace>",               // for USE
//     "schema_change": {...},                     // for schema altering statements
//     "column_types": [{"name": "<name>", "type": "<cql type>"}, ...], // for statements returning rows
//     "paging_state": "<base64>",                 // if there are more pages
//     "result_set": [{"<name>": <value>, ...}, ...] // for statements returning rows
//   }
//
// The JSON is written into a buffer, which is written to out after each row,
// so the response is streamed to the client, with back-pressure, rather than
// rendered whole first.
future<> write_query_result(output_stream<char>& out, const query_result& result) {
    const auto owner = result.message.get_owner_shard();
    const auto summary = owner == this_shard_id()
            ? summarize_query_result(*result.message)
            : co_await smp::submit_to(owner, [&result] { return summarize_query_result(*result.message); });

    std::ostringstream buf;
    rjson::streaming_writer writer(buf);
    auto flush = [&out, &buf] () -> future<> {
        const auto content = buf.view();
        co_await out.write(content.data(), content.size());
        buf.str("");
    };

    writer.StartObject();
    if (result.trace_session_id) {
        writer.Key("trace_session_id");
        writer.String(fmt::to_string(*result.trace_session_id));
    }
    if (!summary.warnings.empty()) {
        writer.Key("warnings");
        writer.StartArray();
        for (const auto& warning : summary.warnings) {
            writer.String(warning);
        }
        writer.EndArray();
    }
    if (summary.set_keyspace) {
        writer.Key("set_keyspace");
        writer.String(*summary.set_keyspace);
    }
    if (summary.schema_change) {
        const auto& change = *summary.schema_change;
        writer.Key("schema_change");
        writer.StartObject();
        writer.Key("change");
        writer.String(change.change);
        writer.Key("target");
        writer.String(change.target);
        writer.Key("keyspace");
        writer.String(change.keyspace);
        writer.Key("arguments");
        writer.StartArray();
        for (const auto& argument : change.arguments) {
            writer.String(argument);
        }
        writer.EndArray();
        writer.EndObject();
    }
    if (summary.result_set) {
        // The column specifications of the result are the owner shard's, so
        // they are rebuilt here, with types of this shard.
        const auto columns = summary.columns | std::views::transform([] (const result_summary::column& col) {
            return make_lw_shared<cql3::column_specification>(col.keyspace, col.table,
                    ::make_shared<cql3::column_identifier>(col.name, true), db::marshal::type_parser::parse(col.type));
        }) | std::ranges::to<std::vector>();

        writer.Key("column_types");
        writer.StartArray();
        for (const auto& col : columns) {
            writer.StartObject();
            writer.Key("name");
            writer.String(col->name->text());
            writer.Key("type");
            writer.String(col->type->cql3_type_name());
            writer.EndObject();
        }
        writer.EndArray();
        if (summary.paging_state) {
            writer.Key("paging_state");
            writer.String(*summary.paging_state);
        }
        writer.Key("result_set");
        writer.StartArray();
        for (const auto& row : summary.result_set->rows()) {
            cql3::print_query_result_row_json(writer, columns, row);
            co_await flush();
            co_await coroutine::maybe_yield();
        }
        writer.EndArray();
    }
    writer.EndObject();
    co_await flush();
}

tracing::trace_state_ptr setup_tracing(const query_request& req, const service::client_state& client_state) {
    if (!req.tracing) {
        return {};
    }

    tracing::trace_state_props_set trace_props;
    trace_props.set<tracing::trace_state_props::full_tracing>();

    auto trace_state = tracing::tracing::get_local_tracing_instance().create_session(tracing::trace_type::QUERY, trace_props);
    tracing::begin(trace_state, "Execute Web Shell query", client_state.get_client_address());
    tracing::set_common_query_parameters(trace_state, req.consistency, req.serial_consistency, api::missing_timestamp);
    tracing::set_page_size(trace_state, req.page_size);
    tracing::set_username(trace_state, client_state.user());
    tracing::add_query(trace_state, req.query);
    return trace_state;
}

future<shared_ptr<result_message>> execute_query_on_shard(cql3::query_processor& qp, const query_request& req,
        service::client_state& client_state, tracing::trace_state_ptr trace_state, cql3::computed_function_values cached_pk_fn_calls = {}) {
    auto query_state = service::query_state(client_state, trace_state, empty_service_permit());

    lw_shared_ptr<const service::pager::paging_state> paging_state;
    if (req.paging_state) {
        try {
            paging_state = service::pager::paging_state::deserialize(*req.paging_state);
        } catch (...) {
            throw webshell_error(reply::status_type::bad_request, seastar::format("Invalid paging_state: {}", std::current_exception()));
        }
    }

    const auto specific_options = cql3::query_options::specific_options{
            req.page_size,
            std::move(paging_state),
            req.serial_consistency,
            api::missing_timestamp,
            service::node_local_only::no};

    auto query_options = cql3::query_options{cql3::default_cql_config, req.consistency, std::nullopt, std::vector<cql3::raw_value_view>(), false, specific_options};
    if (!cached_pk_fn_calls.empty()) {
        query_options.set_cached_pk_function_calls(std::move(cached_pk_fn_calls));
    }

    auto result = co_await qp.execute_direct(req.query, query_state, {}, query_options);
    result->throw_if_exception();
    co_return std::move(result);
}

future<query_result> do_execute_query(sharded<cql3::query_processor>& qp, const gms::gossiper& gossiper, const query_request& req, service::client_state& client_state) {
    auto trace_state = setup_tracing(req, client_state);
    auto trace_session_id = trace_state ? std::make_optional(trace_state->session_id()) : std::nullopt;
    // The tracing session is written when its last reference goes away, which
    // can be much later than the end of the query: e.g. the querier cache keeps
    // the reader of a paged query, along with its reference, for the next page.
    // So like the CQL server, end the session explicitly, when the query is done.
    auto stop_tracing = defer([&trace_state] () noexcept {
        tracing::stop_foreground(trace_state);
    });

    tracing::trace(trace_state, "Executing Web Shell query in scheduling group {}", current_scheduling_group().name());

    auto res = co_await execute_query_on_shard(qp.local(), req, client_state, trace_state);

    if (!res->as_bounce()) {
        co_return query_result{make_foreign(std::move(res)), trace_session_id};
    }

    auto bounce = dynamic_pointer_cast<result_message::bounce>(res);
    // The query can also be bounced to another node: strongly consistent
    // tables execute queries on the node hosting the tablet's leader. The CQL
    // transport forwards such queries over RPC, the Web Shell doesn't.
    if (const auto host = bounce->target_host(); host != qp.local().proxy().get_token_metadata_ptr()->get_topology().my_host_id()) {
        const auto address = gossiper.get_address_map().find(host);
        throw webshell_error(reply::status_type::not_implemented,
                seastar::format("Query has to be executed on node {} ({}), but the Web Shell cannot forward queries to other nodes;"
                        " use the Web Shell of that node, or a CQL driver", host, address ? fmt::to_string(*address) : std::string("unknown address")),
                target_node{host, address});
    }

    const auto shard = bounce->target_shard();
    // The partition key may have been computed by functions (e.g. uuid()),
    // their values have to be the same on the target shard.
    auto cached_pk_fn_calls = bounce->take_cached_pk_function_calls();
    auto gcs = client_state.move_to_other_shard();
    auto gts = tracing::global_trace_state_ptr(trace_state);

    tracing::trace(trace_state, "Query bounced to shard {}", shard);

    auto bounced_res = co_await qp.invoke_on(shard, [&gcs, &gts, &req, &cached_pk_fn_calls] (cql3::query_processor& qp) -> future<foreign_ptr<shared_ptr<result_message>>> {
        auto client_state = gcs.get();
        auto res = co_await execute_query_on_shard(qp, req, client_state, gts.get(), std::move(cached_pk_fn_calls));
        if (auto bounce = res->as_bounce(); bounce) {
            throw std::runtime_error(seastar::format("Unexpected bounce to {}:{}, after handling a bounce", bounce->target_host(), bounce->target_shard()));
        }
        co_return make_foreign(std::move(res));
    });

    co_return query_result{std::move(bounced_res), trace_session_id};
}

// Execute the query, translating the failures to HTTP status codes.
future<query_result> execute_query(sharded<cql3::query_processor>& qp, const gms::gossiper& gossiper, const query_request& req, service::client_state& client_state) {
    try {
        if (req.keyspace) {
            client_state.set_keyspace(qp.local().db().real_database(), *req.keyspace);
        }
        co_return co_await do_execute_query(qp, gossiper, req, client_state);
    } catch (exceptions::unauthorized_exception& e) {
        // The user is authenticated, but doesn't have permission to execute the query.
        throw webshell_error(reply::status_type::forbidden, e.get_message());
    } catch (exceptions::syntax_exception& e) {
        throw webshell_error(reply::status_type::bad_request, seastar::format("Syntax error: {}", e.get_message()));
    } catch (exceptions::request_validation_exception& e) {
        throw webshell_error(reply::status_type::bad_request, e.get_message());
    } catch (exceptions::request_timeout_exception& e) {
        throw webshell_error(reply::status_type::gateway_timeout, e.get_message());
    } catch (exceptions::unavailable_exception& e) {
        throw webshell_error(reply::status_type::service_unavailable, e.get_message());
    } catch (exceptions::overloaded_exception& e) {
        throw webshell_error(reply::status_type::too_many_requests, e.get_message());
    } catch (exceptions::rate_limit_exception& e) {
        throw webshell_error(reply::status_type::too_many_requests, e.get_message());
    } catch (exceptions::cassandra_exception& e) {
        // Any other failure to execute the query, e.g. a read or write
        // failure on the replicas: report it as such, with its message.
        throw webshell_error(reply::status_type::internal_server_error, e.get_message());
    }
}

// Reject requests made by pages of other origins.
//
// Browsers don't let a page of another origin read the responses, nor send
// requests with an Authorization header or a JSON body without asking first
// (a CORS preflight request, which the server doesn't answer). But a "simple"
// request, e.g. one with a text/plain body, is sent without asking. Such a
// request is authenticated if the cluster allows anonymous access, or if the
// browser presents a client certificate on its own, so a malicious page could
// execute queries with the permissions of the user visiting it.
//
// Browsers always send the Origin header with cross-origin POST requests, so
// these are recognized by their Origin not matching the scheme and Host of the
// request. Other clients,
// like curl, don't send Origin, they are not affected.
void check_origin(const request& req) {
    const auto origin = req.get_header("Origin");
    if (origin.empty()) {
        return;
    }
    // The scheme is part of the origin: an http:// page is another origin
    // than the https:// one of the same host.
    if (origin != seastar::format("{}://{}", req.get_protocol_name(), req.get_header("Host"))) {
        throw webshell_error(reply::status_type::forbidden, seastar::format("Cross-origin requests are not allowed, origin: {}", origin));
    }
}

/// Executes a single CQL query.
///
/// Each request is authenticated on its own (see authenticate()) and carries
/// all the options of the query (see parse_query_request()).
class query_handler : public gated_handler {
    server& _server;

public:
    explicit query_handler(request_control& request_control, server& server)
        : gated_handler("query", request_control), _server(server)
    { }

protected:
    virtual future<std::unique_ptr<reply>> do_handle(const sstring& path, std::unique_ptr<request> req, std::unique_ptr<reply> rep, request_permit permit) override {
        check_origin(*req);

        permit.request().stage = client_connection_stage::authenticating;
        auto user = co_await authenticate(_server.auth_service(), *req);

        const auto request = parse_query_request(co_await read_request_body(*req->content_stream));

        auto client_state = service::client_state(
                service::client_state::external_tag{},
                _server.auth_service(),
                &_server.sl_controller(),
                make_timeout_config(_server.timeout_config().current_values(), request.timeout),
                req->get_client_address());

        if (user) {
            client_state.set_login(std::move(*user));
            try {
                co_await client_state.check_user_can_login();
            } catch (exceptions::authentication_exception& e) {
                throw webshell_error(reply::status_type::unauthorized, e.what());
            }
            client_state.maybe_update_per_service_level_params();
        }

        // Run the rest of the request, including writing the response, in the
        // scheduling group of the user's service level.
        const auto sg = _server.sl_controller().get_cached_user_scheduling_group(client_state.user());
        co_await coroutine::switch_to(sg);

        auto& ongoing = permit.request();
        ongoing.stage = client_connection_stage::ready;
        ongoing.username = client_state.user() ? client_state.user()->name : std::nullopt;
        ongoing.sg = sg;

        auto result = co_await execute_query(_server.qp().container(), _server.gossiper(), request, client_state);

        rep->set_status(reply::status_type::ok);
        // The permit is held until the body is written: rendering a large result
        // is a significant part of the work of a query request.
        //
        // The body is written by the HTTP server, after the handler returned, in
        // the scheduling group of the server. Rendering it is part of processing
        // the request, so it is switched to the scheduling group of the user.
        rep->write_body("json", [result = std::move(result), permit = std::move(permit), sg] (output_stream<char>& out) mutable -> future<> {
            co_await coroutine::switch_to(sg);
            try {
                co_await write_query_result(out, result);
            } catch (...) {
                // The status was already sent, the client will get a truncated
                // response, which fails to parse as JSON.
                wslog.error("Unexpected exception while writing query response: {}", std::current_exception());
                throw;
            }
        });

        co_return std::move(rep);
    }
};

} // anonymous namespace

server::server(cql3::query_processor& qp, auth::service& auth_service, qos::service_level_controller& sl_controller,
        updateable_timeout_config& timeout_config, gms::gossiper& gossiper)
    : _qp(qp)
    , _auth_service(auth_service)
    , _sl_controller(sl_controller)
    , _timeout_config(timeout_config)
    , _gossiper(gossiper)
    , _http_server("webshell-http")
    , _https_server("webshell-https")
    , _request_control("webshell")
{
}

void server::set_routes(routes& r) {
    r.add_default_handler(new not_found_handler(_request_control));
    r.put(operation_type::POST, "/query", new query_handler(_request_control, *this));
    r.put(operation_type::GET, "/query", new method_not_allowed_handler(_request_control, "POST"));

    r.register_exeption_handler([] (std::exception_ptr ex) {
        try {
            std::rethrow_exception(ex);
        } catch (webshell_error& e) {
            return make_error_reply(e.status(), e.str(), e.target());
        } catch (base_exception& e) {
            return make_error_reply(e.status(), e.str());
        } catch (...) {
            wslog.warn("Unexpected error while handling request: {}", std::current_exception());
            return make_error_reply(reply::status_type::internal_server_error, fmt::to_string(std::current_exception()));
        }
    });
}

future<> server::init() {
    for (auto* srv : {&_http_server, &_https_server}) {
        set_routes(srv->_routes);
        srv->set_content_length_limit(max_request_body_size);
        srv->set_request_size_limit(max_request_line_and_headers_size);
        srv->set_content_streaming(true);
    }
    return make_ready_future<>();
}

// Build credentials out of `builder`, and keep them at the end of _credentials.
// Only shard 0 watches the certificate files; on a change it rebuilds the other
// shards' copies by index, which is why _credentials is only ever appended to,
// in the same order everywhere.
future<> server::add_credentials(tls::credentials_builder& builder) {
    const size_t i = _credentials.size();
    if (this_shard_id() != 0) {
        _credentials.push_back(builder.build_server_credentials());
        co_return;
    }
    _credentials.push_back(co_await builder.build_reloadable_server_credentials([this, i] (const tls::credentials_builder& b, const std::unordered_set<sstring>& files, std::exception_ptr ep) -> future<> {
        // A reload can fire while the server is being shut down, and there is
        // nothing left to reload into then. Holding the gate keeps this server
        // and its peers on the other shards alive for the duration of the
        // reload - sharded<> destroys the instances only once stop() completed
        // on all of them, and stop() waits for the gate.
        auto holder = _credentials_reload_gate.try_hold();
        if (!holder) {
            co_return;
        }
        if (ep) {
            wslog.warn("Exception loading {}: {}", files, ep);
        } else {
            co_await container().invoke_on_others([&b, i] (server& s) {
                if (i < s._credentials.size() && s._credentials[i]) {
                    b.rebuild(*s._credentials[i]);
                }
            });
            wslog.info("Reloaded {}", files);
        }
    }));
}

future<> server::serve(const listener& l) {
    listen_options lo;
    lo.reuse_address = true;
    lo.proxy_protocol = l.proxy_protocol;

    // The abort_source is what ends this listener, so it has to outlive the
    // call and keep its address: the future below holds a reference to it.
    auto stop = std::make_unique<abort_source>();
    future<> served = make_ready_future<>();
    // Bound before serving, so that a failure to bind reaches the caller here
    // rather than in the future of the listener's whole life.
    if (l.credentials_idx) {
        auto bound = co_await _https_server.bind(l.addr, lo, _credentials[*l.credentials_idx]);
        served = bound.serve(*stop);
    } else {
        auto bound = co_await _http_server.bind(l.addr, lo);
        served = bound.serve(*stop);
    }
    _listeners.emplace(l.addr, serving_listener{l, std::move(stop), std::move(served)});
}

future<> server::stop_serving(serving_listener& listener) {
    listener.stop->request_abort();
    co_await std::move(listener.served);
}

future<std::vector<server::listener_failure>> server::apply_listeners(std::vector<listener> listeners,
        std::vector<tls::credentials_builder> credentials,
        std::vector<std::unordered_map<sstring, sstring>> credentials_options) {
    // Translate the caller's credentials indexes into this server's. A set of
    // options it is already serving with keeps the credentials it has, so a
    // listener that did not change is not disturbed; anything new is appended,
    // never reindexed.
    std::unordered_map<size_t, size_t> translated;
    for (size_t i = 0; i != credentials_options.size(); ++i) {
        auto existing = std::ranges::find(_credentials_options, credentials_options[i]);
        if (existing != _credentials_options.end()) {
            translated[i] = existing - _credentials_options.begin();
            continue;
        }
        co_await add_credentials(credentials[i]);
        _credentials_options.push_back(credentials_options[i]);
        translated[i] = _credentials.size() - 1;
    }

    std::unordered_map<socket_address, listener> wanted;
    for (auto& l : listeners) {
        if (l.credentials_idx) {
            l.credentials_idx = translated.at(*l.credentials_idx);
        }
        wanted.emplace(l.addr, l);
    }

    auto same = [] (const listener& a, const listener& b) {
        return a.proxy_protocol == b.proxy_protocol && a.credentials_idx == b.credentials_idx;
    };

    // Stop the listeners that are gone or have changed.
    for (auto& [addr, l] : _listeners) {
        auto it = wanted.find(addr);
        if (it != wanted.end() && same(it->second, l.config)) {
            continue;
        }
        co_await stop_serving(l);
    }
    std::erase_if(_listeners, [&] (const auto& e) {
        auto it = wanted.find(e.first);
        return it == wanted.end() || !same(it->second, e.second.config);
    });

    // Whether a socket can be bound is only known by binding it, so a listener
    // that cannot be opened is reported, and the others are opened regardless.
    std::vector<listener_failure> failures;
    for (const auto& [addr, l] : wanted) {
        if (_listeners.contains(addr)) {
            continue;
        }
        try {
            co_await serve(l);
        } catch (...) {
            failures.push_back(listener_failure{addr, std::current_exception()});
        }
    }
    co_return failures;
}

future<> server::stop_listening(const std::vector<socket_address>& addrs) {
    for (const auto& addr : addrs) {
        auto it = _listeners.find(addr);
        if (it == _listeners.end()) {
            continue;
        }
        co_await stop_serving(it->second);
        _listeners.erase(it);
    }
}

future<> server::stop() {
    co_await _credentials_reload_gate.close();
    co_await _http_server.stop();
    co_await _https_server.stop();
    // Stopping the servers ends every listener they hold, so these are
    // resolved by now; take them rather than leave a future unwaited.
    for (auto& [addr, listener] : _listeners) {
        co_await std::move(listener.served);
    }
    _listeners.clear();
    co_await _request_control.stop();
}

controller::controller(sharded<cql3::query_processor>& qp, sharded<auth::service>& auth_service,
        sharded<qos::service_level_controller>& sl_controller, sharded<updateable_timeout_config>& timeout_config,
        sharded<gms::gossiper>& gossiper, const db::config& config, db::listener_configs listeners, scheduling_group sg)
    : protocol_server(sg)
    , _qp(qp)
    , _auth_service(auth_service)
    , _sl_controller(sl_controller)
    , _timeout_config(timeout_config)
    , _gossiper(gossiper)
    , _config(config)
    , _listeners(std::move(listeners))
{
}

sstring controller::name() const {
    return "webshell";
}

sstring controller::protocol() const {
    return "webshell";
}

sstring controller::protocol_version() const {
    return "1.0";
}

std::vector<socket_address> controller::listen_addresses() const {
    return _listen_addresses;
}

future<std::vector<socket_address>> controller::reconcile(const db::listener_configs& listeners) {
    auto preferred = _config.listen_interface_prefer_ipv6() ? std::make_optional(net::inet_address::family::INET6) : std::nullopt;
    auto family = _config.enable_ipv6_dns_lookup() || preferred ? std::nullopt : std::make_optional(net::inet_address::family::INET);

    // Work out what it takes to serve the listeners: the address of each
    // resolved, and one set of credentials per distinct set of TLS options.
    // Everything that can fail happens here, so a set that cannot be served is
    // rejected before any socket is touched.
    std::vector<server::listener> resolved;
    std::vector<seastar::tls::credentials_builder> credentials;
    std::vector<std::unordered_map<sstring, sstring>> tls_options;
    for (const auto& [name, l] : listeners) {
        const auto addr = co_await utils::resolve_listen_address(l.address.empty() ? sstring("localhost") : l.address,
                "Web Shell listener address", family, preferred);

        std::optional<size_t> credentials_idx;
        if (l.tls) {
            auto opts = l.tls_options;
            if (opts.empty()) {
                opts = _config.client_encryption_options();
                // The legacy on/off switch of client_encryption_options, which
                // the listener's own "tls" key replaces.
                opts.erase("enabled");
            }
            // TLS session resumption is not needed by the Web Shell's clients,
            // so the session tickets are off, unless asked for.
            opts.try_emplace("enable_session_tickets", "false");
            auto it = std::ranges::find(tls_options, opts);
            credentials_idx = it - tls_options.begin();
            if (it == tls_options.end()) {
                tls_options.push_back(opts);
                co_await utils::configure_tls_creds_builder(credentials.emplace_back(), std::move(opts));
            }
        }

        resolved.push_back(server::listener{
            .addr = socket_address{addr, l.port},
            .proxy_protocol = l.proxy_protocol,
            .credentials_idx = credentials_idx,
        });
    }

    auto per_shard = co_await _server.map([&] (server& s) {
        return s.apply_listeners(resolved, credentials, tls_options);
    });

    std::vector<socket_address> failed;
    for (const auto& failures : per_shard) {
        for (const auto& f : failures) {
            if (std::ranges::find(failed, f.addr) == failed.end()) {
                wslog.error("Failed to listen for Web Shell clients on {}: {}", f.addr, f.error);
                failed.push_back(f.addr);
            }
        }
    }
    if (!failed.empty()) {
        // A listener is served on every shard or on none: close it on the
        // shards that did manage to open it.
        co_await _server.invoke_on_all([&failed] (server& s) {
            return s.stop_listening(failed);
        });
    }

    _listen_addresses.clear();
    for (const auto& l : resolved) {
        if (std::ranges::find(failed, l.addr) == failed.end()) {
            _listen_addresses.push_back(l.addr);
        }
    }
    co_return failed;
}

future<> controller::apply_listeners(db::listener_configs listeners) {
    auto units = co_await get_units(_ops_sem, 1);
    co_await coroutine::switch_to(_sched_group);

    if (!_serving) {
        // Nothing is being served, so there is nothing to reconcile: remember
        // the set. If the server is meant to run, it was only waiting for
        // something to listen on, so start it now.
        _listeners = std::move(listeners);
        if (_wanted) {
            co_await do_start_server();
        }
        co_return;
    }

    if (listeners.empty()) {
        wslog.info("No Web Shell listeners are left, stopping the Web Shell server");
        _listeners = std::move(listeners);
        co_await do_stop_server();
        co_return;
    }

    // Remembered only once reconcile() accepted them: a set which cannot be
    // served (e.g. an address which cannot be resolved) is rejected before
    // any socket is touched, and the previous set stays in effect.
    auto failed = co_await reconcile(listeners);
    _listeners = std::move(listeners);
    if (_listen_addresses.empty()) {
        wslog.error("None of the Web Shell listeners could be opened, stopping the Web Shell server");
        co_await do_stop_server();
        co_return;
    }
    if (!failed.empty()) {
        wslog.error("Could not open the Web Shell listeners on {}; serving the others", failed);
    }
    wslog.info("Web Shell server listening on {}", _listen_addresses);
}

future<> controller::start_server() {
    auto units = co_await get_units(_ops_sem, 1);
    co_await coroutine::switch_to(_sched_group);
    _wanted = true;
    co_await do_start_server();
}

// Start the server, if it is not running yet and there is something to listen
// on. Best effort: a listener that cannot be opened is reported and left out,
// as the Web Shell is not essential for the node to function.
future<> controller::do_start_server() {
    if (_serving) {
        co_return;
    }
    if (_listeners.empty()) {
        wslog.debug("No Web Shell listeners are configured, the Web Shell server will start when one is");
        co_return;
    }

    co_await _server.start(std::ref(_qp), std::ref(_auth_service), std::ref(_sl_controller), std::ref(_timeout_config), std::ref(_gossiper));
    _serving = true;

    std::exception_ptr ex;
    try {
        co_await _server.invoke_on_all(&server::init);
        auto failed = co_await reconcile(_listeners);
        if (_listen_addresses.empty()) {
            throw std::runtime_error(fmt::format("Could not listen for Web Shell clients on {}", failed));
        }
        if (!failed.empty()) {
            wslog.error("Could not open the Web Shell listeners on {}; serving the others", failed);
        }
    } catch (...) {
        ex = std::current_exception();
    }

    if (ex) {
        wslog.error("Failed to start the Web Shell server: {}", ex);
        co_await do_stop_server();
        co_return;
    }

    wslog.info("Web Shell server listening on {}", _listen_addresses);
}

future<utils::chunked_vector<foreign_ptr<std::unique_ptr<client_data>>>> controller::get_client_data() {
    // The server is not started while there is nothing for it to listen on.
    return _serving ? _server.local().get_client_data() : protocol_server::get_client_data();
}

future<> controller::stop_server() {
    auto units = co_await get_units(_ops_sem, 1);
    _wanted = false;
    co_await do_stop_server();
}

future<> controller::do_stop_server() {
    if (!std::exchange(_serving, false)) {
        co_return;
    }
    co_await _server.stop();
    _listen_addresses.clear();
}

future<> controller::request_stop_server() {
    return with_scheduling_group(_sched_group, [this] {
        return stop_server();
    });
}

} // namespace tools::webshell
