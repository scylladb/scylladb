/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <ranges>

#include <seastar/core/seastar.hh>
#include <seastar/core/units.hh>
#include <seastar/coroutine/switch_to.hh>
#include <seastar/http/exception.hh>
#include <seastar/http/httpd.hh>
#include <seastar/util/log.hh>

#include "auth/service.hh"
#include "client_data.hh"
#include "cql3/query_processor.hh"
#include "db/config.hh"
#include "service/qos/service_level_controller.hh"
#include "service/storage_proxy.hh"
#include "tools/webshell/webshell.hh"
#include "utils/rjson.hh"
#include "utils/scoped_item_list.hh"

using namespace httpd;
using request = http::request;
using reply = http::reply;

namespace tools::webshell {

static logger wslog("webshell");

namespace {

// The largest request body accepted. The body holds a single query, along with
// its options, so this is a generous bound. It is also the content length limit
// of the HTTP server, but that is enforced only for requests that declare a
// content length.
constexpr size_t max_request_body_size = 1*MB;
// The largest request line and headers accepted, which the HTTP server buffers
// in memory before the handler runs.
constexpr size_t max_request_line_and_headers_size = 16*KB;

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
std::unique_ptr<reply> make_error_reply(reply::status_type status, std::string_view message) {
    auto rep = std::make_unique<reply>();
    rep->set_status(status);
    if (status == reply::status_type::unauthorized) {
        rep->add_header("WWW-Authenticate", "ScyllaDB-Webshell realm=\"ScyllaDB Web Shell\"");
    }
    if (status == reply::status_type::too_many_requests) {
        rep->add_header("Retry-After", "1");
    }
    rep->write_body("json", seastar::format("{{\"error\":{}}}", rjson::quote_json_string(message)));
    return rep;
}

// Thrown to fail a request with the given status and message, which become the
// status code and the "error" member of the response.
class webshell_error : public base_exception {
public:
    webshell_error(reply::status_type status, const std::string& message)
        : base_exception(message, status)
    { }
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
            updateable_timeout_config& timeout_config);

    cql3::query_processor& qp() { return _qp; }
    auth::service& auth_service() { return _auth_service; }
    qos::service_level_controller& sl_controller() { return _sl_controller; }
    const updateable_timeout_config& timeout_config() const { return _timeout_config; }

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

server::server(cql3::query_processor& qp, auth::service& auth_service, qos::service_level_controller& sl_controller,
        updateable_timeout_config& timeout_config)
    : _qp(qp)
    , _auth_service(auth_service)
    , _sl_controller(sl_controller)
    , _timeout_config(timeout_config)
    , _http_server("webshell-http")
    , _https_server("webshell-https")
    , _request_control("webshell")
{
}

void server::set_routes(routes& r) {
    r.add_default_handler(new not_found_handler(_request_control));

    r.register_exeption_handler([] (std::exception_ptr ex) {
        try {
            std::rethrow_exception(ex);
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
        const db::config& config, db::listener_configs listeners, scheduling_group sg)
    : protocol_server(sg)
    , _qp(qp)
    , _auth_service(auth_service)
    , _sl_controller(sl_controller)
    , _timeout_config(timeout_config)
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

    co_await _server.start(std::ref(_qp), std::ref(_auth_service), std::ref(_sl_controller), std::ref(_timeout_config));
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
