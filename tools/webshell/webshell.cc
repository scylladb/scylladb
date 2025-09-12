/*
 * Copyright (C) 2025-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <random>
#include <ranges>
#include <seastar/core/units.hh>
#include <seastar/coroutine/switch_to.hh>
#include <seastar/http/httpd.hh>
#include <seastar/util/log.hh>

#include "cql3/query_processor.hh"
#include "db/config.hh"
#include "service/client_state.hh"
#include "tools/webshell/webshell.hh"

using namespace httpd;
using request = http::request;
using reply = http::reply;

using namespace tools::webshell;

namespace tools::webshell {

static logger wslog("webshell");

struct config {
    sstring cluster_name;
    scheduling_group scheduling_group;
    ::updateable_timeout_config timeout_config;
    db_clock::duration session_ttl = std::chrono::minutes(10);
    uint64_t max_sessions = 32;
    uint64_t max_concurrent_requests = 16;
    uint64_t max_waiting_requests = 16;
};

class session_id {
    uint64_t _msb;
    uint64_t _lsb;
    uint32_t _shard;
public:
    explicit session_id(uint64_t msb, uint64_t lsb, uint32_t shard)
        : _msb(msb), _lsb(lsb), _shard(shard)
    { }

    session_id(std::string_view id_str) {
        // Parse session_id from string, format is "msb-lsb-shard"
        auto parts = std::views::split(id_str, '-') | std::ranges::to<std::vector<sstring>>();
        if (parts.size() != 3) {
            throw std::invalid_argument("invalid session_id");
        }
        _msb = std::stoull(parts[0].data(), nullptr, 16);
        _lsb = std::stoull(parts[1].data(), nullptr, 16);
        _shard = std::stoul(parts[2].data());
    }

    session_id(const session_id&) = default;
    session_id& operator=(const session_id&) = default;

    bool operator==(const session_id&) const = default;
    bool operator!=(const session_id&) const = default;

    // Generate random session_id, assigned to this shard
    static session_id gen()
    {
        //TODO: good start, but probably need more entropy here (all 128 bits of the session_id)
        std::mt19937_64 engine(std::random_device{}());
        const auto lsb = engine();
        const auto msb = engine();
        return session_id{msb, lsb, this_shard_id()};
    }

    uint64_t msb() const {
        return _msb;
    }

    uint64_t lsb() const {
        return _lsb;
    }

    uint32_t shard() const {
        return _shard;
    }
};

} // namespace tools::webshell

template <>
struct fmt::formatter<tools::webshell::session_id> : fmt::formatter<string_view> {
    auto format(tools::webshell::session_id sid, fmt::format_context& ctx) const -> decltype(ctx.out()) {
        return format_to(ctx.out(), "{:016x}-{:016x}-{:08x}", sid.msb(), sid.lsb(), sid.shard());
    }
};

namespace std {

template <>
struct hash<tools::webshell::session_id> {
    size_t operator()(const tools::webshell::session_id& sid) const noexcept {
        return std::hash<uint64_t>()(sid.msb()) ^ std::hash<uint64_t>()(sid.lsb()) ^ std::hash<uint32_t>()(sid.shard());
    }
};

} // namespace std

namespace tools::webshell {

struct session_data {
    const session_id id;

    explicit session_data(session_id id)
        : id(std::move(id))
    { }
};

class session {
public:
    session_data data;
    service::client_state client_state;

    scheduling_group scheduling_group;
    sstring user_agent;
    bool is_https;

    session(session_id session_id, service::client_state client_state, ::scheduling_group sg, sstring user_agent, bool is_https)
        : data(std::move(session_id))
        , client_state(std::move(client_state))
        , scheduling_group(sg)
        , user_agent(std::move(user_agent))
        , is_https(is_https)
    { }

    session_id id() const noexcept {
        return data.id;
    }

    sstring auth_user() const {
        return client_state.user().value().name.value_or("anonymous");
    }
};

class session_manager {
    const config& _cfg;
    cql3::query_processor& _qp;
    auth::service& _auth_service;
    qos::service_level_controller& _sl_controller;

    std::function<session_manager&()> _get_local_manager;

    std::unordered_map<session_id, lw_shared_ptr<session>> _sessions;

public:
    session_manager(const config& cfg, cql3::query_processor& qp, auth::service& auth_service, qos::service_level_controller& sl_controller,
            std::function<session_manager&()> get_local_manager)
        : _cfg(cfg), _qp(qp), _auth_service(auth_service), _sl_controller(sl_controller), _get_local_manager(std::move(get_local_manager))
    { }

    const config& config() const {
        return _cfg;
    }

    cql3::query_processor& qp() {
        return _qp;
    }

    auth::service& auth_service() {
        return _auth_service;
    }

    qos::service_level_controller& sl_controller() {
        return _sl_controller;
    }

    utils::chunked_vector<client_data> get_client_data() {
        utils::chunked_vector<client_data> ret;

        for (const auto& [session_id, session] : _sessions) {
            ret.emplace_back(client_data{
                // ip/port is the one that was seen at login, it may change later.
                // TODO: return last seen ip/port instead
                .ip = session->client_state.get_client_address(),
                .port = session->client_state.get_client_port(),
                .ct = client_type::webshell,
                .connection_stage = client_connection_stage::ready,
                .shard_id = this_shard_id(),
                // Use the User-Agent header as the driver name, leave driver version unset
                .driver_name = session->user_agent,
                .ssl_enabled = session->is_https,
                .username = session->auth_user(),
                .scheduling_group_name = session->scheduling_group.name(),
                // Leave "protocol_version" unset, it has no meaning in Webshell.
                // Leave "hostname", "ssl_protocol" and "ssl_cipher_suite" unset.
                // As reported in issue #9216, we never set these fields in CQL
                // either (see cql_server::connection::make_client_data()).
            });
        }

        return ret;
    }
};

class request_control {
    named_gate _gate;
    named_semaphore _semaphore;
    uint64_t _max_waiters;

public:
    request_control(sstring name, uint64_t max_concurrent, uint64_t max_waiters)
        : _gate(name)
        , _semaphore(max_concurrent, named_semaphore_exception_factory{.name = name})
        , _max_waiters(max_waiters)
    {}

    bool too_many_waiters() const {
        return _semaphore.waiters() > _max_waiters;
    }

    auto run(auto func) {
        return with_gate(_gate, [this, func = std::move(func)] () mutable {
            return with_semaphore(_semaphore, 1, std::move(func));
        });
    }

    future<> stop() noexcept {
        _semaphore.broken();
        return _gate.close();
    }
};

struct http_listen_config {
    net::inet_address address;
    uint16_t port;
};

struct https_listen_config {
    net::inet_address address;
    uint16_t port;
    seastar::tls::credentials_builder creds;
};

class server : public peering_sharded_service<server> {
    static constexpr size_t content_length_limit = 16*MB;

private:
    config _cfg;
    httpd::http_server _http_server;
    httpd::http_server _https_server;
    ::shared_ptr<seastar::tls::server_credentials> _credentials;

    utils::small_vector<std::reference_wrapper<seastar::httpd::http_server>, 2> _enabled_servers;

    request_control _request_control;
    session_manager _session_manager;

private:
    void set_routes(seastar::httpd::routes& r, bool is_https);

public:
    server(config cfg, cql3::query_processor& qp, auth::service& auth_service, qos::service_level_controller& sl_controller);

    future<> init(std::optional<http_listen_config> http_cfg_opt, std::optional<https_listen_config> https_cfg_opt);
    future<> stop();

    future<utils::chunked_vector<client_data>> get_client_data();
};

void server::set_routes(routes& r, bool is_https) {
}

server::server(config cfg, cql3::query_processor& qp, auth::service& auth_service, qos::service_level_controller& sl_controller)
    : _cfg(cfg)
    , _http_server("scylladb-webshell-http")
    , _https_server("scylladb-webshell-https")
    , _request_control("webshell", _cfg.max_concurrent_requests, _cfg.max_waiting_requests)
    , _session_manager(_cfg, qp, auth_service, sl_controller, [this] () -> session_manager& {
        return container().local()._session_manager;
    })
{
}

future<> server::init(std::optional<http_listen_config> http_cfg_opt, std::optional<https_listen_config> https_cfg_opt) {
    co_await coroutine::switch_to(_cfg.scheduling_group);

    _enabled_servers.clear();

    if (http_cfg_opt) {
        set_routes(_http_server._routes, false);
        _http_server.set_content_length_limit(server::content_length_limit);
        _http_server.set_content_streaming(true);
        co_await _http_server.listen(socket_address{http_cfg_opt->address, http_cfg_opt->port});
        _enabled_servers.push_back(_http_server);
    }

    if (https_cfg_opt) {
        set_routes(_https_server._routes, true);
        _https_server.set_content_length_limit(server::content_length_limit);
        _https_server.set_content_streaming(true);

        if (this_shard_id() == 0) {
            _credentials = co_await https_cfg_opt->creds.build_reloadable_server_credentials([this](const tls::credentials_builder& b, const std::unordered_set<sstring>& files, std::exception_ptr ep) -> future<> {
                if (ep) {
                    wslog.warn("Exception loading {}: {}", files, ep);
                } else {
                    co_await container().invoke_on_others([&b](server& s) {
                        if (s._credentials) {
                            b.rebuild(*s._credentials);
                        }
                    });
                    wslog.info("Reloaded {}", files);
                }
            });
        } else {
            _credentials = https_cfg_opt->creds.build_server_credentials();
        }

        co_await _https_server.listen(socket_address{https_cfg_opt->address, https_cfg_opt->port}, _credentials);

        _enabled_servers.push_back(_https_server);
    }
}

future<> server::stop() {
    co_await parallel_for_each(_enabled_servers, [] (http_server& server) {
        return server.stop();
    });
    co_await _request_control.stop();
}

future<utils::chunked_vector<client_data>> server::get_client_data() {
    co_return _session_manager.get_client_data();
}

controller::controller(sharded<cql3::query_processor>& qp, sharded<auth::service>& auth_service, sharded<qos::service_level_controller>& sl_controller,
    const db::config& config, sstring cluster_name, seastar::scheduling_group sg)
    : protocol_server(sg)
    , _qp(qp)
    , _auth_service(auth_service)
    , _sl_controller(sl_controller)
    , _config(config)
    , _cluster_name(std::move(cluster_name))
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

future<> controller::start_server() {
    co_await coroutine::switch_to(_sched_group);

    utils::small_vector<sstring, 2> uris;

    _listen_addresses.clear();

    tools::webshell::config ws_cfg{
        .cluster_name = _cluster_name,
        .scheduling_group = _sched_group,
        .timeout_config = updateable_timeout_config(_config)
    };
    co_await _server.start(ws_cfg, std::ref(_qp), std::ref(_auth_service), std::ref(_sl_controller));

    auto preferred = _config.listen_interface_prefer_ipv6() ? std::make_optional(net::inet_address::family::INET6) : std::nullopt;
    auto family = _config.enable_ipv6_dns_lookup() || preferred ? std::nullopt : std::make_optional(net::inet_address::family::INET);

    std::optional<tools::webshell::http_listen_config> http_cfg_opt;
    if (_config.webshell_http_port()) {
        http_cfg_opt.emplace(tools::webshell::http_listen_config{
                .address = co_await utils::resolve(_config.webshell_http_address, family),
                .port = _config.webshell_http_port()});
        _listen_addresses.push_back({http_cfg_opt->address, http_cfg_opt->port});

        uris.push_back(format("http://{}:{}", _config.webshell_http_address(), http_cfg_opt->port));
    }

    std::optional<tools::webshell::https_listen_config> https_cfg_opt;
    if (_config.webshell_https_port()) {
        tls::credentials_builder creds;

        std::exception_ptr ex;
        try {
            co_await utils::configure_tls_creds_builder(creds, _config.webshell_https_encryption_options());
        } catch(...) {
            ex = std::current_exception();
        }
        if (ex) {
            wslog.error("Failed to set up Web Shell TLS credentials: {}", ex);
            co_await stop_server();
            throw std::runtime_error("Failed to set up Web Shell TLS credentials");
        }

        https_cfg_opt.emplace(tools::webshell::https_listen_config{
                .address = co_await utils::resolve(_config.webshell_https_address, family),
                .port = _config.webshell_https_port(),
                .creds = std::move(creds)});

        _listen_addresses.push_back({https_cfg_opt->address, https_cfg_opt->port});

        uris.push_back(format("https://{}:{}", _config.webshell_https_address(), https_cfg_opt->port));
    }

    co_await _server.invoke_on_all([&http_cfg_opt, &https_cfg_opt] (tools::webshell::server& ws) {
        return ws.init(http_cfg_opt, https_cfg_opt);
    });

    wslog.info("Webshell available on: {}", fmt::join(uris, ", "));
}

future<> controller::stop_server() {
    co_await _server.stop();
    _listen_addresses.clear();
}

future<> controller::request_stop_server() {
    return with_scheduling_group(_sched_group, [this] {
        return stop_server();
    });
}

future<utils::chunked_vector<client_data>> controller::get_client_data() {
    return _server.local().get_client_data();
}

} // namespace webshell
