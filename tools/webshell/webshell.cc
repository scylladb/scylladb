/*
 * Copyright (C) 2025-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <charconv>
#include <concepts>
#include <ranges>
#include <gnutls/crypto.h>
#include <seastar/core/fstream.hh>
#include <seastar/core/units.hh>
#include <seastar/coroutine/switch_to.hh>
#include <seastar/http/httpd.hh>
#include <seastar/util/log.hh>

#include "cql3/query_processor.hh"
#include "db/config.hh"
#include "service/client_state.hh"
#include "tools/webshell/webshell.hh"
#include "utils/observable.hh"

using namespace httpd;
using request = http::request;
using reply = http::reply;

using namespace tools::webshell;

namespace tools::webshell {

static logger wslog("webshell");

// Parse the webshell_https_client_auth config value into a seastar TLS client
// authentication mode. Unknown values are treated as "none" (with a warning),
// so that a bad live-update does not disable the HTTPS listener.
static seastar::tls::client_auth parse_client_auth(std::string_view mode) {
    if (mode.empty() || mode == "none") {
        return seastar::tls::client_auth::NONE;
    } else if (mode == "request") {
        return seastar::tls::client_auth::REQUEST;
    } else if (mode == "require") {
        return seastar::tls::client_auth::REQUIRE;
    }
    wslog.warn("Unknown webshell_https_client_auth value '{}', falling back to 'none'", mode);
    return seastar::tls::client_auth::NONE;
}

class session_id {
    uint64_t _msb;
    uint64_t _lsb;
    uint32_t _shard;

    // Widths of the three fields of the string form, as the formatter below
    // writes them.
    static constexpr size_t msb_digits = 16;
    static constexpr size_t lsb_digits = 16;
    static constexpr size_t shard_digits = 8;

    // Parse one hex field of the string form, accepting exactly what the
    // formatter emits and nothing else: lower-case hex digits filling the whole
    // field. std::stoull(), which this replaced, also accepted leading
    // whitespace, a sign and trailing garbage ("-1" came out as 0xffffffffffffffff,
    // "2a junk" as 0x2a), which let a whole family of strings alias onto one id.
    template <std::unsigned_integral T>
    static T parse_hex_field(std::string_view field) {
        if (field.find_first_not_of("0123456789abcdef") != std::string_view::npos) {
            throw std::invalid_argument("invalid session_id");
        }

        T value;
        const auto end = field.data() + field.size();
        const auto res = std::from_chars(field.data(), end, value, 16);
        if (res.ec != std::errc{} || res.ptr != end) {
            throw std::invalid_argument("invalid session_id");
        }

        return value;
    }

public:
    explicit session_id(uint64_t msb, uint64_t lsb, uint32_t shard)
        : _msb(msb), _lsb(lsb), _shard(shard)
    { }

    // Parse the string form the formatter below writes, "msb-lsb-shard". This is
    // the one place a client-supplied id is turned into a session_id, so it is
    // where the string has to stop being trusted; anything that is not exactly
    // the form written out is rejected with std::invalid_argument.
    explicit session_id(std::string_view id_str) {
        constexpr size_t lsb_offset = msb_digits + 1;
        constexpr size_t shard_offset = lsb_offset + lsb_digits + 1;
        constexpr size_t expected_size = shard_offset + shard_digits;

        // Every field is fixed-width, so the shape can be checked outright.
        if (id_str.size() != expected_size || id_str[msb_digits] != '-' || id_str[shard_offset - 1] != '-') {
            throw std::invalid_argument("invalid session_id");
        }

        _msb = parse_hex_field<uint64_t>(id_str.substr(0, msb_digits));
        _lsb = parse_hex_field<uint64_t>(id_str.substr(lsb_offset, lsb_digits));
        // Hex, matching the formatter. Parsing this field as decimal, as it used
        // to be, made every id created on shard 10 or above parse back to the
        // wrong shard ("0000000a" -> 0), so its session could never be found
        // again and the client was locked out right after logging in.
        _shard = parse_hex_field<uint32_t>(id_str.substr(shard_offset, shard_digits));

        // The shard field routes the request to the shard owning the session,
        // through smp::submit_to(), which indexes its queue array with it
        // unchecked. An id naming a shard this machine does not have is not
        // merely unknown, it is unusable, so it must not get past parsing.
        if (_shard >= this_smp_shard_count()) {
            throw std::invalid_argument("invalid session_id");
        }
    }

    session_id(const session_id&) = default;
    session_id& operator=(const session_id&) = default;

    bool operator==(const session_id&) const = default;
    bool operator!=(const session_id&) const = default;

    // Generate a random session_id, owned by this shard.
    //
    // A session_id is a bearer credential - presenting it is what authenticates
    // a request - so it has to be unpredictable, which means a CSPRNG. This used
    // to seed std::mt19937_64, which is not one, from a single
    // std::random_device{}() call, whose result_type is 32 bits wide: the 128
    // bits below then had only 2^32 possible values. Worse, Mersenne Twister is
    // deterministic, so that seed-to-id table is the same everywhere and could
    // be built once, offline, and used against any deployment.
    static session_id gen() {
        uint64_t bits[2];
        if (const auto ret = gnutls_rnd(GNUTLS_RND_RANDOM, bits, sizeof(bits)); ret < 0) {
            // Refusing to create the session is the only safe outcome here:
            // falling back to a predictable id would hand out a guessable
            // credential, which is worse than a failed login.
            throw std::runtime_error(seastar::format("failed to generate session_id: {}", gnutls_strerror(ret)));
        }
        return session_id{bits[0], bits[1], this_shard_id()};
    }

    // The form that goes into the session_id cookie, and that
    // session_id(std::string_view) parses back.
    //
    // This is the credential itself, so it is deliberately not what formatting a
    // session_id gives you - see the formatter below. It has to be asked for by
    // name, and the cookie is the only thing that should be asking.
    sstring to_cookie_string() const {
        return seastar::format("{:016x}-{:016x}-{:08x}", _msb, _lsb, _shard);
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

// Writes a short, stable, non-reversible tag for a session_id: "<digest>@<shard>".
//
// Formatting a session_id deliberately does NOT write the id itself. The id stays
// valid for the whole life of the session, so whoever can read it can take the
// session over - and formatting is how values end up in log lines, error
// messages and traces, none of which should hold a credential. The one place that
// legitimately needs the real thing, the cookie, asks for it by name through
// session_id::to_cookie_string().
//
// Folding the two halves of the id together and then keeping only half of the
// result each discard information, so the digest cannot be turned back into an
// id. The shard is passed through as it is: it is not secret - it only says which
// shard owns the session - and keeping it readable is what makes a log line point
// at a shard to go looking at.
//
// To match a session against the log while debugging, feed the raw id - the
// session_id cookie, or the one the web interface shows - to:
//
//     def session_id_log_tag(session_id):
//         msb, lsb, shard = (int(field, 16) for field in session_id.split("-"))
//         mask = (1 << 64) - 1
//         x = (msb * 0x9e3779b97f4a7c15 + lsb) & mask
//         x = ((x ^ (x >> 30)) * 0xbf58476d1ce4e5b9) & mask
//         x = ((x ^ (x >> 27)) * 0x94d049bb133111eb) & mask
//         return f"{((x ^ (x >> 31)) >> 32):08x}@{shard}"
//
//     >>> session_id_log_tag("cec3fa9d6f2a1b04-a71e5d3c98460f21-0000000b")
//     '869d78de@11'
//
// Keep the two in step: changing the mixing below without changing the snippet
// above makes the log unmatchable, which is the only thing the tag is for.
template <>
struct fmt::formatter<tools::webshell::session_id> : fmt::formatter<string_view> {
    auto format(tools::webshell::session_id sid, fmt::format_context& ctx) const -> decltype(ctx.out()) {
        uint64_t x = sid.msb() * 0x9e3779b97f4a7c15ull + sid.lsb();
        x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ull;
        x = (x ^ (x >> 27)) * 0x94d049bb133111ebull;
        return format_to(ctx.out(), "{:08x}@{}", static_cast<uint32_t>((x ^ (x >> 31)) >> 32), sid.shard());
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

class session {
public:
    const session_id id;

    service::client_state client_state;

    scheduling_group scheduling_group;
    sstring user_agent;
    bool is_https;

    session(session_id session_id, service::client_state client_state, ::scheduling_group sg, sstring user_agent, bool is_https)
        : id(std::move(session_id))
        , client_state(std::move(client_state))
        , scheduling_group(sg)
        , user_agent(std::move(user_agent))
        , is_https(is_https)
    { }

    sstring auth_user() const {
        return client_state.user().value().name.value_or("anonymous");
    }
};

class session_manager {
    const config& _config;
    cql3::query_processor& _qp;
    auth::service& _auth_service;
    qos::service_level_controller& _sl_controller;

    std::function<session_manager&()> _get_local_manager;

    client_options_cache_type _client_options_cache;

    std::unordered_map<session_id, lw_shared_ptr<session>> _sessions;

public:
    session_manager(const config& cfg, cql3::query_processor& qp, auth::service& auth_service, qos::service_level_controller& sl_controller,
            std::function<session_manager&()> get_local_manager)
        : _config(cfg), _qp(qp), _auth_service(auth_service), _sl_controller(sl_controller), _get_local_manager(std::move(get_local_manager))
    { }

    const config& config() const {
        return _config;
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

    future<utils::chunked_vector<foreign_ptr<std::unique_ptr<client_data>>>> get_client_data() {
        utils::chunked_vector<foreign_ptr<std::unique_ptr<client_data>>> ret;

        for (const auto& [session_id, session] : _sessions) {
            auto user_agent = co_await _client_options_cache.get_or_load(session->user_agent, [] (const client_options_cache_key_type&) {
                return make_ready_future<options_cache_value_type>(options_cache_value_type{});
            });
            ret.emplace_back(std::make_unique<client_data>(client_data{
                // ip/port is the one that was seen at login, it may change later.
                // TODO: return last seen ip/port instead
                .ip = session->client_state.get_client_address(),
                .port = session->client_state.get_client_port(),
                .ct = client_type::webshell,
                .connection_stage = client_connection_stage::ready,
                .shard_id = this_shard_id(),
                // Use the User-Agent header as the driver name, leave driver version unset
                .driver_name = std::move(user_agent),
                .ssl_enabled = session->is_https,
                .username = session->auth_user(),
                .scheduling_group_name = session->scheduling_group.name(),
                // Leave "protocol_version" unset, it has no meaning in Webshell.
                // Leave "hostname", "ssl_protocol" and "ssl_cipher_suite" unset.
                // As reported in issue #9216, we never set these fields in CQL
                // either (see cql_server::connection::make_client_data()).
            }));
        }

        co_return ret;
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
    config _config;
    scheduling_group _scheduling_group;
    httpd::http_server _http_server;
    httpd::http_server _https_server;
    ::shared_ptr<seastar::tls::server_credentials> _credentials;
    // Fires when webshell_https_client_auth is changed at runtime, so the new
    // client-authentication mode is applied to the (per-shard) credentials.
    std::optional<utils::observer<sstring>> _client_auth_observer;

    utils::small_vector<std::reference_wrapper<seastar::httpd::http_server>, 2> _enabled_servers;

    request_control _request_control;
    session_manager _session_manager;

private:
    void set_routes(seastar::httpd::routes& r, bool is_https);

    // (Re)apply the current webshell_https_client_auth mode to _credentials.
    void apply_client_auth() {
        if (_credentials) {
            _credentials->set_client_auth(parse_client_auth(_config.webshell_https_client_auth()));
        }
    }

public:
    server(config cfg, scheduling_group sg, cql3::query_processor& qp, auth::service& auth_service, qos::service_level_controller& sl_controller);

    future<> init(std::optional<http_listen_config> http_cfg_opt, std::optional<https_listen_config> https_cfg_opt);
    future<> stop();

    future<utils::chunked_vector<foreign_ptr<std::unique_ptr<client_data>>>> get_client_data();
};

void server::set_routes(routes& r, bool is_https) {
}

server::server(config cfg, scheduling_group sg, cql3::query_processor& qp, auth::service& auth_service, qos::service_level_controller& sl_controller)
    : _config(cfg)
    , _scheduling_group(sg)
    , _http_server("scylladb-webshell-http")
    , _https_server("scylladb-webshell-https")
    , _request_control("webshell", _config.max_concurrent_requests, _config.max_waiting_requests)
    , _session_manager(_config, qp, auth_service, sl_controller, [this] () -> session_manager& {
        return container().local()._session_manager;
    })
{
}

future<> server::init(std::optional<http_listen_config> http_cfg_opt, std::optional<https_listen_config> https_cfg_opt) {
    co_await coroutine::switch_to(_scheduling_group);

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
                    // A rebuild resets the client-auth mode to the value baked
                    // into the builder, so re-apply the current live mode on
                    // every shard after reloading the certificate files.
                    apply_client_auth();
                    co_await container().invoke_on_others([&b](server& s) {
                        if (s._credentials) {
                            b.rebuild(*s._credentials);
                            s.apply_client_auth();
                        }
                    });
                    wslog.info("Reloaded {}", files);
                }
            });
        } else {
            _credentials = https_cfg_opt->creds.build_server_credentials();
        }

        // Apply the configured client-auth mode and keep it updated on live
        // changes to webshell_https_client_auth. _config carries a per-shard
        // updateable_value (bound in make_config), so each shard observes its
        // own source and updates its own credentials.
        apply_client_auth();
        _client_auth_observer.emplace(_config.webshell_https_client_auth.observe([this](const sstring& mode) {
            if (_credentials) {
                _credentials->set_client_auth(parse_client_auth(mode));
                wslog.info("Updated Web Shell HTTPS client authentication mode to '{}' due to config update", mode);
            }
        }));

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

future<utils::chunked_vector<foreign_ptr<std::unique_ptr<client_data>>>> server::get_client_data() {
    return _session_manager.get_client_data();
}

config make_config(const db::config& db_cfg, std::string_view cluster_name) {
    return config{
        .cluster_name = sstring(cluster_name),
        .listen_interface_prefer_ipv6 = db_cfg.listen_interface_prefer_ipv6(),
        .enable_ipv6_dns_lookup = db_cfg.enable_ipv6_dns_lookup(),
        .timeout_config = updateable_timeout_config(db_cfg),
        .webshell_http_address = db_cfg.webshell_http_address().empty() ? db_cfg.api_address() : db_cfg.webshell_http_address(),
        .webshell_http_port = db_cfg.webshell_http_port(),
        .webshell_https_address = db_cfg.webshell_https_address(),
        .webshell_https_port = db_cfg.webshell_https_port(),
        .webshell_https_encryption_options = db_cfg.webshell_https_encryption_options(),
        // Bind (not snapshot) so the value tracks live config updates. The
        // binding is to the local shard's source, so make_config must run on
        // each shard that needs it (see controller::start_server).
        .webshell_https_client_auth = db_cfg.webshell_https_client_auth,
        .webshell_resource_manifest_path = std::filesystem::path(db_cfg.webshell_resource_manifest_path()),
    };
}

controller::controller(scheduling_group sg, sharded<cql3::query_processor>& qp, sharded<auth::service>& auth_service,
        sharded<qos::service_level_controller>& sl_controller, std::function<config()> config_factory)
    : protocol_server(sg)
    , _qp(qp)
    , _auth_service(auth_service)
    , _sl_controller(sl_controller)
    , _config_factory(std::move(config_factory))
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
    std::exception_ptr ex;
    try {
        co_await coroutine::switch_to(_sched_group);

        utils::small_vector<sstring, 2> uris;

        _listen_addresses.clear();

        co_await _server.start(sharded_parameter(_config_factory), _sched_group,
                std::ref(_qp), std::ref(_auth_service), std::ref(_sl_controller));

        auto config = _config_factory();

        auto preferred = config.listen_interface_prefer_ipv6 ? std::make_optional(net::inet_address::family::INET6) : std::nullopt;
        auto family = config.enable_ipv6_dns_lookup || preferred ? std::nullopt : std::make_optional(net::inet_address::family::INET);

        std::optional<tools::webshell::http_listen_config> http_cfg_opt;
        if (config.webshell_http_port) {
            http_cfg_opt.emplace(tools::webshell::http_listen_config{
                    .address = co_await gms::inet_address::lookup(config.webshell_http_address, family),
                    .port = config.webshell_http_port});
            _listen_addresses.push_back({http_cfg_opt->address, http_cfg_opt->port});

            uris.push_back(format("http://{}:{}", config.webshell_http_address, http_cfg_opt->port));
        }

        std::optional<tools::webshell::https_listen_config> https_cfg_opt;
        if (config.webshell_https_port) {
            tls::credentials_builder creds;

            std::exception_ptr ex;
            co_await utils::configure_tls_creds_builder(creds, config.webshell_https_encryption_options);

            // Set the initial client-authentication mode on the builder so that
            // it is applied to every shard's credentials (and preserved across
            // certificate reloads). Live changes are handled per shard in
            // server::init via a config observer.
            creds.set_client_auth(parse_client_auth(config.webshell_https_client_auth()));

            https_cfg_opt.emplace(tools::webshell::https_listen_config{
                    .address = co_await gms::inet_address::lookup(config.webshell_https_address, family),
                    .port = config.webshell_https_port,
                    .creds = std::move(creds)});

            _listen_addresses.push_back({https_cfg_opt->address, https_cfg_opt->port});

            uris.push_back(format("https://{}:{}", config.webshell_https_address, https_cfg_opt->port));
        }

        co_await _server.invoke_on_all([&http_cfg_opt, &https_cfg_opt] (tools::webshell::server& ws) {
            return ws.init(http_cfg_opt, https_cfg_opt);
        });

        wslog.info("Webshell available on: {}", fmt::join(uris, ", "));
    } catch (...) {
        ex = std::current_exception();
        wslog.error("Failed to start Webshell server: {}", ex);
    }

    if (ex) {
        co_await stop_server();
        std::rethrow_exception(ex);
    }
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

future<utils::chunked_vector<foreign_ptr<std::unique_ptr<client_data>>>> controller::get_client_data() {
    return _server.local().get_client_data();
}

} // namespace webshell
