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
#include <seastar/http/short_streams.hh>
#include <seastar/util/log.hh>

#include "cql3/query_processor.hh"
#include "cql3/query_result_printer.hh"
#include "db/config.hh"
#include "service/client_state.hh"
#include "tools/webshell/webshell.hh"
#include "utils/base64.hh"

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

enum class output_format {
    text, json
};

sstring to_string(output_format of) {
    switch (of) {
        case output_format::text:
            return "text";
        case output_format::json:
            return "json";
    }
    throw std::runtime_error(format("Unknown output format: {}", static_cast<int>(of)));
}

sstring output_format_to_content_type(output_format of) {
    return to_string(of);
}

class unauthorized_access : public base_exception {
public:
    unauthorized_access(sstring msg) : base_exception(msg, reply::status_type::unauthorized)
    { }
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
    sstring last_query;
    tracing::trace_state_ptr trace_state;

    explicit session_data(session_id id)
        : id(std::move(id))
    { }
};

struct session_options {
    db::consistency_level consistency = db::consistency_level::ONE;
    bool expand = false;
    int32_t page_size = 100; // if <= 0, paging is disabled
    db::consistency_level serial_consistency = db::consistency_level::SERIAL;
    bool tracing = false;
    output_format output_format = output_format::text;
};

} // namespace tools::webshell

template <>
struct fmt::formatter<tools::webshell::session_options> : fmt::formatter<string_view> {
    auto format(tools::webshell::session_options opts, fmt::format_context& ctx) const -> decltype(ctx.out()) {
        return format_to(ctx.out(), "{{consistency={}, expand={}, page_size={}, serial_consistency={}, tracing={}, output_format={}}}",
                opts.consistency,
                opts.expand,
                opts.page_size,
                opts.serial_consistency,
                opts.tracing,
                to_string(opts.output_format));
    }
};

namespace tools::webshell {

class session {
public:
    session_data data;
    session_options options;
    service::client_state client_state;

    scheduling_group scheduling_group;
    sstring user_agent;
    bool is_https;

private:
    seastar::timer<lowres_clock> _ttl_timer;
    semaphore _semaphore{1}; // enforce one concurrent request per session

public:
    session(session_id session_id, service::client_state client_state, ::scheduling_group sg, sstring user_agent, bool is_https, noncopyable_function<void(::session_id)> expire_callback)
        : data(std::move(session_id))
        , client_state(std::move(client_state))
        , scheduling_group(sg)
        , user_agent(std::move(user_agent))
        , is_https(is_https)
        , _ttl_timer([expire_callback = std::move(expire_callback), id = data.id] {
            wslog.debug("session with session_id {} expired", id);
            expire_callback(id);
        })
    {
    }

    void refresh(db_clock::duration session_ttl) {
        _ttl_timer.rearm(lowres_clock::now() + session_ttl);
    }

    session_id id() const noexcept {
        return data.id;
    }

    sstring auth_user() const {
        return client_state.user().value().name.value_or("anonymous");
    }

    friend class session_manager;
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

    size_t session_count() const noexcept {
        return _sessions.size();
    }

    bool has_session(const session_id& session_id) const noexcept {
        return _sessions.find(session_id) != _sessions.end();
    }

    session& create_session(service::client_state client_state, scheduling_group sg, sstring user_agent, bool is_https) {
        auto session_id = session_id::gen();

        wslog.debug("creating session with session_id {} for user {}", session_id, client_state.user().value().name.value_or("anonymous"));

        auto [it, inserted] = _sessions.emplace(session_id, make_lw_shared<session>(session_id, std::move(client_state), sg, std::move(user_agent), is_https, [this] (const ::session_id& id) {
            remove_session(id);
        }));
        if (!inserted) {
            throw std::runtime_error("Failed to create new session, session already exists");
        }
        it->second->refresh(_cfg.session_ttl);
        return *it->second;
    }

    void remove_session(const session_id& session_id) {
        auto it = _sessions.find(session_id);
        if (it != _sessions.end()) {
            _sessions.erase(it);
        }
    }

    template <std::invocable<session_manager&, session*> F>
    auto invoke_on_unchecked(session_id session_id, F f) {
        return smp::submit_to(session_id.shard(), [this, session_id, f = std::move(f)] () mutable
                -> futurize_t<std::invoke_result_t<F, session_manager&, session*>> {
            auto& local_this = _get_local_manager();

            lw_shared_ptr<session> session_ptr;
            auto it = local_this._sessions.find(session_id);
            if (it != local_this._sessions.end()) {
                session_ptr = it->second;
            }

            std::optional<semaphore_units<>> units;
            if (session_ptr) {
                units.emplace(co_await get_units(session_ptr->_semaphore, 1));
            }

            co_return co_await futurize_invoke(std::move(f), local_this, session_ptr.get());
        });
    }

    template <std::invocable<session_manager&, session&> F>
    auto invoke_on(session_id session_id, F f) {
        return invoke_on_unchecked(session_id, [f = std::move(f)] (session_manager& local_this, session* session_opt) mutable {
            if (!session_opt) {
                throw unauthorized_access("Session not found");
            }
            return f(local_this, *session_opt);
        });
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

const std::string_view session_cookies[] {
    "session_id",
    "user_name",
    "cluster_name",
    "paging_state",
    "trace_session_id"
};
const std::string_view http_only_session_cookies[] {
    "session_id",
};

template <typename T>
void set_session_cookie(reply& rep, config cfg, std::string_view key, const T& value) {
    const bool http_only = std::ranges::find(http_only_session_cookies, key) != std::end(http_only_session_cookies);
    const auto max_age = std::chrono::duration_cast<std::chrono::seconds>(cfg.session_ttl).count();
    rep.set_cookie(sstring(key), fmt::format("{}; {}Max-Age={}", value, http_only ? "HttpOnly; " : "", max_age));
}

// FIXME: assumes the Cookie: <cookie-list> syntax, which most clients seems to
// use, but this is not guranteed. If a client uses multiple Cookie headers, this
// will not work.
std::unordered_map<sstring, sstring> handle_cookies(const config& cfg, const request& req, reply& rep) {
    const auto cookie_header = req.get_header("Cookie");

    wslog.trace("handle_cookies({})", cookie_header);

    std::unordered_map<sstring, sstring> cookies;

    auto stripped = [] (std::string_view sv) {
        auto start = sv.find_first_not_of(" \t");
        auto end = sv.find_last_not_of(" \t");
        return sv.substr(start, end - start + 1);
    };

    for (const auto cookie_pair : std::views::split(cookie_header, ';')) {
        auto cookie_pair_v = stripped(std::string_view(cookie_pair.begin(), cookie_pair.end()));
        if (cookie_pair_v.empty()) {
            continue;
        }
        auto eq_pos = cookie_pair_v.find_first_of('=');
        std::unordered_map<sstring, sstring>::iterator it;
        bool inserted = false;
        if (eq_pos == std::string_view::npos) {
            std::tie(it, inserted) = cookies.emplace(sstring(cookie_pair_v), "");
        } else {
            auto name = cookie_pair_v.substr(0, eq_pos);
            auto value = cookie_pair_v.substr(eq_pos + 1);
            std::tie(it, inserted) = cookies.emplace(sstring(name), sstring(value));
        }

        if (std::ranges::find(session_cookies, it->first) == std::end(session_cookies)) {
            rep.set_cookie(it->first, it->second);
        } else {
            set_session_cookie(rep, cfg, it->first, it->second);
        }
    }

    return cookies;
}

void set_session_cookies(reply& rep, const config& cfg, session_id session_id, sstring auth_user) {
    set_session_cookie(rep, cfg, "session_id", session_id);
    set_session_cookie(rep, cfg, "user_name", auth_user);
    set_session_cookie(rep, cfg, "cluster_name", cfg.cluster_name);
}

void erase_session_cookie(reply& rep, std::string_view key) {
    rep.set_cookie(sstring(key), "; Max-Age=0");
}

template <typename T>
void set_or_erase_session_cookie(reply& rep, const config& cfg, std::string_view key, const std::optional<T>& value_opt) {
    if (value_opt) {
        set_session_cookie(rep, cfg, key, *value_opt);
    } else {
        erase_session_cookie(rep, key);
    }
}

std::pair<std::optional<session_id>, sstring> try_get_session_id(const std::unordered_map<sstring, sstring>& cookies) {
    auto it = cookies.find("session_id");
    if (it == cookies.end()) {
        return {std::nullopt, "session_id not found in cookies"};
    }

    try {
        return {session_id(it->second), ""};
    } catch (...) {
        return {std::nullopt, format("Invalid session_id: {}", std::current_exception())};
    }
}

session_id get_session_id(const std::unordered_map<sstring, sstring>& cookies) {
    auto [session_id_opt, error_str] = try_get_session_id(cookies);
    if (!session_id_opt) {
        throw unauthorized_access(error_str);
    }
    return *session_id_opt;
}

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

class gated_handler : public handler_base {
    const char* _name;
    request_control& _request_control;
public:
    explicit gated_handler(const char* name, request_control& request_control)
        : _name(name), _request_control(request_control)
    {}
    virtual future<std::unique_ptr<reply>> do_handle(const sstring& path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) = 0;
    virtual future<std::unique_ptr<reply>> handle(const sstring& path_, std::unique_ptr<request> req, std::unique_ptr<reply> rep) final override {
        const auto path = path_;
        const auto method = req->_method;
        wslog.trace("handler {}: start request {} {}", _name, method, path);

        if (_request_control.too_many_waiters()) {
            wslog.debug("handler {}: dropping {} {}: too many requests", _name, method, path);
            rep->set_status(reply::status_type::service_unavailable);
            rep->write_body("text", "Too many requests, try again later");
            co_return std::move(rep);
        }

        try {
            auto ret = co_await _request_control.run([this, &path, req = std::move(req), rep = std::move(rep)] () mutable {
                return do_handle(path, std::move(req), std::move(rep));
            });
            wslog.trace("handler {}: finish request {} {} {}", _name, method, path, ret->_status);
            co_return ret;
        } catch (gate_closed_exception&) {
            throw base_exception("Server shutting down", reply::status_type::service_unavailable);
        } catch (broken_semaphore&) {
            throw base_exception("Server shutting down", reply::status_type::service_unavailable);
        } catch (base_exception& e) {
            // Prevent the fall-through to the default handler below, which converts unknonw exceptions to 500 Internal Server Error
            // Exceptions derived from base_exception already have a proper status code set, so re-throw them as is.
            wslog.trace("handler {}: finish request {} {} {}", _name, method, path, e.status());
            throw;
        } catch (...) {
            wslog.trace("handler {}: finish request {} {} {}", _name, method, path, reply::status_type::internal_server_error);
            throw;
        }
    }
};

class resource_handler : public gated_handler {
public:
    explicit resource_handler(request_control& request_control)
        : gated_handler("resource", request_control)
    {}
protected:
    virtual future<std::unique_ptr<reply>> do_handle(const sstring& path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) override {
        co_return std::move(rep);
    }
};

class login_handler : public gated_handler {
    constexpr static size_t max_authentication_credentials_length = 128 * 1024; // Maximum length of authentication credentials, just for sanity

    session_manager& _session_manager;
    const bool _is_https;
public:
    login_handler(request_control& request_control, session_manager& session_manager, bool is_https)
        : gated_handler("login", request_control), _session_manager(session_manager), _is_https(is_https)
    {}
protected:
    virtual future<std::unique_ptr<reply>> do_handle(const sstring& path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) override {
        const auto cookies = handle_cookies(_session_manager.config(), *req, *rep);
        auto [session_id_opt, _] = try_get_session_id(cookies);

        if (session_id_opt) {
            const auto has_session = co_await smp::submit_to(session_id_opt->shard(), [&] {
                return _session_manager.has_session(*session_id_opt);
            });
            if (has_session) {
                rep->set_status(reply::status_type::ok);
                rep->write_body("text", format("Already logged in, erase cookies or send request to /logout to log in with another user."));
                co_return std::move(rep);
            }
        }

        if (_session_manager.session_count() >= _session_manager.config().max_sessions) {
            rep->set_status(reply::status_type::service_unavailable);
            rep->write_body("text", "Too many sessions, try again later");
            co_return std::move(rep);
        }

        auto client_state = service::client_state(
                service::client_state::external_tag{},
                _session_manager.auth_service(),
                &_session_manager.sl_controller(),
                _session_manager.config().timeout_config.current_values(),
                req->get_client_address());

        auto& sl_controller = _session_manager.sl_controller();
        auto sg = sl_controller.get_default_scheduling_group();

        auto& auth = client_state.get_auth_service()->underlying_authenticator();
        if (auth.require_authentication()) {
            const auto credentials = co_await util::read_entire_stream_contiguous(*req->content_stream);
            if (credentials.empty()) {
                rep->set_status(reply::status_type::bad_request);
                rep->write_body("text", "No credentials provided, provide credentials in the request body, one per line, first line is the username, second line is the password");
                co_return std::move(rep);
            }
            if (credentials.size() > max_authentication_credentials_length) {
                rep->set_status(reply::status_type::bad_request);
                rep->write_body("text", format("Credentials too long, max length is {}", max_authentication_credentials_length));
                co_return std::move(rep);
            }

            const auto credential_items = std::views::split(credentials, '\n') | std::ranges::to<std::vector<sstring>>();
            if (credential_items.size() != 2) {
                rep->set_status(reply::status_type::bad_request);
                rep->write_body("text", format("Invalid credentials, expected two lines, first line is the username, second line is the password, got {} lines instead", credential_items.size()));
                co_return std::move(rep);
            }

            bytes_ostream buf;
            buf.write(credential_items[0].c_str(), credential_items[0].size()); // authzId (username)
            buf.write("\0", 1); // Add NUL byte as delimiter
            buf.write(credential_items[0].c_str(), credential_items[0].size()); // authnId (username)
            buf.write("\0", 1); // Add NUL byte as delimiter
            buf.write(credential_items[1].c_str(), credential_items[1].size()); // password
            buf.write("\0", 1); // Add NUL byte as delimiter

            auto sasl_challenge = client_state.get_auth_service()->underlying_authenticator().new_sasl_challenge();

            try {
                sasl_challenge->evaluate_response(buf.linearize());

                if (sasl_challenge->is_complete()) {
                    auto user = co_await sasl_challenge->get_authenticated_user();
                    client_state.set_login(std::move(user));
                    sg = co_await sl_controller.get_user_scheduling_group(client_state.user());
                    co_await client_state.check_user_can_login();
                    co_await client_state.maybe_update_per_service_level_params();
                } else {
                    rep->set_status(reply::status_type::internal_server_error);
                    rep->write_body("text", "Configured SASL is a multistage authentication mechanism, currently unsupported by webshell");
                    co_return std::move(rep);
                }

                rep->write_body("text", format("Successfully logged in as user {}", client_state.user().value().name.value()));
            } catch (exceptions::authentication_exception& e) {
                rep->set_status(reply::status_type::bad_request);
                rep->write_body("text", e.what());
                co_return std::move(rep);
            }
        } else {
            rep->write_body("text", "Successfully logged in as anonymous user");
        }

        const auto user_agent = req->get_header("User-Agent");

        auto& session = _session_manager.create_session(std::move(client_state), sg, std::move(user_agent), _is_https);

        set_session_cookies(*rep, _session_manager.config(), session.id(), session.auth_user());

        rep->set_status(reply::status_type::ok);

        co_return std::move(rep);
    }
};

class logout_handler : public gated_handler {
    session_manager& _session_manager;
public:
    logout_handler(request_control& request_control, session_manager& session_manager)
        : gated_handler("logout", request_control), _session_manager(session_manager)
    {}
protected:
    virtual future<std::unique_ptr<reply>> do_handle(const sstring& path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) override {
        const auto cookies = handle_cookies(_session_manager.config(), *req, *rep);
        auto [session_id_opt, _] = try_get_session_id(cookies);

        if (!session_id_opt) {
            rep->set_status(reply::status_type::ok);
            rep->write_body("text", "Already logged out");
            co_return std::move(rep);
        }

        const auto response = co_await _session_manager.invoke_on_unchecked(*session_id_opt,
                [] (session_manager& session_manager, session* session_ptr) {
            if (session_ptr) {
                session_manager.remove_session(session_ptr->id());
                return "Successfully logged out";
            }
            return "Already logged out";
        });

        rep->set_status(reply::status_type::ok);

        // Erase cookies, relies on well-behaved client.
        // Not a problem because we dropped the session internally.
        for (const auto& cookie_name : session_cookies) {
            erase_session_cookie(*rep, cookie_name);
        }

        rep->write_body("text", response);
        co_return std::move(rep);
    }
};

class query_result_visitor : public cql_transport::messages::result_message::visitor {
    output_format _output_format;
    std::ostream& _os;
    lw_shared_ptr<const service::pager::paging_state>& _paging_state;
    std::optional<shard_id>& _bounce_to_shard;
private:
    [[noreturn]] void throw_on_unexpected_message(const char* message_kind) {
        throw std::runtime_error(std::format("unexpected result message {}", message_kind));
    }
public:
    query_result_visitor(output_format of, std::ostream& os, lw_shared_ptr<const service::pager::paging_state>& paging_state, std::optional<shard_id>& bounce_to_shard)
        : _output_format(of)
        , _os(os)
        , _paging_state(paging_state)
        , _bounce_to_shard(bounce_to_shard)
    { }
    virtual void visit(const cql_transport::messages::result_message::void_message&) override {
    }
    virtual void visit(const cql_transport::messages::result_message::set_keyspace& msg) override {
        _os << "Successfully set keyspace " << msg.get_keyspace();
    }
    virtual void visit(const cql_transport::messages::result_message::prepared::cql& msg) override {
        _os << "Query prepared with id " << to_hex(msg.get_id());
    }
    virtual void visit(const cql_transport::messages::result_message::schema_change& msg) override {
        auto& event = *msg.get_change();
        switch (event.change) {
            case cql_transport::event::schema_change::change_type::CREATED:
                _os << "Created ";
                break;
            case cql_transport::event::schema_change::change_type::UPDATED:
                _os << "Updated ";
                break;
            case cql_transport::event::schema_change::change_type::DROPPED:
                _os << "Dropped ";
                break;
        }
        switch (event.target) {
            case cql_transport::event::schema_change::target_type::KEYSPACE:
                _os << "keyspace";
                break;
            case cql_transport::event::schema_change::target_type::TABLE:
                _os << "table";
                break;
            case cql_transport::event::schema_change::target_type::TYPE:
                _os << "type";
                break;
            case cql_transport::event::schema_change::target_type::FUNCTION:
                _os << "function";
                break;
            case cql_transport::event::schema_change::target_type::AGGREGATE:
                _os << "aggregate";
                break;
        }
    }
    virtual void visit(const cql_transport::messages::result_message::bounce_to_shard& msg) override {
        _bounce_to_shard = *msg.move_to_shard();
    }
    virtual void visit(const cql_transport::messages::result_message::exception&) override {
        throw_on_unexpected_message("exception");
    }

    virtual void visit(const cql_transport::messages::result_message::rows& rows) override {
        const auto& result = rows.rs();
        switch (_output_format) {
            case output_format::text:
                cql3::print_query_results_text(_os, result);
                break;
            case output_format::json:
                cql3::print_query_results_json(_os, result);
                break;
        }

        if (result.get_metadata().flags().contains<cql3::metadata::flag::HAS_MORE_PAGES>()) {
            _paging_state = result.get_metadata().paging_state();
        }
    }
};

class query_handler : public gated_handler {
    session_manager& _session_manager;

    static void setup_tracing(const session_options& options, session_data& data, const service::client_state& client_state, std::string_view query) {
        if (!options.tracing || data.last_query != query) {
            data.trace_state = {};
        }
        if (!options.tracing || data.trace_state) {
            return;
        }

        tracing::trace_state_props_set trace_props;
        trace_props.set<tracing::trace_state_props::full_tracing>();

        auto trace_state = tracing::tracing::get_local_tracing_instance().create_session(tracing::trace_type::QUERY, trace_props);
        tracing::begin(trace_state, "Execute webshell query", client_state.get_client_address());
        tracing::add_session_param(trace_state, "session_id", fmt::to_string(data.id));
        tracing::add_session_param(trace_state, "session_options", fmt::to_string(options));
        tracing::add_query(trace_state, query);

        data.trace_state = trace_state;
    }

    struct query_exec_result {
        sstring result;
        output_format result_format;
        std::optional<sstring> paging_state;
        std::optional<utils::UUID> trace_session_id;
    };

    static future<std::variant<query_exec_result, shard_id>> do_execute_query_on_shard(cql3::query_processor& qp, const session_options& options, const sstring& query, const sstring& last_query,
            service::client_state& client_state, tracing::trace_state_ptr trace_state, const bytes_opt& serialized_paging_state) {
        auto query_state = service::query_state(client_state, std::move(trace_state), empty_service_permit());

        lw_shared_ptr<service::pager::paging_state> paging_state_in;
        if (query == last_query && serialized_paging_state) {
            try {
                paging_state_in = service::pager::paging_state::deserialize(*serialized_paging_state);
            } catch (...) {
                throw bad_request_exception(format("Invalid paging_state cookie: {}", std::current_exception()));
            }
        }

        const auto specific_options = cql3::query_options::specific_options{
                options.page_size,
                std::move(paging_state_in),
                options.serial_consistency,
                api::missing_timestamp,
                service::node_local_only::no};

        auto query_options = cql3::query_options{cql3::default_cql_config, options.consistency, std::nullopt, std::vector<cql3::raw_value_view>(), false, specific_options};

        auto result = co_await qp.execute_direct(query, query_state, {}, query_options);
        result->throw_if_exception();

        std::stringstream os;
        lw_shared_ptr<const service::pager::paging_state> paging_state_out;
        std::optional<shard_id> bounce_to_shard;

        query_result_visitor visitor(options.output_format, os, paging_state_out, bounce_to_shard);
        result->accept(visitor);

        if (bounce_to_shard) {
            co_return *bounce_to_shard;
        }

        std::optional<sstring> paging_state_str_out;
        if (paging_state_out) {
            auto buf = paging_state_out->serialize();
            paging_state_str_out = base64_encode(*buf);
        }

        co_return query_exec_result(os.str(), options.output_format, std::move(paging_state_str_out), {});
    }

    static future<query_exec_result> do_execute_query_on_session_shard(sharded<cql3::query_processor>& qp, const session_options& options, session_data& data,
            service::client_state& client_state, const sstring& query, const bytes_opt& serialized_paging_state) {
        tracing::trace(data.trace_state, "executing webshell query");
        wslog.trace("executing query {}", query, options);

        auto res = co_await do_execute_query_on_shard(qp.local(), options, query, data.last_query, client_state, data.trace_state, serialized_paging_state);

        // Handle bounce to another shard
        if (std::holds_alternative<shard_id>(res)) {
            const auto shard = std::get<shard_id>(res);
            auto gcs = client_state.move_to_other_shard();
            auto gts = tracing::global_trace_state_ptr(data.trace_state);

            tracing::trace(data.trace_state, "query bounced to shard {}", shard);
            wslog.trace("query bounced to shard {}", shard);

            res = co_await qp.invoke_on(shard, [&gcs, &gts, &options, &query, &last_query = data.last_query, &serialized_paging_state] (cql3::query_processor& qp)
                    -> future<std::variant<query_exec_result, shard_id>> {
                auto client_state = gcs.get();
                auto trace_state = gts.get();
                co_return co_await do_execute_query_on_shard(qp, options, query, last_query, client_state, std::move(trace_state), serialized_paging_state);
            });

            if (std::holds_alternative<shard_id>(res)) {
                throw std::runtime_error(format("Unexpected bounce to another shard, after handling a bounce to shard {}", shard));
            }
        }

        auto query_res = std::get<query_exec_result>(std::move(res));

        if (data.trace_state) {
            query_res.trace_session_id = data.trace_state->session_id();

            if (!query_res.paging_state) {
                data.trace_state = {};
            }
        }

        co_return std::move(query_res);
    }

public:
    query_handler(request_control& request_control, session_manager& session_manager)
        : gated_handler("query", request_control)
        , _session_manager(session_manager)
    { }

    static future<std::pair<reply::status_type, query_exec_result>> execute_query(sharded<cql3::query_processor>& qp, const session_options& options, session_data& data,
            service::client_state& client_state, const sstring& query, const bytes_opt& serialized_paging_state) {
        setup_tracing(options, data, client_state, query);

        reply::status_type status = reply::status_type::ok;
        query_exec_result result;

        try {
            result = co_await do_execute_query_on_session_shard(qp, options, data, client_state, query, serialized_paging_state);
        } catch (exceptions::unauthorized_exception& e) {
            // This exception is used both when the user is not logged in and
            // when the user is logged in but does not have permissions to execute
            // the query.
            // We want to use distinct HTTP status codes for these cases:
            // * 401 Unauthorized for not logged in
            // * 403 Forbidden for logged in user without permissions
            // We already check the user login when obtaining the session, so the
            // first case is handled there. Any unauthorized exceptions caught here
            // should be for the second case, so we use 403 Forbidden here.
            status = reply::status_type::forbidden;
            result.result = e.get_message();
        } catch (exceptions::syntax_exception& e) {
            status = reply::status_type::bad_request;
            result.result = format("Syntax error: {}", e.get_message());
        } catch (exceptions::request_validation_exception& e) {
            status = reply::status_type::bad_request;
            result.result = e.get_message();
        }

        data.last_query = query;

        co_return std::make_pair(status, std::move(result));
    }

protected:
    virtual future<std::unique_ptr<reply>> do_handle(const sstring& path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) override {
        const auto cookies = handle_cookies(_session_manager.config(), *req, *rep);
        const auto session_id = get_session_id(cookies);

        bytes_opt serialized_paging_state;
        if (cookies.contains("paging_state")) {
            try {
                serialized_paging_state = base64_decode(cookies.at("paging_state"));
            } catch (...) {
                rep->set_status(reply::status_type::bad_request);
                rep->write_body("text", "Invalid paging_state cookie: not valid base64");
                co_return std::move(rep);
            }
        }

        const auto query = co_await util::read_entire_stream_contiguous(*req->content_stream);

        const auto [status, result] = co_await _session_manager.invoke_on(session_id, [&query, &serialized_paging_state] (session_manager& session_manager, session& session) {
            return execute_query(session_manager.qp().container(), session.options, session.data, session.client_state, query, serialized_paging_state).finally([&session_manager, &session] {
                session.refresh(session_manager.config().session_ttl);
            });
        });

        if (status != reply::status_type::ok) {
            rep->set_status(status);
            rep->write_body("text", result.result);
            co_return std::move(rep);
        }

        set_or_erase_session_cookie(*rep, _session_manager.config(), "paging_state", result.paging_state);
        set_or_erase_session_cookie(*rep, _session_manager.config(), "trace_session_id", result.trace_session_id);

        rep->set_status(status);
        rep->write_body(output_format_to_content_type(result.result_format), std::move(result.result));
        co_return std::move(rep);
    }
};

class command_handler : public gated_handler {
    session_manager& _session_manager;

public:
    command_handler(request_control& request_control, session_manager& session_manager)
        : gated_handler("option", request_control)
        , _session_manager(session_manager)
    { }

protected:
    virtual future<std::unique_ptr<reply>> do_handle(const sstring& path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) override {
        (void)_session_manager;
        co_return std::move(rep);
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
    r.add_default_handler(new resource_handler(_request_control));
    r.put(operation_type::POST, "/login", new login_handler(_request_control, _session_manager, is_https));
    r.put(operation_type::POST, "/logout", new logout_handler(_request_control, _session_manager));
    r.put(operation_type::POST, "/query", new query_handler(_request_control, _session_manager));
    r.put(operation_type::POST, "/command", new command_handler(_request_control, _session_manager));

    r.register_exeption_handler([] (std::exception_ptr ex) {
        wslog.trace("handle exception: {}", ex);

        auto handle_exception = [] (reply::status_type status, sstring msg) {
            auto exception_rep = std::make_unique<reply>();
            exception_rep->set_status(status);
            //FIXME: have to use body writer function here, due to https://github.com/scylladb/seastar/issues/2889
            exception_rep->write_body("text", [msg] (output_stream<char>&& out_) -> future<> {
                auto out = std::move(out_);
                co_await out.write(msg.c_str(), msg.size());
                co_await out.flush();
                co_await out.close();
            });
            return exception_rep;
        };

        try {
            std::rethrow_exception(ex);
        } catch (base_exception& e) {
            // Prevent the fall-through to the default handler below, which converts unknown exceptions to 500 Internal Server Error
            // Exceptions derived from base_exception already have a proper status code set, so re-throw them as is.
            return handle_exception(e.status(), e.str());
        } catch (...) {
            return handle_exception(reply::status_type::internal_server_error, fmt::to_string(std::current_exception()));
        }
    });
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
