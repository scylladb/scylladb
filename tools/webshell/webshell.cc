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
#include "utils/rjson.hh"

using namespace httpd;
using request = http::request;
using reply = http::reply;

namespace rjs = rjson::schema;

using namespace tools::webshell;

namespace tools::webshell {

static logger wslog("webshell");

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
    const session_id id;
    session_options options;

    service::client_state client_state;
    tracing::trace_state_ptr trace_state;
    sstring last_query;

    scheduling_group scheduling_group;
    sstring user_agent;
    bool is_https;

private:
    seastar::timer<lowres_clock> _ttl_timer;
    semaphore _semaphore{1}; // enforce one concurrent request per session

public:
    session(session_id session_id, service::client_state client_state, ::scheduling_group sg, sstring user_agent, bool is_https, noncopyable_function<void(::session_id)> expire_callback)
        : id(std::move(session_id))
        , client_state(std::move(client_state))
        , scheduling_group(sg)
        , user_agent(std::move(user_agent))
        , is_https(is_https)
        , _ttl_timer([expire_callback = std::move(expire_callback), id = id] {
            wslog.debug("session with session_id {} expired", id);
            expire_callback(id);
        })
    {
    }

    void refresh(db_clock::duration session_ttl) {
        _ttl_timer.rearm(lowres_clock::now() + session_ttl);
    }

    sstring auth_user() const {
        return client_state.user().value().name.value_or("anonymous");
    }

    friend class session_manager;
};

class session_manager {
    const config& _config;
    cql3::query_processor& _qp;
    auth::service& _auth_service;
    qos::service_level_controller& _sl_controller;

    std::function<session_manager&()> _get_local_manager;

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
        it->second->refresh(_config.session_ttl);
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

    // Invokes f on the local shard, passing session options as well as mutable references to client state, trace state and last query string.
    // The session is locked for the duration of the call.
    template <std::invocable<const session_options&, service::client_state&, tracing::trace_state_ptr&, sstring&> F>
    auto invoke_on_local_shard(session_id session_id, F f) -> futurize_t<std::invoke_result_t<F, const session_options&, service::client_state&, tracing::trace_state_ptr&, sstring&>> {
        struct remote_session {
            lw_shared_ptr<session> session;
            semaphore_units<> session_lock;
            service::client_state::client_state_for_another_shard gcs;
            tracing::global_trace_state_ptr gts;
        };
        auto rs = co_await smp::submit_to(session_id.shard(), [this, session_id] () mutable -> future<foreign_ptr<std::unique_ptr<remote_session>>> {
           auto& local_this = _get_local_manager();

           auto it = local_this._sessions.find(session_id);
           if (it == local_this._sessions.end()) {
               throw unauthorized_access("Session not found");
           }

           auto& session = *it->second;
           co_return make_foreign(std::make_unique<remote_session>(remote_session{
               .session = it->second,
               .session_lock = co_await get_units(session._semaphore, 1),
               .gcs = session.client_state.move_to_other_shard(),
               .gts = tracing::global_trace_state_ptr(session.trace_state)}));
        });

        service::client_state client_state = rs->gcs.get();
        tracing::trace_state_ptr trace_state = rs->gts.get();
        sstring last_query = rs->session->last_query;
        auto res = co_await f(rs->session->options, client_state, trace_state, last_query);

        co_await smp::submit_to(session_id.shard(), [this, rs = std::move(rs), gcs = tracing::global_trace_state_ptr(trace_state), &last_query] () mutable {
            auto& session = *rs->session;

            session.trace_state = gcs;
            session.last_query = std::move(last_query);

            session.refresh(_get_local_manager().config().session_ttl);

            rs.release();
        });

        co_return std::move(res);
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

std::unique_ptr<reply> write_response(std::unique_ptr<reply> rep, reply::status_type status, sstring response) {
    rep->set_status(status);
    rep->write_body("json", [response = std::move(response)] (output_stream<char>&& out_) -> future<> {
        auto out = std::move(out_);

        co_await out.write("{\"response\": ");

        co_await out.write(rjson::quote_json_string(response));

        co_await out.write("}");

        co_await out.flush();
        co_await out.close();
    });
    return rep;
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
            co_return write_response(std::move(rep), reply::status_type::service_unavailable, "Too many requests, try again later");
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
            // Prevent the fall-through to the default handler below, which converts unknown exceptions to 500 Internal Server Error
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
                co_return write_response(std::move(rep), reply::status_type::ok, "Already logged in, erase cookies or send request to /logout to log in with another user.");
            }
        }

        if (_session_manager.session_count() >= _session_manager.config().max_sessions) {
            co_return write_response(std::move(rep), reply::status_type::service_unavailable, "Too many sessions, try again later");
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
        sstring success_response;
        if (auth.require_authentication()) {
            if (req->content_length == 0) {
                co_return write_response(std::move(rep), reply::status_type::bad_request,
                        "No credentials provided, provide credentials in the request body in the format: {\"username\": \"$username\", \"password\": \"$password\"}");
            }
            if (req->content_length > max_authentication_credentials_length) {
                co_return write_response(std::move(rep), reply::status_type::bad_request,
                        format("Credentials too long, max length is {}", max_authentication_credentials_length));
            }

            auto credentials = rjson::parse_and_validate(
                    co_await util::read_entire_stream_contiguous(*req->content_stream),
                    rjs::object({
                        {"username", rjs::scalar::string()},
                        {"password", rjs::scalar::string()}
                    }));
            if (!credentials) {
                co_return write_response(std::move(rep), reply::status_type::bad_request, credentials.error());
            }

            const auto& username = (*credentials)["username"];
            const auto& password = (*credentials)["password"];

            bytes_ostream buf;
            buf.write(username.GetString(), username.GetStringLength()); // authzId (username)
            buf.write("\0", 1); // Add NUL byte as delimiter
            buf.write(username.GetString(), username.GetStringLength()); // authnId (username)
            buf.write("\0", 1); // Add NUL byte as delimiter
            buf.write(password.GetString(), password.GetStringLength()); // password
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
                    co_return write_response(std::move(rep), reply::status_type::internal_server_error, "Configured SASL is a multistage authentication mechanism, currently unsupported by webshell");
                }

                success_response = format("Successfully logged in as user {}", client_state.user().value().name.value());
            } catch (exceptions::authentication_exception& e) {
                co_return write_response(std::move(rep), reply::status_type::bad_request, e.what());
            }
        } else {
            success_response = "Successfully logged in as anonymous user";
        }

        const auto user_agent = req->get_header("User-Agent");

        auto& session = _session_manager.create_session(std::move(client_state), sg, std::move(user_agent), _is_https);

        set_session_cookies(*rep, _session_manager.config(), session.id, session.auth_user());

        co_return write_response(std::move(rep), reply::status_type::ok, std::move(success_response));
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
            co_return write_response(std::move(rep), reply::status_type::ok, "Already logged out");
        }

        const auto response = co_await _session_manager.invoke_on_unchecked(*session_id_opt,
                [] (session_manager& session_manager, session* session_ptr) {
            if (session_ptr) {
                session_manager.remove_session(session_ptr->id);
                return "Successfully logged out";
            }
            return "Already logged out";
        });

        // Erase cookies, relies on well-behaved client.
        // Not a problem because we dropped the session internally.
        for (const auto& cookie_name : session_cookies) {
            erase_session_cookie(*rep, cookie_name);
        }

        co_return write_response(std::move(rep), reply::status_type::ok, std::move(response));
    }
};

using query_result = std::variant<sstring, shared_ptr<cql_transport::messages::result_message::rows>>;

struct query_exec_success {
    query_result result;
    session_options options;
    tracing::trace_state_ptr trace_state;

    query_exec_success(query_result result, session_options options, tracing::trace_state_ptr trace_state)
        : result(std::move(result)), options(std::move(options)), trace_state(std::move(trace_state))
    { }
};

struct query_exec_failure {
    reply::status_type status;
    sstring message;

    query_exec_failure(reply::status_type status, sstring message) : status(status), message(std::move(message)) { }
};

using query_exec_result = std::expected<query_exec_success, query_exec_failure>;

class json_escaping_data_sink : public data_sink_impl {
    output_stream<char>& _os;
public:
    json_escaping_data_sink(output_stream<char>& os) :_os(os) { }
    virtual future<> put(std::span<temporary_buffer<char>> data) override {
        for (auto& buf : data) {
            co_await _os.write(rjson::escape_json_string(std::string_view(buf.get(), buf.size())));
        }
    }
    virtual future<> flush() override { return _os.flush(); }
    virtual future<> close() override { return make_ready_future<>(); }
    virtual size_t buffer_size() const noexcept override { return 128*1024; }
    virtual bool can_batch_flushes() const noexcept override { return false; }
    virtual void on_batch_flush_error() noexcept override { }
};

std::unique_ptr<reply> write_response(std::unique_ptr<reply> rep, query_exec_result result) {
    rep->set_status(result ? reply::status_type::ok : result.error().status);
    rep->write_body("json", [result = std::move(result)] (output_stream<char>&& out_) mutable -> future<> {
        auto out = std::move(out_);
        std::optional<output_stream<char>> json_escaping_os_opt;

        try {
            co_await out.write("{\"response\":");

            if (result) {
                if (std::holds_alternative<sstring>(result->result)) {
                    co_await out.write(rjson::quote_json_string(std::get<sstring>(result->result)));
                } else {
                    const auto& rows = *std::get<shared_ptr<cql_transport::messages::result_message::rows>>(result->result);
                    switch (result->options.output_format) {
                        case output_format::text:
                        {
                            co_await out.write("\"");

                            json_escaping_os_opt.emplace(data_sink(std::make_unique<json_escaping_data_sink>(out)));
                            co_await cql3::print_query_results_text(*json_escaping_os_opt, rows.rs(), result->options.expand);
                            co_await json_escaping_os_opt->flush();
                            co_await json_escaping_os_opt->close();

                            co_await out.write("\"");
                            json_escaping_os_opt.reset();
                            break;
                        }
                        case output_format::json:
                            co_await cql3::print_query_results_json(out, rows.rs());
                            break;
                    }
                    if (rows.rs().get_metadata().flags().contains<cql3::metadata::flag::HAS_MORE_PAGES>()) {
                        co_await out.write(format(",\"paging_state\":\"{}\"", base64_encode(*rows.rs().get_metadata().paging_state()->serialize())));
                    }
                    if (result->trace_state) {
                        co_await out.write(format(",\"trace_session_id\":\"{}\"", result->trace_state->session_id()));
                    }
                }
            } else {
                co_await out.write(rjson::quote_json_string(result.error().message));
            }

            co_await out.write("}");

            co_await out.flush();
        } catch (...) {
            // Caller cannot handle exceptions at this point, we already sent HTTP OK in the header.
            // Best we can do is log the exception, the client will get a truncated response.
            wslog.error("Unexpected exception while writing query response: {}", std::current_exception());
        }

        if (json_escaping_os_opt) {
            co_await json_escaping_os_opt->close();
        }
        co_await out.close();
    });
    return rep;
}

class query_result_visitor : public cql_transport::messages::result_message::visitor {
    shared_ptr<cql_transport::messages::result_message> _result_msg;
    std::optional<query_result> _query_result;
private:
    [[noreturn]] void throw_on_unexpected_message(const char* message_kind) {
        throw std::runtime_error(std::format("unexpected result message {}", message_kind));
    }

    virtual void visit(const cql_transport::messages::result_message::void_message&) override {
        _query_result.emplace("");
    }
    virtual void visit(const cql_transport::messages::result_message::set_keyspace& msg) override {
        _query_result.emplace(format("Successfully set keyspace {}", msg.get_keyspace()));
    }
    virtual void visit(const cql_transport::messages::result_message::prepared::cql& msg) override {
        _query_result.emplace(format("Query prepared with id {}", to_hex(msg.get_id())));
    }
    virtual void visit(const cql_transport::messages::result_message::schema_change& msg) override {
        auto& event = *msg.get_change();
        sstring action, what;
        switch (event.change) {
            case cql_transport::event::schema_change::change_type::CREATED:
                action = "Created ";
                break;
            case cql_transport::event::schema_change::change_type::UPDATED:
                action = "Updated ";
                break;
            case cql_transport::event::schema_change::change_type::DROPPED:
                action = "Dropped ";
                break;
        }
        switch (event.target) {
            case cql_transport::event::schema_change::target_type::KEYSPACE:
                what = "keyspace";
                break;
            case cql_transport::event::schema_change::target_type::TABLE:
                what = "table";
                break;
            case cql_transport::event::schema_change::target_type::TYPE:
                what = "type";
                break;
            case cql_transport::event::schema_change::target_type::FUNCTION:
                what = "function";
                break;
            case cql_transport::event::schema_change::target_type::AGGREGATE:
                what = "aggregate";
                break;
        }
        _query_result.emplace(format("{} {}", action, what));
    }
    virtual void visit(const cql_transport::messages::result_message::bounce_to_shard&) override {
        throw_on_unexpected_message("bounce_to_shard");
    }
    virtual void visit(const cql_transport::messages::result_message::exception&) override {
        throw_on_unexpected_message("exception");
    }
    virtual void visit(const cql_transport::messages::result_message::rows&) override {
        _query_result.emplace(dynamic_pointer_cast<cql_transport::messages::result_message::rows>(_result_msg));
    }
public:
    query_result_visitor(shared_ptr<cql_transport::messages::result_message> result_msg) : _result_msg(std::move(result_msg)) {
        _result_msg->accept(*this);
    }
    query_result get() && {
        if (!_query_result) {
            throw std::runtime_error("query_result_visitor: no result");
        }
        return std::move(_query_result).value();
    }
};

class query_handler : public gated_handler {
    session_manager& _session_manager;

    static tracing::trace_state_ptr setup_tracing(const session_options& options, tracing::trace_state_ptr trace_state, const service::client_state& client_state, std::string_view query) {
        if (!options.tracing) {
            return {};
        }

        if (trace_state) {
            return trace_state;
        }

        tracing::trace_state_props_set trace_props;
        trace_props.set<tracing::trace_state_props::full_tracing>();

        trace_state = tracing::tracing::get_local_tracing_instance().create_session(tracing::trace_type::QUERY, trace_props);
        tracing::begin(trace_state, "Execute webshell query", client_state.get_client_address());
        tracing::add_session_param(trace_state, "session_options", fmt::to_string(options));
        tracing::add_query(trace_state, query);

        return trace_state;
    }

    static future<std::expected<::shared_ptr<cql_transport::messages::result_message>, query_exec_failure>>
    do_execute_query_on_shard(cql3::query_processor& qp, const session_options& options, service::client_state& client_state,
            std::string_view query, tracing::trace_state_ptr trace_state, const bytes_opt& serialized_paging_state) {
        auto query_state = service::query_state(client_state, trace_state, empty_service_permit());

        lw_shared_ptr<service::pager::paging_state> paging_state;
        if (serialized_paging_state) {
            try {
                paging_state = service::pager::paging_state::deserialize(*serialized_paging_state);
            } catch (...) {
                co_return std::unexpected(query_exec_failure(reply::status_type::bad_request, format("Invalid paging_state: {}", std::current_exception())));
            }
        }

        const auto specific_options = cql3::query_options::specific_options{
                options.page_size,
                std::move(paging_state),
                options.serial_consistency,
                api::missing_timestamp,
                service::node_local_only::no};

        auto query_options = cql3::query_options{cql3::default_cql_config, options.consistency, std::nullopt, std::vector<cql3::raw_value_view>(), false, specific_options};

        auto result = co_await qp.execute_direct(query, query_state, {}, query_options);
        result->throw_if_exception();

        co_return std::move(result);
    }

    static future<query_exec_result>
    do_execute_query(sharded<cql3::query_processor>& qp, const session_options& options, service::client_state& client_state,
            std::string_view query, tracing::trace_state_ptr trace_state, const bytes_opt& serialized_paging_state) {
        tracing::trace(trace_state, "executing webshell query");
        wslog.trace("executing query {} with options {}", query, options);

        auto res = co_await do_execute_query_on_shard(qp.local(), options, client_state, query, trace_state, serialized_paging_state);

        if (!res) {
            co_return std::unexpected(res.error());
        }

        if (!res.value()->move_to_shard()) {
            co_return query_exec_success(query_result_visitor(std::move(res.value())).get(), options, trace_state);
        }

        // Handle bounce to another shard
        const auto shard = *res.value()->move_to_shard();
        auto gcs = client_state.move_to_other_shard();
        auto gts = tracing::global_trace_state_ptr(trace_state);

        tracing::trace(trace_state, "query bounced to shard {}", shard);
        wslog.trace("query bounced to shard {}", shard);

        co_return co_await qp.invoke_on(shard, [&gcs, &gts, &options, query, &serialized_paging_state] (cql3::query_processor& qp) -> future<query_exec_result> {
            auto client_state = gcs.get();
            auto trace_state = gts.get();
            auto res = co_await do_execute_query_on_shard(qp, options, client_state, query, std::move(trace_state), serialized_paging_state);

            if (!res) {
                co_return std::unexpected(res.error());
            }

            if (auto shard_opt = res.value()->move_to_shard(); shard_opt) {
                throw std::runtime_error(format("Unexpected bounce to another shard, after handling a bounce to shard {}", *shard_opt));
            }

            auto query_result = query_result_visitor(std::move(res.value())).get();
            if (std::holds_alternative<sstring>(query_result)) {
                co_return query_exec_success(std::get<sstring>(std::move(query_result)), options, trace_state);
            } else {
                throw std::runtime_error(format("Unexpected rows result, after handling a bounce to shard {}", this_shard_id()));
            }
        });
    }

public:
    query_handler(request_control& request_control, session_manager& session_manager)
        : gated_handler("query", request_control)
        , _session_manager(session_manager)
    { }

    static future<query_exec_result> execute_query(sharded<cql3::query_processor>& qp, const session_options& options, service::client_state& client_state,
            std::string_view query, tracing::trace_state_ptr trace_state, const bytes_opt& serialized_paging_state) {
        trace_state = setup_tracing(options, std::move(trace_state), client_state, query);

        try {
            co_return co_await do_execute_query(qp, options, client_state, query, std::move(trace_state), serialized_paging_state);
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
            co_return std::unexpected(query_exec_failure(reply::status_type::forbidden, e.get_message()));
        } catch (exceptions::syntax_exception& e) {
            co_return std::unexpected(query_exec_failure(reply::status_type::bad_request, format("Syntax error: {}", e.get_message())));
        } catch (exceptions::request_validation_exception& e) {
            co_return std::unexpected(query_exec_failure(reply::status_type::bad_request, e.get_message()));
        }
    }

protected:
    virtual future<std::unique_ptr<reply>> do_handle(const sstring& path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) override {
        const auto cookies = handle_cookies(_session_manager.config(), *req, *rep);
        const auto session_id = get_session_id(cookies);

        auto request = rjson::parse_and_validate(
                co_await util::read_entire_stream_contiguous(*req->content_stream),
                rjs::object({
                    {"query", rjs::scalar::string()},
                    {"paging_state", rjs::optional(rjs::scalar::string())}
                }));

        if (!request) {
            co_return write_response(std::move(rep), reply::status_type::bad_request, request.error());
        }

        const auto query = rjson::to_string_view((*request)["query"]);
        bytes_opt serialized_paging_state;
        if (auto it = request->FindMember("paging_state"); it != request->MemberEnd() && !it->value.IsNull()) {
            try {
                serialized_paging_state = base64_decode(rjson::to_string_view(it->value));
            } catch (...) {
                co_return write_response(std::move(rep), reply::status_type::bad_request, "Invalid paging_state cookie: not valid base64");
            }
        }

        auto result = co_await _session_manager.invoke_on_local_shard(session_id, [&, this] (const session_options& options, service::client_state& client_state,
                tracing::trace_state_ptr& trace_state, sstring& last_query) mutable -> future<query_exec_result> {
            if (query != std::exchange(last_query, sstring(query))) {
                trace_state = nullptr;
                serialized_paging_state = std::nullopt;
            }

            auto res = co_await execute_query(_session_manager.qp().container(), options, client_state, query, trace_state, serialized_paging_state);

            if (res) {
                trace_state = res->trace_state;
            }

            co_return std::move(res);
        });

        co_return write_response(std::move(rep), std::move(result));
    }
};

class command_handler : public gated_handler {
    session_manager& _session_manager;

public:
    command_handler(request_control& request_control, session_manager& session_manager)
        : gated_handler("command", request_control)
        , _session_manager(session_manager)
    { }

protected:
    virtual future<std::unique_ptr<reply>> do_handle(const sstring& path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) override {
        (void)_session_manager;
        co_return std::move(rep);
    }
};

class option_handler : public gated_handler {
    session_manager& _session_manager;

public:
    option_handler(request_control& request_control, session_manager& session_manager)
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
    config _config;
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
    r.put(operation_type::POST, "/option", new option_handler(_request_control, _session_manager));

    r.register_exeption_handler([] (std::exception_ptr ex) {
        wslog.trace("handle exception: {}", ex);

        auto handle_exception = [] (reply::status_type status, sstring msg) {
            return write_response(std::make_unique<reply>(), status, std::move(msg));
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
    : _config(cfg)
    , _http_server("scylladb-webshell-http")
    , _https_server("scylladb-webshell-https")
    , _request_control("webshell", _config.max_concurrent_requests, _config.max_waiting_requests)
    , _session_manager(_config, qp, auth_service, sl_controller, [this] () -> session_manager& {
        return container().local()._session_manager;
    })
{
}

future<> server::init(std::optional<http_listen_config> http_cfg_opt, std::optional<https_listen_config> https_cfg_opt) {
    co_await coroutine::switch_to(_config.scheduling_group);

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

config make_config(const db::config& db_cfg, std::string_view cluster_name, scheduling_group sg) {
    return config{
        .cluster_name = sstring(cluster_name),
        .scheduling_group = sg,
        .listen_interface_prefer_ipv6 = db_cfg.listen_interface_prefer_ipv6(),
        .enable_ipv6_dns_lookup = db_cfg.enable_ipv6_dns_lookup(),
        .timeout_config = updateable_timeout_config(db_cfg),
        .webshell_http_address = db_cfg.webshell_http_address().empty() ? db_cfg.api_address() : db_cfg.webshell_http_address(),
        .webshell_http_port = db_cfg.webshell_http_port(),
        .webshell_https_address = db_cfg.webshell_https_address(),
        .webshell_https_port = db_cfg.webshell_https_port(),
        .webshell_https_encryption_options = db_cfg.webshell_https_encryption_options(),
        .webshell_resource_manifest_path = std::filesystem::path(db_cfg.webshell_resource_manifest_path()),
    };
}

controller::controller(sharded<cql3::query_processor>& qp, sharded<auth::service>& auth_service,
        sharded<qos::service_level_controller>& sl_controller, config cfg)
    : protocol_server(cfg.scheduling_group)
    , _qp(qp)
    , _auth_service(auth_service)
    , _sl_controller(sl_controller)
    , _config(cfg)
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

        co_await _server.start(_config, std::ref(_qp), std::ref(_auth_service), std::ref(_sl_controller));

        auto preferred = _config.listen_interface_prefer_ipv6 ? std::make_optional(net::inet_address::family::INET6) : std::nullopt;
        auto family = _config.enable_ipv6_dns_lookup || preferred ? std::nullopt : std::make_optional(net::inet_address::family::INET);

        std::optional<tools::webshell::http_listen_config> http_cfg_opt;
        if (_config.webshell_http_port) {
            http_cfg_opt.emplace(tools::webshell::http_listen_config{
                    .address = co_await gms::inet_address::lookup(_config.webshell_http_address, family),
                    .port = _config.webshell_http_port});
            _listen_addresses.push_back({http_cfg_opt->address, http_cfg_opt->port});

            uris.push_back(format("http://{}:{}", _config.webshell_http_address, http_cfg_opt->port));
        }

        std::optional<tools::webshell::https_listen_config> https_cfg_opt;
        if (_config.webshell_https_port) {
            tls::credentials_builder creds;

            std::exception_ptr ex;
            co_await utils::configure_tls_creds_builder(creds, _config.webshell_https_encryption_options);

            https_cfg_opt.emplace(tools::webshell::https_listen_config{
                    .address = co_await gms::inet_address::lookup(_config.webshell_https_address, family),
                    .port = _config.webshell_https_port,
                    .creds = std::move(creds)});

            _listen_addresses.push_back({https_cfg_opt->address, https_cfg_opt->port});

            uris.push_back(format("https://{}:{}", _config.webshell_https_address, https_cfg_opt->port));
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

future<utils::chunked_vector<client_data>> controller::get_client_data() {
    return _server.local().get_client_data();
}

} // namespace webshell
