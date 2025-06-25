/*
 * Copyright (C) 2025-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <random>
#include <ranges>
#include <seastar/http/short_streams.hh>
#include <seastar/coroutine/switch_to.hh>
#include <seastar/coroutine/as_future.hh>

#include "cql3/query_result_printer.hh"
#include "utils/rjson.hh"
#include "tools/webshell/webshell.hh"

#include "resources/tools/webshell/webshell.resources.hh"

using namespace httpd;
using request = http::request;
using reply = http::reply;

using namespace tools::webshell;

namespace tools::webshell {

static logging::logger wslog("webshell");

enum class output_format {
    text, json
};

sstring output_format_to_content_type(output_format of) {
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

class session_manager;

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

}

namespace tools::webshell {

struct session_options {
    output_format format = output_format::text;
};

class session {
public:
    session_id id;
    service::client_state client_state;
    scheduling_group scheduling_group;
    session_options options;

    session_manager& _manager;
    seastar::timer<lowres_clock> _ttl_timer;

    session(session_manager& manager, session_id session_id, service::client_state client_state, ::scheduling_group sg);

    sstring auth_user() const {
        return client_state.user().value().name.value_or("anonymous");
    }

    void on_ttl_expired();
    void refresh();
};

class session_manager {
    const config& _cfg;
    cql3::query_processor& _qp;
    auth::service& _auth_service;
    qos::service_level_controller& _sl_controller;

    std::function<session_manager&()> _get_local_manager;

    std::unordered_map<session_id, session> _sessions;

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

    session& create_session(service::client_state client_state, scheduling_group sg) {
        auto session_id = session_id::gen();

        wslog.debug("creating session with session_id {} for user {}", session_id, client_state.user().value().name.value_or("anonymous"));

        auto it = _sessions.emplace(session_id, session{*this, session_id, std::move(client_state), sg});
        if (!it.second) {
            throw std::runtime_error("Failed to create new session, session already exists");
        }
        return it.first->second;
    }

    bool has_session(const session_id& session_id) const noexcept {
        return _sessions.find(session_id) != _sessions.end();
    }

    session& get_session(const session_id& session_id) {
        auto it = _sessions.find(session_id);
        if (it == _sessions.end()) {
            throw unauthorized_access("Session not found");
        }
        return it->second;
    }

    void remove_session(const session_id& session_id) {
        auto it = _sessions.find(session_id);
        if (it != _sessions.end()) {
            _sessions.erase(it);
        }
    }

    template <std::invocable<session_manager&, session*> F>
    auto invoke_on_unchecked(session_id session_id, F f) {
        return smp::submit_to(session_id.shard(), [this, session_id, f = std::move(f)] () mutable {
            auto& local_this = _get_local_manager();
            session* session_ptr{nullptr};
            auto it = local_this._sessions.find(session_id);
            if (it != local_this._sessions.end()) {
                session_ptr = &it->second;
            }
            return f(local_this, session_ptr);
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
};

session::session(session_manager& manager, session_id session_id, service::client_state client_state, ::scheduling_group sg)
    : id(std::move(session_id))
    , client_state(std::move(client_state))
    , scheduling_group(sg)
    , _manager(manager)
    , _ttl_timer([this] { on_ttl_expired(); })
{
    _ttl_timer.arm(lowres_clock::now() + _manager.config().session_ttl);
}

void session::on_ttl_expired() {
    wslog.debug("session with session_id {} expired", id);
    _manager.remove_session(id);
}

void session::refresh() {
    _ttl_timer.rearm(lowres_clock::now() + _manager.config().session_ttl);
}

} // namespace webshell

namespace {

std::unordered_map<sstring, sstring> parse_cookies(sstring cookie_header) {
    wslog.trace("parse_cookies({})", cookie_header);

    std::unordered_map<sstring, sstring> cookies;

    auto strip = [] (sstring& s) {
        auto sv = std::string_view(s);
        auto start = sv.find_first_not_of(" \t");
        auto end = sv.find_last_not_of(" \t");
        s = sstring(sv.substr(start, end - start + 1));
    };

    for (const auto cookie_pair : std::views::split(cookie_header, ';')) {
        auto parts = std::views::split(cookie_pair, '=') | std::ranges::to<std::vector<sstring>>();
        std::ranges::for_each(parts, strip);
        if (parts.size() == 2) {
            cookies.emplace(parts[0], parts[1]);
        } else if (parts.size() == 1) {
            cookies.emplace(parts[0], "");
        } else {
            throw bad_request_exception(format("Invalid cookie format for cookie {}", sstring(cookie_pair.begin(), cookie_pair.end())));
        }
    }

    return cookies;
}

void refresh_session(config cfg, session_id session_id, sstring auth_user, reply& rep) {
    const auto max_age = std::chrono::duration_cast<std::chrono::seconds>(cfg.session_ttl).count();

    // TODO: Secure cookie (need HTTPs)
    rep.set_cookie("session_id", format("{}; HttpOnly; Max-Age={}", session_id, max_age));

    // Cookies visible to JavaScript
    rep.set_cookie("user_name", format("{}; Max-Age={}", auth_user, max_age));
    rep.set_cookie("cluster_name", format("{}; Max-Age={}", cfg.cluster_name, max_age));
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

class gated_handler : public handler_base {
    const char* _name;
    gate& _gate;
public:
    explicit gated_handler(const char* name, gate& gate) : _name(name), _gate(gate) {}
    virtual future<std::unique_ptr<reply>> do_handle(const sstring& path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) = 0;
    virtual future<std::unique_ptr<reply>> handle(const sstring& path_, std::unique_ptr<request> req, std::unique_ptr<reply> rep) final override {
        const auto path = path_;
        const auto method = req->_method;
        wslog.trace("handler: {} {} {}", _name, method, path);

        try {
            auto ret = co_await with_gate(_gate, [this, &path, req = std::move(req), rep = std::move(rep)] () mutable {
                return do_handle(path, std::move(req), std::move(rep));
            });
            wslog.trace("handler: {} {} {} {}", _name, method, path, ret->_status);
            co_return ret;
        } catch (httpd::base_exception& e) {
            // Prevent the fall-through to the default handler below, which converts unknonw exceptions to 500 Internal Server Error
            // Exceptions derived from base_exception already have a proper status code set, so re-throw them as is.
            wslog.trace("handler: {} {} {} {}", _name, method, path, e.status());
            throw;
        } catch (...) {
            wslog.trace("handler: {} {} {} {}", _name, method, path, reply::status_type::internal_server_error);
            throw server_error_exception(fmt::to_string(std::current_exception()));
        }
    }
};

class resource_handler : public gated_handler {
public:
    explicit resource_handler(gate& pending_requests) : gated_handler("resource", pending_requests) {}
protected:
    virtual future<std::unique_ptr<reply>> do_handle(const sstring& path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) override {
        if (httpd::str2type(req->_method) != operation_type::GET) {
            rep->set_status(reply::status_type::not_found);
            return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
        }

        const sstring file_path = path == "/" ? "webshell.html" : path.substr(1); // Remove leading slash

        auto resource_it = std::ranges::find_if(resources::webshell_resources_manifest, [&file_path] (const resources::resource& r) { return r.name == file_path; });
        if (resource_it == std::end(resources::webshell_resources_manifest)) {
            rep->set_status(reply::status_type::not_found);
            return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
        }

        rep->add_header("Content-Encoding", "gzip");
        rep->set_status(reply::status_type::ok);
        rep->write_body("text", [content = resource_it->content] (output_stream<char>&& out_) -> future<> {
            auto out = std::move(out_);
            std::exception_ptr ex;
            try {
                co_await out.write(reinterpret_cast<const char*>(content.data()), content.size());
                co_await out.flush();
            } catch (...) {
                ex = std::current_exception();
            }
            co_await out.close();
            if (ex) {
                co_await coroutine::return_exception_ptr(std::move(ex));
            }
        });
        rep->set_mime_type(resource_it->content_type);

        return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
    }
};

class login_handler : public gated_handler {
    constexpr static size_t max_authentication_credentials_length = 128 * 1024; // Maximum length of authentication credentials, just for sanity

    session_manager& _session_manager;
public:
    login_handler(gate& pending_requests, session_manager& session_manager) : gated_handler("login", pending_requests), _session_manager(session_manager) {}
protected:
    virtual future<std::unique_ptr<reply>> do_handle(const sstring& path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) override {
        const auto cookies = parse_cookies(req->get_header("Cookie"));
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

        auto& session = _session_manager.create_session(std::move(client_state), sg);

        refresh_session(_session_manager.config(), session.id, session.auth_user(), *rep);

        rep->set_status(reply::status_type::ok);

        co_return std::move(rep);
    }
};

class logout_handler : public gated_handler {
    session_manager& _session_manager;
public:
    logout_handler(gate& pending_requests, session_manager& session_manager) : gated_handler("logout", pending_requests), _session_manager(session_manager) {}
protected:
    virtual future<std::unique_ptr<reply>> do_handle(const sstring& path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) override {
        const auto cookies = parse_cookies(req->get_header("Cookie"));
        auto [session_id_opt, _] = try_get_session_id(cookies);

        if (!session_id_opt) {
            rep->set_status(reply::status_type::ok);
            rep->write_body("text", "Already logged out");
            co_return std::move(rep);
        }

        const auto response = co_await _session_manager.invoke_on_unchecked(*session_id_opt,
                [] (session_manager& session_manager, session* session_ptr) {
            if (session_ptr) {
                session_manager.remove_session(session_ptr->id);
                return "Successfully logged out";
            }
            return "Already logged out";
        });

        rep->set_status(reply::status_type::ok);

        // Erase cookies, relies on well-behaved client.
        // Not a problem because we dropped the session internally.
        rep->set_cookie("session_id", "; HttpOnly; Max-Age=0");
        rep->set_cookie("user_name", "; Max-Age=0");
        rep->set_cookie("cluster_name", "; Max-Age=0");

        rep->write_body("text", response);
        co_return std::move(rep);
    }
};

class query_result_visitor : public cql_transport::messages::result_message::visitor {
    output_format _output_format;
    std::ostream& _os;
private:
    [[noreturn]] void throw_on_unexpected_message(const char* message_kind) {
        throw std::runtime_error(std::format("unexpected result message {}", message_kind));
    }
public:
    query_result_visitor(output_format of, std::ostream& os) : _output_format(of), _os(os) { }
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
    virtual void visit(const cql_transport::messages::result_message::bounce_to_shard&) override {
        throw_on_unexpected_message("bounce_to_shard");
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
    }
};

class query_handler : public gated_handler {
    session_manager& _session_manager;

public:
    query_handler(gate& pending_requests, session_manager& session_manager)
        : gated_handler("query", pending_requests)
        , _session_manager(session_manager)
    { }

protected:
    virtual future<std::unique_ptr<reply>> do_handle(const sstring& path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) override {
        const auto cookies = parse_cookies(req->get_header("Cookie"));
        const auto session_id = get_session_id(cookies);

        const auto query = co_await util::read_entire_stream_contiguous(*req->content_stream);

        auto fut = co_await coroutine::as_future(_session_manager.invoke_on(session_id,
                    [query = std::move(query)] (session_manager& session_manager, session& session)
                    -> future<std::tuple<sstring, session_options, sstring>> {
            // TODO: limit concurrency using service permit facility
            auto query_state = service::query_state(session.client_state, empty_service_permit());

            auto& qp = session_manager.qp();

            auto result = co_await qp.execute_direct(query, query_state, {}, cql3::query_options::DEFAULT);
            result->throw_if_exception();

            std::stringstream os;

            query_result_visitor visitor(session.options.format, os);
            result->accept(visitor);

            session.refresh();

            co_return std::tuple(os.str(), session.options, session.auth_user());
        }));

        try {
            auto [result, options, auth_user] = fut.get();

            refresh_session(_session_manager.config(), session_id, auth_user, *rep);

            rep->set_status(reply::status_type::ok);
            rep->write_body(output_format_to_content_type(options.format), std::move(result));
            co_return std::move(rep);
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
            rep->set_status(reply::status_type::forbidden);
            rep->write_body("text", e.get_message());
            co_return std::move(rep);
        } catch (exceptions::syntax_exception& e) {
            rep->set_status(reply::status_type::bad_request);
            rep->write_body("text", format("Syntax error: {}", e.get_message()));
            co_return std::move(rep);
        } catch (exceptions::request_validation_exception& e) {
            rep->set_status(reply::status_type::bad_request);
            rep->write_body("text", e.get_message());
            co_return std::move(rep);
        }
    }
};

class option_handler : public gated_handler {
    session_manager& _session_manager;

public:
    option_handler(gate& pending_requests, session_manager& session_manager)
        : gated_handler("option", pending_requests)
        , _session_manager(session_manager)
    { }

protected:
    virtual future<std::unique_ptr<reply>> do_handle(const sstring& path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) override {
        const auto cookies = parse_cookies(req->get_header("Cookie"));
        const session_id session_id = get_session_id(cookies);

        const auto option = co_await util::read_entire_stream_contiguous(*req->content_stream) | std::views::transform([] (char c) { return std::tolower(c); }) | std::ranges::to<sstring>();

        const auto components = std::views::split(option, ' ') | std::ranges::to<std::vector<sstring>>();

        if (components.empty()) {
            throw bad_request_exception("Empty option");
        }

        const auto auth_user = co_await _session_manager.invoke_on(session_id, [&components] (session_manager& session_manager, session& session) {
            if (components[0] == "output-format") {
                if (components.size() != 2) {
                    throw bad_request_exception("Invalid output-format option, expected 'output-format <format>'");
                }

                sstring format_str = components[1];
                if (format_str == "text") {
                    session.options.format = output_format::text;
                } else if (format_str == "json") {
                    session.options.format = output_format::json;
                } else {
                    throw bad_request_exception(format("Unknown output format: {}", format_str));
                }
            } else {
                throw bad_request_exception(format("Unrecognized option: {}", components[0]));
            }

            session.refresh();
            return session.auth_user();
        });

        refresh_session(_session_manager.config(), session_id, auth_user, *rep);

        rep->set_status(reply::status_type::ok);
        co_return std::move(rep);
    }
};

} // anonymous namespace

namespace tools::webshell {

void server::set_routes(routes& r) {
    r.add_default_handler(new resource_handler(_pending_requests));
    r.put(operation_type::POST, "/login", new login_handler(_pending_requests, *_session_manager));
    r.put(operation_type::POST, "/logout", new logout_handler(_pending_requests, *_session_manager));
    r.put(operation_type::POST, "/query", new query_handler(_pending_requests, *_session_manager));
    r.put(operation_type::POST, "/option", new option_handler(_pending_requests, *_session_manager));
}

server::server(config cfg, cql3::query_processor& qp, auth::service& auth_service, qos::service_level_controller& sl_controller)
    : _cfg(cfg)
    , _http_server("scylladb-webshell")
    , _pending_requests("webshell")
    , _session_manager(std::make_unique<session_manager>(_cfg, qp, auth_service, sl_controller, [this] () -> session_manager& {
        return *container().local()._session_manager;
    }))
{
}

server::~server() {
}

future<> server::init(net::inet_address addr, uint16_t port) {
    set_routes(_http_server._routes);
    _http_server.set_content_length_limit(server::content_length_limit);
    _http_server.set_content_streaming(true);
    co_await coroutine::switch_to(_cfg.scheduling_group);
    co_await _http_server.listen(socket_address{addr, port});
}

future<> server::stop() {
    co_await _http_server.stop();
    co_await _pending_requests.close();
}

} // namespace webshell
