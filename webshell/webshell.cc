/*
 * Copyright (C) 2025-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <ranges>
#include <seastar/http/short_streams.hh>
#include <seastar/coroutine/switch_to.hh>

#include "cql3/query_result_printer.hh"
#include "utils/rjson.hh"
#include "webshell.hh"
#include "webshell.html.hh"

using namespace httpd;
using request = http::request;
using reply = http::reply;

using namespace webshell;

namespace webshell {

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

struct session {
    utils::UUID id;
    service::client_state client_state;
    output_format format = output_format::text;

    session(utils::UUID session_id, service::client_state client_state)
        : id(std::move(session_id)), client_state(std::move(client_state))
    { }
};

class session_manager {
    const config& _cfg;
    cql3::query_processor& _qp;
    auth::service& _auth_service;
    qos::service_level_controller& _sl_controller;

    std::unordered_map<utils::UUID, session> _sessions;

public:
    session_manager(const config& cfg, cql3::query_processor& qp, auth::service& auth_service, qos::service_level_controller& sl_controller)
        : _cfg(cfg), _qp(qp), _auth_service(auth_service), _sl_controller(sl_controller)
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

    session& create_session(service::client_state client_state) {
        auto session_id = utils::UUID_gen::get_time_UUID();
        auto it = _sessions.emplace(session_id, session{session_id, std::move(client_state)});
        if (!it.second) {
            throw std::runtime_error(format("Failed to create a new session, session with session_id {} already exists", session_id));
        }
        return it.first->second;
    }

    session& get_session(const utils::UUID& session_id) {
        auto it = _sessions.find(session_id);
        if (it == _sessions.end()) {
            throw std::runtime_error(format("Session with session_id {} not found", session_id));
        }
        return it->second;
    }
};

} // namespace webshell

namespace {

class gated_handler : public handler_base {
    gate& _gate;
public:
    explicit gated_handler(gate& gate) : _gate(gate) {}
    virtual future<std::unique_ptr<reply>> do_handle(const sstring& path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) = 0;
    virtual future<std::unique_ptr<reply>> handle(const sstring& path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) final override {
        return with_gate(_gate, [this, &path, req = std::move(req), rep = std::move(rep)] () mutable {
            return do_handle(path, std::move(req), std::move(rep));
        });
    }
};

class root_handler : public gated_handler {
public:
    explicit root_handler(gate& pending_requests) : gated_handler(pending_requests) {}
protected:
    virtual future<std::unique_ptr<reply>> do_handle(const sstring& path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) override {
        rep->set_status(reply::status_type::ok);
        rep->write_body("html", webshell_html);
        return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
    }
};

class login_handler : public gated_handler {
    session_manager& _session_manager;
public:
    login_handler(gate& pending_requests, session_manager& session_manager) : gated_handler(pending_requests), _session_manager(session_manager) {}
protected:
    virtual future<std::unique_ptr<reply>> do_handle(const sstring& path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) override {
        auto client_state = service::client_state(
                service::client_state::external_tag{},
                _session_manager.auth_service(),
                &_session_manager.sl_controller(),
                _session_manager.config().timeout_config.current_values(),
                req->get_client_address());

        auto& session = _session_manager.create_session(std::move(client_state));

        rep->add_header("Set-Cookie", format("session_id={}", session.id));
        rep->set_status(reply::status_type::ok);

        co_return std::move(rep);
    }
};

std::unordered_map<sstring, sstring> parse_cookies(sstring cookie_header) {
    std::unordered_map<sstring, sstring> cookies;

    for (const auto cookie_pair : std::views::split(cookie_header, "; ")) {
        const auto parts = std::views::split(cookie_pair, '=') | std::ranges::to<std::vector<sstring>>();
        if (parts.size() == 2) {
            cookies.emplace(parts[0], parts[1]);
        } else if (parts.size() == 1) {
            cookies.emplace(parts[0], "");
        } else {
            throw std::runtime_error(format("Invalid cookie format for cookie {}", cookie_pair));
        }
    }

    return cookies;
}

session& get_session(session_manager& session_manager, const std::unordered_map<sstring, sstring>& cookies) {
    auto it = cookies.find("session_id");
    if (it == cookies.end()) {
        throw std::runtime_error("session_id not found in cookies");
    }

    utils::UUID session_id;
    try {
        session_id = utils::UUID(it->second);
    } catch (...) {
        throw std::runtime_error(format("Invalid session_id, failed to parse it as a valid UUID: {}", std::current_exception()));
    }

    return session_manager.get_session(session_id);
}

class query_result_visitor : public cql_transport::messages::result_message::visitor {
    output_format _output_format;
    std::ostream& _os;
private:
    [[noreturn]] void throw_on_unexpected_message(const char* message_kind) {
        throw std::runtime_error(std::format("unexpected result message, expected rows, got {}", message_kind));
    }
public:
    query_result_visitor(output_format of, std::ostream& os) : _output_format(of), _os(os) { }
    virtual void visit(const cql_transport::messages::result_message::void_message&) override { throw_on_unexpected_message("void_message"); }
    virtual void visit(const cql_transport::messages::result_message::set_keyspace&) override { throw_on_unexpected_message("set_keyspace"); }
    virtual void visit(const cql_transport::messages::result_message::prepared::cql&) override { throw_on_unexpected_message("prepared::cql"); }
    virtual void visit(const cql_transport::messages::result_message::schema_change&) override { throw_on_unexpected_message("schema_change"); }
    virtual void visit(const cql_transport::messages::result_message::bounce_to_shard&) override { throw_on_unexpected_message("bounce_to_shard"); }
    virtual void visit(const cql_transport::messages::result_message::exception&) override { throw_on_unexpected_message("exception"); }

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
        : gated_handler(pending_requests)
        , _session_manager(session_manager)
    { }

protected:
    virtual future<std::unique_ptr<reply>> do_handle(const sstring& path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) override {
        auto cookies = parse_cookies(req->get_header("Cookie"));
        session& session = get_session(_session_manager, cookies);

        const auto query = co_await util::read_entire_stream_contiguous(*req->content_stream);

        auto query_state = service::query_state(session.client_state, empty_service_permit());

        auto& qp = _session_manager.qp();
        auto result = co_await qp.execute_direct(query, query_state, {}, cql3::query_options::DEFAULT);
        result->throw_if_exception();

        std::stringstream os;

        query_result_visitor visitor(session.format, os);
        result->accept(visitor);

        rep->set_status(reply::status_type::ok);
        rep->write_body(output_format_to_content_type(session.format), os.str());
        co_return std::move(rep);
    }
};

class option_handler : public gated_handler {
    session_manager& _session_manager;

public:
    option_handler(gate& pending_requests, session_manager& session_manager)
        : gated_handler(pending_requests)
        , _session_manager(session_manager)
    { }

protected:
    virtual future<std::unique_ptr<reply>> do_handle(const sstring& path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) override {
        auto cookies = parse_cookies(req->get_header("Cookie"));
        session& session = get_session(_session_manager, cookies);

        const auto option = co_await util::read_entire_stream_contiguous(*req->content_stream) | std::views::transform([] (char c) { return std::tolower(c); }) | std::ranges::to<sstring>();

        const auto components = std::views::split(option, ' ') | std::ranges::to<std::vector<sstring>>();

        if (components.empty()) {
            throw std::runtime_error("Empty option");
        }

        if (components[0] == "output-format") {
            if (components.size() != 2) {
                throw std::runtime_error("Invalid output-format option, expected 'output-format <format>'");
            }

            sstring format_str = components[1];
            if (format_str == "text") {
                session.format = output_format::text;
            } else if (format_str == "json") {
                session.format = output_format::json;
            } else {
                throw std::runtime_error(format("Unknown output format: {}", format_str));
            }
        } else {
            throw std::runtime_error(format("Unrecognized option: {}", components[0]));
        }

        rep->set_status(reply::status_type::ok);
        co_return std::move(rep);
    }
};

} // anonymous namespace

namespace webshell {

void server::set_routes(routes& r) {
    r.put(operation_type::GET, "/", new root_handler(_pending_requests));
    r.put(operation_type::POST, "/login", new login_handler(_pending_requests, *_session_manager));
    r.put(operation_type::POST, "/query", new query_handler(_pending_requests, *_session_manager));
    r.put(operation_type::POST, "/option", new option_handler(_pending_requests, *_session_manager));
}

server::server(config cfg, cql3::query_processor& qp, auth::service& auth_service, qos::service_level_controller& sl_controller)
    : _cfg(cfg)
    , _http_server("scylladb-webshell")
    , _pending_requests("webshell")
    , _session_manager(std::make_unique<session_manager>(_cfg, qp, auth_service, sl_controller))
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
