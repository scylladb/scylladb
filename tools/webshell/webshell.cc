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
#include <seastar/util/short_streams.hh>
#include <seastar/util/log.hh>

#include "cql3/query_processor.hh"
#include "db/config.hh"
#include "service/client_state.hh"
#include "tools/webshell/webshell.hh"
#include "utils/observable.hh"
#include "utils/rjson.hh"

using namespace httpd;
using request = http::request;
using reply = http::reply;

namespace rjs = rjson::schema;

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
            wslog.debug("session {} expired", id);
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

    size_t session_count() const noexcept {
        return _sessions.size();
    }

    bool has_session(const session_id& session_id) const noexcept {
        return _sessions.find(session_id) != _sessions.end();
    }

    session& create_session(service::client_state client_state, scheduling_group sg, sstring user_agent, bool is_https) {
        auto session_id = session_id::gen();

        wslog.debug("creating session {} for user {}", session_id, client_state.user().value().name.value_or("anonymous"));

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

const std::string_view session_cookies[] {
    "session_id",
    "session_digest",
    "user_name",
    "cluster_name",
};
const std::string_view http_only_session_cookies[] {
    "session_id",
};
// Session cookies the server derives itself, so a request carrying one is
// ignored rather than echoed: what these say is the server's own view of the
// session, and a client cannot be allowed to assert it.
const std::string_view server_owned_session_cookies[] {
    "session_digest",
};

// Attributes every session cookie carries, beyond HttpOnly and Max-Age.
//
// SameSite=Strict keeps the cookie off cross-site requests, so that a page the
// operator happens to be visiting cannot drive the shell with their session.
//
// Secure is set on the cookies of the HTTPS listener only, as a cookie marked
// Secure is not sent over plain HTTP at all - setting it unconditionally would
// break the HTTP listener rather than protect it. The HTTP listener is meant for
// local development, but nothing enforces that, so a session established over
// HTTPS must not be replayable in the clear even if both listeners are up.
static sstring session_cookie_attributes(bool is_https, bool http_only) {
    return seastar::format("{}{}SameSite=Strict; ", http_only ? "HttpOnly; " : "", is_https ? "Secure; " : "");
}

template <typename T>
void set_session_cookie(reply& rep, config cfg, bool is_https, std::string_view key, const T& value) {
    const bool http_only = std::ranges::find(http_only_session_cookies, key) != std::end(http_only_session_cookies);
    const auto max_age = std::chrono::duration_cast<std::chrono::seconds>(cfg.session_ttl).count();
    rep.set_cookie(sstring(key), fmt::format("{}; {}Max-Age={}", value, session_cookie_attributes(is_https, http_only), max_age));
}

// Report the digest the logs identify this session by.
//
// A session_id never appears in a log line - it is a credential, so the logs
// carry the digest that formatting a session_id produces instead. That leaves a
// client holding a session it cannot find in a log, which is why the digest is
// handed back here: it is what to search the log for, and what to quote in a bug
// report. Giving it out costs nothing, as it cannot be turned back into the id,
// and its owner is holding the id anyway.
//
// Not HttpOnly, unlike session_id: the point is for the web interface to be able
// to show it.
void set_session_digest_cookie(reply& rep, const config& cfg, bool is_https, session_id session_id) {
    // Formatting a session_id yields the digest, not the id; see the formatter.
    set_session_cookie(rep, cfg, is_https, "session_digest", session_id);
}

// FIXME: assumes the Cookie: <cookie-list> syntax, which most clients seems to
// use, but this is not guranteed. If a client uses multiple Cookie headers, this
// will not work.
std::unordered_map<sstring, sstring> handle_cookies(const config& cfg, bool is_https, const request& req, reply& rep) {
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

        if (std::ranges::find(server_owned_session_cookies, it->first) != std::end(server_owned_session_cookies)) {
            // Whatever the client sent here is not evidence of anything; the
            // value the server stands behind is written below.
            continue;
        }

        if (std::ranges::find(session_cookies, it->first) == std::end(session_cookies)) {
            rep.set_cookie(it->first, it->second);
        } else {
            set_session_cookie(rep, cfg, is_https, it->first, it->second);
        }
    }

    if (const auto it = cookies.find("session_id"); it != cookies.end()) {
        try {
            set_session_digest_cookie(rep, cfg, is_https, session_id(it->second));
        } catch (const std::invalid_argument&) {
            // Not a session_id at all, so there is no session to report a digest
            // for. The request is about to be rejected over the same thing.
        }
    }

    return cookies;
}

void set_session_cookies(reply& rep, const config& cfg, bool is_https, session_id session_id, sstring auth_user) {
    set_session_cookie(rep, cfg, is_https, "session_id", session_id.to_cookie_string());
    set_session_digest_cookie(rep, cfg, is_https, session_id);
    set_session_cookie(rep, cfg, is_https, "user_name", auth_user);
    set_session_cookie(rep, cfg, is_https, "cluster_name", cfg.cluster_name);
}

void erase_session_cookie(reply& rep, bool is_https, std::string_view key) {
    // Repeat the attributes the cookie was set with: a browser only replaces a
    // cookie when they match, so an expiry that dropped them could leave the
    // original in place.
    const bool http_only = std::ranges::find(http_only_session_cookies, key) != std::end(http_only_session_cookies);
    rep.set_cookie(sstring(key), fmt::format("; {}Max-Age=0", session_cookie_attributes(is_https, http_only)));
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
    // Whether this handler belongs to the HTTPS listener. Each listener gets its
    // own set of handlers (see server::set_routes()), so this is fixed per
    // handler rather than being a property of the request; the cookie code needs
    // it to decide whether to mark session cookies Secure.
    const bool _is_https;
public:
    explicit gated_handler(const char* name, request_control& request_control, bool is_https)
        : _name(name), _request_control(request_control), _is_https(is_https)
    {}

    bool is_https() const noexcept {
        return _is_https;
    }
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
    explicit resource_handler(request_control& request_control, bool is_https)
        : gated_handler("resource", request_control, is_https)
    {}
protected:
    virtual future<std::unique_ptr<reply>> do_handle(const sstring& path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) override {
        co_return std::move(rep);
    }
};

class login_handler : public gated_handler {
    constexpr static size_t max_authentication_credentials_length = 128 * 1024; // Maximum length of authentication credentials, just for sanity

    session_manager& _session_manager;

private:
    future<scheduling_group> finalize_login(service::client_state& client_state, auth::authenticated_user user) {
        client_state.set_login(std::move(user));
        co_await client_state.check_user_can_login();
        client_state.maybe_update_per_service_level_params();
        auto sg = client_state.get_service_level_controller().get_cached_user_scheduling_group(client_state.user());
        co_return sg;
    }

public:
    login_handler(request_control& request_control, session_manager& session_manager, bool is_https)
        : gated_handler("login", request_control, is_https), _session_manager(session_manager)
    {}
protected:
    virtual future<std::unique_ptr<reply>> do_handle(const sstring& path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) override {
        const auto cookies = handle_cookies(_session_manager.config(), is_https(), *req, *rep);
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
            // Try TLS client-certificate authentication first, if the client
            // connected over HTTPS and presented a certificate. This lets an
            // administrator authenticate with mutual TLS (e.g. via
            // 'curl --cert ... --key ...') instead of a username/password body.
            // request::tls_dn is only set if the client connected over TLS and
            // presented a certificate, request::tls_san points to the
            // certificate's subject alternative names (an empty vector if it
            // has none). Both are owned by the connection and are valid for as
            // long as the request is being handled.
            std::optional<auth::authenticated_user> cert_user;
            if (req->tls_dn) {
                try {
                    cert_user = co_await auth.authenticate([req = req.get()] () -> future<std::optional<auth::certificate_info>> {
                        co_return auth::certificate_info{ req->tls_dn->subject, [req] () -> future<std::string> {
                            co_return req->tls_san ? fmt::format("{}", fmt::join(*req->tls_san, ",")) : std::string();
                        } };
                    });
                } catch (exceptions::authentication_exception& e) {
                    co_return write_response(std::move(rep), reply::status_type::bad_request, e.what());
                }
            }

            if (cert_user) {
                try {
                    sg = co_await finalize_login(client_state, std::move(*cert_user));
                } catch (exceptions::authentication_exception& e) {
                    // The certificate mapped to a role which cannot log in
                    // (e.g. it doesn't exist). Report it like a rejected
                    // username/password pair, not as an internal error.
                    co_return write_response(std::move(rep), reply::status_type::bad_request, e.what());
                }
            } else {
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
                        sg = co_await finalize_login(client_state, co_await sasl_challenge->get_authenticated_user());
                    } else {
                        co_return write_response(std::move(rep), reply::status_type::internal_server_error, "Configured SASL is a multistage authentication mechanism, currently unsupported by webshell");
                    }
                } catch (exceptions::authentication_exception& e) {
                    co_return write_response(std::move(rep), reply::status_type::bad_request, e.what());
                }
            }
            success_response = format("Successfully logged in as user {}", client_state.user().value().name.value());
        } else {
            success_response = "Successfully logged in as anonymous user";
        }

        const auto user_agent = req->get_header("User-Agent");

        auto& session = _session_manager.create_session(std::move(client_state), sg, std::move(user_agent), is_https());

        set_session_cookies(*rep, _session_manager.config(), is_https(), session.id, session.auth_user());

        co_return write_response(std::move(rep), reply::status_type::ok, std::move(success_response));
    }
};

class logout_handler : public gated_handler {
    session_manager& _session_manager;
public:
    logout_handler(request_control& request_control, session_manager& session_manager, bool is_https)
        : gated_handler("logout", request_control, is_https), _session_manager(session_manager)
    {}
protected:
    virtual future<std::unique_ptr<reply>> do_handle(const sstring& path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) override {
        const auto cookies = handle_cookies(_session_manager.config(), is_https(), *req, *rep);
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
            erase_session_cookie(*rep, is_https(), cookie_name);
        }

        co_return write_response(std::move(rep), reply::status_type::ok, std::move(response));
    }
};

class query_handler : public gated_handler {
    session_manager& _session_manager;

public:
    query_handler(request_control& request_control, session_manager& session_manager, bool is_https)
        : gated_handler("query", request_control, is_https)
        , _session_manager(session_manager)
    { }

protected:
    virtual future<std::unique_ptr<reply>> do_handle(const sstring& path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) override {
        (void)_session_manager;
        co_return std::move(rep);
    }
};

class command_handler : public gated_handler {
    session_manager& _session_manager;

public:
    command_handler(request_control& request_control, session_manager& session_manager, bool is_https)
        : gated_handler("command", request_control, is_https)
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
    option_handler(request_control& request_control, session_manager& session_manager, bool is_https)
        : gated_handler("option", request_control, is_https)
        , _session_manager(session_manager)
    { }

protected:
    virtual future<std::unique_ptr<reply>> do_handle(const sstring& path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) override {
        (void)_session_manager;
        co_return std::move(rep);
    }
};

class option_query_handler : public gated_handler {
    session_manager& _session_manager;

public:
    option_query_handler(request_control& request_control, session_manager& session_manager, bool is_https)
        : gated_handler("option-query", request_control, is_https)
        , _session_manager(session_manager)
    { }

protected:
    virtual future<std::unique_ptr<reply>> do_handle(const sstring& path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) override {
        (void)_session_manager;
        co_return std::move(rep);
    }
};

/// Answer a request that named a real endpoint with the wrong method.
///
/// The endpoints are all POST. Without this, a GET to one of them would fall
/// through to the resource handler, be looked up as a file, and come back as a
/// bodyless 404 - the one way an API request could get an answer that is not
/// JSON. Other methods are caught by the resource handler itself, which cannot
/// tell an endpoint from a misspelled path.
class method_not_allowed_handler : public gated_handler {
public:
    explicit method_not_allowed_handler(request_control& request_control, bool is_https)
        : gated_handler("method-not-allowed", request_control, is_https)
    {}

protected:
    virtual future<std::unique_ptr<reply>> do_handle(const sstring& path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) override {
        rep->add_header("Allow", "POST");
        co_return write_response(std::move(rep), reply::status_type::method_not_allowed,
                format("{} is not allowed on {}, use POST", req->_method, path));
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
    r.add_default_handler(new resource_handler(_request_control, is_https));
    r.put(operation_type::POST, "/login", new login_handler(_request_control, _session_manager, is_https));
    r.put(operation_type::POST, "/logout", new logout_handler(_request_control, _session_manager, is_https));
    r.put(operation_type::POST, "/query", new query_handler(_request_control, _session_manager, is_https));
    r.put(operation_type::POST, "/command", new command_handler(_request_control, _session_manager, is_https));
    r.put(operation_type::POST, "/option", new option_handler(_request_control, _session_manager, is_https));
    r.put(operation_type::GET, "/option", new option_query_handler(_request_control, _session_manager, is_https));

    // A GET to an endpoint would otherwise be indistinguishable from a request
    // for a file and answered with a bodyless 404. /option is left out, as GET
    // is how its options are read. Every other method lands in the resource
    // handler, which answers with JSON.
    for (const auto& endpoint : {"/login", "/logout", "/query", "/command"}) {
        r.put(operation_type::GET, endpoint, new method_not_allowed_handler(_request_control, is_https));
    }

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
