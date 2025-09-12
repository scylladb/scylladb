/*
 * Copyright (C) 2025-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <array>
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
#include "cql3/query_result_printer.hh"
#include "db/config.hh"
#include "resources/tools/webshell/webshell.resources.hh"
#include "service/client_state.hh"
#include "tools/webshell/webshell.hh"
#include "utils/base64.hh"
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

// The consistency levels accepted by the CONSISTENCY option and by the
// "consistency" per-request override. Keys are lower-case; callers lower-case
// the value before looking it up.
static const std::unordered_map<std::string_view, db::consistency_level>& consistency_levels() {
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

// The consistency levels accepted by the SERIAL CONSISTENCY option and by the
// "serial_consistency" per-request override.
static const std::unordered_map<std::string_view, db::consistency_level>& serial_consistency_levels() {
    static const std::unordered_map<std::string_view, db::consistency_level> str_to_cl {
        {"serial", db::consistency_level::SERIAL},
        {"local_serial", db::consistency_level::LOCAL_SERIAL},
    };
    return str_to_cl;
}

static sstring to_lower(std::string_view sv) {
    return sv | std::views::transform([] (char c) { return std::tolower(c); }) | std::ranges::to<sstring>();
}

static sstring to_upper(std::string_view sv) {
    return sv | std::views::transform([] (char c) { return std::toupper(c); }) | std::ranges::to<sstring>();
}

// Look up a lower-cased consistency level name in one of the maps above.
// option_name is used in the error message only, so that it can name either the
// option (CONSISTENCY) or the request field (consistency) the value came from.
static std::expected<db::consistency_level, sstring> parse_consistency_level(std::string_view cl_str, const std::unordered_map<std::string_view, db::consistency_level>& str_to_cl, const char* option_name) {
    auto it = str_to_cl.find(cl_str);
    if (it == str_to_cl.end()) {
        auto all_cls = std::views::values(str_to_cl) | std::ranges::to<std::vector<db::consistency_level>>();
        std::ranges::sort(all_cls);
        const auto all_cls_upper = all_cls
                | std::views::transform([] (auto cl) { return fmt::to_string(cl); })
                | std::views::transform([] (const auto& s) { return to_upper(s); })
                | std::ranges::to<std::vector<sstring>>();

        const auto last = std::prev(all_cls_upper.end());
        return std::unexpected(seastar::format("Invalid {} argument, expected {} or {}.",
                    option_name, fmt::join(std::ranges::subrange(all_cls_upper.begin(), last), ", "), *last));
    }

    return it->second;
}

static std::expected<enum output_format, sstring> parse_output_format(std::string_view format_str, const char* option_name) {
    if (format_str == "text") {
        return output_format::text;
    } else if (format_str == "json") {
        return output_format::json;
    }
    return std::unexpected(seastar::format("Invalid {} argument, expected TEXT or JSON.", option_name));
}

static std::expected<void, sstring> apply_option_overrides(session_options& options, const rjson::value& request) {
    const auto it = request.FindMember("options");
    if (it == request.MemberEnd() || it->value.IsNull()) {
        return {};
    }

    for (const auto& member : it->value.GetObject()) {
        const auto name = rjson::to_string_view(member.name);

        if (member.value.IsNull()) {
            continue;
        }

        if (name == "consistency") {
            auto cl = parse_consistency_level(to_lower(rjson::to_string_view(member.value)), consistency_levels(), "consistency");
            if (!cl) {
                return std::unexpected(std::move(cl.error()));
            }
            options.consistency = *cl;
        } else if (name == "expand") {
            options.expand = member.value.GetBool();
        } else if (name == "page_size") {
            options.page_size = member.value.GetInt();
        } else if (name == "serial_consistency") {
            auto cl = parse_consistency_level(to_lower(rjson::to_string_view(member.value)), serial_consistency_levels(), "serial_consistency");
            if (!cl) {
                return std::unexpected(std::move(cl.error()));
            }
            options.serial_consistency = *cl;
        } else if (name == "tracing") {
            options.tracing = member.value.GetBool();
        } else if (name == "output_format") {
            auto out_format = parse_output_format(to_lower(rjson::to_string_view(member.value)), "output_format");
            if (!out_format) {
                return std::unexpected(std::move(out_format.error()));
            }
            options.output_format = *out_format;
        } else {
            return std::unexpected(seastar::format("Unrecognized option: {}", name));
        }
    }

    return {};
}

static std::expected<void, sstring> add_option_to_json(std::string_view option, const session_options& options, rjson::value& out) {
    if (option == "consistency") {
        rjson::add(out, "consistency", rjson::from_string(fmt::to_string(options.consistency)));
    } else if (option == "expand") {
        rjson::add(out, "expand", options.expand);
    } else if (option == "output format") {
        rjson::add(out, "output_format", rjson::from_string(to_upper(to_string(options.output_format))));
    } else if (option == "paging") {
        rjson::add(out, "page_size", options.page_size);
    } else if (option == "serial consistency") {
        rjson::add(out, "serial_consistency", rjson::from_string(fmt::to_string(options.serial_consistency)));
    } else if (option == "tracing") {
        rjson::add(out, "tracing", options.tracing);
    } else {
        return std::unexpected(seastar::format("Unrecognized option: {}", option));
    }

    return {};
}

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

    template <std::invocable<session_manager&, session&> F>
    auto invoke_on(session_id session_id, F f) {
        return invoke_on_unchecked(session_id, [f = std::move(f)] (session_manager& local_this, session* session_opt) mutable {
            if (!session_opt) {
                throw unauthorized_access("Session not found");
            }
            return f(local_this, *session_opt);
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

// Like write_response(), but response is already-serialized JSON, which is
// embedded verbatim instead of being quoted as a string. Used by /option, which
// reports option values as a JSON object.
std::unique_ptr<reply> write_json_response(std::unique_ptr<reply> rep, reply::status_type status, sstring response) {
    rep->set_status(status);
    rep->write_body("json", [response = std::move(response)] (output_stream<char>&& out_) -> future<> {
        auto out = std::move(out_);

        co_await out.write("{\"response\": ");

        co_await out.write(response);

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
    const config& _config;
    std::vector<resources::resource> _resource_manifest_storage;
    std::span<const resources::resource> _resource_manifest;
public:
    explicit resource_handler(request_control& request_control, const config& cfg, bool is_https)
        : gated_handler("resource", request_control, is_https)
        , _config(cfg)
    {}

    future<> load_manifest() {
        if (_config.webshell_resource_manifest_path.empty()) {
            _resource_manifest = resources::webshell_resources_manifest;
        } else {
            try {
                _resource_manifest_storage = co_await resources::load_resource_manifest(_config.webshell_resource_manifest_path);
                _resource_manifest = std::span(_resource_manifest_storage.data(), _resource_manifest_storage.size());
            } catch (...) {
                throw std::runtime_error(format("Failed to load resource manifest from {}: {}", _config.webshell_resource_manifest_path, std::current_exception()));
            }
        }
    }
protected:
    virtual future<std::unique_ptr<reply>> do_handle(const sstring& path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) override {
        const auto method = httpd::str2type(req->_method);

        // Being the default handler, this also catches every request that missed
        // all of the endpoints - a POST to a misspelled path, for one. Those are
        // API requests rather than requests for a file, so they are answered
        // with the same JSON error body as any endpoint would produce. HEAD is
        // left out of that, since a HEAD response must not carry a body.
        if (method != operation_type::GET) {
            if (method == operation_type::HEAD) {
                rep->set_status(reply::status_type::not_found);
                co_return std::move(rep);
            }
            co_return write_response(std::move(rep), reply::status_type::not_found,
                    format("No such endpoint: {} {}", req->_method, path));
        }

        co_await load_manifest();

        const sstring file_path = path == "/" ? "webshell.html" : path.substr(1); // Remove leading slash

        auto resource_it = std::ranges::find_if(_resource_manifest, [&file_path] (const resources::resource& r) { return r.name == file_path; });
        if (resource_it == std::end(_resource_manifest)) {
            rep->set_status(reply::status_type::not_found);
            co_return std::move(rep);
        }

        auto& resource = *resource_it;

        if (auto file_path = std::get_if<std::filesystem::path>(&resource.content); file_path) {
            const auto path = _config.webshell_resource_manifest_path.parent_path() / std::get<std::filesystem::path>(resource.content);
            if (!co_await file_accessible(path.native(), access_flags::exists | access_flags::read)) {
                rep->set_status(reply::status_type::not_found);
                rep->write_body("text", "Resource file exists in manifest but either doesn't exists or not readable on disk");
                co_return std::move(rep);
            }
        }

        if (resource.compressed) {
            rep->add_header("Content-Encoding", "gzip");
        }

        rep->set_status(reply::status_type::ok);
        rep->write_body("text", [this, &resource] (output_stream<char>&& out_) -> future<> {
            auto out = std::move(out_);

            std::exception_ptr ex;
            try {
                if (auto content_view = std::get_if<bytes_view>(&resource.content); content_view) {
                    co_await out.write(reinterpret_cast<const char*>(content_view->data()), content_view->size());
                } else {
                    const auto path = _config.webshell_resource_manifest_path.parent_path() / std::get<std::filesystem::path>(resource.content);
                    auto f = co_await open_file_dma(path.native(), open_flags::ro);
                    auto in = make_file_input_stream(f);
                    co_await copy(in, out);
                    co_await in.close();
                }
                co_await out.flush();
            } catch (...) {
                ex = std::current_exception();
            }

            co_await out.close();

            if (ex) {
                co_await coroutine::return_exception_ptr(std::move(ex));
            }
        });
        rep->set_content_type(resource.content_type);

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
    virtual void visit(const cql_transport::messages::result_message::bounce&) override {
        throw_on_unexpected_message("bounce");
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

        if (!res.value()->as_bounce()) {
            co_return query_exec_success(query_result_visitor(std::move(res.value())).get(), options, trace_state);
        }

        // Handle bounce to another shard
        const auto shard = res.value()->as_bounce()->target_shard();
        //FIXME: check target host too
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

            if (auto bounce = res.value()->as_bounce(); bounce) {
                throw std::runtime_error(format("Unexpected bounce to another shard, after handling a bounce to {}:{}", bounce->target_host(), bounce->target_shard()));
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
    query_handler(request_control& request_control, session_manager& session_manager, bool is_https)
        : gated_handler("query", request_control, is_https)
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
        const auto cookies = handle_cookies(_session_manager.config(), is_https(), *req, *rep);
        const auto session_id = get_session_id(cookies);

        auto request = rjson::parse_and_validate(
                co_await util::read_entire_stream_contiguous(*req->content_stream),
                rjs::object({
                    {"query", rjs::scalar::string()},
                    {"paging_state", rjs::optional(rjs::scalar::string())},
                    {"options", rjs::optional(rjs::object({
                        {"consistency", rjs::optional(rjs::scalar::string())},
                        {"expand", rjs::optional(rjs::scalar::boolean())},
                        {"page_size", rjs::optional(rjs::scalar::integer())},
                        {"serial_consistency", rjs::optional(rjs::scalar::string())},
                        {"tracing", rjs::optional(rjs::scalar::boolean())},
                        {"output_format", rjs::optional(rjs::scalar::string())},
                    }))}
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
            auto request_options = options;
            if (auto applied = apply_option_overrides(request_options, *request); !applied) {
                co_return std::unexpected(query_exec_failure(reply::status_type::bad_request, std::move(applied.error())));
            }

            if (query != std::exchange(last_query, sstring(query))) {
                trace_state = nullptr;
                serialized_paging_state = std::nullopt;
            }

            auto res = co_await execute_query(_session_manager.qp().container(), request_options, client_state, query, trace_state, serialized_paging_state);

            if (res) {
                trace_state = res->trace_state;
            }

            co_return std::move(res);
        });

        co_return write_response(std::move(rep), std::move(result));
    }
};

/// Handle commands
///
/// Implements a subset of CQLSH options and commands:
/// * HELP [<command>] - show help.
/// * SHOW [SESSION <tracing-session-id>] - shows the tracing events for the provided tracing session id.
///
/// EXIT and LOGIN are handled by /logout and /login endpoints.
class command_handler : public gated_handler {
    session_manager& _session_manager;

    static future<query_exec_result> handle_help(std::vector<sstring> args, session_manager& session_manager, service::client_state& client_state) {
        const char* help = R"(ScyllaDB WebShell

!!! WebShell is still experimental, things are subject to change and there may be bugs !!!

For more information, see https://docs.scylladb.com/manual/master/operating-scylla/admin-tools/webshell.html.

Available commands:
 * HELP - show this message.
 * SHOW [SESSION <tracing-session-id>] - show tracing session events for the provided tracing session id.

Available options:
 * CONSISTENCY [<level>] - set default consistency level for queries, with no args show current setting (default: ONE).
 * EXPAND [ON|OFF] - enable/disable expanded (vertical) output, with no args show current setting (default: OFF).
 * OUTPUT FORMAT [TEXT|JSON] - set output format, with no args show current setting (default: TEXT).
 * PAGING [ON|OFF|<number>] - enable/disable/limit result paging, with no args show current setting (default: 100).
 * SERIAL CONSISTENCY [<level>] - set default serial consistency level for queries, with no args show current setting (default: SERIAL).
 * TRACING [ON|OFF] - enable/disable query tracing, with no args show current setting (default: OFF).
)";
        return make_ready_future<query_exec_result>(query_exec_success(help, {}, nullptr));
    }

    static future<query_exec_result> handle_show_session(std::vector<sstring> args, session_manager& session_manager, service::client_state& client_state) {
        if (args.size() != 1) {
            co_return std::unexpected(query_exec_failure(reply::status_type::bad_request, "Invalid SHOW command, expected 'SHOW SESSION <tracing_session_id>'."));
        }

        const session_options options{.consistency = db::consistency_level::ONE, .page_size = 0};

        co_return co_await query_handler::execute_query(session_manager.qp().container(), options, client_state,
                format("SELECT * FROM system_traces.events WHERE session_id = {}", args[0]), {}, {});
    }

    static future<query_exec_result> handle_command(sstring command, std::vector<sstring> args, session_manager& session_manager, service::client_state& client_state) {
        using handler = std::function<future<query_exec_result>(std::vector<sstring>, class session_manager&, service::client_state&)>;
        static std::unordered_map<sstring, handler> handlers {
            {"help", command_handler::handle_help},
            {"show session", command_handler::handle_show_session},
        };

        auto it = handlers.find(command);
        if (it == handlers.end()) {
            co_return std::unexpected(query_exec_failure(reply::status_type::bad_request, format("Unrecognized command: {}", command)));
        }

        co_return co_await it->second(args, session_manager, client_state);
    }

public:
    command_handler(request_control& request_control, session_manager& session_manager, bool is_https)
        : gated_handler("command", request_control, is_https)
        , _session_manager(session_manager)
    { }

protected:
    virtual future<std::unique_ptr<reply>> do_handle(const sstring& path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) override {
        const auto cookies = handle_cookies(_session_manager.config(), is_https(), *req, *rep);
        const session_id session_id = get_session_id(cookies);

        auto command_and_args = rjson::parse_and_validate(
                co_await util::read_entire_stream_contiguous(*req->content_stream) | std::views::transform([] (char c) { return std::tolower(c); }) | std::ranges::to<sstring>(),
                rjs::object({
                    {"command", rjs::scalar::string()},
                    {"arguments", rjs::array(rjs::scalar::string())}
                }));
        if (!command_and_args) {
            co_return write_response(std::move(rep), reply::status_type::bad_request, command_and_args.error());
        }

        const auto command = sstring(rjson::to_string_view((*command_and_args)["command"]));
        std::vector<sstring> arguments;
        for (const auto& arg : (*command_and_args)["arguments"].GetArray()) {
            arguments.emplace_back(rjson::to_string_view(arg));
        }

        auto result = co_await _session_manager.invoke_on_local_shard(session_id, [&, this] (const session_options& options, service::client_state& client_state,
                tracing::trace_state_ptr&, sstring&) mutable {
            return handle_command(command, arguments, _session_manager, client_state);
        });

        co_return write_response(std::move(rep), std::move(result));
    }
};

/// Handle session options
///
/// Implements a subset of CQLSH options:
/// * CONSISTENCY [<level>] - set default consistency level for queries, with no args show current setting>
/// * EXPAND [ON|OFF] - enable/disable expanded (vertical) output, with no args show current setting.
/// * PAGING [ON|OFF|<number>] - enable/disable/limit result paging, with no args show current setting.
/// * SERIAL CONSISTENCY [<level>] - set default serial consistency level for queries, with no args show current setting.
/// * TRACING [ON|OFF] - enable/disable query tracing, with no args show current setting.
///
/// Extra options (not in CQLSH):
/// * OUTPUT FORMAT [<format>] - set output format, supported formats are TEXT and JSON, with no args show current setting.
///
/// This endpoint only changes options; reading them is GET /option, see
/// option_query_handler. Either way the response is a JSON object of option
/// values, never prose - wording those values for a human is left to the client.
class option_handler : public gated_handler {
    session_manager& _session_manager;

    // The apply_*() functions below validate the arguments of one option and
    // apply them to options, returning the message for the first argument they
    // reject. They are never called with an empty args - an option with no
    // arguments is a query of its current value, not a change.

    static std::expected<void, sstring> apply_consistency(const std::vector<sstring>& args, session_options& options) {
        if (args.size() > 1) {
            return std::unexpected("Invalid CONSISTENCY option, expected 'CONSISTENCY [<consistency_level>]'.");
        }

        auto cl = parse_consistency_level(args[0], consistency_levels(), "CONSISTENCY");
        if (!cl) {
            return std::unexpected(std::move(cl.error()));
        }
        options.consistency = *cl;
        return {};
    }

    static std::expected<void, sstring> apply_expand(const std::vector<sstring>& args, session_options& options) {
        if (args.size() > 1) {
            return std::unexpected("Invalid EXPAND option, expected 'EXPAND [ON|OFF]'.");
        }

        if (args[0] == "on") {
            options.expand = true;
        } else if (args[0] == "off") {
            options.expand = false;
        } else {
            return std::unexpected("Invalid EXPAND argument, expected ON or OFF.");
        }

        return {};
    }

    static std::expected<void, sstring> apply_output_format(const std::vector<sstring>& args, session_options& options) {
        if (args.size() > 1) {
            return std::unexpected("Invalid OUTPUT FORMAT option, expected 'OUTPUT FORMAT [TEXT|JSON]'.");
        }

        auto out_format = parse_output_format(args[0], "OUTPUT FORMAT");
        if (!out_format) {
            return std::unexpected(std::move(out_format.error()));
        }
        options.output_format = *out_format;
        return {};
    }

    static std::expected<void, sstring> apply_paging(const std::vector<sstring>& args, session_options& options) {
        if (args.size() > 1) {
            return std::unexpected("Invalid PAGING option, expected 'PAGING [ON|OFF|<number>]'.");
        }

        if (args[0] == "off") {
            options.page_size = 0;
        } else if (args[0] == "on") {
            options.page_size = 100; // default page size
        } else {
            // std::stoi reports what it cannot convert by throwing; the two
            // failures are told apart to name which of them happened.
            try {
                options.page_size = std::stoi(args[0]);
            } catch (std::invalid_argument&) {
                return std::unexpected("Page size must be a number.");
            } catch (std::out_of_range&) {
                return std::unexpected("Page size must be a 32 bit integer.");
            }
        }

        return {};
    }

    static std::expected<void, sstring> apply_serial_consistency(const std::vector<sstring>& args, session_options& options) {
        if (args.size() > 1){
            return std::unexpected("Invalid SERIAL CONSISTENCY option, expected 'SERIAL CONSISTENCY [<serial_consistency_level>]'.");
        }

        auto cl = parse_consistency_level(args[0], serial_consistency_levels(), "SERIAL CONSISTENCY");
        if (!cl) {
            return std::unexpected(std::move(cl.error()));
        }
        options.serial_consistency = *cl;
        return {};
    }

    static std::expected<void, sstring> apply_tracing(const std::vector<sstring>& args, session_options& options) {
        if (args.size() > 1) {
            return std::unexpected("Invalid TRACING option, expected 'TRACING [ON|OFF]'.");
        }

        if (args[0] == "off") {
            options.tracing = false;
        } else if (args[0] == "on") {
            options.tracing = true;
        } else {
            return std::unexpected("Invalid TRACING option, expected 'TRACING [ON|OFF]'.");
        }

        return {};
    }

    using applier = std::expected<void, sstring> (*)(const std::vector<sstring>&, session_options&);

    // All the options, in the order the report of all of them lists them. Also
    // the list of recognized option names - add_option_to_json() knows the same
    // names and has to be kept in step with this.
    static const std::array<std::pair<std::string_view, applier>, 6>& option_appliers() {
        static const std::array<std::pair<std::string_view, applier>, 6> appliers {{
            {"consistency", apply_consistency},
            {"expand", apply_expand},
            {"output format", apply_output_format},
            {"paging", apply_paging},
            {"serial consistency", apply_serial_consistency},
            {"tracing", apply_tracing},
        }};
        return appliers;
    }

public:
    /// Change one option and report its new value, as a serialized JSON object.
    ///
    /// The new value is reported because the request does not always determine
    /// it: PAGING ON, for one, resolves to a server-side default page size that
    /// the client would otherwise have to guess.
    ///
    /// Returns the message for an unrecognized option or invalid arguments.
    static std::expected<sstring, sstring> apply_option(std::string_view option, const std::vector<sstring>& args, session_options& options) {
        const auto& appliers = option_appliers();
        const auto it = std::ranges::find_if(appliers, [option] (const auto& applier) { return applier.first == option; });
        if (it == appliers.end()) {
            return std::unexpected(seastar::format("Unrecognized option: {}", option));
        }

        if (auto applied = it->second(args, options); !applied) {
            return std::unexpected(std::move(applied.error()));
        }

        auto response = rjson::empty_object();
        if (auto added = add_option_to_json(option, options, response); !added) {
            return std::unexpected(std::move(added.error()));
        }
        return sstring(fmt::to_string(response));
    }

    /// Report the value of one option, or of all of them if option is empty, as
    /// a serialized JSON object.
    ///
    /// Returns the message for an unrecognized option.
    static std::expected<sstring, sstring> report_options(std::string_view option, const session_options& options) {
        auto response = rjson::empty_object();

        if (option.empty()) {
            for (const auto& applier : option_appliers()) {
                // Every name in option_appliers() is one add_option_to_json()
                // recognizes, so this cannot fail.
                add_option_to_json(applier.first, options, response).value();
            }
        } else {
            if (auto added = add_option_to_json(option, options, response); !added) {
                return std::unexpected(std::move(added.error()));
            }
        }

        return sstring(fmt::to_string(response));
    }

    option_handler(request_control& request_control, session_manager& session_manager, bool is_https)
        : gated_handler("option", request_control, is_https)
        , _session_manager(session_manager)
    { }

protected:
    virtual future<std::unique_ptr<reply>> do_handle(const sstring& path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) override {
        const auto cookies = handle_cookies(_session_manager.config(), is_https(), *req, *rep);
        const session_id session_id = get_session_id(cookies);

        auto option_and_args = rjson::parse_and_validate(
                co_await util::read_entire_stream_contiguous(*req->content_stream) | std::views::transform([] (char c) { return std::tolower(c); }) | std::ranges::to<sstring>(),
                rjs::object({
                    {"option", rjs::scalar::string()},
                    {"arguments", rjs::array(rjs::scalar::string())}
                }));
        if (!option_and_args) {
            co_return write_response(std::move(rep), reply::status_type::bad_request, option_and_args.error());
        }

        const auto option = sstring(rjson::to_string_view((*option_and_args)["option"]));
        std::vector<sstring> arguments;
        for (const auto& arg : (*option_and_args)["arguments"].GetArray()) {
            arguments.emplace_back(rjson::to_string_view(arg));
        }

        // Changing an option is all this endpoint does; reading one is a GET.
        if (arguments.empty()) {
            co_return write_response(std::move(rep), reply::status_type::bad_request,
                    format("No arguments given for option {}. Use GET /option to query the current value.", option));
        }

        auto response = co_await _session_manager.invoke_on(session_id, [&option, &arguments] (session_manager& session_manager, session& session) {
            auto reported = apply_option(option, arguments, session.options);
            session.refresh(session_manager.config().session_ttl);
            return make_ready_future<std::expected<sstring, sstring>>(std::move(reported));
        });
        if (!response) {
            co_return write_response(std::move(rep), reply::status_type::bad_request, std::move(response.error()));
        }

        co_return write_json_response(std::move(rep), reply::status_type::ok, std::move(*response));
    }
};

/// Report session options
///
/// The read half of the session options, split off from POST /option so that
/// each method does one thing: this one never changes anything.
///
/// * GET /option - report every option.
/// * GET /option?option=<name> - report just that one. The name is the same one
///   POST /option takes, so it can contain a space ("serial consistency"), which
///   has to be encoded as "%20" or "+".
class option_query_handler : public gated_handler {
    session_manager& _session_manager;

public:
    option_query_handler(request_control& request_control, session_manager& session_manager, bool is_https)
        : gated_handler("option-query", request_control, is_https)
        , _session_manager(session_manager)
    { }

protected:
    virtual future<std::unique_ptr<reply>> do_handle(const sstring& path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) override {
        const auto cookies = handle_cookies(_session_manager.config(), is_https(), *req, *rep);
        const session_id session_id = get_session_id(cookies);

        // Option names are lower case, as they are in the body of POST /option.
        const auto option = to_lower(req->get_query_param("option"));

        auto response = co_await _session_manager.invoke_on(session_id, [&option] (session_manager& session_manager, session& session) {
            auto reported = option_handler::report_options(option, session.options);
            session.refresh(session_manager.config().session_ttl);
            return make_ready_future<std::expected<sstring, sstring>>(std::move(reported));
        });
        if (!response) {
            co_return write_response(std::move(rep), reply::status_type::bad_request, std::move(response.error()));
        }

        co_return write_json_response(std::move(rep), reply::status_type::ok, std::move(*response));
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
    r.add_default_handler(new resource_handler(_request_control, _config, is_https));
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
