/*
 * Copyright (C) 2018 ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <deque>
#include <unordered_map>
#include <regex>
#include <algorithm>

#include <seastar/net/dns.hh>
#include <seastar/net/api.hh>
#include <seastar/net/tls.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/reactor.hh>

#include <fmt/core.h>
#include <fmt/ostream.h>

#include <kmip.h>
#include <kmip_bio.h>
#include <kmip_io.h>
#include <openssl/bio.h>

#include "kmip_host.hh"
#include "encryption.hh"
#include "encryption_exceptions.hh"
#include "symmetric_key.hh"
#include "utils/hash.hh"
#include "utils/loading_cache.hh"
#include "utils/UUID.hh"
#include "utils/UUID_gen.hh"
#include "utils/http.hh"
#include "marshal_exception.hh"
#include "db/config.hh"

using namespace std::chrono_literals;

static logger kmip_log("kmip");
static constexpr uint16_t kmip_port = 5696u;
// default for command execution/failover retry.
static constexpr int default_num_cmd_retry = 5;
static constexpr int min_num_cmd_retry = 2;
static constexpr auto base_backoff_time = 100ms;

static std::string KMIP_reason2string(result_reason);
static std::string KMIP_error2string(int);
static std::string KMIP_cryptographic_algorithm2string(cryptographic_algorithm);
static std::string KMIP_block_cipher_mode2string(block_cipher_mode);
static std::string KMIP_padding_method2string(padding_method);

namespace encryption {

bool operator==(const kmip_host::key_options& l, const kmip_host::key_options& r) {
    return std::tie(l.template_name, l.key_namespace) == std::tie(r.template_name, r.key_namespace);
}

class kmip_error_category : public std::error_category {
public:
    constexpr kmip_error_category() noexcept : std::error_category{} {}
    const char * name() const noexcept {
        return "KMIP";
    }
    std::string message(int error) const {
        return KMIP_error2string(error);
    }
};

class kmip_reason_category : public std::error_category {
public:
    constexpr kmip_reason_category() noexcept : std::error_category{} {}
    const char * name() const noexcept {
        return "KMIP";
    }
    std::string message(int error) const {
        return KMIP_reason2string(result_reason(error));
    }
};

static const kmip_reason_category kmip_reasonc;

class kmip_error : public std::system_error {
public:
    kmip_error(int res, const std::error_category& ec)
        : system_error(res, ec)
    {}
    kmip_error(int res, const std::error_category& ec, const std::string& msg)
        : system_error(res, ec, msg)
    {}
};

// < 0 -> error.
static void kmip_chk_reason(int res, KMIP* cmd = nullptr) {
    if (res != KMIP_OK) {
        auto msg = ::kmip_last_message();
        if (msg && *msg) {
            throw kmip_error(::kmip_last_reason(), kmip_reasonc, msg);
        }
        throw kmip_error(::kmip_last_reason(), kmip_reasonc);
    }
}

class kmip_host::impl {
public:
    struct kmip_key_info {
        key_info info;
        key_options options;
        bool operator==(const kmip_key_info& i) const {
            return info == i.info && options == i.options;
        }
        friend std::ostream& operator<<(std::ostream& os, const kmip_key_info& info) {
            return os << info.info << ":" << info.options;
        }
    };
    struct kmip_key_info_hash {
        size_t operator()(const kmip_key_info& i) const {
            return utils::tuple_hash()(
                            std::tie(i.info.alg, i.info.len,
                                            i.options.template_name,
                                            i.options.key_namespace));
        }
    };

    using key_and_id_type = std::tuple<shared_ptr<symmetric_key>, id_type>;

    inline static constexpr std::chrono::milliseconds default_expiry = 30s;
    inline static constexpr std::chrono::milliseconds default_refresh = 100s;
    inline static constexpr uintptr_t max_hosts = 1<<8;

    inline static constexpr size_t def_max_pooled_connections_per_host = 8;

    impl(encryption_context& ctxt, const std::string& name, const host_options& options)
                    : _ctxt(ctxt), _name(name), _options(options), _attr_cache(
                                    utils::loading_cache_config{
                                        .max_size = std::numeric_limits<size_t>::max(),
                                        .expiry = options.key_cache_expiry.value_or(
                                                    default_expiry),
                                        .refresh = options.key_cache_refresh.value_or(default_refresh)},
                                    kmip_log,
                                    std::bind(&impl::create_key, this,
                                                    std::placeholders::_1)),
                      _id_cache(
                                utils::loading_cache_config{
                                    .max_size = std::numeric_limits<size_t>::max(),
                                    .expiry = options.key_cache_expiry.value_or(
                                                    default_expiry),
                                    .refresh = options.key_cache_refresh.value_or(default_refresh),
                                    },
                                kmip_log,
                                    std::bind(&impl::find_key, this,
                                                    std::placeholders::_1)),
                                    _max_retry(std::max(size_t(min_num_cmd_retry), options.max_command_retries.value_or(default_num_cmd_retry)))
    {
        if (_options.hosts.size() > max_hosts) {
            throw std::invalid_argument("Too many hosts");
        }
    }

    future<> connect();
    future<> disconnect();
    future<std::tuple<shared_ptr<symmetric_key>, id_type>> get_or_create_key(const key_info&, const key_options& = {});
    future<shared_ptr<symmetric_key>> get_key_by_id(const id_type&, const std::optional<key_info>& = {});

    id_type kmip_id_to_id(const std::string&) const;
    std::string id_to_kmip_string(const id_type&) const;
private:
    future<key_and_id_type> create_key(const kmip_key_info&);
    future<shared_ptr<symmetric_key>> find_key(const id_type&);
    future<std::optional<id_type>> find_matching_key(const kmip_key_info&, std::optional<int> max = {});

    static shared_ptr<symmetric_key> ensure_compatible_key(shared_ptr<symmetric_key>, const key_info&);

    struct response_buffer;
    class kmip_cmd;
    class kmip_data_list;
    class connection;

    friend std::ostream& operator<<(std::ostream& os, const impl& me) {
        fmt::print(os, "{}", me._name);
        return os;
    }

    using con_ptr = ::shared_ptr<connection>;
    using opt_int = std::optional<int>;

    future<con_ptr> get_connection();
    future<con_ptr> get_connection(const std::string&);
    future<> clear_connections(const std::string& host);

    future<> release(con_ptr, bool retain_connection = false);

    template <typename Func, typename... Args>
    futurize_t<std::invoke_result_t<Func, kmip_cmd&, Args...>>
    do_kmip_cmd(Func&& func, kmip_cmd&, Args&&... args) noexcept;

    template <typename Func, typename... Args>
    futurize_t<std::invoke_result_t<Func, kmip_cmd&, con_ptr, Args...>>
    do_kmip_cmd_with_connection(Func&& func, kmip_cmd&, Args&&... args);

    size_t max_pooled_connections_per_host() const {
        return _options.max_pooled_connections_per_host.value_or(def_max_pooled_connections_per_host);
    }
    bool is_current_host(const std::string& host) {
        return host == _options.hosts.at(_index % _options.hosts.size());
    }

    encryption_context& _ctxt;
    std::string _name;
    host_options _options;
    utils::loading_cache<kmip_key_info, key_and_id_type, 2,
                    utils::loading_cache_reload_enabled::yes,
                    utils::simple_entry_size<key_and_id_type>,
                    kmip_key_info_hash> _attr_cache;

    utils::loading_cache<id_type, ::shared_ptr<symmetric_key>, 2,
                    utils::loading_cache_reload_enabled::yes,
                    utils::simple_entry_size<::shared_ptr<symmetric_key>>> _id_cache;

    using connections = std::deque<con_ptr>;
    using host_to_connections = std::unordered_map<std::string, connections>;

    host_to_connections _host_connections;
    // current default host. If a host fails, incremented and
    // we try another in the host ip list.
    size_t _index = 0;
    size_t _max_retry = default_num_cmd_retry;
};

}

template <> struct fmt::formatter<encryption::kmip_host::impl> : fmt::ostream_formatter {};
template <> struct fmt::formatter<encryption::kmip_host::impl::kmip_key_info> : fmt::ostream_formatter {};
template <> struct fmt::formatter<encryption::kmip_host::impl::connection> : fmt::ostream_formatter {};

namespace encryption {

class kmip_host::impl::connection {
public:
    connection(const std::string& host, host_options& options)
        : _host(host)
        , _options(options)
        , _bio(make_bio())
    {}
    ~connection()
    {}

    const std::string& host() const {
        return _host;
    }

    future<> connect();
    future<> wait_for_io();
    future<> close();

    operator ::BIO*() const {
        return _bio.get();
    }
private:
    friend std::ostream& operator<<(std::ostream& os, const connection& me) {
        return os << me._host;
    }

    static BIO_METHOD* bio_method();

    using bio_ptr = std::unique_ptr<BIO, int(*)(BIO*)>;
    bio_ptr make_bio();

    std::string _host;
    host_options& _options;
    bio_ptr _bio;
    output_stream<char> _output;
    input_stream<char> _input;
    seastar::connected_socket _socket;
    std::optional<temporary_buffer<char>> _in_buffer;
    std::optional<future<>> _pending;
};


BIO_METHOD* kmip_host::impl::connection::bio_method() {
    using method_ptr = std::unique_ptr<BIO_METHOD, void(*)(BIO_METHOD*)>;
    static method_ptr method = [] {
        auto method = method_ptr(::BIO_meth_new(::BIO_get_new_index()|BIO_TYPE_SOURCE_SINK, "BIO_s_scylla"), &BIO_meth_free);

        // assume all IO done in seastar thread.
        static auto get_connection = [](BIO* b) -> auto& {
            return *reinterpret_cast<connection*>(BIO_get_data(b));
        };
        static auto custom_read = [](BIO *b, char *data, int dlen) {
            auto& c = get_connection(b);
            try {
                auto buf = c._input.read_exactly(dlen).get();
                data = std::copy(buf.begin(), buf.end(), data);
                return int(buf.size());
            } catch (...) {
                return -1;
            }
        };
        static auto custom_write = [](BIO *b, const char *data, int dlen) {
            auto& c = get_connection(b);
            try {
                c._output.write(data, dlen).get();
                c._output.flush().get();
                return dlen;
            } catch (...) {
                return -1;
            }
        };
        static auto custom_ctrl = [](BIO *b, int cmd, long larg, void *pargs) -> long {
            auto& c = get_connection(b);
            switch(cmd) {
            case BIO_CTRL_FLUSH: // 11
                c._output.flush().get();
                return 1;
            default:
                return 0;
            }
        };

        BIO_meth_set_write(method.get(), custom_write);
        BIO_meth_set_read(method.get(), custom_read);
        BIO_meth_set_ctrl(method.get(), custom_ctrl);

        return method;
    }();
    return method.get();
}

kmip_host::impl::connection::bio_ptr kmip_host::impl::connection::make_bio() {
    bio_ptr p(::BIO_new(bio_method()), &BIO_free);
    BIO_set_data(p.get(), reinterpret_cast<void*>(this));
    return p;
}

struct kmip_host::impl::response_buffer {
    char* buffer = 0;
    int size = 0;

    response_buffer() = default;
    response_buffer(const response_buffer&) = delete;
    response_buffer(response_buffer&&) = delete;
    ~response_buffer() {
        if (buffer) {
            ::kmip_free(nullptr, buffer);
        }
    }
    operator char**() {
        return &buffer;
    }
    operator int*() {
        return &size;
    }
    std::string str() const {
        return std::string(buffer, size);
    }
};

class kmip_host::impl::kmip_cmd {
public:
    static const inline auto kmip_version = KMIP_1_0;

    kmip_cmd() {
        ::kmip_init(&_kmip, nullptr, 0, kmip_version);
    }
    kmip_cmd(const kmip_cmd&) = delete;
    kmip_cmd(kmip_cmd&&) = delete;
    ~kmip_cmd() {
        ::kmip_destroy(&_kmip);
    }
    ::KMIP* kmip() {
        return &_kmip;
    }
    operator ::KMIP*() {
        return &_kmip;
    }
private:
    ::KMIP _kmip = {0,};
};

future<> kmip_host::impl::connection::connect() {
    auto cred = ::make_shared<seastar::tls::certificate_credentials>();
    auto f = make_ready_future();

    kmip_log.debug("connecting {}", _host);

    if (!_options.priority_string.empty()) {
        cred->set_priority_string(_options.priority_string);
    } else {
        cred->set_priority_string(db::config::default_tls_priority);
    }

    if (!_options.certfile.empty()) {
        f = f.then([this, cred] {
            return cred->set_x509_key_file(_options.certfile, _options.keyfile, seastar::tls::x509_crt_format::PEM);
        });
    }
    if (!_options.truststore.empty()) {
        f = f.then([this, cred] {
            return cred->set_x509_trust_file(_options.truststore, seastar::tls::x509_crt_format::PEM);
        });
    } else {
        f = f.then([cred] {
            return cred->set_system_trust();
        });
    }
    return f.then([this, cred] {
        // TODO, find if we should do hostname verification
        // TODO: connect all failovers already?

        // Use the URL parser to handle ipv6 etc proper.
        // Turn host arg into a URL.
        auto info = utils::http::parse_simple_url("kmip://" + _host);
        auto name = info.host;
        auto port = info.port != 80 ? info.port : kmip_port;

        return seastar::net::dns::resolve_name(name).then([this, cred, port, name](seastar::net::inet_address addr) {
            kmip_log.debug("Try connect {}:{}", addr, port);
            // TODO: should we verify non-numeric hosts here? (opts.server_name)
            // Adding this might break existing users with half-baked certs.
            return seastar::tls::connect(cred, seastar::socket_address{addr, uint16_t(port)}).then([this](seastar::connected_socket s) {
                kmip_log.debug("Successfully connected {}", _host);
                // #998 Set keepalive to try avoiding connection going stale in between commands.
                s.set_keepalive_parameters(net::tcp_keepalive_params{60s, 60s, 10});
                s.set_keepalive(true);
                _input = s.input();
                _output = s.output();
            });
        });
    });
}

future<> kmip_host::impl::connection::close() {
    return _output.close().finally([this] {
        return _input.close();
    });
}

template <typename Func, typename... Args>
futurize_t<std::invoke_result_t<Func, kmip_host::impl::kmip_cmd&, Args...>>
kmip_host::impl::do_kmip_cmd(Func&& func, kmip_cmd& cmd, Args&&... args) noexcept {
    TextString u = {
        .value = _options.username.data(),
        .size = _options.username.size(),
    };
    TextString p = {
        .value = _options.password.data(),
        .size = _options.password.size(),
    };
    UsernamePasswordCredential upc = {
        .username = &u,
        .password = &p,
    };
    Credential credential = {
        .credential_type = KMIP_CRED_USERNAME_AND_PASSWORD,
        .credential_value = &upc,
    };
    auto def = defer([&cmd] { ::kmip_remove_credentials(cmd); });
    if (!_options.username.empty()) {
        ::kmip_add_credential(cmd, &credential);
    }
    co_return co_await seastar::async(std::forward<Func>(func), cmd, std::forward<Args>(args)...);
}

template <typename Func, typename... Args>
futurize_t<std::invoke_result_t<Func, kmip_host::impl::kmip_cmd&, kmip_host::impl::con_ptr, Args...>>
kmip_host::impl::do_kmip_cmd_with_connection(Func&& func, kmip_cmd& cmd, Args&&... args) {
    for (auto retry = _max_retry; _max_retry > 0; --_max_retry) {
        con_ptr cp;
        std::exception_ptr ep;
        try {
            auto cp = co_await get_connection();
            co_return co_await do_kmip_cmd(std::forward<Func>(func), cmd, cp, std::forward<Args>(args)...).finally([this, cp] {
                return release(cp);
            });
        } catch (kmip_error& e) {
            if (e.code().value() != KMIP_IO_FAILURE) {
                throw;
            }
        } catch (...) {
            ep = std::current_exception();
        }
        // failed to connect. do more serious backing off.
        // we only retry this once, since get_connection
        // will either give back cached connections, 
        // or explicitly try all avail hosts. 
        // In the first case, we will do the lower retry
        // loop if something is stale/borked, the latter is 
        // more or less dead. 
        if (cp != nullptr) {
            auto host = cp->host();
            co_await clear_connections(host);
        }
        auto sleeptime = base_backoff_time * (_max_retry - retry);
        kmip_log.debug("{}: Connection failed ({}). backoff {}", *this, ep, std::chrono::duration_cast<std::chrono::milliseconds>(sleeptime).count());
        co_await seastar::sleep(sleeptime);
        kmip_log.debug("{}: retrying...", *this);
    }
    throw std::runtime_error("Could not complete request to any host");
}


/**
 * Clears and releases a connection cp. Release connection after.
 * If retain_connection is true, the connection is only cleared of command data and 
 * can be reused by caller, otherwise it is either added to the connection pool
 * or dropped.
*/
future<> kmip_host::impl::release(con_ptr cp, bool retain_connection) {
    auto i = _host_connections.find(cp->host());
    if (!retain_connection && is_current_host(i->first) && max_pooled_connections_per_host() > i->second.size()) {
        i->second.emplace_back(std::move(cp));
    }
    // If we neither retain nor cache the connection, do proper close now. Ensures 
    // TLS goodbye etc are sent and streams are flushed. The latter comes into
    // play if we did background flush that have not yet actually happened...
    // Should not happen since we send and wait for response, but lets be careful
    if (cp && !retain_connection) {
        co_await cp->close();
    }
}

future<kmip_host::impl::con_ptr> kmip_host::impl::get_connection(const std::string& host) {
    // TODO: if a pooled connection is stale, the command run will fail,
    // and the connection will be discarded. Would be good if we could detect this case
    // and re-run command with a new connection. Maybe always verify connection, even if
    // it is old?
    auto& q = _host_connections[host];

    if (!q.empty()) {
        auto cp = q.front();
        q.pop_front();
        co_return cp;
    }

    auto cp = ::make_shared<connection>(host, _options);
    kmip_log.trace("{}: connecting to {}", *this, host);
    co_await cp->connect();

    kmip_log.trace("{}: verifying {}", *this, host);

    kmip_cmd cmd;
    query_function queries[] = {
        KMIP_QUERY_OPERATIONS,
        KMIP_QUERY_OBJECTS,
    };

    ::QueryResponse query_result = {0};
    kmip_chk_reason(co_await do_kmip_cmd(&kmip_bio_query_with_context, cmd, *cp, queries, 2, &query_result));
    kmip_log.trace("{}: connected {}", *this, host);
    co_return cp;
}

future<kmip_host::impl::con_ptr> kmip_host::impl::get_connection() {
    for (auto& ignore : _options.hosts) {
        (void)ignore;
        auto& host = _options.hosts[_index % _options.hosts.size()];
        try {
            co_return co_await get_connection(host);
        } catch (...) {
        }
        // if we get here, we failed
        ++_index;
        // if we fail one host, clear out any 
        // caches for it just in case. 
        co_await clear_connections(host);
    }
    // tried all.
    throw missing_resource_error("Could not connect to any server");
}

future<> kmip_host::impl::clear_connections(const std::string& host) {
    auto q = std::exchange(_host_connections[host], {});
    return parallel_for_each(q.begin(), q.end(), [](con_ptr c) {
        return c->close().handle_exception([c](auto ep) {
            // ignore exceptions
        });
    });
}

future<> kmip_host::impl::connect() {
    return do_for_each(_options.hosts, [this](const std::string& host) {
        return get_connection(host).then([this](auto cp) {
            return release(cp);
        });
    });
}

future<> kmip_host::impl::disconnect() {
    co_await do_for_each(_options.hosts, [this](const std::string& host) {
        return clear_connections(host);
    });
    co_await _attr_cache.stop();
    co_await _id_cache.stop();
}

kmip_host::id_type kmip_host::impl::kmip_id_to_id(const std::string& s) const {
    try {
        // #2205 - we previously made all ID:s into uuids (because the literal functions
        // are called KMIP_CMD_get_uuid etc). This has issues with Keysecure which apparently
        // does _not_ give back UUID format strings, but "other" things.
        // Could just always store ascii as bytes instead, but that would now
        // break existing installations, so we check for UUID, and if it does not
        // match we encode it.
        utils::UUID uuid(std::string_view{s});
        return uuid.serialize();
    } catch (marshal_exception&) {
        // very simple exncoding scheme: add a "len" byte at the end.
        // iff byte size of id + 1 (len) equals 16 (length of UUID),
        // add a padding byte.
        size_t len = s.size() + 1;
        if (len == 16) {
            ++len;
        }
        bytes res(len, 0);
        std::copy(s.begin(), s.end(), res.begin());
        res.back() = int8_t(len - s.size());
        return res;
    }
}

std::string kmip_host::impl::id_to_kmip_string(const id_type& id) const {
    // see comment above for encoding scheme.
    if (id.size() == 16) {
        // if byte size is UUID it must be a UUID. No "old" id:s are
        // not, and we never encode non-uuid as 16 bytes.
        auto uuid = utils::UUID_gen::get_UUID(id);
        return fmt::format("{}", uuid);
    }
    auto len = id.size() - id.back();
    return std::string(id.begin(), id.begin() + len);
}

static cryptographic_algorithm info2alg(const key_info& info) {
    auto [type, mode, padd] = parse_key_spec(info.alg);

    if (type == "aes") {
        return KMIP_CRYPTOALG_AES;
    }
    if (type == "3des") {
        return KMIP_CRYPTOALG_TRIPLE_DES;
    }
    if (type == "blowfish") {
        return KMIP_CRYPTOALG_BLOWFISH;
    }
    if (type == "rc2") {
        return KMIP_CRYPTOALG_RC2;
    }
    throw std::invalid_argument(fmt::format("Invalid algorithm string: {}", info.alg));
}

future<kmip_host::impl::key_and_id_type> kmip_host::impl::create_key(const kmip_key_info& info) {
    if (this_shard_id() == 0) {
        // #1039 First try looking for existing keys on server
        auto opt_id = co_await find_matching_key(info, 1);
        if (opt_id) {
            // got it
            auto id = *opt_id;
            auto k = co_await get_key_by_id(id, info.info);
            co_return key_and_id_type(std::move(k), id);
        }

        kmip_log.debug("{}: Creating key {}", _name, info);

        kmip_cmd cmd;
        /* Build the request message. */
        auto alg = info2alg(info.info);
        int32 length = info.info.len;
        int32 mask = KMIP_CRYPTOMASK_ENCRYPT | KMIP_CRYPTOMASK_DECRYPT;

        std::array<::Attribute, 3> a = {{
            { .type = KMIP_ATTR_CRYPTOGRAPHIC_ALGORITHM, .index = KMIP_UNSET, .value = &alg },
            { .type = KMIP_ATTR_CRYPTOGRAPHIC_LENGTH, .index = KMIP_UNSET, .value = &length },
            { .type = KMIP_ATTR_CRYPTOGRAPHIC_USAGE_MASK, .index = KMIP_UNSET, .value = &mask },
        }};

        TemplateAttribute ta = {
            .attributes = a.data(),
            .attribute_count = a.size()
        };

        auto uuid = co_await do_kmip_cmd_with_connection([&](kmip_cmd& cmd, con_ptr cp) {
            response_buffer rbuf;
            kmip_chk_reason(kmip_bio_create_symmetric_key_with_context(cmd, *cp, &ta, rbuf, rbuf));

            auto uuid = rbuf.str();
            kmip_log.debug("{}: Created {}:{}", _name, info, uuid);

            kmip_chk_reason(kmip_bio_activate_symmetric_key(*cp, uuid.data(), uuid.size()));

            return uuid;
        }, cmd);

        auto id = kmip_id_to_id(uuid);
        auto k = co_await get_key_by_id(id, info.info);

        co_return key_and_id_type(k, id);
    }

    auto [kinfo, b, id] = co_await smp::submit_to(0, [this, info] {
        return _ctxt.get_kmip_host(_name)->get_or_create_key(info.info, info.options).then([](std::tuple<shared_ptr<symmetric_key>, id_type> k_id) {
            auto&& [k, id] = k_id;
            return make_ready_future<std::tuple<key_info, bytes, id_type>>(std::tuple(k->info(), k->key(), id));
        });
    });
    co_return key_and_id_type(make_shared<symmetric_key>(kinfo, b), id);
}

future<std::optional<kmip_host::id_type>> kmip_host::impl::find_matching_key(const kmip_key_info& info, std::optional<int> max) {
    kmip_log.debug("{}: Finding matching key {}", _name, info);

    auto alg = info2alg(info.info);
    int32 length = info.info.len;
    int32 mask = KMIP_CRYPTOMASK_ENCRYPT | KMIP_CRYPTOMASK_DECRYPT;
    object_type loctype = KMIP_OBJTYPE_SYMMETRIC_KEY;
    state s = KMIP_STATE_ACTIVE;

    std::array<::Attribute, 5> a = {{
        { .type = KMIP_ATTR_OBJECT_TYPE, .index = KMIP_UNSET, .value = &loctype },
        { .type = KMIP_ATTR_CRYPTOGRAPHIC_ALGORITHM, .index = KMIP_UNSET, .value = &alg },
        { .type = KMIP_ATTR_CRYPTOGRAPHIC_LENGTH, .index = KMIP_UNSET, .value = &length },
        { .type = KMIP_ATTR_CRYPTOGRAPHIC_USAGE_MASK, .index = KMIP_UNSET, .value = &mask },
        { .type = KMIP_ATTR_STATE, .index = KMIP_UNSET, .value = &s },
    }};

    kmip_cmd cmd;

    auto uuid = co_await do_kmip_cmd_with_connection([&](kmip_cmd& cmd, con_ptr cp) -> std::optional<std::string> {
        LocateResponse result;
        kmip_chk_reason(::kmip_bio_locate_with_context(cmd, *cp, a.data(), int(a.size()), &result, 1, 0));

        if (result.ids_size) {
            return std::string(result.ids[0]);
        }
        return std::nullopt;
    }, cmd);

    if (uuid) {
        kmip_log.debug("{}: Found matching key {} -> {}", _name, info, *uuid);
        co_return kmip_id_to_id(*uuid);
    }

    co_return std::nullopt;
}

future<shared_ptr<symmetric_key>> kmip_host::impl::find_key(const id_type& id) {
    if (this_shard_id() == 0) {
        kmip_cmd cmd;
        auto uuid = id_to_kmip_string(id);
        kmip_log.debug("{}: Finding {}", _name, uuid);

        auto k = co_await do_kmip_cmd_with_connection([&](kmip_cmd& cmd, con_ptr cp) {
            response_buffer rbuf;
            cryptographic_algorithm ca;
            kmip_chk_reason(::kmip_bio_get_symmetric_key_with_context_ex(cmd, *cp, uuid.data(), int(uuid.size()), rbuf, rbuf, &ca));

            ::Attribute *atts = nullptr;
            int n = 0;
            auto cleanup = defer([&] {
                for (int i = 0; i < n; ++i) {
                    ::kmip_free_attribute(cmd, &atts[i]);
                }
                if (atts) {
                    ::kmip_free(nullptr, atts);
                }
            });

            const char* name = "Cryptographic Parameters";
            kmip_chk_reason(::kmip_bio_get_attributes_with_context(cmd, *cp, uuid.data(), int(uuid.size()), &name, 1, &atts, &n));

            /* get a reference to the key material (the actual key value) */
            bytes key_data(reinterpret_cast<const int8_t*>(rbuf.buffer), rbuf.size);

            std::string alg = KMIP_cryptographic_algorithm2string(ca);
            std::string mode;
            std::string padd;

            for (auto& a : std::views::counted(atts, n)) {
                if (a.type == KMIP_ATTR_CRYPTOGRAPHIC_PARAMETERS) {
                    auto* cp = reinterpret_cast<const ::CryptographicParameters*>(a.value);
                    mode = KMIP_block_cipher_mode2string(cp->block_cipher_mode);
                    padd = KMIP_padding_method2string(cp->padding_method);
                    break;
                }
            }

            if (alg.empty()) {
                throw configuration_error("Could not find algorithm");
            }
            if (mode.empty() != padd.empty()) {
                throw configuration_error("Invalid block mode/padding");
            }

            auto str = mode.empty() || padd.empty() ? alg : alg + "/" + mode + "/" + padd;
            key_info derived_info{ str, key_data.size()*8};

            kmip_log.trace("{}: Found {}:{} {}", _name, uuid, derived_info.alg, derived_info.len);

            return make_shared<symmetric_key>(derived_info, std::move(key_data));
        }, cmd);

        co_return k;
    }

    auto [info, b] = co_await smp::submit_to(0, [this, id] {
        return _ctxt.get_kmip_host(_name)->get_key_by_id(id).then([](shared_ptr<symmetric_key> k) {
            return make_ready_future<std::tuple<key_info, bytes>>(std::tuple(k->info(), k->key()));
        });
    });
    co_return make_shared<symmetric_key>(info, b);
}

shared_ptr<symmetric_key> kmip_host::impl::ensure_compatible_key(shared_ptr<symmetric_key> k, const key_info& info) {
    // keys we get back are typically void
    // of block mode/padding info (because this is meaningless
    // from the standpoint of the kmip server).
    // Check and re-init the actual key used based
    // on what the user wants so we adhere to block mode etc.
    if (!info.compatible(k->info())) {
        throw malformed_response_error(fmt::format("Incompatible key: {}", k->info()));
    }
    if (k->info() != info) {
        k = ::make_shared<symmetric_key>(info, k->key());
    }
    return k;
}

[[noreturn]]
static void translate_kmip_error(const kmip_error& e) {
    switch (e.code().value()) {
    case KMIP_IO_FAILURE: 
        std::throw_with_nested(network_error(e.what()));
    case KMIP_MALFORMED_RESPONSE:
        std::throw_with_nested(missing_resource_error(e.what()));        
    //case KMIP_ERROR_AUTH_FAILED: case KMIP_ERROR_CERT_AUTH_FAILED:
      //  std::throw_with_nested(permission_error(e.what()));
    default:
        std::throw_with_nested(service_error(e.what()));
    }
}

future<std::tuple<shared_ptr<symmetric_key>, kmip_host::id_type>> kmip_host::impl::get_or_create_key(const key_info& info, const key_options& opts) {
    kmip_log.debug("{}: Lookup key {}:{}", _name, info, opts);
    try {
        auto linfo = info;
        auto kinfo = co_await _attr_cache.get(kmip_key_info{info, opts});
        co_return std::tuple(ensure_compatible_key(std::get<0>(kinfo), linfo), std::get<1>(kinfo));
    } catch (kmip_error& e) {
        translate_kmip_error(e);
    } catch (base_error&) {
        throw;
    } catch (std::invalid_argument& e) {
        std::throw_with_nested(configuration_error(fmt::format("get_or_create_key: {}", e.what())));
    } catch (...) {
        std::throw_with_nested(service_error(fmt::format("get_or_create_key: {}", std::current_exception())));
    }
}

future<shared_ptr<symmetric_key>> kmip_host::impl::get_key_by_id(const id_type& id, const std::optional<key_info>& info) {
    try {
        auto linfo = info; // maintain on stack
        auto k = co_await _id_cache.get(id);
        if (linfo) {
            k = ensure_compatible_key(k, *linfo);
        }
        co_return k;
    } catch (kmip_error& e) {
        translate_kmip_error(e);
    } catch (base_error&) {
        throw;
    } catch (std::invalid_argument& e) {
        std::throw_with_nested(configuration_error(fmt::format("get_key_by_id: {}", e.what())));
    } catch (...) {
        std::throw_with_nested(service_error(fmt::format("get_key_by_id: {}", std::current_exception())));
    }
}

kmip_host::kmip_host(encryption_context& ctxt, const std::string& name, const std::unordered_map<sstring, sstring>& map)
    : kmip_host(ctxt, name, [&ctxt, &map] {
        host_options opts;
        map_wrapper<std::unordered_map<sstring, sstring>> m(map);

        try {
            static const std::regex wsc("\\s*,\\s*"); // comma+whitespace

            std::string hosts = m("hosts").value();

            auto i = std::sregex_token_iterator(hosts.begin(), hosts.end(), wsc, -1);
            auto e = std::sregex_token_iterator();

            std::for_each(i, e, [&](const std::string & s) {
                opts.hosts.emplace_back(s);
            });
        } catch (std::bad_optional_access&) {
            throw std::invalid_argument("No KMIP host names provided");
        }

        opts.certfile = m("certificate").value_or("");
        opts.keyfile = m("keyfile").value_or("");
        opts.truststore = m("truststore").value_or("");
        opts.priority_string = m("priority_string").value_or("");

        opts.username = m("username").value_or("");
        opts.password = ctxt.maybe_decrypt_config_value(m("password").value_or(""));

        if (m("max_command_retries")) {
            opts.max_command_retries = std::stoul(*m("max_command_retries"));
        }

        opts.key_cache_expiry = parse_expiry(m("key_cache_expiry"));
        opts.key_cache_refresh = parse_expiry(m("key_cache_refresh"));

        return opts;
    }())
{}

kmip_host::kmip_host(encryption_context& ctxt, const std::string& name, const host_options& opts)
    : _impl(std::make_unique<impl>(ctxt, name, opts))
{}

kmip_host::~kmip_host() = default;

future<> kmip_host::connect() {
    return _impl->connect();
}

future<> kmip_host::disconnect() {
    return _impl->disconnect();
}

future<std::tuple<shared_ptr<symmetric_key>, kmip_host::id_type>> kmip_host::get_or_create_key(const key_info& info, const key_options& opts) {
    return _impl->get_or_create_key(info, opts);
}

future<shared_ptr<symmetric_key>> kmip_host::get_key_by_id(const id_type& id, std::optional<key_info> info) {
    return _impl->get_key_by_id(id, info);
}

future<shared_ptr<symmetric_key>> kmip_host::get_key_by_name(const std::string& name) {
    return _impl->get_key_by_id(_impl->kmip_id_to_id(name));    
}

std::ostream& operator<<(std::ostream& os, const kmip_host::key_options& opts) {
    return os << opts.template_name << ":" << opts.key_namespace;
}

}

class memory_file {
public:
    memory_file(size_t size = 128) 
        : _buf(size)
        , _fp(::fmemopen(_buf.data(), _buf.size(), "w"))
    {}
    memory_file(const memory_file&) = delete;
    memory_file(memory_file&&) = delete;
    ~memory_file() {
        ::fclose(_fp);
    }
    operator FILE*() const {
        return _fp;
    }
    std::string str() const {
        ::fflush(_fp);
        return std::string(_buf.begin(), _buf.begin() + ::ftell(_fp));
    }
private:
    std::vector<char> _buf;
    ::FILE* _fp;
};

static std::string KMIP_reason2string(result_reason error) {
    memory_file f;
    ::kmip_print_result_reason_enum(f, error);
    return f.str();
}

static std::string KMIP_error2string(int error) {
    memory_file f;
    ::kmip_print_error_string(f, error);
    return f.str();
}

static std::string KMIP_cryptographic_algorithm2string(cryptographic_algorithm a) {
    memory_file f;
    ::kmip_print_cryptographic_algorithm_enum(f, a);
    return f.str();
}

static std::string KMIP_block_cipher_mode2string(block_cipher_mode a) {
    memory_file f;
    ::kmip_print_block_cipher_mode_enum(f, a);
    return f.str();
}

static std::string KMIP_padding_method2string(padding_method a) {
    memory_file f;
    ::kmip_print_padding_method_enum(f, a);
    return f.str();
}
