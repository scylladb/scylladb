/*
 * Copyright (C) 2025 ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <stdexcept>

#include <boost/regex.hpp>
#include <boost/algorithm/string.hpp>

#include "db/config.hh"
#include "utils/log.hh"
#include "utils/hash.hh"
#include "utils/rjson.hh"
#include "utils/base64.hh"
#include "utils/loading_cache.hh"
#include "utils/rest/client.hh"
#include "utils/azure/identity/default_credentials.hh"
#include "utils/azure/identity/service_principal_credentials.hh"
#include "azure_host.hh"
#include "encryption.hh"
#include "encryption_exceptions.hh"

using namespace std::chrono_literals;

static logging::logger azlog("azure_vault");

namespace encryption {

vault_error vault_error::make_error(const http::reply::status_type& status, std::string_view result) {
    const auto& jres = rjson::parse(result);
    const auto& error_details = rjson::get(jres, ERROR_KEY);
    return vault_error(
            status,
            rjson::get<std::string>(error_details, ERROR_CODE_KEY),
            rjson::get<std::string>(error_details, ERROR_MESSAGE_KEY)
    );
}

class vault_log_filter : public rest::http_log_filter {
public:
    enum class op_type {
        wrapkey,
    };

    explicit vault_log_filter(op_type op) : _op(op) {}

    string_opt filter_header(std::string_view name, std::string_view value) const override {
        if (boost::iequals(name, "Authorization") && value.starts_with("Bearer")) {
            return REDACTED_VALUE;
        }
        return std::nullopt;
    }

    string_opt filter_body(body_type type, std::string_view body) const override {
        if (_op == op_type::wrapkey && type == body_type::request) {
            auto j = rjson::parse(body);
            auto val = rjson::find(j, "value");
            if (val) {
                val->SetString(REDACTED_VALUE);
                return rjson::print(j);
            }
        }
        return std::nullopt;
    }
private:
    op_type _op;
};

class azure_host::impl {
public:
    static inline constexpr std::chrono::milliseconds default_expiry = 600s;
    static inline constexpr std::chrono::milliseconds default_refresh = 1200s;

    impl(const std::string& name, const host_options&);
    future<> init();
    const host_options& options() const;
    future<key_and_id_type> get_or_create_key(const key_info&);
    future<key_ptr> get_key_by_id(const id_type&, const key_info&);
private:
    const std::string _name;
    const std::string _log_prefix;
    const host_options _options;
    std::unique_ptr<azure::credentials> _credentials;

    struct attr_cache_key {
        seastar::sstring master_key;
        key_info info;
        bool operator==(const attr_cache_key& v) const = default;
    };

    struct attr_cache_key_hash {
        size_t operator()(const attr_cache_key& k) const {
            return utils::tuple_hash()(std::tie(k.master_key, k.info.len));
        }
    };

    friend struct fmt::formatter<attr_cache_key>;

    template<typename Key, typename Value, typename Hash>
    using cache_type = utils::loading_cache<
        Key,
        Value,
        2,
        utils::loading_cache_reload_enabled::yes,
        utils::simple_entry_size<Value>,
        Hash
    >;
    cache_type<attr_cache_key, key_and_id_type, attr_cache_key_hash> _attr_cache;

    static constexpr char AKV_HOST_TEMPLATE[] = "{}.vault.azure.net";
    static constexpr char AKV_PATH_TEMPLATE[] = "/keys/{}/{}/{}?api-version=7.4";
    static constexpr char AKV_LATEST_VERSION[] = ""; // an empty version denotes the latest
    static constexpr char AKV_WRAPKEY_OP[] = "wrapkey";
    static constexpr char AKV_ENCRYPTION_ALG[] = "RSA-OAEP-256";
    static constexpr char AKV_TOKEN_RESOURCE_URI[] = "https://vault.azure.net"; // no trailing slash

    static std::tuple<std::string, std::string> parse_key(std::string_view);
    static std::tuple<std::string, std::string, unsigned> parse_vault(std::string_view);
    future<shared_ptr<tls::certificate_credentials>> make_creds();
    future<rjson::value> send_request(const sstring& host, unsigned port, bool use_https, const sstring& path, const rjson::value& body, const rest::http_log_filter& filter);
    future<key_and_id_type> create_key(const attr_cache_key&);
};

azure_host::impl::impl(const std::string& name, const host_options& options)
    : _name(name)
    , _log_prefix(fmt::format("AzureVault:{}", name))
    , _options(options)
    , _credentials()
    , _attr_cache(utils::loading_cache_config{
        .max_size = std::numeric_limits<size_t>::max(),
        .expiry = options.key_cache_expiry.value_or(default_expiry),
        .refresh = options.key_cache_refresh.value_or(default_refresh)}, azlog, std::bind_front(&impl::create_key, this))
{
    if (!_options.tenant_id.empty() && !_options.client_id.empty() && (!_options.client_secret.empty() || !_options.client_cert.empty())) {
        _credentials = std::make_unique<azure::service_principal_credentials>(
                options.tenant_id, options.client_id, options.client_secret, options.client_cert,
                options.authority, options.truststore, options.priority_string, _log_prefix);
        return;
    }
    azlog.debug("[{}] No credentials configured. Falling back to default credentials.", _log_prefix);
    _credentials = std::make_unique<azure::default_credentials>(
            azure::default_credentials::all_sources, _options.imds_endpoint,
            _options.truststore, _options.priority_string, _log_prefix);
}

/**
 * Wraps exceptions to encryption::base_error exceptions.
 * Should be used in all public methods.
 */
template <typename T, typename Callable>
static future<T> wrap_exceptions(const std::string& context, Callable&& func) {
    try {
        co_return co_await func();
    } catch (base_error&) {
        throw;
    } catch (const std::invalid_argument& e) {
        std::throw_with_nested(configuration_error(fmt::format("{}: {}", context, e.what())));
    } catch (const rjson::malformed_value& e) {
        std::throw_with_nested(malformed_response_error(fmt::format("{}: {}", context, e.what())));
    } catch (...) {
        std::throw_with_nested(service_error(fmt::format("{}: {}", context, std::current_exception())));
    }
}

future<> azure_host::impl::init() {
    throw std::logic_error("Not implemented");
}

const azure_host::host_options& azure_host::impl::options() const {
    return _options;
}

future<azure_host::key_and_id_type> azure_host::impl::get_or_create_key(const key_info& info) {
    attr_cache_key key {
        .master_key = _options.master_key,
        .info = info,
    };

    if (key.master_key.empty()) {
        throw configuration_error(fmt::format("[{}] No master key set in azure host config or encryption attributes", _log_prefix));
    }
    co_return co_await wrap_exceptions<key_and_id_type>("get_or_create_key", [this, &key] -> future<key_and_id_type> {
        co_return co_await _attr_cache.get(key);
    });
}

future<azure_host::key_ptr> azure_host::impl::get_key_by_id(const azure_host::id_type& id, const key_info& info) {
    throw std::logic_error("Not implemented");
}

std::tuple<std::string, std::string> azure_host::impl::parse_key(std::string_view spec) {
    auto i = spec.find_last_of('/');
    if (i == std::string_view::npos) {
        throw std::invalid_argument(fmt::format("Invalid master key spec '{}'. Must be in format <vaultname>/<keyname>", spec));
    }
    if (i >= spec.size() - 1) {
        throw std::invalid_argument(fmt::format("Invalid master key spec '{}'. Key name is missing. Expected format: <vaultname>/<keyname>", spec));
    }
    return std::make_tuple(std::string(spec.substr(0, i)), std::string(spec.substr(i + 1)));
}

std::tuple<std::string, std::string, unsigned> azure_host::impl::parse_vault(std::string_view vault) {
    static const boost::regex vault_name_re(R"([a-zA-Z0-9-]+)");
    static const boost::regex vault_endpoint_re(R"((https?)://([^/:]+)(?::(\d+))?)");

    boost::smatch match;
    std::string tmp{vault};

    if (boost::regex_match(tmp, match, vault_name_re)) {
        // If the vault is just a name, use the default Azure Key Vault endpoint.
        return {"https", fmt::format(AKV_HOST_TEMPLATE, vault), 443};
    }

    if (boost::regex_match(tmp, match, vault_endpoint_re)) {
        std::string scheme = match[1];
        std::string host = match[2];
        std::string port_str = match[3];

        unsigned port = (port_str.empty()) ? (scheme == "https" ? 443 : 80) : std::stoi(port_str);
        return {scheme, host, port};
    }

    throw std::invalid_argument(fmt::format("Invalid vault '{}'. Must be either a name or an endpoint in format: http(s)://<host>[:port]", vault));
}

future<shared_ptr<tls::certificate_credentials>> azure_host::impl::make_creds() {
    auto creds = ::make_shared<tls::certificate_credentials>();
    if (!_options.priority_string.empty()) {
        creds->set_priority_string(_options.priority_string);
    } else {
        creds->set_priority_string(db::config::default_tls_priority);
    }
    if (!_options.truststore.empty()) {
        co_await creds->set_x509_trust_file(_options.truststore, seastar::tls::x509_crt_format::PEM);
    } else {
        co_await creds->set_system_trust();
    }
    co_return creds;
}

future<rjson::value> azure_host::impl::send_request(const sstring& host, unsigned port, bool use_https, const sstring& path, const rjson::value& body, const rest::http_log_filter& filter) {
    auto token = co_await _credentials->get_access_token(AKV_TOKEN_RESOURCE_URI);

    shared_ptr<tls::certificate_credentials> creds;
    std::optional<seastar::tls::tls_options> options;

    if (use_https) {
        creds = co_await make_creds();
        // Do not wait when terminating the TLS connection. TLS close_notify
        // alerts are ignored by the Key Vault service.
        //
        // Also, do not use numeric hosts as SNI hostnames. SNI works only with DNS hostnames.
        // This by extension also disables hostname validation, but that's fine; numeric hosts
        // may appear only in testing.
        bool is_numeric_host = seastar::net::inet_address::parse_numerical(host).has_value();
        sstring server_name = is_numeric_host ? sstring{} : host;
        options = { .wait_for_eof_on_shutdown = false, .server_name = server_name };
    }

    rest::httpclient client(host, port, std::move(creds), options);
    client.target(path);
    client.method(httpd::operation_type::POST);
    client.add_header("Authorization", fmt::format("Bearer {}", token.token));
    client.add_header("Content-Type", "application/json");
    client.content(std::move(rjson::print(body)));

    azlog.trace("Sending request: {}", rest::redacted_request_type{ client.request(), filter });

    auto res = co_await client.send();
    if (res.result() == http::reply::status_type::ok) {
        azlog.trace("Got response: {}", rest::redacted_result_type{ res, filter });
        co_return rjson::parse(res.body());
    } else {
        azlog.trace("Got unexpected response: {}", rest::redacted_result_type{ res, filter });
        throw vault_error::make_error(res.result(), res.body());
    }
}

future<azure_host::key_and_id_type> azure_host::impl::create_key(const attr_cache_key& k) {
    static const vault_log_filter filter{vault_log_filter::op_type::wrapkey};
    auto& info = k.info;
    azlog.debug("[{}] Creating new key: {}", _log_prefix, info);
    auto [vault, keyname] = parse_key(k.master_key);
    auto [scheme, host, port] = parse_vault(vault);
    auto key = make_shared<symmetric_key>(info);
    auto path = fmt::format(AKV_PATH_TEMPLATE, keyname, AKV_LATEST_VERSION, AKV_WRAPKEY_OP);
    auto body = [&key] {
        auto b = rjson::empty_object();
        rjson::add(b, "alg", AKV_ENCRYPTION_ALG);
        rjson::add(b, "value", base64url_encode(key->key()));
        return b;
    }();
    rjson::value resp;
    try {
        resp = co_await send_request(host, port, scheme == "https", path, body, filter);
    } catch (...) {
        azlog.error("[{}] Failed to wrap key {} with master_key={}: {}", _log_prefix, info, k.master_key, std::current_exception());
        throw;
    }
    auto key_id = rjson::get<std::string>(resp, "kid");
    auto cipher = rjson::get<std::string>(resp, "value");
    boost::regex version_regex(R"foo(.*/([^/]+)$)foo");
    boost::smatch match;
    if (!boost::regex_search(key_id, match, version_regex)) {
        throw std::runtime_error(fmt::format("Failed to parse key version from key id {}", key_id));
    }
    auto key_version = match[1].str();

    auto sid = fmt::format("{}/{}/{}:{}", vault, keyname, key_version, cipher);
    bytes id(sid.begin(), sid.end());

    azlog.trace("[{}] Created key id {}", _log_prefix, sid);
    co_return key_and_id_type{ key, id };
}

// ==================== azure_host class implementation ====================

azure_host::azure_host(const std::string& name, const host_options& options)
    : _impl(std::make_unique<impl>(name, options))
{}

azure_host::azure_host([[maybe_unused]] encryption_context& ctxt, const std::string& name, const host_options& options)
    : _impl(std::make_unique<impl>(name, options))
{}

azure_host::azure_host([[maybe_unused]] encryption_context& ctxt, const std::string& name, const std::unordered_map<sstring, sstring>& map)
    : azure_host(ctxt, name, [&map] {
        host_options opts;
        map_wrapper<std::unordered_map<sstring, sstring>> m(map);

        opts.tenant_id = m("azure_tenant_id").value_or("");
        opts.client_id = m("azure_client_id").value_or("");
        opts.client_secret = m("azure_client_secret").value_or("");
        opts.client_cert = m("azure_client_certificate_path").value_or("");
        opts.authority = m("azure_authority_host").value_or("");
        opts.imds_endpoint = m("imds_endpoint").value_or("");

        opts.master_key = m("master_key").value_or("");

        opts.truststore = m("truststore").value_or("");
        opts.priority_string = m("priority_string").value_or("");

        opts.key_cache_expiry = parse_expiry(m("key_cache_expiry"));
        opts.key_cache_refresh = parse_expiry(m("key_cache_refresh"));

        return opts;
    }())
{}

azure_host::~azure_host() = default;

future<> azure_host::init() {
    return _impl->init();
}

const azure_host::host_options& azure_host::options() const {
    return _impl->options();
}

future<azure_host::key_and_id_type> azure_host::get_or_create_key(const key_info& info) {
    return _impl->get_or_create_key(info);
}

future<azure_host::key_ptr> azure_host::get_key_by_id(const azure_host::id_type& id, const key_info& info) {
    return _impl->get_key_by_id(id, info);
}

} // namespace encryption

template<>
struct fmt::formatter<encryption::azure_host::impl::attr_cache_key> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    auto format(const encryption::azure_host::impl::attr_cache_key& d, fmt::format_context& ctxt) const {
        return fmt::format_to(ctxt.out(), "{},{},{}", d.master_key, d.info.alg, d.info.len);
    }
};
