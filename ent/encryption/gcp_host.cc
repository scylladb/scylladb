/*
 * Copyright (C) 2024 ScyllaDB
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
#include <seastar/json/formatter.hh>
#include <seastar/http/url.hh>
#include <seastar/http/client.hh>
#include <seastar/http/request.hh>
#include <seastar/http/reply.hh>
#include <seastar/http/exception.hh>
#include <seastar/util/short_streams.hh>

#include <rapidxml.h>
#include <openssl/evp.h>
#include <openssl/pem.h>

#include <boost/regex.hpp>

#define CPP_JWT_USE_VENDORED_NLOHMANN_JSON
#include <jwt/jwt.hpp>

#include <fmt/chrono.h>
#include <fmt/ranges.h>
#include <fmt/std.h>
#include "utils/to_string.hh"

#include "gcp_host.hh"
#include "encryption.hh"
#include "encryption_exceptions.hh"
#include "symmetric_key.hh"
#include "utils/hash.hh"
#include "utils/loading_cache.hh"
#include "utils/UUID.hh"
#include "utils/UUID_gen.hh"
#include "utils/rjson.hh"
#include "marshal_exception.hh"
#include "db/config.hh"

using namespace std::chrono_literals;
using namespace std::string_literals;

logger gcp_log("gcp");

namespace encryption {
bool operator==(const gcp_host::credentials_source& k1, const gcp_host::credentials_source& k2) {
    return k1.gcp_credentials_file == k2.gcp_credentials_file && k1.gcp_impersonate_service_account == k2.gcp_impersonate_service_account;
}
}

template<> 
struct fmt::formatter<encryption::gcp_host::credentials_source> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    auto format(const encryption::gcp_host::credentials_source& d, fmt::format_context& ctxt) const {
        return fmt::format_to(ctxt.out(), "{{ gcp_credentials_file = {}, gcp_impersonate_service_account = {} }}", d.gcp_credentials_file, d.gcp_impersonate_service_account);
    }
};

template<> 
struct std::hash<encryption::gcp_host::credentials_source> {
    size_t operator()(const encryption::gcp_host::credentials_source& a) const {
        return utils::tuple_hash{}(std::tie(a.gcp_credentials_file, a.gcp_impersonate_service_account));
    }
};

class encryption::gcp_host::impl {
public:
    // set a rather long expiry. normal KMS policies are 365-day rotation of keys.
    // we can do with 10 minutes. CMH. maybe even longer.
    // (see comments below on what keys are here)
    static inline constexpr std::chrono::milliseconds default_expiry = 600s;
    static inline constexpr std::chrono::milliseconds default_refresh = 1200s;

    impl(encryption_context& ctxt, const std::string& name, const host_options& options)
        : _ctxt(ctxt)
        , _name(name)
        , _options(options)
        , _attr_cache(utils::loading_cache_config{
            .max_size = std::numeric_limits<size_t>::max(),
            .expiry = options.key_cache_expiry.value_or(default_expiry),
            .refresh = options.key_cache_refresh.value_or(default_refresh)}, gcp_log, std::bind_front(&impl::create_key, this))
        , _id_cache(utils::loading_cache_config{
            .max_size = std::numeric_limits<size_t>::max(),
            .expiry = options.key_cache_expiry.value_or(default_expiry),
            .refresh = options.key_cache_refresh.value_or(default_refresh)}, gcp_log, std::bind_front(&impl::find_key, this))
    {}
    ~impl() = default;

    future<> init();
    const host_options& options() const {
        return _options;
    }

    future<std::tuple<shared_ptr<symmetric_key>, id_type>> get_or_create_key(const key_info&, const option_override* = nullptr);
    future<shared_ptr<symmetric_key>> get_key_by_id(const id_type&, const key_info&, const option_override* = nullptr);

    using scopes_type = std::string; // space separated. avoids some transforms. makes other easy.
private:
    class httpclient;
    using key_and_id_type = std::tuple<shared_ptr<symmetric_key>, id_type>;

    struct attr_cache_key {
        credentials_source src;
        std::string master_key;
        key_info info;
        bool operator==(const attr_cache_key& v) const = default;
    };

    friend struct fmt::formatter<attr_cache_key>;

    struct attr_cache_key_hash {
        size_t operator()(const attr_cache_key& k) const {
            return utils::tuple_hash()(std::tie(k.master_key, k.src, k.info.len));
        }
    };

    struct id_cache_key {
        credentials_source src;
        id_type id;
        bool operator==(const id_cache_key& v) const = default;
    };

    friend struct fmt::formatter<id_cache_key>;

    struct id_cache_key_hash {
        size_t operator()(const id_cache_key& k) const {
            return utils::tuple_hash()(std::tie(k.id, k.src));
        }
    };

    future<key_and_id_type> create_key(const attr_cache_key&);
    future<bytes> find_key(const id_cache_key&);

    using timeout_clock = std::chrono::system_clock;
    using timestamp_type = typename timeout_clock::time_point;

    struct access_token;
    struct user_credentials;
    struct service_account_credentials;
    struct impersonated_service_account_credentials;
    struct compute_engine_credentials{};

    struct google_credentials;


    struct access_token {
        access_token() = default;
        access_token(const rjson::value&);

        std::string token;
        timestamp_type expiry;
        scopes_type scopes;

        bool empty() const;
        bool expired() const;
    };

    struct user_credentials {
        user_credentials(const rjson::value&);

        std::string client_id;
        std::string client_secret;
        std::string refresh_token;
        std::string access_token;
        std::string quota_project_id;
    };

    using p_key = std::unique_ptr<EVP_PKEY>;

    struct service_account_credentials {
        service_account_credentials(const rjson::value&);

        std::string client_id;
        std::string client_email;
        std::string private_key_id;
        std::string private_key_pkcs8;
        std::string token_server_uri;
        std::string project_id;
        std::string quota_project_id;
    };

    struct impersonated_service_account_credentials {
        impersonated_service_account_credentials(std::string principal, google_credentials&&);
        impersonated_service_account_credentials(const rjson::value&);

        std::vector<std::string> delegates;
        std::vector<std::string> scopes;
        std::string quota_project_id;
        std::string iam_endpoint_override;
        std::string target_principal;

        std::unique_ptr<google_credentials> source_credentials;
        access_token token;
    };

    using credentials_variant = std::variant<
        user_credentials,
        service_account_credentials,
        impersonated_service_account_credentials,
        compute_engine_credentials
    >;

    struct google_credentials {
        google_credentials(google_credentials&&) = default;
        google_credentials(credentials_variant&& c)
            : credentials(std::move(c))
        {}
        google_credentials& operator=(google_credentials&&) = default;
        credentials_variant credentials;
        access_token token;
    };

    google_credentials from_data(std::string_view) const;
    google_credentials from_data(const temporary_buffer<char>& buf) const {
        return from_data(std::string_view(buf.get(), buf.size()));
    }
    future<google_credentials> from_file(const std::string& path) const {
        auto buf = co_await read_text_file_fully(path);
        co_return from_data(std::string_view(buf.get(), buf.size()));
    }

    future<google_credentials> get_default_credentials();

    future<access_token> get_access_token(const google_credentials&, const scopes_type& scopes) const;

    future<> refresh(google_credentials&, const scopes_type&) const;

    using key_values = std::initializer_list<std::pair<std::string_view, std::string_view>>;

    static std::string body(key_values kv);

    future<rjson::value> send_request(std::string_view uri, std::string body, std::string_view content_type, httpd::operation_type = httpd::operation_type::GET, key_values headers = {}) const;
    future<rjson::value> send_request(std::string_view uri, const rjson::value& body, httpd::operation_type = httpd::operation_type::GET, key_values headers = {}) const;
    future<> send_request(std::string_view uri, std::string body, std::string_view content_type, const std::function<void(const http::reply&, std::string_view)>&, httpd::operation_type = httpd::operation_type::GET, key_values headers = {}) const;

    static std::tuple<std::string, std::string> parse_key(std::string_view);

    future<rjson::value> gcp_auth_post_with_retry(std::string_view uri, const rjson::value& body, const credentials_source&);

    encryption_context& _ctxt;
    std::string _name;
    host_options _options;

    std::unordered_map<credentials_source, google_credentials> _cached_credentials;

    utils::loading_cache<attr_cache_key, key_and_id_type, 2, utils::loading_cache_reload_enabled::yes,
        utils::simple_entry_size<key_and_id_type>, attr_cache_key_hash> _attr_cache;
    utils::loading_cache<id_cache_key, bytes, 2, utils::loading_cache_reload_enabled::yes, 
        utils::simple_entry_size<bytes>, id_cache_key_hash> _id_cache;
    shared_ptr<seastar::tls::certificate_credentials> _creds;
    std::unordered_map<bytes, shared_ptr<symmetric_key>> _cache;
    bool _initialized = false;
    bool _checked_is_on_gce = false;
    bool _is_on_gce = false;
};

template<typename T, typename C>
static T get_option(const encryption::gcp_host::option_override* oov, std::optional<T> C::* f, const T& def) {
    if (oov) {
        return (oov->*f).value_or(def);
    }
    return {};
};

future<std::tuple<shared_ptr<encryption::symmetric_key>, encryption::gcp_host::id_type>> encryption::gcp_host::impl::get_or_create_key(const key_info& info, const option_override* oov) {
    attr_cache_key key {
        .src = {
            .gcp_credentials_file = get_option(oov, &option_override::gcp_credentials_file, _options.gcp_credentials_file),
            .gcp_impersonate_service_account = get_option(oov, &option_override::gcp_impersonate_service_account, _options.gcp_impersonate_service_account),
        },
        .master_key = get_option(oov, &option_override::master_key, _options.master_key),
        .info = info,
    };

    if (key.master_key.empty()) {
        throw configuration_error("No master key set in gcp host config or encryption attributes");
    }
    try {
        co_return co_await _attr_cache.get(key);
    } catch (base_error&) {
        throw;
    } catch (std::invalid_argument& e) {
        std::throw_with_nested(configuration_error(fmt::format("get_or_create_key: {}", e.what())));
    } catch (rjson::malformed_value& e) {
        std::throw_with_nested(malformed_response_error(fmt::format("get_or_create_key: {}", e.what())));
    } catch (...) {
        std::throw_with_nested(service_error(fmt::format("get_or_create_key: {}", std::current_exception())));
    }
}

future<shared_ptr<encryption::symmetric_key>> encryption::gcp_host::impl::get_key_by_id(const id_type& id, const key_info& info, const option_override* oov) {
    // note: since KMS does not really have any actual "key" association of id -> key,
    // we only cache/query raw bytes of some length. (See below).
    // Thus keys returned are always new objects. But they are not huge...
    id_cache_key key {
        .src = {
            .gcp_credentials_file = get_option(oov, &option_override::gcp_credentials_file, _options.gcp_credentials_file),
            .gcp_impersonate_service_account = get_option(oov, &option_override::gcp_impersonate_service_account, _options.gcp_impersonate_service_account),
        },
        .id = id,
    };
    try {
        auto data = co_await _id_cache.get(key);
        co_return make_shared<symmetric_key>(info, data);
    } catch (base_error&) {
        throw;
    } catch (std::invalid_argument& e) {
        std::throw_with_nested(configuration_error(fmt::format("get_key_by_id: {}", e.what())));
    } catch (rjson::malformed_value& e) {
        std::throw_with_nested(malformed_response_error(fmt::format("get_or_create_key: {}", e.what())));
    } catch (...) {
        std::throw_with_nested(service_error(fmt::format("get_key_by_id: {}", std::current_exception())));
    }
}

static const char CREDENTIAL_ENV_VAR[] = "GOOGLE_APPLICATION_CREDENTIALS";
static const char WELL_KNOWN_CREDENTIALS_FILE[] = "application_default_credentials.json";
static const char CLOUDSDK_CONFIG_DIRECTORY[] = "gcloud";

static const char USER_FILE_TYPE[] = "authorized_user";
static const char SERVICE_ACCOUNT_FILE_TYPE[] = "service_account";
static const char IMPERSONATED_SERVICE_ACCOUNT_FILE_TYPE[] = "impersonated_service_account";

static const char GCE_METADATA_HOST_ENV_VAR[] = "GCE_METADATA_HOST";

static const char DEFAULT_METADATA_SERVER_URL[] = "http://metadata.google.internal";;

static const char METADATA_FLAVOR[] = "Metadata-Flavor";
static const char GOOGLE[] = "Google";

static const char TOKEN_SERVER_URI[] = "https://oauth2.googleapis.com/token";

static const char AUTHORIZATION[] = "Authorization";

static const char KMS_SCOPE[] = "https://www.googleapis.com/auth/cloudkms";
static const char CLOUD_PLATFORM_SCOPE[] = "https://www.googleapis.com/auth/cloud-platform";

//static const char[] CLOUD_SHELL_ENV_VAR = "DEVSHELL_CLIENT_PORT";
//static const char[] SKIP_APP_ENGINE_ENV_VAR = "GOOGLE_APPLICATION_CREDENTIALS_SKIP_APP_ENGINE";
//static const char[] NO_GCE_CHECK_ENV_VAR = "NO_GCE_CHECK";
//static const char[] GCE_METADATA_HOST_ENV_VAR = "GCE_METADATA_HOST";

bool encryption::gcp_host::impl::access_token::empty() const {
    return token.empty();
}

bool encryption::gcp_host::impl::access_token::expired() const {
    if (empty()) {
        return true;
    }
    return timeout_clock::now() >= this->expiry;
}

encryption::gcp_host::impl::user_credentials::user_credentials(const rjson::value& v) 
    : client_id(rjson::get<std::string>(v, "client_id"))
    , client_secret(rjson::get<std::string>(v, "client_secret"))
    , refresh_token(rjson::get<std::string>(v, "refresh_token"))
    , quota_project_id(rjson::get_opt<std::string>(v, "refresh_token").value_or(""))
{}

encryption::gcp_host::impl::service_account_credentials::service_account_credentials(const rjson::value& v)
    : client_id(rjson::get<std::string>(v, "client_id"))
    , client_email(rjson::get<std::string>(v, "client_email"))
    , private_key_id(rjson::get<std::string>(v, "private_key_id"))
    , private_key_pkcs8(rjson::get<std::string>(v, "private_key"))
    , token_server_uri([&] {
        auto token_uri = rjson::get_opt<std::string>(v, "token_uri");
        if (token_uri) {
            // TODO: verify uri
            return *token_uri;
        }
        return std::string{};
    }())
    , project_id(rjson::get_opt<std::string>(v, "project_id").value_or(""))
    , quota_project_id(rjson::get_opt<std::string>(v, "refresh_token").value_or(""))
{}


encryption::gcp_host::impl::impersonated_service_account_credentials::impersonated_service_account_credentials(std::string principal, google_credentials&& c)
    : target_principal(std::move(principal))
    , source_credentials(std::make_unique<google_credentials>(std::move(c))) 
{}

encryption::gcp_host::impl::impersonated_service_account_credentials::impersonated_service_account_credentials(const rjson::value& v)
    : delegates([&] {
        std::vector<std::string> res;
        auto tmp = rjson::find(v, "delegates");
        if (tmp) {
            if (!tmp->IsArray()) {
                throw configuration_error("Malformed json");
            }

            for (const auto& d : tmp->GetArray()) {
                res.emplace_back(std::string(rjson::to_string_view(d)));
            }
        }
        return res;
    }())
    , quota_project_id(rjson::get_opt<std::string>(v, "quota_project_id").value_or(""))
    , target_principal([&] {
        auto url = rjson::get<std::string>(v, "service_account_impersonation_url");

        auto si = url.find_last_of('/');
        auto ei = url.find(":generateAccessToken");

        if (si != std::string::npos && ei != std::string::npos && si < ei) {
            return url.substr(si + 1, ei - si - 1);
        }
        throw configuration_error( "Unable to determine target principal from service account impersonation URL.");
    }())
    , source_credentials([&]() -> decltype(source_credentials) {
        auto& scjson = rjson::get(v, "source_credentials");
        auto type = rjson::get<std::string>(scjson, "type");

        if (type == USER_FILE_TYPE) {
            return std::make_unique<google_credentials>(user_credentials(scjson));
        } else if (type == SERVICE_ACCOUNT_FILE_TYPE) {
            return std::make_unique<google_credentials>(service_account_credentials(scjson));
        }
        throw configuration_error(fmt::format("A credential of type {} is not supported as source credential for impersonation.", type));
    }())
{}

encryption::gcp_host::impl::google_credentials
encryption::gcp_host::impl::from_data(std::string_view content) const {
    auto json = rjson::parse(content);
    auto type = rjson::get_opt<std::string>(json, "type");

    if (!type) {
        throw configuration_error("Error reading credentials from stream, 'type' field not specified.");
    }
    if (type == USER_FILE_TYPE) {
        return google_credentials(user_credentials(json));
    }
    if (type == SERVICE_ACCOUNT_FILE_TYPE) {
        return google_credentials(service_account_credentials(json));
    }
    if (type == IMPERSONATED_SERVICE_ACCOUNT_FILE_TYPE) {
        return google_credentials(impersonated_service_account_credentials(json));
    }
    throw configuration_error(fmt::format(
        "Error reading credentials from stream, 'type' value '{}' not recognized. Expecting '{}', '{}' or '{}'."
        , type, USER_FILE_TYPE, SERVICE_ACCOUNT_FILE_TYPE, IMPERSONATED_SERVICE_ACCOUNT_FILE_TYPE));
}

static std::string get_metadata_server_url() {
    auto meta_host = std::getenv(GCE_METADATA_HOST_ENV_VAR);
    auto token_uri = meta_host ? std::string("http://") + meta_host : DEFAULT_METADATA_SERVER_URL;
    return token_uri;
}

future<encryption::gcp_host::impl::google_credentials>
encryption::gcp_host::impl::get_default_credentials() {
    auto credentials_path = std::getenv(CREDENTIAL_ENV_VAR);

    if (credentials_path != nullptr && strlen(credentials_path)) {
        gcp_log.debug("Attempting to load credentials from file: {}", credentials_path);

        try {
            co_return co_await from_file(credentials_path);
        } catch (...) {
            std::throw_with_nested(configuration_error(fmt::format(
                "Error reading credential file from environment variable {}, value '{}'"
                , CREDENTIAL_ENV_VAR
                , credentials_path
                ))
            );
        }
    }

    {
        std::string well_known_file;
        auto env_path = std::getenv("CLOUDSDK_CONFIG");
        if (env_path) {
            well_known_file = fmt::format("~/{}/{}", env_path, WELL_KNOWN_CREDENTIALS_FILE);
        } else {
            well_known_file = fmt::format("~/.config/{}/{}", CLOUDSDK_CONFIG_DIRECTORY, WELL_KNOWN_CREDENTIALS_FILE);
        }

        if (co_await seastar::file_exists(well_known_file)) {
            gcp_log.debug("Attempting to load credentials from well known file: {}", well_known_file);
            try {
                co_return co_await from_file(well_known_file);
            } catch (...) {
                std::throw_with_nested(configuration_error(fmt::format(
                    "Error reading credential file from location {}"
                    , well_known_file
                    ))
                );
            }
        }
    }

    {
        // Then try Compute Engine and GAE 8 standard environment
        gcp_log.debug("Attempting to load credentials from GCE");

        auto is_on_gce = [this]() -> future<bool> {
            if (_checked_is_on_gce) {
                co_return _is_on_gce;
            }

            auto token_uri = get_metadata_server_url();

            for (int i = 1; i <= 3; ++i) {
                try {
                    co_await send_request(token_uri, std::string{}, "", [&](const http::reply& rep, std::string_view) {
                        _checked_is_on_gce = true;
                        _is_on_gce = rep.get_header(METADATA_FLAVOR) == GOOGLE;
                    }, httpd::operation_type::GET, { { METADATA_FLAVOR, GOOGLE } });
                    if (_checked_is_on_gce) {
                        co_return _is_on_gce;;
                    }
                } catch (...) {
                    // TODO: handle timeout
                    break;
                }
            }

            auto linux_path = "/sys/class/dmi/id/product_name";
            if (co_await seastar::file_exists(linux_path)) {
                auto f = file_desc::open(linux_path, O_RDONLY | O_CLOEXEC);
                char buf[128] = {};
                f.read(buf, 128);
                _is_on_gce = std::string_view(buf).find(GOOGLE) == 0;
            }

            _checked_is_on_gce = true;
            co_return _is_on_gce;
        };

        if (co_await is_on_gce()) {
            co_return compute_engine_credentials{};
        }
    }

    throw configuration_error("Could not determine initial credentials");
}

template<typename Func>
static void for_each_scope(const encryption::gcp_host::impl::scopes_type& s, Func&& f) {
    size_t i = 0;
    while(i < s.size()) {
        auto j = s.find(' ', i + 1);
        f(s.substr(i, j - i));
        i = j;
    }
}

encryption::gcp_host::impl::access_token::access_token(const rjson::value& json)
    : token(rjson::get<std::string>(json, "access_token"))
    , expiry(timeout_clock::now() + std::chrono::seconds(rjson::get<int>(json, "expires_in")))
    , scopes(rjson::get_opt<std::string>(json, "scope").value_or(""))
{}

std::string encryption::gcp_host::impl::body(key_values kv) {
    std::ostringstream ss;
    std::string_view sep = "";
    for (auto& [k, v] : kv) {
        ss << sep << k << "=" << http::internal::url_encode(v);
        sep = "&";
    }
    return ss.str();
}

future<rjson::value> encryption::gcp_host::impl::send_request(std::string_view uri, const rjson::value& body, httpd::operation_type op, key_values headers) const {
    return send_request(uri, rjson::print(body), "application/json", op, std::move(headers));
}

future<rjson::value> encryption::gcp_host::impl::send_request(std::string_view uri, std::string body, std::string_view content_type, httpd::operation_type op, key_values headers) const {
    rjson::value v;
    co_await send_request(uri, std::move(body), content_type, [&](const http::reply& rep, std::string_view s) {
        if (rep._status != http::reply::status_type::ok) {
            gcp_log.trace("Got unexpected response ({})", rep._status);
            for (auto& [k, v] : rep._headers) {
                gcp_log.trace("{}: {}", k, v);
            }
            gcp_log.trace("{}", s);
            throw httpd::unexpected_status_error(rep._status);
        }
        v = rjson::parse(s); 
    }, op, std::move(headers));
    co_return v;
}

future<> encryption::gcp_host::impl::send_request(std::string_view uri, std::string body, std::string_view content_type, const std::function<void(const http::reply&, std::string_view)>& handler, httpd::operation_type op, key_values headers) const {
    // Extremely simplified URI parsing. Does not handle any params etc. But we do not expect such here.
    static boost::regex simple_url(R"foo((https?):\/\/([^\/:]+)(:\d+)?(\/.*)?)foo");

    boost::smatch m;
    std::string tmp(uri);
    if (!boost::regex_match(tmp, m, simple_url)) {
        throw std::invalid_argument(fmt::format("Could not parse URI {}", uri));
    }

    auto scheme = m[1].str();
    auto host = m[2].str();
    auto port = m[3].str();
    auto path = m[4].str();

    auto addr = co_await net::dns::resolve_name(host, net::inet_address::family::INET /* CMH our client does not handle ipv6 well?*/);
    auto certs = scheme == "https"
        ? ::make_shared<tls::certificate_credentials>()
        : shared_ptr<tls::certificate_credentials>()
        ;
    if (certs) {
        if (!_options.priority_string.empty()) {
            certs->set_priority_string(_options.priority_string);
        } else {
            certs->set_priority_string(db::config::default_tls_priority);
        }
        if (!_options.certfile.empty()) {
            co_await certs->set_x509_key_file(_options.certfile, _options.keyfile, seastar::tls::x509_crt_format::PEM);
        }
        if (!_options.truststore.empty()) {
            co_await certs->set_x509_trust_file(_options.truststore, seastar::tls::x509_crt_format::PEM);
        } else {
            co_await certs->set_system_trust();
        }
    }

    uint16_t pi = port.empty() ? (certs ? 443 : 80) : uint16_t(std::stoi(port.substr(1)));
    auto client = certs 
        ? http::experimental::client(socket_address(addr, pi), std::move(certs), host)
        : http::experimental::client(socket_address(addr, pi))
        ;
    if (path.empty()) {
        path = "/";
    }

    gcp_log.trace("Resolved {} -> {}:{}{}", uri, addr, pi, path);

    auto req = http::request::make(op, host, path);

    for (auto& [k, v] : headers) {
        req._headers[sstring(k)] = sstring(v);
    }

    if (!body.empty()) {
        if (content_type.empty()) {
            content_type = "application/x-www-form-urlencoded";
        }
        req.write_body("", std::move(body));
        req.set_mime_type(sstring(content_type));
    }

    gcp_log.trace("Sending {} request to {} ({}): {}", content_type, uri, headers, body);

    co_await client.make_request(std::move(req), [&] (const http::reply& rep, input_stream<char>&& in) -> future<> {
        auto&lh = handler;
        auto lin = std::move(in);
        auto result = co_await util::read_entire_stream_contiguous(lin);
        gcp_log.trace("Got response {}: {}", int(rep._status), result);
        lh(rep, result);
    });

    co_await client.close();
}


future<> encryption::gcp_host::impl::refresh(google_credentials& c, const scopes_type& scopes) const {
    if (!c.token.expired() && c.token.scopes == scopes) {
        co_return;
    }
    c.token = co_await get_access_token(c, scopes);
}

future<encryption::gcp_host::impl::access_token> 
encryption::gcp_host::impl::get_access_token(const google_credentials& creds, const scopes_type& scope) const {
    co_return co_await std::visit(overloaded_functor {
        [&](const user_credentials& c) -> future<access_token> {
            assert(!c.refresh_token.empty());
            auto json = co_await send_request(TOKEN_SERVER_URI, body({
                { "client_id", c.client_id },
                { "client_secret", c.client_secret },
                { "refresh_token", c.refresh_token },
                { "grant_type", "grant_type" },
            }), "", httpd::operation_type::POST);

            co_return access_token{ json };
        },
        [&](const service_account_credentials& c) -> future<access_token> {
            using namespace jwt::params;

            jwt::jwt_object obj{algorithm("RS256"), secret(c.private_key_pkcs8), headers({{"kid", c.private_key_id }})};

            auto uri = c.token_server_uri.empty() ? TOKEN_SERVER_URI : c.token_server_uri;
            obj.add_claim("iss", c.client_email)
                .add_claim("iat", timeout_clock::now())
                .add_claim("exp", timeout_clock::now() + std::chrono::seconds(3600))
                .add_claim("scope", scope)
                .add_claim("aud", uri)
            ;
            auto sign = obj.signature();

            auto json = co_await send_request(uri, body({
                { "grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer" },
                { "assertion", sign }
            }), "", httpd::operation_type::POST);
            co_return access_token{ json };
        },
        [&](const impersonated_service_account_credentials& c) -> future<access_token> {
            auto json_body = rjson::empty_object();
            auto scopes = rjson::empty_array();
            for_each_scope(scope, [&](std::string s) {
                rjson::push_back(scopes, rjson::from_string(s));
            });

            rjson::add(json_body, "scope", std::move(scopes));

            if (!c.delegates.empty()) {
                auto delegates = rjson::empty_array();
                for (auto& d : c.delegates) {
                    rjson::push_back(delegates, rjson::from_string(d));
                }
                rjson::add(json_body, "delegates", std::move(delegates));
            }

            rjson::add(json_body, "lifetime", "3600s");

            co_await refresh(*c.source_credentials, CLOUD_PLATFORM_SCOPE);

            auto endpoint = c.iam_endpoint_override.empty()
                ? fmt::format("https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/{}:generateAccessToken", c.target_principal)
                : c.iam_endpoint_override
                ;
            auto json = co_await send_request(endpoint, json_body, httpd::operation_type::POST, {
                { AUTHORIZATION, fmt::format("Bearer {}", c.source_credentials->token.token) },
            });

            struct tm tmp;
            ::strptime(rjson::get<std::string>(json, "expireTime").data(), "%FT%TZ", &tmp);

            access_token a;

            a.expiry = timeout_clock::from_time_t(::mktime(&tmp));
            a.scopes = scope;
            a.token = rjson::get<std::string>(json, "accessToken");

            co_return a;
        },
        [this](const compute_engine_credentials& c) -> future<access_token> {
            auto meta_uri = get_metadata_server_url();
            auto token_uri = meta_uri + "/computeMetadata/v1/instance/service-accounts/default/token";
            try {
                 auto json = co_await send_request(token_uri, std::string{}, "", httpd::operation_type::GET, { { METADATA_FLAVOR, GOOGLE } });
                 co_return access_token{ json };
            } catch (...) {
                std::throw_with_nested(service_error("Unexpected Error code trying to get security access token from Compute Engine metadata for the default service account"));
            }
        }
    }, creds.credentials);
}

future<rjson::value> encryption::gcp_host::impl::gcp_auth_post_with_retry(std::string_view uri, const rjson::value& body, const credentials_source& src) {
    auto i = _cached_credentials.find(src);
    if (i == _cached_credentials.end()) {
        try {
            auto c = !src.gcp_credentials_file.empty()
                ? co_await from_file(src.gcp_credentials_file)
                : co_await get_default_credentials()
                ;
            if (!src.gcp_credentials_file.empty()) {
                gcp_log.trace("Loaded credentials from {}", src.gcp_credentials_file);
            }
            if (!src.gcp_impersonate_service_account.empty()) {
                c = google_credentials(impersonated_service_account_credentials(src.gcp_impersonate_service_account, std::move(c)));
            }
            i = _cached_credentials.emplace(src, std::move(c)).first;
        } catch (...) {
            gcp_log.warn("Error resolving credentials for {}: {}", src, std::current_exception());
            throw;
        }
    }

    assert(i != _cached_credentials.end()); // should either be set now or we threw.

    auto& creds = i->second;

    int retries = 0;

    for (;;) {
        try {
            co_await this->refresh(creds, KMS_SCOPE);
        } catch (...) {
            std::throw_with_nested(permission_error("Error refreshing credentials"));
        }

        try {
            auto res = co_await send_request(uri, body, httpd::operation_type::POST, {
                { AUTHORIZATION, fmt::format("Bearer {}", creds.token.token) },
            });
            co_return res;
        } catch (httpd::unexpected_status_error& e) {
            gcp_log.debug("{}: Got unexpected response: {}", uri, e.status());
            if (e.status() == http::reply::status_type::unauthorized && retries++ < 3) {
                // refresh access token and retry.
                continue;
            }
            if (e.status() == http::reply::status_type::unauthorized) {
                std::throw_with_nested(permission_error(std::string(uri)));
            }
            std::throw_with_nested(service_error(std::string(uri)));
        } catch (...) {
            std::throw_with_nested(network_error(std::string(uri)));
        }
    }
}

static constexpr char GCP_KMS_QUERY_TEMPLATE[] = "https://cloudkms.googleapis.com/v1/projects/{}/locations/{}/keyRings/{}/cryptoKeys/{}:{}";

future<> encryption::gcp_host::impl::init() {
    if (_initialized) {
        co_return;
    }

    if (!_options.master_key.empty()) {
        gcp_log.debug("Looking up master key");

        attr_cache_key k{
            .src = _options,
            .master_key = _options.master_key,
            .info = key_info{ .alg = "AES", .len = 128 },
        };
        co_await create_key(k);
        gcp_log.debug("Master key exists");
    } else {
        gcp_log.info("No default master key configured. Not verifying.");
    }
    _initialized = true;
}

std::tuple<std::string, std::string> encryption::gcp_host::impl::parse_key(std::string_view spec) {
    auto i = spec.find_last_of('/');
    if (i == std::string_view::npos) {
        throw std::invalid_argument(fmt::format("Invalid master key spec '{}'. Must be in format <keyring>/<keyname>", spec));
    }
    return std::make_tuple(std::string(spec.substr(0, i)), std::string(spec.substr(i + 1)));
}

future<encryption::gcp_host::impl::key_and_id_type> encryption::gcp_host::impl::create_key(const attr_cache_key& k) {
    auto& info = k.info;

    /**
     * Google GCP KMS does allow us to create keys, but like AWS this would 
     * force us to deal with permissions and assignments etc. We instead
     * require a pre-prepared key.
     * 
     * Like AWS, we cannot get the actual key out, nor can we really bulk 
     * encrypt/decrypt things. So we do just like with AWS KMS, and generate 
     * a data key, and encrypt it as the key ID.
     * 
     * For ID -> key, we simply split the ID into the encrypted key part, and
     * the master key name part, decrypt the first using the second (AWS KMS Decrypt),
     * and create a local key using the result.
     * 
     * Data recovery:
     * Assuming you have data encrypted using a KMS generated key, you will have
     * metadata detailing algorithm, key length etc (see sstable metadata, and key info).
     * Metadata will also include a byte blob representing the ID of the encryption key.
     * For GCP KMS, the ID will actually be a text string:
     *  <Key chain name>:<Key name>:<base64 encoded blob>
     *
     * I.e. something like:
     *   mykeyring:mykey:e56sadfafa3324ff=/wfsdfwssdf
     *
     * The actual data key can be retrieved by doing a KMS "Decrypt" of the data blob part
     * using the KMS key referenced by the key ID. This gives back actual key data that can
     * be used to create a symmetric_key with algo, length etc as specified by metadata.
     *
     */

    // avoid creating too many keys and too many calls. If we are not shard 0, delegate there.
    if (this_shard_id() != 0) {
        auto [data, id] = co_await smp::submit_to(0, [this, k]() -> future<std::tuple<bytes, id_type>> {
            auto host = _ctxt.get_gcp_host(_name);
            auto [key, id] = co_await host->_impl->_attr_cache.get(k);
            co_return std::make_tuple(key != nullptr ? key->key() : bytes{}, id);
        });
        co_return key_and_id_type{ 
            data.empty() ? nullptr : make_shared<symmetric_key>(info, data), 
            id 
        };
    }

    // note: since external keys are _not_ stored,
    // there is nothing we can "look up" or anything. Always 
    // new key here.

    gcp_log.debug("Creating new key: {}", info);

    auto [keyring, keyname] = parse_key(k.master_key);

    auto key = make_shared<symmetric_key>(info);
    auto url = fmt::format(GCP_KMS_QUERY_TEMPLATE, 
        _options.gcp_project_id,
        _options.gcp_location,
        keyring,
        keyname,
        "encrypt"
    );
    auto query = rjson::empty_object();
    rjson::add(query, "plaintext", std::string(base64_encode(key->key())));

    auto response = co_await gcp_auth_post_with_retry(url, query, k.src);
    auto cipher = rjson::get<std::string>(response, "ciphertext");
    auto data = base64_decode(cipher);

    auto sid = fmt::format("{}/{}:{}", keyring, keyname, cipher);
    bytes id(sid.begin(), sid.end());

    gcp_log.trace("Created key id {}", sid);

    co_return key_and_id_type{ key, id };
}

future<bytes> encryption::gcp_host::impl::find_key(const id_cache_key& k) {
    // avoid creating too many keys and too many calls. If we are not shard 0, delegate there.
    if (this_shard_id() != 0) {
        co_return co_await smp::submit_to(0, [this, k]() -> future<bytes> {
            auto host = _ctxt.get_gcp_host(_name);
            auto bytes = co_await host->_impl->_id_cache.get(k);
            co_return bytes;
        });
    }

    // See create_key. ID consists of <master id>:<encrypted key blob>.
    // master id can contain ':', but blob will not.
    // (we are being wasteful, and keeping the base64 encoding - easier to read)
    std::string_view id(reinterpret_cast<const char*>(k.id.data()), k.id.size());
    gcp_log.debug("Finding key: {}", id);

    auto pos = id.find_last_of(':');
    auto pos2 = id.find_last_of('/', pos - 1);
    if (pos == id_type::npos || pos2 == id_type::npos || pos2 >= pos) {
        throw std::invalid_argument(fmt::format("Not a valid key id: {}", id));
    }

    std::string keyring(id.begin(), id.begin() + pos2);
    std::string keyname(id.begin() + pos2 + 1, id.begin() + pos);
    std::string enc(id.begin() + pos + 1, id.end());

    auto url = fmt::format(GCP_KMS_QUERY_TEMPLATE, 
        _options.gcp_project_id,
        _options.gcp_location,
        keyring,
        keyname,
        "decrypt"
    );
    auto query = rjson::empty_object();
    rjson::add(query, "ciphertext", enc);

    auto response = co_await gcp_auth_post_with_retry(url, query, k.src);
    auto data = base64_decode(rjson::get<std::string>(response, "plaintext"));

    // we know nothing about key type etc, so just return data.
    co_return data;
}

encryption::gcp_host::gcp_host(encryption_context& ctxt, const std::string& name, const host_options& options)
    : _impl(std::make_unique<impl>(ctxt, name, options))
{}

encryption::gcp_host::gcp_host(encryption_context& ctxt, const std::string& name, const std::unordered_map<sstring, sstring>& map)
    : gcp_host(ctxt, name, [&map] {
        host_options opts;
        map_wrapper<std::unordered_map<sstring, sstring>> m(map);

        opts.master_key = m("master_key").value_or("");

        opts.gcp_project_id = m("gcp_project_id").value_or(""); 
        opts.gcp_location = m("gcp_location").value_or("");

        opts.gcp_credentials_file = m("gcp_credentials_file").value_or("");
        opts.gcp_impersonate_service_account = m("gcp_impersonate_service_account").value_or("");

        opts.certfile = m("certfile").value_or("");
        opts.keyfile = m("keyfile").value_or("");
        opts.truststore = m("truststore").value_or("");
        opts.priority_string = m("priority_string").value_or("");

        opts.key_cache_expiry = parse_expiry(m("key_cache_expiry"));
        opts.key_cache_refresh = parse_expiry(m("key_cache_refresh"));

        return opts;
    }())
{}

encryption::gcp_host::~gcp_host() = default;

future<> encryption::gcp_host::init() {
    return _impl->init();
}

const encryption::gcp_host::host_options& encryption::gcp_host::options() const {
    return _impl->options();
}

future<std::tuple<shared_ptr<encryption::symmetric_key>, encryption::gcp_host::id_type>> encryption::gcp_host::get_or_create_key(const key_info& info, const option_override* oov) {
    return _impl->get_or_create_key(info, oov);
}

future<shared_ptr<encryption::symmetric_key>> encryption::gcp_host::get_key_by_id(const id_type& id, const key_info& info, const option_override* oov) {
    return _impl->get_key_by_id(id, info, oov);
}

template<> 
struct fmt::formatter<encryption::gcp_host::impl::attr_cache_key> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    auto format(const encryption::gcp_host::impl::attr_cache_key& d, fmt::format_context& ctxt) const {
        return fmt::format_to(ctxt.out(), "{},{},{}", d.master_key, d.src.gcp_credentials_file, d.src.gcp_impersonate_service_account);
    }
};

template<> 
struct fmt::formatter<encryption::gcp_host::impl::id_cache_key> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    auto format(const encryption::gcp_host::impl::id_cache_key& d, fmt::format_context& ctxt) const {
        return fmt::format_to(ctxt.out(), "{},{},{}", d.id, d.src.gcp_credentials_file, d.src.gcp_impersonate_service_account);
    }
};
