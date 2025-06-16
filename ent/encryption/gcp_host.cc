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
#include <seastar/http/exception.hh>

#include <rapidxml.h>
#include <openssl/evp.h>
#include <openssl/pem.h>

#include <boost/regex.hpp>

#include <fmt/chrono.h>
#include <fmt/ranges.h>
#include <fmt/std.h>
#include "utils/to_string.hh"

#include "gcp_host.hh"
#include "encryption.hh"
#include "encryption_exceptions.hh"
#include "symmetric_key.hh"
#include "utils.hh"
#include "utils/hash.hh"
#include "utils/loading_cache.hh"
#include "utils/UUID.hh"
#include "utils/UUID_gen.hh"
#include "utils/rjson.hh"
#include "utils/gcp/gcp_credentials.hh"
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

using namespace utils::gcp;
using namespace rest;

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
    future<> stop();
    const host_options& options() const {
        return _options;
    }

    future<std::tuple<shared_ptr<symmetric_key>, id_type>> get_or_create_key(const key_info&, const option_override* = nullptr);
    future<shared_ptr<symmetric_key>> get_key_by_id(const id_type&, const key_info&, const option_override* = nullptr);

    using scopes_type = std::string; // space separated. avoids some transforms. makes other easy.
private:
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

    using key_values = std::initializer_list<std::pair<std::string_view, std::string_view>>;

    static std::tuple<std::string, std::string> parse_key(std::string_view);

    future<rjson::value> gcp_auth_post_with_retry(std::string_view uri, const rjson::value& body, const credentials_source&);

    encryption_context& _ctxt;
    std::string _name;
    host_options _options;

    shared_ptr<tls::certificate_credentials> _certs;

    std::unordered_map<credentials_source, google_credentials> _cached_credentials;

    utils::loading_cache<attr_cache_key, key_and_id_type, 2, utils::loading_cache_reload_enabled::yes,
        utils::simple_entry_size<key_and_id_type>, attr_cache_key_hash> _attr_cache;
    utils::loading_cache<id_cache_key, bytes, 2, utils::loading_cache_reload_enabled::yes, 
        utils::simple_entry_size<bytes>, id_cache_key_hash> _id_cache;
    shared_ptr<seastar::tls::certificate_credentials> _creds;
    std::unordered_map<bytes, shared_ptr<symmetric_key>> _cache;
    bool _initialized = false;
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

static const char KMS_SCOPE[] = "https://www.googleapis.com/auth/cloudkms";

future<rjson::value> encryption::gcp_host::impl::gcp_auth_post_with_retry(std::string_view uri, const rjson::value& body, const credentials_source& src) {
    auto i = _cached_credentials.find(src);
    if (i == _cached_credentials.end()) {
        try {
            auto c = !src.gcp_credentials_file.empty()
                ? co_await google_credentials::from_file(src.gcp_credentials_file)
                : co_await google_credentials::get_default_credentials()
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
            co_await creds.refresh(KMS_SCOPE, _certs);
        } catch (...) {
            std::throw_with_nested(permission_error("Error refreshing credentials"));
        }

        try {
            auto res = co_await send_request(uri, _certs, body, httpd::operation_type::POST, key_values({
                { utils::gcp::AUTHORIZATION, utils::gcp::format_bearer(creds.token) },
            }));
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

    // will only do network calls on shard 0
    _certs =::make_shared<tls::certificate_credentials>();

    if (!_options.priority_string.empty()) {
        _certs->set_priority_string(_options.priority_string);
    } else {
        _certs->set_priority_string(db::config::default_tls_priority);
    }
    if (!_options.certfile.empty()) {
        co_await _certs->set_x509_key_file(_options.certfile, _options.keyfile, seastar::tls::x509_crt_format::PEM);
    }
    if (!_options.truststore.empty()) {
        co_await _certs->set_x509_trust_file(_options.truststore, seastar::tls::x509_crt_format::PEM);
    } else {
        co_await _certs->set_system_trust();
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

future<> encryption::gcp_host::impl::stop() {
    co_await _attr_cache.stop();
    co_await _id_cache.stop();
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
     * the master key name part, decrypt the first using the second (GCP KMS Decrypt),
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

future<> encryption::gcp_host::stop() {
    return _impl->stop();
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
