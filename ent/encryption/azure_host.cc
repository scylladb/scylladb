/*
 * Copyright (C) 2025 ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <stdexcept>

#include "utils/log.hh"
#include "utils/hash.hh"
#include "utils/loading_cache.hh"
#include "utils/azure/identity/default_credentials.hh"
#include "utils/azure/identity/service_principal_credentials.hh"
#include "azure_host.hh"
#include "encryption.hh"

using namespace std::chrono_literals;

static logging::logger azlog("azure_vault");

namespace encryption {

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

future<> azure_host::impl::init() {
    throw std::logic_error("Not implemented");
}

const azure_host::host_options& azure_host::impl::options() const {
    return _options;
}

future<azure_host::key_and_id_type> azure_host::impl::get_or_create_key(const key_info& info) {
    throw std::logic_error("Not implemented");
}

future<azure_host::key_and_id_type> azure_host::impl::create_key(const attr_cache_key& key) {
    throw std::logic_error("Not implemented");
}

future<azure_host::key_ptr> azure_host::impl::get_key_by_id(const azure_host::id_type& id, const key_info& info) {
    throw std::logic_error("Not implemented");
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
