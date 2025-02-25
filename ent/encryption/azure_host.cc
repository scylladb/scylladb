/*
 * Copyright (C) 2025 ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <stdexcept>

#include "azure_host.hh"
#include "encryption.hh"

namespace encryption {

class azure_host::impl {
public:
    impl(const std::string& name, const host_options&);
    future<> init();
    const host_options& options() const;
    future<key_and_id_type> get_or_create_key(const key_info&);
    future<key_ptr> get_key_by_id(const id_type&, const key_info&);
private:
    const std::string _name;
    const host_options _options;
};

azure_host::impl::impl(const std::string& name, const host_options&) {}

future<> azure_host::impl::init() {
    throw std::logic_error("Not implemented");
}

const azure_host::host_options& azure_host::impl::options() const {
    return _options;
}

future<azure_host::key_and_id_type> azure_host::impl::get_or_create_key(const key_info& info) {
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