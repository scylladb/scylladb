/*
 * Copyright (C) 2025 ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <memory>

#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>

#include "symmetric_key.hh"

namespace encryption {

class encryption_context;

class azure_host {
public:
    class impl;

    using id_type = bytes;
    using key_ptr = shared_ptr<symmetric_key>;
    using key_and_id_type = std::tuple<key_ptr, id_type>;

    struct host_options {
        std::string tenant_id;
        std::string client_id;
        std::string client_secret;
        std::string client_cert;
        std::string authority;
        std::string imds_endpoint;

        // Azure Key Vault Key to encrypt data keys with. Format: <vault name>/<keyname>
        std::string master_key;

        // TLS options
        std::string truststore;
        std::string priority_string;

        std::optional<std::chrono::milliseconds> key_cache_expiry;
        std::optional<std::chrono::milliseconds> key_cache_refresh;
    };

    azure_host(const std::string& name, const host_options&); // used for testing
    azure_host(encryption_context&, const std::string& name, const host_options&);
    azure_host(encryption_context&, const std::string& name, const std::unordered_map<sstring, sstring>&);
    ~azure_host();

    /**
     * Async initialization.
     *
     * Performs preliminary checks.
     * Should precede any calls to the Key Management API.
     */
    future<> init();
    const host_options& options() const;

    /**
     * Key Management API to create or retrieve data keys.
     *
     * Throws encryption::base_error derivatives to indicate failure conditions
     * such as host misconfigurations, network issues, or authentication failures.
     */
    future<key_and_id_type> get_or_create_key(const key_info&);
    future<key_ptr> get_key_by_id(const id_type&, const key_info&);
private:
    std::unique_ptr<impl> _impl;
};

}