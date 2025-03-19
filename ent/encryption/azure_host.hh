/*
 * Copyright (C) 2025 ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <memory>

#include <seastar/http/reply.hh>
#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>

#include "symmetric_key.hh"

namespace encryption {

class encryption_context;

class vault_error : public std::exception {
    // The error response always contains a code and a message.
    // https://github.com/microsoft/api-guidelines/blob/vNext/azure/Guidelines.md#handling-errors
    static constexpr char ERROR_KEY[] = "error";
    static constexpr char ERROR_CODE_KEY[] = "code";
    static constexpr char ERROR_MESSAGE_KEY[] = "message";
    http::reply::status_type _status;
    std::string _code, _msg;
public:
    vault_error(const http::reply::status_type& status, std::string_view code, std::string_view msg)
        : _status(status)
        , _code(code)
        , _msg(fmt::format("{}: {}", code, msg))
    {}
    const http::reply::status_type& status() const {
        return _status;
    }
    const std::string& code() const {
        return _code;
    }
    const char* what() const noexcept override {
        return _msg.c_str();
    }
    static vault_error make_error(const http::reply::status_type& status, std::string_view result);
};

// Common error codes from Azure Key Vault:
// https://learn.microsoft.com/en-us/azure/key-vault/general/rest-error-codes
//
// Not to be confused with the more specialized 'innererror' codes:
// https://learn.microsoft.com/en-us/azure/key-vault/general/common-error-codes
namespace vault_errors {
    [[maybe_unused]] static constexpr char Unauthorized[] = "Unauthorized";
    [[maybe_unused]] static constexpr char Forbidden[] = "Forbidden";
    [[maybe_unused]] static constexpr char Throttled[] = "Throttled";
    [[maybe_unused]] static constexpr char KeyNotFound[] = "KeyNotFound";
}

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