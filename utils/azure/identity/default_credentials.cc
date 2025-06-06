/*
 * Copyright (C) 2025 ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <seastar/core/coroutine.hh>
#include <seastar/core/with_timeout.hh>
#include <seastar/http/exception.hh>

#include "exceptions.hh"
#include "default_credentials.hh"
#include "azure_cli_credentials.hh"
#include "managed_identity_credentials.hh"
#include "service_principal_credentials.hh"

namespace azure {

default_credentials::default_credentials(const source_set& sources,
        const sstring& imds_endpoint, const sstring& truststore,
        const sstring& priority_string, credential_logger logger)
    : credentials(std::move(logger))
    , _sources(sources)
    , _truststore(truststore)
    , _priority_string(priority_string)
    , _imds_endpoint(imds_endpoint)
{}

future<> default_credentials::refresh(const resource_type& resource_uri) {
    if (!_creds) {
        co_await detect(resource_uri);
    }
    SCYLLA_ASSERT(_creds);
    token = co_await _creds->get_access_token(resource_uri);
}

future<> default_credentials::detect(const resource_type& resource_uri) {
    if (_creds) {
        co_return;
    }
    if (_sources.contains<source::Env>()) {
        logger.debug(*this, "Detecting credentials in environment");
        if (auto creds = co_await get_credentials_from_env(resource_uri)) {
            logger.debug(*this, "Credentials found in environment!");
            _creds = std::move(*creds);
            co_return;
        }
    }
    if (_sources.contains<source::AzureCli>()) {
        logger.debug(*this, "Detecting credentials in CLI");
        if (auto creds = co_await get_credentials_from_azure_cli(resource_uri)) {
            logger.debug(*this, "Credentials found in CLI!");
            _creds = std::move(*creds);
            co_return;
        }
    }
    if (_sources.contains<source::Imds>()) {
        logger.debug(*this, "Detecting credentials in IMDS");
        if (auto creds = co_await get_credentials_from_imds(resource_uri)) {
            logger.debug(*this, "Credentials found in IMDS!");
            _creds = std::move(*creds);
            co_return;
        }
    }
    throw auth_error("No credentials found in any source.");
}

future<default_credentials::credentials_opt> default_credentials::get_credentials_from_env(const resource_type& resource_uri) {
    auto tenant_id = std::getenv("AZURE_TENANT_ID");
    auto client_id = std::getenv("AZURE_CLIENT_ID");
    auto client_secret = std::getenv("AZURE_CLIENT_SECRET");
    auto client_certificate_path = std::getenv("AZURE_CLIENT_CERTIFICATE_PATH");
    auto authority = std::getenv("AZURE_AUTHORITY_HOST");
    auto creds_found = tenant_id || client_id || client_secret || client_certificate_path;
    auto creds_complete = tenant_id && client_id && (client_secret || client_certificate_path);
    if (!creds_found) {
        logger.debug(*this, "No credentials found in environment");
        co_return std::nullopt;
    } else if (!creds_complete) {
        logger.debug(*this, "Incomplete credentials. Both 'AZURE_TENANT_ID' and 'AZURE_CLIENT_ID', and at least one of 'AZURE_CLIENT_SECRET', 'AZURE_CLIENT_CERTIFICATE_PATH' must be provided. Currently:");
        logger.debug(*this, "Tenant ID is {}set", tenant_id ? "" : "NOT ");
        logger.debug(*this, "Client ID is {}set", client_id ? "" : "NOT ");
        logger.debug(*this, "Client Secret is {}set", client_secret ? "" : "NOT ");
        logger.debug(*this, "Client Certificate is {}set", client_certificate_path ? "" : "NOT ");
        logger.debug(*this, "Authority host is {}set", authority ? "" : "NOT ");
        co_return std::nullopt;
    }
    auto creds = std::make_unique<service_principal_credentials>(
        tenant_id,
        client_id,
        client_secret ? client_secret : "",
        client_certificate_path ? client_certificate_path : "",
        authority ? authority : "",
        _truststore,
        _priority_string,
        logger);
    try {
        co_await creds->get_access_token(resource_uri);
    } catch (auth_error& e) {
        logger.debug(*this, "Failed to obtain token from environment: {}", e.what());
        co_return std::nullopt;
    }
    co_return creds;
}

future<default_credentials::credentials_opt> default_credentials::get_credentials_from_azure_cli(const resource_type& resource_uri) {
    auto creds = std::make_unique<azure_cli_credentials>(logger);
    try {
        co_await creds->get_access_token(resource_uri);
    } catch (auth_error& e) {
        logger.debug(*this, "Failed to obtain token from Azure CLI: {}", e.what());
        co_return std::nullopt;
    }
    co_return creds;
}

/**
 * @brief Get Managed Identity credentials from Azure Instance Metadata Service (IMDS).
 *
 * Assume the node is an Azure VM, and try to query the IMDS token endpoint.
 *
 * If it's not an Azure VM, the service will be unreachable.
 * Instead of waiting for the system timeout (connect(2) ETIMEDOUT), which can
 * take several minutes, assume the service is unreachable after 3 seconds.
 *
 * If it is an Azure VM, but the node has none or multiple user-assigned
 * managed identities, the request will fail with a 400 error.
 *
 * If it is an Azure VM and it has a system-assigned managed identity, or exactly
 * one user-assigned managed identity, the request will succeed.
 */
future<default_credentials::credentials_opt> default_credentials::get_credentials_from_imds(const resource_type& resource_uri) {
    const auto timeout = std::chrono::seconds(3);
    auto creds = std::make_unique<managed_identity_credentials>(_imds_endpoint, logger);
    try {
        auto fut = creds->get_access_token(resource_uri);
        // Leave the future behind on timeout. The socket will eventually time out as well.
        co_await with_timeout(timer<>::clock::now() + timeout, std::move(fut));
    } catch (timed_out_error&) {
        logger.debug(*this, "Failed to connect to IMDS.");
        co_return std::nullopt;
    } catch (auth_error& e) {
        logger.debug(*this, "Got unexpected return from IMDS: {}", e.what());
        co_return std::nullopt;
    }
    co_return creds;
}

}