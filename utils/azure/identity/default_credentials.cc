/*
 * Copyright (C) 2025 ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <seastar/core/coroutine.hh>

#include "exceptions.hh"
#include "default_credentials.hh"
#include "azure_cli_credentials.hh"
#include "managed_identity_credentials.hh"
#include "service_principal_credentials.hh"

namespace azure {

default_credentials::default_credentials(const source_set& sources,
        const sstring& imds_endpoint, const sstring& truststore,
        const sstring& priority_string, const sstring& logctx)
    : credentials(logctx)
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
    _token = co_await _creds->get_access_token(resource_uri);
}

future<> default_credentials::detect(const resource_type& resource_uri) {
    if (_creds) {
        co_return;
    }
    if (_sources.contains<source::Env>()) {
        az_creds_logger.info("[{}] Detecting credentials in environment", *this);
        if (auto creds = co_await get_credentials_from_env(resource_uri)) {
            az_creds_logger.info("[{}] Credentials found in environment!", *this);
            _creds = std::move(*creds);
            co_return;
        }
    }
    if (_sources.contains<source::AzureCli>()) {
        az_creds_logger.info("[{}] Detecting credentials in CLI", *this);
        if (auto creds = co_await get_credentials_from_azure_cli(resource_uri)) {
            az_creds_logger.info("[{}] Credentials found in CLI!", *this);
            _creds = std::move(*creds);
            co_return;
        }
    }
    if (_sources.contains<source::Imds>()) {
        az_creds_logger.info("[{}] Detecting credentials in IMDS", *this);
        if (auto creds = co_await get_credentials_from_imds(resource_uri)) {
            az_creds_logger.info("[{}] Credentials found in IMDS!", *this);
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
        az_creds_logger.info("[{}] No credentials found in environment", *this);
        co_return std::nullopt;
    } else if (!creds_complete) {
        az_creds_logger.info("[{}] Incomplete credentials. Both 'AZURE_TENANT_ID' and 'AZURE_CLIENT_ID', and at least one of 'AZURE_CLIENT_SECRET', 'AZURE_CLIENT_CERTIFICATE_PATH' must be provided. Currently:\n"
                "Tenant ID is {}set\n"
                "Client ID is {}set\n"
                "Client Secret is {}set\n"
                "Client Certificate is {}set\n"
                "Authority host is {}set",
                *this,
                tenant_id ? "" : "NOT ",
                client_id ? "" : "NOT ",
                client_secret ? "" : "NOT ",
                client_certificate_path ? "" : "NOT ",
                authority ? "" : "NOT ");
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
        _logctx);
    try {
        co_await creds->get_access_token(resource_uri);
    } catch (auth_error& e) {
        az_creds_logger.info("[{}] Failed to obtain token from environment: {}", *this, e.what());
        co_return std::nullopt;
    }
    co_return creds;
}

future<default_credentials::credentials_opt> default_credentials::get_credentials_from_azure_cli(const resource_type& resource_uri) {
    auto creds = std::make_unique<azure_cli_credentials>(_logctx);
    try {
        co_await creds->get_access_token(resource_uri);
    } catch (auth_error& e) {
        az_creds_logger.info("[{}] Failed to obtain token from Azure CLI: {}", *this, e.what());
        co_return std::nullopt;
    }
    co_return creds;
}

/**
 * @brief Get Managed Identity credentials from Azure Instance Metadata Service (IMDS).
 *
 * Assume the node is an Azure VM, and try to query the IMDS token endpoint.
 *
 * If it's not an Azure VM, the service will be unreachable and
 * `get_access_token()` will throw a `timed_out_error`.
 *
 * If it is an Azure VM, but the node has none or multiple user-assigned
 * managed identities, the request will fail with a 400 error.
 *
 * If it is an Azure VM and it has a system-assigned managed identity, or exactly
 * one user-assigned managed identity, the request will succeed.
 */
future<default_credentials::credentials_opt> default_credentials::get_credentials_from_imds(const resource_type& resource_uri) {
    auto creds = std::make_unique<managed_identity_credentials>(_imds_endpoint, _logctx);
    try {
        co_await creds->get_access_token(resource_uri);
    } catch (timed_out_error& e) {
        az_creds_logger.info("[{}] Failed to connect to IMDS: timed out", *this);
        co_return std::nullopt;
    } catch (std::system_error& e) {
        az_creds_logger.info("[{}] System error while accessing IMDS: {}", *this, e.what());
        switch (e.code().value()) {
        case static_cast<int>(std::errc::network_unreachable):
        case static_cast<int>(std::errc::host_unreachable):
        case static_cast<int>(std::errc::connection_refused):
        case static_cast<int>(std::errc::connection_reset):
        case static_cast<int>(std::errc::connection_aborted):
        case static_cast<int>(std::errc::timed_out):
            co_return std::nullopt;
        default:
            throw;
        }
    } catch (auth_error& e) {
        az_creds_logger.info("[{}] Got unexpected return from IMDS: {}", *this, e.what());
        co_return std::nullopt;
    }
    co_return creds;
}

}