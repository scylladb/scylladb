/*
 * Copyright (C) 2025 ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <seastar/http/reply.hh>

#include "utils/rjson.hh"

namespace azure {

class auth_error : public std::exception {
    std::string _msg;
public:
    auth_error(std::string_view msg) : _msg(msg) {}
    const char* what() const noexcept override {
        return _msg.c_str();
    }
};

// Exception representing an error from Azure Entra or IMDS.
class creds_auth_error : public auth_error {
    // The response body may contain an 'error' and 'error_description'.
    // https://learn.microsoft.com/en-us/entra/identity-platform/reference-error-codes
    // https://learn.microsoft.com/en-us/entra/identity/managed-identities-azure-resources/how-to-use-vm-token#error-handling
    static constexpr char ERROR_CODE_KEY[] = "error";
    static constexpr char ERROR_DESCRIPTION_KEY[] = "error_description";
    http::reply::status_type _status;
    std::string _code;
public:
    creds_auth_error(const http::reply::status_type& status)
        : auth_error(fmt::format("{} ({})", status, fmt::to_string(status)))
        , _status(status)
        , _code("")
    {}
    creds_auth_error(const http::reply::status_type& status, std::string_view code, std::string_view desc)
        : auth_error(fmt::format("{}: {}", code, desc))
        , _status(status)
        , _code(code)
    {}
    const http::reply::status_type& status() const {
        return _status;
    }
    const std::string& code() const {
        return _code;
    }
    static creds_auth_error make_error(const http::reply::status_type& status, std::string_view result) {
        const auto& jres = rjson::try_parse(result);
        if (!jres) {
            return creds_auth_error(status);
        }
        const auto& code = rjson::get_opt<std::string>(*jres, ERROR_CODE_KEY);
        const auto& desc = rjson::get_opt<std::string>(*jres, ERROR_DESCRIPTION_KEY);
        if (code && desc) {
            return creds_auth_error(status, *code, *desc);
        }
        return creds_auth_error(status);
    }
};

// Common error codes from Microsoft Entra:
// https://learn.microsoft.com/en-us/entra/identity-platform/reference-error-codes#handling-error-codes-in-your-application
namespace entra_errors {
    [[maybe_unused]] static constexpr char InvalidRequest[] = "invalid_request";
    [[maybe_unused]] static constexpr char InvalidGrant[] = "invalid_grant";
    [[maybe_unused]] static constexpr char UnauthorizedClient[] = "unauthorized_client";
    [[maybe_unused]] static constexpr char InvalidClient[] = "invalid_client";
    [[maybe_unused]] static constexpr char UnsupportedGrantType[] = "unsupported_grant_type";
    [[maybe_unused]] static constexpr char InvalidResource[] = "invalid_resource";
    [[maybe_unused]] static constexpr char InteractionRequired[] = "interaction_required";
    [[maybe_unused]] static constexpr char TemporarilyUnavailable[] = "temporarily_unavailable";
}

// Common error codes from IMDS:
// https://learn.microsoft.com/en-us/entra/identity/managed-identities-azure-resources/how-to-use-vm-token#http-response-reference
namespace imds_errors {
    [[maybe_unused]] static constexpr char InvalidResource[] = "invalid_resource";
    [[maybe_unused]] static constexpr char BadRequest102[] = "bad_request_102";
    [[maybe_unused]] static constexpr char UnknownSource[] = "unknown_source";
    [[maybe_unused]] static constexpr char InvalidRequest[] = "invalid_request";
    [[maybe_unused]] static constexpr char UnauthorizedClient[] = "unauthorized_client";
    [[maybe_unused]] static constexpr char AccessDenied[] = "access_denied";
    [[maybe_unused]] static constexpr char UnsupportedResponseType[] = "unsupported_response_type";
    [[maybe_unused]] static constexpr char InvalidScope[] = "invalid_scope";
    [[maybe_unused]] static constexpr char Unknown[] = "unknown";
}

}