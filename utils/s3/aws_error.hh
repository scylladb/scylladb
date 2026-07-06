/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <seastar/core/sstring.hh>
#include <seastar/http/reply.hh>
#include <seastar/util/bool_class.hh>
#include <string>
#include <string_view>
#include <unordered_map>
#include "utils/http_client_error_processing.hh"

namespace aws {

enum class aws_error_type : uint8_t {
    INCOMPLETE_SIGNATURE = 0,
    INTERNAL_FAILURE = 1,
    INVALID_ACTION = 2,
    INVALID_CLIENT_TOKEN_ID = 3,
    INVALID_PARAMETER_COMBINATION = 4,
    INVALID_QUERY_PARAMETER = 5,
    INVALID_PARAMETER_VALUE = 6,
    MISSING_ACTION = 7,
    MISSING_AUTHENTICATION_TOKEN = 8,
    MISSING_PARAMETER = 9,
    OPT_IN_REQUIRED = 10,
    REQUEST_EXPIRED = 11,
    SERVICE_UNAVAILABLE = 12,
    THROTTLING = 13,
    VALIDATION = 14,
    ACCESS_DENIED = 15,
    RESOURCE_NOT_FOUND = 16,
    UNRECOGNIZED_CLIENT = 17,
    MALFORMED_QUERY_STRING = 18,
    SLOW_DOWN = 19,
    REQUEST_TIME_TOO_SKEWED = 20,
    INVALID_SIGNATURE = 21,
    SIGNATURE_DOES_NOT_MATCH = 22,
    INVALID_ACCESS_KEY_ID = 23,
    REQUEST_TIMEOUT = 24,
    NOT_INITIALIZED = 25,
    MEMORY_ALLOCATION = 26,
    NETWORK_CONNECTION = 99,
    UNKNOWN = 100,
    CLIENT_SIGNING_FAILURE = 101,
    USER_CANCELLED = 102,
    ENDPOINT_RESOLUTION_FAILURE = 103,
    // HTTP errors without additional information in the response body
    HTTP_UNAUTHORIZED = 115,
    HTTP_FORBIDDEN = 116,
    HTTP_NOT_FOUND = 117,
    HTTP_TOO_MANY_REQUESTS = 118,
    HTTP_INTERNAL_SERVER_ERROR = 119,
    HTTP_BANDWIDTH_LIMIT_EXCEEDED = 120,
    HTTP_SERVICE_UNAVAILABLE = 121,
    HTTP_REQUEST_TIMEOUT = 122,
    HTTP_PAGE_EXPIRED = 123,
    HTTP_LOGIN_TIMEOUT = 124,
    HTTP_GATEWAY_TIMEOUT = 125,
    HTTP_NETWORK_CONNECT_TIMEOUT = 126,
    HTTP_NETWORK_READ_TIMEOUT = 127,
    SERVICE_EXTENSION_START_RANGE = 128,
    // S3 specific
    BUCKET_ALREADY_EXISTS = 129,
    BUCKET_ALREADY_OWNED_BY_YOU = 130,
    INVALID_OBJECT_STATE = 131,
    NO_SUCH_BUCKET = 132,
    NO_SUCH_KEY = 133,
    NO_SUCH_UPLOAD = 134,
    OBJECT_ALREADY_IN_ACTIVE_TIER = 135,
    OBJECT_NOT_IN_ACTIVE_TIER = 136,
    // STS specific
    EXPIRED_TOKEN = 137,
    INVALID_AUTHORIZATION_MESSAGE = 138,
    INVALID_IDENTITY_TOKEN = 139,
    I_D_P_COMMUNICATION_ERROR = 140,
    I_D_P_REJECTED_CLAIM = 141,
    MALFORMED_POLICY_DOCUMENT = 142,
    PACKED_POLICY_TOO_LARGE = 143,
    REGION_DISABLED = 144,
    // S3 specific - cont.
    ANNOTATION_LIMIT_EXCEEDED = 145,
    ANNOTATION_NAME_TOO_LONG = 146,
    ENCRYPTION_TYPE_MISMATCH = 147,
    IDEMPOTENCY_PARAMETER_MISMATCH = 148,
    INVALID_ANNOTATION_NAME = 149,
    INVALID_PREFIX = 150,
    INVALID_REQUEST = 151,
    INVALID_WRITE_OFFSET = 152,
    NO_SUCH_ANNOTATION = 153,
    TOO_MANY_PARTS = 154,
    UNSUPPORTED_MEDIA_TYPE = 155,
    // OCI specific
    OCI_CANNOT_PARSE_REQUEST = 156,
    OCI_LIMIT_EXCEEDED = 157,
    OCI_QUOTA_EXCEEDED = 158,
    OCI_RELATED_RESOURCE_NOT_AUTHORIZED_OR_NOT_FOUND = 159,
    OCI_CONFLICT = 160,
    OCI_EXTERNAL_SERVER_INCORRECT_STATE = 161,
    OCI_INCORRECT_STATE = 162,
    OCI_INVALIDATED_RETRY_TOKEN = 163,
    OCI_RESOURCE_LOCKED = 164,
    OCI_NOT_AUTHORIZED_OR_RESOURCE_ALREADY_EXISTS = 165,
    OCI_NO_ETAG_MATCH = 166,
    OCI_PAYLOAD_TOO_LARGE = 167,
    OCI_UNPROCESSABLE_ENTITY = 168,
    OCI_REQUEST_HEADER_FIELDS_TOO_LARGE = 169,
    OCI_METHOD_NOT_IMPLEMENTED = 170,
    OCI_EXTERNAL_SERVER_UNREACHABLE = 171,
    OCI_EXTERNAL_SERVER_TIMEOUT = 172,
    OCI_EXTERNAL_SERVER_INVALID_RESPONSE = 173,
    OCI_INVALID_STORAGE_TIER = 174,
    // No error set
    OK = 255
};

class aws_error;
using aws_errors = std::unordered_map<std::string_view, const aws_error>;

class aws_error {
    aws_error_type _type{aws_error_type::OK};
    std::string _message;
    utils::http::retryable _is_retryable{utils::http::retryable::no};

public:
    aws_error() = default;
    aws_error(aws_error_type error_type, utils::http::retryable is_retryable);
    aws_error(aws_error_type error_type, std::string&& error_message, utils::http::retryable is_retryable);
    [[nodiscard]] const std::string& get_error_message() const { return _message; }
    [[nodiscard]] aws_error_type get_error_type() const { return _type; }
    [[nodiscard]] utils::http::retryable is_retryable() const { return _is_retryable; }
    static std::optional<aws_error> parse(seastar::sstring&& body);
    static aws_error from_http_code(seastar::http::reply::status_type http_code);
    static aws_error from_system_error(const std::system_error& system_error);
    static aws_error from_exception_ptr(std::exception_ptr exception);
    static const aws_errors& get_errors();
};

class aws_exception : public std::exception {
    aws_error _error;

public:
    explicit aws_exception(const aws_error& error) noexcept : _error(error) {}
    explicit aws_exception(aws_error&& error) noexcept : _error(std::move(error)) {}

    const char* what() const noexcept override { return _error.get_error_message().c_str(); }

    const aws_error& error() const noexcept { return _error; }
};

} // namespace aws

template <>
struct fmt::formatter<aws::aws_error_type> : fmt::formatter<string_view> {
    auto format(const aws::aws_error_type& error_type, fmt::format_context& ctx) const {
        return fmt::format_to(ctx.out(), "{}", static_cast<unsigned>(error_type));
    }
};
