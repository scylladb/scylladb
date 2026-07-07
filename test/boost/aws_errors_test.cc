/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#define BOOST_TEST_MODULE object_storage

#include "utils/s3/aws_error.hh"
#include <boost/test/unit_test.hpp>
#include <seastar/core/sstring.hh>
#include <seastar/http/exception.hh>

enum class message_style : uint8_t { singular = 1, plural = 2 };

namespace aws {
std::ostream& boost_test_print_type(std::ostream& os, const aws::aws_error_type& error_type) {
    return os << fmt::underlying(error_type);
}
} // namespace aws

static seastar::sstring
build_xml_response(const std::string& exception, const std::string& message, const std::string& requestId, message_style style = message_style::singular) {
    return fmt::format(R"(<?xml version="1.0" encoding="UTF-8"?>
{}
    {}
        <Error>
            <Code>{}</Code>
            <Message>{}</Message>
            <Resource>/mybucket/myfile.bin</Resource>
            {}
        </Error>
    {}
    {}
{})",
                       style == message_style::plural ? "<OtherRoot>" : "",
                       style == message_style::plural ? "<Errors>" : "",
                       exception,
                       message,
                       style == message_style::singular ? "<RequestId>" + requestId + "</RequestId>" : "",
                       style == message_style::plural ? "</Errors>" : "",
                       style == message_style::plural ? "<RequestId>" + requestId + "</RequestId>" : "",
                       style == message_style::plural ? "</OtherRoot>" : "");
}

BOOST_AUTO_TEST_CASE(TestXmlErrorPayload) {
    std::string message = "Test Message";
    std::string requestId = "Request Id";
    auto error = aws::aws_error::parse(build_xml_response("IncompleteSignatureException", message, requestId)).value();
    BOOST_REQUIRE_EQUAL(aws::aws_error_type::INCOMPLETE_SIGNATURE, error.get_error_type());
    BOOST_REQUIRE_EQUAL(message, error.get_error_message());
    BOOST_REQUIRE_EQUAL(error.is_retryable(), utils::http::retryable::no);

    error = aws::aws_error::parse(build_xml_response("InternalFailure", message, requestId, message_style::plural)).value();
    BOOST_REQUIRE_EQUAL(aws::aws_error_type::INTERNAL_FAILURE, error.get_error_type());
    BOOST_REQUIRE_EQUAL(message, error.get_error_message());
    BOOST_REQUIRE_EQUAL(error.is_retryable(), utils::http::retryable::yes);

    error = aws::aws_error::parse(build_xml_response("IDontExist", message, requestId, message_style::plural)).value();
    BOOST_REQUIRE_EQUAL(aws::aws_error_type::UNKNOWN, error.get_error_type());
    BOOST_REQUIRE_EQUAL(message, error.get_error_message());
    BOOST_REQUIRE_EQUAL(error.is_retryable(), utils::http::retryable::no);

    auto no_error = aws::aws_error::parse("");
    BOOST_REQUIRE_EQUAL(no_error.has_value(), false);

    no_error =
        aws::aws_error::parse("Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.");
    BOOST_REQUIRE_EQUAL(no_error.has_value(), false);

    std::string response = "                                                              ";
    response += build_xml_response("InternalFailure", message, requestId, message_style::singular);
    error = aws::aws_error::parse(response).value();
    BOOST_REQUIRE_EQUAL(aws::aws_error_type::INTERNAL_FAILURE, error.get_error_type());
    BOOST_REQUIRE_EQUAL(message, error.get_error_message());
    BOOST_REQUIRE_EQUAL(error.is_retryable(), utils::http::retryable::yes);
}

BOOST_AUTO_TEST_CASE(TestErrorsWithPrefixParse) {
    std::string message = "Test Message";
    std::string exceptionPrefix = "blahblahblah#";
    std::string requestId = "Request Id";
    for (const auto& [exception, err] : aws::aws_error::get_errors()) {
        auto error = aws::aws_error::parse(build_xml_response(exceptionPrefix + std::string(exception), message, requestId)).value();
        BOOST_REQUIRE_EQUAL(err.get_error_type(), error.get_error_type());
        BOOST_REQUIRE_EQUAL(message, error.get_error_message());
        BOOST_REQUIRE_EQUAL(err.is_retryable(), error.is_retryable());
    }

    auto error = aws::aws_error::parse(build_xml_response(exceptionPrefix + "IDon'tExist", "JunkMessage", requestId)).value();
    BOOST_REQUIRE_EQUAL(aws::aws_error_type::UNKNOWN, error.get_error_type());
    BOOST_REQUIRE_EQUAL("JunkMessage", error.get_error_message());
    BOOST_REQUIRE_EQUAL(error.is_retryable(), utils::http::retryable::no);
}

BOOST_AUTO_TEST_CASE(TestErrorsWithoutPrefixParse) {
    std::string message = "Test Message";
    std::string requestId = "Request Id";
    for (const auto& [exception, err] : aws::aws_error::get_errors()) {
        auto error = aws::aws_error::parse(build_xml_response(std::string(exception), message, requestId)).value();
        BOOST_REQUIRE_EQUAL(err.get_error_type(), error.get_error_type());
        BOOST_REQUIRE_EQUAL(message, error.get_error_message());
        BOOST_REQUIRE_EQUAL(err.is_retryable(), error.is_retryable());
    }
    auto error = aws::aws_error::parse(build_xml_response("IDon'tExist", "JunkMessage", requestId)).value();
    BOOST_REQUIRE_EQUAL(aws::aws_error_type::UNKNOWN, error.get_error_type());
    BOOST_REQUIRE_EQUAL("JunkMessage", error.get_error_message());
    BOOST_REQUIRE_EQUAL(error.is_retryable(), utils::http::retryable::no);
}

BOOST_AUTO_TEST_CASE(TestAwsS3ModeledErrors) {
    static const std::unordered_map<std::string_view, aws::aws_error_type> expected_errors{
        {"AnnotationLimitExceeded", aws::aws_error_type::ANNOTATION_LIMIT_EXCEEDED},
        {"AnnotationNameTooLong", aws::aws_error_type::ANNOTATION_NAME_TOO_LONG},
        {"EncryptionTypeMismatch", aws::aws_error_type::ENCRYPTION_TYPE_MISMATCH},
        {"IdempotencyParameterMismatch", aws::aws_error_type::IDEMPOTENCY_PARAMETER_MISMATCH},
        {"InvalidAnnotationName", aws::aws_error_type::INVALID_ANNOTATION_NAME},
        {"InvalidPrefix", aws::aws_error_type::INVALID_PREFIX},
        {"InvalidRequest", aws::aws_error_type::INVALID_REQUEST},
        {"InvalidWriteOffset", aws::aws_error_type::INVALID_WRITE_OFFSET},
        {"NoSuchAnnotation", aws::aws_error_type::NO_SUCH_ANNOTATION},
        {"TooManyParts", aws::aws_error_type::TOO_MANY_PARTS},
        {"UnsupportedMediaType", aws::aws_error_type::UNSUPPORTED_MEDIA_TYPE},
    };

    std::string message = "Test Message";
    std::string requestId = "Request Id";
    for (const auto& [exception, error_type] : expected_errors) {
        auto error = aws::aws_error::parse(build_xml_response(std::string(exception), message, requestId)).value();
        BOOST_REQUIRE_EQUAL(error_type, error.get_error_type());
        BOOST_REQUIRE_EQUAL(message, error.get_error_message());
        BOOST_REQUIRE_EQUAL(error.is_retryable(), utils::http::retryable::no);
    }
}

BOOST_AUTO_TEST_CASE(TestOciObjectStorageErrors) {
    struct expected_error {
        aws::aws_error_type type;
        utils::http::retryable is_retryable;
    };
    static const std::unordered_map<std::string_view, expected_error> expected_errors{
        {"CannotParseRequest", {aws::aws_error_type::OCI_CANNOT_PARSE_REQUEST, utils::http::retryable::no}},
        {"InvalidParameter", {aws::aws_error_type::INVALID_PARAMETER_VALUE, utils::http::retryable::no}},
        {"LimitExceeded", {aws::aws_error_type::OCI_LIMIT_EXCEEDED, utils::http::retryable::no}},
        {"MissingParameter", {aws::aws_error_type::MISSING_PARAMETER, utils::http::retryable::no}},
        {"QuotaExceeded", {aws::aws_error_type::OCI_QUOTA_EXCEEDED, utils::http::retryable::no}},
        {"RelatedResourceNotAuthorizedOrNotFound", {aws::aws_error_type::OCI_RELATED_RESOURCE_NOT_AUTHORIZED_OR_NOT_FOUND, utils::http::retryable::no}},
        {"NotAuthenticated", {aws::aws_error_type::MISSING_AUTHENTICATION_TOKEN, utils::http::retryable::no}},
        {"NotAllowed", {aws::aws_error_type::INVALID_ACTION, utils::http::retryable::no}},
        {"NotAuthorized", {aws::aws_error_type::ACCESS_DENIED, utils::http::retryable::no}},
        {"SignUpRequired", {aws::aws_error_type::OPT_IN_REQUIRED, utils::http::retryable::no}},
        {"NotAuthorizedOrNotFound", {aws::aws_error_type::RESOURCE_NOT_FOUND, utils::http::retryable::no}},
        {"NotFound", {aws::aws_error_type::RESOURCE_NOT_FOUND, utils::http::retryable::no}},
        {"NamespaceNotFound", {aws::aws_error_type::RESOURCE_NOT_FOUND, utils::http::retryable::no}},
        {"MethodNotAllowed", {aws::aws_error_type::INVALID_ACTION, utils::http::retryable::no}},
        {"Conflict", {aws::aws_error_type::OCI_CONFLICT, utils::http::retryable::no}},
        {"ExternalServerIncorrectState", {aws::aws_error_type::OCI_EXTERNAL_SERVER_INCORRECT_STATE, utils::http::retryable::yes}},
        {"IncorrectState", {aws::aws_error_type::OCI_INCORRECT_STATE, utils::http::retryable::yes}},
        {"InvalidatedRetryToken", {aws::aws_error_type::OCI_INVALIDATED_RETRY_TOKEN, utils::http::retryable::no}},
        {"ResourceLocked", {aws::aws_error_type::OCI_RESOURCE_LOCKED, utils::http::retryable::no}},
        {"NotAuthorizedOrResourceAlreadyExists", {aws::aws_error_type::OCI_NOT_AUTHORIZED_OR_RESOURCE_ALREADY_EXISTS, utils::http::retryable::no}},
        {"NoEtagMatch", {aws::aws_error_type::OCI_NO_ETAG_MATCH, utils::http::retryable::no}},
        {"PayloadTooLarge", {aws::aws_error_type::OCI_PAYLOAD_TOO_LARGE, utils::http::retryable::no}},
        {"UnprocessableEntity", {aws::aws_error_type::OCI_UNPROCESSABLE_ENTITY, utils::http::retryable::no}},
        {"TooManyRequests", {aws::aws_error_type::THROTTLING, utils::http::retryable::yes}},
        {"RequestHeaderFieldsTooLarge", {aws::aws_error_type::OCI_REQUEST_HEADER_FIELDS_TOO_LARGE, utils::http::retryable::no}},
        {"InternalServerError", {aws::aws_error_type::INTERNAL_FAILURE, utils::http::retryable::yes}},
        {"MethodNotImplemented", {aws::aws_error_type::OCI_METHOD_NOT_IMPLEMENTED, utils::http::retryable::no}},
        {"ExternalServerUnreachable", {aws::aws_error_type::OCI_EXTERNAL_SERVER_UNREACHABLE, utils::http::retryable::yes}},
        {"ExternalServerTimeout", {aws::aws_error_type::OCI_EXTERNAL_SERVER_TIMEOUT, utils::http::retryable::yes}},
        {"ExternalServerInvalidResponse", {aws::aws_error_type::OCI_EXTERNAL_SERVER_INVALID_RESPONSE, utils::http::retryable::yes}},
        {"ServiceUnavailable", {aws::aws_error_type::SERVICE_UNAVAILABLE, utils::http::retryable::yes}},
        {"InvalidStorageTier", {aws::aws_error_type::OCI_INVALID_STORAGE_TIER, utils::http::retryable::no}},
    };

    std::string message = "Test Message";
    std::string requestId = "Request Id";
    for (const auto& [exception, expected] : expected_errors) {
        auto error = aws::aws_error::parse(build_xml_response(std::string(exception), message, requestId)).value();
        BOOST_REQUIRE_EQUAL(expected.type, error.get_error_type());
        BOOST_REQUIRE_EQUAL(message, error.get_error_message());
        BOOST_REQUIRE_EQUAL(expected.is_retryable, error.is_retryable());
    }
}

BOOST_AUTO_TEST_CASE(TestHelperFunctions) {
    BOOST_REQUIRE_EQUAL(utils::http::from_http_code(seastar::http::reply::status_type::service_unavailable), utils::http::retryable::yes);
    BOOST_REQUIRE_EQUAL(utils::http::from_http_code(seastar::http::reply::status_type::unauthorized), utils::http::retryable::no);

    BOOST_REQUIRE_EQUAL(utils::http::from_system_error(std::system_error(ECONNRESET, std::system_category())), utils::http::retryable::yes);
    BOOST_REQUIRE_EQUAL(utils::http::from_system_error(std::system_error(EADDRINUSE, std::system_category())), utils::http::retryable::no);
}

BOOST_AUTO_TEST_CASE(TestNestedException) {
    // Test nested exceptions where the innermost is a system_error
    try {
        try {
            try {
                throw std::system_error(std::error_code(ECONNABORTED, std::system_category()));
            } catch (...) {
                std::throw_with_nested(std::runtime_error("Higher level runtime_error"));
            }
        } catch (...) {
            std::throw_with_nested(std::logic_error("Higher level logic_error"));
        }
    } catch (...) {
        auto error = aws::aws_error::from_exception_ptr(std::current_exception());
        BOOST_REQUIRE_EQUAL(aws::aws_error_type::NETWORK_CONNECTION, error.get_error_type());
        BOOST_REQUIRE_EQUAL("Software caused connection abort", error.get_error_message());
        BOOST_REQUIRE_EQUAL(error.is_retryable(), utils::http::retryable::yes);
    }

    // Test nested exceptions where the innermost is NOT a system_error
    try {
        try {
            throw std::logic_error("Something bad happened");
        } catch (...) {
            std::throw_with_nested(std::runtime_error("Higher level runtime_error"));
        }
    } catch (...) {
        auto error = aws::aws_error::from_exception_ptr(std::current_exception());
        BOOST_REQUIRE_EQUAL(aws::aws_error_type::UNKNOWN, error.get_error_type());
        BOOST_REQUIRE_EQUAL("Higher level runtime_error", error.get_error_message());
        BOOST_REQUIRE_EQUAL(error.is_retryable(), utils::http::retryable::no);
    }

    // Test single exception which is NOT a nested exception
    try {
        throw std::runtime_error("Something bad happened");
    } catch (...) {
        auto error = aws::aws_error::from_exception_ptr(std::current_exception());
        BOOST_REQUIRE_EQUAL(aws::aws_error_type::UNKNOWN, error.get_error_type());
        BOOST_REQUIRE_EQUAL("Something bad happened", error.get_error_message());
        BOOST_REQUIRE_EQUAL(error.is_retryable(), utils::http::retryable::no);
    }

    // Test with non-std::exception
    try {
        throw "foo";
    } catch (...) {
        auto error = aws::aws_error::from_exception_ptr(std::current_exception());
        BOOST_REQUIRE_EQUAL(aws::aws_error_type::UNKNOWN, error.get_error_type());
        BOOST_REQUIRE_EQUAL("No error message was provided, exception content: char const*", error.get_error_message());
        BOOST_REQUIRE_EQUAL(error.is_retryable(), utils::http::retryable::no);
    }

    // Test system_error
    try {
        throw std::system_error(std::error_code(ECONNABORTED, std::system_category()));
    } catch (...) {
        auto error = aws::aws_error::from_exception_ptr(std::current_exception());
        BOOST_REQUIRE_EQUAL(aws::aws_error_type::NETWORK_CONNECTION, error.get_error_type());
        BOOST_REQUIRE_EQUAL("Software caused connection abort", error.get_error_message());
        BOOST_REQUIRE_EQUAL(error.is_retryable(), utils::http::retryable::yes);
    }

    // Test aws_exception
    try {
        throw aws::aws_exception(aws::aws_error::get_errors().at("HTTP_TOO_MANY_REQUESTS"));
    } catch (...) {
        auto error = aws::aws_error::from_exception_ptr(std::current_exception());
        BOOST_REQUIRE_EQUAL(aws::aws_error_type::HTTP_TOO_MANY_REQUESTS, error.get_error_type());
        BOOST_REQUIRE_EQUAL("", error.get_error_message());
        BOOST_REQUIRE_EQUAL(error.is_retryable(), utils::http::retryable::yes);
    }

    // Test httpd::unexpected_status_error
    try {
        throw seastar::httpd::unexpected_status_error(seastar::http::reply::status_type::network_connect_timeout);
    } catch (...) {
        auto error = aws::aws_error::from_exception_ptr(std::current_exception());
        BOOST_REQUIRE_EQUAL(aws::aws_error_type::HTTP_NETWORK_CONNECT_TIMEOUT, error.get_error_type());
        BOOST_REQUIRE_EQUAL(" HTTP code: 599 Network Connect Timeout", error.get_error_message());
        BOOST_REQUIRE_EQUAL(error.is_retryable(), utils::http::retryable::yes);
    }
}
