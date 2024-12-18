/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#define BOOST_TEST_MODULE object_storage

#include "utils/s3/aws_error.hh"
#include <boost/test/unit_test.hpp>
#include <seastar/core/sstring.hh>

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
    BOOST_REQUIRE_EQUAL(error.is_retryable(), aws::retryable::no);

    error = aws::aws_error::parse(build_xml_response("InternalFailure", message, requestId, message_style::plural)).value();
    BOOST_REQUIRE_EQUAL(aws::aws_error_type::INTERNAL_FAILURE, error.get_error_type());
    BOOST_REQUIRE_EQUAL(message, error.get_error_message());
    BOOST_REQUIRE_EQUAL(error.is_retryable(), aws::retryable::yes);

    error = aws::aws_error::parse(build_xml_response("IDontExist", message, requestId, message_style::plural)).value();
    BOOST_REQUIRE_EQUAL(aws::aws_error_type::UNKNOWN, error.get_error_type());
    BOOST_REQUIRE_EQUAL(message, error.get_error_message());
    BOOST_REQUIRE_EQUAL(error.is_retryable(), aws::retryable::no);

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
    BOOST_REQUIRE_EQUAL(error.is_retryable(), aws::retryable::yes);
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
    BOOST_REQUIRE_EQUAL(error.is_retryable(), aws::retryable::no);
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
    BOOST_REQUIRE_EQUAL(error.is_retryable(), aws::retryable::no);
}
