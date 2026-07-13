/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#if __has_include(<rapidxml.h>)
#include <rapidxml.h>
#else
#include <rapidxml/rapidxml.hpp>
#endif

#include "aws_error.hh"
#include "utils/exceptions.hh"
#include "utils/log.hh"
#include <seastar/util/log.hh>
#include <seastar/http/exception.hh>
#include <memory>

namespace s3 {
extern logging::logger s3l;
}

namespace aws {

using namespace utils::http;

aws_error::aws_error(aws_error_type error_type, retryable is_retryable) : _type(error_type), _is_retryable(is_retryable) {
}

aws_error::aws_error(aws_error_type error_type, std::string&& error_message, retryable is_retryable)
    : _type(error_type), _message(std::move(error_message)), _is_retryable(is_retryable) {
}

std::optional<aws_error> aws_error::parse(seastar::sstring&& body) {
    if (body.empty()) {
        return {};
    }

    auto doc = std::make_unique<rapidxml::xml_document<>>();
    try {
        doc->parse<0>(body.data());
    } catch (const rapidxml::parse_error&) {
        // Most likely not an XML which is possible, just return
        return {};
    }

    const auto* error_node = doc->first_node("Error");
    if (!error_node && doc->first_node()) {
        error_node = doc->first_node()->first_node("Errors");
        if (error_node) {
            error_node = error_node->first_node("Error");
        }
    }

    if (!error_node) {
        return {};
    }

    const auto* code_node = error_node->first_node("Code");
    const auto* message_node = error_node->first_node("Message");
    aws_error ret_val;

    if (code_node && message_node) {
        std::string code = code_node->value();

        if (auto pound_loc = code.find('#'); pound_loc != std::string::npos) {
            code = code.substr(pound_loc + 1);
        } else if (auto colon_loc = code.find(':'); colon_loc != std::string::npos) {
            code = code.substr(0, colon_loc);
        }
        const auto& all_errors = aws_error::get_errors();
        if (auto found = all_errors.find(code); found != all_errors.end()) {
            ret_val = found->second;
        } else {
            ::s3::s3l.warn("Unknown S3 error code: {}, message: {}", code, message_node->value());
            ret_val._type = aws_error_type::UNKNOWN;
        }
        ret_val._message = message_node->value();
    } else {
        std::string code_value = "missing Code node";
        std::string message_value = "missing Message node";

        if (code_node) {
            code_value = code_node->value();
        }
        if (message_node) {
            message_value = message_node->value();
        }
        ret_val._message = seastar::format("Malformed S3 error response. Code: {}, Message: {}", code_value, message_value);
        ret_val._type = aws_error_type::UNKNOWN;
        ::s3::s3l.warn("{}", ret_val._message);
    }
    return ret_val;
}

aws_error aws_error::from_http_code(seastar::http::reply::status_type http_code) {
    const auto& all_errors = get_errors();
    aws_error ret_val;
    switch (http_code) {
    case seastar::http::reply::status_type::unauthorized:
        ret_val = all_errors.at("HTTP_UNAUTHORIZED");
        break;
    case seastar::http::reply::status_type::forbidden:
        ret_val = all_errors.at("HTTP_FORBIDDEN");
        break;
    case seastar::http::reply::status_type::not_found:
        ret_val = all_errors.at("HTTP_NOT_FOUND");
        break;
    case seastar::http::reply::status_type::too_many_requests:
        ret_val = all_errors.at("HTTP_TOO_MANY_REQUESTS");
        break;
    case seastar::http::reply::status_type::internal_server_error:
        ret_val = all_errors.at("HTTP_INTERNAL_SERVER_ERROR");
        break;
    case seastar::http::reply::status_type::bandwidth_limit_exceeded:
        ret_val = all_errors.at("HTTP_BANDWIDTH_LIMIT_EXCEEDED");
        break;
    case seastar::http::reply::status_type::service_unavailable:
        ret_val = all_errors.at("HTTP_SERVICE_UNAVAILABLE");
        break;
    case seastar::http::reply::status_type::request_timeout:
        ret_val = all_errors.at("HTTP_REQUEST_TIMEOUT");
        break;
    case seastar::http::reply::status_type::page_expired:
        ret_val = all_errors.at("HTTP_PAGE_EXPIRED");
        break;
    case seastar::http::reply::status_type::login_timeout:
        ret_val = all_errors.at("HTTP_LOGIN_TIMEOUT");
        break;
    case seastar::http::reply::status_type::gateway_timeout:
        ret_val = all_errors.at("HTTP_GATEWAY_TIMEOUT");
        break;
    case seastar::http::reply::status_type::network_connect_timeout:
        ret_val = all_errors.at("HTTP_NETWORK_CONNECT_TIMEOUT");
        break;
    case seastar::http::reply::status_type::network_read_timeout:
        ret_val = all_errors.at("HTTP_NETWORK_READ_TIMEOUT");
        break;
    default:
        ret_val = {aws_error_type::UNKNOWN,
                   "Unknown server error has been encountered.",
                   retryable{seastar::http::reply::classify_status(http_code) == seastar::http::reply::status_class::server_error}};
    }
    ret_val._message = seastar::format("{} HTTP code: {}", ret_val._message, http_code);
    return ret_val;
}

aws_error aws_error::from_system_error(const std::system_error& system_error) {
    auto is_retryable = utils::http::from_system_error(system_error);
    if (is_retryable == retryable::yes) {
        return {aws_error_type::NETWORK_CONNECTION, system_error.code().message(), is_retryable};
    }

    return {aws_error_type::UNKNOWN,
            format("Non-retryable system error occurred. Message: {}, code: {}", system_error.code().message(), system_error.code().value()),
            is_retryable};
}

aws_error aws_error::from_exception_ptr(std::exception_ptr exception) {
    return dispatch_exception<aws_error>(
        std::move(exception),
        [](std::exception_ptr eptr, std::string&& original_message) {
            if (!original_message.empty()) {
                return aws_error{aws_error_type::UNKNOWN, std::move(original_message), retryable::no};
            }
            if (!eptr) {
                return aws_error{aws_error_type::UNKNOWN, "No exception was provided to `aws_error::from_exception_ptr` function call", retryable::no};
            }
            return aws_error{
                aws_error_type::UNKNOWN, seastar::format("No error message was provided, exception content: {}", eptr), retryable::no};
        },
        [](const aws_exception& ex) { return ex.error(); },
        [](const seastar::httpd::unexpected_status_error& ex) { return from_http_code(ex.status()); },
        [](const std::system_error& ex) { return from_system_error(ex); });
}
} // namespace aws
