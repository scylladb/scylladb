/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#if __has_include(<rapidxml.h>)
#include <rapidxml.h>
#else
#include <rapidxml/rapidxml.hpp>
#endif

#include "aws_error.hh"
#include <seastar/util/log.hh>
#include <seastar/http/exception.hh>
#include <memory>

namespace aws {

aws_error::aws_error(aws_error_type error_type, utils::http::retryable is_retryable) : _type(error_type), _is_retryable(is_retryable) {
}

aws_error::aws_error(aws_error_type error_type, std::string&& error_message, utils::http::retryable is_retryable)
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
            ret_val._type = aws_error_type::UNKNOWN;
        }
        ret_val._message = message_node->value();
    } else {
        ret_val._type = aws_error_type::UNKNOWN;
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
                   utils::http::retryable{seastar::http::reply::classify_status(http_code) == seastar::http::reply::status_class::server_error}};
    }
    ret_val._message = seastar::format("{} HTTP code: {}", ret_val._message, http_code);
    return ret_val;
}

aws_error aws_error::from_system_error(const std::system_error& system_error) {
    auto is_retryable = utils::http::from_system_error(system_error);
    if (is_retryable == utils::http::retryable::yes) {
        return {aws_error_type::NETWORK_CONNECTION, system_error.code().message(), is_retryable};
    }

    return {aws_error_type::UNKNOWN,
            format("Non-retryable system error occurred. Message: {}, code: {}", system_error.code().message(), system_error.code().value()),
            is_retryable};
}

aws_error aws_error::from_exception_ptr(std::exception_ptr exception) {
    return utils::http::dispatch_exception<aws_error>(
        std::move(exception),
        [](std::exception_ptr eptr, std::string&& original_message) {
            if (!original_message.empty()) {
                return aws_error{aws_error_type::UNKNOWN, std::move(original_message), utils::http::retryable::no};
            }
            if (!eptr) {
                return aws_error{
                    aws_error_type::UNKNOWN, "No exception was provided to `aws_error::from_exception_ptr` function call", utils::http::retryable::no};
            }
            return aws_error{aws_error_type::UNKNOWN,
                             seastar::format("No error message was provided, exception content: {}", std::current_exception()),
                             utils::http::retryable::no};
        },
        utils::http::make_handler<aws_exception>([](const auto& ex) { return ex.error(); }),
        utils::http::make_handler<seastar::httpd::unexpected_status_error>([](const auto& ex) { return from_http_code(ex.status()); }),
        utils::http::make_handler<std::system_error>([](const auto& ex) { return from_system_error(ex); }));
}

const aws_errors& aws_error::get_errors() {
    static const std::unordered_map<std::string_view, const aws_error> aws_error_map{
        {"IncompleteSignature", aws_error(aws_error_type::INCOMPLETE_SIGNATURE, utils::http::retryable::no)},
        {"IncompleteSignatureException", aws_error(aws_error_type::INCOMPLETE_SIGNATURE, utils::http::retryable::no)},
        {"InvalidSignatureException", aws_error(aws_error_type::INVALID_SIGNATURE, utils::http::retryable::no)},
        {"InvalidSignature", aws_error(aws_error_type::INVALID_SIGNATURE, utils::http::retryable::no)},
        {"InternalFailureException", aws_error(aws_error_type::INTERNAL_FAILURE, utils::http::retryable::yes)},
        {"InternalFailure", aws_error(aws_error_type::INTERNAL_FAILURE, utils::http::retryable::yes)},
        {"InternalServerError", aws_error(aws_error_type::INTERNAL_FAILURE, utils::http::retryable::yes)},
        {"InternalError", aws_error(aws_error_type::INTERNAL_FAILURE, utils::http::retryable::yes)},
        {"InvalidActionException", aws_error(aws_error_type::INVALID_ACTION, utils::http::retryable::no)},
        {"InvalidAction", aws_error(aws_error_type::INVALID_ACTION, utils::http::retryable::no)},
        {"InvalidClientTokenIdException", aws_error(aws_error_type::INVALID_CLIENT_TOKEN_ID, utils::http::retryable::no)},
        {"InvalidClientTokenId", aws_error(aws_error_type::INVALID_CLIENT_TOKEN_ID, utils::http::retryable::no)},
        {"InvalidParameterCombinationException", aws_error(aws_error_type::INVALID_PARAMETER_COMBINATION, utils::http::retryable::no)},
        {"InvalidParameterCombination", aws_error(aws_error_type::INVALID_PARAMETER_COMBINATION, utils::http::retryable::no)},
        {"InvalidParameterValueException", aws_error(aws_error_type::INVALID_PARAMETER_VALUE, utils::http::retryable::no)},
        {"InvalidParameterValue", aws_error(aws_error_type::INVALID_PARAMETER_VALUE, utils::http::retryable::no)},
        {"InvalidQueryParameterException", aws_error(aws_error_type::INVALID_QUERY_PARAMETER, utils::http::retryable::no)},
        {"InvalidQueryParameter", aws_error(aws_error_type::INVALID_QUERY_PARAMETER, utils::http::retryable::no)},
        {"MalformedQueryStringException", aws_error(aws_error_type::MALFORMED_QUERY_STRING, utils::http::retryable::no)},
        {"MalformedQueryString", aws_error(aws_error_type::MALFORMED_QUERY_STRING, utils::http::retryable::no)},
        {"MissingActionException", aws_error(aws_error_type::MISSING_ACTION, utils::http::retryable::no)},
        {"MissingAction", aws_error(aws_error_type::MISSING_ACTION, utils::http::retryable::no)},
        {"MissingAuthenticationTokenException", aws_error(aws_error_type::MISSING_AUTHENTICATION_TOKEN, utils::http::retryable::no)},
        {"MissingAuthenticationToken", aws_error(aws_error_type::MISSING_AUTHENTICATION_TOKEN, utils::http::retryable::no)},
        {"MissingParameterException", aws_error(aws_error_type::MISSING_PARAMETER, utils::http::retryable::no)},
        {"MissingParameter", aws_error(aws_error_type::MISSING_PARAMETER, utils::http::retryable::no)},
        {"OptInRequired", aws_error(aws_error_type::OPT_IN_REQUIRED, utils::http::retryable::no)},
        {"RequestExpiredException", aws_error(aws_error_type::REQUEST_EXPIRED, utils::http::retryable::yes)},
        {"RequestExpired", aws_error(aws_error_type::REQUEST_EXPIRED, utils::http::retryable::yes)},
        {"ServiceUnavailableException", aws_error(aws_error_type::SERVICE_UNAVAILABLE, utils::http::retryable::yes)},
        {"ServiceUnavailableError", aws_error(aws_error_type::SERVICE_UNAVAILABLE, utils::http::retryable::yes)},
        {"ServiceUnavailable", aws_error(aws_error_type::SERVICE_UNAVAILABLE, utils::http::retryable::yes)},
        {"RequestThrottledException", aws_error(aws_error_type::THROTTLING, utils::http::retryable::yes)},
        {"RequestThrottled", aws_error(aws_error_type::THROTTLING, utils::http::retryable::yes)},
        {"ThrottlingException", aws_error(aws_error_type::THROTTLING, utils::http::retryable::yes)},
        {"ThrottledException", aws_error(aws_error_type::THROTTLING, utils::http::retryable::yes)},
        {"Throttling", aws_error(aws_error_type::THROTTLING, utils::http::retryable::yes)},
        {"ValidationErrorException", aws_error(aws_error_type::VALIDATION, utils::http::retryable::no)},
        {"ValidationException", aws_error(aws_error_type::VALIDATION, utils::http::retryable::no)},
        {"ValidationError", aws_error(aws_error_type::VALIDATION, utils::http::retryable::no)},
        {"AccessDeniedException", aws_error(aws_error_type::ACCESS_DENIED, utils::http::retryable::no)},
        {"AccessDenied", aws_error(aws_error_type::ACCESS_DENIED, utils::http::retryable::no)},
        {"ResourceNotFoundException", aws_error(aws_error_type::RESOURCE_NOT_FOUND, utils::http::retryable::no)},
        {"ResourceNotFound", aws_error(aws_error_type::RESOURCE_NOT_FOUND, utils::http::retryable::no)},
        {"UnrecognizedClientException", aws_error(aws_error_type::UNRECOGNIZED_CLIENT, utils::http::retryable::no)},
        {"UnrecognizedClient", aws_error(aws_error_type::UNRECOGNIZED_CLIENT, utils::http::retryable::no)},
        {"SlowDownException", aws_error(aws_error_type::SLOW_DOWN, utils::http::retryable::yes)},
        {"SlowDown", aws_error(aws_error_type::SLOW_DOWN, utils::http::retryable::yes)},
        {"SignatureDoesNotMatchException", aws_error(aws_error_type::SIGNATURE_DOES_NOT_MATCH, utils::http::retryable::no)},
        {"SignatureDoesNotMatch", aws_error(aws_error_type::SIGNATURE_DOES_NOT_MATCH, utils::http::retryable::no)},
        {"InvalidAccessKeyIdException", aws_error(aws_error_type::INVALID_ACCESS_KEY_ID, utils::http::retryable::no)},
        {"InvalidAccessKeyId", aws_error(aws_error_type::INVALID_ACCESS_KEY_ID, utils::http::retryable::no)},
        {"RequestTimeTooSkewedException", aws_error(aws_error_type::REQUEST_TIME_TOO_SKEWED, utils::http::retryable::yes)},
        {"RequestTimeTooSkewed", aws_error(aws_error_type::REQUEST_TIME_TOO_SKEWED, utils::http::retryable::yes)},
        {"RequestTimeoutException", aws_error(aws_error_type::REQUEST_TIMEOUT, utils::http::retryable::yes)},
        {"RequestTimeout", aws_error(aws_error_type::REQUEST_TIMEOUT, utils::http::retryable::yes)},
        {"HTTP_UNAUTHORIZED", aws_error(aws_error_type::HTTP_UNAUTHORIZED, utils::http::retryable::no)},
        {"HTTP_FORBIDDEN", aws_error(aws_error_type::HTTP_FORBIDDEN, utils::http::retryable::no)},
        {"HTTP_NOT_FOUND", aws_error(aws_error_type::HTTP_NOT_FOUND, utils::http::retryable::no)},
        {"HTTP_TOO_MANY_REQUESTS", aws_error(aws_error_type::HTTP_TOO_MANY_REQUESTS, utils::http::retryable::yes)},
        {"HTTP_INTERNAL_SERVER_ERROR", aws_error(aws_error_type::HTTP_INTERNAL_SERVER_ERROR, utils::http::retryable::yes)},
        {"HTTP_BANDWIDTH_LIMIT_EXCEEDED", aws_error(aws_error_type::HTTP_BANDWIDTH_LIMIT_EXCEEDED, utils::http::retryable::yes)},
        {"HTTP_SERVICE_UNAVAILABLE", aws_error(aws_error_type::HTTP_SERVICE_UNAVAILABLE, utils::http::retryable::yes)},
        {"HTTP_REQUEST_TIMEOUT", aws_error(aws_error_type::HTTP_REQUEST_TIMEOUT, utils::http::retryable::yes)},
        {"HTTP_PAGE_EXPIRED", aws_error(aws_error_type::HTTP_PAGE_EXPIRED, utils::http::retryable::yes)},
        {"HTTP_LOGIN_TIMEOUT", aws_error(aws_error_type::HTTP_LOGIN_TIMEOUT, utils::http::retryable::yes)},
        {"HTTP_GATEWAY_TIMEOUT", aws_error(aws_error_type::HTTP_GATEWAY_TIMEOUT, utils::http::retryable::yes)},
        {"HTTP_NETWORK_CONNECT_TIMEOUT", aws_error(aws_error_type::HTTP_NETWORK_CONNECT_TIMEOUT, utils::http::retryable::yes)},
        {"HTTP_NETWORK_READ_TIMEOUT", aws_error(aws_error_type::HTTP_NETWORK_READ_TIMEOUT, utils::http::retryable::yes)},
        {"NoSuchUpload", aws_error(aws_error_type::NO_SUCH_UPLOAD, utils::http::retryable::no)},
        {"BucketAlreadyOwnedByYou", aws_error(aws_error_type::BUCKET_ALREADY_OWNED_BY_YOU, utils::http::retryable::no)},
        {"ObjectAlreadyInActiveTierError", aws_error(aws_error_type::OBJECT_ALREADY_IN_ACTIVE_TIER, utils::http::retryable::no)},
        {"NoSuchBucket", aws_error(aws_error_type::NO_SUCH_BUCKET, utils::http::retryable::no)},
        {"NoSuchKey", aws_error(aws_error_type::NO_SUCH_KEY, utils::http::retryable::no)},
        {"ObjectNotInActiveTierError", aws_error(aws_error_type::OBJECT_NOT_IN_ACTIVE_TIER, utils::http::retryable::no)},
        {"BucketAlreadyExists", aws_error(aws_error_type::BUCKET_ALREADY_EXISTS, utils::http::retryable::no)},
        {"InvalidObjectState", aws_error(aws_error_type::INVALID_OBJECT_STATE, utils::http::retryable::no)},
        {"ExpiredTokenException", aws_error(aws_error_type::EXPIRED_TOKEN, utils::http::retryable::no)},
        {"InvalidAuthorizationMessageException", aws_error(aws_error_type::INVALID_AUTHORIZATION_MESSAGE, utils::http::retryable::no)},
        {"InvalidIdentityToken", aws_error(aws_error_type::INVALID_IDENTITY_TOKEN, utils::http::retryable::no)},
        {"IDPCommunicationError", aws_error(aws_error_type::I_D_P_COMMUNICATION_ERROR, utils::http::retryable::no)},
        {"IDPRejectedClaim", aws_error(aws_error_type::I_D_P_REJECTED_CLAIM, utils::http::retryable::no)},
        {"MalformedPolicyDocument", aws_error(aws_error_type::MALFORMED_POLICY_DOCUMENT, utils::http::retryable::no)},
        {"PackedPolicyTooLarge", aws_error(aws_error_type::PACKED_POLICY_TOO_LARGE, utils::http::retryable::no)},
        {"RegionDisabledException", aws_error(aws_error_type::REGION_DISABLED, utils::http::retryable::no)}};
    return aws_error_map;
}

} // namespace aws
