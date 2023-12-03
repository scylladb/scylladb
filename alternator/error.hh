/*
 * Copyright 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <seastar/http/httpd.hh>
#include "seastarx.hh"
#include "utils/rjson.hh"

namespace alternator {

// api_error contains a DynamoDB error message to be returned to the user.
// It can be returned by value (see executor::request_return_type) or thrown.
// The DynamoDB's error messages are described in detail in
// https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Programming.Errors.html
// An error message has an HTTP code (almost always 400), a type, e.g.,
// "ResourceNotFoundException", and a human readable message.
// Eventually alternator::api_handler will convert a returned or thrown
// api_error into a JSON object, and that is returned to the user.
class api_error final : public std::exception {
public:
    using status_type = http::reply::status_type;
    status_type _http_code;
    std::string _type;
    std::string _msg;
    // Additional data attached to the error, null value if not set. It's wrapped in copyable_value
    // class because copy constructor is required for exception classes otherwise it won't compile
    // (despite that its use may be optimized away).
    rjson::copyable_value _extra_fields; 
    api_error(std::string type, std::string msg, status_type http_code = status_type::bad_request,
    rjson::value extra_fields = rjson::null_value())
        : _http_code(std::move(http_code))
        , _type(std::move(type))
        , _msg(std::move(msg))
        , _extra_fields(std::move(extra_fields))
    { }

    // Factory functions for some common types of DynamoDB API errors
    static api_error validation(std::string msg) {
        return api_error("ValidationException", std::move(msg));
    }
    static api_error resource_not_found(std::string msg) {
        return api_error("ResourceNotFoundException", std::move(msg));
    }
    static api_error resource_in_use(std::string msg) {
        return api_error("ResourceInUseException", std::move(msg));
    }
    static api_error invalid_signature(std::string msg) {
        return api_error("InvalidSignatureException", std::move(msg));
    }
    static api_error missing_authentication_token(std::string msg) {
        return api_error("MissingAuthenticationTokenException", std::move(msg));
    }
    static api_error unrecognized_client(std::string msg) {
        return api_error("UnrecognizedClientException", std::move(msg));
    }
    static api_error unknown_operation(std::string msg) {
        return api_error("UnknownOperationException", std::move(msg));
    }
    static api_error access_denied(std::string msg) {
        return api_error("AccessDeniedException", std::move(msg));
    }
    static api_error conditional_check_failed(std::string msg, rjson::value&& item) {
        if (!item.IsNull()) {
            auto tmp = rjson::empty_object();
            rjson::add(tmp, "Item", std::move(item));
            item = std::move(tmp);
        }
        return api_error("ConditionalCheckFailedException", std::move(msg), status_type::bad_request, std::move(item));
    }
    static api_error expired_iterator(std::string msg) {
        return api_error("ExpiredIteratorException", std::move(msg));
    }
    static api_error trimmed_data_access_exception(std::string msg) {
        return api_error("TrimmedDataAccessException", std::move(msg));
    }
    static api_error request_limit_exceeded(std::string msg) {
        return api_error("RequestLimitExceeded", std::move(msg));
    }
    static api_error serialization(std::string msg) {
        return api_error("SerializationException", std::move(msg));
    }
    static api_error table_not_found(std::string msg) {
        return api_error("TableNotFoundException", std::move(msg));
    }
    static api_error internal(std::string msg) {
        return api_error("InternalServerError", std::move(msg), http::reply::status_type::internal_server_error);
    }

    // Provide the "std::exception" interface, to make it easier to print this
    // exception in log messages. Note that this function is *not* used to
    // format the error to send it back to the client - server.cc has
    // generate_error_reply() to format an api_error as the DynamoDB protocol
    // requires.
    virtual const char* what() const noexcept override;
    mutable std::string _what_string;
};

}

