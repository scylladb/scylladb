/*
 * Copyright 2019-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include <seastar/http/httpd.hh>
#include "seastarx.hh"

namespace alternator {

// api_error contains a DynamoDB error message to be returned to the user.
// It can be returned by value (see executor::request_return_type) or thrown.
// The DynamoDB's error messages are described in detail in
// https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Programming.Errors.html
// An error message has an HTTP code (almost always 400), a type, e.g.,
// "ResourceNotFoundException", and a human readable message.
// Eventually alternator::api_handler will convert a returned or thrown
// api_error into a JSON object, and that is returned to the user.
class api_error final {
public:
    using status_type = httpd::reply::status_type;
    status_type _http_code;
    std::string _type;
    std::string _msg;
    api_error(std::string type, std::string msg, status_type http_code = status_type::bad_request)
        : _http_code(std::move(http_code))
        , _type(std::move(type))
        , _msg(std::move(msg))
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
    static api_error conditional_check_failed(std::string msg) {
        return api_error("ConditionalCheckFailedException", std::move(msg));
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
    static api_error internal(std::string msg) {
        return api_error("InternalServerError", std::move(msg), reply::status_type::internal_server_error);
    }
};

}

