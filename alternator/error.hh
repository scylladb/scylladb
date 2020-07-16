/*
 * Copyright 2019 ScyllaDB
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
namespace detail {
    enum class exception_type {
        ValidationException,
        ResourceNotFoundException,
        AccessDeniedException,
        InvalidSignatureException,
    };

    std::ostream& operator<<(std::ostream&, exception_type);
}

// DynamoDB's error messages are described in detail in
// https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Programming.Errors.html
// Ah An error message has a "type", e.g., "ResourceNotFoundException", a coarser
// HTTP code (almost always, 400), and a human readable message. Eventually these
// will be wrapped into a JSON object returned to the client.
class api_error : public std::exception {
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
    api_error() = default;
    virtual const char* what() const noexcept override { return _msg.c_str(); }
protected:
    api_error(detail::exception_type, std::string msg, status_type http_code);
};

namespace detail {
template<detail::exception_type Type>
class t_api_error : public api_error {
public:
    t_api_error(std::string msg, status_type http_code = status_type::bad_request)
        : api_error(Type, std::move(msg), http_code)
    {}
};
}

using validation_exception = detail::t_api_error<detail::exception_type::ValidationException>;
using resource_not_found_exception = detail::t_api_error<detail::exception_type::ResourceNotFoundException>;
using access_denied_exception = detail::t_api_error<detail::exception_type::AccessDeniedException>;
using invalid_signature_exception = detail::t_api_error<detail::exception_type::InvalidSignatureException>;

}

