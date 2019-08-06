/*
 * Copyright 2019 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include <seastar/http/httpd.hh>
#include "seastarx.hh"

namespace alternator {

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
};

}

