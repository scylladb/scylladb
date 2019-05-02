/*
 * Copyright 2019 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include "log.hh"
#include "alternator/executor.hh"
#include <seastar/core/future.hh>
#include <seastar/http/httpd.hh>

namespace alternator {

class server {
    seastar::httpd::http_server_control _control;
    seastar::sharded<executor>& _executor;
public:
    server(seastar::sharded<executor>& executor) : _executor(executor) {}

    seastar::future<> init(uint16_t port);
private:
    void set_routes(seastar::httpd::routes& r);
};

// DynamoDB's error messages are described in detail in
// https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Programming.Errors.html
// An error message has a "type", e.g., "ResourceNotFoundException", a coarser
// HTTP code (in the above case, 400), and a human readable message. Eventually these
// will be wrapped into a JSON object returned to the client.
class api_error : public std::exception {
public:
    reply::status_type _http_code;
    sstring _type;
    sstring _msg;
    api_error(reply::status_type http_code, sstring type, sstring msg)
        : _http_code(std::move(http_code))
        , _type(std::move(type))
        , _msg(std::move(msg))
    { }
    api_error() { }
    virtual const char* what() const noexcept override { return _msg.begin(); }
};

}

