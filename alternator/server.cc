/*
 * Copyright 2019 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include "alternator/server.hh"
#include "log.hh"
#include <seastar/http/function_handlers.hh>
#include <seastar/json/json_elements.hh>
#include <seastarx.hh>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/classification.hpp>

static logging::logger slogger("alternator-server");

using namespace httpd;

namespace alternator {

static constexpr auto TARGET = "X-Amz-Target";

inline std::vector<sstring> split(const sstring& text, const char* separator) {
    if (text == "") {
        return std::vector<sstring>();
    }
    std::vector<sstring> tokens;
    return boost::split(tokens, text, boost::is_any_of(separator));
}


void server::set_routes(routes& r) {
    function_handler* handler = new function_handler([this](std::unique_ptr<request> req) -> future<json::json_return_type> {
        slogger.warn("REQ {} {}", req->content, req->content_length);
        sstring target = req->get_header(TARGET);
        std::vector<sstring> split_target = split(target, ".");
        //NOTICE(sarna): Target consists of Dynamo API version folllowed by a dot '.' and operation type (e.g. CreateTable)
        sstring op = split_target.empty() ? sstring() : split_target.back();

        slogger.warn("Got Request <{}>", op);
        if (op == "CreateTable") {
            return _executor.local().create_table(req->content);
        } else if (op == "PutItem") {
            return _executor.local().put_item(req->content);
        } else if (op == "GetItem") {
            return _executor.local().get_item(req->content);
        }
        throw std::runtime_error(format("Operation not supported: {}", req->content));
    });

    r.add(operation_type::POST, url("/"), handler);
}

future<> server::init(uint16_t port) {
    return _executor.invoke_on_all([] (executor& e) {
        return e.start();
    }).then([this] {
        return _control.start();
    }).then([this] {
        _control.set_routes(std::bind(&server::set_routes, this, std::placeholders::_1));
    }).then([this, port] {
        _control.listen(port);
    }).then([port] {
        slogger.info("Alternator HTTP server listening on port {}", port);
    }).handle_exception([port] (std::exception_ptr e) {
        slogger.warn("Failed to set up Alternator HTTP server on port {}: {}", port, e);
    });
}

}

