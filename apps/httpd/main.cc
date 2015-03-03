/*
 * This file is open source software, licensed to you under the terms
 * of the Apache License, Version 2.0 (the "License").  See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership.  You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Copyright 2015 Cloudius Systems
 */

#include "http/httpd.hh"
#include "http/handlers.hh"

namespace bpo = boost::program_options;

using namespace httpd;

class handl : public httpd::handler_base {
public:
    virtual void handle(const sstring& path, parameters* params,
            httpd::const_req& req, httpd::reply& rep) {
        rep._content = "hello";
        rep.done("html");
    }
};

int main(int ac, char** av) {
    app_template app;
    app.add_options()("port", bpo::value<uint16_t>()->default_value(10000),
            "HTTP Server port");
    return app.run(ac, av,
            [&] {
                auto&& config = app.configuration();
                uint16_t port = config["port"].as<uint16_t>();
                auto server = new distributed<http_server>;
                server->start().then([server = std::move(server), port] () mutable {
                            server->invoke_on_all([](http_server& server) {
                                handl* h1 = new handl();
                                server._routes.add(operation_type::GET, url("/"), h1);
                            });
                            server->invoke_on_all(&http_server::listen, ipv4_addr {port});
                        }).then([port] {
                            std::cout << "Seastar HTTP server listening on port " << port << " ...\n";
                        });
            });
}
