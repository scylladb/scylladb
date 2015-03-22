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
#include "http/function_handlers.hh"

namespace bpo = boost::program_options;

using namespace httpd;

class handl : public httpd::handler_base {
public:
    virtual future<std::unique_ptr<reply> > handle(const sstring& path,
            std::unique_ptr<request> req, std::unique_ptr<reply> rep) {
        rep->_content = "hello";
        rep->done("html");
        return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
    }
};

void set_routes(routes& r) {
    function_handler* h1 = new function_handler([](const_req req) {
        return "hello";
    });
    r.add(operation_type::GET, url("/"), h1);
}

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
                                set_routes(server._routes);
                            });
                            server->invoke_on_all(&http_server::listen, ipv4_addr {port});
                        }).then([port] {
                            std::cout << "Seastar HTTP server listening on port " << port << " ...\n";
                        });
            });
}
