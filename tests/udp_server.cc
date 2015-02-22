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
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#include "core/distributed.hh"
#include "core/app-template.hh"
#include "core/future-util.hh"

using namespace net;
using namespace std::chrono_literals;

class udp_server {
private:
    udp_channel _chan;
    timer<> _stats_timer;
    uint64_t _n_sent {};
public:
    void start(uint16_t port) {
        ipv4_addr listen_addr{port};
        _chan = engine().net().make_udp_channel(listen_addr);

        _stats_timer.set_callback([this] {
            std::cout << "Out: " << _n_sent << " pps" << std::endl;
            _n_sent = 0;
        });
        _stats_timer.arm_periodic(1s);

        keep_doing([this] {
            return _chan.receive().then([this] (udp_datagram dgram) {
                return _chan.send(dgram.get_src(), std::move(dgram.get_data())).then([this] {
                    _n_sent++;
                });
            });
        });
    }
};

namespace bpo = boost::program_options;

int main(int ac, char ** av) {
    app_template app;
    app.add_options()
        ("port", bpo::value<uint16_t>()->default_value(10000), "UDP server port") ;
    return app.run(ac, av, [&] {
        auto&& config = app.configuration();
        uint16_t port = config["port"].as<uint16_t>();
        auto server = new distributed<udp_server>;
        server->start().then([server = std::move(server), port] () mutable {
            server->invoke_on_all(&udp_server::start, port);
        }).then([port] {
            std::cout << "Seastar UDP server listening on port " << port << " ...\n";
        });
    });
}
