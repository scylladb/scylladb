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

#include "core/app-template.hh"
#include "core/future-util.hh"

using namespace net;
using namespace std::chrono_literals;

class client {
private:
    udp_channel _chan;
    uint64_t n_sent {};
    uint64_t n_received {};
    uint64_t n_failed {};
    timer<> _stats_timer;
public:
    void start(ipv4_addr server_addr) {
        std::cout << "Sending to " << server_addr << std::endl;

        _chan = engine().net().make_udp_channel();

        _stats_timer.set_callback([this] {
            std::cout << "Out: " << n_sent << " pps, \t";
            std::cout << "Err: " << n_failed << " pps, \t";
            std::cout << "In: " << n_received << " pps" << std::endl;
            n_sent = 0;
            n_received = 0;
            n_failed = 0;
        });
        _stats_timer.arm_periodic(1s);

        keep_doing([this, server_addr] {
            return _chan.send(server_addr, "hello!\n")
                .then_wrapped([this] (auto&& f) {
                    try {
                        f.get();
                        n_sent++;
                    } catch (...) {
                        n_failed++;
                    }
                });
        });

        keep_doing([this] {
            return _chan.receive().then([this] (auto) {
                n_received++;
            });
        });
    }
};

namespace bpo = boost::program_options;

int main(int ac, char ** av) {
    client _client;
    app_template app;
    app.add_options()
        ("server", bpo::value<std::string>(), "Server address")
        ;
    return app.run(ac, av, [&_client, &app] {
        auto&& config = app.configuration();
        _client.start(config["server"].as<std::string>());
    });
}
