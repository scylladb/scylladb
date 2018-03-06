
/*
 * Copyright (C) 2015 ScyllaDB
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
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "core/reactor.hh"
#include "core/app-template.hh"
#include "db/system_distributed_keyspace.hh"
#include "message/messaging_service.hh"
#include "gms/failure_detector.hh"
#include "gms/gossiper.hh"
#include "gms/application_state.hh"
#include "service/storage_service.hh"
#include "utils/fb_utilities.hh"
#include "locator/snitch_base.hh"
#include "log.hh"
#include <seastar/core/thread.hh>
#include <chrono>

namespace bpo = boost::program_options;

// === How to run the test
// node1:
// ./gossip --seed 127.0.0.1  --listen-address 127.0.0.1 -c 2
//
// node2:
// ./gossip --seed 127.0.0.1  --listen-address 127.0.0.2 -c 2
//
// node3:
// ./gossip --seed 127.0.0.1  --listen-address 127.0.0.3 -c 2
//
// === What to expect
//
// Each node should see the LOAD status of other nodes. The load status is increased by 0.0001 every second.
// And the version number in the HeartBeatState increases every second.
// Example of the output:
//
// DEBUG [shard 0] gossip - ep=127.0.0.1, eps=HeartBeatState = { generation = 1446454365, version = 68 }, AppStateMap = { LOAD : Value(0.5019,67) }
// DEBUG [shard 0] gossip - ep=127.0.0.2, eps=HeartBeatState = { generation = 1446454380, version = 27 }, AppStateMap = { LOAD : Value(0.5005,26) }

int main(int ac, char ** av) {
    distributed<database> db;
    sharded<auth::service> auth_service;
    app_template app;
    app.add_options()
        ("seed", bpo::value<std::vector<std::string>>(), "IP address of seed node")
        ("listen-address", bpo::value<std::string>()->default_value("0.0.0.0"), "IP address to listen");
    return app.run_deprecated(ac, av, [&auth_service, &db, &app] {
        auto config = app.configuration();
        logging::logger_registry().set_logger_level("gossip", logging::log_level::trace);
        const gms::inet_address listen = gms::inet_address(config["listen-address"].as<std::string>());
        utils::fb_utilities::set_broadcast_address(listen);
        utils::fb_utilities::set_broadcast_rpc_address(listen);
        auto vv = std::make_shared<gms::versioned_value::factory>();
        locator::i_endpoint_snitch::create_snitch("SimpleSnitch").then([&auth_service, &db] {
            sharded<db::system_distributed_keyspace> sys_dist_ks;
            return service::init_storage_service(db, auth_service, sys_dist_ks);
        }).then([vv, listen, config] {
            return netw::get_messaging_service().start(listen);
        }).then([config] {
            auto& server = netw::get_local_messaging_service();
            auto port = server.port();
            auto listen = server.listen_address();
            print("Messaging server listening on ip %s port %d ...\n", listen, port);
            return gms::get_failure_detector().start();
        }).then([vv, config] {
            return gms::get_gossiper().start();
        }).then([vv, config] {
            std::set<gms::inet_address> seeds;
            for (auto s : config["seed"].as<std::vector<std::string>>()) {
                seeds.emplace(std::move(s));
            }

            std::cout << "Start gossiper service ...\n";
            auto& gossiper = gms::get_local_gossiper();
            gossiper.set_seeds(std::move(seeds));
            gossiper.set_cluster_name("Test Cluster");

            std::map<gms::application_state, gms::versioned_value> app_states = {
                { gms::application_state::LOAD, vv->load(0.5) },
            };

            using namespace std::chrono;
            auto now = high_resolution_clock::now().time_since_epoch();
            int generation_number = duration_cast<seconds>(now).count();
            return gossiper.start_gossiping(generation_number, app_states);
        }).then([vv] {
            return seastar::async([vv] {
                static double load = 0.5;
                for (;;) {
                    auto value = vv->load(load);
                    load += 0.0001;
                    gms::get_local_gossiper().add_local_application_state(gms::application_state::LOAD, value).get();
                    sleep(std::chrono::seconds(1)).get();
                }
            });
        });
    });
}
