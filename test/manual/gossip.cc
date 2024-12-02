
/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <seastar/core/seastar.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/app-template.hh>
#include <seastar/util/closeable.hh>

#include "db/config.hh"
#include "db/system_distributed_keyspace.hh"
#include "gms/feature_service.hh"
#include "message/messaging_service.hh"
#include "gms/gossiper.hh"
#include "gms/application_state.hh"
#include "service/qos/service_level_controller.hh"
#include "utils/log.hh"
#include <seastar/core/thread.hh>
#include <chrono>
#include "db/schema_tables.hh"


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
    distributed<replica::database> db;
    app_template app;
    app.add_options()
        ("seed", bpo::value<std::vector<std::string>>(), "IP address of seed node")
        ("listen-address", bpo::value<std::string>()->default_value("0.0.0.0"), "IP address to listen");
    return app.run_deprecated(ac, av, [&app] {
        return async([&app] {
            auto config = app.configuration();
            logging::logger_registry().set_logger_level("gossip", logging::log_level::trace);
            const gms::inet_address listen = gms::inet_address(config["listen-address"].as<std::string>());

            sharded<abort_source> abort_sources;
            sharded<locator::shared_token_metadata> token_metadata;
            sharded<utils::walltime_compressor_tracker> compressor_tracker;
            sharded<gms::feature_service> feature_service;
            sharded<gms::gossip_address_map> gossip_address_map;
            sharded<netw::messaging_service> messaging;
            sharded<auth::service> auth_service;

            abort_sources.start().get();
            auto stop_abort_source = defer([&] { abort_sources.stop().get(); });

            locator::token_metadata::config tm_cfg;
            auto my_address = gms::inet_address("localhost");
            tm_cfg.topo_cfg.this_endpoint = my_address;
            tm_cfg.topo_cfg.this_cql_address = my_address;
            token_metadata.start([] () noexcept { return db::schema_tables::hold_merge_lock(); }, tm_cfg).get();
            auto stop_token_mgr = defer([&] { token_metadata.stop().get(); });
            locator::shared_token_metadata tm({}, {});
            sharded<qos::service_level_controller> sl_controller;
            scheduling_group default_scheduling_group = create_scheduling_group("sl_default_sg", 1.0).get();
            sharded<abort_source> as;
            as.start().get();
            auto stop_as = defer([&as] { as.stop().get(); });
            sl_controller.start(std::ref(auth_service), std::ref(tm), std::ref(as), qos::service_level_options{.shares = 1000}, default_scheduling_group).get();
            compressor_tracker.start([] { return utils::walltime_compressor_tracker::config{}; }).get();
            auto stop_compressor_tracker = deferred_stop(compressor_tracker);

            auto cfg = gms::feature_config_from_db_config(db::config(), {});
            feature_service.start(cfg).get();

            gossip_address_map.start().get();

            messaging.start(locator::host_id{}, listen, 7000, std::ref(feature_service),
                            std::ref(gossip_address_map), std::ref(compressor_tracker),
                            std::ref(sl_controller)).get();
            auto stop_messaging = deferred_stop(messaging);

            gms::gossip_config gcfg;
            gcfg.cluster_name = "Test Cluster";
            for (auto s : config["seed"].as<std::vector<std::string>>()) {
                gcfg.seeds.emplace(std::move(s));
            }
            sharded<gms::gossiper> gossiper;
            gossiper.start(std::ref(abort_sources), std::ref(token_metadata), std::ref(messaging), std::move(gcfg), std::ref(gossip_address_map)).get();

            auto& server = messaging.local();
            auto port = server.port();
            auto msg_listen = server.listen_address();
            fmt::print("Messaging server listening on ip {} port {:d} ...\n", msg_listen, port);

            std::cout << "Start gossiper service ...\n";

            gms::application_state_map app_states = {
                { gms::application_state::LOAD, gms::versioned_value::load(0.5) },
            };

            using namespace std::chrono;
            auto now = high_resolution_clock::now().time_since_epoch();
            auto generation_number = gms::generation_type(duration_cast<seconds>(now).count());
            gossiper.local().start_gossiping(generation_number, app_states).get();
            static double load = 0.5;
            for (;;) {
                auto value = gms::versioned_value::load(load);
                load += 0.0001;
                gossiper.local().add_local_application_state(gms::application_state::LOAD, value).get();
                sleep(std::chrono::seconds(1)).get();
           }
        });
    });
}
