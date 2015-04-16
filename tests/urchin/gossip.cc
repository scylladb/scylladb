#include "core/reactor.hh"
#include "core/app-template.hh"
#include "message/messaging_service.hh"
#include "gms/failure_detector.hh"
#include "gms/gossiper.hh"
#include "gms/application_state.hh"

namespace bpo = boost::program_options;

int main(int ac, char ** av) {
    app_template app;
    app.add_options()
        ("seed", bpo::value<std::vector<std::string>>(), "IP address of seed node")
        ("listen-address", bpo::value<std::string>()->default_value("0.0.0.0"), "IP address to listen");
    return app.run(ac, av, [&] {
        auto&& config = app.configuration();
        auto listen = gms::inet_address(config["listen-address"].as<std::string>());
        net::get_messaging_service().start(std::ref(listen)).then([&] {
            auto& server = net::get_local_messaging_service();
            auto min = server.port_min();
            auto max = server.port_max();
            auto listen = server.listen_address();
            print("Messaging server listening on ip %s ports %d to %d ...\n", listen, min, max);
            gms::get_failure_detector().start_single().then([&] {
                gms::get_gossiper().start_single().then([&] {
                    auto&& config = app.configuration();
                    std::set<gms::inet_address> seeds;
                    for (auto s : config["seed"].as<std::vector<std::string>>()) {
                        seeds.emplace(std::move(s));
                    }

                    std::cout << "Start gossiper service ...\n";
                    auto& gossiper = gms::get_local_gossiper();
                    gossiper.set_seeds(std::move(seeds));

                    std::map<gms::application_state, gms::versioned_value> app_states = {
                        { gms::application_state::LOAD, gms::versioned_value::versioned_value_factory::load(0.5) },
                    };

                    int generation_number = 1;
                    gossiper.start(generation_number, app_states);

                    auto reporter = std::make_shared<timer<lowres_clock>>();
                    reporter->set_callback ([reporter] {
                        auto& gossiper = gms::get_local_gossiper();
                        gossiper.dump_endpoint_state_map();
                    });
                    reporter->arm_periodic(std::chrono::milliseconds(1000));
                });
            });
        });
    });
}
