/*
 * Copyright 2014 Cloudius Systems
 */


#include "database.hh"
#include "core/app-template.hh"
#include "core/smp.hh"
#include "thrift/server.hh"

namespace bpo = boost::program_options;

int main(int ac, char** av) {
    app_template app;
    app.add_options()
        ("thrift-port", bpo::value<uint16_t>()->default_value(9160), "Thrift port")
        ("datadir", bpo::value<std::string>()->default_value("/var/lib/cassandra/data"), "data directory");

    auto server = std::make_unique<distributed<thrift_server>>();;

    return app.run(ac, av, [&] {
        auto&& config = app.configuration();
        uint16_t port = config["thrift-port"].as<uint16_t>();
        sstring datadir = config["datadir"].as<std::string>();

        database db(datadir);

        auto server = new distributed<thrift_server>;
        server->start(std::ref(db)).then([server = std::move(server), port] () mutable {
            server->invoke_on_all(&thrift_server::listen, ipv4_addr{port});
        }).then([port] {
            std::cout << "Thrift server listening on port " << port << " ...\n";
        });
    });
}

