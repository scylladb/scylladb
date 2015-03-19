/*
 * Copyright 2014 Cloudius Systems
 */


#include "database.hh"
#include "core/app-template.hh"
#include "core/distributed.hh"
#include "thrift/server.hh"
#include "transport/server.hh"

namespace bpo = boost::program_options;

int main(int ac, char** av) {
    app_template app;
    app.add_options()
        ("cql-port", bpo::value<uint16_t>()->default_value(9042), "CQL port")
        ("thrift-port", bpo::value<uint16_t>()->default_value(9160), "Thrift port")
        ("datadir", bpo::value<std::string>()->default_value("/var/lib/cassandra/data"), "data directory");

    auto server = std::make_unique<distributed<thrift_server>>();
    distributed<database> db;
    distributed<cql3::query_processor> qp;
    service::storage_proxy proxy{db};

    return app.run(ac, av, [&] {
        auto&& config = app.configuration();
        uint16_t thrift_port = config["thrift-port"].as<uint16_t>();
        uint16_t cql_port = config["cql-port"].as<uint16_t>();
        sstring datadir = config["datadir"].as<std::string>();

        return db.start().then([datadir, &db] {
            engine().at_exit([&db] { return db.stop(); });
            return db.invoke_on_all(&database::init_from_data_directory, datadir);
        }).then([&db, &proxy, &qp] {
            return qp.start(std::ref(proxy), std::ref(db));
        }).then([&db, &proxy, &qp, cql_port, thrift_port] {
            auto cserver = new distributed<cql_server>;
            cserver->start(std::ref(proxy), std::ref(qp)).then([server = std::move(cserver), cql_port] () mutable {
                    server->invoke_on_all(&cql_server::listen, ipv4_addr{cql_port});
            }).then([cql_port] {
                std::cout << "CQL server listening on port " << cql_port << " ...\n";
            });
            auto tserver = new distributed<thrift_server>;
            tserver->start(std::ref(db)).then([server = std::move(tserver), thrift_port] () mutable {
                    server->invoke_on_all(&thrift_server::listen, ipv4_addr{thrift_port});
            }).then([thrift_port] {
                std::cout << "Thrift server listening on port " << thrift_port << " ...\n";
            });
        }).or_terminate();
    });
}
