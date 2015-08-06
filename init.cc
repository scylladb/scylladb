/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#include "init.hh"
#include "message/messaging_service.hh"
#include "gms/failure_detector.hh"
#include "gms/gossiper.hh"
#include "service/storage_service.hh"

future<> init_storage_service(distributed<database>& db) {
    return service::init_storage_service(db).then([] {
        engine().at_exit([] { return service::deinit_storage_service(); });
    });
}

future<> init_ms_fd_gossiper(sstring listen_address, db::seed_provider_type seed_provider) {
    const gms::inet_address listen(listen_address);
    // Init messaging_service
    return net::get_messaging_service().start(listen).then([]{
        engine().at_exit([] { return net::get_messaging_service().stop(); });
    }).then([] {
        // Init failure_detector
        return gms::get_failure_detector().start().then([] {
            engine().at_exit([]{ return gms::get_failure_detector().stop(); });
        });
    }).then([listen_address, seed_provider] {
        // Init gossiper
        std::set<gms::inet_address> seeds;
        if (seed_provider.parameters.count("seeds") > 0) {
            size_t begin = 0;
            size_t next = 0;
            sstring seeds_str = seed_provider.parameters.find("seeds")->second;
            while (begin < seeds_str.length() && begin != (next=seeds_str.find(",",begin))) {
                seeds.emplace(gms::inet_address(seeds_str.substr(begin,next-begin)));
                begin = next+1;
            }
        }
        if (seeds.empty()) {
            seeds.emplace(gms::inet_address("127.0.0.1"));
        }
        return gms::get_gossiper().start().then([seeds] {
            auto& gossiper = gms::get_local_gossiper();
            gossiper.set_seeds(seeds);
            engine().at_exit([]{ return gms::get_gossiper().stop(); });
        });
    });
}
