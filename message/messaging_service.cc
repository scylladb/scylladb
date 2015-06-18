/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#include "message/messaging_service.hh"
#include "core/distributed.hh"
#include "gms/failure_detector.hh"
#include "gms/gossiper.hh"
#include "service/storage_service.hh"

namespace net {
distributed<messaging_service> _the_messaging_service;

future<> deinit_messaging_service() {
    return gms::get_gossiper().stop().then([] {
            return gms::get_failure_detector().stop();
    }).then([] {
            return net::get_messaging_service().stop();
    }).then([]{
            return service::deinit_storage_service();
    });
}

future<> init_messaging_service(sstring listen_address, db::config::seed_provider_type seed_provider) {
    const gms::inet_address listen(listen_address);
    std::set<gms::inet_address> seeds;
    if (seed_provider.parameters.count("seeds") > 0) {
        size_t begin = 0;
        size_t next = 0;
        sstring& seeds_str = seed_provider.parameters.find("seeds")->second;
        while (begin < seeds_str.length() && begin != (next=seeds_str.find(",",begin))) {
            seeds.emplace(gms::inet_address(seeds_str.substr(begin,next-begin)));
            begin = next+1;
        }
    }
    if (seeds.empty()) {
        seeds.emplace(gms::inet_address("127.0.0.1"));
    }

    engine().at_exit([]{
            return deinit_messaging_service();
    });

    return net::get_messaging_service().start(listen).then([seeds] {
        auto& ms = net::get_local_messaging_service();
        print("Messaging server listening on ip %s port %d ...\n", ms.listen_address(), ms.port());
        return gms::get_failure_detector().start().then([seeds] {
            return gms::get_gossiper().start().then([seeds] {
                auto& gossiper = gms::get_local_gossiper();
                gossiper.set_seeds(seeds);
                auto& ss = service::get_local_storage_service();
                return ss.init_server();
            });
        });
    });
}

}
