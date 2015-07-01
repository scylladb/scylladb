/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#include "message/messaging_service.hh"
#include "core/distributed.hh"
#include "gms/failure_detector.hh"
#include "gms/gossiper.hh"
#include "service/storage_service.hh"

namespace net {

future<> ser_messaging_verb(output_stream<char>& out, messaging_verb& v) {
    bytes b(bytes::initialized_later(), sizeof(v));
    auto _out = b.begin();
    serialize_int32(_out, int32_t(v));
    return out.write(reinterpret_cast<const char*>(b.c_str()), sizeof(v));
}

future<> des_messaging_verb(input_stream<char>& in, messaging_verb& v) {
    return in.read_exactly(sizeof(v)).then([&v] (temporary_buffer<char> buf) mutable {
        if (buf.size() != sizeof(v)) {
            throw rpc::closed_error();
        }
        bytes_view bv(reinterpret_cast<const int8_t*>(buf.get()), sizeof(v));
        v = messaging_verb(read_simple<int32_t>(bv));
    });
}

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
            });
        });
    });
}

} // namespace net
