/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
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

#include "init.hh"
#include "message/messaging_service.hh"
#include "gms/failure_detector.hh"
#include "gms/gossiper.hh"
#include "service/storage_service.hh"
#include "service/pending_range_calculator_service.hh"
#include "to_string.hh"
#include "gms/inet_address.hh"

//
// NOTE: there functions are (temporarily)
// duplicated in cql_test_env.cc
// until proper shutdown is done.

future<> init_storage_service(distributed<database>& db) {
    return service::get_pending_range_calculator_service().start(std::ref(db)).then([] {
        // #293 - do not stop anything
        // engine().at_exit([] { return service::get_pending_range_calculator_service().stop(); });
    }).then([&db] {
        return service::init_storage_service(db).then([] {
            // #293 - do not stop anything
            //engine().at_exit([] { return service::deinit_storage_service(); });
        });
    });
}

future<> init_ms_fd_gossiper(sstring listen_address
                , uint16_t storage_port
                , uint16_t ssl_storage_port
                , sstring ms_encrypt_what
                , sstring ms_trust_store
                , sstring ms_cert
                , sstring ms_key
                , db::seed_provider_type seed_provider
                , sstring cluster_name
                , double phi)
{
    const gms::inet_address listen(listen_address);

    using encrypt_what = net::messaging_service::encrypt_what;
    using namespace seastar::tls;

    encrypt_what ew = encrypt_what::none;
    if (ms_encrypt_what == "all") {
        ew = encrypt_what::all;
    } else if (ms_encrypt_what == "dc") {
        ew = encrypt_what::dc;
    } else if (ms_encrypt_what == "rack") {
        ew = encrypt_what::rack;
    }

    future<> f = make_ready_future<>();
    ::shared_ptr<server_credentials> creds;

    if (ew != encrypt_what::none) {
        // note: credentials are immutable after this, and ok to share across shards
        creds = ::make_shared<server_credentials>(::make_shared<dh_params>(dh_params::level::MEDIUM));
        f = creds->set_x509_key_file(ms_cert, ms_key, x509_crt_format::PEM).then([creds, ms_trust_store] {
            return ms_trust_store.empty()
                            ? creds->set_system_trust()
                                            : creds->set_x509_trust_file(ms_trust_store, x509_crt_format::PEM)
                                              ;

        });
    }

    // Init messaging_service
    return f.then([listen, storage_port, creds, ssl_storage_port, ew] {
        return net::get_messaging_service().start(listen, storage_port, ew, ssl_storage_port, creds);
        // #293 - do not stop anything
        //engine().at_exit([] { return net::get_messaging_service().stop(); });
    }).then([phi] {
        // Init failure_detector
        return gms::get_failure_detector().start(std::move(phi)).then([] {
            // #293 - do not stop anything
            //engine().at_exit([]{ return gms::get_failure_detector().stop(); });
        });
    }).then([listen_address, seed_provider, cluster_name] {
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
        auto broadcast_address = utils::fb_utilities::get_broadcast_address();
        if (broadcast_address != listen_address && seeds.count(listen_address)) {
            print("Use broadcast_address instead of listen_address for seeds list: seeds=%s, listen_address=%s, broadcast_address=%s\n",
                  to_string(seeds), listen_address, broadcast_address);
            throw std::runtime_error("Use broadcast_address for seeds list");
        }
        return gms::get_gossiper().start().then([seeds, cluster_name] {
            auto& gossiper = gms::get_local_gossiper();
            gossiper.set_seeds(seeds);
            // #293 - do not stop anything
            //engine().at_exit([]{ return gms::get_gossiper().stop(); });
        }).then([cluster_name] {
            return gms::get_gossiper().invoke_on_all([cluster_name](gms::gossiper& g) {
                g.set_cluster_name(cluster_name);
            });
        });
    });
}
