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

#include "init.hh"
#include "message/messaging_service.hh"
#include "gms/failure_detector.hh"
#include "gms/gossiper.hh"
#include "service/storage_service.hh"
#include "to_string.hh"
#include "gms/inet_address.hh"

logging::logger startlog("init");

//
// NOTE: there functions are (temporarily)
// duplicated in cql_test_env.cc
// until proper shutdown is done.

void init_storage_service(distributed<database>& db, sharded<auth::service>& auth_service, sharded<db::system_distributed_keyspace>& sys_dist_ks) {
    service::init_storage_service(db, auth_service, sys_dist_ks).get();
    // #293 - do not stop anything
    //engine().at_exit([] { return service::deinit_storage_service(); });
}

void init_ms_fd_gossiper(sstring listen_address_in
                , uint16_t storage_port
                , uint16_t ssl_storage_port
                , bool tcp_nodelay_inter_dc
                , sstring ms_encrypt_what
                , sstring ms_trust_store
                , sstring ms_cert
                , sstring ms_key
                , sstring ms_tls_prio
                , bool ms_client_auth
                , sstring ms_compress
                , db::seed_provider_type seed_provider
                , size_t available_memory
                , init_scheduling_config scheduling_config
                , sstring cluster_name
                , double phi
                , bool sltba)
{
    const auto listen = gms::inet_address::lookup(listen_address_in).get0();

    using encrypt_what = netw::messaging_service::encrypt_what;
    using compress_what = netw::messaging_service::compress_what;
    using tcp_nodelay_what = netw::messaging_service::tcp_nodelay_what;
    using namespace seastar::tls;

    encrypt_what ew = encrypt_what::none;
    if (ms_encrypt_what == "all") {
        ew = encrypt_what::all;
    } else if (ms_encrypt_what == "dc") {
        ew = encrypt_what::dc;
    } else if (ms_encrypt_what == "rack") {
        ew = encrypt_what::rack;
    }

    compress_what cw = compress_what::none;
    if (ms_compress == "all") {
        cw = compress_what::all;
    } else if (ms_compress == "dc") {
        cw = compress_what::dc;
    }

    tcp_nodelay_what tndw = tcp_nodelay_what::all;
    if (!tcp_nodelay_inter_dc) {
        tndw = tcp_nodelay_what::local;
    }

    future<> f = make_ready_future<>();
    std::shared_ptr<credentials_builder> creds;

    if (ew != encrypt_what::none) {
        creds = std::make_shared<credentials_builder>();
        creds->set_dh_level(dh_params::level::MEDIUM);

        creds->set_x509_key_file(ms_cert, ms_key, x509_crt_format::PEM).get();
        if (ms_trust_store.empty()) {
            creds->set_system_trust().get();
        } else {
            creds->set_x509_trust_file(ms_trust_store, x509_crt_format::PEM).get();
        }

        creds->set_priority_string(db::config::default_tls_priority);

        if (!ms_tls_prio.empty()) {
            creds->set_priority_string(ms_tls_prio);
        }
        if (ms_client_auth) {
            creds->set_client_auth(seastar::tls::client_auth::REQUIRE);
        }
    }

    // Init messaging_service
    // Delay listening messaging_service until gossip message handlers are registered
    bool listen_now = false;
    netw::messaging_service::memory_config mcfg = { std::max<size_t>(0.08 * available_memory, 1'000'000) };
    netw::messaging_service::scheduling_config scfg;
    scfg.statement = scheduling_config.statement;
    scfg.streaming = scheduling_config.streaming;
    scfg.gossip = scheduling_config.gossip;
    netw::get_messaging_service().start(listen, storage_port, ew, cw, tndw, ssl_storage_port, creds, mcfg, scfg, sltba, listen_now).get();

    // #293 - do not stop anything
    //engine().at_exit([] { return netw::get_messaging_service().stop(); });
    // Init failure_detector
    gms::get_failure_detector().start(std::move(phi)).get();
    // #293 - do not stop anything
    //engine().at_exit([]{ return gms::get_failure_detector().stop(); });
    // Init gossiper
    std::set<gms::inet_address> seeds;
    if (seed_provider.parameters.count("seeds") > 0) {
        size_t begin = 0;
        size_t next = 0;
        sstring seeds_str = seed_provider.parameters.find("seeds")->second;
        while (begin < seeds_str.length() && begin != (next=seeds_str.find(",",begin))) {
            auto seed = boost::trim_copy(seeds_str.substr(begin,next-begin));
            try {
                seeds.emplace(gms::inet_address::lookup(seed).get0());
            } catch (...) {
                startlog.error("Bad configuration: invalid value in 'seeds': '{}': {}", seed, std::current_exception());
                throw bad_configuration_error();
            }
            begin = next+1;
        }
    }
    if (seeds.empty()) {
        seeds.emplace(gms::inet_address("127.0.0.1"));
    }
    auto broadcast_address = utils::fb_utilities::get_broadcast_address();
    if (broadcast_address != listen && seeds.count(listen)) {
        print("Use broadcast_address instead of listen_address for seeds list: seeds=%s, listen_address=%s, broadcast_address=%s\n",
                to_string(seeds), listen_address_in, broadcast_address);
        throw std::runtime_error("Use broadcast_address for seeds list");
    }
    gms::get_gossiper().start().get();
    auto& gossiper = gms::get_local_gossiper();
    gossiper.set_seeds(seeds);
    // #293 - do not stop anything
    //engine().at_exit([]{ return gms::get_gossiper().stop(); });
    gms::get_gossiper().invoke_on_all([cluster_name](gms::gossiper& g) {
        g.set_cluster_name(cluster_name);
    });
}


std::vector<std::reference_wrapper<configurable>>& configurable::configurables() {
    static std::vector<std::reference_wrapper<configurable>> configurables;
    return configurables;
}

void configurable::register_configurable(configurable & c) {
    configurables().emplace_back(std::ref(c));
}

void configurable::append_all(db::config& cfg, boost::program_options::options_description_easy_init& init) {
    for (configurable& c : configurables()) {
        c.append_options(cfg, init);
    }
}

future<> configurable::init_all(const boost::program_options::variables_map& opts, const db::config& cfg, db::extensions& exts) {
    return do_for_each(configurables(), [&](configurable& c) {
        return c.initialize(opts, cfg, exts);
    });
}

future<> configurable::init_all(const db::config& cfg, db::extensions& exts) {
    return do_with(boost::program_options::variables_map{}, [&](auto& opts) {
        return init_all(opts, cfg, exts);
    });
}
