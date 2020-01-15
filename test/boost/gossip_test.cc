
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


#include <boost/test/unit_test.hpp>

#include <seastar/util/defer.hh>

#include <seastar/testing/test_case.hh>
#include "message/messaging_service.hh"
#include "gms/failure_detector.hh"
#include "gms/gossiper.hh"
#include "gms/feature_service.hh"
#include <seastar/core/reactor.hh>
#include "service/storage_service.hh"
#include <seastar/core/distributed.hh>
#include <seastar/core/abort_source.hh>
#include "database.hh"
#include "db/system_distributed_keyspace.hh"
#include "db/config.hh"
#include "cql3/cql_config.hh"

namespace db::view {
class view_update_generator;
}

SEASTAR_TEST_CASE(test_boot_shutdown){
    return seastar::async([] {
        distributed<database> db;
        database_config dbcfg;
        db::config cfg;
        sharded<abort_source> abort_sources;
        sharded<auth::service> auth_service;
        sharded<db::system_distributed_keyspace> sys_dist_ks;
        sharded<db::view::view_update_generator> view_update_generator;
        utils::fb_utilities::set_broadcast_address(gms::inet_address("127.0.0.1"));
        sharded<gms::feature_service> feature_service;

        abort_sources.start().get();
        auto stop_abort_sources = defer([&] { abort_sources.stop().get(); });

        feature_service.start().get();
        auto stop_feature_service = defer([&] { feature_service.stop().get(); });

        locator::i_endpoint_snitch::create_snitch("SimpleSnitch").get();
        auto stop_snitch = defer([&] { locator::i_endpoint_snitch::stop_snitch().get(); });

        netw::get_messaging_service().start(gms::inet_address("127.0.0.1"), 7000, false /* don't bind */).get();
        auto stop_messaging_service = defer([&] { netw::get_messaging_service().stop().get(); });

        gms::get_gossiper().start(std::ref(abort_sources), std::ref(feature_service), std::ref(cfg)).get();
        auto stop_gossiper = defer([&] { gms::get_gossiper().stop().get(); });

        service::storage_service_config sscfg;
        sscfg.available_memory =  memory::stats().total_memory();
        sharded<cql3::cql_config> cql_config;
        cql_config.start().get();
        auto stop_cql_config = defer([&] { cql_config.stop().get(); });

        service::get_storage_service().start(std::ref(abort_sources), std::ref(db), std::ref(gms::get_gossiper()), std::ref(auth_service), std::ref(cql_config), std::ref(sys_dist_ks), std::ref(view_update_generator), std::ref(feature_service), sscfg, true).get();
        auto stop_ss = defer([&] { service::get_storage_service().stop().get(); });

        db.start(std::ref(cfg), dbcfg).get();
        auto stop_db = defer([&] { db.stop().get(); });
        auto stop_database_d = defer([&db] {
            stop_database(db).get();
        });

    });
}
