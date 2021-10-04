
/*
 * Copyright (C) 2015-present ScyllaDB
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
#include <seastar/core/seastar.hh>
#include "service/storage_service.hh"
#include "service/raft/raft_group_registry.hh"
#include <seastar/core/distributed.hh>
#include <seastar/core/abort_source.hh>
#include "cdc/generation_service.hh"
#include "repair/repair.hh"
#include "database.hh"
#include "db/system_distributed_keyspace.hh"
#include "db/config.hh"
#include "compaction/compaction_manager.hh"
#include "service/endpoint_lifecycle_subscriber.hh"
#include "db/schema_tables.hh"
#include "schema_registry.hh"

namespace db::view {
class view_update_generator;
}

SEASTAR_TEST_CASE(test_boot_shutdown){
    return seastar::async([] {
        distributed<database> db;
        database_config dbcfg;
        dbcfg.available_memory = memory::stats().total_memory();
        auto cfg = std::make_unique<db::config>();
        sharded<service::migration_notifier> mm_notif;
        sharded<abort_source> abort_sources;
        sharded<db::system_distributed_keyspace> sys_dist_ks;
        utils::fb_utilities::set_broadcast_address(gms::inet_address("127.0.0.1"));
        sharded<gms::feature_service> feature_service;
        sharded<locator::shared_token_metadata> token_metadata;
        sharded<netw::messaging_service> _messaging;
        sharded<cdc::generation_service> cdc_generation_service;
        sharded<repair_service> repair;
        sharded<service::migration_manager> migration_manager;
        sharded<cql3::query_processor> qp;
        sharded<service::raft_group_registry> raft_gr;
        sharded<service::endpoint_lifecycle_notifier> elc_notif;

        token_metadata.start([] () noexcept { return db::schema_tables::hold_merge_lock(); }).get();
        auto stop_token_mgr = defer([&token_metadata] { token_metadata.stop().get(); });

        mm_notif.start().get();
        auto stop_mm_notif = defer([&mm_notif] { mm_notif.stop().get(); });

        abort_sources.start().get();
        auto stop_abort_sources = defer([&] { abort_sources.stop().get(); });

        feature_service.start(gms::feature_config_from_db_config(*cfg)).get();
        auto stop_feature_service = defer([&] { feature_service.stop().get(); });

        locator::i_endpoint_snitch::create_snitch("SimpleSnitch").get();
        auto stop_snitch = defer([&] { locator::i_endpoint_snitch::stop_snitch().get(); });

        _messaging.start(gms::inet_address("127.0.0.1"), 7000).get();
        auto stop_messaging_service = defer([&] { _messaging.stop().get(); });

        gms::gossip_config gcfg;
        gms::get_gossiper().start(std::ref(abort_sources), std::ref(feature_service), std::ref(token_metadata), std::ref(_messaging), std::ref(*cfg), std::move(gcfg)).get();
        auto stop_gossiper = defer([&] { gms::get_gossiper().stop().get(); });

        service::storage_service_config sscfg;
        sscfg.available_memory =  memory::stats().total_memory();

        raft_gr.start(std::ref(_messaging), std::ref(gms::get_gossiper()), std::ref(qp)).get();
        auto stop_raft = defer([&raft_gr] { raft_gr.stop().get(); });

        elc_notif.start().get();
        auto stop_elc_notif = defer([&elc_notif] { elc_notif.stop().get(); });

        sharded<service::storage_service> ss;
        ss.start(std::ref(abort_sources),
            std::ref(db), std::ref(gms::get_gossiper()),
            std::ref(sys_dist_ks),
            std::ref(feature_service), sscfg,
            std::ref(migration_manager), std::ref(token_metadata),
            std::ref(_messaging),
            std::ref(cdc_generation_service), std::ref(repair),
            std::ref(raft_gr), std::ref(elc_notif)).get();
        auto stop_ss = defer([&] { ss.stop().get(); });

        sharded<semaphore> sst_dir_semaphore;
        sst_dir_semaphore.start(cfg->initial_sstable_loading_concurrency()).get();
        auto stop_sst_dir_sem = defer([&sst_dir_semaphore] {
            sst_dir_semaphore.stop().get();
        });

        sharded<schema_registry> schema_registry;
        schema_registry.start().get();
        schema_registry.invoke_on_all([] (::schema_registry& sr) {
            ::set_local_schema_registry(sr);
        }).get();
        auto stop_schema_registry = defer([&schema_registry] {
            schema_registry.stop().get();
        });

        db.start(std::ref(*cfg), dbcfg, std::ref(mm_notif), std::ref(feature_service), std::ref(token_metadata), std::ref(schema_registry), std::ref(abort_sources), std::ref(sst_dir_semaphore)).get();
        auto stop_db = defer([&] { db.stop().get(); });

        cdc::generation_service::config cdc_cfg;
        cdc_generation_service.start(std::move(cdc_cfg), std::ref(gms::get_gossiper()), std::ref(sys_dist_ks), std::ref(abort_sources), std::ref(token_metadata), std::ref(feature_service), std::ref(db)).get();
        auto stop_cdc_generation_service = defer([&cdc_generation_service] {
            cdc_generation_service.stop().get();
        });
    });
}
