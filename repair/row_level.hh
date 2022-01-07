/*
 * Copyright (C) 2018-present ScyllaDB
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

#pragma once

#include <vector>
#include "gms/inet_address.hh"
#include "repair/repair.hh"
#include <seastar/core/distributed.hh>

class row_level_repair_gossip_helper;

namespace service {
class migration_manager;
class storage_proxy;
}

namespace db {

class system_distributed_keyspace;
class batchlog_manager;

}

namespace gms {
    class gossiper;
}

class repair_history {
public:
    // The key for the map is the table_id
    std::unordered_map<utils::UUID, std::unordered_map<dht::token_range, size_t>> finished_ranges;
    gc_clock::time_point repair_time = gc_clock::time_point::max();
};

class repair_service : public seastar::peering_sharded_service<repair_service> {
    distributed<gms::gossiper>& _gossiper;
    netw::messaging_service& _messaging;
    sharded<replica::database>& _db;
    sharded<service::storage_proxy>& _sp;
    sharded<db::batchlog_manager>& _bm;
    sharded<db::system_distributed_keyspace>& _sys_dist_ks;
    sharded<db::view::view_update_generator>& _view_update_generator;
    service::migration_manager& _mm;

    std::unordered_map<utils::UUID, repair_history> _finished_ranges_history;

    shared_ptr<row_level_repair_gossip_helper> _gossip_helper;
    std::unique_ptr<tracker> _tracker;
    bool _stopped = false;

    size_t _max_repair_memory;
    seastar::semaphore _memory_sem;

    future<> init_ms_handlers();
    future<> uninit_ms_handlers();

    future<> init_metrics();

public:
    repair_service(distributed<gms::gossiper>& gossiper,
            netw::messaging_service& ms,
            sharded<replica::database>& db,
            sharded<service::storage_proxy>& sp,
            sharded<db::batchlog_manager>& bm,
            sharded<db::system_distributed_keyspace>& sys_dist_ks,
            sharded<db::view::view_update_generator>& vug,
            service::migration_manager& mm, size_t max_repair_memory);
    ~repair_service();
    future<> start();
    future<> stop();

    // shutdown() stops all ongoing repairs started on this node (and
    // prevents any further repairs from being started). It returns a future
    // saying when all repairs have stopped, and attempts to stop them as
    // quickly as possible (we do not wait for repairs to finish but rather
    // stop them abruptly).
    future<> shutdown();

    future<std::optional<gc_clock::time_point>> update_history(utils::UUID repair_id, utils::UUID table_id, dht::token_range range, gc_clock::time_point repair_time);
    future<> cleanup_history(utils::UUID repair_id);
    future<> load_history();

    int do_repair_start(sstring keyspace, std::unordered_map<sstring, sstring> options_map);

    // The tokens are the tokens assigned to the bootstrap node.
    future<> bootstrap_with_repair(locator::token_metadata_ptr tmptr, std::unordered_set<dht::token> bootstrap_tokens);
    future<> decommission_with_repair(locator::token_metadata_ptr tmptr);
    future<> removenode_with_repair(locator::token_metadata_ptr tmptr, gms::inet_address leaving_node, shared_ptr<node_ops_info> ops);
    future<> rebuild_with_repair(locator::token_metadata_ptr tmptr, sstring source_dc);
    future<> replace_with_repair(locator::token_metadata_ptr tmptr, std::unordered_set<dht::token> replacing_tokens, std::list<gms::inet_address> ignore_nodes);
private:
    future<> do_decommission_removenode_with_repair(locator::token_metadata_ptr tmptr, gms::inet_address leaving_node, shared_ptr<node_ops_info> ops);
    future<> do_rebuild_replace_with_repair(locator::token_metadata_ptr tmptr, sstring op, sstring source_dc, streaming::stream_reason reason, std::list<gms::inet_address> ignore_nodes);

    future<> sync_data_using_repair(sstring keyspace,
            dht::token_range_vector ranges,
            std::unordered_map<dht::token_range, repair_neighbors> neighbors,
            streaming::stream_reason reason,
            std::optional<utils::UUID> ops_uuid);

    future<> do_sync_data_using_repair(sstring keyspace,
            dht::token_range_vector ranges,
            std::unordered_map<dht::token_range, repair_neighbors> neighbors,
            streaming::stream_reason reason,
            std::optional<utils::UUID> ops_uuid);

    future<repair_update_system_table_response> repair_update_system_table_handler(
            gms::inet_address from,
            repair_update_system_table_request req);

    future<repair_flush_hints_batchlog_response> repair_flush_hints_batchlog_handler(
            gms::inet_address from,
            repair_flush_hints_batchlog_request req);

public:
    netw::messaging_service& get_messaging() noexcept { return _messaging; }
    sharded<replica::database>& get_db() noexcept { return _db; }
    service::migration_manager& get_migration_manager() noexcept { return _mm; }
    sharded<db::system_distributed_keyspace>& get_sys_dist_ks() noexcept { return _sys_dist_ks; }
    sharded<db::view::view_update_generator>& get_view_update_generator() noexcept { return _view_update_generator; }
    gms::gossiper& get_gossiper() noexcept { return _gossiper.local(); }
    size_t max_repair_memory() const { return _max_repair_memory; }
    seastar::semaphore& memory_sem() { return _memory_sem; }
};

class repair_info;

future<> repair_cf_range_row_level(repair_info& ri,
        sstring cf_name, utils::UUID table_id, dht::token_range range,
        const std::vector<gms::inet_address>& all_peer_nodes);

future<> shutdown_all_row_level_repair();
