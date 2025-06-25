/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <vector>
#include "gms/gossip_address_map.hh"
#include "gms/inet_address.hh"
#include "repair/repair.hh"
#include "repair/task_manager_module.hh"
#include "service/topology_guard.hh"
#include "tasks/task_manager.hh"
#include "locator/abstract_replication_strategy.hh"
#include <seastar/core/distributed.hh>
#include <seastar/util/bool_class.hh>
#include "utils/user_provided_param.hh"
#include "locator/tablet_metadata_guard.hh"

using namespace seastar;

class row_level_repair_gossip_helper;

namespace service {
class migration_manager;
class storage_proxy;
}

namespace db {

class system_keyspace;
class system_distributed_keyspace;
class batchlog_manager;

namespace view {
class view_building_worker;
}

}

namespace gms {
    class gossiper;
}

struct small_table_optimization_params {
    locator::effective_replication_map_ptr erm;
};

class repair_meta;

using repair_meta_ptr = shared_ptr<repair_meta>;

struct shard_config {
    unsigned shard;
    unsigned shard_count;
    unsigned ignore_msb;
};

class repair_history {
public:
    std::unordered_map<table_id, std::unordered_map<dht::token_range, size_t>> finished_ranges;
    gc_clock::time_point repair_time = gc_clock::time_point::max();
};

class node_ops_metrics {
    shared_ptr<repair::task_manager_module> _module;
public:
    node_ops_metrics(shared_ptr<repair::task_manager_module> module);

    uint64_t bootstrap_total_ranges{0};
    uint64_t bootstrap_finished_ranges{0};
    uint64_t replace_total_ranges{0};
    uint64_t replace_finished_ranges{0};
    uint64_t rebuild_total_ranges{0};
    uint64_t rebuild_finished_ranges{0};
    uint64_t decommission_total_ranges{0};
    uint64_t decommission_finished_ranges{0};
    uint64_t removenode_total_ranges{0};
    uint64_t removenode_finished_ranges{0};
    uint64_t repair_total_ranges_sum{0};
    uint64_t repair_finished_ranges_sum{0};
private:
    seastar::metrics::metric_groups _metrics;
public:
    float bootstrap_finished_percentage();
    float replace_finished_percentage();
    float rebuild_finished_percentage();
    float decommission_finished_percentage();
    float removenode_finished_percentage();
    float repair_finished_percentage();
};

using host2ip_t = std::function<future<gms::inet_address> (locator::host_id)>;

class repair_service : public seastar::peering_sharded_service<repair_service> {
    sharded<service::topology_state_machine>& _tsm;
    distributed<gms::gossiper>& _gossiper;
    netw::messaging_service& _messaging;
    sharded<replica::database>& _db;
    sharded<service::storage_proxy>& _sp;
    sharded<db::batchlog_manager>& _bm;
    sharded<db::system_keyspace>& _sys_ks;
    db::view::view_builder& _view_builder;
    sharded<db::view::view_building_worker>& _view_building_worker;
    shared_ptr<repair::task_manager_module> _repair_module;
    service::migration_manager& _mm;
    node_ops_metrics _node_ops_metrics;
    std::unordered_map<node_repair_meta_id, repair_meta_ptr> _repair_metas;
    uint32_t _next_repair_meta_id = 0;  // used only on shard 0

    std::unordered_map<tasks::task_id, repair_history> _finished_ranges_history;

    shared_ptr<row_level_repair_gossip_helper> _gossip_helper;
    bool _stopped = false;

    size_t _max_repair_memory;
    seastar::semaphore _memory_sem;
    seastar::named_semaphore _load_parallelism_semaphore = {16, named_semaphore_exception_factory{"Load repair history parallelism"}};

    future<> _load_history_done = make_ready_future<>();

    mutable std::default_random_engine _random_engine{std::random_device{}()};

    future<> init_ms_handlers();
    future<> uninit_ms_handlers();

    seastar::semaphore _flush_hints_batchlog_sem{1};
    gc_clock::time_point _flush_hints_batchlog_time;
    future<std::tuple<bool, gc_clock::time_point>> flush_hints(repair_uniq_id id,
            sstring keyspace, std::vector<sstring> cfs,
            std::unordered_set<locator::host_id> ignore_nodes);

public:
    repair_service(sharded<service::topology_state_machine>& tsm,
            distributed<gms::gossiper>& gossiper,
            netw::messaging_service& ms,
            sharded<replica::database>& db,
            sharded<service::storage_proxy>& sp,
            sharded<db::batchlog_manager>& bm,
            sharded<db::system_keyspace>& sys_ks,
            db::view::view_builder& vb,
            sharded<db::view::view_building_worker>& vbw,
            tasks::task_manager& tm,
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

    future<std::optional<gc_clock::time_point>> update_history(tasks::task_id repair_id, table_id table_id, dht::token_range range, gc_clock::time_point repair_time, bool is_tablet);
    future<> cleanup_history(tasks::task_id repair_id);
    future<> load_history();

    future<int> do_repair_start(gms::gossip_address_map& addr_map, sstring keyspace, std::unordered_map<sstring, sstring> options_map);

    // The tokens are the tokens assigned to the bootstrap node.
    // all repair-based node operation entry points must be called on shard 0
    future<> bootstrap_with_repair(locator::token_metadata_ptr tmptr, std::unordered_set<dht::token> bootstrap_tokens);
    future<> decommission_with_repair(locator::token_metadata_ptr tmptr);
    future<> removenode_with_repair(locator::token_metadata_ptr tmptr, locator::host_id leaving_node, shared_ptr<node_ops_info> ops);
    future<> rebuild_with_repair(std::unordered_map<sstring, locator::vnode_effective_replication_map_ptr> ks_erms, locator::token_metadata_ptr tmptr, utils::optional_param source_dc);
    future<> replace_with_repair(std::unordered_map<sstring, locator::vnode_effective_replication_map_ptr> ks_erms, locator::token_metadata_ptr tmptr, std::unordered_set<dht::token> replacing_tokens, std::unordered_set<locator::host_id> ignore_nodes, locator::host_id replaced_node);
private:
    future<> do_decommission_removenode_with_repair(locator::token_metadata_ptr tmptr, locator::host_id leaving_node, shared_ptr<node_ops_info> ops);

    future<> do_rebuild_replace_with_repair(std::unordered_map<sstring, locator::vnode_effective_replication_map_ptr> ks_erms, locator::token_metadata_ptr tmptr, sstring op, utils::optional_param source_dc, streaming::stream_reason reason, std::unordered_set<locator::host_id> ignore_nodes = {}, locator::host_id replaced_node = {});

    // Must be called on shard 0
    future<> sync_data_using_repair(sstring keyspace,
            locator::effective_replication_map_ptr erm,
            dht::token_range_vector ranges,
            std::unordered_map<dht::token_range, repair_neighbors> neighbors,
            streaming::stream_reason reason,
            shared_ptr<node_ops_info> ops_info);

public:
    future<> repair_tablets(repair_uniq_id id, sstring keyspace_name, std::vector<sstring> table_names, bool primary_replica_only = true, dht::token_range_vector ranges_specified = {}, std::vector<sstring> dcs = {}, std::unordered_set<locator::host_id> hosts = {}, std::unordered_set<locator::host_id> ignore_nodes = {}, std::optional<int> ranges_parallelism = std::nullopt);

    future<gc_clock::time_point> repair_tablet(gms::gossip_address_map& addr_map, locator::tablet_metadata_guard& guard, locator::global_tablet_id gid, tasks::task_info global_tablet_repair_task_info, service::frozen_topology_guard topo_guard, std::optional<locator::tablet_replica_set> rebuild_replicas);
private:

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
    db::view::view_builder& get_view_builder() noexcept { return _view_builder; }
    sharded<db::view::view_building_worker>& get_view_building_worker() noexcept { return _view_building_worker; }
    gms::gossiper& get_gossiper() noexcept { return _gossiper.local(); }
    size_t max_repair_memory() const { return _max_repair_memory; }
    seastar::semaphore& memory_sem() { return _memory_sem; }
    locator::host_id my_host_id() const noexcept;

    repair::task_manager_module& get_repair_module() noexcept {
        return *_repair_module;
    }

    const node_ops_metrics& get_metrics() const noexcept {
        return _node_ops_metrics;
    };
    node_ops_metrics& get_metrics() noexcept {
        return _node_ops_metrics;
    };

    // returns a vector with the ids of the active repairs
    future<std::vector<int>> get_active_repairs();

    // returns the status of repair task `id`
    future<repair_status> get_status(int id);

    // If the repair job is finished (SUCCESSFUL or FAILED), it returns immediately.
    // It blocks if the repair job is still RUNNING until timeout.
    future<repair_status> await_completion(int id, std::chrono::steady_clock::time_point timeout);

    // Abort all the repairs
    future<> abort_all();

    std::unordered_map<node_repair_meta_id, repair_meta_ptr>& repair_meta_map() noexcept {
        return _repair_metas;
    }

    repair_meta_ptr get_repair_meta(locator::host_id from, uint32_t repair_meta_id);

    future<>
    insert_repair_meta(
            locator::host_id from_id,
            uint32_t src_cpu_id,
            uint32_t repair_meta_id,
            dht::token_range range,
            row_level_diff_detect_algorithm algo,
            uint64_t max_row_buf_size,
            uint64_t seed,
            shard_config master_node_shard_config,
            table_schema_version schema_version,
            streaming::stream_reason reason,
            gc_clock::time_point compaction_time,
            abort_source& as,
            service::frozen_topology_guard topo_guard);

    future<>
    remove_repair_meta(const locator::host_id& from,
            uint32_t repair_meta_id,
            sstring ks_name,
            sstring cf_name,
            dht::token_range range);

    future<> remove_repair_meta(locator::host_id from);

    future<> remove_repair_meta();

    future<uint32_t> get_next_repair_meta_id();

    friend class repair::user_requested_repair_task_impl;
    friend class repair::data_sync_repair_task_impl;
    friend class repair::tablet_repair_task_impl;
};

class repair_info;
using repair_master = bool_class<class repair_master_tag>;
class partition_key_and_mutation_fragments;
using repair_rows_on_wire = std::list<partition_key_and_mutation_fragments>;
class repair_row;
class repair_hasher;
class repair_writer;

future<> repair_cf_range_row_level(repair::shard_repair_task_impl& shard_task,
        sstring cf_name, table_id table_id, dht::token_range range,
        const std::vector<locator::host_id>& all_peer_nodes, bool small_table_optimization, gc_clock::time_point flush_time,
        service::frozen_topology_guard topo_guard);
future<std::list<repair_row>> to_repair_rows_list(repair_rows_on_wire rows,
        schema_ptr s, uint64_t seed, repair_master is_master,
        reader_permit permit, repair_hasher hasher);
void flush_rows(schema_ptr s, std::list<repair_row>& rows, lw_shared_ptr<repair_writer>& writer, std::optional<small_table_optimization_params> small_table_optimization = std::nullopt, repair_meta* rm = nullptr);
