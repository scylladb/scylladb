/*
 * Copyright 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <seastar/core/shared_future.hh>
#include <seastar/core/abort_source.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/condition-variable.hh>
#include "dht/token.hh"
#include "raft/raft.hh"
#include "schema/schema_fwd.hh"
#include "dht/i_partitioner_fwd.hh"
#include "service/migration_listener.hh"

namespace replica {
class database;
}

namespace netw {
class messaging_service;
}

namespace service {
class raft_group0_client;
}

namespace db {
class system_keyspace;
}

using namespace seastar;

namespace db::view {

class view_update_generator;

class view_building_worker : public seastar::peering_sharded_service<view_building_worker>, public service::migration_listener::only_view_notifications {
    struct vb_parameters {
        std::set<table_id> views;
        std::vector<view_ptr> view_ptrs;
        dht::token_range range;
    };

    struct vb_operation {
        vb_parameters params;
        raft::term_t term;
        abort_source as;
        shared_future<std::vector<table_id>> task;
    };

    replica::database& _db;
    db::system_keyspace& _sys_ks;
    service::raft_group0_client& _group0_client;
    view_update_generator& _vug;
    sharded<netw::messaging_service>& _messaging;
    abort_source _as;

    seastar::semaphore _op_sem{1};
    std::optional<table_id> _base_id;
    std::unordered_map<dht::token_range, std::set<table_id>> _per_range_built_views_history;
    std::optional<vb_operation> _building_op;

    condition_variable _cond;
    future<> _staging_detector_fiber = make_ready_future();

public:
    view_building_worker(replica::database& db, db::system_keyspace& sys_ks, service::raft_group0_client& group0_client, view_update_generator& vug, sharded<netw::messaging_service>& messaging);

    virtual void on_create_view(const sstring&, const sstring&) override {};
    virtual void on_update_view(const sstring& ks_name, const sstring& view_name, bool columns_changed) override;
    virtual void on_drop_view(const sstring&, const sstring&) override;

    future<> notify();
    future<> stop();

    class consumer;
private:
    future<> sync_state();
    future<> clear_current_operation();
    
    future<std::vector<table_id>> do_build_operation(table_id base_id, dht::token_range range, std::vector<table_id> views, raft::term_t term);
    future<std::vector<table_id>> build_views_range(vb_parameters& params, abort_source& as);
    future<> maybe_abort_current_operation(raft::term_t term);
    future<> register_staging_sstables(table_id base_id, dht::token_range_vector ranges);

    future<> start_staging_detector();
    future<> detect_staging_sstables();

    void init_messaging_service();
    future<> uninit_messaging_service();
};

}
