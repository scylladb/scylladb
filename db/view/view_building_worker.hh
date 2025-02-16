/*
 * Copyright 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "schema/schema_fwd.hh"
#include "seastar/core/abort_source.hh"
#include "seastar/core/condition-variable.hh"
#include "seastar/core/future.hh"
#include "seastar/core/sharded.hh"
#include "dht/i_partitioner_fwd.hh"

namespace db {
class system_keyspace;
}

namespace replica {
class database;
}

namespace netw {
class messaging_service;
}

namespace service {
class raft_group0_client;
}

using namespace seastar;

namespace db::view {

class view_update_generator;

class view_building_worker : public seastar::peering_sharded_service<view_building_worker> {
    replica::database& _db;
    service::raft_group0_client& _group0_client;
    db::system_keyspace& _sys_ks;
    view_update_generator& _vug;
    sharded<netw::messaging_service>& _messaging;

    abort_source _view_building_as;

    condition_variable _cond;
    abort_source _detector_as;
    future<> _staging_detector_fiber = make_ready_future();

public:
    view_building_worker(replica::database& db, service::raft_group0_client& group0_client, db::system_keyspace& sys_ks, view_update_generator& vug, sharded<netw::messaging_service>& messaging);

    future<> notify();
    future<> stop();

    class consumer;
private:
    future<> build_views_range(table_id base_id, dht::token_range range, std::vector<table_id> views);
    future<> register_staging_sstables(table_id base_id, dht::token_range_vector ranges);

    future<> start_staging_detector();
    future<> detect_staging_sstables();

    void init_messaging_service();
    future<> uninit_messaging_service();
};

}
