/*
 * Copyright 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "schema/schema_fwd.hh"
#include "seastar/core/abort_source.hh"
#include "seastar/core/sharded.hh"
#include "dht/i_partitioner_fwd.hh"

namespace replica {
class database;
}

namespace netw {
class messaging_service;
}

using namespace seastar;

namespace db::view {

class view_update_generator;

class view_building_worker : public seastar::peering_sharded_service<view_building_worker> {
    replica::database& _db;
    view_update_generator& _vug;
    sharded<netw::messaging_service>& _messaging;

    abort_source _as;

public:
    view_building_worker(replica::database& db, view_update_generator& vug, sharded<netw::messaging_service>& messaging);

    future<> stop();

    class consumer;
private:
    future<> build_views_range(table_id base_id, dht::token_range range, std::vector<table_id> views);

    void init_messaging_service();
    future<> uninit_messaging_service();
};

}
