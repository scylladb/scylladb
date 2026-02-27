/*
 * Copyright 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "node_ops/id.hh"

enum class node_ops_cmd : uint32_t {
     removenode_prepare,
     removenode_heartbeat,
     removenode_sync_data,
     removenode_abort,
     removenode_done,
     replace_prepare,
     replace_prepare_mark_alive,
     replace_prepare_pending_ranges,
     replace_heartbeat,
     replace_abort,
     replace_done,
     decommission_prepare,
     decommission_heartbeat,
     decommission_abort,
     decommission_done,
     bootstrap_prepare,
     bootstrap_heartbeat,
     bootstrap_abort,
     bootstrap_done,
     query_pending_ops,
     repair_updater,
};

class node_ops_id final {
    utils::UUID uuid();
};

struct node_ops_cmd_request {
    node_ops_cmd cmd;
    node_ops_id ops_uuid;
    std::list<gms::inet_address> ignore_nodes;
    std::list<gms::inet_address> leaving_nodes;
    std::unordered_map<gms::inet_address, gms::inet_address> replace_nodes;
    std::unordered_map<gms::inet_address, std::list<dht::token>> bootstrap_nodes;
    std::list<table_id> repair_tables;
};

struct node_ops_cmd_response {
    bool ok;
    std::list<node_ops_id> pending_ops;
};

verb [[with_client_info]] node_ops_cmd(node_ops_cmd_request) -> node_ops_cmd_response
