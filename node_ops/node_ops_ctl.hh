/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "dht/token.hh"
#include "gms/inet_address.hh"
#include "locator/host_id.hh"
#include "node_ops/id.hh"
#include "schema/schema_fwd.hh"
#include "locator/host_id.hh"

#include <seastar/core/abort_source.hh>

#include <list>


class node_ops_info {
public:
    node_ops_id ops_uuid;
    shared_ptr<abort_source> as;
    std::list<locator::host_id> ignore_nodes;

public:
    node_ops_info(node_ops_id ops_uuid_, shared_ptr<abort_source> as_, std::list<locator::host_id>&& ignore_nodes_) noexcept;
    node_ops_info(const node_ops_info&) = delete;
    node_ops_info(node_ops_info&&) = delete;

    void check_abort();
};

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

template <>
struct fmt::formatter<node_ops_cmd> : fmt::formatter<string_view> {
    auto format(node_ops_cmd, fmt::format_context& ctx) const -> decltype(ctx.out());
};

// The cmd and ops_uuid are mandatory for each request.
// The ignore_nodes and leaving_node are optional.
struct node_ops_cmd_request {
    // Mandatory field, set by all cmds
    node_ops_cmd cmd;
    // Mandatory field, set by all cmds
    node_ops_id ops_uuid;
    // Optional field, list nodes to ignore, set by all cmds
    std::list<gms::inet_address> ignore_nodes;
    // Optional field, list leaving nodes, set by decommission and removenode cmd
    std::list<gms::inet_address> leaving_nodes;
    // Optional field, map existing nodes to replacing nodes, set by replace cmd
    std::unordered_map<gms::inet_address, gms::inet_address> replace_nodes;
    // Optional field, map bootstrapping nodes to bootstrap tokens, set by bootstrap cmd
    std::unordered_map<gms::inet_address, std::list<dht::token>> bootstrap_nodes;
    // Optional field, list uuids of tables being repaired, set by repair cmd
    std::list<table_id> repair_tables;
    node_ops_cmd_request(node_ops_cmd command,
            node_ops_id uuid,
            std::list<gms::inet_address> ignore = {},
            std::list<gms::inet_address> leaving = {},
            std::unordered_map<gms::inet_address, gms::inet_address> replace = {},
            std::unordered_map<gms::inet_address, std::list<dht::token>> bootstrap = {},
            std::list<table_id> tables = {})
        : cmd(command)
        , ops_uuid(std::move(uuid))
        , ignore_nodes(std::move(ignore))
        , leaving_nodes(std::move(leaving))
        , replace_nodes(std::move(replace))
        , bootstrap_nodes(std::move(bootstrap))
        , repair_tables(std::move(tables)) {
    }
};

struct node_ops_cmd_response {
    // Mandatory field, set by all cmds
    bool ok;
    // Optional field, set by query_pending_ops cmd
    std::list<node_ops_id> pending_ops;
    node_ops_cmd_response(bool o, std::list<node_ops_id> pending = {})
        : ok(o)
        , pending_ops(std::move(pending)) {
    }
};

template <>
struct fmt::formatter<node_ops_cmd_request> : fmt::formatter<string_view> {
    auto format(const node_ops_cmd_request&, fmt::format_context& ctx) const -> decltype(ctx.out());
};
