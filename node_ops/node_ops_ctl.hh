/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
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
#include <unordered_set>

namespace service {
class storage_service;
}

namespace locator {
class token_metadata;
}

class node_ops_info {
public:
    node_ops_id ops_uuid;
    shared_ptr<abort_source> as;
    std::list<gms::inet_address> ignore_nodes;

public:
    node_ops_info(node_ops_id ops_uuid_, shared_ptr<abort_source> as_, std::list<gms::inet_address>&& ignore_nodes_) noexcept;
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

enum class node_ops_cmd_category {
    prepare,
    heartbeat,
    sync_data,
    abort,
    done,
    other
};

node_ops_cmd_category categorize_node_ops_cmd(node_ops_cmd cmd) noexcept;

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

class node_ops_ctl {
    std::unordered_set<gms::inet_address> nodes_unknown_verb;
    std::unordered_set<gms::inet_address> nodes_down;
    std::unordered_set<gms::inet_address> nodes_failed;

public:
    const service::storage_service& ss;
    sstring desc;
    locator::host_id host_id;   // Host ID of the node operand (i.e. added, replaced, or leaving node)
    gms::inet_address endpoint;      // IP address of the node operand (i.e. added, replaced, or leaving node)
    lw_shared_ptr<const locator::token_metadata> tmptr;
    std::unordered_set<gms::inet_address> sync_nodes;
    std::unordered_set<gms::inet_address> ignore_nodes;
    node_ops_cmd_request req;
    std::chrono::seconds heartbeat_interval;
    abort_source as;
    std::optional<future<>> heartbeat_updater_done_fut;

    explicit node_ops_ctl(const service::storage_service& ss_, node_ops_cmd cmd, locator::host_id id, gms::inet_address ep, node_ops_id uuid = node_ops_id::create_random_id());
    ~node_ops_ctl();
    const node_ops_id& uuid() const noexcept;
    // may be called multiple times
    void start(sstring desc_, std::function<bool(gms::inet_address)> sync_to_node = [] (gms::inet_address) { return true; });
    void refresh_sync_nodes(std::function<bool(gms::inet_address)> sync_to_node = [] (gms::inet_address) { return true; });
    future<> stop() noexcept;
    // Caller should set the required req members before prepare
    future<> prepare(node_ops_cmd cmd) noexcept;
    void start_heartbeat_updater(node_ops_cmd cmd);
    future<> query_pending_op();
    future<> stop_heartbeat_updater() noexcept;
    future<> done(node_ops_cmd cmd) noexcept;
    future<> abort(node_ops_cmd cmd) noexcept;
    future<> abort_on_error(node_ops_cmd cmd, std::exception_ptr ex) noexcept;
    future<> send_to_all(node_ops_cmd cmd);
    future<> heartbeat_updater(node_ops_cmd cmd);
};

template <>
struct fmt::formatter<node_ops_cmd_request> : fmt::formatter<string_view> {
    auto format(const node_ops_cmd_request&, fmt::format_context& ctx) const -> decltype(ctx.out());
};
