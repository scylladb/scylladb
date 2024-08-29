/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "db/config.hh"
#include "gms/gossiper.hh"
#include "message/messaging_service.hh"
#include "node_ops/node_ops_ctl.hh"
#include "service/storage_service.hh"

#include <fmt/ranges.h>
#include <seastar/core/sleep.hh>
#include <seastar/coroutine/parallel_for_each.hh>

static logging::logger nlogger("node_ops");

node_ops_ctl::node_ops_ctl(const service::storage_service& ss_, node_ops_cmd cmd, locator::host_id id, gms::inet_address ep, node_ops_id uuid)
    : ss(ss_)
    , host_id(id)
    , endpoint(ep)
    , tmptr(ss.get_token_metadata_ptr())
    , req(cmd, uuid)
    , heartbeat_interval(ss._db.local().get_config().nodeops_heartbeat_interval_seconds())
{}

node_ops_ctl::~node_ops_ctl() {
    if (heartbeat_updater_done_fut) {
        on_internal_error_noexcept(nlogger, "node_ops_ctl destroyed without stopping");
    }
}

const node_ops_id& node_ops_ctl::uuid() const noexcept {
    return req.ops_uuid;
}

// may be called multiple times
void node_ops_ctl::start(sstring desc_, std::function<bool(gms::inet_address)> sync_to_node) {
    desc = std::move(desc_);

    nlogger.info("{}[{}]: Started {} operation: node={}/{}", desc, uuid(), desc, host_id, endpoint);

    refresh_sync_nodes(std::move(sync_to_node));
}

void node_ops_ctl::refresh_sync_nodes(std::function<bool(gms::inet_address)> sync_to_node) {
    // sync data with all normal token owners
    sync_nodes.clear();
    auto can_sync_with_node = [] (const locator::node& node) {
        // Sync with reachable token owners.
        // Note that although nodes in `being_replaced` and `being_removed`
        // are still token owners, they are known to be dead and can't be sync'ed with.
        switch (node.get_state()) {
        case locator::node::state::normal:
        case locator::node::state::being_decommissioned:
            return true;
        default:
            return false;
        }
    };
    tmptr->for_each_token_owner([&] (const locator::node& node) {
        seastar::thread::maybe_yield();
        // FIXME: use node* rather than endpoint
        auto endpoint = node.endpoint();
        if (!ignore_nodes.contains(endpoint) && can_sync_with_node(node) && sync_to_node(endpoint)) {
            sync_nodes.insert(endpoint);
        }
    });

    for (auto& node : sync_nodes) {
        if (!ss.gossiper().is_alive(node)) {
            nodes_down.emplace(node);
        }
    }
    if (!nodes_down.empty()) {
        auto msg = ::format("{}[{}]: Cannot start: nodes={} needed for {} operation are down. It is highly recommended to fix the down nodes and try again.", desc, uuid(), nodes_down, desc);
        nlogger.warn("{}", msg);
        throw std::runtime_error(msg);
    }

    nlogger.info("{}[{}]: sync_nodes={}, ignore_nodes={}", desc, uuid(), sync_nodes, ignore_nodes);
}

future<> node_ops_ctl::stop() noexcept {
    co_await stop_heartbeat_updater();
}

// Caller should set the required req members before prepare
future<> node_ops_ctl::prepare(node_ops_cmd cmd) noexcept {
    return send_to_all(cmd);
}

void node_ops_ctl::start_heartbeat_updater(node_ops_cmd cmd) {
    if (heartbeat_updater_done_fut) {
        on_internal_error(nlogger, "heartbeat_updater already started");
    }
    heartbeat_updater_done_fut = heartbeat_updater(cmd);
}

future<> node_ops_ctl::query_pending_op() {
    req.cmd = node_ops_cmd::query_pending_ops;
    co_await coroutine::parallel_for_each(sync_nodes, [this] (const gms::inet_address& node) -> future<> {
        auto resp = co_await ss._messaging.local().send_node_ops_cmd(netw::msg_addr(node), req);
        nlogger.debug("{}[{}]: Got query_pending_ops response from node={}, resp.pending_ops={}", desc, uuid(), node, resp.pending_ops);
        if (boost::find(resp.pending_ops, uuid()) == resp.pending_ops.end()) {
            throw std::runtime_error(::format("{}[{}]: Node {} no longer tracks the operation", desc, uuid(), node));
        }
    });
}

future<> node_ops_ctl::stop_heartbeat_updater() noexcept {
    if (heartbeat_updater_done_fut) {
        as.request_abort();
        co_await *std::exchange(heartbeat_updater_done_fut, std::nullopt);
    }
}

future<> node_ops_ctl::done(node_ops_cmd cmd) noexcept {
    co_await stop_heartbeat_updater();
    co_await send_to_all(cmd);
}

future<> node_ops_ctl::abort(node_ops_cmd cmd) noexcept {
    co_await stop_heartbeat_updater();
    co_await send_to_all(cmd);
}

future<> node_ops_ctl::abort_on_error(node_ops_cmd cmd, std::exception_ptr ex) noexcept {
    nlogger.error("{}[{}]: Operation failed, sync_nodes={}: {}", desc, uuid(), sync_nodes, ex);
    try {
        co_await abort(cmd);
    } catch (...) {
        nlogger.warn("{}[{}]: The {} command failed while handling a previous error, sync_nodes={}: {}. Ignoring", desc, uuid(), cmd, sync_nodes, std::current_exception());
    }
    co_await coroutine::return_exception_ptr(std::move(ex));
}

future<> node_ops_ctl::send_to_all(node_ops_cmd cmd) {
    req.cmd = cmd;
    req.ignore_nodes = boost::copy_range<std::list<gms::inet_address>>(ignore_nodes);
    sstring op_desc = ::format("{}", cmd);
    nlogger.info("{}[{}]: Started {}", desc, uuid(), req);
    auto cmd_category = categorize_node_ops_cmd(cmd);
    co_await coroutine::parallel_for_each(sync_nodes, [&] (const gms::inet_address& node) -> future<> {
        if (nodes_unknown_verb.contains(node) || nodes_down.contains(node) ||
                (nodes_failed.contains(node) && (cmd_category != node_ops_cmd_category::abort))) {
            // Note that we still send abort commands to failed nodes.
            co_return;
        }
        try {
            co_await ss._messaging.local().send_node_ops_cmd(netw::msg_addr(node), req);
            nlogger.debug("{}[{}]: Got {} response from node={}", desc, uuid(), op_desc, node);
        } catch (const seastar::rpc::unknown_verb_error&) {
            if (cmd_category == node_ops_cmd_category::prepare) {
                nlogger.warn("{}[{}]: Node {} does not support the {} verb", desc, uuid(), node, op_desc);
            } else {
                nlogger.warn("{}[{}]: Node {} did not find ops_uuid={} or does not support the {} verb", desc, uuid(), node, uuid(), op_desc);
            }
            nodes_unknown_verb.emplace(node);
        } catch (const seastar::rpc::closed_error&) {
            nlogger.warn("{}[{}]: Node {} is down for {} verb", desc, uuid(), node, op_desc);
            nodes_down.emplace(node);
        } catch (...) {
            nlogger.warn("{}[{}]: Node {} failed {} verb: {}", desc, uuid(), node, op_desc, std::current_exception());
            nodes_failed.emplace(node);
        }
    });
    std::vector<sstring> errors;
    if (!nodes_failed.empty()) {
        errors.emplace_back(::format("The {} command failed for nodes={}", op_desc, nodes_failed));
    }
    if (!nodes_unknown_verb.empty()) {
        if (cmd_category == node_ops_cmd_category::prepare) {
            errors.emplace_back(::format("The {} command is unsupported on nodes={}. Please upgrade your cluster and run operation again", op_desc, nodes_unknown_verb));
        } else {
            errors.emplace_back(::format("The ops_uuid={} was not found or the {} command is unsupported on nodes={}", uuid(), op_desc, nodes_unknown_verb));
        }
    }
    if (!nodes_down.empty()) {
        errors.emplace_back(::format("The {} command failed for nodes={}: the needed nodes are down. It is highly recommended to fix the down nodes and try again", op_desc, nodes_down));
    }
    if (!errors.empty()) {
        co_await coroutine::return_exception(std::runtime_error(fmt::to_string(fmt::join(errors, "; "))));
    }
    nlogger.info("{}[{}]: Finished {}", desc, uuid(), req);
}

future<> node_ops_ctl::heartbeat_updater(node_ops_cmd cmd) {
    nlogger.info("{}[{}]: Started heartbeat_updater (interval={}s)", desc, uuid(), heartbeat_interval.count());
    while (!as.abort_requested()) {
        auto req = node_ops_cmd_request{cmd, uuid(), {}, {}, {}};
        co_await coroutine::parallel_for_each(sync_nodes, [&] (const gms::inet_address& node) -> future<> {
            try {
                co_await ss._messaging.local().send_node_ops_cmd(netw::msg_addr(node), req);
                nlogger.debug("{}[{}]: Got heartbeat response from node={}", desc, uuid(), node);
            } catch (...) {
                nlogger.warn("{}[{}]: Failed to get heartbeat response from node={}", desc, uuid(), node);
            };
        });
        co_await sleep_abortable(heartbeat_interval, as).handle_exception([] (std::exception_ptr) {});
    }
    nlogger.info("{}[{}]: Stopped heartbeat_updater", desc, uuid());
}
