/*
 *
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include "storage_service.hh"
#include "compaction/task_manager_module.hh"
#include "db/system_auth_keyspace.hh"
#include "gc_clock.hh"
#include "raft/raft.hh"
#include "service/topology_guard.hh"
#include "service/session.hh"
#include "dht/boot_strapper.hh"
#include <exception>
#include <optional>
#include <seastar/core/distributed.hh>
#include <seastar/util/defer.hh>
#include <seastar/coroutine/as_future.hh>
#include "gms/endpoint_state.hh"
#include "locator/snitch_base.hh"
#include "db/system_keyspace.hh"
#include "db/system_distributed_keyspace.hh"
#include "db/consistency_level.hh"
#include "seastar/core/when_all.hh"
#include "service/tablet_allocator.hh"
#include "locator/tablets.hh"
#include "locator/tablet_metadata_guard.hh"
#include "replica/tablet_mutation_builder.hh"
#include <seastar/core/smp.hh>
#include "mutation/canonical_mutation.hh"
#include "seastar/core/on_internal_error.hh"
#include "service/raft/group0_state_machine.hh"
#include "service/raft/raft_group0_client.hh"
#include "service/topology_state_machine.hh"
#include "utils/UUID.hh"
#include "gms/inet_address.hh"
#include "log.hh"
#include "service/migration_manager.hh"
#include "service/raft/raft_group0.hh"
#include "gms/gossiper.hh"
#include "gms/feature_service.hh"
#include <seastar/core/thread.hh>
#include <algorithm>
#include "locator/local_strategy.hh"
#include "version.hh"
#include "dht/range_streamer.hh"
#include <boost/range/adaptors.hpp>
#include <boost/range/algorithm.hpp>
#include "transport/server.hh"
#include <seastar/core/rwlock.hh>
#include "db/batchlog_manager.hh"
#include "db/commitlog/commitlog.hh"
#include "db/hints/manager.hh"
#include "utils/exceptions.hh"
#include "message/messaging_service.hh"
#include "supervisor.hh"
#include "compaction/compaction_manager.hh"
#include "sstables/sstables.hh"
#include "db/config.hh"
#include "db/schema_tables.hh"
#include "replica/database.hh"
#include "replica/tablets.hh"
#include <seastar/core/metrics.hh>
#include "cdc/generation.hh"
#include "cdc/generation_service.hh"
#include "repair/repair.hh"
#include "repair/row_level.hh"
#include "gms/generation-number.hh"
#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include <seastar/coroutine/parallel_for_each.hh>
#include <seastar/coroutine/as_future.hh>
#include <seastar/coroutine/exception.hh>
#include "utils/stall_free.hh"
#include "utils/error_injection.hh"
#include "locator/util.hh"
#include "idl/storage_service.dist.hh"
#include "service/storage_proxy.hh"
#include "service/raft/raft_address_map.hh"
#include "service/raft/join_node.hh"
#include "idl/join_node.dist.hh"
#include "protocol_server.hh"
#include "node_ops/node_ops_ctl.hh"
#include "service/topology_mutation.hh"
#include "service/topology_coordinator.hh"
#include "cql3/query_processor.hh"

#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/join.hpp>

using token = dht::token;
using UUID = utils::UUID;
using inet_address = gms::inet_address;

extern logging::logger cdc_log;

namespace service {

static logging::logger slogger("storage_service");

static thread_local session_manager topology_session_manager;

session_manager& get_topology_session_manager() {
    return topology_session_manager;
}

static constexpr std::chrono::seconds wait_for_live_nodes_timeout{30};

storage_service::storage_service(abort_source& abort_source,
    distributed<replica::database>& db, gms::gossiper& gossiper,
    sharded<db::system_keyspace>& sys_ks,
    sharded<db::system_distributed_keyspace>& sys_dist_ks,
    gms::feature_service& feature_service,
    sharded<service::migration_manager>& mm,
    locator::shared_token_metadata& stm,
    locator::effective_replication_map_factory& erm_factory,
    sharded<netw::messaging_service>& ms,
    sharded<repair_service>& repair,
    sharded<streaming::stream_manager>& stream_manager,
    endpoint_lifecycle_notifier& elc_notif,
    sharded<db::batchlog_manager>& bm,
    sharded<locator::snitch_ptr>& snitch,
    sharded<service::tablet_allocator>& tablet_allocator,
    sharded<cdc::generation_service>& cdc_gens,
    cql3::query_processor& qp)
        : _abort_source(abort_source)
        , _feature_service(feature_service)
        , _db(db)
        , _gossiper(gossiper)
        , _messaging(ms)
        , _migration_manager(mm)
        , _qp(qp)
        , _repair(repair)
        , _stream_manager(stream_manager)
        , _snitch(snitch)
        , _group0(nullptr)
        , _node_ops_abort_thread(node_ops_abort_thread())
        , _shared_token_metadata(stm)
        , _erm_factory(erm_factory)
        , _lifecycle_notifier(elc_notif)
        , _batchlog_manager(bm)
        , _sys_ks(sys_ks)
        , _sys_dist_ks(sys_dist_ks)
        , _snitch_reconfigure([this] {
            return container().invoke_on(0, [] (auto& ss) {
                return ss.snitch_reconfigured();
            });
        })
        , _tablet_allocator(tablet_allocator)
        , _cdc_gens(cdc_gens)
{
    register_metrics();

    _listeners.emplace_back(make_lw_shared(bs2::scoped_connection(sstable_read_error.connect([this] { do_isolate_on_error(disk_error::regular); }))));
    _listeners.emplace_back(make_lw_shared(bs2::scoped_connection(sstable_write_error.connect([this] { do_isolate_on_error(disk_error::regular); }))));
    _listeners.emplace_back(make_lw_shared(bs2::scoped_connection(general_disk_error.connect([this] { do_isolate_on_error(disk_error::regular); }))));
    _listeners.emplace_back(make_lw_shared(bs2::scoped_connection(commit_error.connect([this] { do_isolate_on_error(disk_error::commit); }))));

    if (_snitch.local_is_initialized()) {
        _listeners.emplace_back(make_lw_shared(_snitch.local()->when_reconfigured(_snitch_reconfigure)));
    }

    auto& cfg = _db.local().get_config();
    init_messaging_service(cfg.check_experimental(db::experimental_features_t::feature::CONSISTENT_TOPOLOGY_CHANGES));
}

enum class node_external_status {
    UNKNOWN        = 0,
    STARTING       = 1,
    JOINING        = 2,
    NORMAL         = 3,
    LEAVING        = 4,
    DECOMMISSIONED = 5,
    DRAINING       = 6,
    DRAINED        = 7,
    MOVING         = 8, //deprecated
    MAINTENANCE    = 9
};

static node_external_status map_operation_mode(storage_service::mode m) {
    switch (m) {
    case storage_service::mode::NONE: return node_external_status::STARTING;
    case storage_service::mode::STARTING: return node_external_status::STARTING;
    case storage_service::mode::BOOTSTRAP: return node_external_status::JOINING;
    case storage_service::mode::JOINING: return node_external_status::JOINING;
    case storage_service::mode::NORMAL: return node_external_status::NORMAL;
    case storage_service::mode::LEAVING: return node_external_status::LEAVING;
    case storage_service::mode::DECOMMISSIONED: return node_external_status::DECOMMISSIONED;
    case storage_service::mode::DRAINING: return node_external_status::DRAINING;
    case storage_service::mode::DRAINED: return node_external_status::DRAINED;
    case storage_service::mode::MOVING: return node_external_status::MOVING;
    case storage_service::mode::MAINTENANCE: return node_external_status::MAINTENANCE;
    }
    return node_external_status::UNKNOWN;
}

void storage_service::register_metrics() {
    if (this_shard_id() != 0) {
        // the relevant data is distributed between the shards,
        // We only need to register it once.
        return;
    }
    namespace sm = seastar::metrics;
    _metrics.add_group("node", {
            sm::make_gauge("operation_mode", sm::description("The operation mode of the current node. UNKNOWN = 0, STARTING = 1, JOINING = 2, NORMAL = 3, "
                    "LEAVING = 4, DECOMMISSIONED = 5, DRAINING = 6, DRAINED = 7, MOVING = 8, MAINTENANCE = 9"), [this] {
                return static_cast<std::underlying_type_t<node_external_status>>(map_operation_mode(_operation_mode));
            }),
    });
}

bool storage_service::is_replacing() {
    const auto& cfg = _db.local().get_config();
    if (!cfg.replace_node_first_boot().empty()) {
        if (_sys_ks.local().bootstrap_complete()) {
            slogger.info("Replace node on first boot requested; this node is already bootstrapped");
            return false;
        }
        return true;
    }
    if (!cfg.replace_address_first_boot().empty()) {
      if (_sys_ks.local().bootstrap_complete()) {
        slogger.info("Replace address on first boot requested; this node is already bootstrapped");
        return false;
      }
      return true;
    }
    // Returning true if cfg.replace_address is provided
    // will trigger an exception down the road if bootstrap_complete(),
    // as it is an error to use this option post bootstrap.
    // That said, we should just stop supporting it and force users
    // to move to the new, replace_node_first_boot config option.
    return !cfg.replace_address().empty();
}

bool storage_service::is_first_node() {
    if (is_replacing()) {
        return false;
    }
    auto seeds = _gossiper.get_seeds();
    if (seeds.empty()) {
        return false;
    }
    // Node with the smallest IP address is chosen as the very first node
    // in the cluster. The first node is the only node that does not
    // bootstrap in the cluster. All other nodes will bootstrap.
    std::vector<gms::inet_address> sorted_seeds(seeds.begin(), seeds.end());
    std::sort(sorted_seeds.begin(), sorted_seeds.end());
    if (sorted_seeds.front() == get_broadcast_address()) {
        slogger.info("I am the first node in the cluster. Skip bootstrap. Node={}", get_broadcast_address());
        return true;
    }
    return false;
}

bool storage_service::should_bootstrap() {
    return !_sys_ks.local().bootstrap_complete() && !is_first_node();
}

/* Broadcasts the chosen tokens through gossip,
 * together with a CDC generation timestamp and STATUS=NORMAL.
 *
 * Assumes that no other functions modify CDC_GENERATION_ID, TOKENS or STATUS
 * in the gossiper's local application state while this function runs.
 */
static future<> set_gossip_tokens(gms::gossiper& g,
        const std::unordered_set<dht::token>& tokens, std::optional<cdc::generation_id> cdc_gen_id) {
    assert(!tokens.empty());

    // Order is important: both the CDC streams timestamp and tokens must be known when a node handles our status.
    return g.add_local_application_state({
        { gms::application_state::TOKENS, gms::versioned_value::tokens(tokens) },
        { gms::application_state::CDC_GENERATION_ID, gms::versioned_value::cdc_generation_id(cdc_gen_id) },
        { gms::application_state::STATUS, gms::versioned_value::normal(tokens) }
    });
}

static std::unordered_map<token, gms::inet_address> get_token_to_endpoint(const locator::token_metadata& tm) {
    const auto& map = tm.get_token_to_endpoint();
    std::unordered_map<token, gms::inet_address> result;
    result.reserve(map.size());
    for (const auto [t, id]: map) {
        result.insert({t, tm.get_endpoint_for_host_id(id)});
    }
    return result;
}

/*
 * The helper waits for two things
 *  1) for schema agreement
 *  2) there's no pending node operations
 * before proceeding with the bootstrap or replace.
 *
 * This function must only be called if we're not the first node
 * (i.e. booting into existing cluster).
 *
 * Precondition: gossiper observed at least one other live node;
 * see `gossiper::wait_for_live_nodes_to_show_up()`.
 */
future<> storage_service::wait_for_ring_to_settle() {
    auto t = gms::gossiper::clk::now();
    while (true) {
        slogger.info("waiting for schema information to complete");
        while (!_migration_manager.local().have_schema_agreement()) {
            co_await sleep_abortable(std::chrono::milliseconds(10), _abort_source);
        }
        co_await update_topology_change_info("joining");

        auto tmptr = get_token_metadata_ptr();
        if (!_db.local().get_config().consistent_rangemovement() ||
                (tmptr->get_bootstrap_tokens().empty() && tmptr->get_leaving_endpoints().empty())) {
            break;
        }
        auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(gms::gossiper::clk::now() - t).count();
        slogger.info("Checking bootstrapping/leaving nodes: tokens {}, leaving {}, sleep 1 second and check again ({} seconds elapsed)",
                tmptr->get_bootstrap_tokens().size(),
                tmptr->get_leaving_endpoints().size(),
                elapsed);

        if (gms::gossiper::clk::now() > t + std::chrono::seconds(60)) {
            throw std::runtime_error("Other bootstrapping/leaving nodes detected, cannot bootstrap while consistent_rangemovement is true");
        }
        co_await sleep_abortable(std::chrono::seconds(1), _abort_source);
    }
    slogger.info("Checking bootstrapping/leaving nodes: ok");
}

static locator::node::state to_topology_node_state(node_state ns) {
    switch (ns) {
        case node_state::bootstrapping: return locator::node::state::bootstrapping;
        case node_state::decommissioning: return locator::node::state::being_decommissioned;
        case node_state::removing: return locator::node::state::being_removed;
        case node_state::normal: return locator::node::state::normal;
        case node_state::left: return locator::node::state::left;
        case node_state::replacing: return locator::node::state::replacing;
        case node_state::rebuilding: return locator::node::state::normal;
        case node_state::none: return locator::node::state::none;
    }
    on_internal_error(rtlogger, format("unhandled node state: {}", ns));
}

// Synchronizes the local node state (token_metadata, system.peers/system.local tables,
// gossiper) to align it with the other raft topology nodes.
future<> storage_service::sync_raft_topology_nodes(mutable_token_metadata_ptr tmptr, std::optional<locator::host_id> target_node, std::unordered_set<raft::server_id> prev_normal) {
    const auto& am = _group0->address_map();
    const auto& t = _topology_state_machine._topology;

    auto update_topology = [&] (locator::host_id id, std::optional<inet_address> ip, const replica_state& rs) {
        tmptr->update_topology(id, locator::endpoint_dc_rack{rs.datacenter, rs.rack},
                               to_topology_node_state(rs.state), rs.shard_count);
        if (ip) {
            tmptr->update_host_id(id, *ip);
        }
    };

    auto get_used_ips = [&, used_ips = std::optional<std::unordered_set<inet_address>>{}]() mutable
            -> const std::unordered_set<inet_address>&
    {
        if (!used_ips) {
            used_ips.emplace();
            for (const auto& [sid, rs]: boost::range::join(t.normal_nodes, t.transition_nodes)) {
                if (const auto used_ip = am.find(sid)) {
                    used_ips->insert(*used_ip);
                }
            }
        }
        return *used_ips;
    };

    using host_id_to_ip_map_t = std::unordered_map<locator::host_id, gms::inet_address>;
    auto get_host_id_to_ip_map = [&, map = std::optional<host_id_to_ip_map_t>{}]() mutable -> future<const host_id_to_ip_map_t*> {
        if (!map.has_value()) {
            const auto ep_to_id_map = co_await _sys_ks.local().load_host_ids();
            map.emplace();
            map->reserve(ep_to_id_map.size());
            for (const auto& [ep, id]: ep_to_id_map) {
                const auto [it, inserted] = map->insert({id, ep});
                if (!inserted) {
                    on_internal_error(slogger, ::format("duplicate IP for host_id {}, first IP {}, second IP {}",
                        id, it->second, ep));
                }
            }
        }
        co_return &*map;
    };

    std::vector<future<>> sys_ks_futures;

    auto remove_ip = [&](inet_address ip, bool notify) -> future<> {
        sys_ks_futures.push_back(_sys_ks.local().remove_endpoint(ip));

        if (_gossiper.get_endpoint_state_ptr(ip) && !get_used_ips().contains(ip)) {
            co_await _gossiper.force_remove_endpoint(ip, gms::null_permit_id);
            if (notify) {
                co_await notify_left(ip);
            }
        }
    };

    auto process_left_node = [&] (raft::server_id id) -> future<> {
        if (const auto ip = am.find(id)) {
            co_await remove_ip(*ip, true);
        }

        locator::host_id host_id{id.uuid()};
        if (t.left_nodes_rs.find(id) != t.left_nodes_rs.end()) {
            update_topology(host_id, std::nullopt, t.left_nodes_rs.at(id));
        }

        // However if we do that, we need to also implement unbanning a node and do it if `removenode` is aborted.
        co_await _messaging.local().ban_host(locator::host_id{id.uuid()});
    };

    auto process_normal_node = [&] (raft::server_id id, const replica_state& rs) -> future<> {
        locator::host_id host_id{id.uuid()};
        auto ip = am.find(id);

        rtlogger.trace("loading topology: raft id={} ip={} node state={} dc={} rack={} tokens state={} tokens={} shards={}",
                      id, ip, rs.state, rs.datacenter, rs.rack, _topology_state_machine._topology.tstate, rs.ring.value().tokens, rs.shard_count, rs.cleanup);
        // Save tokens, not needed for raft topology management, but needed by legacy
        // Also ip -> id mapping is needed for address map recreation on reboot
        if (is_me(host_id)) {
            sys_ks_futures.push_back(_sys_ks.local().update_tokens(rs.ring.value().tokens));
            co_await _gossiper.add_local_application_state({
                { gms::application_state::TOKENS, gms::versioned_value::tokens(rs.ring.value().tokens) },
                { gms::application_state::CDC_GENERATION_ID, gms::versioned_value::cdc_generation_id(_topology_state_machine._topology.committed_cdc_generations.back()) },
                { gms::application_state::STATUS, gms::versioned_value::normal(rs.ring.value().tokens) }
            });
        } else if (ip && !is_me(*ip)) {
            // In replace-with-same-ip scenario the replaced node IP will be the same
            // as ours, we shouldn't put it into system.peers.

            // We need to fetch host_id_map before the update_peer_info call below,
            // get_host_id_to_ip_map checks for duplicates and update_peer_info can
            // add one.
            const auto& host_id_to_ip_map = *(co_await get_host_id_to_ip_map());

            // Some state that is used to fill in 'peeers' table is still propagated over gossiper.
            // Populate the table with the state from the gossiper here since storage_service::on_change()
            // (which is called each time gossiper state changes) may have skipped it because the tokens
            // for the node were not in the 'normal' state yet
            auto info = get_peer_info_for_update(*ip);
            // And then amend with the info from raft
            info.tokens = rs.ring.value().tokens;
            info.data_center = rs.datacenter;
            info.rack = rs.rack;
            info.release_version = rs.release_version;
            info.supported_features = fmt::to_string(fmt::join(rs.supported_features, ","));
            sys_ks_futures.push_back(_sys_ks.local().update_peer_info(*ip, host_id, info));
            if (!prev_normal.contains(id)) {
                co_await notify_joined(*ip);
            }

            if (const auto it = host_id_to_ip_map.find(host_id); it != host_id_to_ip_map.end() && it->second != *ip) {
                utils::get_local_injector().inject("crash-before-prev-ip-removed", [] {
                    slogger.info("crash-before-prev-ip-removed hit, killing the node");
                    _exit(1);
                });
                // IP change is not expected to emit REMOVED_NODE notifications
                co_await remove_ip(it->second, false);
            }
        }
        update_topology(host_id, ip, rs);
        co_await tmptr->update_normal_tokens(rs.ring.value().tokens, host_id);
    };

    auto process_transition_node = [&](raft::server_id id, const replica_state& rs) -> future<> {
        locator::host_id host_id{id.uuid()};
        auto ip = am.find(id);

        rtlogger.trace("loading topology: raft id={} ip={} node state={} dc={} rack={} tokens state={} tokens={}",
                      id, ip, rs.state, rs.datacenter, rs.rack, _topology_state_machine._topology.tstate,
                      seastar::value_of([&] () -> sstring {
                          return rs.ring ? ::format("{}", rs.ring->tokens) : sstring("null");
                      }));

        switch (rs.state) {
        case node_state::bootstrapping:
            if (rs.ring.has_value()) {
                if (ip && !is_me(*ip)) {
                    // Save ip -> id mapping in peers table because we need it on restart, but do not save tokens until owned
                    sys_ks_futures.push_back(_sys_ks.local().update_peer_info(*ip, host_id, {}));
                }
                update_topology(host_id, ip, rs);
                if (_topology_state_machine._topology.normal_nodes.empty()) {
                    // This is the first node in the cluster. Insert the tokens as normal to the token ring early
                    // so we can perform writes to regular 'distributed' tables during the bootstrap procedure
                    // (such as the CDC generation write).
                    // It doesn't break anything to set the tokens to normal early in this single-node case.
                    co_await tmptr->update_normal_tokens(rs.ring.value().tokens, host_id);
                } else {
                    tmptr->add_bootstrap_tokens(rs.ring.value().tokens, host_id);
                    co_await update_topology_change_info(tmptr, ::format("bootstrapping node {}/{}", id, ip));
                }
            }
            break;
        case node_state::decommissioning:
            // A decommissioning node loses its tokens when topology moves to left_token_ring.
            if (_topology_state_machine._topology.tstate == topology::transition_state::left_token_ring) {
                break;
            }
            [[fallthrough]];
        case node_state::removing:
            if (_topology_state_machine._topology.tstate == topology::transition_state::rollback_to_normal) {
                // no need for double writes anymore since op failed
                co_await process_normal_node(id, rs);
                break;
            }
            update_topology(host_id, ip, rs);
            co_await tmptr->update_normal_tokens(rs.ring.value().tokens, host_id);
            tmptr->add_leaving_endpoint(host_id);
            co_await update_topology_change_info(tmptr, ::format("{} {}/{}", rs.state, id, ip));
            break;
        case node_state::replacing: {
            if (rs.ring.has_value()) {
                assert(_topology_state_machine._topology.req_param.contains(id));
                auto replaced_id = std::get<replace_param>(_topology_state_machine._topology.req_param[id]).replaced_id;
                auto existing_ip = am.find(replaced_id);
                if (!existing_ip) {
                    // FIXME: What if not known?
                    on_fatal_internal_error(rtlogger, ::format("Cannot map id of a node being replaced {} to its ip", replaced_id));
                }
                assert(existing_ip);
                const auto replaced_host_id = locator::host_id(replaced_id.uuid());
                tmptr->update_topology(replaced_host_id, std::nullopt, locator::node::state::being_replaced);
                update_topology(host_id, ip, rs);
                tmptr->add_replacing_endpoint(replaced_host_id, host_id);
                co_await update_topology_change_info(tmptr, ::format("replacing {}/{} by {}/{}", replaced_id, *existing_ip, id, ip));
            }
        }
            break;
        case node_state::rebuilding:
            // Rebuilding node is normal
            co_await process_normal_node(id, rs);
            break;
        default:
            on_fatal_internal_error(rtlogger, ::format("Unexpected state {} for node {}", rs.state, id));
        }
    };

    if (target_node) {
        raft::server_id raft_id{target_node->uuid()};
        if (t.left_nodes.contains(raft_id)) {
            co_await process_left_node(raft_id);
        } else if (auto it = t.normal_nodes.find(raft_id); it != t.normal_nodes.end()) {
            co_await process_normal_node(raft_id, it->second);
        } else if ((it = t.transition_nodes.find(raft_id)) != t.transition_nodes.end()) {
            co_await process_transition_node(raft_id, it->second);
        }
    } else {
        sys_ks_futures.reserve(t.left_nodes.size() + t.normal_nodes.size() + t.transition_nodes.size());
        for (const auto& id: t.left_nodes) {
            co_await process_left_node(id);
        }
        for (const auto& [id, rs]: t.normal_nodes) {
            co_await process_normal_node(id, rs);
        }
        for (const auto& [id, rs]: t.transition_nodes) {
            co_await process_transition_node(id, rs);
        }
        for (auto id : t.get_excluded_nodes()) {
            locator::node* n = tmptr->get_topology().find_node(locator::host_id(id.uuid()));
            if (n) {
                n->set_excluded(true);
            }
        }
    }

    co_await when_all_succeed(sys_ks_futures.begin(), sys_ks_futures.end()).discard_result();
}

future<> storage_service::topology_state_load() {
#ifdef SEASTAR_DEBUG
    static bool running = false;
    assert(!running); // The function is not re-entrant
    auto d = defer([] {
        running = false;
    });
    running = true;
#endif

    if (!_raft_experimental_topology) {
        co_return;
    }

    rtlogger.debug("reload raft topology state");
    std::unordered_set<raft::server_id> prev_normal = boost::copy_range<std::unordered_set<raft::server_id>>(_topology_state_machine._topology.normal_nodes | boost::adaptors::map_keys);

    std::unordered_set<locator::host_id> tablet_hosts;
    if (_db.local().get_config().check_experimental(db::experimental_features_t::feature::TABLETS)) {
        tablet_hosts = co_await replica::read_required_hosts(_qp);
    }

    // read topology state from disk and recreate token_metadata from it
    _topology_state_machine._topology = co_await _sys_ks.local().load_topology_state(tablet_hosts);

    if (_manage_topology_change_kind_from_group0) {
        _topology_change_kind_enabled = upgrade_state_to_topology_op_kind(_topology_state_machine._topology.upgrade_state);
    }
    if (_topology_state_machine._topology.upgrade_state != topology::upgrade_state_type::done) {
        co_return;
    }

    co_await _qp.container().invoke_on_all([] (cql3::query_processor& qp) {
        // auth-v2 gets enabled when consistent topology changes are enabled
        // (see topology::upgrade_state_type::done above) as we use the same migration procedure
        qp.auth_version = db::system_auth_keyspace::version_t::v2;
    });

    co_await _feature_service.container().invoke_on_all([&] (gms::feature_service& fs) {
        return fs.enable(boost::copy_range<std::set<std::string_view>>(_topology_state_machine._topology.enabled_features));
    });

    // Update the legacy `enabled_features` key in `system.scylla_local`.
    // It's OK to update it after enabling features because `system.topology` now
    // is the source of truth about enabled features.
    co_await _sys_ks.local().save_local_enabled_features(_topology_state_machine._topology.enabled_features, false);

    auto saved_tmpr = get_token_metadata_ptr();
    {
        auto tmlock = co_await get_token_metadata_lock();
        auto tmptr = make_token_metadata_ptr(token_metadata::config {
            get_token_metadata().get_topology().get_config()
        });
        tmptr->invalidate_cached_rings();

        tmptr->set_version(_topology_state_machine._topology.version);

        const auto read_new = std::invoke([](std::optional<topology::transition_state> state) {
            using read_new_t = locator::token_metadata::read_new_t;
            if (!state.has_value()) {
                return read_new_t::no;
            }
            switch (*state) {
                case topology::transition_state::join_group0:
                    [[fallthrough]];
                case topology::transition_state::tablet_migration:
                    [[fallthrough]];
                case topology::transition_state::commit_cdc_generation:
                    [[fallthrough]];
                case topology::transition_state::tablet_draining:
                    [[fallthrough]];
                case topology::transition_state::write_both_read_old:
                    [[fallthrough]];
                case topology::transition_state::left_token_ring:
                    [[fallthrough]];
                case topology::transition_state::rollback_to_normal:
                    return read_new_t::no;
                case topology::transition_state::write_both_read_new:
                    return read_new_t::yes;
            }
        }, _topology_state_machine._topology.tstate);
        tmptr->set_read_new(read_new);

        co_await sync_raft_topology_nodes(tmptr, std::nullopt, std::move(prev_normal));

        if (_db.local().get_config().check_experimental(db::experimental_features_t::feature::TABLETS)) {
            tmptr->set_tablets(co_await replica::read_tablet_metadata(_qp));
            tmptr->tablets().set_balancing_enabled(_topology_state_machine._topology.tablet_balancing_enabled);
        }

        co_await replicate_to_all_cores(std::move(tmptr));
    }

    co_await update_fence_version(_topology_state_machine._topology.fence_version);

    // We don't load gossiper endpoint states in storage_service::join_cluster
    // if raft_topology_change_enabled(). On the other hand gossiper is still needed
    // even in case of raft_topology_change_enabled() mode, since it still contains part
    // of the cluster state. To work correctly, the gossiper needs to know the current
    // endpoints. We cannot rely on seeds alone, since it is not guaranteed that seeds
    // will be up to date and reachable at the time of restart.
    const auto tmptr = get_token_metadata_ptr();
    for (const auto& e: tmptr->get_all_endpoints()) {
        if (is_me(e)) {
            continue;
        }
        const auto ep = tmptr->get_endpoint_for_host_id(e);
        if (ep == inet_address{}) {
            continue;
        }
        auto permit = co_await _gossiper.lock_endpoint(ep, gms::null_permit_id);
        // Add the endpoint if it doesn't exist yet in gossip
        // since it is not loaded in join_cluster in the
        // raft_topology_change_enabled() case.
        if (!_gossiper.get_endpoint_state_ptr(ep)) {
            co_await _gossiper.add_saved_endpoint(ep, permit.id());
        }
    }

    // As soon as a node joins token_metadata.topology we
    // need to drop all its rpc connections with ignored_topology flag.
    {
        std::vector<future<>> futures;
        tmptr->get_topology().for_each_node([&](const locator::node* n) {
            const auto ep = n->endpoint();
            if (ep != inet_address{} && !saved_tmpr->get_topology().has_endpoint(ep)) {
                futures.push_back(remove_rpc_client_with_ignored_topology(ep));
            }
        });
        co_await when_all_succeed(futures.begin(), futures.end()).discard_result();
    }

    for (const auto& gen_id : _topology_state_machine._topology.committed_cdc_generations) {
        co_await _cdc_gens.local().handle_cdc_generation(gen_id);
        if (gen_id == _topology_state_machine._topology.committed_cdc_generations.back()) {
            rtlogger.debug("topology_state_load: the last committed CDC generation ID: {}", gen_id);
        }
    }

    for (auto& id : _topology_state_machine._topology.ignored_nodes) {
        // Ban all ignored nodes. We do not allow them to go back online
        co_await _messaging.local().ban_host(locator::host_id{id.uuid()});
    }

    slogger.debug("topology_state_load: excluded nodes: {}", _topology_state_machine._topology.get_excluded_nodes());
}

future<> storage_service::topology_transition() {
    assert(this_shard_id() == 0);
    co_await topology_state_load(); // reload new state

    _topology_state_machine.event.broadcast();
}

future<> storage_service::merge_topology_snapshot(raft_topology_snapshot snp) {
   std::vector<mutation> muts;
   muts.reserve(snp.topology_mutations.size() + snp.cdc_generation_mutations.size() + snp.topology_requests_mutations.size());
   {
       auto s = _db.local().find_schema(db::system_keyspace::NAME, db::system_keyspace::TOPOLOGY);
       boost::transform(snp.topology_mutations, std::back_inserter(muts), [s] (const canonical_mutation& m) {
           return m.to_mutation(s);
       });
   }
   if (snp.cdc_generation_mutations.size() > 0) {
       auto s = _db.local().find_schema(db::system_keyspace::NAME, db::system_keyspace::CDC_GENERATIONS_V3);
       boost::transform(snp.cdc_generation_mutations, std::back_inserter(muts), [s] (const canonical_mutation& m) {
           return m.to_mutation(s);
       });
   }
   if (snp.topology_requests_mutations.size()) {
       auto s = _db.local().find_schema(db::system_keyspace::NAME, db::system_keyspace::TOPOLOGY_REQUESTS);
       boost::transform(snp.topology_requests_mutations, std::back_inserter(muts), [s] (const canonical_mutation& m) {
           return m.to_mutation(s);
       });
   }
   co_await _db.local().apply(freeze(muts), db::no_timeout);
}

// Moves the coroutine lambda onto the heap and extends its
// lifetime until the resulting future is completed.
// This allows to use captures in coroutine lambda after co_await-s.
// Without this helper the coroutine lambda is destroyed immediately after
// the caller (e.g. 'then' function implementation) has invoked it and got the future,
// so referencing the captures after co_await would be use-after-free.
template <typename Coro>
static auto ensure_alive(Coro&& coro) {
    return [coro_ptr = std::make_unique<Coro>(std::move(coro))]<typename ...Args>(Args&&... args) mutable {
        auto& coro = *coro_ptr;
        return coro(std::forward<Args>(args)...).finally([coro_ptr = std::move(coro_ptr)] {});
    };
}

// {{{ raft_ip_address_updater

class storage_service::raft_ip_address_updater: public gms::i_endpoint_state_change_subscriber {
    raft_address_map& _address_map;
    storage_service& _ss;

    future<>
    on_endpoint_change(gms::inet_address endpoint, gms::endpoint_state_ptr ep_state, gms::permit_id permit_id, const char* ev) {
        auto app_state_ptr = ep_state->get_application_state_ptr(gms::application_state::HOST_ID);
        if (!app_state_ptr) {
            co_return;
        }
        raft::server_id id(utils::UUID(app_state_ptr->value()));
        rslog.debug("raft_ip_address_updater::on_endpoint_change({}) {} {}", ev, endpoint, id);

        const auto prev_ip = _address_map.find(id);
        _address_map.add_or_update_entry(id, endpoint, ep_state->get_heart_beat_state().get_generation());
        if (prev_ip == endpoint) {
            co_return;
        }
        if (_address_map.find(id) == prev_ip) {
            // Address map refused to update IP for the host_id,
            // this means prev_ip has higher generation than endpoint.
            // We can immediately remove endpoint from gossiper
            // since it represents an old IP (before an IP change)
            // for the given host_id. This is not strictly
            // necessary, but it reduces the noise circulated
            // in gossiper messages and allows for clearer
            // expectations of the gossiper state in tests.

            co_await _ss._gossiper.force_remove_endpoint(endpoint, permit_id);
            co_return;
        }

        // If the host_id <-> IP mapping has changed, we need to update system tables, token_metadat and erm.
        if (_ss.raft_topology_change_enabled()) {
            rslog.debug("raft_ip_address_updater::on_endpoint_change({}), host_id {}, "
                        "ip changed from [{}] to [{}], "
                        "waiting for group 0 read/apply mutex before reloading Raft topology state...",
                ev, id, prev_ip, endpoint);

            // We're in a gossiper event handler, so gossiper is currently holding a lock
            // for the endpoint parameter of on_endpoint_change.
            // The topology_state_load function can also try to acquire gossiper locks.
            // If we call sync_raft_topology_nodes here directly, a gossiper lock and
            // the _group0.read_apply_mutex could be taken in cross-order leading to a deadlock.
            // To avoid this, we don't wait for sync_raft_topology_nodes to finish.
            (void)futurize_invoke(ensure_alive([this, id, endpoint, h = _ss._async_gate.hold()]() -> future<> {
                auto guard = co_await _ss._group0->client().hold_read_apply_mutex();
                const auto hid = locator::host_id{id.uuid()};
                if (_address_map.find(id) != endpoint ||
                    _ss.get_token_metadata().get_endpoint_for_host_id_if_known(hid) == endpoint)
                {
                    co_return;
                }
                co_await utils::get_local_injector().inject("ip-change-raft-sync-delay", std::chrono::milliseconds(500));
                co_await _ss.mutate_token_metadata([this, hid](mutable_token_metadata_ptr t) {
                    return _ss.sync_raft_topology_nodes(std::move(t), hid, {});
                });
            }));
        }
    }

public:
    raft_ip_address_updater(raft_address_map& address_map, storage_service& ss)
        : _address_map(address_map)
        , _ss(ss)
    {}

    virtual future<>
    on_join(gms::inet_address endpoint, gms::endpoint_state_ptr ep_state, gms::permit_id permit_id) override {
        return on_endpoint_change(endpoint, ep_state, permit_id, "on_join");
    }

    virtual future<>
    on_change(gms::inet_address endpoint, const gms::application_state_map& states, gms::permit_id) override {
        // Raft server ID never changes - do nothing
        return make_ready_future<>();
    }

    virtual future<>
    on_alive(gms::inet_address endpoint, gms::endpoint_state_ptr ep_state, gms::permit_id permit_id) override {
        return on_endpoint_change(endpoint, ep_state, permit_id, "on_alive");
    }

    virtual future<>
    on_dead(gms::inet_address endpoint, gms::endpoint_state_ptr state, gms::permit_id) override {
        return make_ready_future<>();
    }

    virtual future<>
    on_remove(gms::inet_address endpoint, gms::permit_id) override {
        // The mapping is removed when the server is removed from
        // Raft configuration, not when it's dead or alive, or
        // removed
        return make_ready_future<>();
    }

    virtual future<>
    on_restart(gms::inet_address endpoint, gms::endpoint_state_ptr ep_state, gms::permit_id permit_id) override {
        return on_endpoint_change(endpoint, ep_state, permit_id, "on_restart");
    }
};

// }}} raft_ip_address_updater

future<> storage_service::sstable_cleanup_fiber(raft::server& server, sharded<service::storage_proxy>& proxy) noexcept {
    while (!_group0_as.abort_requested()) {
        bool err = false;
        try {
            co_await _topology_state_machine.event.when([&] {
                auto me = _topology_state_machine._topology.find(server.id());
                return me && me->second.cleanup == cleanup_status::running;
            });

            std::vector<future<>> tasks;

            auto do_cleanup_ks = [this, &proxy] (sstring ks_name, std::vector<table_info> table_infos) -> future<> {
                // Wait for all local writes to complete before cleanup
                co_await proxy.invoke_on_all([] (storage_proxy& sp) -> future<> {
                    co_return co_await sp.await_pending_writes();
                });
                auto& compaction_module = _db.local().get_compaction_manager().get_task_manager_module();
                // we flush all tables before cleanup the keyspaces individually, so skip the flush-tables step here
                auto task = co_await compaction_module.make_and_start_task<cleanup_keyspace_compaction_task_impl>(
                    {}, ks_name, _db, table_infos, flush_mode::skip);
                try {
                    co_return co_await task->done();
                } catch (...) {
                    rtlogger.error("cleanup failed keyspace={} tables={} failed: {}", task->get_status().keyspace, table_infos, std::current_exception());
                    throw;
                }
            };

            {
                // The scope for the guard
                auto guard = co_await _group0->client().start_operation(&_group0_as);
                auto me = _topology_state_machine._topology.find(server.id());
                // Recheck that cleanup is needed after the barrier
                if (!me || me->second.cleanup != cleanup_status::running) {
                    rtlogger.trace("cleanup triggered, but not needed");
                    continue;
                }

                rtlogger.info("start cleanup");

                // Skip tablets tables since they do their own cleanup and system tables
                // since they are local and not affected by range movements.
                auto ks_erms = _db.local().get_non_local_strategy_keyspaces_erms();
                tasks.reserve(ks_erms.size());

                co_await _db.invoke_on_all([&] (replica::database& db) {
                    return db.flush_all_tables();
                });
                for (auto [ks_name, erm] : ks_erms) {
                    auto& ks = _db.local().find_keyspace(ks_name);
                    const auto& cf_meta_data = ks.metadata().get()->cf_meta_data();
                    std::vector<table_info> table_infos;
                    table_infos.reserve(cf_meta_data.size());
                    for (const auto& [name, schema] : cf_meta_data) {
                        table_infos.emplace_back(table_info{name, schema->id()});
                    }

                    tasks.push_back(do_cleanup_ks(std::move(ks_name), std::move(table_infos)));
                };
            }

            // Note that the guard is released while we are waiting for cleanup tasks to complete
            co_await when_all_succeed(tasks.begin(), tasks.end()).discard_result();

            rtlogger.info("cleanup ended");

            while (true) {
                auto guard = co_await _group0->client().start_operation(&_group0_as);
                topology_mutation_builder builder(guard.write_timestamp());
                builder.with_node(server.id()).set("cleanup_status", cleanup_status::clean);

                topology_change change{{builder.build()}};
                group0_command g0_cmd = _group0->client().prepare_command(std::move(change), guard, ::format("cleanup completed for {}", server.id()));

                try {
                    co_await _group0->client().add_entry(std::move(g0_cmd), std::move(guard), &_group0_as);
                } catch (group0_concurrent_modification&) {
                    rtlogger.info("cleanup flag clearing: concurrent operation is detected, retrying.");
                    continue;
                }
                break;
            }
            rtlogger.debug("cleanup flag cleared");
        } catch (const seastar::abort_requested_exception &) {
             rtlogger.info("cleanup fiber aborted");
             break;
        } catch (raft::request_aborted&) {
             rtlogger.info("cleanup fiber aborted");
             break;
        } catch (...) {
             rtlogger.error("cleanup fiber got an error: {}", std::current_exception());
             err = true;
        }
        if (err) {
            co_await sleep_abortable(std::chrono::seconds(1), _group0_as);
        }
    }
}

future<> storage_service::raft_state_monitor_fiber(raft::server& raft, sharded<db::system_distributed_keyspace>& sys_dist_ks) {
    std::optional<abort_source> as;

    try {
        while (!_group0_as.abort_requested()) {
            // Wait for a state change in case we are not a leader yet, or we are are the leader
            // and coordinator work is running (in which case 'as' is engaged)
            while (!raft.is_leader() || as) {
                co_await raft.wait_for_state_change(&_group0_as);
                if (as) {
                    as->request_abort(); // we are no longer a leader, so abort the coordinator
                    co_await std::exchange(_topology_change_coordinator, make_ready_future<>());
                    as = std::nullopt;
                    try {
                        _tablet_allocator.local().on_leadership_lost();
                    } catch (...) {
                        rtlogger.error("tablet_allocator::on_leadership_lost() failed: {}", std::current_exception());
                    }
                }
            }
            // We are the leader now but that can change any time!
            as.emplace();
            // start topology change coordinator in the background
            _topology_change_coordinator = run_topology_coordinator(
                    sys_dist_ks, _gossiper, _messaging.local(), _shared_token_metadata,
                    _sys_ks.local(), _db.local(), *_group0, _topology_state_machine, *as, raft,
                    std::bind_front(&storage_service::raft_topology_cmd_handler, this),
                    _tablet_allocator.local(),
                    get_ring_delay(),
                    _lifecycle_notifier);
        }
    } catch (...) {
        rtlogger.info("raft_state_monitor_fiber aborted with {}", std::current_exception());
    }
    if (as) {
        as->request_abort(); // abort current coordinator if running
        co_await std::move(_topology_change_coordinator);
    }
}

std::unordered_set<raft::server_id> storage_service::find_raft_nodes_from_hoeps(const std::list<locator::host_id_or_endpoint>& hoeps) {
    std::unordered_set<raft::server_id> ids;
    for (const auto& hoep : hoeps) {
        std::optional<raft::server_id> id;
        if (hoep.has_host_id()) {
            id = raft::server_id{hoep.id.uuid()};
        } else {
            id = _group0->address_map().find_by_addr(hoep.endpoint);
            if (!id) {
                throw std::runtime_error(::format("Cannot find a mapping to IP {}", hoep.endpoint));
            }
        }
        if (!_topology_state_machine._topology.find(*id)) {
            throw std::runtime_error(::format("Node {} is not found in the cluster", id));
        }
        ids.insert(*id);
    }
    return ids;
}

std::unordered_set<raft::server_id> storage_service::ignored_nodes_from_join_params(const join_node_request_params& params) {
    std::unordered_set<raft::server_id> ignored_nodes;

    if (!params.ignore_nodes.empty()) {
        std::list<locator::host_id_or_endpoint> ignore_nodes_params;
        for (const auto& n : params.ignore_nodes) {
            ignore_nodes_params.emplace_back(n);
        }

        ignored_nodes = find_raft_nodes_from_hoeps(ignore_nodes_params);
    }

    if (params.replaced_id) {
        // insert node that should be replaced to ignore list so that other topology operations
        // can ignore it
        ignored_nodes.insert(*params.replaced_id);
    }

    return ignored_nodes;
}

std::vector<canonical_mutation> storage_service::build_mutation_from_join_params(const join_node_request_params& params, service::group0_guard& guard) {
    topology_mutation_builder builder(guard.write_timestamp());
    auto ignored_nodes = ignored_nodes_from_join_params(params);

    if (!ignored_nodes.empty()) {
        auto bad_id = std::find_if_not(ignored_nodes.begin(), ignored_nodes.end(), [&] (auto n) {
            return _topology_state_machine._topology.normal_nodes.contains(n);
        });
        if (bad_id != ignored_nodes.end()) {
            throw std::runtime_error(::format("replace: there is no node with id {} in normal state. Cannot ignore it.", *bad_id));
        }
        builder.add_ignored_nodes(std::move(ignored_nodes));
    }

    auto& node_builder = builder.with_node(params.host_id)
        .set("node_state", node_state::none)
        .set("datacenter", params.datacenter)
        .set("rack", params.rack)
        .set("release_version", params.release_version)
        .set("num_tokens", params.num_tokens)
        .set("shard_count", params.shard_count)
        .set("ignore_msb", params.ignore_msb)
        .set("cleanup_status", cleanup_status::clean)
        .set("supported_features", boost::copy_range<std::set<sstring>>(params.supported_features));

    if (params.replaced_id) {
        node_builder
            .set("topology_request", topology_request::replace)
            .set("replaced_id", *params.replaced_id);
    } else {
        node_builder
            .set("topology_request", topology_request::join);
    }
    node_builder.set("request_id", params.request_id);
    topology_request_tracking_mutation_builder rtbuilder(params.request_id);
    rtbuilder.set("initiating_host",_group0->group0_server().id().uuid())
             .set("done", false);

    return {builder.build(), rtbuilder.build()};
}

class join_node_rpc_handshaker : public service::group0_handshaker {
private:
    service::storage_service& _ss;
    const join_node_request_params& _req;

public:
    join_node_rpc_handshaker(service::storage_service& ss, const join_node_request_params& req)
            : _ss(ss)
            , _req(req)
    {}

    future<> pre_server_start(const group0_info& g0_info) override {
        rtlogger.info("join: sending the join request to {}", g0_info.ip_addr);

        auto result = co_await ser::join_node_rpc_verbs::send_join_node_request(
                &_ss._messaging.local(), netw::msg_addr(g0_info.ip_addr), g0_info.id, _req);
        std::visit(overloaded_functor {
            [this] (const join_node_request_result::ok&) {
                rtlogger.info("join: request to join placed, waiting"
                             " for the response from the topology coordinator");

                _ss._join_node_request_done.set_value();
            },
            [] (const join_node_request_result::rejected& rej) {
                throw std::runtime_error(
                        format("the topology coordinator rejected request to join the cluster: {}", rej.reason));
            },
        }, result.result);

        co_return;
    }

    future<bool> post_server_start(const group0_info& g0_info, abort_source& as) override {
        // Group 0 has been started. Allow the join_node_response to be handled.
        _ss._join_node_group0_started.set_value();

        // Processing of the response is done in `join_node_response_handler`.
        // Wait for it to complete. If the topology coordinator fails to
        // deliver the rejection, it won't complete. In such a case, the
        // operator is responsible for shutting down the joining node.
        co_await _ss._join_node_response_done.get_shared_future(as);
        rtlogger.info("join: success");
        co_return true;
    }
};

future<> storage_service::raft_initialize_discovery_leader(const join_node_request_params& params) {
    // No other server can become a member of the cluster until
    // we finish adding ourselves. This is ensured by
    // waiting in join_node_request_handler for
    // _topology_state_machine._topology.normal_nodes to be not empty.
    // We cannot mark ourselves as dead during this process.
    // This means there is no valid way we can lose quorum here,
    // but some subsystems may just become unreasonably slow for various reasons,
    // so we nonetheless use raft_timeout{} here.

    if (_topology_state_machine._topology.is_empty()) {
        co_await _group0->group0_server_with_timeouts().read_barrier(&_group0_as, raft_timeout{});
    }

    while (_topology_state_machine._topology.is_empty()) {
        if (params.replaced_id.has_value()) {
            throw std::runtime_error(::format("Cannot perform a replace operation because this is the first node in the cluster"));
        }

        rtlogger.info("adding myself as the first node to the topology");
        auto guard = co_await _group0->client().start_operation(&_group0_as, raft_timeout{});

        auto insert_join_request_mutations = build_mutation_from_join_params(params, guard);

        // We are the first node and we define the cluster.
        // Set the enabled_features field to our features.
        topology_mutation_builder builder(guard.write_timestamp());
        builder.add_enabled_features(boost::copy_range<std::set<sstring>>(params.supported_features))
                .set_upgrade_state(topology::upgrade_state_type::done); // Skip upgrade, start right in the topology-on-raft mode
        auto enable_features_mutation = builder.build();

        insert_join_request_mutations.push_back(std::move(enable_features_mutation));
        topology_change change{std::move(insert_join_request_mutations)};
        group0_command g0_cmd = _group0->client().prepare_command(std::move(change), guard,
                "bootstrap: adding myself as the first node to the topology");
        try {
            co_await _group0->client().add_entry(std::move(g0_cmd), std::move(guard), &_group0_as, raft_timeout{});
        } catch (group0_concurrent_modification&) {
            rtlogger.info("bootstrap: concurrent operation is detected, retrying.");
        }
    }
}

future<> storage_service::update_topology_with_local_metadata(raft::server& raft_server) {
    // TODO: include more metadata here
    auto local_shard_count = smp::count;
    auto local_ignore_msb = _db.local().get_config().murmur3_partitioner_ignore_msb_bits();
    auto local_release_version = version::release();
    auto local_supported_features = boost::copy_range<std::set<sstring>>(_feature_service.supported_feature_set());

    auto synchronized = [&] () {
        auto it = _topology_state_machine._topology.find(raft_server.id());
        if (!it) {
            throw std::runtime_error{"Removed from topology while performing metadata update"};
        }

        auto& replica_state = it->second;

        return replica_state.shard_count == local_shard_count
            && replica_state.ignore_msb == local_ignore_msb
            && replica_state.release_version == local_release_version
            && replica_state.supported_features == local_supported_features;
    };

    // We avoid performing a read barrier if we're sure that our metadata stored in topology
    // is the same as local metadata. Note that only we can update our metadata, other nodes cannot.
    //
    // We use a persisted flag `must_update_topology` to avoid the following scenario:
    // 1. the node restarts and its metadata changes
    // 2. the node commits the new metadata to topology, but before the update is applied
    //    to the local state machine, the node crashes
    // 3. then the metadata changes back to old values and node restarts again
    // 4. the local state machine tells us that we're in sync, which is wrong
    // If the persisted flag is true, it tells us that we attempted a metadata change earlier,
    // forcing us to perform a read barrier even when the local state machine tells us we're in sync.

    if (synchronized() && !(co_await _sys_ks.local().get_must_synchronize_topology())) {
        co_return;
    }

    while (true) {
        rtlogger.info("refreshing topology to check if it's synchronized with local metadata");

        auto guard = co_await _group0->client().start_operation(&_group0_as, raft_timeout{});

        if (synchronized()) {
            break;
        }

        // It might happen that, in the previous run, the node commits a command
        // that adds support for a feature, crashes before applying it and now
        // it is not safe to disable support for it. If there is an attempt to
        // downgrade the node then `enable_features_on_startup` called much
        // earlier won't catch it, we only can do it here after performing
        // a read barrier - so we repeat it here.
        //
        // Fortunately, there is no risk that this feature was marked as enabled
        // because it requires that the current node responded to a barrier
        // request - which will fail in this situation.
        const auto& enabled_features = _topology_state_machine._topology.enabled_features;
        const auto unsafe_to_disable_features = _topology_state_machine._topology.calculate_not_yet_enabled_features();
        _feature_service.check_features(enabled_features, unsafe_to_disable_features);

        rtlogger.info("updating topology with local metadata");

        co_await _sys_ks.local().set_must_synchronize_topology(true);

        topology_mutation_builder builder(guard.write_timestamp());
        builder.with_node(raft_server.id())
               .set("shard_count", local_shard_count)
               .set("ignore_msb", local_ignore_msb)
               .set("release_version", local_release_version)
               .set("supported_features", local_supported_features);
        topology_change change{{builder.build()}};
        group0_command g0_cmd = _group0->client().prepare_command(
                std::move(change), guard, ::format("{}: update topology with local metadata", raft_server.id()));

        try {
            co_await _group0->client().add_entry(std::move(g0_cmd), std::move(guard), &_group0_as, raft_timeout{});
        } catch (group0_concurrent_modification&) {
            rtlogger.info("update topology with local metadata:"
                         " concurrent operation is detected, retrying.");
        }
    }

    co_await _sys_ks.local().set_must_synchronize_topology(false);
}

future<> storage_service::start_upgrade_to_raft_topology() {
    assert(this_shard_id() == 0);

    if (_topology_state_machine._topology.upgrade_state != topology::upgrade_state_type::not_upgraded) {
        co_return;
    }

    if ((co_await _group0->client().get_group0_upgrade_state()).second != group0_upgrade_state::use_post_raft_procedures) {
        throw std::runtime_error(fmt::format("Upgrade to schema-on-raft didn't complete yet. It is a prerequisite for starting "
                "upgrade to raft topology. Refusing to continue. Consult the documentation for more details: {}",
                raft_upgrade_doc));
    }

    if (!_feature_service.supports_consistent_topology_changes) {
        throw std::runtime_error("The SUPPORTS_CONSISTENT_TOPOLOGY_CHANGES feature is not enabled yet. "
                "Not all nodes in the cluster might support topology on raft yet. Make sure that "
                "all nodes in the cluster are upgraded to the same version. Refusing to continue.");
    }

    if (auto unreachable = _gossiper.get_unreachable_token_owners(); !unreachable.empty()) {
        throw std::runtime_error(fmt::format(
            "Nodes {} are seen as down. All nodes must be alive in order to start the upgrade. "
            "Refusing to continue.",
            unreachable));
    }

    while (true) {
        auto guard = co_await _group0->client().start_operation(&_group0_as);

        if (_topology_state_machine._topology.upgrade_state != topology::upgrade_state_type::not_upgraded) {
            co_return;
        }

        rtlogger.info("requesting to start upgrade to topology on raft");
        topology_mutation_builder builder(guard.write_timestamp());
        builder.set_upgrade_state(topology::upgrade_state_type::build_coordinator_state);
        topology_change change{{builder.build()}};
        group0_command g0_cmd = _group0->client().prepare_command(std::move(change), guard, "upgrade: start");

        try {
            co_await _group0->client().add_entry(std::move(g0_cmd), std::move(guard), &_group0_as);
            break;
        } catch (group0_concurrent_modification&) {
            rtlogger.info("upgrade: concurrent operation is detected, retrying.");
            continue;
        }
    };

    rtlogger.info("upgrade to topology on raft is scheduled");
    co_return;
}

topology::upgrade_state_type storage_service::get_topology_upgrade_state() const {
    assert(this_shard_id() == 0);
    return _topology_state_machine._topology.upgrade_state;
}

future<> storage_service::await_tablets_rebuilt(raft::server_id replaced_id) {
    auto is_drained = [&] {
        return !get_token_metadata().tablets().has_replica_on(locator::host_id(replaced_id.uuid()));
    };
    if (!is_drained()) {
        slogger.info("Waiting for tablet replicas from the replaced node to be rebuilt");
        co_await _topology_state_machine.event.wait([&] {
            return is_drained();
        });
    }
    slogger.info("Tablet replicas from the replaced node have been rebuilt");
}

future<> storage_service::join_token_ring(sharded<db::system_distributed_keyspace>& sys_dist_ks,
        sharded<service::storage_proxy>& proxy,
        sharded<gms::gossiper>& gossiper,
        std::unordered_set<gms::inet_address> initial_contact_nodes,
        std::unordered_set<gms::inet_address> loaded_endpoints,
        std::unordered_map<gms::inet_address, sstring> loaded_peer_features,
        std::chrono::milliseconds delay,
        start_hint_manager start_hm,
        gms::generation_type new_generation) {
    std::unordered_set<token> bootstrap_tokens;
    gms::application_state_map app_states;
    /* The timestamp of the CDC streams generation that this node has proposed when joining.
     * This value is nullopt only when:
     * 1. this node is being upgraded from a non-CDC version,
     * 2. this node is starting for the first time or restarting with CDC previously disabled,
     *    in which case the value should become populated before we leave the join_token_ring procedure.
     *
     * Important: this variable is using only during the startup procedure. It is moved out from
     * at the end of `join_token_ring`; the responsibility handling of CDC generations is passed
     * to cdc::generation_service.
     *
     * DO NOT use this variable after `join_token_ring` (i.e. after we call `generation_service::after_join`
     * and pass it the ownership of the timestamp.
     */
    std::optional<cdc::generation_id> cdc_gen_id;

    bool replacing_a_node_with_same_ip = false;
    bool replacing_a_node_with_diff_ip = false;
    std::optional<replacement_info> ri;
    std::optional<gms::inet_address> replace_address;
    std::optional<locator::host_id> replaced_host_id;
    std::optional<raft_group0::replace_info> raft_replace_info;
    auto tmlock = std::make_unique<token_metadata_lock>(co_await get_token_metadata_lock());
    auto tmptr = co_await get_mutable_token_metadata_ptr();
    if (is_replacing()) {
        if (_sys_ks.local().bootstrap_complete()) {
            throw std::runtime_error("Cannot replace address with a node that is already bootstrapped");
        }
        ri = co_await prepare_replacement_info(initial_contact_nodes, loaded_peer_features);
        replace_address = ri->address;
        raft_replace_info = raft_group0::replace_info {
            .ip_addr = *replace_address,
            .raft_id = raft::server_id{ri->host_id.uuid()},
        };
        replacing_a_node_with_same_ip = *replace_address == get_broadcast_address();
        replacing_a_node_with_diff_ip = *replace_address != get_broadcast_address();
        if (!raft_topology_change_enabled()) {
            bootstrap_tokens = std::move(ri->tokens);

            slogger.info("Replacing a node with {} IP address, my address={}, node being replaced={}",
                get_broadcast_address() == *replace_address ? "the same" : "a different",
                get_broadcast_address(), *replace_address);
            tmptr->update_topology(tmptr->get_my_id(), std::nullopt, locator::node::state::replacing);
            tmptr->update_topology(ri->host_id, std::move(ri->dc_rack), locator::node::state::being_replaced);
            co_await tmptr->update_normal_tokens(bootstrap_tokens, ri->host_id);
            tmptr->update_host_id(ri->host_id, *replace_address);

            replaced_host_id = ri->host_id;
        }
    } else if (should_bootstrap()) {
        co_await check_for_endpoint_collision(initial_contact_nodes, loaded_peer_features);
    } else {
        auto local_features = _feature_service.supported_feature_set();
        slogger.info("Performing gossip shadow round, initial_contact_nodes={}", initial_contact_nodes);
        co_await _gossiper.do_shadow_round(initial_contact_nodes, gms::gossiper::mandatory::no);
        if (!raft_topology_change_enabled()) {
            _gossiper.check_knows_remote_features(local_features, loaded_peer_features);
        }
        _gossiper.check_snitch_name_matches(_snitch.local()->get_name());
        // Check if the node is already removed from the cluster
        auto local_host_id = get_token_metadata().get_my_id();
        auto my_ip = get_broadcast_address();
        if (!_gossiper.is_safe_for_restart(my_ip, local_host_id)) {
            throw std::runtime_error(::format("The node {} with host_id {} is removed from the cluster. Can not restart the removed node to join the cluster again!",
                    my_ip, local_host_id));
        }
        co_await _gossiper.reset_endpoint_state_map();
        for (auto ep : loaded_endpoints) {
            // gossiping hasn't started yet
            // so no need to lock the endpoint
            co_await _gossiper.add_saved_endpoint(ep, gms::null_permit_id);
        }
    }
    auto features = _feature_service.supported_feature_set();
    slogger.info("Save advertised features list in the 'system.{}' table", db::system_keyspace::LOCAL);
    // Save the advertised feature set to system.local table after
    // all remote feature checks are complete and after gossip shadow rounds are done.
    // At this point, the final feature set is already determined before the node joins the ring.
    co_await _sys_ks.local().save_local_supported_features(features);

    // If this is a restarting node, we should update tokens before gossip starts
    auto my_tokens = co_await _sys_ks.local().get_saved_tokens();
    bool restarting_normal_node = _sys_ks.local().bootstrap_complete() && !is_replacing() && !my_tokens.empty();
    if (restarting_normal_node) {
        slogger.info("Restarting a node in NORMAL status");
        // This node must know about its chosen tokens before other nodes do
        // since they may start sending writes to this node after it gossips status = NORMAL.
        // Therefore we update _token_metadata now, before gossip starts.
        tmptr->update_topology(tmptr->get_my_id(), _snitch.local()->get_location(), locator::node::state::normal);
        co_await tmptr->update_normal_tokens(my_tokens, tmptr->get_my_id());

        cdc_gen_id = co_await _sys_ks.local().get_cdc_generation_id();
        if (!cdc_gen_id) {
            // We could not have completed joining if we didn't generate and persist a CDC streams timestamp,
            // unless we are restarting after upgrading from non-CDC supported version.
            // In that case we won't begin a CDC generation: it should be done by one of the nodes
            // after it learns that it everyone supports the CDC feature.
            cdc_log.warn(
                    "Restarting node in NORMAL status with CDC enabled, but no streams timestamp was proposed"
                    " by this node according to its local tables. Are we upgrading from a non-CDC supported version?");
        }
    }

    // have to start the gossip service before we can see any info on other nodes.  this is necessary
    // for bootstrap to get the load info it needs.
    // (we won't be part of the storage ring though until we add a counterId to our state, below.)
    // Seed the host ID-to-endpoint map with our own ID.
    auto local_host_id = get_token_metadata().get_my_id();
    if (!replacing_a_node_with_diff_ip) {
        auto endpoint = get_broadcast_address();
        auto eps = _gossiper.get_endpoint_state_ptr(endpoint);
        if (eps) {
            auto replace_host_id = _gossiper.get_host_id(get_broadcast_address());
            slogger.info("Host {}/{} is replacing {}/{} using the same address", local_host_id, endpoint, replace_host_id, endpoint);
        }
        tmptr->update_host_id(local_host_id, get_broadcast_address());
    }

    // Replicate the tokens early because once gossip runs other nodes
    // might send reads/writes to this node. Replicate it early to make
    // sure the tokens are valid on all the shards.
    co_await replicate_to_all_cores(std::move(tmptr));
    tmlock.reset();

    auto broadcast_rpc_address = get_token_metadata_ptr()->get_topology().my_cql_address();
    // Ensure we know our own actual Schema UUID in preparation for updates
    co_await db::schema_tables::recalculate_schema_version(_sys_ks, proxy, _feature_service);

    app_states.emplace(gms::application_state::NET_VERSION, versioned_value::network_version());
    app_states.emplace(gms::application_state::HOST_ID, versioned_value::host_id(local_host_id));
    app_states.emplace(gms::application_state::RPC_ADDRESS, versioned_value::rpcaddress(broadcast_rpc_address));
    app_states.emplace(gms::application_state::RELEASE_VERSION, versioned_value::release_version());
    app_states.emplace(gms::application_state::SUPPORTED_FEATURES, versioned_value::supported_features(features));
    app_states.emplace(gms::application_state::CACHE_HITRATES, versioned_value::cache_hitrates(""));
    app_states.emplace(gms::application_state::SCHEMA_TABLES_VERSION, versioned_value(db::schema_tables::version));
    app_states.emplace(gms::application_state::RPC_READY, versioned_value::cql_ready(false));
    app_states.emplace(gms::application_state::VIEW_BACKLOG, versioned_value(""));
    app_states.emplace(gms::application_state::SCHEMA, versioned_value::schema(_db.local().get_version()));
    if (restarting_normal_node) {
        // Order is important: both the CDC streams timestamp and tokens must be known when a node handles our status.
        // Exception: there might be no CDC streams timestamp proposed by us if we're upgrading from a non-CDC version.
        app_states.emplace(gms::application_state::TOKENS, versioned_value::tokens(my_tokens));
        app_states.emplace(gms::application_state::CDC_GENERATION_ID, versioned_value::cdc_generation_id(cdc_gen_id));
        app_states.emplace(gms::application_state::STATUS, versioned_value::normal(my_tokens));
    }
    if (!raft_topology_change_enabled() && (replacing_a_node_with_same_ip || replacing_a_node_with_diff_ip)) {
        app_states.emplace(gms::application_state::TOKENS, versioned_value::tokens(bootstrap_tokens));
    }
    app_states.emplace(gms::application_state::SNITCH_NAME, versioned_value::snitch_name(_snitch.local()->get_name()));
    app_states.emplace(gms::application_state::SHARD_COUNT, versioned_value::shard_count(smp::count));
    app_states.emplace(gms::application_state::IGNORE_MSB_BITS, versioned_value::ignore_msb_bits(_db.local().get_config().murmur3_partitioner_ignore_msb_bits()));

    for (auto&& s : _snitch.local()->get_app_states()) {
        app_states.emplace(s.first, std::move(s.second));
    }

    auto schema_change_announce = _db.local().observable_schema_version().observe([this] (table_schema_version schema_version) mutable {
        _migration_manager.local().passive_announce(std::move(schema_version));
    });

    _listeners.emplace_back(make_lw_shared(std::move(schema_change_announce)));

    slogger.info("Starting up server gossip");

    auto advertise = gms::advertise_myself(!replacing_a_node_with_same_ip);
    co_await _gossiper.start_gossiping(new_generation, app_states, advertise);

    if (!raft_topology_change_enabled() && should_bootstrap()) {
        // Wait for NORMAL state handlers to finish for existing nodes now, so that connection dropping
        // (happening at the end of `handle_state_normal`: `notify_joined`) doesn't interrupt
        // group 0 joining or repair. (See #12764, #12956, #12972, #13302)
        //
        // But before we can do that, we must make sure that gossip sees at least one other node
        // and fetches the list of peers from it; otherwise `wait_for_normal_state_handled_on_boot`
        // may trivially finish without waiting for anyone.
        co_await _gossiper.wait_for_live_nodes_to_show_up(2);

        // Note: in Raft topology mode this is unnecessary.
        // Node state changes are propagated to the cluster through explicit global barriers.
        co_await wait_for_normal_state_handled_on_boot();

        // NORMAL doesn't necessarily mean UP (#14042). Wait for these nodes to be UP as well
        // to reduce flakiness (we need them to be UP to perform CDC generation write and for repair/streaming).
        //
        // This could be done in Raft topology mode as well, but the calculation of nodes to sync with
        // has to be done based on topology state machine instead of gossiper as it is here;
        // furthermore, the place in the code where we do this has to be different (it has to be coordinated
        // by the topology coordinator after it joins the node to the cluster).
        //
        // We calculate nodes to wait for based on token_metadata. Previously we would use gossiper
        // directly for this, but gossiper may still contain obsolete entries from 1. replaced nodes
        // and 2. nodes that have changed their IPs; these entries are eventually garbage-collected,
        // but here they may still be present if we're performing topology changes in quick succession.
        // `token_metadata` has all host ID / token collisions resolved so in particular it doesn't contain
        // these obsolete IPs. Refs: #14487, #14468
        auto& tm = get_token_metadata();
        auto ignore_nodes = ri
                ? parse_node_list(_db.local().get_config().ignore_dead_nodes_for_replace(), tm)
                // TODO: specify ignore_nodes for bootstrap
                : std::unordered_set<gms::inet_address>{};

        std::vector<gms::inet_address> sync_nodes;
        tm.get_topology().for_each_node([&] (const locator::node* np) {
            auto ep = np->endpoint();
            if (!ignore_nodes.contains(ep) && (!ri || ep != ri->address)) {
                sync_nodes.push_back(ep);
            }
        });

        slogger.info("Waiting for nodes {} to be alive", sync_nodes);
        co_await _gossiper.wait_alive(sync_nodes, wait_for_live_nodes_timeout);
        slogger.info("Nodes {} are alive", sync_nodes);
    }

    assert(_group0);

    join_node_request_params join_params {
        .host_id = _group0->load_my_id(),
        .cluster_name = _db.local().get_config().cluster_name(),
        .snitch_name = _db.local().get_snitch_name(),
        .datacenter = _snitch.local()->get_datacenter(),
        .rack = _snitch.local()->get_rack(),
        .release_version = version::release(),
        .num_tokens = _db.local().get_config().num_tokens(),
        .shard_count = smp::count,
        .ignore_msb =  _db.local().get_config().murmur3_partitioner_ignore_msb_bits(),
        .supported_features = boost::copy_range<std::vector<sstring>>(_feature_service.supported_feature_set()),
        .request_id = utils::UUID_gen::get_time_UUID(),
    };

    if (raft_replace_info) {
        join_params.replaced_id = raft_replace_info->raft_id;
        join_params.ignore_nodes = utils::split_comma_separated_list(_db.local().get_config().ignore_dead_nodes_for_replace());
    }

    // if the node is bootstrapped the function will do nothing since we already created group0 in main.cc
    ::shared_ptr<group0_handshaker> handshaker = raft_topology_change_enabled()
            ? ::make_shared<join_node_rpc_handshaker>(*this, join_params)
            : _group0->make_legacy_handshaker(false);
    co_await _group0->setup_group0(_sys_ks.local(), initial_contact_nodes, std::move(handshaker),
            raft_replace_info, *this, _qp, _migration_manager.local(), raft_topology_change_enabled());

    raft::server* raft_server = co_await [this] () -> future<raft::server*> {
        if (!raft_topology_change_enabled()) {
            co_return nullptr;
        } else if (_sys_ks.local().bootstrap_complete()) {
            auto [upgrade_lock_holder, upgrade_state] = co_await _group0->client().get_group0_upgrade_state();
            co_return upgrade_state == group0_upgrade_state::use_post_raft_procedures ? &_group0->group0_server() : nullptr;
        } else {
            auto upgrade_state = (co_await _group0->client().get_group0_upgrade_state()).second;
            if (upgrade_state != group0_upgrade_state::use_post_raft_procedures) {
                on_internal_error(rtlogger, "cluster not upgraded to use group 0 after setup_group0");
            }
            co_return &_group0->group0_server();
        }
    } ();

    co_await _gossiper.wait_for_gossip_to_settle();

    // This is the moment when the locator::topology has gathered information about other nodes
    // in the cluster -- either through gossiper, or by loading it from disk -- so it's safe
    // to start the hint managers.
    if (start_hm) {
        co_await proxy.invoke_on_all([&gossiper] (storage_proxy& local_proxy) {
            return local_proxy.start_hints_manager(gossiper.local().shared_from_this());
        });
    }
    
    if (!raft_topology_change_enabled()) {
        co_await _feature_service.enable_features_on_join(_gossiper, _sys_ks.local(), *this);
    }

    set_mode(mode::JOINING);

    if (raft_server) { // Raft is enabled. Check if we need to bootstrap ourself using raft
        rtlogger.info("topology changes are using raft");

        // Nodes that are not discovery leaders have their join request inserted
        // on their behalf by an existing node in the cluster during the handshake.
        // Discovery leaders on the other need to insert the join request themselves,
        // we do that here.
        co_await raft_initialize_discovery_leader(join_params);

        // start topology coordinator fiber
        _raft_state_monitor = raft_state_monitor_fiber(*raft_server, sys_dist_ks);
        // start cleanup fiber
        _sstable_cleanup_fiber = sstable_cleanup_fiber(*raft_server, proxy);

        // Need to start system_distributed_keyspace before bootstrap because bootstrapping
        // process may access those tables.
        supervisor::notify("starting system distributed keyspace");
        co_await sys_dist_ks.invoke_on_all(&db::system_distributed_keyspace::start);

        if (_sys_ks.local().bootstrap_complete()) {
            if (_topology_state_machine._topology.left_nodes.contains(raft_server->id())) {
                throw std::runtime_error("A node that already left the cluster cannot be restarted");
            }
        } else {
            auto err = co_await wait_for_topology_request_completion(join_params.request_id);
            if (!err.empty()) {
                throw std::runtime_error(fmt::format("{} failed. See earlier errors ({})", raft_replace_info ? "Replace" : "Bootstrap", err));
            }

            if (raft_replace_info) {
                co_await await_tablets_rebuilt(raft_replace_info->raft_id);
            }
        }

        // If we were the first node in the cluster, at this point `upgrade_state` will be
        // initialized properly. Yield control to group 0
        _manage_topology_change_kind_from_group0 = true;
        _topology_change_kind_enabled = upgrade_state_to_topology_op_kind(_topology_state_machine._topology.upgrade_state);

        co_await update_topology_with_local_metadata(*raft_server);

        // Node state is enough to know that bootstrap has completed, but to make legacy code happy
        // let it know that the bootstrap is completed as well
        co_await _sys_ks.local().set_bootstrap_state(db::system_keyspace::bootstrap_state::COMPLETED);
        set_mode(mode::NORMAL);

        if (get_token_metadata().sorted_tokens().empty()) {
            auto err = ::format("join_token_ring: Sorted token in token_metadata is empty");
            slogger.error("{}", err);
            throw std::runtime_error(err);
        }

        co_await _group0->finish_setup_after_join(*this, _qp, _migration_manager.local(), _raft_experimental_topology);

        // Initializes monitor only after updating local topology.
        start_tablet_split_monitor();

        co_return;
    }

    _manage_topology_change_kind_from_group0 = true;
    _topology_change_kind_enabled = upgrade_state_to_topology_op_kind(_topology_state_machine._topology.upgrade_state);

    // We bootstrap if we haven't successfully bootstrapped before, as long as we are not a seed.
    // If we are a seed, or if the user manually sets auto_bootstrap to false,
    // we'll skip streaming data from other nodes and jump directly into the ring.
    //
    // The seed check allows us to skip the RING_DELAY sleep for the single-node cluster case,
    // which is useful for both new users and testing.
    //
    // We attempted to replace this with a schema-presence check, but you need a meaningful sleep
    // to get schema info from gossip which defeats the purpose.  See CASSANDRA-4427 for the gory details.
    if (should_bootstrap()) {
        bool resume_bootstrap = _sys_ks.local().bootstrap_in_progress();
        if (resume_bootstrap) {
            slogger.warn("Detected previous bootstrap failure; retrying");
        } else {
            co_await _sys_ks.local().set_bootstrap_state(db::system_keyspace::bootstrap_state::IN_PROGRESS);
        }
        slogger.info("waiting for ring information");

        // if our schema hasn't matched yet, keep sleeping until it does
        // (post CASSANDRA-1391 we don't expect this to be necessary very often, but it doesn't hurt to be careful)
        co_await wait_for_ring_to_settle();

        if (!replace_address) {
            auto tmptr = get_token_metadata_ptr();

            if (tmptr->is_normal_token_owner(tmptr->get_my_id())) {
                throw std::runtime_error("This node is already a member of the token ring; bootstrap aborted. (If replacing a dead node, remove the old one from the ring first.)");
            }
            slogger.info("getting bootstrap token");
            if (resume_bootstrap) {
                bootstrap_tokens = co_await _sys_ks.local().get_saved_tokens();
                if (!bootstrap_tokens.empty()) {
                    slogger.info("Using previously saved tokens = {}", bootstrap_tokens);
                } else {
                    bootstrap_tokens = boot_strapper::get_bootstrap_tokens(tmptr, _db.local().get_config(), dht::check_token_endpoint::yes);
                }
            } else {
                bootstrap_tokens = boot_strapper::get_bootstrap_tokens(tmptr, _db.local().get_config(), dht::check_token_endpoint::yes);
            }
        } else {
            if (*replace_address != get_broadcast_address()) {
                // Sleep additionally to make sure that the server actually is not alive
                // and giving it more time to gossip if alive.
                slogger.info("Sleeping before replacing {}...", *replace_address);
                co_await sleep_abortable(2 * get_ring_delay(), _abort_source);

                // check for operator errors...
                const auto tmptr = get_token_metadata_ptr();
                for (auto token : bootstrap_tokens) {
                    auto existing = tmptr->get_endpoint(token);
                    if (existing) {
                        auto eps = _gossiper.get_endpoint_state_ptr(tmptr->get_endpoint_for_host_id(*existing));
                        if (eps && eps->get_update_timestamp() > gms::gossiper::clk::now() - delay) {
                            throw std::runtime_error("Cannot replace a live node...");
                        }
                    } else {
                        throw std::runtime_error(::format("Cannot replace token {} which does not exist!", token));
                    }
                }
            } else {
                slogger.info("Sleeping before replacing {}...", *replace_address);
                co_await sleep_abortable(get_ring_delay(), _abort_source);
            }
            slogger.info("Replacing a node with token(s): {}", bootstrap_tokens);
            // bootstrap_tokens was previously set using tokens gossiped by the replaced node
        }
        co_await sys_dist_ks.invoke_on_all(&db::system_distributed_keyspace::start);
        co_await mark_existing_views_as_built();
        co_await _sys_ks.local().update_tokens(bootstrap_tokens);
        co_await bootstrap(bootstrap_tokens, cdc_gen_id, ri);
    } else {
        supervisor::notify("starting system distributed keyspace");
        co_await sys_dist_ks.invoke_on_all(&db::system_distributed_keyspace::start);
        bootstrap_tokens = co_await _sys_ks.local().get_saved_tokens();
        if (bootstrap_tokens.empty()) {
            bootstrap_tokens = boot_strapper::get_bootstrap_tokens(get_token_metadata_ptr(), _db.local().get_config(), dht::check_token_endpoint::no);
            co_await _sys_ks.local().update_tokens(bootstrap_tokens);
        } else {
            size_t num_tokens = _db.local().get_config().num_tokens();
            if (bootstrap_tokens.size() != num_tokens) {
                throw std::runtime_error(::format("Cannot change the number of tokens from {:d} to {:d}", bootstrap_tokens.size(), num_tokens));
            } else {
                slogger.info("Using saved tokens {}", bootstrap_tokens);
            }
        }
    }

    slogger.debug("Setting tokens to {}", bootstrap_tokens);
    co_await mutate_token_metadata([this, &bootstrap_tokens] (mutable_token_metadata_ptr tmptr) -> future<> {
        // This node must know about its chosen tokens before other nodes do
        // since they may start sending writes to this node after it gossips status = NORMAL.
        // Therefore, in case we haven't updated _token_metadata with our tokens yet, do it now.
        tmptr->update_topology(tmptr->get_my_id(), _snitch.local()->get_location(), locator::node::state::normal);
        co_await tmptr->update_normal_tokens(bootstrap_tokens, tmptr->get_my_id());
    });

    if (!_sys_ks.local().bootstrap_complete()) {
        // If we're not bootstrapping then we shouldn't have chosen a CDC streams timestamp yet.
        assert(should_bootstrap() || !cdc_gen_id);

        // Don't try rewriting CDC stream description tables.
        // See cdc.md design notes, `Streams description table V1 and rewriting` section, for explanation.
        co_await _sys_ks.local().cdc_set_rewritten(std::nullopt);
    }

    if (!cdc_gen_id) {
        // If we didn't observe any CDC generation at this point, then either
        // 1. we're replacing a node,
        // 2. we've already bootstrapped, but are upgrading from a non-CDC version,
        // 3. we're the first node, starting a fresh cluster.

        // In the replacing case we won't create any CDC generation: we're not introducing any new tokens,
        // so the current generation used by the cluster is fine.

        // In the case of an upgrading cluster, one of the nodes is responsible for creating
        // the first CDC generation. We'll check if it's us.

        // Finally, if we're the first node, we'll create the first generation.

        if (!is_replacing()
                && (!_sys_ks.local().bootstrap_complete()
                    || cdc::should_propose_first_generation(get_broadcast_address(), _gossiper))) {
            try {
                cdc_gen_id = co_await _cdc_gens.local().legacy_make_new_generation(bootstrap_tokens, !is_first_node());
            } catch (...) {
                cdc_log.warn(
                    "Could not create a new CDC generation: {}. This may make it impossible to use CDC or cause performance problems."
                    " Use nodetool checkAndRepairCdcStreams to fix CDC.", std::current_exception());
            }
        }
    }

    // Persist the CDC streams timestamp before we persist bootstrap_state = COMPLETED.
    if (cdc_gen_id) {
        co_await _sys_ks.local().update_cdc_generation_id(*cdc_gen_id);
    }
    // If we crash now, we will choose a new CDC streams timestamp anyway (because we will also choose a new set of tokens).
    // But if we crash after setting bootstrap_state = COMPLETED, we will keep using the persisted CDC streams timestamp after restarting.

    co_await _sys_ks.local().set_bootstrap_state(db::system_keyspace::bootstrap_state::COMPLETED);
    // At this point our local tokens and CDC streams timestamp are chosen (bootstrap_tokens, cdc_gen_id) and will not be changed.

    // start participating in the ring.
    co_await set_gossip_tokens(_gossiper, bootstrap_tokens, cdc_gen_id);

    set_mode(mode::NORMAL);

    if (get_token_metadata().sorted_tokens().empty()) {
        auto err = ::format("join_token_ring: Sorted token in token_metadata is empty");
        slogger.error("{}", err);
        throw std::runtime_error(err);
    }

    assert(_group0);
    co_await _group0->finish_setup_after_join(*this, _qp, _migration_manager.local(), _raft_experimental_topology);
    co_await _cdc_gens.local().after_join(std::move(cdc_gen_id));

    if (_raft_experimental_topology) {
        // Waited on during stop()
        (void)([] (storage_service& me, sharded<db::system_distributed_keyspace>& sys_dist_ks, sharded<service::storage_proxy>& proxy) -> future<> {
            try {
                co_await me.track_upgrade_progress_to_topology_coordinator(sys_dist_ks, proxy);
            } catch (const abort_requested_exception&) {
                // Ignore
            }
            // Other errors are handled internally by track_upgrade_progress_to_topology_coordinator
        })(*this, sys_dist_ks, proxy);
    }
}

future<> storage_service::track_upgrade_progress_to_topology_coordinator(sharded<db::system_distributed_keyspace>& sys_dist_ks, sharded<service::storage_proxy>& proxy) {
    assert(_group0);

    while (true) {
        _group0_as.check();
        try {
            co_await _group0->client().wait_until_group0_upgraded(_group0_as);

            // First, wait for the feature to become enabled
            shared_promise<> p;
            _feature_service.supports_consistent_topology_changes.when_enabled([&] () noexcept { p.set_value(); });
            co_await p.get_shared_future(_group0_as);
            rtlogger.info("The cluster is ready to start upgrade to the raft topology. The procedure needs to be manually triggered. Refer to the documentation");

            // Wait until upgrade is started
            co_await _topology_state_machine.event.when([this] {
                return !legacy_topology_change_enabled();
            });
            rtlogger.info("upgrade to raft topology has started");
            break;
        } catch (const seastar::abort_requested_exception&) {
            throw;
        } catch (...) {
            rtlogger.error("the fiber tracking readiness of upgrade to raft toplogy got an unexpected error: {}", std::current_exception());
        }

        co_await sleep_abortable(std::chrono::seconds(1), _group0_as);
    }

    // Start the topology coordinator monitor fiber. If we are the leader, this will start
    // the topology coordinator which is responsible for driving the upgrade process.
    try {
        _raft_state_monitor = raft_state_monitor_fiber(_group0->group0_server(), sys_dist_ks);
    } catch (...) {
        // The calls above can theoretically fail due to coroutine frame allocation failure.
        // Abort in this case as the node should be in a pretty bad shape anyway.
        rtlogger.error("failed to start the topology coordinator: {}", std::current_exception());
        abort();
    }

    while (true) {
        _group0_as.check();
        try {
            // Wait until upgrade is finished
            co_await _topology_state_machine.event.when([this] {
                return raft_topology_change_enabled();
            });
            rtlogger.info("upgrade to raft topology has finished");
            break;
        } catch (const seastar::abort_requested_exception&) {
            throw;
        } catch (...) {
            rtlogger.error("the fiber tracking progress of upgrade to raft toplogy got an unexpected error. "
                    "Will not report in logs when upgrade has completed. Error: {}", std::current_exception());
        }
    }

    try {
        _sstable_cleanup_fiber = sstable_cleanup_fiber(_group0->group0_server(), proxy);
        start_tablet_split_monitor();
    } catch (...) {
        rtlogger.error("failed to start one of the raft-related background fibers: {}", std::current_exception());
        abort();
    }
}

future<> storage_service::mark_existing_views_as_built() {
    assert(this_shard_id() == 0);
    auto views = _db.local().get_views();
    co_await coroutine::parallel_for_each(views, [this] (view_ptr& view) -> future<> {
        co_await _sys_ks.local().mark_view_as_built(view->ks_name(), view->cf_name());
        co_await _sys_dist_ks.local().finish_view_build(view->ks_name(), view->cf_name());
    });
}

std::unordered_set<gms::inet_address> storage_service::parse_node_list(sstring comma_separated_list, const token_metadata& tm) {
    std::vector<sstring> ignore_nodes_strs = utils::split_comma_separated_list(std::move(comma_separated_list));
    std::unordered_set<gms::inet_address> ignore_nodes;
    for (const sstring& n : ignore_nodes_strs) {
        try {
            auto ep_and_id = tm.parse_host_id_and_endpoint(n);
            ignore_nodes.insert(ep_and_id.endpoint);
        } catch (...) {
            throw std::runtime_error(::format("Failed to parse node list: {}: invalid node={}: {}", ignore_nodes_strs, n, std::current_exception()));
        }
    }
    return ignore_nodes;
}

// Runs inside seastar::async context
future<> storage_service::bootstrap(std::unordered_set<token>& bootstrap_tokens, std::optional<cdc::generation_id>& cdc_gen_id, const std::optional<replacement_info>& replacement_info) {
    return seastar::async([this, &bootstrap_tokens, &cdc_gen_id, &replacement_info] {
        auto bootstrap_rbno = is_repair_based_node_ops_enabled(streaming::stream_reason::bootstrap);

        set_mode(mode::BOOTSTRAP);
        slogger.debug("bootstrap: rbno={} replacing={}", bootstrap_rbno, is_replacing());

        // Wait until we know tokens of existing node before announcing replacing status.
        slogger.info("Wait until local node knows tokens of peer nodes");
        _gossiper.wait_for_range_setup().get();

        _db.invoke_on_all([] (replica::database& db) {
            for (auto& cf : db.get_non_system_column_families()) {
                cf->notify_bootstrap_or_replace_start();
            }
        }).get();

        {
            int retry = 0;
            while (get_token_metadata_ptr()->count_normal_token_owners() == 0) {
                if (retry++ < 500) {
                    sleep_abortable(std::chrono::milliseconds(10), _abort_source).get();
                    continue;
                }
                // We're joining an existing cluster, so there are normal nodes in the cluster.
                // We've waited for tokens to arrive.
                // But we didn't see any normal token owners. Something's wrong, we cannot proceed.
                throw std::runtime_error{
                        "Failed to learn about other nodes' tokens during bootstrap or replace. Make sure that:\n"
                        " - the node can contact other nodes in the cluster,\n"
                        " - the `ring_delay` parameter is large enough (the 30s default should be enough for small-to-middle-sized clusters),\n"
                        " - a node with this IP didn't recently leave the cluster. If it did, wait for some time first (the IP is quarantined),\n"
                        "and retry the bootstrap/replace."};
            }
        }

        if (!replacement_info) {
            // Even if we reached this point before but crashed, we will make a new CDC generation.
            // It doesn't hurt: other nodes will (potentially) just do more generation switches.
            // We do this because with this new attempt at bootstrapping we picked a different set of tokens.

            // Update pending ranges now, so we correctly count ourselves as a pending replica
            // when inserting the new CDC generation.
            if (!bootstrap_rbno) {
                // When is_repair_based_node_ops_enabled is true, the bootstrap node
                // will use node_ops_cmd to bootstrap, node_ops_cmd will update the pending ranges.
                slogger.debug("bootstrap: update pending ranges: endpoint={} bootstrap_tokens={}", get_broadcast_address(), bootstrap_tokens);
                mutate_token_metadata([this, &bootstrap_tokens] (mutable_token_metadata_ptr tmptr) {
                    auto endpoint = get_broadcast_address();
                    tmptr->update_topology(tmptr->get_my_id(), _snitch.local()->get_location(), locator::node::state::bootstrapping);
                    tmptr->add_bootstrap_tokens(bootstrap_tokens, tmptr->get_my_id());
                    return update_topology_change_info(std::move(tmptr), ::format("bootstrapping node {}", endpoint));
                }).get();
            }

            // After we pick a generation timestamp, we start gossiping it, and we stick with it.
            // We don't do any other generation switches (unless we crash before complecting bootstrap).
            assert(!cdc_gen_id);

            cdc_gen_id = _cdc_gens.local().legacy_make_new_generation(bootstrap_tokens, !is_first_node()).get();

            if (!bootstrap_rbno) {
                // When is_repair_based_node_ops_enabled is true, the bootstrap node
                // will use node_ops_cmd to bootstrap, bootstrapping gossip status is not needed for bootstrap.
                _gossiper.add_local_application_state({
                    { gms::application_state::TOKENS, versioned_value::tokens(bootstrap_tokens) },
                    { gms::application_state::CDC_GENERATION_ID, versioned_value::cdc_generation_id(cdc_gen_id) },
                    { gms::application_state::STATUS, versioned_value::bootstrapping(bootstrap_tokens) },
                }).get();

                slogger.info("sleeping {} ms for pending range setup", get_ring_delay().count());
                _gossiper.wait_for_range_setup().get();
                dht::boot_strapper bs(_db, _stream_manager, _abort_source, get_token_metadata_ptr()->get_my_id(), _snitch.local()->get_location(), bootstrap_tokens, get_token_metadata_ptr());
                slogger.info("Starting to bootstrap...");
                bs.bootstrap(streaming::stream_reason::bootstrap, _gossiper, null_topology_guard).get();
            } else {
                // Even with RBNO bootstrap we need to announce the new CDC generation immediately after it's created.
                _gossiper.add_local_application_state({
                    { gms::application_state::CDC_GENERATION_ID, versioned_value::cdc_generation_id(cdc_gen_id) },
                }).get();
                slogger.info("Starting to bootstrap...");
                run_bootstrap_ops(bootstrap_tokens);
            }
        } else {
            auto replace_addr = replacement_info->address;
            auto replaced_host_id = replacement_info->host_id;

            slogger.debug("Removing replaced endpoint {} from system.peers", replace_addr);
            _sys_ks.local().remove_endpoint(replace_addr).get();

            assert(replaced_host_id);
            auto raft_id = raft::server_id{replaced_host_id.uuid()};
            assert(_group0);
            bool raft_available = _group0->wait_for_raft().get();
            if (raft_available) {
                slogger.info("Replace: removing {}/{} from group 0...", replace_addr, raft_id);
                _group0->remove_from_group0(raft_id).get();
            }

            slogger.info("Starting to bootstrap...");
            run_replace_ops(bootstrap_tokens, *replacement_info);
        }

        _db.invoke_on_all([] (replica::database& db) {
            for (auto& cf : db.get_non_system_column_families()) {
                cf->notify_bootstrap_or_replace_end();
            }
        }).get();

        slogger.info("Bootstrap completed! for the tokens {}", bootstrap_tokens);
    });
}

future<std::unordered_map<dht::token_range, inet_address_vector_replica_set>>
storage_service::get_range_to_address_map(locator::effective_replication_map_ptr erm) const {
    return get_range_to_address_map(erm, erm->get_token_metadata_ptr()->sorted_tokens());
}

// Caller is responsible to hold token_metadata valid until the returned future is resolved
future<std::unordered_map<dht::token_range, inet_address_vector_replica_set>>
storage_service::get_range_to_address_map(locator::effective_replication_map_ptr erm,
        const std::vector<token>& sorted_tokens) const {
    co_return co_await construct_range_to_endpoint_map(erm, co_await get_all_ranges(sorted_tokens));
}

future<> storage_service::handle_state_bootstrap(inet_address endpoint, gms::permit_id pid) {
    slogger.debug("endpoint={} handle_state_bootstrap: permit_id={}", endpoint, pid);
    // explicitly check for TOKENS, because a bootstrapping node might be bootstrapping in legacy mode; that is, not using vnodes and no token specified
    auto tokens = get_tokens_for(endpoint);

    slogger.debug("Node {} state bootstrapping, token {}", endpoint, tokens);

    // if this node is present in token metadata, either we have missed intermediate states
    // or the node had crashed. Print warning if needed, clear obsolete stuff and
    // continue.
    auto tmlock = co_await get_token_metadata_lock();
    auto tmptr = co_await get_mutable_token_metadata_ptr();
    const auto host_id = _gossiper.get_host_id(endpoint);
    if (tmptr->is_normal_token_owner(host_id)) {
        // If isLeaving is false, we have missed both LEAVING and LEFT. However, if
        // isLeaving is true, we have only missed LEFT. Waiting time between completing
        // leave operation and rebootstrapping is relatively short, so the latter is quite
        // common (not enough time for gossip to spread). Therefore we report only the
        // former in the log.
        if (!tmptr->is_leaving(host_id)) {
            slogger.info("Node {} state jump to bootstrap", host_id);
        }
        tmptr->remove_endpoint(host_id);
    }
    tmptr->update_topology(host_id, get_dc_rack_for(endpoint), locator::node::state::bootstrapping);
    tmptr->add_bootstrap_tokens(tokens, host_id);
    tmptr->update_host_id(host_id, endpoint);

    co_await update_topology_change_info(tmptr, ::format("handle_state_bootstrap {}", endpoint));
    co_await replicate_to_all_cores(std::move(tmptr));
}

future<> storage_service::handle_state_normal(inet_address endpoint, gms::permit_id pid) {
    slogger.debug("endpoint={} handle_state_normal: permit_id={}", endpoint, pid);

    auto tokens = get_tokens_for(endpoint);

    slogger.info("Node {} is in normal state, tokens: {}", endpoint, tokens);

    auto tmlock = std::make_unique<token_metadata_lock>(co_await get_token_metadata_lock());
    auto tmptr = co_await get_mutable_token_metadata_ptr();
    std::unordered_set<inet_address> endpoints_to_remove;

    auto do_remove_node = [&] (gms::inet_address node) {
        // this lambda is called in three cases:
        // 1. old endpoint for the given host_id is ours, we remove the new endpoint;
        // 2. new endpoint for the given host_id has bigger generation, we remove the old endpoint;
        // 3. old endpoint for the given host_id has bigger generation, we remove the new endpoint.
        // In all of these cases host_id is retained, only the IP addresses are changed.
        // We don't need to call remove_endpoint on tmptr, since it will be called
        // indirectly through the chain endpoints_to_remove->storage_service::remove_endpoint ->
        // _gossiper.remove_endpoint -> storage_service::on_remove.

        endpoints_to_remove.insert(node);
    };
    // Order Matters, TM.updateHostID() should be called before TM.updateNormalToken(), (see CASSANDRA-4300).
    auto host_id = _gossiper.get_host_id(endpoint);
    if (tmptr->is_normal_token_owner(host_id)) {
        slogger.info("handle_state_normal: node {}/{} was already a normal token owner", endpoint, host_id);
    }
    auto existing = tmptr->get_endpoint_for_host_id_if_known(host_id);

    // Old node in replace-with-same-IP scenario.
    std::optional<locator::host_id> replaced_id;

    if (existing && *existing != endpoint) {
        // This branch in taken when a node changes its IP address.

        if (*existing == get_broadcast_address()) {
            slogger.warn("Not updating host ID {} for {} because it's mine", host_id, endpoint);
            do_remove_node(endpoint);
        } else if (_gossiper.compare_endpoint_startup(endpoint, *existing) > 0) {
            // The new IP has greater generation than the existing one.
            // Here we remap the host_id to the new IP. The 'owned_tokens' calculation logic below
            // won't detect any changes - the branch 'endpoint == current_owner' will be taken.
            // We still need to call 'remove_endpoint' for existing IP to remove it from system.peers.

            slogger.warn("Host ID collision for {} between {} and {}; {} is the new owner", host_id, *existing, endpoint, endpoint);
            do_remove_node(*existing);
            slogger.info("Set host_id={} to be owned by node={}, existing={}", host_id, endpoint, *existing);
            tmptr->update_host_id(host_id, endpoint);
        } else {
            // The new IP has smaller generation than the existing one,
            // we are going to remove it, so we add it to the endpoints_to_remove.
            // How does this relate to the tokens this endpoint may have?
            // There is a condition below which checks that if endpoints_to_remove
            // contains 'endpoint', then the owned_tokens must be empty, otherwise internal_error
            // is triggered. This means the following is expected to be true:
            // 1. each token from the tokens variable (which is read from gossiper) must have an owner node
            // 2. this owner must be different from 'endpoint'
            // 3. its generation must be greater than endpoint's

            slogger.warn("Host ID collision for {} between {} and {}; ignored {}", host_id, *existing, endpoint, endpoint);
            do_remove_node(endpoint);
        }
    } else if (existing && *existing == endpoint) {
        // This branch is taken for all gossiper-managed topology operations.
        // For example, if this node is a member of the cluster and a new node is added,
        // handle_state_normal is called on this node as the final step
        // in the endpoint bootstrap process.
        // This method is also called for both replace scenarios - with either the same or with a different IP.
        // If the new node has a different IP, the old IP is removed by the block of
        // logic below - we detach the old IP from token ring,
        // it gets added to candidates_for_removal, then storage_service::remove_endpoint ->
        // _gossiper.remove_endpoint -> storage_service::on_remove -> remove from token_metadata.
        // If the new node has the same IP, we need to explicitly remove old host_id from
        // token_metadata, since no IPs will be removed in this case.
        // We do this after update_normal_tokens, allowing for tokens to be properly
        // migrated to the new host_id.

        slogger.info("Host ID {} continues to be owned by {}", host_id, endpoint);
        if (const auto old_host_id = tmptr->get_host_id_if_known(endpoint); old_host_id && *old_host_id != host_id) {
            // Replace with same IP scenario
            slogger.info("The IP {} previously owned host ID {}", endpoint, *old_host_id);
            replaced_id = *old_host_id;
        }
    } else {
        // This branch is taken if this node wasn't involved in node_ops
        // workflow (storage_service::node_ops_cmd_handler wasn't called on it) and it just
        // receives the current state of the cluster from the gossiper.
        // For example, a new node receives this notification for every
        // existing node in the cluster.

        auto nodes = _gossiper.get_nodes_with_host_id(host_id);
        bool left = std::any_of(nodes.begin(), nodes.end(), [this] (const gms::inet_address& node) { return _gossiper.is_left(node); });
        if (left) {
            slogger.info("Skip to set host_id={} to be owned by node={}, because the node is removed from the cluster, nodes {} used to own the host_id", host_id, endpoint, nodes);
            _normal_state_handled_on_boot.insert(endpoint);
            co_return;
        }
        slogger.info("Set host_id={} to be owned by node={}", host_id, endpoint);
        tmptr->update_host_id(host_id, endpoint);
    }

    // Tokens owned by the handled endpoint.
    // The endpoint broadcasts its set of chosen tokens. If a token was also chosen by another endpoint,
    // the collision is resolved by assigning the token to the endpoint which started later.
    std::unordered_set<token> owned_tokens;

    // token_to_endpoint_map is used to track the current token owners for the purpose of removing replaced endpoints.
    // when any token is replaced by a new owner, we track the existing owner in `candidates_for_removal`
    // and eventually, if any candidate for removal ends up owning no tokens, it is removed from token_metadata.
    std::unordered_map<token, inet_address> token_to_endpoint_map = get_token_to_endpoint(get_token_metadata());
    std::unordered_set<inet_address> candidates_for_removal;

    // Here we convert endpoint tokens from gossiper to owned_tokens, which will be assigned as a new
    // normal tokens to the token_metadata.
    // This transformation accounts for situations where some tokens
    // belong to outdated nodes - the ones with smaller generation.
    // We use endpoints instead of host_ids here since gossiper operates
    // with endpoints and generations are tied to endpoints, not host_ids.
    // In replace-with-same-ip scenario we won't be able to distinguish
    // between the old and new IP owners, so we assume the old replica
    // is down and won't be resurrected.

    for (auto t : tokens) {
        // we don't want to update if this node is responsible for the token and it has a later startup time than endpoint.
        auto current = token_to_endpoint_map.find(t);
        if (current == token_to_endpoint_map.end()) {
            slogger.debug("handle_state_normal: New node {} at token {}", endpoint, t);
            owned_tokens.insert(t);
            continue;
        }
        auto current_owner = current->second;
        if (endpoint == current_owner) {
            slogger.info("handle_state_normal: endpoint={} == current_owner={} token {}", endpoint, current_owner, t);
            // set state back to normal, since the node may have tried to leave, but failed and is now back up
            owned_tokens.insert(t);
        } else if (_gossiper.compare_endpoint_startup(endpoint, current_owner) > 0) {
            slogger.debug("handle_state_normal: endpoint={} > current_owner={}, token {}", endpoint, current_owner, t);
            owned_tokens.insert(t);
            slogger.info("handle_state_normal: remove endpoint={} token={}", current_owner, t);
            // currentOwner is no longer current, endpoint is.  Keep track of these moves, because when
            // a host no longer has any tokens, we'll want to remove it.
            token_to_endpoint_map.erase(current);
            candidates_for_removal.insert(current_owner);
            slogger.info("handle_state_normal: Nodes {} and {} have the same token {}. {} is the new owner", endpoint, current_owner, t, endpoint);
        } else {
            // current owner of this token is kept and endpoint attempt to own it is rejected.
            // Keep track of these moves, because when a host no longer has any tokens, we'll want to remove it.
            token_to_endpoint_map.erase(current);
            candidates_for_removal.insert(endpoint);
            slogger.info("handle_state_normal: Nodes {} and {} have the same token {}. Ignoring {}", endpoint, current_owner, t, endpoint);
        }
    }

    // After we replace all tokens owned by current_owner
    // We check for each candidate for removal if it still owns any tokens,
    // and remove it if it doesn't anymore.
    if (!candidates_for_removal.empty()) {
        for (const auto& [t, ep] : token_to_endpoint_map) {
            if (candidates_for_removal.contains(ep)) {
                slogger.info("handle_state_normal: endpoint={} still owns tokens, will not be removed", ep);
                candidates_for_removal.erase(ep);
                if (candidates_for_removal.empty()) {
                    break;
                }
            }
        }
    }

    for (const auto& ep : candidates_for_removal) {
        slogger.info("handle_state_normal: endpoints_to_remove endpoint={}", ep);
        endpoints_to_remove.insert(ep);
    }

    bool is_normal_token_owner = tmptr->is_normal_token_owner(host_id);
    bool do_notify_joined = false;

    if (endpoints_to_remove.contains(endpoint)) [[unlikely]] {
        if (!owned_tokens.empty()) {
            on_fatal_internal_error(slogger, ::format("endpoint={} is marked for removal but still owns {} tokens", endpoint, owned_tokens.size()));
        }
    } else {
        if (owned_tokens.empty()) {
            on_internal_error(slogger, ::format("endpoint={} is not marked for removal but owns no tokens", endpoint));
        }

        if (!is_normal_token_owner) {
            do_notify_joined = true;
        }

        const auto dc_rack = get_dc_rack_for(endpoint);
        tmptr->update_topology(host_id, dc_rack, locator::node::state::normal);
        co_await tmptr->update_normal_tokens(owned_tokens, host_id);
        if (replaced_id) {
            if (tmptr->is_normal_token_owner(*replaced_id)) {
                on_internal_error(slogger, ::format("replaced endpoint={}/{} still owns tokens {}",
                    endpoint, *replaced_id, tmptr->get_tokens(*replaced_id)));
            } else {
                tmptr->remove_endpoint(*replaced_id);
                slogger.info("node {}/{} is removed from token_metadata since it's replaced by {}/{} ",
                    endpoint, *replaced_id, endpoint, host_id);
            }
        }
    }

    co_await update_topology_change_info(tmptr, ::format("handle_state_normal {}", endpoint));
    co_await replicate_to_all_cores(std::move(tmptr));
    tmlock.reset();

    for (auto ep : endpoints_to_remove) {
        co_await remove_endpoint(ep, ep == endpoint ? pid : gms::null_permit_id);
    }
    slogger.debug("handle_state_normal: endpoint={} is_normal_token_owner={} endpoint_to_remove={} owned_tokens={}", endpoint, is_normal_token_owner, endpoints_to_remove.contains(endpoint), owned_tokens);
    if (!is_me(endpoint) && !owned_tokens.empty() && !endpoints_to_remove.count(endpoint)) {
        try {
            auto info = get_peer_info_for_update(endpoint);
            info.tokens = std::move(owned_tokens);
            co_await _sys_ks.local().update_peer_info(endpoint, host_id, info);
        } catch (...) {
            slogger.error("handle_state_normal: fail to update tokens for {}: {}", endpoint, std::current_exception());
        }
    }

    // Send joined notification only when this node was not a member prior to this
    if (do_notify_joined) {
        co_await notify_joined(endpoint);
        co_await remove_rpc_client_with_ignored_topology(endpoint);
    }

    if (slogger.is_enabled(logging::log_level::debug)) {
        const auto& tm = get_token_metadata();
        auto ver = tm.get_ring_version();
        for (auto& x : tm.get_token_to_endpoint()) {
            slogger.debug("handle_state_normal: token_metadata.ring_version={}, token={} -> endpoint={}/{}", ver, x.first, tm.get_endpoint_for_host_id(x.second), x.second);
        }
    }
    _normal_state_handled_on_boot.insert(endpoint);
    slogger.info("handle_state_normal for {}/{} finished", endpoint, host_id);
}

future<> storage_service::handle_state_left(inet_address endpoint, std::vector<sstring> pieces, gms::permit_id pid) {
    slogger.debug("endpoint={} handle_state_left: permit_id={}", endpoint, pid);

    if (pieces.size() < 2) {
        slogger.warn("Fail to handle_state_left endpoint={} pieces={}", endpoint, pieces);
        co_return;
    }
    const auto host_id = _gossiper.get_host_id(endpoint);
    auto tokens = get_tokens_for(endpoint);
    slogger.debug("Node {}/{} state left, tokens {}", endpoint, host_id, tokens);
    if (tokens.empty()) {
        auto eps = _gossiper.get_endpoint_state_ptr(endpoint);
        if (eps) {
            slogger.warn("handle_state_left: Tokens for node={} are empty, endpoint_state={}", endpoint, *eps);
        } else {
            slogger.warn("handle_state_left: Couldn't find endpoint state for node={}", endpoint);
        }
        auto tokens_from_tm = get_token_metadata().get_tokens(host_id);
        slogger.warn("handle_state_left: Get tokens from token_metadata, node={}/{}, tokens={}", endpoint, host_id, tokens_from_tm);
        tokens = std::unordered_set<dht::token>(tokens_from_tm.begin(), tokens_from_tm.end());
    }
    co_await excise(tokens, endpoint, extract_expire_time(pieces), pid);
}

future<> storage_service::handle_state_removed(inet_address endpoint, std::vector<sstring> pieces, gms::permit_id pid) {
    slogger.debug("endpoint={} handle_state_removed: permit_id={}", endpoint, pid);

    if (endpoint == get_broadcast_address()) {
        slogger.info("Received removenode gossip about myself. Is this node rejoining after an explicit removenode?");
        try {
            co_await drain();
        } catch (...) {
            slogger.error("Fail to drain: {}", std::current_exception());
            throw;
        }
        co_return;
    }
    const auto host_id = _gossiper.get_host_id(endpoint);
    if (get_token_metadata().is_normal_token_owner(host_id)) {
        auto state = pieces[0];
        auto remove_tokens = get_token_metadata().get_tokens(host_id);
        std::unordered_set<token> tmp(remove_tokens.begin(), remove_tokens.end());
        co_await excise(std::move(tmp), endpoint, extract_expire_time(pieces), pid);
    } else { // now that the gossiper has told us about this nonexistent member, notify the gossiper to remove it
        add_expire_time_if_found(endpoint, extract_expire_time(pieces));
        co_await remove_endpoint(endpoint, pid);
    }
}

future<> storage_service::on_join(gms::inet_address endpoint, gms::endpoint_state_ptr ep_state, gms::permit_id pid) {
    slogger.debug("endpoint={} on_join: permit_id={}", endpoint, pid);
    co_await on_change(endpoint, ep_state->get_application_state_map(), pid);
}

future<> storage_service::on_alive(gms::inet_address endpoint, gms::endpoint_state_ptr state, gms::permit_id pid) {
    const auto& tm = get_token_metadata();
    const auto tm_host_id_opt = tm.get_host_id_if_known(endpoint);
    slogger.debug("endpoint={}/{} on_alive: permit_id={}", endpoint, tm_host_id_opt, pid);
    bool is_normal_token_owner = tm_host_id_opt && tm.is_normal_token_owner(*tm_host_id_opt);
    if (is_normal_token_owner) {
        co_await notify_up(endpoint);
    } else if (raft_topology_change_enabled()) {
        slogger.debug("ignore on_alive since topology changes are using raft and "
                      "endpoint {}/{} is not a normal token owner", endpoint, tm_host_id_opt);
    } else {
        auto tmlock = co_await get_token_metadata_lock();
        auto tmptr = co_await get_mutable_token_metadata_ptr();
        const auto dc_rack = get_dc_rack_for(endpoint);
        const auto host_id = _gossiper.get_host_id(endpoint);
        tmptr->update_host_id(host_id, endpoint);
        tmptr->update_topology(host_id, dc_rack);
        co_await replicate_to_all_cores(std::move(tmptr));
    }
}

future<> storage_service::on_change(gms::inet_address endpoint, const gms::application_state_map& states_, gms::permit_id pid) {
    // copy the states map locally since the coroutine may yield
    auto states = states_;
    slogger.debug("endpoint={} on_change:     states={}, permit_id={}", endpoint, states, pid);
    if (raft_topology_change_enabled()) {
        slogger.debug("ignore status changes since topology changes are using raft");
    } else {
        co_await on_application_state_change(endpoint, states, application_state::STATUS, pid, [this] (inet_address endpoint, const gms::versioned_value& value, gms::permit_id pid) -> future<> {
            std::vector<sstring> pieces;
            boost::split(pieces, value.value(), boost::is_any_of(sstring(versioned_value::DELIMITER_STR)));
            if (pieces.empty()) {
                slogger.warn("Fail to split status in on_change: endpoint={}, app_state={}, value={}", endpoint, application_state::STATUS, value);
                co_return;
            }
            const sstring& move_name = pieces[0];
            if (move_name == sstring(versioned_value::STATUS_BOOTSTRAPPING)) {
                co_await handle_state_bootstrap(endpoint, pid);
            } else if (move_name == sstring(versioned_value::STATUS_NORMAL) ||
                       move_name == sstring(versioned_value::SHUTDOWN)) {
                co_await handle_state_normal(endpoint, pid);
            } else if (move_name == sstring(versioned_value::REMOVED_TOKEN)) {
                co_await handle_state_removed(endpoint, std::move(pieces), pid);
            } else if (move_name == sstring(versioned_value::STATUS_LEFT)) {
                co_await handle_state_left(endpoint, std::move(pieces), pid);
            } else {
                co_return; // did nothing.
            }
        });
    }
    auto ep_state = _gossiper.get_endpoint_state_ptr(endpoint);
    if (!ep_state || _gossiper.is_dead_state(*ep_state)) {
        slogger.debug("Ignoring state change for dead or unknown endpoint: {}", endpoint);
        co_return;
    }
    const auto host_id = _gossiper.get_host_id(endpoint);
    const auto& tm = get_token_metadata();
    const auto ep = tm.get_endpoint_for_host_id_if_known(host_id);
    // The check *ep == endpoint is needed when a node changes
    // its IP - on_change can be called by the gossiper for old IP as part
    // of its removal, after handle_state_normal has already been called for
    // the new one. Without the check, the do_update_system_peers_table call
    // overwrites the IP back to its old value.
    // In essence, the code under the 'if' should fire if the given IP is a normal_token_owner.
    if (ep && *ep == endpoint && tm.is_normal_token_owner(host_id)) {
        if (!is_me(endpoint)) {
            slogger.debug("endpoint={}/{} on_change:     updating system.peers table", endpoint, host_id);
            co_await _sys_ks.local().update_peer_info(endpoint, host_id, get_peer_info_for_update(endpoint, states));
        }
        if (states.contains(application_state::RPC_READY)) {
            slogger.debug("Got application_state::RPC_READY for node {}, is_cql_ready={}", endpoint, ep_state->is_cql_ready());
            co_await notify_cql_change(endpoint, ep_state->is_cql_ready());
        }
        if (auto it = states.find(application_state::INTERNAL_IP); it != states.end()) {
            co_await maybe_reconnect_to_preferred_ip(endpoint, inet_address(it->second.value()));
        }
    }
}

future<> storage_service::maybe_reconnect_to_preferred_ip(inet_address ep, inet_address local_ip) {
    if (!_snitch.local()->prefer_local()) {
        co_return;
    }

    const auto& topo = get_token_metadata().get_topology();
    if (topo.get_datacenter() == topo.get_datacenter(ep) && _messaging.local().get_preferred_ip(ep) != local_ip) {
        slogger.debug("Initiated reconnect to an Internal IP {} for the {}", local_ip, ep);
        co_await _messaging.invoke_on_all([ep, local_ip] (auto& local_ms) {
            local_ms.cache_preferred_ip(ep, local_ip);
        });
    }
}


future<> storage_service::on_remove(gms::inet_address endpoint, gms::permit_id pid) {
    slogger.debug("endpoint={} on_remove: permit_id={}", endpoint, pid);

    if (raft_topology_change_enabled()) {
        slogger.debug("ignore on_remove since topology changes are using raft");
        co_return;
    }

    auto tmlock = co_await get_token_metadata_lock();
    auto tmptr = co_await get_mutable_token_metadata_ptr();
    // We should handle the case when we aren't able to find endpoint -> ip mapping in token_metadata.
    // This could happen e.g. when the new endpoint has bigger generation in handle_state_normal - the code
    // in handle_state_normal will remap host_id to the new IP and we won't find
    // old IP here. We should just skip the remove in that case.
    if (const auto host_id = tmptr->get_host_id_if_known(endpoint); host_id) {
        tmptr->remove_endpoint(*host_id);
    }
    co_await update_topology_change_info(tmptr, ::format("on_remove {}", endpoint));
    co_await replicate_to_all_cores(std::move(tmptr));
}

future<> storage_service::on_dead(gms::inet_address endpoint, gms::endpoint_state_ptr state, gms::permit_id pid) {
    slogger.debug("endpoint={} on_dead: permit_id={}", endpoint, pid);
    return notify_down(endpoint);
}

future<> storage_service::on_restart(gms::inet_address endpoint, gms::endpoint_state_ptr state, gms::permit_id pid) {
    slogger.debug("endpoint={} on_restart: permit_id={}", endpoint, pid);
    // If we have restarted before the node was even marked down, we need to reset the connection pool
    if (endpoint != get_broadcast_address() && _gossiper.is_alive(endpoint)) {
        return on_dead(endpoint, state, pid);
    }
    return make_ready_future();
}

db::system_keyspace::peer_info storage_service::get_peer_info_for_update(inet_address endpoint) {
    auto ep_state = _gossiper.get_endpoint_state_ptr(endpoint);
    if (!ep_state) {
        return db::system_keyspace::peer_info{};
    }
    return get_peer_info_for_update(endpoint, ep_state->get_application_state_map());
}

db::system_keyspace::peer_info storage_service::get_peer_info_for_update(inet_address endpoint, const gms::application_state_map& app_state_map) {
    db::system_keyspace::peer_info ret;

    auto set_field = [&]<typename T> (std::optional<T>& field,
            const gms::versioned_value& value,
            std::string_view name,
            bool managed_by_raft_in_raft_topology)
    {
        if (raft_topology_change_enabled() && managed_by_raft_in_raft_topology) {
            return;
        }
        try {
            field = T(value.value());
        } catch (...) {
            on_internal_error(slogger, format("failed to parse {} {} for {}: {}", name, value.value(),
                endpoint, std::current_exception()));
        }
    };

    for (const auto& [state, value] : app_state_map) {
        switch (state) {
        case application_state::DC:
            set_field(ret.data_center, value, "data_center", true);
            break;
        case application_state::INTERNAL_IP:
            set_field(ret.preferred_ip, value, "preferred_ip", false);
            break;
        case application_state::RACK:
            set_field(ret.rack, value, "rack", true);
            break;
        case application_state::RELEASE_VERSION:
            set_field(ret.release_version, value, "release_version", true);
            break;
        case application_state::RPC_ADDRESS:
            set_field(ret.rpc_address, value, "rpc_address", false);
            break;
        case application_state::SCHEMA:
            set_field(ret.schema_version, value, "schema_version", false);
            break;
        case application_state::TOKENS:
            // tokens are updated separately
            break;
        case application_state::SUPPORTED_FEATURES:
            set_field(ret.supported_features, value, "supported_features", true);
            break;
        default:
            break;
        }
    }
    return ret;
}

std::unordered_set<locator::token> storage_service::get_tokens_for(inet_address endpoint) {
    auto tokens_string = _gossiper.get_application_state_value(endpoint, application_state::TOKENS);
    slogger.trace("endpoint={}, tokens_string={}", endpoint, tokens_string);
    auto ret = versioned_value::tokens_from_string(tokens_string);
    slogger.trace("endpoint={}, tokens={}", endpoint, ret);
    return ret;
}

std::optional<locator::endpoint_dc_rack> storage_service::get_dc_rack_for(const gms::endpoint_state& ep_state) {
    auto* dc = ep_state.get_application_state_ptr(gms::application_state::DC);
    auto* rack = ep_state.get_application_state_ptr(gms::application_state::RACK);
    if (!dc || !rack) {
        return std::nullopt;
    }
    return locator::endpoint_dc_rack{
        .dc = dc->value(),
        .rack = rack->value(),
    };
}

std::optional<locator::endpoint_dc_rack> storage_service::get_dc_rack_for(inet_address endpoint) {
    auto eps = _gossiper.get_endpoint_state_ptr(endpoint);
    if (!eps) {
        return std::nullopt;
    }
    return get_dc_rack_for(*eps);
}

void endpoint_lifecycle_notifier::register_subscriber(endpoint_lifecycle_subscriber* subscriber)
{
    _subscribers.add(subscriber);
}

future<> endpoint_lifecycle_notifier::unregister_subscriber(endpoint_lifecycle_subscriber* subscriber) noexcept
{
    return _subscribers.remove(subscriber);
}

future<> storage_service::stop_transport() {
    if (!_transport_stopped.has_value()) {
        promise<> stopped;
        _transport_stopped = stopped.get_future();

        seastar::async([this] {
            slogger.info("Stop transport: starts");

            slogger.debug("shutting down migration manager");
            _migration_manager.invoke_on_all(&service::migration_manager::drain).get();

            shutdown_protocol_servers().get();
            slogger.info("Stop transport: shutdown rpc and cql server done");

            _gossiper.container().invoke_on_all(&gms::gossiper::shutdown).get();
            slogger.info("Stop transport: stop_gossiping done");

            _messaging.invoke_on_all(&netw::messaging_service::shutdown).get();
            slogger.info("Stop transport: shutdown messaging_service done");

            _stream_manager.invoke_on_all(&streaming::stream_manager::shutdown).get();
            slogger.info("Stop transport: shutdown stream_manager done");

            slogger.info("Stop transport: done");
        }).forward_to(std::move(stopped));
    }

    return _transport_stopped.value();
}

future<> storage_service::drain_on_shutdown() {
    assert(this_shard_id() == 0);
    return (_operation_mode == mode::DRAINING || _operation_mode == mode::DRAINED) ?
        _drain_finished.get_future() : do_drain();
}

void storage_service::set_group0(raft_group0& group0, bool raft_experimental_topology) {
    _group0 = &group0;
    _raft_experimental_topology = raft_experimental_topology;
}

future<> storage_service::init_address_map(raft_address_map& address_map, gms::generation_type new_generation) {
    for (auto [ip, host] : co_await _sys_ks.local().load_host_ids()) {
        address_map.add_or_update_entry(raft::server_id(host.uuid()), ip);
    }
    const auto& topology = get_token_metadata().get_topology();
    address_map.add_or_update_entry(raft::server_id{topology.my_host_id().uuid()},
        topology.my_address(), new_generation);
    _raft_ip_address_updater = make_shared<raft_ip_address_updater>(address_map, *this);
    _gossiper.register_(_raft_ip_address_updater);
}

future<> storage_service::uninit_address_map() {
    return _gossiper.unregister_(_raft_ip_address_updater);
}

bool storage_service::is_topology_coordinator_enabled() const {
    return raft_topology_change_enabled();
}

future<> storage_service::join_cluster(sharded<db::system_distributed_keyspace>& sys_dist_ks, sharded<service::storage_proxy>& proxy,
        sharded<gms::gossiper>& gossiper, start_hint_manager start_hm, gms::generation_type new_generation) {
    assert(this_shard_id() == 0);

    if (_sys_ks.local().was_decommissioned()) {
        auto msg = sstring("This node was decommissioned and will not rejoin the ring unless "
                           "all existing data is removed and the node is bootstrapped again");
        slogger.error("{}", msg);
        throw std::runtime_error(msg);
    }

    set_mode(mode::STARTING);

    std::unordered_set<inet_address> loaded_endpoints;
    if (_db.local().get_config().load_ring_state() && !raft_topology_change_enabled()) {
        slogger.info("Loading persisted ring state");
        auto loaded_tokens = co_await _sys_ks.local().load_tokens();
        auto loaded_host_ids = co_await _sys_ks.local().load_host_ids();
        auto loaded_dc_rack = co_await _sys_ks.local().load_dc_rack_info();

        auto get_dc_rack = [&loaded_dc_rack] (inet_address ep) {
            if (loaded_dc_rack.contains(ep)) {
                return loaded_dc_rack[ep];
            } else {
                return locator::endpoint_dc_rack::default_location;
            }
        };

        if (slogger.is_enabled(logging::log_level::debug)) {
            for (auto& x : loaded_tokens) {
                slogger.debug("Loaded tokens: endpoint={}, tokens={}", x.first, x.second);
            }

            for (auto& x : loaded_host_ids) {
                slogger.debug("Loaded host_id: endpoint={}, uuid={}", x.first, x.second);
            }
        }

        auto tmlock = co_await get_token_metadata_lock();
        auto tmptr = co_await get_mutable_token_metadata_ptr();
        for (auto x : loaded_tokens) {
            auto ep = x.first;
            auto tokens = x.second;
            if (ep == get_broadcast_address()) {
                // entry has been mistakenly added, delete it
                co_await _sys_ks.local().remove_endpoint(ep);
            } else {
                const auto dc_rack = get_dc_rack(ep);
                const auto hostIdIt = loaded_host_ids.find(ep);
                if (hostIdIt == loaded_host_ids.end()) {
                    on_internal_error(slogger, format("can't find host_id for ep {}", ep));
                }
                tmptr->update_topology(hostIdIt->second, dc_rack, locator::node::state::normal);
                co_await tmptr->update_normal_tokens(tokens, hostIdIt->second);
                tmptr->update_host_id(hostIdIt->second, ep);
                loaded_endpoints.insert(ep);
                // gossiping hasn't started yet
                // so no need to lock the endpoint
                co_await _gossiper.add_saved_endpoint(ep, gms::null_permit_id);
            }
        }
        co_await replicate_to_all_cores(std::move(tmptr));
    }

    // Seeds are now only used as the initial contact point nodes. If the
    // loaded_endpoints are empty which means this node is a completely new
    // node, we use the nodes specified in seeds as the initial contact
    // point nodes, otherwise use the peer nodes persisted in system table.
    auto seeds = _gossiper.get_seeds();
    auto initial_contact_nodes = loaded_endpoints.empty() ?
        std::unordered_set<gms::inet_address>(seeds.begin(), seeds.end()) :
        loaded_endpoints;
    auto loaded_peer_features = co_await _sys_ks.local().load_peer_features();
    slogger.info("initial_contact_nodes={}, loaded_endpoints={}, loaded_peer_features={}",
            initial_contact_nodes, loaded_endpoints, loaded_peer_features.size());
    for (auto& x : loaded_peer_features) {
        slogger.info("peer={}, supported_features={}", x.first, x.second);
    }

    if (!_raft_experimental_topology) {
        slogger.info("Booting in legacy operations mode because experimental raft topology is not enabled");
        _topology_change_kind_enabled = topology_change_kind::legacy;
    } else if (utils::get_local_injector().is_enabled("force_gossip_based_join") && !_sys_ks.local().bootstrap_complete()) {
        slogger.info("Booting in legacy topology operations mode because it was requested by an error injection");
        _topology_change_kind_enabled = topology_change_kind::legacy;
    } else if (_group0->client().in_recovery()) {
        slogger.info("Raft recovery - starting in legacy topology operations mode");
        _topology_change_kind_enabled = topology_change_kind::legacy;
    } else if (_group0->joined_group0()) {
        // We are a part of group 0. The _topology_change_kind_enabled flag is maintained from there.
        _manage_topology_change_kind_from_group0 = true;
        _topology_change_kind_enabled = upgrade_state_to_topology_op_kind(_topology_state_machine._topology.upgrade_state);
        slogger.info("The node is already in group 0 and will restart in {} mode", raft_topology_change_enabled() ? "raft" : "legacy");
    } else if (_sys_ks.local().bootstrap_complete()) {
        // We already bootstrapped but we are not a part of group 0. This means that we are restarting after recovery.
        slogger.info("Restarting in legacy mode. The node was either upgraded from a non-raft-topology version or is restarting after recovery.");
        _topology_change_kind_enabled = topology_change_kind::legacy;
    } else {
        // We are not in group 0 and we are just bootstrapping. We need to discover group 0.
        const std::vector<gms::inet_address> contact_nodes{initial_contact_nodes.begin(), initial_contact_nodes.end()};
        auto g0_info = co_await _group0->discover_group0(contact_nodes, _qp);
        slogger.info("Found group 0 with ID {}, with leader of ID {} and IP {}",
                g0_info.group0_id, g0_info.id, g0_info.ip_addr);

        if (_group0->load_my_id() == g0_info.id) {
            // We're creating the group 0.
            slogger.info("We are creating the group 0. Start in raft topology operations mode");
            _topology_change_kind_enabled = topology_change_kind::raft;
        } else {
            // Ask the current member of the raft group about which mode to use
            auto params = join_node_query_params {};
            auto result = co_await ser::join_node_rpc_verbs::send_join_node_query(
                    &_messaging.local(), netw::msg_addr(g0_info.ip_addr), g0_info.id, std::move(params));
            switch (result.topo_mode) {
            case join_node_query_result::topology_mode::raft:
                slogger.info("Will join existing cluster in raft topology operations mode");
                _topology_change_kind_enabled = topology_change_kind::raft;
                break;
            case join_node_query_result::topology_mode::legacy:
                slogger.info("Will join existing cluster in legacy topology operations mode because the cluster still doesn't use raft-based topology operations");
                _topology_change_kind_enabled = topology_change_kind::legacy;
            }
        }
    }

    co_return co_await join_token_ring(sys_dist_ks, proxy, gossiper, std::move(initial_contact_nodes),
            std::move(loaded_endpoints), std::move(loaded_peer_features), get_ring_delay(), start_hm, new_generation);
}

future<> storage_service::replicate_to_all_cores(mutable_token_metadata_ptr tmptr) noexcept {
    assert(this_shard_id() == 0);

    slogger.debug("Replicating token_metadata to all cores");
    std::exception_ptr ex;

    std::vector<mutable_token_metadata_ptr> pending_token_metadata_ptr;
    pending_token_metadata_ptr.resize(smp::count);
    std::vector<std::unordered_map<sstring, locator::vnode_effective_replication_map_ptr>> pending_effective_replication_maps;
    pending_effective_replication_maps.resize(smp::count);
    std::vector<std::unordered_map<table_id, locator::effective_replication_map_ptr>> pending_table_erms;
    pending_table_erms.resize(smp::count);

    std::unordered_set<session_id> open_sessions;

    // Collect open sessions
    {
        auto session = _topology_state_machine._topology.session;
        if (session) {
            open_sessions.insert(session);
        }

        for (auto&& [table_id, tmap]: tmptr->tablets().all_tables()) {
            for (auto&& [tid, trinfo]: tmap.transitions()) {
                if (trinfo.session_id) {
                    auto id = session_id(trinfo.session_id);
                    open_sessions.insert(id);
                }
            }
        }
    }

    try {
        auto base_shard = this_shard_id();
        pending_token_metadata_ptr[base_shard] = tmptr;
        // clone a local copy of updated token_metadata on all other shards
        co_await smp::invoke_on_others(base_shard, [&, tmptr] () -> future<> {
            pending_token_metadata_ptr[this_shard_id()] = make_token_metadata_ptr(co_await tmptr->clone_async());
        });

        // Precalculate new effective_replication_map for all keyspaces
        // and clone to all shards;
        //
        // TODO: at the moment create on shard 0 first
        // but in the future we may want to use hash() % smp::count
        // to evenly distribute the load.
        auto& db = _db.local();
        auto keyspaces = db.get_all_keyspaces();
        for (auto& ks_name : keyspaces) {
            auto rs = db.find_keyspace(ks_name).get_replication_strategy_ptr();
            if (rs->is_per_table()) {
                continue;
            }
            auto erm = co_await get_erm_factory().create_effective_replication_map(rs, tmptr);
            pending_effective_replication_maps[base_shard].emplace(ks_name, std::move(erm));
        }
        co_await container().invoke_on_others([&] (storage_service& ss) -> future<> {
            auto& db = ss._db.local();
            for (auto& ks_name : keyspaces) {
                auto rs = db.find_keyspace(ks_name).get_replication_strategy_ptr();
                if (rs->is_per_table()) {
                    continue;
                }
                auto tmptr = pending_token_metadata_ptr[this_shard_id()];
                auto erm = co_await ss.get_erm_factory().create_effective_replication_map(rs, tmptr);
                pending_effective_replication_maps[this_shard_id()].emplace(ks_name, std::move(erm));
            }
        });
        // Prepare per-table erms.
        co_await container().invoke_on_all([&] (storage_service& ss) {
            auto& db = ss._db.local();
            auto tmptr = pending_token_metadata_ptr[this_shard_id()];
            db.get_tables_metadata().for_each_table([&] (table_id id, lw_shared_ptr<replica::table> table) {
                auto rs = db.find_keyspace(table->schema()->keypace_name()).get_replication_strategy_ptr();
                locator::effective_replication_map_ptr erm;
                if (auto pt_rs = rs->maybe_as_per_table()) {
                    erm = pt_rs->make_replication_map(id, tmptr);
                } else {
                    erm = pending_effective_replication_maps[this_shard_id()][table->schema()->keypace_name()];
                }
                pending_table_erms[this_shard_id()].emplace(id, std::move(erm));
            });
        });
    } catch (...) {
        ex = std::current_exception();
    }

    // Rollback on metadata replication error
    if (ex) {
        try {
            co_await smp::invoke_on_all([&] () -> future<> {
                auto tmptr = std::move(pending_token_metadata_ptr[this_shard_id()]);
                auto erms = std::move(pending_effective_replication_maps[this_shard_id()]);
                auto table_erms = std::move(pending_table_erms[this_shard_id()]);

                co_await utils::clear_gently(erms);
                co_await utils::clear_gently(tmptr);
            });
        } catch (...) {
            slogger.warn("Failure to reset pending token_metadata in cleanup path: {}. Ignored.", std::current_exception());
        }

        std::rethrow_exception(std::move(ex));
    }

    // Apply changes on all shards
    try {
        co_await container().invoke_on_all([&] (storage_service& ss) -> future<> {
            ss._shared_token_metadata.set(std::move(pending_token_metadata_ptr[this_shard_id()]));
            auto& db = ss._db.local();

            auto& erms = pending_effective_replication_maps[this_shard_id()];
            for (auto it = erms.begin(); it != erms.end(); ) {
                auto& ks = db.find_keyspace(it->first);
                ks.update_effective_replication_map(std::move(it->second));
                it = erms.erase(it);
            }

            auto& table_erms = pending_table_erms[this_shard_id()];
            for (auto it = table_erms.begin(); it != table_erms.end(); ) {
                auto& cf = db.find_column_family(it->first);
                co_await cf.update_effective_replication_map(std::move(it->second));
                if (cf.uses_tablets()) {
                    register_tablet_split_candidate(it->first);
                }
                it = table_erms.erase(it);
            }

            auto& session_mgr = get_topology_session_manager();
            session_mgr.initiate_close_of_sessions_except(open_sessions);
            for (auto id : open_sessions) {
                session_mgr.create_session(id);
            }
        });
    } catch (...) {
        // applying the changes on all shards should never fail
        // it will end up in an inconsistent state that we can't recover from.
        slogger.error("Failed to apply token_metadata changes: {}. Aborting.", std::current_exception());
        abort();
    }
}

future<> storage_service::stop() {
    co_await uninit_messaging_service();
    // make sure nobody uses the semaphore
    node_ops_signal_abort(std::nullopt);
    _listeners.clear();
    co_await _async_gate.close();
    co_await std::move(_node_ops_abort_thread);
    _tablet_split_monitor_event.signal();
    co_await std::move(_tablet_split_monitor);
}

future<> storage_service::wait_for_group0_stop() {
    _group0_as.request_abort();
    _topology_state_machine.event.broken(make_exception_ptr(abort_requested_exception()));
    co_await when_all(std::move(_raft_state_monitor), std::move(_sstable_cleanup_fiber), std::move(_upgrade_to_topology_coordinator_fiber));
}

future<> storage_service::check_for_endpoint_collision(std::unordered_set<gms::inet_address> initial_contact_nodes, const std::unordered_map<gms::inet_address, sstring>& loaded_peer_features) {
    slogger.debug("Starting shadow gossip round to check for endpoint collision");

    return seastar::async([this, initial_contact_nodes, loaded_peer_features] {
        auto t = gms::gossiper::clk::now();
        bool found_bootstrapping_node = false;
        auto local_features = _feature_service.supported_feature_set();
        do {
            slogger.info("Performing gossip shadow round");
            _gossiper.do_shadow_round(initial_contact_nodes, gms::gossiper::mandatory::yes).get();
            if (!raft_topology_change_enabled()) {
                _gossiper.check_knows_remote_features(local_features, loaded_peer_features);
            }
            _gossiper.check_snitch_name_matches(_snitch.local()->get_name());
            auto addr = get_broadcast_address();
            if (!_gossiper.is_safe_for_bootstrap(addr)) {
                throw std::runtime_error(::format("A node with address {} already exists, cancelling join. "
                    "Use replace_address if you want to replace this node.", addr));
            }
            if (_db.local().get_config().consistent_rangemovement() &&
                // Raft is responsible for consistency, so in case it is enable no need to check here
                !raft_topology_change_enabled()) {
                found_bootstrapping_node = false;
                for (const auto& addr : _gossiper.get_endpoints()) {
                    auto state = _gossiper.get_gossip_status(addr);
                    if (state == sstring(versioned_value::STATUS_UNKNOWN)) {
                        throw std::runtime_error(::format("Node {} has gossip status=UNKNOWN. Try fixing it before adding new node to the cluster.", addr));
                    }
                    slogger.debug("Checking bootstrapping/leaving/moving nodes: node={}, status={} (check_for_endpoint_collision)", addr, state);
                    if (state == sstring(versioned_value::STATUS_BOOTSTRAPPING)) {
                        if (gms::gossiper::clk::now() > t + std::chrono::seconds(60)) {
                            throw std::runtime_error("Other bootstrapping/leaving/moving nodes detected, cannot bootstrap while consistent_rangemovement is true (check_for_endpoint_collision)");
                        } else {
                            sstring saved_state(state);
                            _gossiper.goto_shadow_round();
                            _gossiper.reset_endpoint_state_map().get();
                            found_bootstrapping_node = true;
                            auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(gms::gossiper::clk::now() - t).count();
                            slogger.info("Checking bootstrapping/leaving/moving nodes: node={}, status={}, sleep 1 second and check again ({} seconds elapsed) (check_for_endpoint_collision)", addr, saved_state, elapsed);
                            sleep_abortable(std::chrono::seconds(1), _abort_source).get();
                            break;
                        }
                    }
                }
            }
        } while (found_bootstrapping_node);
        slogger.info("Checking bootstrapping/leaving/moving nodes: ok (check_for_endpoint_collision)");
        _gossiper.reset_endpoint_state_map().get();
    });
}

future<> storage_service::remove_endpoint(inet_address endpoint, gms::permit_id pid) {
    co_await _gossiper.remove_endpoint(endpoint, pid);
    try {
        co_await _sys_ks.local().remove_endpoint(endpoint);
    } catch (...) {
        slogger.error("fail to remove endpoint={}: {}", endpoint, std::current_exception());
    }
}

future<storage_service::replacement_info>
storage_service::prepare_replacement_info(std::unordered_set<gms::inet_address> initial_contact_nodes, const std::unordered_map<gms::inet_address, sstring>& loaded_peer_features) {
    locator::host_id replace_host_id;
    gms::inet_address replace_address;

    auto& cfg = _db.local().get_config();
    if (!cfg.replace_node_first_boot().empty()) {
        replace_host_id = locator::host_id(utils::UUID(cfg.replace_node_first_boot()));
    } else if (!cfg.replace_address_first_boot().empty()) {
        replace_address = gms::inet_address(cfg.replace_address_first_boot());
        slogger.warn("The replace_address_first_boot={} option is deprecated. Please use the replace_node_first_boot option", replace_address);
    } else if (!cfg.replace_address().empty()) {
        replace_address = gms::inet_address(cfg.replace_address());
        slogger.warn("The replace_address={} option is deprecated. Please use the replace_node_first_boot option", replace_address);
    } else {
        on_internal_error(slogger, "No replace_node or replace_address configuration options found");
    }

    slogger.info("Gathering node replacement information for {}/{}", replace_host_id, replace_address);

    auto seeds = _gossiper.get_seeds();
    if (seeds.size() == 1 && seeds.contains(replace_address)) {
        throw std::runtime_error(::format("Cannot replace_address {} because no seed node is up", replace_address));
    }

    // make magic happen
    slogger.info("Performing gossip shadow round");
    co_await _gossiper.do_shadow_round(initial_contact_nodes, gms::gossiper::mandatory::yes);
    if (!raft_topology_change_enabled()) {
        auto local_features = _feature_service.supported_feature_set();
        _gossiper.check_knows_remote_features(local_features, loaded_peer_features);
    }

    // now that we've gossiped at least once, we should be able to find the node we're replacing
    if (replace_host_id) {
        auto nodes = _gossiper.get_nodes_with_host_id(replace_host_id);
        if (nodes.empty()) {
            throw std::runtime_error(::format("Replaced node with Host ID {} not found", replace_host_id));
        }
        if (nodes.size() > 1) {
            throw std::runtime_error(::format("Found multiple nodes with Host ID {}: {}", replace_host_id, nodes));
        }
        replace_address = *nodes.begin();
    }

    auto state = _gossiper.get_endpoint_state_ptr(replace_address);
    if (!state) {
        throw std::runtime_error(::format("Cannot replace_address {} because it doesn't exist in gossip", replace_address));
    }

    // Reject to replace a node that has left the ring
    auto status = _gossiper.get_gossip_status(replace_address);
    if (status == gms::versioned_value::STATUS_LEFT || status == gms::versioned_value::REMOVED_TOKEN) {
        throw std::runtime_error(::format("Cannot replace_address {} because it has left the ring, status={}", replace_address, status));
    }

    std::unordered_set<dht::token> tokens;
    if (!raft_topology_change_enabled()) {
        tokens = get_tokens_for(replace_address);
        if (tokens.empty()) {
            throw std::runtime_error(::format("Could not find tokens for {} to replace", replace_address));
        }
    }

    auto dc_rack = get_dc_rack_for(replace_address).value_or(locator::endpoint_dc_rack::default_location);

    if (!replace_host_id) {
        replace_host_id = _gossiper.get_host_id(replace_address);
    }
    slogger.info("Host {}/{} is replacing {}/{}", get_token_metadata().get_my_id(), get_broadcast_address(), replace_host_id, replace_address);
    co_await _gossiper.reset_endpoint_state_map();

    co_return replacement_info {
        .tokens = std::move(tokens),
        .dc_rack = std::move(dc_rack),
        .host_id = std::move(replace_host_id),
        .address = replace_address,
    };
}

future<std::map<gms::inet_address, float>> storage_service::get_ownership() {
    return run_with_no_api_lock([] (storage_service& ss) {
        const auto& tm = ss.get_token_metadata();
        auto token_map = dht::token::describe_ownership(tm.sorted_tokens());
        // describeOwnership returns tokens in an unspecified order, let's re-order them
        std::map<gms::inet_address, float> ownership;
        for (auto entry : token_map) {
            locator::host_id id = tm.get_endpoint(entry.first).value();
            auto token_ownership = entry.second;
            ownership[tm.get_endpoint_for_host_id(id)] += token_ownership;
        }
        return ownership;
    });
}

future<std::map<gms::inet_address, float>> storage_service::effective_ownership(sstring keyspace_name, sstring table_name) {
    return run_with_no_api_lock([keyspace_name, table_name] (storage_service& ss) mutable -> future<std::map<gms::inet_address, float>> {
        locator::effective_replication_map_ptr erm;
        if (keyspace_name != "") {
            //find throws no such keyspace if it is missing
            const replica::keyspace& ks = ss._db.local().find_keyspace(keyspace_name);
            // This is ugly, but it follows origin
            auto&& rs = ks.get_replication_strategy();  // clang complains about typeid(ks.get_replication_strategy());
            if (typeid(rs) == typeid(locator::local_strategy)) {
                throw std::runtime_error("Ownership values for keyspaces with LocalStrategy are meaningless");
            }

            if (table_name.empty()) {
                erm = ks.get_vnode_effective_replication_map();
            } else {
                auto& cf = ss._db.local().find_column_family(keyspace_name, table_name);
                erm = cf.get_effective_replication_map();
            }
        } else {
            auto non_system_keyspaces = ss._db.local().get_non_system_keyspaces();

            //system_traces is a non-system keyspace however it needs to be counted as one for this process
            size_t special_table_count = 0;
            if (std::find(non_system_keyspaces.begin(), non_system_keyspaces.end(), "system_traces") !=
                    non_system_keyspaces.end()) {
                special_table_count += 1;
            }
            if (non_system_keyspaces.size() > special_table_count) {
                throw std::runtime_error("Non-system keyspaces don't have the same replication settings, effective ownership information is meaningless");
            }
            keyspace_name = "system_traces";
            const auto& ks = ss._db.local().find_keyspace(keyspace_name);
            erm = ks.get_vnode_effective_replication_map();
        }

        // The following loops seems computationally heavy, but it's not as bad.
        // The upper two simply iterate over all the endpoints by iterating over all the
        // DC and all the instances in each DC.
        //
        // The call for get_range_for_endpoint is done once per endpoint
        const auto& tm = *erm->get_token_metadata_ptr();
        const auto tokens = co_await std::invoke([&]() -> future<std::vector<token>> {
            if (!erm->get_replication_strategy().uses_tablets()) {
                return make_ready_future<std::vector<token>>(tm.sorted_tokens());
            } else {
                auto& cf = ss._db.local().find_column_family(keyspace_name, table_name);
                const auto& tablets = tm.tablets().get_tablet_map(cf.schema()->id());
                return tablets.get_sorted_tokens();
            }
        });

        const auto token_ownership = dht::token::describe_ownership(tokens);
        const auto datacenter_endpoints = tm.get_topology().get_datacenter_endpoints();
        std::map<gms::inet_address, float> final_ownership;

        for (const auto& [dc, endpoints_map] : datacenter_endpoints) {
            for (auto endpoint : endpoints_map) {
                // calculate the ownership with replication and add the endpoint to the final ownership map
                try {
                    float ownership = 0.0f;
                    auto ranges = ss.get_ranges_for_endpoint(erm, endpoint);
                    for (auto& r : ranges) {
                        // get_ranges_for_endpoint will unwrap the first range.
                        // With t0 t1 t2 t3, the first range (t3,t0] will be split
                        // as (min,t0] and (t3,max]. Skippping the range (t3,max]
                        // we will get the correct ownership number as if the first
                        // range were not split.
                        if (!r.end()) {
                            continue;
                        }
                        auto end_token = r.end()->value();
                        auto loc = token_ownership.find(end_token);
                        if (loc != token_ownership.end()) {
                            ownership += loc->second;
                        }
                    }
                    final_ownership[endpoint] = ownership;
                }  catch (replica::no_such_keyspace&) {
                    // In case ss.get_ranges_for_endpoint(keyspace_name, endpoint) is not found, just mark it as zero and continue
                    final_ownership[endpoint] = 0;
                }
            }
        }
        co_return final_ownership;
    });
}

void storage_service::set_mode(mode m) {
    if (m == mode::MAINTENANCE && _operation_mode != mode::NONE) {
        // Prevent from calling `start_maintenance_mode` after `join_cluster`.
        on_fatal_internal_error(slogger, format("Node should enter maintenance mode only from mode::NONE (current mode: {})", _operation_mode));
    }
    if (m == mode::STARTING && _operation_mode == mode::MAINTENANCE) {
        // Prevent from calling `join_cluster` after `start_maintenance_mode`.
        on_fatal_internal_error(slogger, "Node in the maintenance mode cannot enter the starting mode");
    }

    if (m != _operation_mode) {
        slogger.info("entering {} mode", m);
        _operation_mode = m;
    } else {
        // This shouldn't happen, but it's too much for an assert,
        // so -- just emit a warning in the hope that it will be
        // noticed, reported and fixed
        slogger.warn("re-entering {} mode", m);
    }
}

sstring storage_service::get_release_version() {
    return version::release();
}

sstring storage_service::get_schema_version() {
    return _db.local().get_version().to_sstring();
}

static constexpr auto UNREACHABLE = "UNREACHABLE";

future<std::unordered_map<sstring, std::vector<sstring>>> storage_service::describe_schema_versions() {
    auto live_hosts = _gossiper.get_live_members();
    std::unordered_map<sstring, std::vector<sstring>> results;
    netw::messaging_service& ms = _messaging.local();
    return map_reduce(std::move(live_hosts), [&ms] (auto host) {
        auto f0 = ms.send_schema_check(netw::msg_addr{ host, 0 });
        return std::move(f0).then_wrapped([host] (auto f) {
            if (f.failed()) {
                f.ignore_ready_future();
                return std::pair<gms::inet_address, std::optional<table_schema_version>>(host, std::nullopt);
            }
            return std::pair<gms::inet_address, std::optional<table_schema_version>>(host, f.get());
        });
    }, std::move(results), [] (auto results, auto host_and_version) {
        auto version = host_and_version.second ? host_and_version.second->to_sstring() : UNREACHABLE;
        results.try_emplace(version).first->second.emplace_back(host_and_version.first.to_sstring());
        return results;
    }).then([this] (auto results) {
        // we're done: the results map is ready to return to the client.  the rest is just debug logging:
        auto it_unreachable = results.find(UNREACHABLE);
        if (it_unreachable != results.end()) {
            slogger.debug("Hosts not in agreement. Didn't get a response from everybody: {}", fmt::join(it_unreachable->second, ","));
        }
        auto my_version = get_schema_version();
        for (auto&& entry : results) {
            // check for version disagreement. log the hosts that don't agree.
            if (entry.first == UNREACHABLE || entry.first == my_version) {
                continue;
            }
            for (auto&& host : entry.second) {
                slogger.debug("{} disagrees ({})", host, entry.first);
            }
        }
        if (results.size() == 1) {
            slogger.debug("Schemas are in agreement.");
        }
        return results;
    });
};

future<storage_service::mode> storage_service::get_operation_mode() {
    return run_with_no_api_lock([] (storage_service& ss) {
        return make_ready_future<mode>(ss._operation_mode);
    });
}

future<bool> storage_service::is_gossip_running() {
    return run_with_no_api_lock([] (storage_service& ss) {
        return ss._gossiper.is_enabled();
    });
}

future<> storage_service::start_gossiping() {
    return run_with_api_lock(sstring("start_gossiping"), [] (storage_service& ss) -> future<> {
        if (!ss._gossiper.is_enabled()) {
            slogger.warn("Starting gossip by operator request");
            co_await ss._gossiper.container().invoke_on_all(&gms::gossiper::start);
            bool should_stop_gossiper = false; // undo action
            try {
                auto cdc_gen_ts = co_await ss._sys_ks.local().get_cdc_generation_id();
                if (!cdc_gen_ts) {
                    cdc_log.warn("CDC generation timestamp missing when starting gossip");
                }
                co_await set_gossip_tokens(ss._gossiper,
                        co_await ss._sys_ks.local().get_local_tokens(),
                        cdc_gen_ts);
                ss._gossiper.force_newer_generation();
                co_await ss._gossiper.start_gossiping(gms::get_generation_number());
            } catch (...) {
                should_stop_gossiper = true;
            }
            if (should_stop_gossiper) {
                co_await ss._gossiper.container().invoke_on_all(&gms::gossiper::stop);
            }
        }
    });
}

future<> storage_service::stop_gossiping() {
    return run_with_api_lock(sstring("stop_gossiping"), [] (storage_service& ss) {
        if (ss._gossiper.is_enabled()) {
            slogger.warn("Stopping gossip by operator request");
            return ss._gossiper.container().invoke_on_all(&gms::gossiper::stop);
        }
        return make_ready_future<>();
    });
}

static
void on_streaming_finished() {
    utils::get_local_injector().inject("storage_service_streaming_sleep3", std::chrono::seconds{3}).get();
}

future<> storage_service::raft_decommission() {
    auto& raft_server = _group0->group0_server();
    utils::UUID request_id;

    while (true) {
        auto guard = co_await _group0->client().start_operation(&_group0_as, raft_timeout{});

        auto it = _topology_state_machine._topology.find(raft_server.id());
        if (!it) {
            throw std::runtime_error(::format("local node {} is not a member of the cluster", raft_server.id()));
        }

        const auto& rs = it->second;

        if (rs.state != node_state::normal) {
            throw std::runtime_error(::format("local node is not in the normal state (current state: {})", rs.state));
        }

        if (_topology_state_machine._topology.normal_nodes.size() == 1) {
            throw std::runtime_error("Cannot decommission last node in the cluster");
        }

        rtlogger.info("request decommission for: {}", raft_server.id());
        topology_mutation_builder builder(guard.write_timestamp());
        builder.with_node(raft_server.id())
               .set("topology_request", topology_request::leave)
               .set("request_id", guard.new_group0_state_id());
        topology_request_tracking_mutation_builder rtbuilder(guard.new_group0_state_id());
        rtbuilder.set("initiating_host",_group0->group0_server().id().uuid())
                 .set("done", false);
        topology_change change{{builder.build(), rtbuilder.build()}};
        group0_command g0_cmd = _group0->client().prepare_command(std::move(change), guard, ::format("decommission: request decommission for {}", raft_server.id()));

        request_id = guard.new_group0_state_id();
        try {
            co_await _group0->client().add_entry(std::move(g0_cmd), std::move(guard), &_group0_as, raft_timeout{});
        } catch (group0_concurrent_modification&) {
            rtlogger.info("decommission: concurrent operation is detected, retrying.");
            continue;
        }
        break;
    }

    auto error = co_await wait_for_topology_request_completion(request_id);

    if (error.empty()) {
        // Need to set it otherwise gossiper will try to send shutdown on exit
        co_await _gossiper.add_local_application_state({{ gms::application_state::STATUS, gms::versioned_value::left({}, _gossiper.now().time_since_epoch().count()) }});
    } else  {
        auto err = fmt::format("Decommission failed. See earlier errors ({})", error);
        rtlogger.error("{}", err);
        throw std::runtime_error(err);
    }
}

future<> storage_service::decommission() {
    return run_with_api_lock(sstring("decommission"), [] (storage_service& ss) {
        return seastar::async([&ss] {
            ss.check_ability_to_perform_topology_operation("decommission");
            std::exception_ptr leave_group0_ex;
            if (ss.raft_topology_change_enabled()) {
                ss.raft_decommission().get();
            } else {
                bool left_token_ring = false;
                auto uuid = node_ops_id::create_random_id();
                auto& db = ss._db.local();
                node_ops_ctl ctl(ss, node_ops_cmd::decommission_prepare, ss.get_token_metadata().get_my_id(), ss.get_broadcast_address());
                auto stop_ctl = deferred_stop(ctl);

                // Step 1: Decide who needs to sync data
                // TODO: wire ignore_nodes provided by user
                ctl.start("decommission");

                uuid = ctl.uuid();
                auto endpoint = ctl.endpoint;
                const auto& tmptr = ctl.tmptr;
                if (!tmptr->is_normal_token_owner(ctl.host_id)) {
                    throw std::runtime_error("local node is not a member of the token ring yet");
                }
                // We assume that we're a member of group 0 if we're in decommission()` and Raft is enabled.
                // We have no way to check that we're not a member: attempting to perform group 0 operations
                // would simply hang in that case, the leader would refuse to talk to us.
                // If we aren't a member then we shouldn't be here anyway, since it means that either
                // an earlier decommission finished (leave_group0 is the last operation in decommission)
                // or that we were removed using `removenode`.
                //
                // For handling failure scenarios such as a group 0 member that is not a token ring member,
                // there's `removenode`.

                auto temp = tmptr->clone_after_all_left().get();
                auto num_tokens_after_all_left = temp.sorted_tokens().size();
                temp.clear_gently().get();
                if (num_tokens_after_all_left < 2) {
                    throw std::runtime_error("no other normal nodes in the ring; decommission would be pointless");
                }

                if (ss._operation_mode != mode::NORMAL) {
                    throw std::runtime_error(::format("Node in {} state; wait for status to become normal or restart", ss._operation_mode));
                }

                ss.update_topology_change_info(::format("decommission {}", endpoint)).get();

                auto non_system_keyspaces = db.get_non_local_vnode_based_strategy_keyspaces();
                for (const auto& keyspace_name : non_system_keyspaces) {
                    if (ss._db.local().find_keyspace(keyspace_name).get_vnode_effective_replication_map()->has_pending_ranges(ss.get_token_metadata_ptr()->get_my_id())) {
                        throw std::runtime_error("data is currently moving to this node; unable to leave the ring");
                    }
                }

                slogger.info("DECOMMISSIONING: starts");
                ctl.req.leaving_nodes = std::list<gms::inet_address>{endpoint};

                assert(ss._group0);
                bool raft_available = ss._group0->wait_for_raft().get();

                try {
                    // Step 2: Start heartbeat updater
                    ctl.start_heartbeat_updater(node_ops_cmd::decommission_heartbeat);

                    // Step 3: Prepare to sync data
                    ctl.prepare(node_ops_cmd::decommission_prepare).get();

                    // Step 4: Start to sync data
                    slogger.info("DECOMMISSIONING: unbootstrap starts");
                    ss.unbootstrap().get();
                    on_streaming_finished();
                    slogger.info("DECOMMISSIONING: unbootstrap done");

                    // Step 5: Become a group 0 non-voter before leaving the token ring.
                    //
                    // Thanks to this, even if we fail after leaving the token ring but before leaving group 0,
                    // group 0's availability won't be reduced.
                    if (raft_available) {
                        slogger.info("decommission[{}]: becoming a group 0 non-voter", uuid);
                        ss._group0->become_nonvoter(ss._group0_as).get();
                        slogger.info("decommission[{}]: became a group 0 non-voter", uuid);
                    }

                    // Step 6: Verify that other nodes didn't abort in the meantime.
                    // See https://github.com/scylladb/scylladb/issues/12989.
                    ctl.query_pending_op().get();

                    // Step 7: Leave the token ring
                    slogger.info("decommission[{}]: leaving token ring", uuid);
                    ss.leave_ring().get();
                    left_token_ring = true;
                    slogger.info("decommission[{}]: left token ring", uuid);

                    // Step 8: Finish token movement
                    ctl.done(node_ops_cmd::decommission_done).get();
                } catch (...) {
                    ctl.abort_on_error(node_ops_cmd::decommission_abort, std::current_exception()).get();
                }

                // Step 8: Leave group 0
                //
                // If the node failed to leave the token ring, don't remove it from group 0
                // --- hence the `left_token_ring` check.
                try {
                    utils::get_local_injector().inject("decommission_fail_before_leave_group0",
                        [] { throw std::runtime_error("decommission_fail_before_leave_group0"); });

                    if (raft_available && left_token_ring) {
                        slogger.info("decommission[{}]: leaving Raft group 0", uuid);
                        assert(ss._group0);
                        ss._group0->leave_group0().get();
                        slogger.info("decommission[{}]: left Raft group 0", uuid);
                    }
                } catch (...) {
                    // Even though leave_group0 failed, we will finish decommission and shut down everything.
                    // There's nothing smarter we could do. We should not continue operating in this broken
                    // state (we're not a member of the token ring any more).
                    //
                    // If we didn't manage to leave group 0, we will stay as a non-voter
                    // (which is not too bad - non-voters at least do not reduce group 0's availability).
                    // It's possible to remove the garbage member using `removenode`.
                    slogger.error(
                        "decommission[{}]: FAILED when trying to leave Raft group 0: \"{}\". This node"
                        " is no longer a member of the token ring, so it will finish shutting down its services."
                        " It may still be a member of Raft group 0. To remove it, shut it down and use `removenode`."
                        " Consult the `decommission` and `removenode` documentation for more details.",
                        uuid, std::current_exception());
                    leave_group0_ex = std::current_exception();
                }
            }

            ss.stop_transport().get();
            slogger.info("DECOMMISSIONING: stopped transport");

            ss.get_batchlog_manager().invoke_on_all([] (auto& bm) {
                return bm.drain();
            }).get();
            slogger.info("DECOMMISSIONING: stop batchlog_manager done");

            // StageManager.shutdownNow();
            ss._sys_ks.local().set_bootstrap_state(db::system_keyspace::bootstrap_state::DECOMMISSIONED).get();
            slogger.info("DECOMMISSIONING: set_bootstrap_state done");
            ss.set_mode(mode::DECOMMISSIONED);

            if (leave_group0_ex) {
                std::rethrow_exception(leave_group0_ex);
            }

            slogger.info("DECOMMISSIONING: done");
            // let op be responsible for killing the process
        });
    });
}

// Runs inside seastar::async context
void storage_service::run_bootstrap_ops(std::unordered_set<token>& bootstrap_tokens) {
    node_ops_ctl ctl(*this, node_ops_cmd::bootstrap_prepare, get_token_metadata().get_my_id(), get_broadcast_address());
    auto stop_ctl = deferred_stop(ctl);
    const auto& uuid = ctl.uuid();

    // Step 1: Decide who needs to sync data for bootstrap operation
    // TODO: Specify ignore_nodes
    ctl.start("bootstrap");

    auto start_time = std::chrono::steady_clock::now();
    for (;;) {
        ctl.sync_nodes.insert(get_broadcast_address());

        // Step 2: Wait until no pending node operations
        std::unordered_map<gms::inet_address, std::list<node_ops_id>> pending_ops;
        auto req = node_ops_cmd_request(node_ops_cmd::query_pending_ops, uuid);
        parallel_for_each(ctl.sync_nodes, [this, req, uuid, &pending_ops] (const gms::inet_address& node) {
            return _messaging.local().send_node_ops_cmd(netw::msg_addr(node), req).then([uuid, node, &pending_ops] (node_ops_cmd_response resp) {
                slogger.debug("bootstrap[{}]: Got query_pending_ops response from node={}, resp.pending_ops={}", uuid, node, resp.pending_ops);
                if (!resp.pending_ops.empty()) {
                    pending_ops.emplace(node, resp.pending_ops);
                }
                return make_ready_future<>();
            });
        }).handle_exception([uuid] (std::exception_ptr ep) {
            slogger.warn("bootstrap[{}]: Failed to query_pending_ops : {}", uuid, ep);
        }).get();
        if (pending_ops.empty()) {
            break;
        } else {
            if (std::chrono::steady_clock::now() > start_time + std::chrono::seconds(60)) {
                throw std::runtime_error(::format("bootstrap[{}]: Found pending node ops = {}, reject bootstrap", uuid, pending_ops));
            }
            slogger.warn("bootstrap[{}]: Found pending node ops = {}, sleep 5 seconds and check again", uuid, pending_ops);
            sleep_abortable(std::chrono::seconds(5), _abort_source).get();
            ctl.refresh_sync_nodes();
            // the bootstrapping node will be added back when we loop
        }
    }

    auto tokens = std::list<dht::token>(bootstrap_tokens.begin(), bootstrap_tokens.end());
    ctl.req.bootstrap_nodes = {
        {get_broadcast_address(), tokens},
    };
    try {
        // Step 2: Start heartbeat updater
        ctl.start_heartbeat_updater(node_ops_cmd::bootstrap_heartbeat);

        // Step 3: Prepare to sync data
        ctl.prepare(node_ops_cmd::bootstrap_prepare).get();

        // Step 5: Sync data for bootstrap
        _repair.local().bootstrap_with_repair(get_token_metadata_ptr(), bootstrap_tokens).get();
        on_streaming_finished();

        // Step 6: Finish
        ctl.done(node_ops_cmd::bootstrap_done).get();
    } catch (...) {
        ctl.abort_on_error(node_ops_cmd::bootstrap_abort, std::current_exception()).get();
    }
}

// Runs inside seastar::async context
void storage_service::run_replace_ops(std::unordered_set<token>& bootstrap_tokens, replacement_info replace_info) {
    node_ops_ctl ctl(*this, node_ops_cmd::replace_prepare, replace_info.host_id, replace_info.address);
    auto stop_ctl = deferred_stop(ctl);
    const auto& uuid = ctl.uuid();
    gms::inet_address replace_address = replace_info.address;
    ctl.ignore_nodes = parse_node_list(_db.local().get_config().ignore_dead_nodes_for_replace(), *ctl.tmptr);
    // Step 1: Decide who needs to sync data for replace operation
    // The replacing node is not a normal token owner yet
    // Add it back explicitly after checking all other nodes.
    ctl.start("replace", [&] (gms::inet_address node) {
        return node != replace_address;
    });
    ctl.sync_nodes.insert(get_broadcast_address());

    auto sync_nodes_generations = _gossiper.get_generation_for_nodes(ctl.sync_nodes).get();
    // Map existing nodes to replacing nodes
    ctl.req.replace_nodes = {
        {replace_address, get_broadcast_address()},
    };
    try {
        // Step 2: Start heartbeat updater
        ctl.start_heartbeat_updater(node_ops_cmd::replace_heartbeat);

        // Step 3: Prepare to sync data
        ctl.prepare(node_ops_cmd::replace_prepare).get();

        // Step 4: Allow nodes in sync_nodes list to mark the replacing node as alive
        _gossiper.advertise_to_nodes(sync_nodes_generations).get();
        slogger.info("replace[{}]: Allow nodes={} to mark replacing node={} as alive", uuid, ctl.sync_nodes, get_broadcast_address());

        // Step 5: Wait for nodes to finish marking the replacing node as live
        ctl.send_to_all(node_ops_cmd::replace_prepare_mark_alive).get();

        // Step 6: Update pending ranges on nodes
        ctl.send_to_all(node_ops_cmd::replace_prepare_pending_ranges).get();

        // Step 7: Sync data for replace
        if (is_repair_based_node_ops_enabled(streaming::stream_reason::replace)) {
            slogger.info("replace[{}]: Using repair based node ops to sync data", uuid);
            _repair.local().replace_with_repair(get_token_metadata_ptr(), bootstrap_tokens, ctl.ignore_nodes).get();
        } else {
            slogger.info("replace[{}]: Using streaming based node ops to sync data", uuid);
            dht::boot_strapper bs(_db, _stream_manager, _abort_source, get_token_metadata_ptr()->get_my_id(), _snitch.local()->get_location(), bootstrap_tokens, get_token_metadata_ptr());
            bs.bootstrap(streaming::stream_reason::replace, _gossiper, null_topology_guard, replace_address).get();
        }
        on_streaming_finished();

        // Step 8: Finish
        ctl.done(node_ops_cmd::replace_done).get();

        // Allow any nodes to mark the replacing node as alive
        _gossiper.advertise_to_nodes({}).get();
        slogger.info("replace[{}]: Allow any nodes to mark replacing node={} as alive", uuid,  get_broadcast_address());
    } catch (...) {
        // we need to revert the effect of prepare verb the replace ops is failed
        ctl.abort_on_error(node_ops_cmd::replace_abort, std::current_exception()).get();
    }
}

future<> storage_service::raft_removenode(locator::host_id host_id, std::list<locator::host_id_or_endpoint> ignore_nodes_params) {
    auto id = raft::server_id{host_id.uuid()};
    utils::UUID request_id;

    while (true) {
        auto guard = co_await _group0->client().start_operation(&_group0_as, raft_timeout{});

        auto it = _topology_state_machine._topology.find(id);

        if (!it) {
            throw std::runtime_error(::format("removenode: host id {} is not found in the cluster", host_id));
        }

        auto& rs = it->second; // not usable after yield

        if (rs.state != node_state::normal) {
            throw std::runtime_error(::format("removenode: node {} is in '{}' state. Wait for it to be in 'normal' state", id, rs.state));
        }
        const auto& am = _group0->address_map();
        auto ip = am.find(id);
        if (!ip) {
            // What to do if there is no mapping? Wait and retry?
            on_fatal_internal_error(rtlogger, ::format("Remove node cannot find a mapping from node id {} to its ip", id));
        }

        if (_gossiper.is_alive(*ip)) {
            const std::string message = ::format(
                "removenode: Rejected removenode operation for node {} ip {} "
                "the node being removed is alive, maybe you should use decommission instead?",
                id, *ip);
            rtlogger.warn("{}", message);
            throw std::runtime_error(message);
        }

        auto ignored_ids = find_raft_nodes_from_hoeps(ignore_nodes_params);
        if (!ignored_ids.empty()) {
            auto bad_id = std::find_if_not(ignored_ids.begin(), ignored_ids.end(), [&] (auto n) {
                return _topology_state_machine._topology.normal_nodes.contains(n);
            });
            if (bad_id != ignored_ids.end()) {
                throw std::runtime_error(::format("removenode: there is no node with id {} in normal state. Cannot ignore it.", *bad_id));
            }
        }
        // insert node that should be removed to ignore list so that other topology operations
        // can ignore it
        ignored_ids.insert(id);

        rtlogger.info("request removenode for: {}, new ignored nodes: {}, existing ignore nodes: {}", id, ignored_ids, _topology_state_machine._topology.ignored_nodes);
        topology_mutation_builder builder(guard.write_timestamp());
        builder.add_ignored_nodes(ignored_ids).with_node(id)
               .set("topology_request", topology_request::remove)
               .set("request_id", guard.new_group0_state_id());
        topology_request_tracking_mutation_builder rtbuilder(guard.new_group0_state_id());
        rtbuilder.set("initiating_host",_group0->group0_server().id().uuid())
                 .set("done", false);
        topology_change change{{builder.build(), rtbuilder.build()}};
        group0_command g0_cmd = _group0->client().prepare_command(std::move(change), guard, ::format("removenode: request remove for {}", id));

        request_id = guard.new_group0_state_id();
        try {
            // Make non voter during request submission for better HA
            co_await _group0->make_nonvoters(ignored_ids, _group0_as, raft_timeout{});
            co_await _group0->client().add_entry(std::move(g0_cmd), std::move(guard), &_group0_as, raft_timeout{});
        } catch (group0_concurrent_modification&) {
            rtlogger.info("removenode: concurrent operation is detected, retrying.");
            continue;
        }

        break;
    }

    rtlogger.info("removenode: wait for completion");

    // Wait until request completes
    auto error = co_await wait_for_topology_request_completion(request_id);

    if (error.empty()) {
        try {
            co_await _group0->remove_from_raft_config(id);
        } catch (raft::not_a_member&) {
            rtlogger.info("removenode: already removed from the raft config by the topology coordinator");
        }
    } else {
        auto err = fmt::format("Removenode failed. See earlier errors ({})", error);
        rtlogger.error("{}", err);
        throw std::runtime_error(err);
    }
}

future<> storage_service::removenode(locator::host_id host_id, std::list<locator::host_id_or_endpoint> ignore_nodes_params) {
    return run_with_api_lock(sstring("removenode"), [host_id, ignore_nodes_params = std::move(ignore_nodes_params)] (storage_service& ss) mutable {
        return seastar::async([&ss, host_id, ignore_nodes_params = std::move(ignore_nodes_params)] () mutable {
            ss.check_ability_to_perform_topology_operation("removenode");
            if (ss.raft_topology_change_enabled()) {
                ss.raft_removenode(host_id, std::move(ignore_nodes_params)).get();
                return;
            }
            node_ops_ctl ctl(ss, node_ops_cmd::removenode_prepare, host_id, gms::inet_address());
            auto stop_ctl = deferred_stop(ctl);
            auto uuid = ctl.uuid();
            const auto& tmptr = ctl.tmptr;
            auto endpoint_opt = tmptr->get_endpoint_for_host_id_if_known(host_id);
            assert(ss._group0);
            auto raft_id = raft::server_id{host_id.uuid()};
            bool raft_available = ss._group0->wait_for_raft().get();
            bool is_group0_member = raft_available && ss._group0->is_member(raft_id, false);
            if (!endpoint_opt && !is_group0_member) {
                throw std::runtime_error(::format("removenode[{}]: Node {} not found in the cluster", uuid, host_id));
            }

            // If endpoint_opt is engaged, the node is a member of the token ring.
            // is_group0_member indicates whether the node is a member of Raft group 0.
            // A node might be a member of group 0 but not a member of the token ring, e.g. due to a
            // previously failed removenode/decommission. The code is written to handle this
            // situation. Parts related to removing this node from the token ring are conditioned on
            // endpoint_opt, while parts related to removing from group 0 are conditioned on
            // is_group0_member.

            if (endpoint_opt && ss._gossiper.is_alive(*endpoint_opt)) {
                const std::string message = ::format(
                    "removenode[{}]: Rejected removenode operation (node={}); "
                    "the node being removed is alive, maybe you should use decommission instead?",
                    uuid, *endpoint_opt);
                slogger.warn("{}", message);
                throw std::runtime_error(message);
            }

            for (auto& hoep : ignore_nodes_params) {
                hoep.resolve(*tmptr);
                ctl.ignore_nodes.insert(hoep.endpoint);
            }

            bool removed_from_token_ring = !endpoint_opt;
            if (endpoint_opt) {
                auto endpoint = *endpoint_opt;
                ctl.endpoint = endpoint;

                // Step 1: Make the node a group 0 non-voter before removing it from the token ring.
                //
                // Thanks to this, even if we fail after removing the node from the token ring
                // but before removing it group 0, group 0's availability won't be reduced.
                if (is_group0_member && ss._group0->is_member(raft_id, true)) {
                    slogger.info("removenode[{}]: making node {} a non-voter in group 0", uuid, raft_id);
                    ss._group0->make_nonvoter(raft_id, ss._group0_as).get();
                    slogger.info("removenode[{}]: made node {} a non-voter in group 0", uuid, raft_id);
                }

                // Step 2: Decide who needs to sync data
                //
                // By default, we require all nodes in the cluster to participate
                // the removenode operation and sync data if needed. We fail the
                // removenode operation if any of them is down or fails.
                //
                // If the user want the removenode operation to succeed even if some of the nodes
                // are not available, the user has to explicitly pass a list of
                // node that can be skipped for the operation.
                ctl.start("removenode", [&] (gms::inet_address node) {
                    return node != endpoint;
                });

                auto tokens = tmptr->get_tokens(host_id);

                try {
                    // Step 3: Start heartbeat updater
                    ctl.start_heartbeat_updater(node_ops_cmd::removenode_heartbeat);

                    // Step 4: Prepare to sync data
                    ctl.req.leaving_nodes = {endpoint};
                    ctl.prepare(node_ops_cmd::removenode_prepare).get();

                    // Step 5: Start to sync data
                    ctl.send_to_all(node_ops_cmd::removenode_sync_data).get();
                    on_streaming_finished();

                    // Step 6: Finish token movement
                    ctl.done(node_ops_cmd::removenode_done).get();

                    // Step 7: Announce the node has left
                    slogger.info("removenode[{}]: Advertising that the node left the ring", uuid);
                    auto permit = ss._gossiper.lock_endpoint(endpoint, gms::null_permit_id).get();
                    const auto& pid = permit.id();
                    ss._gossiper.advertise_token_removed(endpoint, host_id, pid).get();
                    std::unordered_set<token> tmp(tokens.begin(), tokens.end());
                    ss.excise(std::move(tmp), endpoint, pid).get();
                    removed_from_token_ring = true;
                    slogger.info("removenode[{}]: Finished removing the node from the ring", uuid);
                } catch (...) {
                    // we need to revert the effect of prepare verb the removenode ops is failed
                    ctl.abort_on_error(node_ops_cmd::removenode_abort, std::current_exception()).get();
                }
            }

            // Step 8: Remove the node from group 0
            //
            // If the node was a token ring member but we failed to remove it,
            // don't remove it from group 0 -- hence the `removed_from_token_ring` check.
            try {
                utils::get_local_injector().inject("removenode_fail_before_remove_from_group0",
                    [] { throw std::runtime_error("removenode_fail_before_remove_from_group0"); });

                if (is_group0_member && removed_from_token_ring) {
                    slogger.info("removenode[{}]: removing node {} from Raft group 0", uuid, raft_id);
                    ss._group0->remove_from_group0(raft_id).get();
                    slogger.info("removenode[{}]: removed node {} from Raft group 0", uuid, raft_id);
                }
            } catch (...) {
                slogger.error(
                    "removenode[{}]: FAILED when trying to remove the node from Raft group 0: \"{}\". The node"
                    " is no longer a member of the token ring, but it may still be a member of Raft group 0."
                    " Please retry `removenode`. Consult the `removenode` documentation for more details.",
                    uuid, std::current_exception());
                throw;
            }

            slogger.info("removenode[{}]: Finished removenode operation, host id={}", uuid, host_id);
        });
    });
}

future<> storage_service::check_and_repair_cdc_streams() {
    assert(this_shard_id() == 0);

    if (!_cdc_gens.local_is_initialized()) {
        return make_exception_future<>(std::runtime_error("CDC generation service not initialized yet"));
    }

    check_ability_to_perform_topology_operation("checkAndRepairCdcStreams");

    if (raft_topology_change_enabled()) {
        return raft_check_and_repair_cdc_streams();
    }

    return _cdc_gens.local().check_and_repair_cdc_streams();
}

class node_ops_meta_data {
    node_ops_id _ops_uuid;
    gms::inet_address _coordinator;
    std::function<future<> ()> _abort;
    shared_ptr<abort_source> _abort_source;
    std::function<void ()> _signal;
    shared_ptr<node_ops_info> _ops;
    seastar::timer<lowres_clock> _watchdog;
    std::chrono::seconds _watchdog_interval;
public:
    explicit node_ops_meta_data(
            node_ops_id ops_uuid,
            gms::inet_address coordinator,
            std::list<gms::inet_address> ignore_nodes,
            std::chrono::seconds watchdog_interval,
            std::function<future<> ()> abort_func,
            std::function<void ()> signal_func);
    shared_ptr<node_ops_info> get_ops_info();
    shared_ptr<abort_source> get_abort_source();
    future<> abort();
    void update_watchdog();
    void cancel_watchdog();
};

void storage_service::node_ops_cmd_check(gms::inet_address coordinator, const node_ops_cmd_request& req) {
    auto ops_uuids = boost::copy_range<std::vector<node_ops_id>>(_node_ops| boost::adaptors::map_keys);
    std::string msg;
    if (req.cmd == node_ops_cmd::removenode_prepare || req.cmd == node_ops_cmd::replace_prepare ||
            req.cmd == node_ops_cmd::decommission_prepare || req.cmd == node_ops_cmd::bootstrap_prepare) {
        // Peer node wants to start a new node operation. Make sure no pending node operation is in progress.
        if (!_node_ops.empty()) {
            msg = ::format("node_ops_cmd_check: Node {} rejected node_ops_cmd={} from node={} with ops_uuid={}, pending_node_ops={}, pending node ops is in progress",
                    get_broadcast_address(), req.cmd, coordinator, req.ops_uuid, ops_uuids);
        }
    } else if (req.cmd == node_ops_cmd::decommission_heartbeat || req.cmd == node_ops_cmd::removenode_heartbeat ||
            req.cmd == node_ops_cmd::replace_heartbeat || req.cmd == node_ops_cmd::bootstrap_heartbeat) {
        // We allow node_ops_cmd heartbeat to be sent before prepare cmd
    } else {
        if (ops_uuids.size() == 1 && ops_uuids.front() == req.ops_uuid) {
            // Check is good, since we know this ops_uuid and this is the only ops_uuid we are working on.
        } else if (ops_uuids.size() == 0) {
            // The ops_uuid received is unknown. Fail the request.
            msg = ::format("node_ops_cmd_check: Node {} rejected node_ops_cmd={} from node={} with ops_uuid={}, pending_node_ops={}, the node ops is unknown",
                    get_broadcast_address(), req.cmd, coordinator, req.ops_uuid, ops_uuids);
        } else {
            // Other node ops is in progress. Fail the request.
            msg = ::format("node_ops_cmd_check: Node {} rejected node_ops_cmd={} from node={} with ops_uuid={}, pending_node_ops={}, pending node ops is in progress",
                    get_broadcast_address(), req.cmd, coordinator, req.ops_uuid, ops_uuids);
        }
    }
    if (!msg.empty()) {
        slogger.warn("{}", msg);
        throw std::runtime_error(msg);
    }
}

void storage_service::on_node_ops_registered(node_ops_id ops_uuid) {
    utils::get_local_injector().inject("storage_service_nodeops_prepare_handler_sleep3", std::chrono::seconds{3}).get();
    utils::get_local_injector().inject("storage_service_nodeops_abort_after_1s", [this, ops_uuid] {
        (void)with_gate(_async_gate, [this, ops_uuid] {
            return seastar::sleep_abortable(std::chrono::seconds(1), _abort_source).then([this, ops_uuid] {
                node_ops_signal_abort(ops_uuid);
            });
        });
    });
}

void storage_service::node_ops_insert(node_ops_id ops_uuid,
                                      gms::inet_address coordinator,
                                      std::list<inet_address> ignore_nodes,
                                      std::function<future<>()> abort_func) {
    auto watchdog_interval = std::chrono::seconds(_db.local().get_config().nodeops_watchdog_timeout_seconds());
    auto meta = node_ops_meta_data(ops_uuid, coordinator, std::move(ignore_nodes), watchdog_interval, std::move(abort_func),
                                   [this, ops_uuid]() mutable { node_ops_signal_abort(ops_uuid); });
    _node_ops.emplace(ops_uuid, std::move(meta));
    on_node_ops_registered(ops_uuid);
}

future<node_ops_cmd_response> storage_service::node_ops_cmd_handler(gms::inet_address coordinator, std::optional<locator::host_id> coordinator_host_id, node_ops_cmd_request req) {
    return seastar::async([this, coordinator, coordinator_host_id, req = std::move(req)] () mutable {
        auto ops_uuid = req.ops_uuid;
        auto topo_guard = null_topology_guard;
        slogger.debug("node_ops_cmd_handler cmd={}, ops_uuid={}", req.cmd, ops_uuid);

        if (req.cmd == node_ops_cmd::query_pending_ops) {
            bool ok = true;
            auto ops_uuids = boost::copy_range<std::list<node_ops_id>>(_node_ops| boost::adaptors::map_keys);
            node_ops_cmd_response resp(ok, ops_uuids);
            slogger.debug("node_ops_cmd_handler: Got query_pending_ops request from {}, pending_ops={}", coordinator, ops_uuids);
            return resp;
        } else if (req.cmd == node_ops_cmd::repair_updater) {
            slogger.debug("repair[{}]: Got repair_updater request from {}", ops_uuid, coordinator);
            _db.invoke_on_all([coordinator, ops_uuid, tables = req.repair_tables] (replica::database &db) {
                for (const auto& table_id : tables) {
                    try {
                        auto& table = db.find_column_family(table_id);
                        table.update_off_strategy_trigger();
                        slogger.debug("repair[{}]: Updated off_strategy_trigger for table {}.{} by node {}",
                                ops_uuid, table.schema()->ks_name(), table.schema()->cf_name(), coordinator);
                    } catch (replica::no_such_column_family&) {
                        // The table could be dropped by user, ignore it.
                    } catch (...) {
                        throw;
                    }
                }
            }).get();
            bool ok = true;
            return node_ops_cmd_response(ok);
        }

        node_ops_cmd_check(coordinator, req);

        if (req.cmd == node_ops_cmd::removenode_prepare) {
            if (req.leaving_nodes.size() > 1) {
                auto msg = ::format("removenode[{}]: Could not removenode more than one node at a time: leaving_nodes={}", req.ops_uuid, req.leaving_nodes);
                slogger.warn("{}", msg);
                throw std::runtime_error(msg);
            }
            mutate_token_metadata([coordinator, &req, this] (mutable_token_metadata_ptr tmptr) mutable {
                for (auto& node : req.leaving_nodes) {
                    slogger.info("removenode[{}]: Added node={} as leaving node, coordinator={}", req.ops_uuid, node, coordinator);
                    tmptr->add_leaving_endpoint(tmptr->get_host_id(node));
                }
                return update_topology_change_info(tmptr, ::format("removenode {}", req.leaving_nodes));
            }).get();
            node_ops_insert(ops_uuid, coordinator, std::move(req.ignore_nodes), [this, coordinator, req = std::move(req)] () mutable {
                return mutate_token_metadata([this, coordinator, req = std::move(req)] (mutable_token_metadata_ptr tmptr) mutable {
                    for (auto& node : req.leaving_nodes) {
                        slogger.info("removenode[{}]: Removed node={} as leaving node, coordinator={}", req.ops_uuid, node, coordinator);
                        tmptr->del_leaving_endpoint(tmptr->get_host_id(node));
                    }
                    return update_topology_change_info(tmptr, ::format("removenode {}", req.leaving_nodes));
                });
            });
        } else if (req.cmd == node_ops_cmd::removenode_heartbeat) {
            slogger.debug("removenode[{}]: Updated heartbeat from coordinator={}", req.ops_uuid,  coordinator);
            node_ops_update_heartbeat(ops_uuid).get();
        } else if (req.cmd == node_ops_cmd::removenode_done) {
            slogger.info("removenode[{}]: Marked ops done from coordinator={}", req.ops_uuid, coordinator);
            node_ops_done(ops_uuid).get();
        } else if (req.cmd == node_ops_cmd::removenode_sync_data) {
            auto it = _node_ops.find(ops_uuid);
            if (it == _node_ops.end()) {
                throw std::runtime_error(::format("removenode[{}]: Can not find ops_uuid={}", ops_uuid, ops_uuid));
            }
            auto ops = it->second.get_ops_info();
            auto as = it->second.get_abort_source();
            for (auto& node : req.leaving_nodes) {
                if (is_repair_based_node_ops_enabled(streaming::stream_reason::removenode)) {
                    slogger.info("removenode[{}]: Started to sync data for removing node={} using repair, coordinator={}", req.ops_uuid, node, coordinator);
                    _repair.local().removenode_with_repair(get_token_metadata_ptr(), node, ops).get();
                } else {
                    slogger.info("removenode[{}]: Started to sync data for removing node={} using stream, coordinator={}", req.ops_uuid, node, coordinator);
                    removenode_with_stream(node, topo_guard, as).get();
                }
            }
        } else if (req.cmd == node_ops_cmd::removenode_abort) {
            node_ops_abort(ops_uuid).get();
        } else if (req.cmd == node_ops_cmd::decommission_prepare) {
            utils::get_local_injector().inject(
                "storage_service_decommission_prepare_handler_sleep", std::chrono::milliseconds{1500}).get();
            if (req.leaving_nodes.size() > 1) {
                auto msg = ::format("decommission[{}]: Could not decommission more than one node at a time: leaving_nodes={}", req.ops_uuid, req.leaving_nodes);
                slogger.warn("{}", msg);
                throw std::runtime_error(msg);
            }
            mutate_token_metadata([coordinator, &req, this] (mutable_token_metadata_ptr tmptr) mutable {
                for (auto& node : req.leaving_nodes) {
                    slogger.info("decommission[{}]: Added node={} as leaving node, coordinator={}", req.ops_uuid, node, coordinator);
                    tmptr->add_leaving_endpoint(tmptr->get_host_id(node));
                }
                return update_topology_change_info(tmptr, ::format("decommission {}", req.leaving_nodes));
            }).get();
            node_ops_insert(ops_uuid, coordinator, std::move(req.ignore_nodes), [this, coordinator, req = std::move(req)] () mutable {
                return mutate_token_metadata([this, coordinator, req = std::move(req)] (mutable_token_metadata_ptr tmptr) mutable {
                    for (auto& node : req.leaving_nodes) {
                        // Decommission calls leave_ring() as one of its last steps.
                        // The leave_ring() function removes the endpoint from the local token_metadata,
                        // sends a notification about this through gossiper (node status becomes 'left')
                        // and waits for ring_delay. It's possible the node being decommissioned might
                        // die after it has sent this notification. If this happens, the node would
                        // have already been removed from this token_metadata, so we wouldn't find it here.
                        const auto node_id = tmptr->get_host_id_if_known(node);
                        slogger.info("decommission[{}]: Removed node={} as leaving node, coordinator={}", req.ops_uuid, node, coordinator);
                        if (node_id) {
                            tmptr->del_leaving_endpoint(*node_id);
                        }
                    }
                    return update_topology_change_info(tmptr, ::format("decommission {}", req.leaving_nodes));
                });
            });
        } else if (req.cmd == node_ops_cmd::decommission_heartbeat) {
            slogger.debug("decommission[{}]: Updated heartbeat from coordinator={}", req.ops_uuid,  coordinator);
            node_ops_update_heartbeat(ops_uuid).get();
        } else if (req.cmd == node_ops_cmd::decommission_done) {
            bool check_again = false;
            auto start_time = std::chrono::steady_clock::now();
            slogger.info("decommission[{}]: Started to check if nodes={} have left the cluster, coordinator={}", req.ops_uuid, req.leaving_nodes, coordinator);
            do {
                check_again = false;
                for (auto& node : req.leaving_nodes) {
                    auto tmptr = get_token_metadata_ptr();
                    const auto host_id = tmptr->get_host_id_if_known(node);
                    if (host_id && tmptr->is_normal_token_owner(*host_id)) {
                        check_again = true;
                        if (std::chrono::steady_clock::now() > start_time + std::chrono::seconds(60)) {
                            auto msg = ::format("decommission[{}]: Node {}/{} is still in the cluster", req.ops_uuid, node, host_id);
                            throw std::runtime_error(msg);
                        }
                        slogger.warn("decommission[{}]: Node {}/{} is still in the cluster, sleep and check again", req.ops_uuid, node, host_id);
                        sleep_abortable(std::chrono::milliseconds(500), _abort_source).get();
                        break;
                    }
                }
            } while (check_again);
            slogger.info("decommission[{}]: Finished to check if nodes={} have left the cluster, coordinator={}", req.ops_uuid, req.leaving_nodes, coordinator);
            slogger.info("decommission[{}]: Marked ops done from coordinator={}", req.ops_uuid, coordinator);
            slogger.debug("Triggering off-strategy compaction for all non-system tables on decommission completion");
            _db.invoke_on_all([](replica::database &db) {
                for (auto& table : db.get_non_system_column_families()) {
                    table->trigger_offstrategy_compaction();
                }
            }).get();
            node_ops_done(ops_uuid).get();
        } else if (req.cmd == node_ops_cmd::decommission_abort) {
            node_ops_abort(ops_uuid).get();
        } else if (req.cmd == node_ops_cmd::replace_prepare) {
            // Mark the replacing node as replacing
            if (req.replace_nodes.size() > 1) {
                auto msg = ::format("replace[{}]: Could not replace more than one node at a time: replace_nodes={}", req.ops_uuid, req.replace_nodes);
                slogger.warn("{}", msg);
                throw std::runtime_error(msg);
            }
            if (!coordinator_host_id) {
                throw std::runtime_error("Coordinator host_id not found");
            }
            mutate_token_metadata([coordinator, coordinator_host_id, &req, this] (mutable_token_metadata_ptr tmptr) mutable {
                for (auto& x: req.replace_nodes) {
                    auto existing_node = x.first;
                    auto replacing_node = x.second;
                    const auto existing_node_id = tmptr->get_host_id(existing_node);
                    const auto replacing_node_id = *coordinator_host_id;
                    slogger.info("replace[{}]: Added replacing_node={}/{} to replace existing_node={}/{}, coordinator={}/{}",
                        req.ops_uuid, replacing_node, replacing_node_id, existing_node, existing_node_id, coordinator, *coordinator_host_id);

                    // In case of replace-with-same-ip we need to map both host_id-s
                    // to the same IP. The locator::topology allows this specifically in case
                    // where one node is being_replaced and another is replacing,
                    // so here we adjust the state of the original node accordingly.
                    // The host_id -> IP map works as usual, and IP -> host_id will map
                    // IP to the being_replaced node - this is what is implied by the
                    // current code. The IP will be placed in pending_endpoints and
                    // excluded from normal_endpoints (maybe_remove_node_being_replaced function).
                    // In handle_state_normal we'll remap the IP to the new host_id.
                    tmptr->update_topology(existing_node_id, std::nullopt, locator::node::state::being_replaced);
                    tmptr->update_topology(replacing_node_id, get_dc_rack_for(replacing_node), locator::node::state::replacing);
                    tmptr->update_host_id(replacing_node_id, replacing_node);
                    tmptr->add_replacing_endpoint(existing_node_id, replacing_node_id);
                }
                return make_ready_future<>();
            }).get();
            node_ops_insert(ops_uuid, coordinator, std::move(req.ignore_nodes), [this, coordinator, coordinator_host_id, req = std::move(req)] () mutable {
                return mutate_token_metadata([this, coordinator, coordinator_host_id, req = std::move(req)] (mutable_token_metadata_ptr tmptr) mutable {
                    for (auto& x: req.replace_nodes) {
                        auto existing_node = x.first;
                        auto replacing_node = x.second;
                        const auto existing_node_id = tmptr->get_host_id(existing_node);
                        const auto replacing_node_id = *coordinator_host_id;
                        slogger.info("replace[{}]: Removed replacing_node={}/{} to replace existing_node={}/{}, coordinator={}/{}",
                            req.ops_uuid, replacing_node, replacing_node_id, existing_node, existing_node_id, coordinator, *coordinator_host_id);

                        tmptr->del_replacing_endpoint(existing_node_id);
                        const auto dc_rack = get_dc_rack_for(replacing_node);
                        tmptr->update_topology(existing_node_id, dc_rack, locator::node::state::normal);
                        tmptr->remove_endpoint(replacing_node_id);
                    }
                    return update_topology_change_info(tmptr, ::format("replace {}", req.replace_nodes));
                });
            });
        } else if (req.cmd == node_ops_cmd::replace_prepare_mark_alive) {
            // Wait for local node has marked replacing node as alive
            auto nodes = boost::copy_range<std::vector<inet_address>>(req.replace_nodes| boost::adaptors::map_values);
            try {
                _gossiper.wait_alive(nodes, std::chrono::milliseconds(120 * 1000)).get();
            } catch (...) {
                slogger.warn("replace[{}]: Failed to wait for marking replacing node as up, replace_nodes={}: {}",
                        req.ops_uuid, req.replace_nodes, std::current_exception());
                throw;
            }
        } else if (req.cmd == node_ops_cmd::replace_prepare_pending_ranges) {
            // Update the pending_ranges for the replacing node
            slogger.debug("replace[{}]: Updated pending_ranges from coordinator={}", req.ops_uuid, coordinator);
            mutate_token_metadata([&req, this] (mutable_token_metadata_ptr tmptr) mutable {
                return update_topology_change_info(tmptr, ::format("replace {}", req.replace_nodes));
            }).get();
        } else if (req.cmd == node_ops_cmd::replace_heartbeat) {
            slogger.debug("replace[{}]: Updated heartbeat from coordinator={}", req.ops_uuid, coordinator);
            node_ops_update_heartbeat(ops_uuid).get();
        } else if (req.cmd == node_ops_cmd::replace_done) {
            slogger.info("replace[{}]: Marked ops done from coordinator={}", req.ops_uuid, coordinator);
            node_ops_done(ops_uuid).get();
        } else if (req.cmd == node_ops_cmd::replace_abort) {
            node_ops_abort(ops_uuid).get();
        } else if (req.cmd == node_ops_cmd::bootstrap_prepare) {
            // Mark the bootstrap node as bootstrapping
            if (req.bootstrap_nodes.size() > 1) {
                auto msg = ::format("bootstrap[{}]: Could not bootstrap more than one node at a time: bootstrap_nodes={}", req.ops_uuid, req.bootstrap_nodes);
                slogger.warn("{}", msg);
                throw std::runtime_error(msg);
            }
            if (!coordinator_host_id) {
                throw std::runtime_error("Coordinator host_id not found");
            }
            mutate_token_metadata([coordinator, coordinator_host_id, &req, this] (mutable_token_metadata_ptr tmptr) mutable {
                for (auto& x: req.bootstrap_nodes) {
                    auto& endpoint = x.first;
                    auto tokens = std::unordered_set<dht::token>(x.second.begin(), x.second.end());
                    const auto host_id = *coordinator_host_id;
                    const auto dc_rack = get_dc_rack_for(endpoint);
                    slogger.info("bootstrap[{}]: Added node={}/{} as bootstrap, coordinator={}/{}",
                        req.ops_uuid, endpoint, host_id, coordinator, *coordinator_host_id);
                    tmptr->update_host_id(host_id, endpoint);
                    tmptr->update_topology(host_id, dc_rack, locator::node::state::bootstrapping);
                    tmptr->add_bootstrap_tokens(tokens, host_id);
                }
                return update_topology_change_info(tmptr, ::format("bootstrap {}", req.bootstrap_nodes));
            }).get();
            node_ops_insert(ops_uuid, coordinator, std::move(req.ignore_nodes), [this, coordinator, req = std::move(req)] () mutable {
                return mutate_token_metadata([this, coordinator, req = std::move(req)] (mutable_token_metadata_ptr tmptr) mutable {
                    for (auto& x: req.bootstrap_nodes) {
                        auto& endpoint = x.first;
                        auto tokens = std::unordered_set<dht::token>(x.second.begin(), x.second.end());
                        slogger.info("bootstrap[{}]: Removed node={} as bootstrap, coordinator={}", req.ops_uuid, endpoint, coordinator);
                        tmptr->remove_bootstrap_tokens(tokens);
                    }
                    return update_topology_change_info(tmptr, ::format("bootstrap {}", req.bootstrap_nodes));
                });
            });
        } else if (req.cmd == node_ops_cmd::bootstrap_heartbeat) {
            slogger.debug("bootstrap[{}]: Updated heartbeat from coordinator={}", req.ops_uuid, coordinator);
            node_ops_update_heartbeat(ops_uuid).get();
        } else if (req.cmd == node_ops_cmd::bootstrap_done) {
            slogger.info("bootstrap[{}]: Marked ops done from coordinator={}", req.ops_uuid, coordinator);
            node_ops_done(ops_uuid).get();
        } else if (req.cmd == node_ops_cmd::bootstrap_abort) {
            node_ops_abort(ops_uuid).get();
        } else {
            auto msg = ::format("node_ops_cmd_handler: ops_uuid={}, unknown cmd={}", req.ops_uuid, req.cmd);
            slogger.warn("{}", msg);
            throw std::runtime_error(msg);
        }
        bool ok = true;
        node_ops_cmd_response resp(ok);
        return resp;
    });
}

future<> storage_service::reload_schema() {
    // Flush memtables and clear cache so that we use the same state we would after node restart
    // to rule out potential discrepancies which could stem from merging with memtable/cache readers.
    co_await replica::database::flush_keyspace_on_all_shards(_db, db::schema_tables::v3::NAME);
    co_await replica::database::drop_cache_for_keyspace_on_all_shards(_db, db::schema_tables::v3::NAME);
    co_await _migration_manager.invoke_on(0, [] (auto& mm) {
        return mm.reload_schema();
    });
}

future<> storage_service::drain() {
    return run_with_api_lock(sstring("drain"), [] (storage_service& ss) {
        if (ss._operation_mode == mode::DRAINED) {
            slogger.warn("Cannot drain node (did it already happen?)");
            return make_ready_future<>();
        }

        ss.set_mode(mode::DRAINING);
        return ss.do_drain().then([&ss] {
            ss._drain_finished.set_value();
            ss.set_mode(mode::DRAINED);
        });
    });
}

future<> storage_service::do_drain() {
    co_await stop_transport();

    co_await tracing::tracing::tracing_instance().invoke_on_all(&tracing::tracing::shutdown);

    co_await get_batchlog_manager().invoke_on_all([] (auto& bm) {
        return bm.drain();
    });

    co_await _db.invoke_on_all(&replica::database::drain);
    co_await _sys_ks.invoke_on_all(&db::system_keyspace::shutdown);
    co_await _repair.invoke_on_all(&repair_service::shutdown);
}

future<> storage_service::do_cluster_cleanup() {
    auto& raft_server = _group0->group0_server();

    while (true) {
        auto guard = co_await _group0->client().start_operation(&_group0_as, raft_timeout{});

        auto curr_req = _topology_state_machine._topology.global_request;
        if (curr_req && *curr_req != global_topology_request::cleanup) {
            // FIXME: replace this with a queue
            throw std::runtime_error{
                "topology coordinator: cluster cleanup: a different topology request is already pending, try again later"};
        }


        auto it = _topology_state_machine._topology.find(raft_server.id());
        if (!it) {
            throw std::runtime_error(::format("local node {} is not a member of the cluster", raft_server.id()));
        }

        const auto& rs = it->second;

        if (rs.state != node_state::normal) {
            throw std::runtime_error(::format("local node is not in the normal state (current state: {})", rs.state));
        }

        rtlogger.info("cluster cleanup requested");
        topology_mutation_builder builder(guard.write_timestamp());
        builder.set_global_topology_request(global_topology_request::cleanup);
        topology_change change{{builder.build()}};
        group0_command g0_cmd = _group0->client().prepare_command(std::move(change), guard, ::format("cleanup: cluster cleanup requested"));

        try {
            co_await _group0->client().add_entry(std::move(g0_cmd), std::move(guard), &_group0_as, raft_timeout{});
        } catch (group0_concurrent_modification&) {
            rtlogger.info("cleanup: concurrent operation is detected, retrying.");
            continue;
        }
        break;
    }

    // Wait cleanup finishes on all nodes
    co_await _topology_state_machine.event.when([this] {
        return std::all_of(_topology_state_machine._topology.normal_nodes.begin(), _topology_state_machine._topology.normal_nodes.end(), [] (auto& n) {
            return n.second.cleanup == cleanup_status::clean;
        });
    });
    rtlogger.info("cluster cleanup done");
}

future<sstring> storage_service::wait_for_topology_request_completion(utils::UUID id) {
    while (true) {
        auto [done, error] = co_await  _sys_ks.local().get_topology_request_state(id);
        if (done) {
            co_return error;
        }
        co_await _topology_state_machine.event.when();
    }

    co_return sstring();
}

future<> storage_service::raft_rebuild(sstring source_dc) {
    auto& raft_server = _group0->group0_server();
    utils::UUID request_id;

    while (true) {
        auto guard = co_await _group0->client().start_operation(&_group0_as, raft_timeout{});

        auto it = _topology_state_machine._topology.find(raft_server.id());
        if (!it) {
            throw std::runtime_error(::format("local node {} is not a member of the cluster", raft_server.id()));
        }

        const auto& rs = it->second;

        if (rs.state != node_state::normal) {
            throw std::runtime_error(::format("local node is not in the normal state (current state: {})", rs.state));
        }

        if (_topology_state_machine._topology.normal_nodes.size() == 1) {
            throw std::runtime_error("Cannot rebuild a single node");
        }

        rtlogger.info("request rebuild for: {}", raft_server.id());
        topology_mutation_builder builder(guard.write_timestamp());
        builder.set_session(session_id(guard.new_group0_state_id()));
        builder.with_node(raft_server.id())
               .set("topology_request", topology_request::rebuild)
               .set("rebuild_option", source_dc)
               .set("request_id", guard.new_group0_state_id());
        topology_request_tracking_mutation_builder rtbuilder(guard.new_group0_state_id());
        rtbuilder.set("initiating_host",_group0->group0_server().id().uuid())
                 .set("done", false);
        topology_change change{{builder.build(), rtbuilder.build()}};
        group0_command g0_cmd = _group0->client().prepare_command(std::move(change), guard, ::format("rebuild: request rebuild for {} ({})", raft_server.id(), source_dc));

        request_id = guard.new_group0_state_id();

        try {
            co_await _group0->client().add_entry(std::move(g0_cmd), std::move(guard), &_group0_as, raft_timeout{});
        } catch (group0_concurrent_modification&) {
            rtlogger.info("rebuild: concurrent operation is detected, retrying.");
            continue;
        }
        break;
    }

    // Wait until request completes
    auto err = co_await wait_for_topology_request_completion(request_id);
    if (!err.empty()) {
        throw std::runtime_error(::format("rebuild failed: {}", err));
    }
}

future<> storage_service::raft_check_and_repair_cdc_streams() {
    std::optional<cdc::generation_id_v2> last_committed_gen;

    while (true) {
        rtlogger.info("request check_and_repair_cdc_streams, refreshing topology");
        auto guard = co_await _group0->client().start_operation(&_group0_as, raft_timeout{});
        auto curr_req = _topology_state_machine._topology.global_request;
        if (curr_req && *curr_req != global_topology_request::new_cdc_generation) {
            // FIXME: replace this with a queue
            throw std::runtime_error{
                "check_and_repair_cdc_streams: a different topology request is already pending, try again later"};
        }

        if (_topology_state_machine._topology.committed_cdc_generations.empty()) {
            slogger.error("check_and_repair_cdc_streams: no committed CDC generations, requesting a new one.");
        } else {
            last_committed_gen = _topology_state_machine._topology.committed_cdc_generations.back();
            auto gen = co_await _sys_ks.local().read_cdc_generation(last_committed_gen->id);
            if (cdc::is_cdc_generation_optimal(gen, get_token_metadata())) {
                cdc_log.info("CDC generation {} does not need repair", last_committed_gen);
                co_return;
            }
            cdc_log.info("CDC generation {} needs repair, requesting a new one", last_committed_gen);
        }

        topology_mutation_builder builder(guard.write_timestamp());
        builder.set_global_topology_request(global_topology_request::new_cdc_generation);
        topology_change change{{builder.build()}};
        group0_command g0_cmd = _group0->client().prepare_command(std::move(change), guard,
                ::format("request check+repair CDC generation from {}", _group0->group0_server().id()));
        try {
            co_await _group0->client().add_entry(std::move(g0_cmd), std::move(guard), &_group0_as, raft_timeout{});
        } catch (group0_concurrent_modification&) {
            rtlogger.info("request check+repair CDC: concurrent operation is detected, retrying.");
            continue;
        }
        break;
    }

    // Wait until we commit a new CDC generation.
    co_await _topology_state_machine.event.when([this, &last_committed_gen] {
        auto gen = _topology_state_machine._topology.committed_cdc_generations.empty()
                ? std::nullopt
                : std::optional(_topology_state_machine._topology.committed_cdc_generations.back());
        return last_committed_gen != gen;
    });
}

future<> storage_service::rebuild(sstring source_dc) {
    return run_with_api_lock(sstring("rebuild"), [source_dc] (storage_service& ss) -> future<> {
        ss.check_ability_to_perform_topology_operation("rebuild");
        if (ss.raft_topology_change_enabled()) {
            co_await ss.raft_rebuild(source_dc);
        } else {
            slogger.info("rebuild from dc: {}", source_dc == "" ? "(any dc)" : source_dc);
            auto tmptr = ss.get_token_metadata_ptr();
            if (ss.is_repair_based_node_ops_enabled(streaming::stream_reason::rebuild)) {
                co_await ss._repair.local().rebuild_with_repair(tmptr, std::move(source_dc));
            } else {
                auto streamer = make_lw_shared<dht::range_streamer>(ss._db, ss._stream_manager, tmptr, ss._abort_source,
                        tmptr->get_my_id(), ss._snitch.local()->get_location(), "Rebuild", streaming::stream_reason::rebuild, null_topology_guard);
                streamer->add_source_filter(std::make_unique<dht::range_streamer::failure_detector_source_filter>(ss._gossiper.get_unreachable_members()));
                if (source_dc != "") {
                    streamer->add_source_filter(std::make_unique<dht::range_streamer::single_datacenter_filter>(source_dc));
                }
                auto ks_erms = ss._db.local().get_non_local_strategy_keyspaces_erms();
                for (const auto& [keyspace_name, erm] : ks_erms) {
                    co_await streamer->add_ranges(keyspace_name, erm, ss.get_ranges_for_endpoint(erm, ss.get_broadcast_address()), ss._gossiper, false);
                }
                try {
                    co_await streamer->stream_async();
                    slogger.info("Streaming for rebuild successful");
                } catch (...) {
                    auto ep = std::current_exception();
                    // This is used exclusively through JMX, so log the full trace but only throw a simple RTE
                    slogger.warn("Error while rebuilding node: {}", ep);
                    std::rethrow_exception(std::move(ep));
                }
            }
        }
    });
}

void storage_service::check_ability_to_perform_topology_operation(std::string_view operation_name) const {
    switch (_topology_change_kind_enabled) {
    case topology_change_kind::unknown:
        throw std::runtime_error(fmt::format("{} is not allowed at this time - the node is still starting", operation_name));
    case topology_change_kind::upgrading_to_raft:
        throw std::runtime_error(fmt::format("{} is not allowed at this time - the node is still in the process"
                " of upgrading to raft topology", operation_name));
    case topology_change_kind::legacy:
        return;
    case topology_change_kind::raft:
        return;
    }
}

int32_t storage_service::get_exception_count() {
    // FIXME
    // We return 0 for no exceptions, it should probably be
    // replaced by some general exception handling that would count
    // the unhandled exceptions.
    //return (int)StorageMetrics.exceptions.count();
    return 0;
}

future<std::unordered_multimap<dht::token_range, inet_address>>
storage_service::get_changed_ranges_for_leaving(locator::vnode_effective_replication_map_ptr erm, inet_address endpoint) {
    // First get all ranges the leaving endpoint is responsible for
    auto ranges = get_ranges_for_endpoint(erm, endpoint);

    slogger.debug("Node {} ranges [{}]", endpoint, ranges);

    std::unordered_map<dht::token_range, inet_address_vector_replica_set> current_replica_endpoints;

    // Find (for each range) all nodes that store replicas for these ranges as well
    for (auto& r : ranges) {
        auto end_token = r.end() ? r.end()->value() : dht::maximum_token();
        auto eps = erm->get_natural_endpoints(end_token);
        current_replica_endpoints.emplace(r, std::move(eps));
        co_await coroutine::maybe_yield();
    }

    auto temp = co_await get_token_metadata_ptr()->clone_after_all_left();

    // endpoint might or might not be 'leaving'. If it was not leaving (that is, removenode
    // command was used), it is still present in temp and must be removed.
    if (const auto host_id = temp.get_host_id_if_known(endpoint); host_id && temp.is_normal_token_owner(*host_id)) {
        temp.remove_endpoint(*host_id);
    }

    std::unordered_multimap<dht::token_range, inet_address> changed_ranges;

    // Go through the ranges and for each range check who will be
    // storing replicas for these ranges when the leaving endpoint
    // is gone. Whoever is present in newReplicaEndpoints list, but
    // not in the currentReplicaEndpoints list, will be needing the
    // range.
    const auto& rs = erm->get_replication_strategy();
    for (auto& r : ranges) {
        auto end_token = r.end() ? r.end()->value() : dht::maximum_token();
        auto new_replica_endpoints = co_await rs.calculate_natural_ips(end_token, temp);

        auto rg = current_replica_endpoints.equal_range(r);
        for (auto it = rg.first; it != rg.second; it++) {
            const dht::token_range& range_ = it->first;
            inet_address_vector_replica_set& current_eps = it->second;
            slogger.debug("range={}, current_replica_endpoints={}, new_replica_endpoints={}", range_, current_eps, new_replica_endpoints);
            for (auto ep : it->second) {
                auto beg = new_replica_endpoints.begin();
                auto end = new_replica_endpoints.end();
                new_replica_endpoints.erase(std::remove(beg, end, ep), end);
            }
        }

        if (slogger.is_enabled(logging::log_level::debug)) {
            if (new_replica_endpoints.empty()) {
                slogger.debug("Range {} already in all replicas", r);
            } else {
                slogger.debug("Range {} will be responsibility of {}", r, new_replica_endpoints);
            }
        }
        for (auto& ep : new_replica_endpoints) {
            changed_ranges.emplace(r, ep);
        }
        // Replication strategy doesn't necessarily yield in calculate_natural_endpoints.
        // E.g. everywhere_replication_strategy
        co_await coroutine::maybe_yield();
    }
    co_await temp.clear_gently();

    co_return changed_ranges;
}

future<> storage_service::unbootstrap() {
    slogger.info("Started batchlog replay for decommission");
    co_await get_batchlog_manager().local().do_batch_log_replay();
    slogger.info("Finished batchlog replay for decommission");

    if (is_repair_based_node_ops_enabled(streaming::stream_reason::decommission)) {
        co_await _repair.local().decommission_with_repair(get_token_metadata_ptr());
    } else {
        std::unordered_map<sstring, std::unordered_multimap<dht::token_range, inet_address>> ranges_to_stream;

        auto ks_erms = _db.local().get_non_local_strategy_keyspaces_erms();
        for (const auto& [keyspace_name, erm] : ks_erms) {
            auto ranges_mm = co_await get_changed_ranges_for_leaving(erm, get_broadcast_address());
            if (slogger.is_enabled(logging::log_level::debug)) {
                std::vector<wrapping_interval<token>> ranges;
                for (auto& x : ranges_mm) {
                    ranges.push_back(x.first);
                }
                slogger.debug("Ranges needing transfer for keyspace={} are [{}]", keyspace_name, ranges);
            }
            ranges_to_stream.emplace(keyspace_name, std::move(ranges_mm));
        }

        set_mode(mode::LEAVING);

        auto stream_success = stream_ranges(std::move(ranges_to_stream));

        // wait for the transfer runnables to signal the latch.
        slogger.debug("waiting for stream acks.");
        try {
            co_await std::move(stream_success);
        } catch (...) {
            slogger.warn("unbootstrap fails to stream : {}", std::current_exception());
            throw;
        }
        slogger.debug("stream acks all received.");
    }
}

future<> storage_service::removenode_add_ranges(lw_shared_ptr<dht::range_streamer> streamer, gms::inet_address leaving_node) {
    auto my_address = get_broadcast_address();
    auto ks_erms = _db.local().get_non_local_strategy_keyspaces_erms();
    for (const auto& [keyspace_name, erm] : ks_erms) {
        std::unordered_multimap<dht::token_range, inet_address> changed_ranges = co_await get_changed_ranges_for_leaving(erm, leaving_node);
        dht::token_range_vector my_new_ranges;
        for (auto& x : changed_ranges) {
            if (x.second == my_address) {
                my_new_ranges.emplace_back(x.first);
            }
        }
        std::unordered_multimap<inet_address, dht::token_range> source_ranges = co_await get_new_source_ranges(erm, my_new_ranges);
        std::unordered_map<inet_address, dht::token_range_vector> ranges_per_endpoint;
        for (auto& x : source_ranges) {
            ranges_per_endpoint[x.first].emplace_back(x.second);
        }
        streamer->add_rx_ranges(keyspace_name, std::move(ranges_per_endpoint));
    }
}

future<> storage_service::removenode_with_stream(gms::inet_address leaving_node,
                                                 frozen_topology_guard topo_guard,
                                                 shared_ptr<abort_source> as_ptr) {
    return seastar::async([this, leaving_node, as_ptr, topo_guard] {
        auto tmptr = get_token_metadata_ptr();
        abort_source as;
        auto sub = _abort_source.subscribe([&as] () noexcept {
            if (!as.abort_requested()) {
                as.request_abort();
            }
        });
        if (!as_ptr) {
            throw std::runtime_error("removenode_with_stream: abort_source is nullptr");
        }
        auto as_ptr_sub = as_ptr->subscribe([&as] () noexcept {
            if (!as.abort_requested()) {
                as.request_abort();
            }
        });
        auto streamer = make_lw_shared<dht::range_streamer>(_db, _stream_manager, tmptr, as, tmptr->get_my_id(), _snitch.local()->get_location(), "Removenode", streaming::stream_reason::removenode, topo_guard);
        removenode_add_ranges(streamer, leaving_node).get();
        try {
            streamer->stream_async().get();
        } catch (...) {
            slogger.warn("removenode_with_stream: stream failed: {}", std::current_exception());
            throw;
        }
    });
}

future<> storage_service::excise(std::unordered_set<token> tokens, inet_address endpoint, gms::permit_id pid) {
    slogger.info("Removing tokens {} for {}", tokens, endpoint);
    // FIXME: HintedHandOffManager.instance.deleteHintsForEndpoint(endpoint);
    co_await remove_endpoint(endpoint, pid);
    auto tmlock = std::make_optional(co_await get_token_metadata_lock());
    auto tmptr = co_await get_mutable_token_metadata_ptr();
    if (const auto host_id = tmptr->get_host_id_if_known(endpoint); host_id) {
        tmptr->remove_endpoint(*host_id);
    }
    tmptr->remove_bootstrap_tokens(tokens);

    co_await update_topology_change_info(tmptr, ::format("excise {}", endpoint));
    co_await replicate_to_all_cores(std::move(tmptr));
    tmlock.reset();

    co_await notify_left(endpoint);
}

future<> storage_service::excise(std::unordered_set<token> tokens, inet_address endpoint, int64_t expire_time, gms::permit_id pid) {
    add_expire_time_if_found(endpoint, expire_time);
    return excise(tokens, endpoint, pid);
}

future<> storage_service::leave_ring() {
    co_await _cdc_gens.local().leave_ring();
    co_await _sys_ks.local().set_bootstrap_state(db::system_keyspace::bootstrap_state::NEEDS_BOOTSTRAP);
    co_await mutate_token_metadata([this] (mutable_token_metadata_ptr tmptr) {
        auto endpoint = get_broadcast_address();
        const auto my_id = tmptr->get_my_id();
        tmptr->remove_endpoint(my_id);
        return update_topology_change_info(std::move(tmptr), ::format("leave_ring {}/{}", endpoint, my_id));
    });

    auto expire_time = _gossiper.compute_expire_time().time_since_epoch().count();
    co_await _gossiper.add_local_application_state(gms::application_state::STATUS,
            versioned_value::left(co_await _sys_ks.local().get_local_tokens(), expire_time));
    auto delay = std::max(get_ring_delay(), gms::gossiper::INTERVAL);
    slogger.info("Announcing that I have left the ring for {}ms", delay.count());
    co_await sleep_abortable(delay, _abort_source);
}

future<>
storage_service::stream_ranges(std::unordered_map<sstring, std::unordered_multimap<dht::token_range, inet_address>> ranges_to_stream_by_keyspace) {
    auto streamer = dht::range_streamer(_db, _stream_manager, get_token_metadata_ptr(), _abort_source, get_token_metadata_ptr()->get_my_id(), _snitch.local()->get_location(), "Unbootstrap", streaming::stream_reason::decommission, null_topology_guard);
    for (auto& entry : ranges_to_stream_by_keyspace) {
        const auto& keyspace = entry.first;
        auto& ranges_with_endpoints = entry.second;

        if (ranges_with_endpoints.empty()) {
            continue;
        }

        std::unordered_map<inet_address, dht::token_range_vector> ranges_per_endpoint;
        for (auto& end_point_entry : ranges_with_endpoints) {
            dht::token_range r = end_point_entry.first;
            inet_address endpoint = end_point_entry.second;
            ranges_per_endpoint[endpoint].emplace_back(r);
            co_await coroutine::maybe_yield();
        }
        streamer.add_tx_ranges(keyspace, std::move(ranges_per_endpoint));
    }
    try {
        co_await streamer.stream_async();
        slogger.info("stream_ranges successful");
    } catch (...) {
        auto ep = std::current_exception();
        slogger.warn("stream_ranges failed: {}", ep);
        std::rethrow_exception(std::move(ep));
    }
}

void storage_service::add_expire_time_if_found(inet_address endpoint, int64_t expire_time) {
    if (expire_time != 0L) {
        using clk = gms::gossiper::clk;
        auto time = clk::time_point(clk::duration(expire_time));
        _gossiper.add_expire_time_for_endpoint(endpoint, time);
    }
}

future<> storage_service::shutdown_protocol_servers() {
    for (auto& server : _protocol_servers) {
        slogger.info("Shutting down {} server", server->name());
        try {
            co_await server->stop_server();
        } catch (...) {
            slogger.error("Unexpected error shutting down {} server: {}",
                    server->name(), std::current_exception());
            throw;
        }
        slogger.info("Shutting down {} server was successful", server->name());
    }
}

future<std::unordered_multimap<inet_address, dht::token_range>>
storage_service::get_new_source_ranges(locator::vnode_effective_replication_map_ptr erm, const dht::token_range_vector& ranges) const {
    auto my_address = get_broadcast_address();
    std::unordered_map<dht::token_range, inet_address_vector_replica_set> range_addresses = co_await erm->get_range_addresses();
    std::unordered_multimap<inet_address, dht::token_range> source_ranges;

    // find alive sources for our new ranges
    auto tmptr = erm->get_token_metadata_ptr();
    for (auto r : ranges) {
        inet_address_vector_replica_set sources;
        auto it = range_addresses.find(r);
        if (it != range_addresses.end()) {
            sources = it->second;
        }

        tmptr->get_topology().sort_by_proximity(my_address, sources);

        if (std::find(sources.begin(), sources.end(), my_address) != sources.end()) {
            auto err = ::format("get_new_source_ranges: sources={}, my_address={}", sources, my_address);
            slogger.warn("{}", err);
            throw std::runtime_error(err);
        }


        for (auto& source : sources) {
            if (_gossiper.is_alive(source)) {
                source_ranges.emplace(source, r);
                break;
            }
        }

        co_await coroutine::maybe_yield();
    }
    co_return source_ranges;
}

future<> storage_service::move(token new_token) {
    return run_with_api_lock(sstring("move"), [] (storage_service& ss) mutable {
        return make_exception_future<>(std::runtime_error("Move operation is not supported only more"));
    });
}

future<std::vector<storage_service::token_range_endpoints>>
storage_service::describe_ring(const sstring& keyspace, bool include_only_local_dc) const {
    if (_db.local().find_keyspace(keyspace).uses_tablets()) {
        throw std::runtime_error(fmt::format("The keyspace {} has tablet table. Query describe_ring with the table parameter!", keyspace));
    }
    co_return co_await locator::describe_ring(_db.local(), _gossiper, keyspace, include_only_local_dc);
}

future<std::vector<dht::token_range_endpoints>>
storage_service::describe_ring_for_table(const sstring& keyspace_name, const sstring& table_name) const {
    slogger.debug("describe_ring for table {}.{}", keyspace_name, table_name);
    auto& t = _db.local().find_column_family(keyspace_name, table_name);
    if (!t.uses_tablets()) {
        auto ranges = co_await describe_ring(keyspace_name);
        co_return ranges;
    }
    table_id tid = t.schema()->id();
    auto erm = t.get_effective_replication_map();
    auto& tmap = erm->get_token_metadata_ptr()->tablets().get_tablet_map(tid);
    const auto& topology = erm->get_topology();
    std::vector<dht::token_range_endpoints> ranges;
    co_await tmap.for_each_tablet([&] (locator::tablet_id id, const locator::tablet_info& info) -> future<> {
        auto range = tmap.get_token_range(id);
        auto& replicas = info.replicas;
        dht::token_range_endpoints tr;
        if (range.start()) {
            tr._start_token = range.start()->value().to_sstring();
        }
        if (range.end()) {
            tr._end_token = range.end()->value().to_sstring();
        }
        for (auto& r : replicas) {
            dht::endpoint_details details;
            auto& hostid = r.host;
            auto endpoint = host2ip(hostid);
            details._datacenter = topology.get_datacenter(hostid);
            details._rack = topology.get_rack(hostid);
            details._host = endpoint;
            tr._rpc_endpoints.push_back(_gossiper.get_rpc_address(endpoint));
            tr._endpoints.push_back(fmt::to_string(details._host));
            tr._endpoint_details.push_back(std::move(details));
        }
        ranges.push_back(std::move(tr));
        return make_ready_future<>();
    });
    co_return ranges;
}

future<std::unordered_map<dht::token_range, inet_address_vector_replica_set>>
storage_service::construct_range_to_endpoint_map(
        locator::effective_replication_map_ptr erm,
        const dht::token_range_vector& ranges) const {
    std::unordered_map<dht::token_range, inet_address_vector_replica_set> res;
    res.reserve(ranges.size());
    for (auto r : ranges) {
        res[r] = erm->get_natural_endpoints(
                r.end() ? r.end()->value() : dht::maximum_token());
        co_await coroutine::maybe_yield();
    }
    co_return res;
}


std::map<token, inet_address> storage_service::get_token_to_endpoint_map() {
    const auto& tm = get_token_metadata();
    std::map<token, inet_address> result;
    for (const auto [t, id]: tm.get_token_to_endpoint()) {
        result.insert({t, tm.get_endpoint_for_host_id(id)});
    }
    for (const auto [t, id]: tm.get_bootstrap_tokens()) {
        result.insert({t, tm.get_endpoint_for_host_id(id)});
    }
    return result;
}

std::chrono::milliseconds storage_service::get_ring_delay() {
    auto ring_delay = _db.local().get_config().ring_delay_ms();
    slogger.trace("Get RING_DELAY: {}ms", ring_delay);
    return std::chrono::milliseconds(ring_delay);
}

future<locator::token_metadata_lock> storage_service::get_token_metadata_lock() noexcept {
    assert(this_shard_id() == 0);
    return _shared_token_metadata.get_lock();
}

// Acquire the token_metadata lock and get a mutable_token_metadata_ptr.
// Pass that ptr to \c func, and when successfully done,
// replicate it to all cores.
//
// By default the merge_lock (that is unified with the token_metadata_lock)
// is acquired for mutating the token_metadata.  Pass acquire_merge_lock::no
// when called from paths that already acquire the merge_lock, like
// db::schema_tables::do_merge_schema.
//
// Note: must be called on shard 0.
future<> storage_service::mutate_token_metadata(std::function<future<> (mutable_token_metadata_ptr)> func, acquire_merge_lock acquire_merge_lock) noexcept {
    assert(this_shard_id() == 0);
    std::optional<token_metadata_lock> tmlock;

    if (acquire_merge_lock) {
        tmlock.emplace(co_await get_token_metadata_lock());
    }
    auto tmptr = co_await get_mutable_token_metadata_ptr();
    co_await func(tmptr);
    co_await replicate_to_all_cores(std::move(tmptr));
}

future<> storage_service::update_topology_change_info(mutable_token_metadata_ptr tmptr, sstring reason) {
    assert(this_shard_id() == 0);

    try {
        locator::dc_rack_fn get_dc_rack_by_host_id([this, &tm = *tmptr] (locator::host_id host_id) -> std::optional<locator::endpoint_dc_rack> {
            if (raft_topology_change_enabled()) {
                const auto server_id = raft::server_id(host_id.uuid());
                const auto* node = _topology_state_machine._topology.find(server_id);
                if (node) {
                    return locator::endpoint_dc_rack {
                        .dc = node->second.datacenter,
                        .rack = node->second.rack,
                    };
                }
                return std::nullopt;
            }

            return get_dc_rack_for(tm.get_endpoint_for_host_id(host_id));
        });
        co_await tmptr->update_topology_change_info(get_dc_rack_by_host_id);
    } catch (...) {
        auto ep = std::current_exception();
        slogger.error("Failed to update topology change info for {}: {}", reason, ep);
        std::rethrow_exception(std::move(ep));
    }
}

future<> storage_service::update_topology_change_info(sstring reason, acquire_merge_lock acquire_merge_lock) {
    return mutate_token_metadata([this, reason = std::move(reason)] (mutable_token_metadata_ptr tmptr) mutable {
        return update_topology_change_info(std::move(tmptr), std::move(reason));
    }, acquire_merge_lock);
}

future<> storage_service::keyspace_changed(const sstring& ks_name) {
    // The keyspace_changed notification is called on all shards
    // after any keyspace schema change, but we need to mutate_token_metadata
    // once after all shards are done with database::update_keyspace.
    // mutate_token_metadata (via update_topology_change_info) will update the
    // token metadata and effective_replication_map on all shards.
    if (this_shard_id() != 0) {
        return make_ready_future<>();
    }
    // Update pending ranges since keyspace can be changed after we calculate pending ranges.
    sstring reason = ::format("keyspace {}", ks_name);
    return update_topology_change_info(reason, acquire_merge_lock::no);
}

void storage_service::on_update_tablet_metadata() {
    if (this_shard_id() != 0) {
        // replicate_to_all_cores() takes care of other shards.
        return;
    }
    // FIXME: Avoid reading whole tablet metadata on partial changes.
    load_tablet_metadata().get();
    _topology_state_machine.event.broadcast(); // wake up load balancer.
}

future<> storage_service::load_tablet_metadata() {
    if (!_db.local().get_config().check_experimental(db::experimental_features_t::feature::TABLETS)) {
        return make_ready_future<>();
    }
    return mutate_token_metadata([this] (mutable_token_metadata_ptr tmptr) -> future<> {
        tmptr->set_tablets(co_await replica::read_tablet_metadata(_qp));
        tmptr->tablets().set_balancing_enabled(_topology_state_machine._topology.tablet_balancing_enabled);
    }, acquire_merge_lock::no);
}

future<> storage_service::process_tablet_split_candidate(table_id table) {
    auto all_compaction_groups_split = [&] () mutable {
        return _db.map_reduce0([table_ = table] (replica::database& db) {
            auto all_split = db.find_column_family(table_).all_storage_groups_split();
            return make_ready_future<bool>(all_split);
        }, bool{true}, std::logical_and<bool>());
    };

    auto split_all_compaction_groups = [&] () -> future<> {
        return _db.invoke_on_all([table] (replica::database& db) -> future<> {
            return db.find_column_family(table).split_all_storage_groups();
        });
    };

    exponential_backoff_retry split_retry = exponential_backoff_retry(std::chrono::seconds(5), std::chrono::seconds(300));
    bool sleep = false;

    while (!_async_gate.is_closed() && !_group0_as.abort_requested()) {
        try {
            // Ensures that latest changes to tablet metadata, in group0, are visible
            auto guard = co_await _group0->client().start_operation(&_group0_as);
            auto& tmap = get_token_metadata().tablets().get_tablet_map(table);
            if (!tmap.needs_split()) {
                release_guard(std::move(guard));
                break;
            }

            if (co_await all_compaction_groups_split()) {
                slogger.info0("All compaction groups of table {} are split ready.", table);
                release_guard(std::move(guard));
                break;
            } else {
                release_guard(std::move(guard));
                co_await split_all_compaction_groups();
            }
        } catch (...) {
            slogger.error("Failed to complete splitting of table {} due to {}, retrying after {} seconds",
                          table, std::current_exception(), split_retry.sleep_time());
            sleep = true;
            break;
        }
        if (sleep) {
            co_await split_retry.retry(_group0_as);
        }
    }
}

void storage_service::register_tablet_split_candidate(table_id table) noexcept {
    if (this_shard_id() != 0) {
        return;
    }
    try {
        if (get_token_metadata().tablets().get_tablet_map(table).needs_split()) {
            _tablet_split_candidates.push_back(table);
            _tablet_split_monitor_event.signal();
        }
    } catch (...) {
        slogger.error("Unable to register table {} as candidate for tablet splitting, due to {}", table, std::current_exception());
    }
}

future<> storage_service::run_tablet_split_monitor() {
    while (!_async_gate.is_closed() && !_group0_as.abort_requested()) {
        while (!_tablet_split_candidates.empty()) {
            auto candidate = _tablet_split_candidates.front();
            _tablet_split_candidates.pop_front();
            co_await process_tablet_split_candidate(candidate);
        }
        co_await _tablet_split_monitor_event.when();
    }
}

void storage_service::start_tablet_split_monitor() {
    if (this_shard_id() != 0) {
        return;
    }
    if (!_db.local().get_config().check_experimental(db::experimental_features_t::feature::TABLETS)) {
        return;
    }
    slogger.info("Starting the tablet split monitor...");
    _tablet_split_monitor = run_tablet_split_monitor();
}

future<> storage_service::snitch_reconfigured() {
    assert(this_shard_id() == 0);
    auto& snitch = _snitch.local();
    co_await mutate_token_metadata([&snitch] (mutable_token_metadata_ptr tmptr) -> future<> {
        // re-read local rack and DC info
        tmptr->update_topology(tmptr->get_my_id(), snitch->get_location());
        return make_ready_future<>();
    });

    if (_gossiper.is_enabled()) {
        co_await _gossiper.add_local_application_state(snitch->get_app_states());
    }
}

future<raft_topology_cmd_result> storage_service::raft_topology_cmd_handler(raft::term_t term, uint64_t cmd_index, const raft_topology_cmd& cmd) {
    raft_topology_cmd_result result;
    rtlogger.debug("topology cmd rpc {} is called", cmd.cmd);

    // The retrier does:
    // If no operation was previously started - start it now
    // If previous operation still running - wait for it an return its result
    // If previous operation completed successfully - return immediately
    // If previous operation failed - restart it
    auto retrier = [] (std::optional<shared_future<>>& f, auto&& func) -> future<> {
        if (!f || f->failed()) {
            if (f) {
                rtlogger.info("retry streaming after previous attempt failed with {}", f->get_future().get_exception());
            } else {
                rtlogger.info("start streaming");
            }
            f = func();
        } else {
            rtlogger.debug("already streaming");
        }
        co_await f.value().get_future();
        rtlogger.info("streaming completed");
    };

    try {
        auto& raft_server = _group0->group0_server();
        // do barrier to make sure we always see the latest topology
        co_await raft_server.read_barrier(&_group0_as);
        if (raft_server.get_current_term() != term) {
           // Return an error since the command is from outdated leader
           co_return result;
        }

        {
            auto& state = _raft_topology_cmd_handler_state;
            if (state.term != term) {
                state.term = term;
            } else if (cmd_index <= state.last_index) {
                // Return an error since the command is outdated
                co_return result;
            }
            state.last_index = cmd_index;
        }

        // We capture the topology version right after the checks
        // above, before any yields. This is crucial since _topology_state_machine._topology
        // might be altered concurrently while this method is running,
        // which can cause the fence command to apply an invalid fence version.
        const auto version = _topology_state_machine._topology.version;

        switch (cmd.cmd) {
            case raft_topology_cmd::command::barrier: {
                utils::get_local_injector().inject("raft_topology_barrier_fail",
                                       [] { throw std::runtime_error("raft topology barrier failed due to error injection"); });
                // This barrier might have been issued by the topology coordinator
                // as a step in enabling a feature, i.e. it noticed that all
                // nodes support some feature, then issue the barrier to make
                // sure that all nodes observed this fact in their local state
                // (a node cannot revoke support for a feature after that), and
                // after receiving a confirmation from all nodes it will mark
                // the feature as enabled.
                //
                // However, it might happen that the node handles this request
                // early in the boot process, before it did the second feature
                // check that happens when the node updates its metadata
                // in `system.topology`. The node might have committed a command
                // that advertises support for a feature as the last node
                // to do so, crashed and now it doesn't support it. This should
                // be rare, but it can happen and we can detect it right here.
                std::exception_ptr ex;
                try {
                    const auto& enabled_features = _topology_state_machine._topology.enabled_features;
                    const auto unsafe_to_disable_features = _topology_state_machine._topology.calculate_not_yet_enabled_features();
                    _feature_service.check_features(enabled_features, unsafe_to_disable_features);
                } catch (const gms::unsupported_feature_exception&) {
                    ex = std::current_exception();
                }
                if (ex) {
                    rtlogger.error("feature check during barrier failed: {}", ex);
                    co_await drain();
                    break;
                }

                // we already did read barrier above
                result.status = raft_topology_cmd_result::command_status::success;
            }
            break;
            case raft_topology_cmd::command::barrier_and_drain: {
                co_await container().invoke_on_all([version] (storage_service& ss) -> future<> {
                    const auto current_version = ss._shared_token_metadata.get()->get_version();
                    rtlogger.debug("Got raft_topology_cmd::barrier_and_drain, version {}, current version {}",
                        version, current_version);

                    // This shouldn't happen under normal operation, it's only plausible
                    // if the topology change coordinator has
                    // moved to another node and managed to update the topology
                    // parallel to this method. The previous coordinator
                    // should be inactive now, so it won't observe this
                    // exception. By returning exception we aim
                    // to reveal any other conditions where this may arise.
                    if (current_version != version) {
                        co_await coroutine::return_exception(std::runtime_error(
                            ::format("raft topology: command::barrier_and_drain, the version has changed, "
                                     "version {}, current_version {}, the topology change coordinator "
                                     " had probably migrated to another node",
                                version, current_version)));
                    }

                    co_await ss._shared_token_metadata.stale_versions_in_use();
                    co_await get_topology_session_manager().drain_closing_sessions();

                    rtlogger.debug("raft_topology_cmd::barrier_and_drain done");
                });

                co_await utils::get_local_injector().inject("raft_topology_barrier_and_drain_fail", [this] (auto& handler) -> future<> {
                    auto ks = handler.get("keyspace");
                    auto cf = handler.get("table");
                    auto last_token = dht::token::from_int64(std::atoll(handler.get("last_token")->data()));
                    auto table_id = _db.local().find_column_family(*ks, *cf).schema()->id();
                    auto stage = co_await replica::read_tablet_transition_stage(_qp, table_id, last_token);
                    if (stage) {
                        sstring want_stage(handler.get("stage").value());
                        if (*stage == locator::tablet_transition_stage_from_string(want_stage)) {
                            rtlogger.info("raft_topology_cmd: barrier handler waits");
                            co_await handler.wait_for_message(std::chrono::steady_clock::now() + std::chrono::minutes{5});
                            rtlogger.info("raft_topology_cmd: barrier handler continues");
                        }
                    }
                });

                result.status = raft_topology_cmd_result::command_status::success;
            }
            break;
            case raft_topology_cmd::command::stream_ranges: {
                co_await with_scheduling_group(_db.local().get_streaming_scheduling_group(), coroutine::lambda([&] () -> future<> {
                    const auto& rs = _topology_state_machine._topology.find(raft_server.id())->second;
                    auto tstate = _topology_state_machine._topology.tstate;
                    if (!rs.ring ||
                        (tstate != topology::transition_state::write_both_read_old && rs.state != node_state::normal && rs.state != node_state::rebuilding)) {
                        rtlogger.warn("got stream_ranges request while my tokens state is {} and node state is {}", tstate, rs.state);
                        co_return;
                    }

                    utils::get_local_injector().inject("stream_ranges_fail",
                                           [] { throw std::runtime_error("stream_range failed due to error injection"); });

                    switch(rs.state) {
                    case node_state::bootstrapping:
                    case node_state::replacing: {
                        set_mode(mode::BOOTSTRAP);
                        // See issue #4001
                        co_await mark_existing_views_as_built();
                        co_await _db.invoke_on_all([] (replica::database& db) {
                            for (auto& cf : db.get_non_system_column_families()) {
                                cf->notify_bootstrap_or_replace_start();
                            }
                        });
                        if (rs.state == node_state::bootstrapping) {
                            if (!_topology_state_machine._topology.normal_nodes.empty()) { // stream only if there is a node in normal state
                                co_await retrier(_bootstrap_result, coroutine::lambda([&] () -> future<> {
                                    if (is_repair_based_node_ops_enabled(streaming::stream_reason::bootstrap)) {
                                        co_await _repair.local().bootstrap_with_repair(get_token_metadata_ptr(), rs.ring.value().tokens);
                                    } else {
                                        dht::boot_strapper bs(_db, _stream_manager, _abort_source, get_token_metadata_ptr()->get_my_id(),
                                            locator::endpoint_dc_rack{rs.datacenter, rs.rack}, rs.ring.value().tokens, get_token_metadata_ptr());
                                        co_await bs.bootstrap(streaming::stream_reason::bootstrap, _gossiper, _topology_state_machine._topology.session);
                                    }
                                }));
                            }
                            // Bootstrap did not complete yet, but streaming did
                        } else {
                            co_await retrier(_bootstrap_result, coroutine::lambda([&] () ->future<> {
                                if (!_topology_state_machine._topology.req_param.contains(raft_server.id())) {
                                    on_internal_error(rtlogger, ::format("Cannot find request_param for node id {}", raft_server.id()));
                                }
                                if (is_repair_based_node_ops_enabled(streaming::stream_reason::replace)) {
                                    // FIXME: we should not need to translate ids to IPs here. See #6403.
                                    std::unordered_set<gms::inet_address> ignored_ips;
                                    for (const auto& id : _topology_state_machine._topology.ignored_nodes) {
                                        auto ip = _group0->address_map().find(id);
                                        if (!ip) {
                                            on_fatal_internal_error(rtlogger, ::format("Cannot find a mapping from node id {} to its ip", id));
                                        }
                                        ignored_ips.insert(*ip);
                                    }
                                    co_await _repair.local().replace_with_repair(get_token_metadata_ptr(), rs.ring.value().tokens, std::move(ignored_ips));
                                } else {
                                    dht::boot_strapper bs(_db, _stream_manager, _abort_source, get_token_metadata_ptr()->get_my_id(),
                                                          locator::endpoint_dc_rack{rs.datacenter, rs.rack}, rs.ring.value().tokens, get_token_metadata_ptr());
                                    auto replaced_id = std::get<replace_param>(_topology_state_machine._topology.req_param[raft_server.id()]).replaced_id;
                                    auto existing_ip = _group0->address_map().find(replaced_id);
                                    assert(existing_ip);
                                    co_await bs.bootstrap(streaming::stream_reason::replace, _gossiper, _topology_state_machine._topology.session, *existing_ip);
                                }
                            }));
                        }
                        co_await _db.invoke_on_all([] (replica::database& db) {
                            for (auto& cf : db.get_non_system_column_families()) {
                                cf->notify_bootstrap_or_replace_end();
                            }
                        });
                        result.status = raft_topology_cmd_result::command_status::success;
                    }
                    break;
                    case node_state::decommissioning:
                        co_await retrier(_decommission_result, coroutine::lambda([&] () { return unbootstrap(); }));
                        result.status = raft_topology_cmd_result::command_status::success;
                    break;
                    case node_state::normal: {
                        // If asked to stream a node in normal state it means that remove operation is running
                        // Find the node that is been removed
                        auto it = boost::find_if(_topology_state_machine._topology.transition_nodes, [] (auto& e) { return e.second.state == node_state::removing; });
                        if (it == _topology_state_machine._topology.transition_nodes.end()) {
                            rtlogger.warn("got stream_ranges request while my state is normal but cannot find a node that is been removed");
                            break;
                        }
                        auto id = it->first;
                        rtlogger.debug("streaming to remove node {}", id);
                        const auto& am = _group0->address_map();
                        auto ip = am.find(id); // map node id to ip
                        assert (ip); // what to do if address is unknown?
                        co_await retrier(_remove_result[id], coroutine::lambda([&] () {
                            auto as = make_shared<abort_source>();
                            auto sub = _abort_source.subscribe([as] () noexcept {
                                if (!as->abort_requested()) {
                                    as->request_abort();
                                }
                            });
                            if (is_repair_based_node_ops_enabled(streaming::stream_reason::removenode)) {
                                // FIXME: we should not need to translate ids to IPs here. See #6403.
                                std::list<gms::inet_address> ignored_ips;
                                for (const auto& ignored_id : _topology_state_machine._topology.ignored_nodes) {
                                    auto ip = _group0->address_map().find(ignored_id);
                                    if (!ip) {
                                        on_fatal_internal_error(rtlogger, ::format("Cannot find a mapping from node id {} to its ip", ignored_id));
                                    }
                                    ignored_ips.push_back(*ip);
                                }
                                auto ops = seastar::make_shared<node_ops_info>(node_ops_id::create_random_id(), as, std::move(ignored_ips));
                                return _repair.local().removenode_with_repair(get_token_metadata_ptr(), *ip, ops);
                            } else {
                                return removenode_with_stream(*ip, _topology_state_machine._topology.session, as);
                            }
                        }));
                        result.status = raft_topology_cmd_result::command_status::success;
                    }
                    break;
                    case node_state::rebuilding: {
                        auto source_dc = std::get<rebuild_param>(_topology_state_machine._topology.req_param[raft_server.id()]).source_dc;
                        rtlogger.info("rebuild from dc: {}", source_dc == "" ? "(any dc)" : source_dc);
                        co_await retrier(_rebuild_result, [&] () -> future<> {
                            auto tmptr = get_token_metadata_ptr();
                            if (is_repair_based_node_ops_enabled(streaming::stream_reason::rebuild)) {
                                co_await _repair.local().rebuild_with_repair(tmptr, std::move(source_dc));
                            } else {
                                auto streamer = make_lw_shared<dht::range_streamer>(_db, _stream_manager, tmptr, _abort_source,
                                        tmptr->get_my_id(), _snitch.local()->get_location(), "Rebuild", streaming::stream_reason::rebuild, _topology_state_machine._topology.session);
                                streamer->add_source_filter(std::make_unique<dht::range_streamer::failure_detector_source_filter>(_gossiper.get_unreachable_members()));
                                if (source_dc != "") {
                                    streamer->add_source_filter(std::make_unique<dht::range_streamer::single_datacenter_filter>(source_dc));
                                }
                                auto ks_erms = _db.local().get_non_local_strategy_keyspaces_erms();
                                for (const auto& [keyspace_name, erm] : ks_erms) {
                                    co_await streamer->add_ranges(keyspace_name, erm, get_ranges_for_endpoint(erm, get_broadcast_address()), _gossiper, false);
                                }
                                try {
                                    co_await streamer->stream_async();
                                    rtlogger.info("streaming for rebuild successful");
                                } catch (...) {
                                    auto ep = std::current_exception();
                                    // This is used exclusively through JMX, so log the full trace but only throw a simple RTE
                                    rtlogger.warn("error while rebuilding node: {}", ep);
                                    std::rethrow_exception(std::move(ep));
                                }
                            }
                        });
                        _rebuild_result.reset();
                        result.status = raft_topology_cmd_result::command_status::success;
                    }
                    break;
                    case node_state::left:
                    case node_state::none:
                    case node_state::removing:
                        on_fatal_internal_error(rtlogger, ::format("Node {} got streaming request in state {}. It should be either dead or not part of the cluster",
                                         raft_server.id(), rs.state));
                    break;
                    }
                }));
            }
            break;
            case raft_topology_cmd::command::wait_for_ip: {
                std::vector<raft::server_id> ids;
                {
                    const auto& new_nodes = _topology_state_machine._topology.new_nodes;
                    ids.reserve(new_nodes.size());
                    for (const auto& [id, rs]: new_nodes) {
                        ids.push_back(id);
                    }
                }
                rtlogger.debug("Got raft_topology_cmd::wait_for_ip, new nodes [{}]", ids);
                for (const auto& id: ids) {
                    co_await wait_for_ip(id, _group0->address_map(), _abort_source);
                }
                rtlogger.debug("raft_topology_cmd::wait_for_ip done [{}]", ids);
                result.status = raft_topology_cmd_result::command_status::success;
                break;
            }
        }
    } catch (...) {
        rtlogger.error("raft_topology_cmd failed with: {}", std::current_exception());
    }
    co_return result;
}

future<> storage_service::update_fence_version(token_metadata::version_t new_version) {
    return container().invoke_on_all([new_version] (storage_service& ss) {
        ss._shared_token_metadata.update_fence_version(new_version);
    });
}

inet_address storage_service::host2ip(locator::host_id host) const {
    auto ip = _group0->address_map().find(raft::server_id(host.uuid()));
    if (!ip) {
        throw std::runtime_error(::format("Cannot map host {} to ip", host));
    }
    return *ip;
}

// Performs a replica-side operation for a given tablet.
// What operation is performed is determined by "op" based on the
// current state of tablet metadata. The coordinator is supposed to prepare tablet
// metadata according to his intent and trigger the operation,
// without passing any transient information.
//
// If the operation succeeds, and the coordinator is still valid, it means
// that the operation intended by the coordinator was performed.
// If the coordinator is no longer valid, the operation may succeed but
// the actual operation performed may be different than intended, it may
// be the one intended by the new coordinator. This is not a problem
// because the old coordinator should do nothing with such result.
//
// The triggers may be retried. They may also be reordered with older triggers, from
// the same or a different coordinator. There is a protocol which ensures that
// stale triggers won't cause operations to run beyond the migration stage they were
// intended for. For example, that streaming is not still running after the coordinator
// moved past the "streaming" stage, and that it won't be started when the stage is not appropriate.
// A non-stale trigger is the one which completed successfully and caused the valid coordinator
// to advance tablet migration to the next stage. Other triggers are called stale.
// We can divide stale triggers into categories:
//   (1) Those which start after the tablet was moved to the next stage
//   Those which start before the tablet was moved to the next stage,
//     (2) ...but after the non-stale trigger finished
//     (3) ...but before the non-stale trigger finished
//
// By "start" I mean the atomic block which inserts into _tablet_ops, and by "finish" I mean
// removal from _tablet_ops.
// So event ordering is local from the perspective of this replica, and is linear because
// this happens on the same shard.
//
// What prevents (1) from running is the fact that triggers check the state of tablet
// metadata, and will fail immediately if the stage is not appropriate. It can happen
// that the trigger is so stale that it will match with an appropriate stage of the next
// migration of the same tablet. This is not a problem because we fall into the same
// category as a stale trigger which was started in the new migration, so cases (2) or (3) apply.
//
// What prevents (2) from running is the fact that after the coordinator moves on to
// the next stage, it executes a token metadata barrier, which will wait for such triggers
// to complete as they hold on to erm via tablet_metadata_barrier. They should be aborted
// soon after the coordinator changes the stage by the means of tablet_metadata_barrier::get_abort_source().
//
// What prevents (3) from running is that they will join with the non-stale trigger, or non-stale
// trigger will join with them, depending on which came first. In that case they finish at the same time.
//
// It's very important that the global token metadata barrier involves all nodes which
// may receive stale triggers started in the previous stage, so that those nodes will
// see tablet metadata which reflects group0 state. This will cut-off stale triggers
// as soon as the coordinator moves to the next stage.
future<> storage_service::do_tablet_operation(locator::global_tablet_id tablet,
                                              sstring op_name,
                                              std::function<future<>(locator::tablet_metadata_guard&)> op) {
    // The coordinator may not execute global token metadata barrier before triggering the operation, so we need
    // a barrier here to see the token metadata which is at least as recent as that of the sender.
    auto& raft_server = _group0->group0_server();
    co_await raft_server.read_barrier(&_group0_as);

    if (_tablet_ops.contains(tablet)) {
        rtlogger.debug("{} retry joining with existing session for tablet {}", op_name, tablet);
        co_await _tablet_ops[tablet].done.get_future();
        co_return;
    }

    locator::tablet_metadata_guard guard(_db.local().find_column_family(tablet.table), tablet);
    auto& as = guard.get_abort_source();
    auto sub = _group0_as.subscribe([&as] () noexcept {
        as.request_abort();
    });

    auto async_gate_holder = _async_gate.hold();
    promise<> p;
    _tablet_ops.emplace(tablet, tablet_operation {
        op_name, seastar::shared_future<>(p.get_future())
    });
    auto erase_registry_entry = seastar::defer([&] {
        _tablet_ops.erase(tablet);
    });

    try {
        co_await op(guard);
        p.set_value();
        rtlogger.debug("{} for tablet migration of {} successful", op_name, tablet);
    } catch (...) {
        p.set_exception(std::current_exception());
        rtlogger.warn("{} for tablet migration of {} failed: {}", op_name, tablet, std::current_exception());
        throw;
    }
}

// Streams data to the pending tablet replica of a given tablet on this node.
// The source tablet replica is determined from the current transition info of the tablet.
future<> storage_service::stream_tablet(locator::global_tablet_id tablet) {
    return do_tablet_operation(tablet, "Streaming", [this, tablet] (locator::tablet_metadata_guard& guard) -> future<> {
        auto tm = guard.get_token_metadata();
        auto& tmap = guard.get_tablet_map();
        auto* trinfo = tmap.get_tablet_transition_info(tablet.tablet);

        // Check if the request is still valid.
        // If there is mismatch, it means this streaming was canceled and the coordinator moved on.
        if (!trinfo) {
            throw std::runtime_error(fmt::format("No transition info for tablet {}", tablet));
        }
        if (trinfo->stage != locator::tablet_transition_stage::streaming) {
            throw std::runtime_error(fmt::format("Tablet {} stage is not at streaming", tablet));
        }
        auto topo_guard = trinfo->session_id;
        if (!trinfo->session_id) {
            throw std::runtime_error(fmt::format("Tablet {} session is not set", tablet));
        }
        auto pending_replica = trinfo->pending_replica;
        if (pending_replica.host != tm->get_my_id()) {
            throw std::runtime_error(fmt::format("Tablet {} has pending replica different than this one", tablet));
        }

        auto& tinfo = tmap.get_tablet_info(tablet.tablet);
        auto range = tmap.get_token_range(tablet.tablet);
        locator::tablet_replica leaving_replica = locator::get_leaving_replica(tinfo, *trinfo);
        if (leaving_replica.host == tm->get_my_id()) {
            // The algorithm doesn't work with tablet migration within the same node because
            // it assumes there is only one tablet replica, picked by the sharder, on local node.
            throw std::runtime_error(fmt::format("Cannot stream within the same node, tablet: {}, shard {} -> {}",
                                            tablet, leaving_replica.shard, trinfo->pending_replica.shard));
        }

        locator::tablet_migration_streaming_info streaming_info = get_migration_streaming_info(tm->get_topology(), tinfo, *trinfo);

        streaming::stream_reason reason = std::invoke([&] {
            switch (trinfo->transition) {
                case locator::tablet_transition_kind::migration: return streaming::stream_reason::tablet_migration;
                case locator::tablet_transition_kind::rebuild: return streaming::stream_reason::rebuild;
                default:
                    throw std::runtime_error(fmt::format("stream_tablet(): Invalid tablet transition: {}", trinfo->transition));
            }
        });

        auto& table = _db.local().find_column_family(tablet.table);
        std::vector<sstring> tables = {table.schema()->cf_name()};
        auto streamer = make_lw_shared<dht::range_streamer>(_db, _stream_manager, std::move(tm),
                                                            guard.get_abort_source(),
                                                            tm->get_my_id(), _snitch.local()->get_location(),
                                                            format("Tablet {}", trinfo->transition),
                                                            reason,
                                                            topo_guard,
                                                            std::move(tables));
        tm = nullptr;
        streamer->add_source_filter(std::make_unique<dht::range_streamer::failure_detector_source_filter>(
                _gossiper.get_unreachable_members()));

        std::unordered_map<inet_address, dht::token_range_vector> ranges_per_endpoint;
        for (auto r : streaming_info.read_from) {
            ranges_per_endpoint[host2ip(r.host)].emplace_back(range);
        }
        streamer->add_rx_ranges(table.schema()->ks_name(), std::move(ranges_per_endpoint));
        co_await streamer->stream_async();

        // If new pending tablet replica needs splitting, streaming waits for it to complete.
        // That's to provide a guarantee that once migration is over, the coordinator can finalize
        // splitting under the promise that compaction groups of tablets are all split, ready
        // for the subsequent topology change.
        //
        // FIXME:
        //  We could do the splitting not in the streaming stage, but in a later stage, so that
        //  from the tablet scheduler's perspective migrations blocked on compaction are not
        //  participating in streaming anymore (which is true), so it could schedule more
        //  migrations. This way compaction would run in parallel with streaming which can
        //  reduce the delay.
        co_await _db.invoke_on(pending_replica.shard, [tablet] (replica::database& db) {
            auto& table = db.find_column_family(tablet.table);
            return table.maybe_split_compaction_group_of(tablet.tablet);
        });

        co_return;
    });
}

future<> storage_service::cleanup_tablet(locator::global_tablet_id tablet) {
    utils::get_local_injector().inject("cleanup_tablet_crash", [] {
        slogger.info("Crashing tablet cleanup");
        _exit(1);
    });

    return do_tablet_operation(tablet, "Cleanup", [this, tablet] (locator::tablet_metadata_guard& guard) {
        shard_id shard;

        {
            auto tm = guard.get_token_metadata();
            auto& tmap = guard.get_tablet_map();
            auto *trinfo = tmap.get_tablet_transition_info(tablet.tablet);

            // Check if the request is still valid.
            // If there is mismatch, it means this cleanup was canceled and the coordinator moved on.
            if (!trinfo) {
                throw std::runtime_error(fmt::format("No transition info for tablet {}", tablet));
            }

            if (trinfo->stage == locator::tablet_transition_stage::cleanup) {
                auto& tinfo = tmap.get_tablet_info(tablet.tablet);
                locator::tablet_replica leaving_replica = locator::get_leaving_replica(tinfo, *trinfo);
                if (leaving_replica.host != tm->get_my_id()) {
                    throw std::runtime_error(fmt::format("Tablet {} has leaving replica different than this one", tablet));
                }
            } else if (trinfo->stage == locator::tablet_transition_stage::cleanup_target) {
                if (trinfo->pending_replica.host != tm->get_my_id()) {
                    throw std::runtime_error(fmt::format("Tablet {} has pending replica different than this one", tablet));
                }
            } else {
                throw std::runtime_error(fmt::format("Tablet {} stage is not at cleanup/cleanup_target", tablet));
            }

            auto shard_opt = tmap.get_shard(tablet.tablet, tm->get_my_id());
            if (!shard_opt) {
                on_internal_error(rtlogger, format("Tablet {} has no shard on this node", tablet));
            }
            shard = *shard_opt;
        }
        return _db.invoke_on(shard, [tablet, &sys_ks = _sys_ks] (replica::database& db) {
            auto& table = db.find_column_family(tablet.table);
            return table.cleanup_tablet(db, sys_ks.local(), tablet.tablet);
        });
    });
}

static bool increases_replicas_per_rack(const locator::topology& topology, const locator::tablet_info& tinfo, sstring dst_rack) {
    std::unordered_map<sstring, size_t> m;
    for (auto& replica: tinfo.replicas) {
        m[topology.get_rack(replica.host)]++;
    }
    auto max = *boost::max_element(m | boost::adaptors::map_values);
    return m[dst_rack] + 1 > max;
}

future<> storage_service::move_tablet(table_id table, dht::token token, locator::tablet_replica src, locator::tablet_replica dst, loosen_constraints force) {
    auto holder = _async_gate.hold();

    if (this_shard_id() != 0) {
        // group0 is only set on shard 0.
        co_return co_await container().invoke_on(0, [&] (auto& ss) {
            return ss.move_tablet(table, token, src, dst, force);
        });
    }

    while (true) {
        auto guard = co_await _group0->client().start_operation(&_abort_source);

        while (_topology_state_machine._topology.is_busy()) {
            const auto tstate = *_topology_state_machine._topology.tstate;
            if (tstate == topology::transition_state::tablet_draining ||
                tstate == topology::transition_state::tablet_migration) {
                break;
            }
            rtlogger.debug("move_tablet(): topology state machine is busy: {}", tstate);
            release_guard(std::move(guard));
            co_await _topology_state_machine.event.wait();
            guard = co_await _group0->client().start_operation(&_abort_source);
        }

        std::vector<canonical_mutation> updates;
        auto& tmap = get_token_metadata().tablets().get_tablet_map(table);
        auto tid = tmap.get_tablet_id(token);
        auto& tinfo = tmap.get_tablet_info(tid);
        auto last_token = tmap.get_last_token(tid);
        auto gid = locator::global_tablet_id{table, tid};

        if (tmap.get_tablet_transition_info(tid)) {
            throw std::runtime_error(fmt::format("Tablet {} is in transition", gid));
        }
        if (!locator::contains(tinfo.replicas, src)) {
            throw std::runtime_error(format("Tablet {} has no replica on {}", gid, src));
        }
        auto* node = get_token_metadata().get_topology().find_node(dst.host);
        if (!node) {
            throw std::runtime_error(format("Unknown host: {}", dst.host));
        }
        if (dst.shard >= node->get_shard_count()) {
            throw std::runtime_error(format("Host {} does not have shard {}", *node, dst.shard));
        }
        if (src.host == dst.host) {
            throw std::runtime_error("Migrating within the same node is not supported");
        }

        if (src == dst) {
            co_return;
        }

        if (locator::contains(tinfo.replicas, dst.host)) {
            throw std::runtime_error(fmt::format("Tablet {} has replica on {}", gid, dst.host));
        }
        auto src_dc_rack = get_token_metadata().get_topology().get_location(src.host);
        auto dst_dc_rack = get_token_metadata().get_topology().get_location(dst.host);
        if (src_dc_rack.dc != dst_dc_rack.dc) {
            if (force) {
                slogger.warn("Moving tablet {} between DCs ({} and {})", gid, src_dc_rack.dc, dst_dc_rack.dc);
            } else {
                throw std::runtime_error(fmt::format("Attempted to move tablet {} between DCs ({} and {})", gid, src_dc_rack.dc, dst_dc_rack.dc));
            }
        }
        if (src_dc_rack.rack != dst_dc_rack.rack && increases_replicas_per_rack(get_token_metadata().get_topology(), tinfo, dst_dc_rack.rack)) {
            if (force) {
                slogger.warn("Moving tablet {} between racks ({} and {}) which reduces availability", gid, src_dc_rack.rack, dst_dc_rack.rack);
            } else {
                throw std::runtime_error(fmt::format("Attempted to move tablet {} between racks ({} and {}) which would reduce availability", gid, src_dc_rack.rack, dst_dc_rack.rack));
            }
        }

        updates.push_back(canonical_mutation(replica::tablet_mutation_builder(guard.write_timestamp(), table)
            .set_new_replicas(last_token, locator::replace_replica(tinfo.replicas, src, dst))
            .set_stage(last_token, locator::tablet_transition_stage::allow_write_both_read_old)
            .set_transition(last_token, locator::tablet_transition_kind::migration)
            .build()));
        updates.push_back(canonical_mutation(topology_mutation_builder(guard.write_timestamp())
            .set_transition_state(topology::transition_state::tablet_migration)
            .set_version(_topology_state_machine._topology.version + 1)
            .build()));

        sstring reason = format("Moving tablet {} from {} to {}", gid, src, dst);
        rtlogger.info("{}", reason);
        rtlogger.trace("do update {} reason {}", updates, reason);
        topology_change change{std::move(updates)};
        group0_command g0_cmd = _group0->client().prepare_command(std::move(change), guard, reason);
        try {
            co_await _group0->client().add_entry(std::move(g0_cmd), std::move(guard), &_abort_source);
            break;
        } catch (group0_concurrent_modification&) {
            rtlogger.debug("move_tablet(): concurrent modification, retrying");
        }
    }

    // Wait for migration to finish.
    co_await _topology_state_machine.event.wait([&] {
        auto& tmap = get_token_metadata().tablets().get_tablet_map(table);
        return !tmap.get_tablet_transition_info(tmap.get_tablet_id(token));
    });
}

future<locator::load_stats> storage_service::load_stats_for_tablet_based_tables() {
    auto holder = _async_gate.hold();

    if (this_shard_id() != 0) {
        // topology coordinator only exists in shard 0.
        co_return co_await container().invoke_on(0, [&] (auto& ss) {
            return ss.load_stats_for_tablet_based_tables();
        });
    }

    using table_erms_t = std::unordered_map<table_id, const locator::effective_replication_map_ptr>;
    // Creates a snapshot of transitions, so different shards will find their tablet replicas in the
    // same migration stage. Important for intra-node migration.
    const auto erms = co_await std::invoke([this] () -> future<table_erms_t> {
        table_erms_t erms;
        co_await _db.local().get_tables_metadata().for_each_table_gently([&] (table_id id, lw_shared_ptr<replica::table> table) mutable {
            if (table->uses_tablets()) {
                erms.emplace(id, table->get_effective_replication_map());
            }
            return make_ready_future<>();
        });
        co_return std::move(erms);
    });

    // Each node combines a per-table load map from all of its shards and returns it to the coordinator.
    // So if there are 1k nodes, there will be 1k RPCs in total.
    auto load_stats = co_await _db.map_reduce0([&erms] (replica::database& db) -> future<locator::load_stats> {
        locator::load_stats load_stats{};
        auto& tables_metadata = db.get_tables_metadata();

        for (const auto& [id, erm] : erms) {
            auto table = tables_metadata.get_table_if_exists(id);
            if (!table) {
                continue;
            }

            auto& token_metadata = erm->get_token_metadata();
            auto& tmap = token_metadata.tablets().get_tablet_map(id);
            auto me = locator::tablet_replica { token_metadata.get_my_id(), this_shard_id() };

            // It's important to tackle the anomaly in reported size, since both leaving and
            // pending replicas could otherwise be accounted during tablet migration.
            // If transition hasn't reached cleanup stage, then leaving replicas are accounted.
            // If transition is past cleanup stage, then pending replicas are accounted.
            // This helps to reduce the discrepancy window.
            auto tablet_filter = [&tmap, &me] (locator::global_tablet_id id) {
                auto transition = tmap.get_tablet_transition_info(id.tablet);
                auto& info = tmap.get_tablet_info(id.tablet);

                // if tablet is not in transit, it's filtered in.
                if (!transition) {
                    return true;
                }

                bool is_pending = transition->pending_replica == me;
                bool is_leaving = locator::get_leaving_replica(info, *transition) == me;
                auto s = transition->stage;

                return (!is_pending && !is_leaving)
                       || (is_leaving && s < locator::tablet_transition_stage::cleanup)
                       || (is_pending && s >= locator::tablet_transition_stage::cleanup);
            };

            load_stats.tables.emplace(id, table->table_load_stats(tablet_filter));
            co_await coroutine::maybe_yield();
        }

        co_return std::move(load_stats);
    }, locator::load_stats{}, std::plus<locator::load_stats>());

    co_return std::move(load_stats);
}

future<> storage_service::set_tablet_balancing_enabled(bool enabled) {
    auto holder = _async_gate.hold();

    if (this_shard_id() != 0) {
        // group0 is only set on shard 0.
        co_return co_await container().invoke_on(0, [&] (auto& ss) {
            return ss.set_tablet_balancing_enabled(enabled);
        });
    }

    while (true) {
        group0_guard guard = co_await _group0->client().start_operation(&_abort_source);

        while (_topology_state_machine._topology.is_busy()) {
            rtlogger.debug("set_tablet_balancing_enabled(): topology is busy");
            release_guard(std::move(guard));
            co_await _topology_state_machine.event.wait();
            guard = co_await _group0->client().start_operation(&_abort_source);
        }

        std::vector<canonical_mutation> updates;
        updates.push_back(canonical_mutation(topology_mutation_builder(guard.write_timestamp())
            .set_tablet_balancing_enabled(enabled)
            .build()));

        sstring reason = format("Setting tablet balancing to {}", enabled);
        rtlogger.info("{}", reason);
        topology_change change{std::move(updates)};
        group0_command g0_cmd = _group0->client().prepare_command(std::move(change), guard, reason);
        try {
            co_await _group0->client().add_entry(std::move(g0_cmd), std::move(guard), &_abort_source);
            break;
        } catch (group0_concurrent_modification&) {
            rtlogger.debug("set_tablet_balancing_enabled(): concurrent modification");
        }
    }
}

future<join_node_request_result> storage_service::join_node_request_handler(join_node_request_params params) {
    join_node_request_result result;
    rtlogger.info("received request to join from host_id: {}", params.host_id);

    // Sanity check. We should already be using raft topology changes because
    // the node asked us via join_node_query about which node to use and
    // we responded that they should use raft. We cannot go back from raft
    // to legacy (unless we switch to recovery between handling join_node_query
    // and join_node_request, which is extremely unlikely).
    check_ability_to_perform_topology_operation("join");

    if (params.cluster_name != _db.local().get_config().cluster_name()) {
        result.result = join_node_request_result::rejected{
            .reason = ::format("Cluster name check failed. This node cannot join the cluster "
                                "because it expected cluster name \"{}\" and not \"{}\"",
                                params.cluster_name,
                                _db.local().get_config().cluster_name()),
        };
        co_return result;
    }

    if (params.snitch_name != _db.local().get_snitch_name()) {
        result.result = join_node_request_result::rejected{
            .reason = ::format("Snitch name check failed. This node cannot join the cluster "
                                "because it uses \"{}\" and not \"{}\"",
                                params.snitch_name,
                                _db.local().get_snitch_name()),
        };
        co_return result;
    }

    co_await _topology_state_machine.event.when([this] {
        // The first node defines the cluster and inserts its entry to the
        // `system.topology` without checking anything. It is possible that the
        // `join_node_request_handler` fires before the first node sets itself
        // as a normal node, therefore we might need to wait until that happens,
        // here. If we didn't do it, the topology coordinator could handle the
        // joining node as the first one and skip the necessary join node
        // handshake.
        return !_topology_state_machine._topology.normal_nodes.empty();
    });

    auto& g0_server = _group0->group0_server();
    if (params.replaced_id && *params.replaced_id == g0_server.current_leader()) {
        // There is a peculiar case that can happen if the leader is killed
        // and then replaced very quickly:
        //
        // - Cluster with nodes `A`, `B`, `C` - `A` is the topology
        //   coordinator/group0 leader,
        // - `A` is killed,
        // - New node `D` attempts to replace `A` with the same IP as `A`,
        //   sends `join_node_request` rpc to node `B`,
        // - Node `B` handles the RPC and wants to perform group0 operation
        //   and wants to perform a barrier - still thinks that `A`
        //   is the leader and is alive, sends an RPC to its IP,
        // - `D` accidentally receives the request that was meant to `A`
        //   but throws an exception because of host_id mismatch,
        // - Failure is propagated back to `B`, and then to `D` - and `D`
        //   fails the replace operation.
        //
        // We can try to detect if this failure might happen: if the new node
        // is going to replace but the ID of the replaced node is the same
        // as the leader, wait for a short while until a reelection happens.
        // If replaced ID == leader ID, then this indicates either the situation
        // above or an operator error (actually trying to replace a live node).

        const auto timeout = std::chrono::seconds(10);

        rtlogger.warn("the node {} which was requested to be"
                " replaced has the same ID as the current group 0 leader ({});"
                " this looks like an attempt to join a node with the same IP"
                " as a leader which might have just crashed; waiting for"
                " a reelection",
                params.host_id, g0_server.current_leader());

        abort_source as;
        timer<lowres_clock> t;
        t.set_callback([&as] {
            as.request_abort();
        });
        t.arm(timeout);

        try {
            while (!g0_server.current_leader() || *params.replaced_id == g0_server.current_leader()) {
                // FIXME: Wait for the next term instead of sleeping in a loop
                // Waiting for state change is not enough because a new leader
                // might be chosen without us going through the candidate state.
                co_await sleep_abortable(std::chrono::milliseconds(100), as);
            }
        } catch (abort_requested_exception&) {
            rtlogger.warn("the node {} tries to replace the"
                    " current leader {} but the leader didn't change within"
                    " {}s. Rejecting the node",
                    params.host_id,
                    *params.replaced_id,
                    std::chrono::duration_cast<std::chrono::seconds>(timeout).count());

            result.result = join_node_request_result::rejected{
                .reason = format(
                        "It is only allowed to replace dead nodes, however the"
                        " node that was requested to be replaced is still seen"
                        " as the group0 leader after {}s, which indicates that"
                        " it might be still alive. You are either trying to replace"
                        " a live node or trying to replace a node very quickly"
                        " after it went down and reelection didn't happen within"
                        " the timeout. Refusing to continue",
                        std::chrono::duration_cast<std::chrono::seconds>(timeout).count()),
            };
            co_return result;
        }
    }

    while (true) {
        auto guard = co_await _group0->client().start_operation(&_group0_as, raft_timeout{});

        if (const auto *p = _topology_state_machine._topology.find(params.host_id)) {
            const auto& rs = p->second;
            if (rs.state == node_state::left) {
                rtlogger.warn("the node {} attempted to join",
                        " but it was removed from the cluster. Rejecting"
                        " the node",
                        params.host_id);
                result.result = join_node_request_result::rejected{
                    .reason = "The node has already been removed from the cluster",
                };
            } else {
                rtlogger.warn("the node {} attempted to join",
                        " again after an unfinished attempt but it is no longer"
                        " allowed to do so. Rejecting the node",
                        params.host_id);
                result.result = join_node_request_result::rejected{
                    .reason = "The node requested to join before but didn't finish the procedure. "
                              "Please clear the data directory and restart.",
                };
            }
            co_return result;
        }

        if (params.replaced_id) {
            try {
                auto ip = co_await wait_for_ip(*params.replaced_id, _group0->address_map(), _group0_as);
                if (is_me(ip) || _gossiper.is_alive(ip)) {
                    result.result = join_node_request_result::rejected{
                        .reason = fmt::format("tried to replace alive node {}", *params.replaced_id),
                    };
                    co_return result;
                }
            } catch (wait_for_ip_timeout& ex) {
                rtlogger.warn("Failed to check liveness for replaced id {}. Asumming it is dead, Error: {}", *params.replaced_id, ex);
            }
        }

        auto mutation = build_mutation_from_join_params(params, guard);

        topology_change change{{std::move(mutation)}};
        group0_command g0_cmd = _group0->client().prepare_command(std::move(change), guard,
                format("raft topology: placing join request for {}", params.host_id));

        co_await utils::get_local_injector().inject("join-node-before-add-entry", [] (auto& handler) -> future<> {
            rtlogger.info("join-node-before-add-entry injection hit");
            co_await handler.wait_for_message(std::chrono::steady_clock::now() + std::chrono::minutes{5});
            rtlogger.info("join-node-before-add-entry injection done");
        });

        try {
            // Make replaced node and ignored nodes non voters earlier for better HA
            co_await _group0->make_nonvoters(ignored_nodes_from_join_params(params), _group0_as, raft_timeout{});
            co_await _group0->client().add_entry(std::move(g0_cmd), std::move(guard), &_group0_as, raft_timeout{});
            break;
        } catch (group0_concurrent_modification&) {
            rtlogger.info("join_node_request: concurrent operation is detected, retrying.");
        }
    }

    rtlogger.info("placed join request for {}", params.host_id);

    // Success
    result.result = join_node_request_result::ok {};
    co_return result;
}

future<join_node_response_result> storage_service::join_node_response_handler(join_node_response_params params) {
    assert(this_shard_id() == 0);

    // Usually this handler will only run once, but there are some cases where we might get more than one RPC,
    // possibly happening at the same time, e.g.:
    //
    // - Another node becomes the topology coordinator while the old one waits for the RPC,
    // - Topology coordinator finished the RPC but failed to update the group 0 state.

    // Serialize handling the responses.
    auto lock = co_await get_units(_join_node_response_handler_mutex, 1);

    // Wait until we sent and completed the join_node_request RPC
    co_await _join_node_request_done.get_shared_future(_group0_as);

    if (_join_node_response_done.available()) {
        // We already handled this RPC. No need to retry it.
        rtlogger.info("the node got join_node_response RPC for the second time, ignoring");

        if (std::holds_alternative<join_node_response_params::accepted>(params.response)
                &&  _join_node_response_done.failed()) {
            // The topology coordinator accepted the node that was rejected before or failed while handling
            // the response. Inform the coordinator about it so it moves the node to the left state.
            throw _join_node_response_done.get_shared_future().get_exception();
        }

        co_return join_node_response_result{};
    }

    try {
        co_return co_await std::visit(overloaded_functor {
            [&] (const join_node_response_params::accepted& acc) -> future<join_node_response_result> {
                // Allow other nodes to mark the replacing node as alive. It has
                // effect only if the replacing node is reusing the IP of the
                // replaced node. In such a case, we do not allow the replacing
                // node to advertise itself earlier. Thanks to this, if the
                // topology sees the node being replaced as alive, it can safely
                // reject the join request because it can be sure that it is not
                // the replacing node that is alive.
                co_await _gossiper.advertise_to_nodes({});

                // Do a read barrier to read/initialize the topology state
                auto& raft_server = _group0->group0_server();
                co_await raft_server.read_barrier(&_group0_as);

                // Calculate nodes to ignore
                // TODO: ignore_dead_nodes setting for bootstrap
                std::unordered_set<raft::server_id> ignored_ids = _topology_state_machine._topology.ignored_nodes;
                auto my_request_it =
                        _topology_state_machine._topology.req_param.find(_group0->load_my_id());
                if (my_request_it != _topology_state_machine._topology.req_param.end()) {
                    if (auto* replace = std::get_if<service::replace_param>(&my_request_it->second)) {
                        ignored_ids.insert(replace->replaced_id);
                    }
                }

                // After this RPC finishes, repair or streaming will be run, and
                // both of them require this node to see the normal nodes as UP.
                // This condition might not be true yet as this information is
                // propagated through gossip. In order to reduce the chance of
                // repair/streaming failure, wait here until we see normal nodes
                // as UP (or the timeout elapses).
                const auto& amap = _group0->address_map();
                std::vector<gms::inet_address> sync_nodes;
                // FIXME: https://github.com/scylladb/scylladb/issues/12279
                // Keep trying to translate host IDs to IPs until all are available in gossip
                // Ultimately, we should take this information from token_metadata
                const auto sync_nodes_resolve_deadline = lowres_clock::now() + wait_for_live_nodes_timeout;
                while (true) {
                    sync_nodes.clear();
                    std::vector<raft::server_id> untranslated_ids;
                    for (const auto& [id, _] : _topology_state_machine._topology.normal_nodes) {
                        if (ignored_ids.contains(id)) {
                            continue;
                        }
                        if (auto ip = amap.find(id)) {
                            sync_nodes.push_back(*ip);
                        } else {
                            untranslated_ids.push_back(id);
                        }
                    }

                    if (!untranslated_ids.empty()) {
                        if (lowres_clock::now() > sync_nodes_resolve_deadline) {
                            throw std::runtime_error(fmt::format(
                                    "Failed to obtain IP addresses of nodes that should be seen"
                                    " as alive within {}s",
                                    std::chrono::duration_cast<std::chrono::seconds>(wait_for_live_nodes_timeout).count()));
                        }

                        static logger::rate_limit rate_limit{std::chrono::seconds(1)};
                        rtlogger.log(log_level::warn, rate_limit, "cannot map nodes {} to ips, retrying.",
                                untranslated_ids);

                        co_await sleep_abortable(std::chrono::milliseconds(5), _group0_as);
                    } else {
                        break;
                    }
                }

                rtlogger.info("coordinator accepted request to join, "
                        "waiting for nodes {} to be alive before responding and continuing",
                        sync_nodes);
                co_await _gossiper.wait_alive(sync_nodes, wait_for_live_nodes_timeout);
                rtlogger.info("nodes {} are alive", sync_nodes);

                // Unblock waiting join_node_rpc_handshaker::post_server_start,
                // which will start the raft server and continue
                _join_node_response_done.set_value();

                co_return join_node_response_result{};
            },
            [&] (const join_node_response_params::rejected& rej) -> future<join_node_response_result> {
                auto eptr = std::make_exception_ptr(std::runtime_error(
                        format("the topology coordinator rejected request to join the cluster: {}", rej.reason)));
                _join_node_response_done.set_exception(std::move(eptr));

                co_return join_node_response_result{};
            },
        }, params.response);
    } catch (...) {
        auto eptr = std::current_exception();
        rtlogger.warn("error while handling the join response from the topology coordinator. "
                "The node will not join the cluster. Error: {}", eptr);
        _join_node_response_done.set_exception(std::move(eptr));

        throw;
    }
}

future<std::vector<canonical_mutation>> storage_service::get_system_mutations(schema_ptr schema) {
    std::vector<canonical_mutation> result;
    auto rs = co_await db::system_keyspace::query_mutations(_db, schema);
    result.reserve(rs->partitions().size());
    boost::range::transform(
            rs->partitions(), std::back_inserter(result), [schema] (const partition& p) {
        return canonical_mutation{p.mut().unfreeze(schema)};
    });
    co_return result;
}

future<std::vector<canonical_mutation>> storage_service::get_system_mutations(const sstring& ks_name, const sstring& cf_name) {
    auto s = _db.local().find_schema(ks_name, cf_name);
    return get_system_mutations(s);
}

node_state storage_service::get_node_state(locator::host_id id) {
    if (this_shard_id() != 0) {
        on_internal_error(rtlogger, "cannot access node state on non zero shard");
    }
    auto rid = raft::server_id{id.uuid()};
    if (!_topology_state_machine._topology.contains(rid)) {
        on_internal_error(rtlogger, format("unknown node {}", rid));
    }
    auto p = _topology_state_machine._topology.find(rid);
    if (!p) {
        return node_state::left;
    }
    return p->second.state;
}

void storage_service::init_messaging_service(bool raft_topology_change_enabled) {
    _messaging.local().register_node_ops_cmd([this] (const rpc::client_info& cinfo, node_ops_cmd_request req) {
        auto coordinator = cinfo.retrieve_auxiliary<gms::inet_address>("baddr");
        std::optional<locator::host_id> coordinator_host_id;
        if (const auto* id = cinfo.retrieve_auxiliary_opt<locator::host_id>("host_id")) {
            coordinator_host_id = *id;
        }
        return container().invoke_on(0, [coordinator, coordinator_host_id, req = std::move(req)] (auto& ss) mutable {
            return ss.node_ops_cmd_handler(coordinator, coordinator_host_id, std::move(req));
        });
    });
    if (raft_topology_change_enabled) {
        auto handle_raft_rpc = [this] (raft::server_id dst_id, auto handler) {
            return container().invoke_on(0, [dst_id, handler = std::move(handler)] (auto& ss) mutable {
                if (!ss._group0 || !ss._group0->joined_group0()) {
                    throw std::runtime_error("The node did not join group 0 yet");
                }
                if (ss._group0->load_my_id() != dst_id) {
                    throw raft_destination_id_not_correct(ss._group0->load_my_id(), dst_id);
                }
                return handler(ss);
            });
        };
        ser::storage_service_rpc_verbs::register_raft_topology_cmd(&_messaging.local(), [handle_raft_rpc] (raft::server_id dst_id, raft::term_t term, uint64_t cmd_index, raft_topology_cmd cmd) {
            return handle_raft_rpc(dst_id, [cmd = std::move(cmd), term, cmd_index] (auto& ss) {
                return ss.raft_topology_cmd_handler(term, cmd_index, cmd);
            });
        });
        ser::storage_service_rpc_verbs::register_raft_pull_topology_snapshot(&_messaging.local(), [handle_raft_rpc] (raft::server_id dst_id, raft_topology_pull_params params) {
            return handle_raft_rpc(dst_id, [] (storage_service& ss) -> future<raft_topology_snapshot> {
                std::vector<canonical_mutation> topology_mutations;
                {
                    // FIXME: make it an rwlock, here we only need to lock for reads,
                    // might be useful if multiple nodes are trying to pull concurrently.
                    auto read_apply_mutex_holder = co_await ss._group0->client().hold_read_apply_mutex();
                    topology_mutations = co_await ss.get_system_mutations(db::system_keyspace::NAME, db::system_keyspace::TOPOLOGY);
                }

                std::vector<canonical_mutation> cdc_generation_mutations;
                {
                    // FIXME: when we bootstrap nodes in quick succession, the timestamp of the newest CDC generation
                    // may be for some time larger than the clocks of our nodes. The last bootstrapped node will only
                    // read the newest CDC generation into memory and not earlier ones, so it will only be able
                    // to coordinate writes to CDC-enabled tables after its clock advances to reach the newest
                    // generation's timestamp. In other words, it may not be able to coordinate writes for some
                    // time after bootstrapping and drivers connecting to it will receive errors.
                    // To fix that, we could store in topology a small history of recent CDC generation IDs
                    // (garbage-collected with time) instead of just the last one, and load all of them.
                    // Alternatively, a node would wait for some time before switching to normal state.
                    auto read_apply_mutex_holder = co_await ss._group0->client().hold_read_apply_mutex();
                    cdc_generation_mutations = co_await ss.get_system_mutations(db::system_keyspace::NAME, db::system_keyspace::CDC_GENERATIONS_V3);
                }

                std::vector<canonical_mutation> topology_requests_mutations;
                {
                    // FIXME: make it an rwlock, here we only need to lock for reads,
                    // might be useful if multiple nodes are trying to pull concurrently.
                    auto read_apply_mutex_holder = co_await ss._group0->client().hold_read_apply_mutex();
                    topology_requests_mutations = co_await ss.get_system_mutations(db::system_keyspace::NAME, db::system_keyspace::TOPOLOGY_REQUESTS);
                }

                co_return raft_topology_snapshot{
                    .topology_mutations = std::move(topology_mutations),
                    .cdc_generation_mutations = std::move(cdc_generation_mutations),
                    .topology_requests_mutations = std::move(topology_requests_mutations),
                };
            });
        });
        ser::storage_service_rpc_verbs::register_raft_pull_snapshot(&_messaging.local(), [handle_raft_rpc] (raft::server_id dst_id, raft_snapshot_pull_params params) {
            return handle_raft_rpc(dst_id, [params = std::move(params)] (storage_service& ss) -> future<raft_snapshot> {
                utils::chunked_vector<canonical_mutation> mutations;
                // FIXME: make it an rwlock, here we only need to lock for reads,
                // might be useful if multiple nodes are trying to pull concurrently.
                auto read_apply_mutex_holder = co_await ss._group0->client().hold_read_apply_mutex();
                for (const auto& table : params.tables) {
                    auto schema = ss._db.local().find_schema(table);
                    auto muts = co_await ss.get_system_mutations(schema);
                    mutations.reserve(mutations.size() + muts.size());
                    std::move(muts.begin(), muts.end(), std::back_inserter(mutations));
                }

                co_return raft_snapshot{
                    .mutations = std::move(mutations),
                };
            });
        });
        ser::storage_service_rpc_verbs::register_tablet_stream_data(&_messaging.local(), [handle_raft_rpc] (raft::server_id dst_id, locator::global_tablet_id tablet) {
            return handle_raft_rpc(dst_id, [tablet] (auto& ss) {
                return ss.stream_tablet(tablet);
            });
        });
        ser::storage_service_rpc_verbs::register_tablet_cleanup(&_messaging.local(), [handle_raft_rpc] (raft::server_id dst_id, locator::global_tablet_id tablet) {
            return handle_raft_rpc(dst_id, [tablet] (auto& ss) {
                return ss.cleanup_tablet(tablet);
            });
        });
        ser::storage_service_rpc_verbs::register_table_load_stats(&_messaging.local(), [handle_raft_rpc] (raft::server_id dst_id) {
            return handle_raft_rpc(dst_id, [] (auto& ss) mutable {
                return ss.load_stats_for_tablet_based_tables();
            });
        });
        ser::join_node_rpc_verbs::register_join_node_request(&_messaging.local(), [handle_raft_rpc] (raft::server_id dst_id, service::join_node_request_params params) {
            return handle_raft_rpc(dst_id, [params = std::move(params)] (auto& ss) mutable {
                return ss.join_node_request_handler(std::move(params));
            });
        });
        ser::join_node_rpc_verbs::register_join_node_response(&_messaging.local(), [this] (raft::server_id dst_id, service::join_node_response_params params) {
            return container().invoke_on(0, [dst_id, params = std::move(params)] (auto& ss) mutable -> future<join_node_response_result> {
                co_await ss._join_node_group0_started.get_shared_future(ss._group0_as);
                if (ss._group0->load_my_id() != dst_id) {
                    throw raft_destination_id_not_correct(ss._group0->load_my_id(), dst_id);
                }
                co_return co_await ss.join_node_response_handler(std::move(params));
            });
        });
        ser::join_node_rpc_verbs::register_join_node_query(&_messaging.local(), [handle_raft_rpc] (raft::server_id dst_id, service::join_node_query_params) {
            return handle_raft_rpc(dst_id, [] (auto& ss) -> future<join_node_query_result> {
                if (!ss.legacy_topology_change_enabled() && !ss.raft_topology_change_enabled()) {
                    throw std::runtime_error("The cluster is upgrading to raft topology. Nodes cannot join at this time.");
                }
                auto result = join_node_query_result{
                    .topo_mode = ss.raft_topology_change_enabled()
                            ? join_node_query_result::topology_mode::raft
                            : join_node_query_result::topology_mode::legacy,
                };
                return make_ready_future<join_node_query_result>(std::move(result));
            });
        });
    }
}

future<> storage_service::uninit_messaging_service() {
    return when_all_succeed(
        _messaging.local().unregister_node_ops_cmd(),
        ser::storage_service_rpc_verbs::unregister(&_messaging.local()),
        ser::join_node_rpc_verbs::unregister(&_messaging.local())
    ).discard_result();
}

void storage_service::do_isolate_on_error(disk_error type)
{
    static std::atomic<bool> isolated = { false };

    if (!isolated.exchange(true)) {
        slogger.error("Shutting down communications due to I/O errors until operator intervention: {} error: {}", type == disk_error::commit ? "Commitlog" : "Disk", std::current_exception());
        // isolated protect us against multiple stops
        //FIXME: discarded future.
        (void)isolate();
    }
}

future<> storage_service::isolate() {
    return run_with_no_api_lock([] (storage_service& ss) {
        return ss.stop_transport();
    });
}

future<sstring> storage_service::get_removal_status() {
    return run_with_no_api_lock([] (storage_service& ss) {
        return make_ready_future<sstring>(sstring("No token removals in process."));
    });
}

future<> storage_service::force_remove_completion() {
    if (raft_topology_change_enabled()) {
        return make_exception_future<>(std::runtime_error("The unsafe nodetool removenode force is not supported anymore"));
    }

    slogger.warn("The unsafe nodetool removenode force is deprecated and will not be supported in future releases");
    return run_with_no_api_lock([] (storage_service& ss) -> future<> {
        while (!ss._operation_in_progress.empty()) {
            if (ss._operation_in_progress != sstring("removenode")) {
                throw std::runtime_error(::format("Operation {} is in progress, try again", ss._operation_in_progress));
            }

            // This flag will make removenode stop waiting for the confirmation,
            // wait it to complete
            slogger.info("Operation removenode is in progress, wait for it to complete");
            co_await sleep_abortable(std::chrono::seconds(1), ss._abort_source);
        }
        ss._operation_in_progress = sstring("removenode_force");

        try {
            const auto& tm = ss.get_token_metadata();
            if (!tm.get_leaving_endpoints().empty()) {
                auto leaving = tm.get_leaving_endpoints();
                slogger.warn("Removal not confirmed, Leaving={}", leaving);
                for (auto host_id : leaving) {
                    const auto endpoint = tm.get_endpoint_for_host_id_if_known(host_id);
                    if (!endpoint) {
                        slogger.warn("No endpoint is found for host_id {}", host_id);
                        continue;
                    }
                    auto tokens = tm.get_tokens(host_id);
                    auto permit = co_await ss._gossiper.lock_endpoint(*endpoint, gms::null_permit_id);
                    const auto& pid = permit.id();
                    co_await ss._gossiper.advertise_token_removed(*endpoint, host_id, pid);
                    std::unordered_set<token> tokens_set(tokens.begin(), tokens.end());
                    co_await ss.excise(tokens_set, *endpoint, pid);

                    slogger.info("force_remove_completion: removing endpoint {} from group 0", *endpoint);
                    assert(ss._group0);
                    bool raft_available = co_await ss._group0->wait_for_raft();
                    if (raft_available) {
                        co_await ss._group0->remove_from_group0(raft::server_id{host_id.uuid()});
                    }
                }
            } else {
                slogger.warn("No tokens to force removal on, call 'removenode' first");
            }
            ss._operation_in_progress = {};
        } catch (...) {
            ss._operation_in_progress = {};
            throw;
        }
    });
}

/**
 * Takes an ordered list of adjacent tokens and divides them in the specified number of ranges.
 */
static std::vector<std::pair<dht::token_range, uint64_t>>
calculate_splits(std::vector<dht::token> tokens, uint64_t split_count, replica::column_family& cf) {
    auto sstables = cf.get_sstables();
    const double step = static_cast<double>(tokens.size() - 1) / split_count;
    auto prev_token_idx = 0;
    std::vector<std::pair<dht::token_range, uint64_t>> splits;
    splits.reserve(split_count);
    for (uint64_t i = 1; i <= split_count; ++i) {
        auto index = static_cast<uint32_t>(std::round(i * step));
        dht::token_range range({{ std::move(tokens[prev_token_idx]), false }}, {{ tokens[index], true }});
        // always return an estimate > 0 (see CASSANDRA-7322)
        uint64_t estimated_keys_for_range = 0;
        for (auto&& sst : *sstables) {
            estimated_keys_for_range += sst->estimated_keys_for_range(range);
        }
        splits.emplace_back(std::move(range), std::max(static_cast<uint64_t>(cf.schema()->min_index_interval()), estimated_keys_for_range));
        prev_token_idx = index;
    }
    return splits;
};

std::vector<std::pair<dht::token_range, uint64_t>>
storage_service::get_splits(const sstring& ks_name, const sstring& cf_name, wrapping_interval<dht::token> range, uint32_t keys_per_split) {
    using range_type = dht::token_range;
    auto& cf = _db.local().find_column_family(ks_name, cf_name);
    auto schema = cf.schema();
    auto sstables = cf.get_sstables();
    uint64_t total_row_count_estimate = 0;
    std::vector<dht::token> tokens;
    std::vector<range_type> unwrapped;
    if (range.is_wrap_around(dht::token_comparator())) {
        auto uwr = range.unwrap();
        unwrapped.emplace_back(std::move(uwr.second));
        unwrapped.emplace_back(std::move(uwr.first));
    } else {
        unwrapped.emplace_back(std::move(range));
    }
    tokens.push_back(std::move(unwrapped[0].start().value_or(range_type::bound(dht::minimum_token()))).value());
    for (auto&& r : unwrapped) {
        std::vector<dht::token> range_tokens;
        for (auto &&sst : *sstables) {
            total_row_count_estimate += sst->estimated_keys_for_range(r);
            auto keys = sst->get_key_samples(*cf.schema(), r);
            std::transform(keys.begin(), keys.end(), std::back_inserter(range_tokens), [](auto&& k) { return std::move(k.token()); });
        }
        std::sort(range_tokens.begin(), range_tokens.end());
        std::move(range_tokens.begin(), range_tokens.end(), std::back_inserter(tokens));
    }
    tokens.push_back(std::move(unwrapped[unwrapped.size() - 1].end().value_or(range_type::bound(dht::maximum_token()))).value());

    // split_count should be much smaller than number of key samples, to avoid huge sampling error
    constexpr uint32_t min_samples_per_split = 4;
    uint64_t max_split_count = tokens.size() / min_samples_per_split + 1;
    uint64_t split_count = std::max(uint64_t(1), std::min(max_split_count, total_row_count_estimate / keys_per_split));

    return calculate_splits(std::move(tokens), split_count, cf);
};

dht::token_range_vector
storage_service::get_ranges_for_endpoint(const locator::effective_replication_map_ptr& erm, const gms::inet_address& ep) const {
    return erm->get_ranges(ep);
}

// Caller is responsible to hold token_metadata valid until the returned future is resolved
future<dht::token_range_vector>
storage_service::get_all_ranges(const std::vector<token>& sorted_tokens) const {
    if (sorted_tokens.empty())
        co_return dht::token_range_vector();
    int size = sorted_tokens.size();
    dht::token_range_vector ranges;
    ranges.reserve(size + 1);
    ranges.push_back(dht::token_range::make_ending_with(interval_bound<token>(sorted_tokens[0], true)));
    co_await coroutine::maybe_yield();
    for (int i = 1; i < size; ++i) {
        dht::token_range r(wrapping_interval<token>::bound(sorted_tokens[i - 1], false), wrapping_interval<token>::bound(sorted_tokens[i], true));
        ranges.push_back(r);
        co_await coroutine::maybe_yield();
    }
    ranges.push_back(dht::token_range::make_starting_with(interval_bound<token>(sorted_tokens[size-1], false)));

    co_return ranges;
}

inet_address_vector_replica_set
storage_service::get_natural_endpoints(const sstring& keyspace,
        const sstring& cf, const sstring& key) const {
    auto& table = _db.local().find_column_family(keyspace, cf);
    const auto schema = table.schema();
    partition_key pk = partition_key::from_nodetool_style_string(schema, key);
    dht::token token = schema->get_partitioner().get_token(*schema, pk.view());
    const auto& ks = _db.local().find_keyspace(keyspace);
    if (ks.uses_tablets()) {
        return table.get_effective_replication_map()->get_natural_endpoints(token);
    }
    return ks.get_vnode_effective_replication_map()->get_natural_endpoints(token);
}

future<> endpoint_lifecycle_notifier::notify_down(gms::inet_address endpoint) {
    return seastar::async([this, endpoint] {
        _subscribers.thread_for_each([endpoint] (endpoint_lifecycle_subscriber* subscriber) {
            try {
                subscriber->on_down(endpoint);
            } catch (...) {
                slogger.warn("Down notification failed {}: {}", endpoint, std::current_exception());
            }
        });
    });
}

future<> storage_service::notify_down(inet_address endpoint) {
    co_await container().invoke_on_all([endpoint] (auto&& ss) {
        ss._messaging.local().remove_rpc_client(netw::msg_addr{endpoint, 0});
        return ss._lifecycle_notifier.notify_down(endpoint);
    });
    slogger.debug("Notify node {} has been down", endpoint);
}

future<> endpoint_lifecycle_notifier::notify_left(gms::inet_address endpoint) {
    return seastar::async([this, endpoint] {
        _subscribers.thread_for_each([endpoint] (endpoint_lifecycle_subscriber* subscriber) {
            try {
                subscriber->on_leave_cluster(endpoint);
            } catch (...) {
                slogger.warn("Leave cluster notification failed {}: {}", endpoint, std::current_exception());
            }
        });
    });
}

future<> storage_service::notify_left(inet_address endpoint) {
    co_await container().invoke_on_all([endpoint] (auto&& ss) {
        return ss._lifecycle_notifier.notify_left(endpoint);
    });
    slogger.debug("Notify node {} has left the cluster", endpoint);
}

future<> endpoint_lifecycle_notifier::notify_up(gms::inet_address endpoint) {
    return seastar::async([this, endpoint] {
        _subscribers.thread_for_each([endpoint] (endpoint_lifecycle_subscriber* subscriber) {
            try {
                subscriber->on_up(endpoint);
            } catch (...) {
                slogger.warn("Up notification failed {}: {}", endpoint, std::current_exception());
            }
        });
    });
}

future<> storage_service::notify_up(inet_address endpoint) {
    if (!_gossiper.is_cql_ready(endpoint) || !_gossiper.is_alive(endpoint)) {
        co_return;
    }
    co_await container().invoke_on_all([endpoint] (auto&& ss) {
        return ss._lifecycle_notifier.notify_up(endpoint);
    });
    slogger.debug("Notify node {} has been up", endpoint);
}

future<> endpoint_lifecycle_notifier::notify_joined(gms::inet_address endpoint) {
    return seastar::async([this, endpoint] {
        _subscribers.thread_for_each([endpoint] (endpoint_lifecycle_subscriber* subscriber) {
            try {
                subscriber->on_join_cluster(endpoint);
            } catch (...) {
                slogger.warn("Join cluster notification failed {}: {}", endpoint, std::current_exception());
            }
        });
    });
}

future<> storage_service::notify_joined(inet_address endpoint) {
    co_await utils::get_local_injector().inject(
        "storage_service_notify_joined_sleep", std::chrono::milliseconds{500});

    co_await container().invoke_on_all([endpoint] (auto&& ss) {
        return ss._lifecycle_notifier.notify_joined(endpoint);
    });
    slogger.debug("Notify node {} has joined the cluster", endpoint);
}

future<> storage_service::remove_rpc_client_with_ignored_topology(inet_address endpoint) {
    return container().invoke_on_all([endpoint] (auto&& ss) {
        ss._messaging.local().remove_rpc_client_with_ignored_topology(netw::msg_addr{endpoint, 0});
    });
}

future<> storage_service::notify_cql_change(inet_address endpoint, bool ready) {
    if (ready) {
        co_await notify_up(endpoint);
    } else {
        co_await notify_down(endpoint);
    }
}

bool storage_service::is_normal_state_handled_on_boot(gms::inet_address node) {
    return _normal_state_handled_on_boot.contains(node);
}

// Wait for normal state handlers to finish on boot
future<> storage_service::wait_for_normal_state_handled_on_boot() {
    static logger::rate_limit rate_limit{std::chrono::seconds{5}};
    static auto fmt_nodes_with_statuses = [this] (const auto& eps) {
        return boost::algorithm::join(
                eps | boost::adaptors::transformed([this] (const auto& ep) {
                    return ::format("({}, status={})", ep, _gossiper.get_gossip_status(ep));
                }), ", ");
    };

    slogger.info("Started waiting for normal state handlers to finish");
    auto start_time = std::chrono::steady_clock::now();
    std::vector<gms::inet_address> eps;
    while (true) {
        eps = _gossiper.get_endpoints();
        auto it = std::partition(eps.begin(), eps.end(),
                [this, me = get_broadcast_address()] (const gms::inet_address& ep) {
            return ep == me || !_gossiper.is_normal_ring_member(ep) || is_normal_state_handled_on_boot(ep);
        });

        if (it == eps.end()) {
            break;
        }

        if (std::chrono::steady_clock::now() > start_time + std::chrono::seconds(60)) {
            auto err = ::format("Timed out waiting for normal state handlers to finish for nodes {}",
                    fmt_nodes_with_statuses(boost::make_iterator_range(it, eps.end())));
            slogger.error("{}", err);
            throw std::runtime_error{std::move(err)};
        }

        slogger.log(log_level::info, rate_limit, "Normal state handlers not yet finished for nodes {}",
                    fmt_nodes_with_statuses(boost::make_iterator_range(it, eps.end())));

        co_await sleep_abortable(std::chrono::milliseconds{100}, _abort_source);
    }

    slogger.info("Finished waiting for normal state handlers; endpoints observed in gossip: {}",
                 fmt_nodes_with_statuses(eps));
}

storage_service::topology_change_kind storage_service::upgrade_state_to_topology_op_kind(topology::upgrade_state_type upgrade_state) const {
    switch (upgrade_state) {
    case topology::upgrade_state_type::done:
        return topology_change_kind::raft;
    case topology::upgrade_state_type::not_upgraded:
        // Did not start upgrading to raft topology yet - use legacy
        return topology_change_kind::legacy;
    default:
        // Upgrade is in progress - disallow topology operations
        return topology_change_kind::upgrading_to_raft;
    }
}

future<bool> storage_service::is_cleanup_allowed(sstring keyspace) {
    return container().invoke_on(0, [keyspace = std::move(keyspace)] (storage_service& ss) {
        const auto my_id = ss.get_token_metadata().get_my_id();
        const auto pending_ranges = ss._db.local().find_keyspace(keyspace).get_vnode_effective_replication_map()->has_pending_ranges(my_id);
        const bool is_bootstrap_mode = ss._operation_mode == mode::BOOTSTRAP;
        slogger.debug("is_cleanup_allowed: keyspace={}, is_bootstrap_mode={}, pending_ranges={}",
                keyspace, is_bootstrap_mode, pending_ranges);
        return !is_bootstrap_mode && !pending_ranges;
    });
}

bool storage_service::is_repair_based_node_ops_enabled(streaming::stream_reason reason) {
    static const std::unordered_map<sstring, streaming::stream_reason> reason_map{
        {"replace", streaming::stream_reason::replace},
        {"bootstrap", streaming::stream_reason::bootstrap},
        {"decommission", streaming::stream_reason::decommission},
        {"removenode", streaming::stream_reason::removenode},
        {"rebuild", streaming::stream_reason::rebuild},
    };
    auto enabled_list_str = _db.local().get_config().allowed_repair_based_node_ops();
    std::vector<sstring> enabled_list = utils::split_comma_separated_list(std::move(enabled_list_str));
    std::unordered_set<streaming::stream_reason> enabled_set;
    for (const sstring& op : enabled_list) {
        try {
            auto it = reason_map.find(op);
            if (it != reason_map.end()) {
                enabled_set.insert(it->second);
            } else {
                throw std::invalid_argument(::format("unsupported operation name: {}", op));
            }
        } catch (...) {
            throw std::invalid_argument(::format("Failed to parse allowed_repair_based_node_ops parameter [{}]: {}",
                    enabled_list_str, std::current_exception()));
        }
    }
    bool global_enabled = _db.local().get_config().enable_repair_based_node_ops();
    slogger.info("enable_repair_based_node_ops={}, allowed_repair_based_node_ops={{{}}}", global_enabled, fmt::join(enabled_set, " ,"));
    return global_enabled && enabled_set.contains(reason);
}

future<> storage_service::start_maintenance_mode() {
    set_mode(mode::MAINTENANCE);

    return mutate_token_metadata([this] (mutable_token_metadata_ptr token_metadata) -> future<> {
        return token_metadata->update_normal_tokens({ dht::token{} }, get_token_metadata_ptr()->get_topology().my_host_id());
    }, acquire_merge_lock::yes);
}

node_ops_meta_data::node_ops_meta_data(
        node_ops_id ops_uuid,
        gms::inet_address coordinator,
        std::list<gms::inet_address> ignore_nodes,
        std::chrono::seconds watchdog_interval,
        std::function<future<> ()> abort_func,
        std::function<void ()> signal_func)
    : _ops_uuid(std::move(ops_uuid))
    , _coordinator(std::move(coordinator))
    , _abort(std::move(abort_func))
    , _abort_source(seastar::make_shared<abort_source>())
    , _signal(std::move(signal_func))
    , _ops(seastar::make_shared<node_ops_info>(_ops_uuid, _abort_source, std::move(ignore_nodes)))
    , _watchdog([sig = _signal] { sig(); })
    , _watchdog_interval(watchdog_interval)
{
    slogger.debug("node_ops_meta_data: ops_uuid={} arm interval={}", _ops_uuid, _watchdog_interval.count());
    _watchdog.arm(_watchdog_interval);
}

future<> node_ops_meta_data::abort() {
    slogger.debug("node_ops_meta_data: ops_uuid={} abort", _ops_uuid);
    _watchdog.cancel();
    return _abort();
}

void node_ops_meta_data::update_watchdog() {
    slogger.debug("node_ops_meta_data: ops_uuid={} update_watchdog", _ops_uuid);
    if (_abort_source->abort_requested()) {
        return;
    }
    _watchdog.cancel();
    _watchdog.arm(_watchdog_interval);
}

void node_ops_meta_data::cancel_watchdog() {
    slogger.debug("node_ops_meta_data: ops_uuid={} cancel_watchdog", _ops_uuid);
    _watchdog.cancel();
}

shared_ptr<node_ops_info> node_ops_meta_data::get_ops_info() {
    return _ops;
}

shared_ptr<abort_source> node_ops_meta_data::get_abort_source() {
    return _abort_source;
}

future<> storage_service::node_ops_update_heartbeat(node_ops_id ops_uuid) {
    slogger.debug("node_ops_update_heartbeat: ops_uuid={}", ops_uuid);
    auto permit = co_await seastar::get_units(_node_ops_abort_sem, 1);
    auto it = _node_ops.find(ops_uuid);
    if (it != _node_ops.end()) {
        node_ops_meta_data& meta = it->second;
        meta.update_watchdog();
    }
}

future<> storage_service::node_ops_done(node_ops_id ops_uuid) {
    slogger.debug("node_ops_done: ops_uuid={}", ops_uuid);
    auto permit = co_await seastar::get_units(_node_ops_abort_sem, 1);
    auto it = _node_ops.find(ops_uuid);
    if (it != _node_ops.end()) {
        node_ops_meta_data& meta = it->second;
        meta.cancel_watchdog();
        _node_ops.erase(it);
    }
}

future<> storage_service::node_ops_abort(node_ops_id ops_uuid) {
    slogger.debug("node_ops_abort: ops_uuid={}", ops_uuid);
    auto permit = co_await seastar::get_units(_node_ops_abort_sem, 1);

    if (!ops_uuid) {
        for (auto& [uuid, meta] : _node_ops) {
            co_await meta.abort();
            auto as = meta.get_abort_source();
            if (as && !as->abort_requested()) {
                as->request_abort();
            }
        }
        _node_ops.clear();
        co_return;
    }

    auto it = _node_ops.find(ops_uuid);
    if (it != _node_ops.end()) {
        node_ops_meta_data& meta = it->second;
        slogger.info("aborting node operation ops_uuid={}", ops_uuid);
        co_await meta.abort();
        auto as = meta.get_abort_source();
        if (as && !as->abort_requested()) {
            as->request_abort();
        }
        _node_ops.erase(it);
    } else {
        slogger.info("aborting node operation ops_uuid={}: operation not found", ops_uuid);
    }
}

void storage_service::node_ops_signal_abort(std::optional<node_ops_id> ops_uuid) {
    if (ops_uuid) {
        slogger.warn("Node operation ops_uuid={} watchdog expired. Signaling the operation to abort", ops_uuid);
    }
    _node_ops_abort_queue.push_back(ops_uuid);
    _node_ops_abort_cond.signal();
}

future<> storage_service::node_ops_abort_thread() {
    slogger.info("Started node_ops_abort_thread");
    for (;;) {
        co_await _node_ops_abort_cond.wait([this] { return !_node_ops_abort_queue.empty(); });
        slogger.debug("Awoke node_ops_abort_thread: node_ops_abort_queue={}", _node_ops_abort_queue);
        while (!_node_ops_abort_queue.empty()) {
            auto uuid_opt = _node_ops_abort_queue.front();
            _node_ops_abort_queue.pop_front();
            try {
                co_await node_ops_abort(uuid_opt.value_or(node_ops_id::create_null_id()));
            } catch (...) {
                slogger.warn("Failed to abort node operation ops_uuid={}: {}", *uuid_opt, std::current_exception());
            }
            if (!uuid_opt) {
                slogger.info("Stopped node_ops_abort_thread");
                co_return;
            }
        }
    }
    __builtin_unreachable();
}

} // namespace service

