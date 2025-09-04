/*
 *
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.0 and Apache-2.0)
 */

#include "gms/inet_address.hh"
#include "gms/endpoint_state.hh"
#include "gms/gossip_digest.hh"
#include "gms/gossip_digest_syn.hh"
#include "gms/gossip_digest_ack.hh"
#include "gms/gossip_digest_ack2.hh"
#include "gms/versioned_value.hh"
#include "gms/gossiper.hh"
#include "gms/feature_service.hh"
#include "gms/application_state.hh"
#include "gms/i_endpoint_state_change_subscriber.hh"
#include "message/messaging_service.hh"
#include "utils/log.hh"
#include "db/system_keyspace.hh"
#include <algorithm>
#include <fmt/chrono.h>
#include <fmt/ranges.h>
#include <ranges>
#include <seastar/core/sleep.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/metrics.hh>
#include <seastar/core/on_internal_error.hh>
#include <seastar/util/defer.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/parallel_for_each.hh>
#include <seastar/coroutine/exception.hh>
#include <chrono>
#include "locator/host_id.hh"
#include <boost/range/algorithm/set_algorithm.hpp>
#include <boost/range/algorithm/partition.hpp>
#include <utility>
#include "gms/generation-number.hh"
#include "locator/token_metadata.hh"
#include <seastar/core/shard_id.hh>
#include <seastar/rpc/rpc_types.hh>
#include "utils/assert.hh"
#include "utils/exceptions.hh"
#include "utils/error_injection.hh"
#include "idl/gossip.dist.hh"
#include <csignal>
#include "build_mode.hh"
#include "utils/labels.hh"

namespace gms {

using clk = gossiper::clk;

static logging::logger logger("gossip");

constexpr std::chrono::milliseconds gossiper::INTERVAL;
constexpr std::chrono::hours gossiper::A_VERY_LONG_TIME;
constexpr generation_type::value_type gossiper::MAX_GENERATION_DIFFERENCE;

const sstring& gossiper::get_cluster_name() const noexcept {
    return _gcfg.cluster_name;
}

void gossiper::set_group0_id(utils::UUID group0_id) {
    if (_gcfg.group0_id) {
        on_internal_error(logger, "group0_id is already set");
    }
    _gcfg.group0_id = std::move(group0_id);
}

const utils::UUID& gossiper::get_group0_id() const noexcept {
    return _gcfg.group0_id;
}

const utils::UUID& gossiper::get_recovery_leader() const noexcept {
    return _gcfg.recovery_leader;
}

const std::set<inet_address>& gossiper::get_seeds() const noexcept {
    return _gcfg.seeds;
}

std::chrono::milliseconds gossiper::quarantine_delay() const noexcept {
    auto delay = std::max(unsigned(30000), _gcfg.ring_delay_ms);
    auto ring_delay = std::chrono::milliseconds(delay);
    return ring_delay * 2;
}

gossiper::gossiper(abort_source& as, const locator::shared_token_metadata& stm, netw::messaging_service& ms, gossip_config gcfg, gossip_address_map& address_map)
        : _abort_source(as)
        , _shared_token_metadata(stm)
        , _messaging(ms)
        , _address_map(address_map)
        , _gcfg(std::move(gcfg)) {
    // Gossiper's stuff below runs only on CPU0
    if (this_shard_id() != 0) {
        return;
    }

    _scheduled_gossip_task.set_callback(_gcfg.gossip_scheduling_group, [this] { run(); });
    // half of QUARATINE_DELAY, to ensure _just_removed_endpoints has enough leeway to prevent re-gossip
    fat_client_timeout = quarantine_delay() / 2;
    // Register this instance with JMX
    namespace sm = seastar::metrics;
    auto ep = my_host_id();
    _metrics.add_group("gossip", {
        sm::make_counter("heart_beat",
            [ep, this] {
                auto es = get_endpoint_state_ptr(ep);
                if (es) {
                    return es->get_heart_beat_state().get_heart_beat_version().value();
                } else {
                    return 0;
                }
            }, sm::description("Heartbeat of the current Node."))(basic_level),
        sm::make_gauge("live",
            [this] {
                return _live_endpoints.size();
            }, sm::description("How many live nodes the current node sees"))(basic_level),
        sm::make_gauge("unreachable",
            [this] {
                return _unreachable_endpoints.size();
            }, sm::description("How many unreachable nodes the current node sees"))(basic_level),
    });

    // Add myself to the map on start
    _address_map.add_or_update_entry(_gcfg.host_id, get_broadcast_address());
    _address_map.set_nonexpiring(_gcfg.host_id);
}

/*
 * First construct a map whose key is the endpoint in the GossipDigest and the value is the
 * GossipDigest itself. Then build a list of version differences i.e difference between the
 * version in the GossipDigest and the version in the local state for a given InetAddress.
 * Sort this list. Now loop through the sorted list and retrieve the GossipDigest corresponding
 * to the endpoint from the map that was initially constructed.
*/
void gossiper::do_sort(utils::chunked_vector<gossip_digest>& g_digest_list) const {
    /* Construct a map of endpoint to GossipDigest. */
    std::map<inet_address, gossip_digest> ep_to_digest_map;
    for (auto g_digest : g_digest_list) {
        ep_to_digest_map.emplace(g_digest.get_endpoint(), g_digest);
    }

    /*
     * These digests have their maxVersion set to the difference of the version
     * of the local EndpointState and the version found in the GossipDigest.
    */
    utils::chunked_vector<gossip_digest> diff_digests;
    for (auto g_digest : g_digest_list) {
        auto ep = g_digest.get_endpoint();
        locator::host_id id = try_get_host_id(ep).value_or(locator::host_id{});

        auto ep_state = get_endpoint_state_ptr(id);
        version_type version = ep_state ? get_max_endpoint_state_version(*ep_state) : version_type();
        int32_t diff_version = ::abs((version - g_digest.get_max_version()).value());
        diff_digests.emplace_back(gossip_digest(ep, g_digest.get_generation(), version_type(diff_version)));
    }

    g_digest_list.clear();
    std::sort(diff_digests.begin(), diff_digests.end());
    int size = diff_digests.size();
    /*
     * Report the digests in descending order. This takes care of the endpoints
     * that are far behind w.r.t this local endpoint
    */
    for (int i = size - 1; i >= 0; --i) {
        g_digest_list.emplace_back(ep_to_digest_map[diff_digests[i].get_endpoint()]);
    }
}

// Depends on
// - no external dependency
future<> gossiper::handle_syn_msg(locator::host_id from, gossip_digest_syn syn_msg) {
    logger.trace(
            "handle_syn_msg():from={},cluster_name:peer={},local={},group0_id:peer={},local={},"
            "recovery_leader:peer={},local={},partitioner_name:peer={},local={}",
            from, syn_msg.cluster_id(), get_cluster_name(), syn_msg.group0_id(), get_group0_id(),
            syn_msg.recovery_leader(), get_recovery_leader(), syn_msg.partioner(), get_partitioner_name());
    if (!is_enabled()) {
        co_return;
    }

    /* If the message is from a different cluster throw it away. */
    if (syn_msg.cluster_id() != get_cluster_name()) {
        logger.warn("ClusterName mismatch from {} {}!={}", from, syn_msg.cluster_id(), get_cluster_name());
        co_return;
    }

    // Recovery leader mismatch implies an administrator's mistake during the Raft-based recovery procedure.
    // Throw away the message and signal that something is wrong.
    bool both_nodes_in_recovery = syn_msg.recovery_leader() && get_recovery_leader();
    if (both_nodes_in_recovery && syn_msg.recovery_leader() != get_recovery_leader()) {
        logger.warn("Recovery leader mismatch from {} {} != {},",
                from, syn_msg.recovery_leader(), get_recovery_leader());
        co_return;
    }

    // If the message is from a node with a different group0 id throw it away.
    // A group0 id mismatch is expected during a rolling restart in the Raft-based recovery procedure.
    bool no_recovery = !syn_msg.recovery_leader() && !get_recovery_leader();
    bool group0_ids_mismatch = syn_msg.group0_id() && get_group0_id() && syn_msg.group0_id() != get_group0_id();
    if (no_recovery && group0_ids_mismatch) {
        logger.warn("Group0Id mismatch from {} {} != {}", from, syn_msg.group0_id(), get_group0_id());
        co_return;
    }

    if (syn_msg.partioner() != "" && syn_msg.partioner() != get_partitioner_name()) {
        logger.warn("Partitioner mismatch from {} {}!={}", from, syn_msg.partioner(), get_partitioner_name());
        co_return;
    }

    syn_msg_pending& p = _syn_handlers[from];
    if (p.pending) {
        // The latest syn message from peer has the latest information, so
        // it is safe to drop the previous syn message and keep the latest
        // one only.
        logger.debug("Queue gossip syn msg from node {}, syn_msg={}", from, syn_msg);
        p.syn_msg = std::move(syn_msg);
        co_return;
    }

    // Process the syn message immediately
    logger.debug("Process gossip syn msg from node {}, syn_msg={}", from, syn_msg);
    p.pending = true;
    for (;;) {
        try {
            co_await do_send_ack_msg(from, std::move(syn_msg));
            if (!_syn_handlers.contains(from)) {
                co_return;
            }
            syn_msg_pending& p = _syn_handlers[from];
            if (p.syn_msg) {
                // Process pending gossip syn msg and send ack msg back
                logger.debug("Handle queued gossip syn msg from node {}, syn_msg={}, pending={}",
                        from, p.syn_msg, p.pending);
                syn_msg = std::move(p.syn_msg.value());
                p.syn_msg = {};
                continue;
            } else {
                // No more pending syn msg to process
                p.pending = false;
                logger.debug("No more queued gossip syn msg from node {}, syn_msg={}, pending={}",
                        from, p.syn_msg, p.pending);
                co_return;
            }
        } catch (...) {
            auto ep = std::current_exception();
            if (_syn_handlers.contains(from)) {
                syn_msg_pending& p = _syn_handlers[from];
                p.pending = false;
                p.syn_msg = {};
            }
            logger.warn("Failed to process gossip syn msg from node {}: {}", from, ep);
            throw;
        }
    }
}

future<> gossiper::do_send_ack_msg(locator::host_id from, gossip_digest_syn syn_msg) {
    auto g_digest_list = syn_msg.get_gossip_digests();
    do_sort(g_digest_list);
    utils::chunked_vector<gossip_digest> delta_gossip_digest_list;
    std::map<inet_address, endpoint_state> delta_ep_state_map;
    examine_gossiper(g_digest_list, delta_gossip_digest_list, delta_ep_state_map);
    gms::gossip_digest_ack ack_msg(std::move(delta_gossip_digest_list), std::move(delta_ep_state_map));
    logger.debug("Calling do_send_ack_msg to node {}, syn_msg={}, ack_msg={}", from, syn_msg, ack_msg);
    co_await ser::gossip_rpc_verbs::send_gossip_digest_ack(&_messaging, from, std::move(ack_msg));
}

static bool should_count_as_msg_processing(const std::map<inet_address, endpoint_state>& map) {
    bool count_as_msg_processing  = false;
    for (const auto& x : map) {
        const auto& state = x.second;
        for (const auto& entry : state.get_application_state_map()) {
            const auto& app_state = entry.first;

            auto is_critical_state = [](application_state state) {
                switch (state) {
                case application_state::LOAD:
                case application_state::VIEW_BACKLOG:
                case application_state::CACHE_HITRATES:
                case application_state::GROUP0_STATE_ID:
                    return false;
                default:
                    return true;
                }
            };

            if (is_critical_state(app_state)) {
                count_as_msg_processing = true;
                logger.debug("node={}, app_state={}, count_as_msg_processing={}",
                        x.first, app_state, count_as_msg_processing);
                return count_as_msg_processing;
            }
        }
    }
    return count_as_msg_processing;
}

// Depends on
// - failure_detector
// - on_change callbacks, e.g., storage_service -> access db system_table
// - on_restart callbacks
// - on_join callbacks
// - on_alive
future<> gossiper::handle_ack_msg(locator::host_id id, gossip_digest_ack ack_msg) {
    logger.trace("handle_ack_msg():from={},msg={}", id, ack_msg);

    if (!is_enabled()) {
        co_return;
    }

    auto g_digest_list = ack_msg.get_gossip_digest_list();
    auto& ep_state_map = ack_msg.get_endpoint_state_map();

    bool count_as_msg_processing = should_count_as_msg_processing(ep_state_map);
    if (count_as_msg_processing) {
        _msg_processing++;
    }
    auto mp = defer([count_as_msg_processing, this] {
        if (count_as_msg_processing) {
            _msg_processing--;
        }
    });

    if (ep_state_map.size() > 0) {
        update_timestamp_for_nodes(ep_state_map);
        co_await apply_state_locally(std::move(ep_state_map));
    }

    auto from = id;
    auto ack_msg_digest = std::move(g_digest_list);
    ack_msg_pending& p = _ack_handlers[from];
    if (p.pending) {
        // The latest ack message digests from peer has the latest information, so
        // it is safe to drop the previous ack message digests and keep the latest
        // one only.
        logger.debug("Queue gossip ack msg digests from node {}, ack_msg_digest={}", from, ack_msg_digest);
        p.ack_msg_digest = std::move(ack_msg_digest);
        co_return;
    }

    // Process the ack message immediately
    logger.debug("Process gossip ack msg digests from node {}, ack_msg_digest={}", from, ack_msg_digest);
    p.pending = true;
    for (;;) {
        try {
            co_await do_send_ack2_msg(from, std::move(ack_msg_digest));
            if (!_ack_handlers.contains(from)) {
                co_return;
            }
            ack_msg_pending& p = _ack_handlers[from];
            if (p.ack_msg_digest) {
                // Process pending gossip ack msg digests and send ack2 msg back
                logger.debug("Handle queued gossip ack msg digests from node {}, ack_msg_digest={}, pending={}",
                        from, p.ack_msg_digest, p.pending);
                ack_msg_digest = std::move(p.ack_msg_digest.value());
                p.ack_msg_digest= {};
                continue;
            } else {
                // No more pending ack msg digests to process
                p.pending = false;
                logger.debug("No more queued gossip ack msg digests from node {}, ack_msg_digest={}, pending={}",
                        from, p.ack_msg_digest, p.pending);
                co_return;
            }
        } catch (...) {
            auto ep = std::current_exception();
            if (_ack_handlers.contains(from)) {
                ack_msg_pending& p = _ack_handlers[from];
                p.pending = false;
                p.ack_msg_digest = {};
            }
            logger.warn("Failed to process gossip ack msg digests from node {}: {}", from, ep);
            throw;
        }
    }
}

future<> gossiper::do_send_ack2_msg(locator::host_id from, utils::chunked_vector<gossip_digest> ack_msg_digest) {
    /* Get the state required to send to this gossipee - construct GossipDigestAck2Message */
    std::map<inet_address, endpoint_state> delta_ep_state_map;
    for (auto g_digest : ack_msg_digest) {
        inet_address addr = g_digest.get_endpoint();
        locator::host_id id = try_get_host_id(addr).value_or(locator::host_id{});
        const auto es = get_endpoint_state_ptr(id);
        if (!es || es->get_heart_beat_state().get_generation() < g_digest.get_generation()) {
            continue;
        }
        // Local generation for addr may have been increased since the
        // current node sent an initial SYN. Comparing versions across
        // different generations in get_state_for_version_bigger_than
        // could result in losing some app states with smaller versions.
        const auto version = es->get_heart_beat_state().get_generation() > g_digest.get_generation()
            ? version_type(0)
            : g_digest.get_max_version();
        auto local_ep_state_ptr = get_state_for_version_bigger_than(id, version);
        if (local_ep_state_ptr) {
            delta_ep_state_map.emplace(addr, *local_ep_state_ptr);
        }
    }
    gms::gossip_digest_ack2 ack2_msg(std::move(delta_ep_state_map));
    logger.debug("Calling do_send_ack2_msg to node {}, ack_msg_digest={}, ack2_msg={}", from, ack_msg_digest, ack2_msg);
    co_await ser::gossip_rpc_verbs::send_gossip_digest_ack2(&_messaging, from, std::move(ack2_msg));
    logger.debug("finished do_send_ack2_msg to node {}, ack_msg_digest={}, ack2_msg={}", from, ack_msg_digest, ack2_msg);
}

// Depends on
// - failure_detector
// - on_change callbacks, e.g., storage_service -> access db system_table
// - on_restart callbacks
// - on_join callbacks
// - on_alive callbacks
future<> gossiper::handle_ack2_msg(locator::host_id from, gossip_digest_ack2 msg) {
    logger.trace("handle_ack2_msg():msg={}", msg);
    if (!is_enabled()) {
        co_return;
    }


    auto& remote_ep_state_map = msg.get_endpoint_state_map();
    update_timestamp_for_nodes(remote_ep_state_map);

    bool count_as_msg_processing = should_count_as_msg_processing(remote_ep_state_map);
    if (count_as_msg_processing) {
        _msg_processing++;
    }
    auto mp = defer([count_as_msg_processing, this] {
        if (count_as_msg_processing) {
            _msg_processing--;
        }
    });

    co_await apply_state_locally(std::move(remote_ep_state_map));
}

future<> gossiper::handle_echo_msg(locator::host_id from_hid, seastar::rpc::opt_time_point timeout, std::optional<int64_t> generation_number_opt, bool notify_up) {
    bool respond = true;
    if (!_advertise_to_nodes.empty()) {
        auto it = _advertise_to_nodes.find(from_hid);
        if (it == _advertise_to_nodes.end()) {
            respond = false;
        } else {
            auto es = get_endpoint_state_ptr(from_hid);
            if (es) {
                auto saved_generation_number = it->second;
                auto current_generation_number = generation_number_opt ?
                        generation_type(generation_number_opt.value()) : es->get_heart_beat_state().get_generation();
                respond = saved_generation_number == current_generation_number;
                logger.debug("handle_echo_msg: from={}, saved_generation_number={}, current_generation_number={}",
                        from_hid, saved_generation_number, current_generation_number);
            } else {
                respond = false;
            }
        }
    }
    if (!respond) {
        throw std::runtime_error("Not ready to respond gossip echo message");
    }
    if (notify_up) {
        if (!timeout) {
            on_internal_error(logger, "UP notification should have a timeout");
        }
        auto normal = [] (gossiper& g, locator::host_id hid) {
            const auto& topo = g.get_token_metadata_ptr()->get_topology();
            return topo.has_node(hid) && topo.find_node(hid)->is_normal();
        };
        co_await container().invoke_on(0, [from_hid, timeout, &normal] (gossiper& g) -> future<> {
            try {
                // Wait to see the node as normal. It may node be the case if the node bootstraps
                while (rpc::rpc_clock_type::now() < *timeout && !(normal(g, from_hid) && g.is_alive(from_hid))) {
                    co_await sleep_abortable(std::chrono::milliseconds(100), g._abort_source);
                }
            } catch(...) {
                logger.warn("handle_echo_msg: UP notification from {} failed with {}", from_hid, std::current_exception());
            }
        });
    }
}

future<> gossiper::handle_shutdown_msg(locator::host_id from, std::optional<int64_t> generation_number_opt) {
    if (!is_enabled()) {
        logger.debug("Ignoring shutdown message from {} because gossip is disabled", from);
        co_return;
    }

    auto permit = co_await lock_endpoint(from, null_permit_id);
    if (generation_number_opt) {
        debug_validate_gossip_generation(*generation_number_opt);
        auto es = get_endpoint_state_ptr(from);
        if (es) {
            auto local_generation = es->get_heart_beat_state().get_generation();
            logger.info("Got shutdown message from {}, received_generation={}, local_generation={}",
                    from, generation_number_opt.value(), local_generation);
            if (local_generation.value() != generation_number_opt.value()) {
                logger.warn("Ignoring shutdown message from {} because generation number does not match, received_generation={}, local_generation={}",
                        from, generation_number_opt.value(), local_generation);
                co_return;
            }
        } else {
            logger.warn("Ignoring shutdown message from {} because generation number does not match, received_generation={}, local_generation=not found",
                    from, generation_number_opt.value());
            co_return;
        }
    }
    co_await mark_as_shutdown(from, permit.id());
}

future<gossip_get_endpoint_states_response>
gossiper::handle_get_endpoint_states_msg(gossip_get_endpoint_states_request request) {
    std::unordered_map<gms::inet_address, gms::endpoint_state> map;
    const auto& application_states_wanted = request.application_states;
    for (const auto& [node, state] : _endpoint_state_map) {
        const heart_beat_state& hbs = state->get_heart_beat_state();
        auto it = map.find(state->get_ip());
        if (it != map.end()) {
            if (it->second.get_heart_beat_state().get_generation() < hbs.get_generation()) {
                map.erase(it);
            }
        }
        auto state_wanted = endpoint_state(hbs, state->get_ip());
        auto& apps = state->get_application_state_map();
        for (const auto& app : apps) {
            if (application_states_wanted.count(app.first) > 0) {
                state_wanted.get_application_state_map().emplace(app);
            }
        }
        map.emplace(state->get_ip(), std::move(state_wanted));
    }
    return make_ready_future<gossip_get_endpoint_states_response>(gossip_get_endpoint_states_response{std::move(map)});
}

future<rpc::no_wait_type> gossiper::background_msg(sstring type, noncopyable_function<future<>(gossiper&)> fn) {
    (void)with_gate(_background_msg, [this, type = std::move(type), fn = std::move(fn)] () mutable {
        return container().invoke_on(0, std::move(fn)).handle_exception([type = std::move(type)] (auto ep) {
            logger.warn("Failed to handle {}: {}", type, ep);
        });
    });
    return make_ready_future<rpc::no_wait_type>(netw::messaging_service::no_wait());
}

void gossiper::init_messaging_service_handler() {
    ser::gossip_rpc_verbs::register_gossip_digest_syn(&_messaging, [this] (const rpc::client_info& cinfo, gossip_digest_syn syn_msg) {
        auto from = cinfo.retrieve_auxiliary<locator::host_id>("host_id");
        return background_msg("GOSSIP_DIGEST_SYN", [from, syn_msg = std::move(syn_msg)] (gms::gossiper& gossiper) mutable {
            return gossiper.handle_syn_msg(from, std::move(syn_msg));
        });
    });
     ser::gossip_rpc_verbs::register_gossip_digest_ack(&_messaging, [this] (const rpc::client_info& cinfo, gossip_digest_ack msg) {
        auto from = cinfo.retrieve_auxiliary<locator::host_id>("host_id");
        return background_msg("GOSSIP_DIGEST_ACK", [from, msg = std::move(msg)] (gms::gossiper& gossiper) mutable {
            return gossiper.handle_ack_msg(from, std::move(msg));
        });
    });
    ser::gossip_rpc_verbs::register_gossip_digest_ack2(&_messaging, [this] (const rpc::client_info& cinfo, gossip_digest_ack2 msg) {
        auto from = cinfo.retrieve_auxiliary<locator::host_id>("host_id");
        return background_msg("GOSSIP_DIGEST_ACK2", [from, msg = std::move(msg)] (gms::gossiper& gossiper) mutable {
            return gossiper.handle_ack2_msg(from, std::move(msg));
        });
    });
    ser::gossip_rpc_verbs::register_gossip_echo(&_messaging, [this] (const rpc::client_info& cinfo, seastar::rpc::opt_time_point timeout, rpc::optional<int64_t> generation_number_opt, rpc::optional<bool> notify_up_opt) {
        auto from_hid = cinfo.retrieve_auxiliary<locator::host_id>("host_id");
        return handle_echo_msg(from_hid, timeout, generation_number_opt, notify_up_opt.value_or(false));
    });
    ser::gossip_rpc_verbs::register_gossip_shutdown(&_messaging, [this] (const rpc::client_info& cinfo, inet_address from, rpc::optional<int64_t> generation_number_opt) {
        auto from_hid = cinfo.retrieve_auxiliary<locator::host_id>("host_id");
        return background_msg("GOSSIP_SHUTDOWN", [from_hid, generation_number_opt] (gms::gossiper& gossiper) {
            return gossiper.handle_shutdown_msg(from_hid, generation_number_opt);
        });
    });
    ser::gossip_rpc_verbs::register_gossip_get_endpoint_states(&_messaging, [this] (const rpc::client_info& cinfo,  rpc::opt_time_point, gossip_get_endpoint_states_request request) {
        return container().invoke_on(0, [request = std::move(request)] (gms::gossiper& gossiper) mutable {
            return gossiper.handle_get_endpoint_states_msg(std::move(request));
        });
    });
}

future<> gossiper::uninit_messaging_service_handler() {
    auto& ms = _messaging;
    return ser::gossip_rpc_verbs::unregister(&ms);
}

template<typename T>
future<> gossiper::send_gossip(gossip_digest_syn message, std::set<T> epset) {
    utils::chunked_vector<T> __live_endpoints(epset.begin(), epset.end());
    size_t size = __live_endpoints.size();
    if (size < 1) {
        return make_ready_future<>();
    }
    /* Generate a random number from 0 -> size */
    std::uniform_int_distribution<int> dist(0, size - 1);
    int index = dist(_random_engine);
    std::conditional_t<std::is_same_v<T, gms::inet_address>, netw::msg_addr, T> id{__live_endpoints[index]};
    logger.trace("Sending a GossipDigestSyn to {} ...", id);
    return ser::gossip_rpc_verbs::send_gossip_digest_syn(&_messaging, id, std::move(message)).handle_exception([id] (auto ep) {
        // It is normal to reach here because it is normal that a node
        // tries to send a SYN message to a peer node which is down before
        // failure_detector thinks that peer node is down.
        logger.trace("Fail to send GossipDigestSyn to {}: {}", id, ep);
    });
}


future<> gossiper::do_apply_state_locally(locator::host_id node, endpoint_state remote_state, bool shadow_round) {

    co_await utils::get_local_injector().inject("delay_gossiper_apply", [&node, &remote_state](auto& handler) -> future<> {
        const auto gossip_delay_node = handler.template get<std::string_view>("delay_node");
        if (gossip_delay_node && !remote_state.get_host_id() && inet_address(sstring(gossip_delay_node.value())) == remote_state.get_ip()) {
            logger.debug("delay_gossiper_apply: suspend for node {}", node);
            co_await handler.wait_for_message(std::chrono::steady_clock::now() + std::chrono::minutes{5});
            logger.debug("delay_gossiper_apply: resume for node {}", node);
        }
    });

    // If state does not exist just add it. If it does then add it if the remote generation is greater.
    // If there is a generation tie, attempt to break it by heartbeat version.
    auto permit = co_await lock_endpoint(node, null_permit_id);
    auto es = get_endpoint_state_ptr(node);

    // If remote state update does not contain a host id, check whether the endpoint still
    // exists in the `_endpoint_state_map` since after a preemption point it could have been deleted.
    if (!remote_state.get_host_id() && !es) {
        throw std::runtime_error(format("Entry for host id {} does not exist in the endpoint state map.", node));
    }

    if (es) {
        endpoint_state local_state = *es;
        auto local_generation = local_state.get_heart_beat_state().get_generation();
        auto remote_generation = remote_state.get_heart_beat_state().get_generation();
        logger.trace("{} local generation {}, remote generation {}", node, local_generation, remote_generation);
        if (remote_generation > generation_type(get_generation_number().value() + MAX_GENERATION_DIFFERENCE)) {
            // assume some peer has corrupted memory and is broadcasting an unbelievable generation about another peer (or itself)
            logger.warn("received an invalid gossip generation for peer {}; local generation = {}, received generation = {}",
                node, local_generation, remote_generation);
        } else if (remote_generation > local_generation) {
            logger.trace("Updating heartbeat state generation to {} from {} for {} (notify={})", remote_generation, local_generation, node, !shadow_round);
            // major state change will handle the update by inserting the remote state directly
            co_await handle_major_state_change(std::move(remote_state), permit.id(), shadow_round);
        } else if (remote_generation == local_generation) {
            // find maximum state
            auto local_max_version = get_max_endpoint_state_version(local_state);
            auto remote_max_version = get_max_endpoint_state_version(remote_state);
            if (remote_max_version > local_max_version) {
                // apply states, but do not notify since there is no major change
                co_await apply_new_states(std::move(local_state), remote_state, permit.id(), shadow_round);
            } else {
                logger.debug("Ignoring remote version {} <= {} for {}", remote_max_version, local_max_version, node);
            }
            // Re-rake after apply_new_states
            es = get_endpoint_state_ptr(node);
            if (!is_alive(es->get_host_id()) && !is_dead_state(*es) && !shadow_round) { // unless of course, it was dead
                mark_alive(es);
            }
        } else {
            logger.debug("Ignoring remote generation {} < {}", remote_generation, local_generation);
        }
    } else {
        logger.debug("Applying remote_state for node {} ({} node)", node, !shadow_round ? "old" : "new");
        co_await handle_major_state_change(std::move(remote_state), permit.id(), shadow_round);
    }
}

future<> gossiper::apply_state_locally_in_shadow_round(std::unordered_map<inet_address, endpoint_state> map) {
    for (auto& [node, remote_state] : map) {
        remote_state.set_ip(node);
        auto id = remote_state.get_host_id();
        co_await do_apply_state_locally(id, std::move(remote_state), true);
    }
}

future<> gossiper::apply_state_locally(std::map<inet_address, endpoint_state> map) {
    auto start = std::chrono::steady_clock::now();
    auto endpoints = map | std::views::keys | std::ranges::to<utils::chunked_vector<inet_address>>();
    std::shuffle(endpoints.begin(), endpoints.end(), _random_engine);
    auto node_is_seed = [this] (gms::inet_address ip) { return is_seed(ip); };
    boost::partition(endpoints, node_is_seed);
    logger.debug("apply_state_locally_endpoints={}", endpoints);

    co_await coroutine::parallel_for_each(endpoints, [this, &map] (auto&& ep) -> future<> {
        if (ep == get_broadcast_address()) {
            return make_ready_future<>();
        }
        auto it = map.find(ep);
        it->second.set_ip(ep);
        locator::host_id hid = it->second.get_host_id();
        if (hid == locator::host_id::create_null_id()) {
            // If there is no host id in the new state there should be one locally
            hid = get_host_id(ep);
        }
        if (hid == my_host_id()) {
            logger.trace("Ignoring gossip for {} because it maps to local id, but is not local address", ep);
            return make_ready_future<>();
        }
        if (_topo_sm) {
            if (_topo_sm->_topology.left_nodes.contains(raft::server_id(hid.uuid()))) {
                logger.trace("Ignoring gossip for {} because it left", ep);
                return make_ready_future<>();
            }
        } else {
            if (_just_removed_endpoints.contains(hid)) {
                logger.trace("Ignoring gossip for {} because it is quarantined", hid);
                return make_ready_future<>();
            }
        }
        return seastar::with_semaphore(_apply_state_locally_semaphore, 1, [this, hid, state = std::move(it->second)] () mutable {
            return do_apply_state_locally(hid, std::move(state), false);
        });
    });

    logger.debug("apply_state_locally() took {} ms", std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start).count());
}

future<> gossiper::force_remove_endpoint(locator::host_id id, permit_id pid) {
    return container().invoke_on(0, [this, pid, id] (auto& gossiper) mutable -> future<> {
        auto permit = co_await gossiper.lock_endpoint(id, pid);
        pid = permit.id();
        try {
            if (id == my_host_id()) {
                throw std::runtime_error(format("Can not force remove node {} itself", id));
            }
            if (!gossiper._endpoint_state_map.contains(id)) {
                logger.debug("Force remove node is called on non exiting endpoint {}", id);
                co_return;
            }
            co_await gossiper.remove_endpoint(id, pid);
            co_await gossiper.evict_from_membership(id, pid);
            logger.info("Finished to force remove node {}", id);
        } catch (...) {
            logger.warn("Failed to force remove node {}: {}", id, std::current_exception());
        }
    });
}

future<> gossiper::remove_endpoint(locator::host_id endpoint, permit_id pid) {
    auto permit = co_await lock_endpoint(endpoint, pid);
    pid = permit.id();

    auto state = get_endpoint_state_ptr(endpoint);
    auto ip = state ? state->get_ip() : inet_address{};

    // do subscribers first so anything in the subscriber that depends on gossiper state won't get confused
    try {
        co_await _subscribers.for_each([endpoint, ip, pid] (shared_ptr<i_endpoint_state_change_subscriber> subscriber) {
            return subscriber->on_remove(ip, endpoint, pid);
        });
    } catch (...) {
        logger.warn("Fail to call on_remove callback: {}", std::current_exception());
    }

    if (!state) {
        logger.warn("There is no state for the removed IP {}", endpoint);
        co_return;
    }

    if(_seeds.contains(ip)) {
        build_seeds_list();
        _seeds.erase(ip);
        logger.info("removed {} from _seeds, updated _seeds list = {}", endpoint, _seeds);
    }

    auto host_id = state->get_host_id();

    bool was_alive = false;

    co_await mutate_live_and_unreachable_endpoints([host_id, &was_alive] (live_and_unreachable_endpoints& data) {
        was_alive = data.live.erase(host_id);
        data.unreachable.erase(host_id);
    });
    _syn_handlers.erase(host_id);
    _ack_handlers.erase(host_id);
    quarantine_endpoint(host_id);
    logger.info("Removed endpoint {}", endpoint);

    if (was_alive) {
        try {
            logger.info("InetAddress {}/{} is now DOWN, status = {}", state->get_host_id(), endpoint, get_gossip_status(*state));
            co_await do_on_dead_notifications(ip, std::move(state), pid);
        } catch (...) {
            logger.warn("Fail to call on_dead callback: {}", std::current_exception());
        }
    }
}

future<> gossiper::do_status_check() {
    logger.trace("Performing status check ...");

    auto now = this->now();

    for (const auto& host_id : get_endpoints()) {
        if (host_id == my_host_id()) {
            continue;
        }

        auto permit = co_await lock_endpoint(host_id, null_permit_id);
        const auto& pid = permit.id();

        auto eps = get_endpoint_state_ptr(host_id);
        if (!eps) {
            continue;
        }
        auto& ep_state = *eps;
        if (host_id != ep_state.get_host_id()) {
            on_internal_error(logger, fmt::format("Gossiper entry with id {} has state with id {}", host_id, ep_state.get_host_id()));
        }
        bool is_alive = this->is_alive(host_id);
        auto update_timestamp = ep_state.get_update_timestamp();

        // check if this is a fat client. fat clients are removed automatically from
        // gossip after FatClientTimeout.  Do not remove dead states here.
        if (is_gossip_only_member(host_id)
            && !_just_removed_endpoints.contains(host_id)
            && ((now - update_timestamp) > fat_client_timeout)) {
            logger.info("FatClient {} has been silent for {}ms, removing from gossip", host_id, fat_client_timeout.count());
            co_await remove_endpoint(host_id, pid); // will put it in _just_removed_endpoints to respect quarantine delay
            co_await evict_from_membership(host_id, pid); // can get rid of the state immediately
            continue;
        }

        // check for dead state removal
        auto expire_time = get_expire_time_for_endpoint(host_id);
        if (!is_alive && (now > expire_time)) {
            const auto* node = get_token_metadata_ptr()->get_topology().find_node(host_id);
            if (!host_id || !node || !node->is_member()) {
                logger.debug("time is expiring for endpoint : {} ({})", host_id, expire_time.time_since_epoch().count());
                co_await evict_from_membership(host_id, pid);
            }
        }
    }

    for (auto it = _just_removed_endpoints.begin(); it != _just_removed_endpoints.end();) {
        auto& t= it->second;
        if ((now - t) > quarantine_delay()) {
            logger.info("{} ms elapsed, {} gossip quarantine over", quarantine_delay().count(), it->first);
            it = _just_removed_endpoints.erase(it);
        } else {
            it++;
        }
    }
}

gossiper::endpoint_permit::endpoint_permit(endpoint_locks_map::entry_ptr&& ptr, locator::host_id addr, seastar::compat::source_location caller) noexcept
    : _ptr(std::move(ptr))
    , _permit_id(_ptr->pid)
    , _addr(std::move(addr))
    , _caller(std::move(caller))
{
    ++_ptr->holders;
    if (!_ptr->first_holder) {
        _ptr->first_holder = _caller;
    }
    _ptr->last_holder = _caller;
    logger.debug("{}: lock_endpoint {}: acquired: permit_id={} holders={}", _caller.function_name(), _addr, _permit_id, _ptr->holders);
}

gossiper::endpoint_permit::endpoint_permit(endpoint_permit&& o) noexcept
    : _ptr(std::exchange(o._ptr, nullptr))
    , _permit_id(std::exchange(o._permit_id, null_permit_id))
    , _addr(std::exchange(o._addr, locator::host_id{}))
    , _caller(std::move(o._caller))
{}

gossiper::endpoint_permit::~endpoint_permit() {
    release();
}

bool gossiper::endpoint_permit::release() noexcept {
    if (auto ptr = std::exchange(_ptr, nullptr)) {
        SCYLLA_ASSERT(ptr->pid == _permit_id);
        logger.debug("{}: lock_endpoint {}: released: permit_id={} holders={}", _caller.function_name(), _addr, _permit_id, ptr->holders);
        if (!--ptr->holders) {
            logger.debug("{}: lock_endpoint {}: released: permit_id={}", _caller.function_name(), _addr, _permit_id);
            ptr->units.return_all();
            ptr->pid = null_permit_id;
            ptr->first_holder = ptr->last_holder = std::nullopt;
            _permit_id = null_permit_id;
            return true;
        }
    }
    return false;
}

gossiper::endpoint_lock_entry::endpoint_lock_entry() noexcept
    : sem(1)
    , pid(permit_id::create_null_id())
{}

future<gossiper::endpoint_permit> gossiper::lock_endpoint(locator::host_id ep, permit_id pid, seastar::compat::source_location l) {
    if (this_shard_id() != 0) {
        on_internal_error(logger, "lock_endpoint must be called on shard 0");
    }
    auto eptr = co_await _endpoint_locks.get_or_load(ep, [] (const locator::host_id& ep) { return endpoint_lock_entry(); });
    if (pid) {
        if (eptr->pid == pid) {
            // Already locked with the same permit
            co_return endpoint_permit(std::move(eptr), std::move(ep), std::move(l));
        } else {
            // permit_id mismatch means either that the endpoint lock was released,
            // or maybe we're passed a permit_id that was acquired for a different endpoint.
            on_internal_error_noexcept(logger, fmt::format("{}: lock_endpoint {}: permit_id={}: endpoint_lock_entry has mismatching permit_id={}", l.function_name(), ep, pid, eptr->pid));
        }
    }
    pid = permit_id::create_random_id();
    logger.debug("{}: lock_endpoint {}: waiting: permit_id={}", l.function_name(), ep, pid);
    while (true) {
        _abort_source.check();
        static constexpr auto duration = std::chrono::minutes{1};
        abort_on_expiry aoe(lowres_clock::now() + duration);
        auto sub = _abort_source.subscribe([&aoe] () noexcept {
            aoe.abort_source().request_abort();
        });
        SCYLLA_ASSERT(sub); // due to check() above
        try {
            eptr->units = co_await get_units(eptr->sem, 1, aoe.abort_source());
            break;
        } catch (const abort_requested_exception&) {
            if (_abort_source.abort_requested()) {
                throw;
            }

            // If we didn't rethrow above, the abort had to come from `abort_on_expiry`'s timer.

            static constexpr auto fmt_loc = [] (const seastar::compat::source_location& l) {
                return fmt::format("{}({}:{}) `{}`", l.file_name(), l.line(), l.column(), l.function_name());
            };
            static constexpr auto fmt_loc_opt = [] (const std::optional<seastar::compat::source_location>& l) {
                if (!l) {
                    return "null"s;
                }
                return fmt_loc(*l);
            };
            logger.error(
                "{}: waiting for endpoint lock (ep={}) took more than {}, signifying possible deadlock;"
                " holders: {}, first holder: {}, last holder (might not be current): {}",
                fmt_loc(l), ep, duration, eptr->holders, fmt_loc_opt(eptr->first_holder), fmt_loc_opt(eptr->last_holder));
        }
    }
    eptr->pid = pid;
    if (eptr->holders) {
        on_internal_error_noexcept(logger, fmt::format("{}: lock_endpoint {}: newly held endpoint_lock_entry has {} holders", l.function_name(), ep, eptr->holders));
    }
    _abort_source.check();
    co_return endpoint_permit(std::move(eptr), std::move(ep), std::move(l));
}

void gossiper::permit_internal_error(const locator::host_id& addr, permit_id pid) {
    on_internal_error(logger, fmt::format("Must be called under lock_endpoint for node {}", addr));
}

future<semaphore_units<>> gossiper::lock_endpoint_update_semaphore() {
    if (this_shard_id() != 0) {
        on_internal_error(logger, "must be called on shard 0");
    }
    return get_units(_endpoint_update_semaphore, 1, _abort_source);
}

future<> gossiper::mutate_live_and_unreachable_endpoints(std::function<void(live_and_unreachable_endpoints&)> func) {
    auto lock = co_await lock_endpoint_update_semaphore();
    auto cloned = std::make_unique<live_and_unreachable_endpoints>(_live_endpoints, _unreachable_endpoints);
    func(*cloned);

    // Bump the _live_endpoints_version unconditionally,
    // even if only _unreachable_endpoints changed.
    // It will trigger a new round in the failure_detector_loop
    // but that's not too bad as changing _unreachable_endpoints
    // is rare enough.
    co_await replicate_live_endpoints_on_change(make_foreign(std::move(cloned)), _live_endpoints_version + 1);
}

future<std::set<inet_address>> gossiper::get_live_members_synchronized() {
    return container().invoke_on(0, [] (gms::gossiper& g) -> future<std::set<inet_address>> {
        // Make sure the value we return is synchronized on all shards
        auto lock = co_await g.lock_endpoint_update_semaphore();
        co_return g.get_live_members() | std::views::transform([&g] (auto id) { return g._address_map.get(id); }) | std::ranges::to<std::set>();
    });
}

future<std::set<inet_address>> gossiper::get_unreachable_members_synchronized() {
    return container().invoke_on(0, [] (gms::gossiper& g) -> future<std::set<inet_address>> {
        // Make sure the value we return is synchronized on all shards
        auto lock = co_await g.lock_endpoint_update_semaphore();
        co_return g.get_unreachable_members() | std::views::transform([&g] (auto id) { return g._address_map.get(id); }) | std::ranges::to<std::set>();
    });
}

future<> gossiper::send_echo(locator::host_id host_id, std::chrono::milliseconds timeout_ms, int64_t generation_number, bool notify_up) {
    return ser::gossip_rpc_verbs::send_gossip_echo(&_messaging, host_id, netw::messaging_service::clock_type::now() + timeout_ms, _abort_source, generation_number, notify_up);
}

future<> gossiper::failure_detector_loop_for_node(locator::host_id host_id, generation_type gossip_generation, uint64_t live_endpoints_version) {
    auto last = gossiper::clk::now();
    auto diff = gossiper::clk::duration(0);
    auto echo_interval = std::chrono::seconds(2);
    auto max_duration = echo_interval + std::chrono::milliseconds(_gcfg.failure_detector_timeout_ms());
    auto node = _address_map.get(host_id);
    while (is_enabled() && !_abort_source.abort_requested()) {
        bool failed = false;
        try {
            logger.debug("failure_detector_loop: Send echo to node {}/{}, status = started", host_id, node);
            co_await send_echo(host_id, max_duration, gossip_generation.value(), false);
            logger.debug("failure_detector_loop: Send echo to node {}/{}, status = ok", host_id, node);
        } catch (...) {
            failed = true;
            logger.warn("failure_detector_loop: Send echo to node {}/{}, status = failed: {}", host_id, node, std::current_exception());
        }
        auto now = gossiper::clk::now();
        diff = now - last;
        if (!failed) {
            last = now;
        }
        if (diff > max_duration) {
            logger.info("failure_detector_loop: Mark node {}/{} as DOWN", host_id, node);
            co_await container().invoke_on(0, [host_id] (gms::gossiper& g) {
                return g.convict(host_id);
            });
            co_return;
        }

        // When live_endpoints changes, live_endpoints_version changes. When
        // live_endpoints changes, it is the time to re-distribute live nodes
        // to different shards. We return from the per node loop here. The
        // failure_detector_loop main loop will restart the per node loop.
        if (_live_endpoints_version != live_endpoints_version) {
            logger.debug("failure_detector_loop: Finished loop for node {}/{}, live_endpoints={}, current_live_endpoints_version={}, live_endpoints_version={}",
                    host_id, node, _live_endpoints, _live_endpoints_version, live_endpoints_version);
            co_return;
        } else  {
            co_await sleep_abortable(echo_interval, _abort_source).handle_exception_type([] (const abort_requested_exception&) {});
        }
    }
    co_return;
}

future<> gossiper::failure_detector_loop() {
    auto shard = this_shard_id();
    if (shard != 0) {
        co_return;
    }
    logger.info("failure_detector_loop: Started main loop");
    while (is_enabled()) {
        try {
            if (_live_endpoints.empty()) {
                logger.debug("failure_detector_loop: Wait until live_nodes={} is not empty", _live_endpoints);
                co_await sleep_abortable(std::chrono::seconds(1), _abort_source);
                continue;
            }
            auto nodes = _live_endpoints | std::ranges::to<std::vector>();
            auto live_endpoints_version = _live_endpoints_version;
            auto generation_number = my_endpoint_state().get_heart_beat_state().get_generation();
            co_await coroutine::parallel_for_each(std::views::iota(0u, nodes.size()), [this, generation_number, live_endpoints_version, &nodes] (size_t idx) {
                const auto& node = nodes[idx];
                auto shard = idx % smp::count;
                logger.debug("failure_detector_loop: Started new round for node={} on shard={}, live_nodes={}, live_endpoints_version={}",
                        node, shard, nodes, live_endpoints_version);
                return container().invoke_on(shard, [node, generation_number, live_endpoints_version] (gms::gossiper& g) {
                    return g.failure_detector_loop_for_node(node, generation_number, live_endpoints_version);
                });
            });
        } catch (...) {
            logger.warn("failure_detector_loop: Got error in the loop, live_nodes={}: {}",
                    _live_endpoints, std::current_exception());
        }
    }
    logger.info("failure_detector_loop: Finished main loop");
}

// This needs to be run with a lock
future<> gossiper::replicate_live_endpoints_on_change(foreign_ptr<std::unique_ptr<live_and_unreachable_endpoints>> data0, uint64_t new_version) {
    auto coordinator = this_shard_id();
    SCYLLA_ASSERT(coordinator == 0);
    //
    // Gossiper task runs only on CPU0:
    //
    //    - replicate _live_endpoints and _unreachable_endpoints
    //      across all other shards.
    //    - use _live_endpoints_version on each shard
    //      to determine if it has the latest copy, and replicate the respective
    //      member from shard 0, if the shard is outdated.
    //
    logger.debug("replicating live and unreachable endpoints to other shards");

    std::vector<foreign_ptr<std::unique_ptr<live_and_unreachable_endpoints>>> per_shard_data;
    per_shard_data.resize(smp::count);
    per_shard_data[coordinator] = std::move(data0);

    // Prepare copies on each other shard
    co_await coroutine::parallel_for_each(std::views::iota(0u, smp::count), [&per_shard_data, coordinator] (auto shard) -> future<> {
        if (shard != this_shard_id()) {
            const auto& src = *per_shard_data[coordinator];
            per_shard_data[shard] = co_await smp::submit_to(shard, [&] {
                return make_foreign(std::make_unique<live_and_unreachable_endpoints>(src));
            });
        }
    });

    // Apply copies on each other shard
    co_await container().invoke_on_all([&] (gossiper& local_gossiper) noexcept {
        if (local_gossiper._live_endpoints_version >= new_version) {
            on_fatal_internal_error(logger, fmt::format("shard already has unexpected live_endpoints_version {} > {}",
                    local_gossiper._live_endpoints_version, new_version));
        }

        auto data = per_shard_data[this_shard_id()].release();
        local_gossiper._live_endpoints = std::move(data->live);
        local_gossiper._unreachable_endpoints = std::move(data->unreachable);
        local_gossiper._live_endpoints_version = new_version;
    });
}

// Depends on:
// - failure_detector
// - on_remove callbacks, e.g, storage_service -> access token_metadata
void gossiper::run() {
   // Run it in the background.
  (void)seastar::with_semaphore(_callback_running, 1, [this] {
    return seastar::async([this, g = shared_from_this()] {
            logger.trace("=== Gossip round START");

            //wait on messaging service to start listening
            // MessagingService.instance().waitUntilListening();

            {
                auto permit = lock_endpoint(my_host_id(), null_permit_id).get();
                /* Update the local heartbeat counter. */
                heart_beat_state& hbs = my_endpoint_state().get_heart_beat_state();
                hbs.update_heart_beat();

                logger.trace("My heartbeat is now {}", hbs.get_heart_beat_version());
            }

            utils::chunked_vector<gossip_digest> g_digests = make_random_gossip_digest();

            if (g_digests.size() > 0) {
                gossip_digest_syn message(
                        get_cluster_name(), get_partitioner_name(), g_digests, get_group0_id(), get_recovery_leader());

                if (_endpoints_to_talk_with.empty() && !_live_endpoints.empty()) {
                    auto live_endpoints = _live_endpoints | std::ranges::to<std::vector>();
                    std::shuffle(live_endpoints.begin(), live_endpoints.end(), _random_engine);
                    // This guarantees the local node will talk with all nodes
                    // in live_endpoints at least once within nr_rounds gossip rounds.
                    // Other gossip implementation like SWIM uses similar approach.
                    // https://www.cs.cornell.edu/projects/Quicksilver/public_pdfs/SWIM.pdf
                    constexpr size_t nr_rounds = 10;
                    size_t nodes_per_round = (live_endpoints.size() + nr_rounds - 1) / nr_rounds;
                    _endpoints_to_talk_with = live_endpoints | std::views::chunk(nodes_per_round) | std::ranges::to<std::list<std::vector<locator::host_id>>>();
                    logger.debug("Set live nodes to talk: endpoint_state_map={}, all_live_nodes={}, endpoints_to_talk_with={}",
                            _endpoint_state_map.size(), live_endpoints, _endpoints_to_talk_with);
                }

                if (!_endpoints_to_talk_with.empty()) {
                    auto live_nodes = std::move(_endpoints_to_talk_with.front());
                    _endpoints_to_talk_with.pop_front();
                    logger.debug("Talk to live nodes: {}", live_nodes);
                    for (auto& ep: live_nodes) {
                        (void)with_gate(_background_msg, [this, message, ep] () mutable {
                            return do_gossip_to_live_member(message, ep).handle_exception([] (auto ep) {
                                logger.trace("Failed to send gossip to live members: {}", ep);
                            });
                        });
                    }
                } else if (!_seeds.empty()) {
                    logger.debug("No live nodes yet: try initial contact point nodes={}", _seeds);
                    for (auto& ep: _seeds) {
                        (void)with_gate(_background_msg, [this, message, ep] () mutable {
                            return do_gossip_to_live_member(message, ep).handle_exception([] (auto ep) {
                                logger.trace("Failed to send gossip to live members: {}", ep);
                            });
                        });
                    }
                } else {
                    logger.debug("No one to talk with");
                }

                /* Gossip to some unreachable member with some probability to check if he is back up */
                (void)with_gate(_background_msg, [this, message = std::move(message)] () mutable {
                    return do_gossip_to_unreachable_member(std::move(message)).handle_exception([] (auto ep) {
                        logger.trace("Failed to send gossip to unreachable members: {}", ep);
                    });
                });
                if (!_topo_sm) {
                    do_status_check().get();
                }
            }
    }).then_wrapped([this] (auto&& f) {
        try {
            f.get();
            _nr_run++;
            logger.trace("=== Gossip round OK");
        } catch (...) {
            logger.warn("=== Gossip round FAIL: {}", std::current_exception());
        }

        if (logger.is_enabled(logging::log_level::trace)) {
            for (auto& x : _endpoint_state_map) {
                logger.trace("ep={}, eps={}", x.first, *x.second);
            }
        }
        if (is_enabled()) {
            _scheduled_gossip_task.arm(INTERVAL);
        } else {
            logger.info("Gossip loop is not scheduled because it is disabled");
        }
    });
  });
}

bool gossiper::is_seed(const gms::inet_address& endpoint) const {
    return _seeds.contains(endpoint);
}

void gossiper::register_(shared_ptr<i_endpoint_state_change_subscriber> subscriber) {
    _subscribers.add(subscriber);
}

future<> gossiper::unregister_(shared_ptr<i_endpoint_state_change_subscriber> subscriber) {
    return _subscribers.remove(subscriber);
}

std::set<locator::host_id> gossiper::get_live_members() const {
    std::set<locator::host_id> live_members(_live_endpoints.begin(), _live_endpoints.end());
    auto myip = get_broadcast_address();
    logger.debug("live_members before={}", live_members);
    if (!is_shutdown(myip)) {
        live_members.insert(my_host_id());
    }
    logger.debug("live_members after={}", live_members);
    return live_members;
}

std::set<locator::host_id> gossiper::get_live_token_owners() const {
    std::set<locator::host_id> token_owners;
    auto normal_token_owners = get_token_metadata_ptr()->get_normal_token_owners();
    for (auto& node: normal_token_owners) {
        if (is_alive(node)) {
            token_owners.insert(node);
        }
    }
    return token_owners;
}

std::set<locator::host_id> gossiper::get_unreachable_nodes() const {
    std::set<locator::host_id> unreachable_nodes;
    auto nodes = get_token_metadata_ptr()->get_topology().get_all_host_ids();
    for (auto& node: nodes) {
        if (!is_alive(node)) {
            unreachable_nodes.insert(node);
        }
    }
    return unreachable_nodes;
}

// Return downtime in microseconds
int64_t gossiper::get_endpoint_downtime(locator::host_id ep) const noexcept {
    auto it = _unreachable_endpoints.find(ep);
    if (it != _unreachable_endpoints.end()) {
        auto& downtime = it->second;
        return std::chrono::duration_cast<std::chrono::microseconds>(now() - downtime).count();
    } else {
        return 0L;
    }
}

// Depends on
// - on_dead callbacks
// It is called from failure_detector
future<> gossiper::convict(locator::host_id endpoint) {
    auto permit = co_await lock_endpoint(endpoint, null_permit_id);
    auto state = get_endpoint_state_ptr(endpoint);
    if (!state || !is_alive(state->get_host_id())) {
        co_return;
    }
    if (is_shutdown(endpoint)) {
        co_await mark_as_shutdown(endpoint, permit.id());
    } else {
        co_await mark_dead(endpoint, state, permit.id());
    }
}

std::set<locator::host_id> gossiper::get_unreachable_members() const {
    return _unreachable_endpoints | std::views::keys | std::ranges::to<std::set>();
}

version_type gossiper::get_max_endpoint_state_version(const endpoint_state& state) const noexcept {
    auto max_version = state.get_heart_beat_state().get_heart_beat_version();
    for (auto& entry : state.get_application_state_map()) {
        auto& value = entry.second;
        max_version = std::max(max_version, value.version());
    }
    return max_version;
}

future<> gossiper::evict_from_membership(locator::host_id hid, permit_id pid) {
    verify_permit(hid, pid);

    co_await mutate_live_and_unreachable_endpoints([hid] (live_and_unreachable_endpoints& data) {
        data.unreachable.erase(hid);
        data.live.erase(hid);
    });

    co_await container().invoke_on_all([hid] (auto& g) {
        if (this_shard_id() == 0) {
            g._address_map.set_expiring(hid);
        }
        g._endpoint_state_map.erase(hid);
    });
    _expire_time_endpoint_map.erase(hid);
    quarantine_endpoint(hid);
    logger.debug("evicting {} from gossip", hid);
}

void gossiper::quarantine_endpoint(locator::host_id id) {
    quarantine_endpoint(id, now());
}

void gossiper::quarantine_endpoint(locator::host_id id, clk::time_point quarantine_start) {
    if (!_topo_sm) {
        // In raft topology mode the coodinator maintains banned nodes list
        _just_removed_endpoints[id] = quarantine_start;
    }
}

utils::chunked_vector<gossip_digest> gossiper::make_random_gossip_digest() const {
    std::unordered_map<inet_address, gossip_digest> g_digests;
    generation_type generation;
    version_type max_version;

    // local epstate will be part of _endpoint_state_map
    utils::chunked_vector<locator::host_id> endpoints;
    for (auto&& x : _endpoint_state_map) {
        endpoints.push_back(x.first);
    }
    std::shuffle(endpoints.begin(), endpoints.end(), _random_engine);
    for (auto& endpoint : endpoints) {
        auto es = get_endpoint_state_ptr(endpoint);
        if (es) {
            auto& eps = *es;
            generation = eps.get_heart_beat_state().get_generation();
            max_version = get_max_endpoint_state_version(eps);
        }
        gossip_digest d{es->get_ip(), generation, max_version};
        auto [it, inserted] = g_digests.emplace(es->get_ip(), d);
        if (!inserted && it->second.get_generation() < generation) {
            // If there are multiple hosts with the same IP send out the one with newest generation
            it->second = d;
        }
    }
    return g_digests | std::views::values | std::ranges::to<utils::chunked_vector<gossip_digest>>();
}

future<> gossiper::replicate(endpoint_state es, permit_id pid) {
    if (!es.get_host_id()) {
        // TODO (#25818): re-introduce the on_internal_error() call once all the code paths leading to this are fixed
        logger.warn("attempting to add a state with empty host id for ip: {}", es.get_ip());
        co_return;
    }

    verify_permit(es.get_host_id(), pid);

    // First pass: replicate the new endpoint_state on all shards.
    // Use foreign_ptr<std::unique_ptr> to ensure destroy on remote shards on exception
    std::vector<foreign_ptr<endpoint_state_ptr>> ep_states;
    ep_states.resize(smp::count);
    auto p = make_foreign(make_endpoint_state_ptr(std::move(es)));
    const auto *eps = p.get();
    ep_states[this_shard_id()] = std::move(p);
    co_await coroutine::parallel_for_each(std::views::iota(0u, smp::count), [&, orig = this_shard_id()] (auto shard) -> future<> {
        if (shard != orig) {
            ep_states[shard] = co_await smp::submit_to(shard, [eps] {
                return make_foreign(make_endpoint_state_ptr(*eps));
            });
        }
     });

    co_await utils::get_local_injector().inject("gossiper_replicate_sleep", std::chrono::seconds{1});

    // Second pass: set replicated endpoint_state on all shards
    // Must not throw
    try {
        co_return co_await container().invoke_on_all([&] (gossiper& g) {
            auto eps = ep_states[this_shard_id()].release();
            auto hid = eps->get_host_id();
            if (this_shard_id() == 0) {
                g._address_map.add_or_update_entry(hid, eps->get_ip(), eps->get_heart_beat_state().get_generation());
                g._address_map.set_nonexpiring(hid);
            }
            g._endpoint_state_map[hid] = std::move(eps);
        });
    } catch (...) {
        on_fatal_internal_error(logger, fmt::format("Failed to replicate endpoint_state: {}", std::current_exception()));
    }
}

future<> gossiper::advertise_token_removed(locator::host_id host_id, permit_id pid) {
    auto permit = co_await lock_endpoint(host_id, pid);
    pid = permit.id();
    auto eps = get_endpoint_state(host_id);
    eps.update_timestamp(); // make sure we don't evict it too soon
    eps.get_heart_beat_state().force_newer_generation_unsafe();
    auto expire_time = compute_expire_time();
    eps.add_application_state(application_state::STATUS, versioned_value::removed_nonlocal(host_id, expire_time.time_since_epoch().count()));
    logger.info("Completing removal of {}", host_id);
    add_expire_time_for_endpoint(host_id, expire_time);
    co_await replicate(std::move(eps), pid);
    // ensure at least one gossip round occurs before returning
    co_await sleep_abortable(INTERVAL * 2, _abort_source);
}

future<> gossiper::assassinate_endpoint(sstring address) {
    if (_topo_sm) {
        throw std::runtime_error("Assassinating endpoint is not supported in topology over raft mode");
    }
    co_await container().invoke_on(0, [&] (auto&& gossiper) -> future<> {
        auto endpoint_opt = gossiper.try_get_host_id(inet_address(address));
        if (!endpoint_opt) {
            logger.warn("There is no endpoint {} to assassinate", address);
            throw std::runtime_error(format("There is no endpoint {} to assassinate", address));
        }
        auto endpoint = *endpoint_opt;
        auto permit = co_await gossiper.lock_endpoint(endpoint, null_permit_id);
        auto es = gossiper.get_endpoint_state_ptr(endpoint);
        auto now = gossiper.now();
        generation_type gen(std::chrono::duration_cast<std::chrono::seconds>((now + std::chrono::seconds(60)).time_since_epoch()).count());
        version_type ver(9999);
        if (!es) {
            logger.warn("There is no endpoint {} to assassinate", endpoint);
            throw std::runtime_error(format("There is no endpoint {} to assassinate", endpoint));
        }
        endpoint_state ep_state = *es;
        std::vector<dht::token> tokens;
        logger.warn("Assassinating {} via gossip", endpoint);

        tokens = gossiper.get_token_metadata_ptr()->get_tokens(endpoint);
        if (tokens.empty()) {
            logger.warn("Unable to calculate tokens for {}.  Will use a random one", address);
            throw std::runtime_error(format("Unable to calculate tokens for {}", endpoint));
        }

        auto generation = ep_state.get_heart_beat_state().get_generation();
        auto heartbeat = ep_state.get_heart_beat_state().get_heart_beat_version();
        auto ring_delay = std::chrono::milliseconds(gossiper._gcfg.ring_delay_ms);
        logger.info("Sleeping for {} ms to ensure {} does not change", ring_delay.count(), endpoint);
        // make sure it did not change
        co_await sleep_abortable(ring_delay, gossiper._abort_source);

        es = gossiper.get_endpoint_state_ptr(endpoint);
        if (!es) {
            logger.warn("Endpoint {} disappeared while trying to assassinate, continuing anyway", endpoint);
        } else {
            auto& new_state = *es;
            if (new_state.get_heart_beat_state().get_generation() != generation) {
                throw std::runtime_error(format("Endpoint still alive: {} generation changed while trying to assassinate it", endpoint));
            } else if (new_state.get_heart_beat_state().get_heart_beat_version() != heartbeat) {
                throw std::runtime_error(format("Endpoint still alive: {} heartbeat changed while trying to assassinate it", endpoint));
            }
        }
        ep_state.update_timestamp(); // make sure we don't evict it too soon
        ep_state.get_heart_beat_state().force_newer_generation_unsafe();

        // do not pass go, do not collect 200 dollars, just gtfo
        std::unordered_set<dht::token> tokens_set(tokens.begin(), tokens.end());
        auto expire_time = gossiper.compute_expire_time();
        ep_state.add_application_state(application_state::STATUS, versioned_value::left(tokens_set, expire_time.time_since_epoch().count()));
        co_await gossiper.handle_major_state_change(std::move(ep_state), permit.id(), true);
        co_await sleep_abortable(INTERVAL * 4, gossiper._abort_source);
        logger.warn("Finished assassinating {}", endpoint);
    });
}

future<generation_type> gossiper::get_current_generation_number(locator::host_id endpoint) const {
    // FIXME: const container() has no const invoke_on variant
    return const_cast<gossiper*>(this)->container().invoke_on(0, [endpoint] (const gossiper& gossiper) {
        return gossiper.get_endpoint_state(endpoint).get_heart_beat_state().get_generation();
    });
}

future<version_type> gossiper::get_current_heart_beat_version(locator::host_id endpoint) const {
    // FIXME: const container() has no const invoke_on variant
    return const_cast<gossiper*>(this)->container().invoke_on(0, [endpoint] (const gossiper& gossiper) {
        return gossiper.get_endpoint_state(endpoint).get_heart_beat_state().get_heart_beat_version();
    });
}

template<typename T>
future<> gossiper::do_gossip_to_live_member(gossip_digest_syn message, T ep) {
    return send_gossip<T>(message, {ep});
}

future<> gossiper::do_gossip_to_unreachable_member(gossip_digest_syn message) {
    double live_endpoint_count = _live_endpoints.size();
    double unreachable_endpoint_count = _unreachable_endpoints.size();
    if (unreachable_endpoint_count > 0) {
        /* based on some probability */
        double prob = unreachable_endpoint_count / (live_endpoint_count + 1);
        std::uniform_real_distribution<double> dist(0, 1);
        double rand_dbl = dist(_random_engine);
        if (rand_dbl < prob) {
            std::set<locator::host_id> addrs;
            for (auto&& x : _unreachable_endpoints) {
                // Ignore the node which is decommissioned
                if (get_gossip_status(_address_map.get(x.first)) != sstring(versioned_value::STATUS_LEFT)) {
                    addrs.insert(x.first);
                }
            }
            logger.trace("do_gossip_to_unreachable_member: live_endpoint nr={} unreachable_endpoints nr={}",
                live_endpoint_count, unreachable_endpoint_count);
            return send_gossip(message, addrs);
        }
    }
    return make_ready_future<>();
}

bool gossiper::is_gossip_only_member(locator::host_id host_id) const {
    auto es = get_endpoint_state_ptr(host_id);
    if (!es) {
        return false;
    }
    const auto* node = get_token_metadata_ptr()->get_topology().find_node(host_id);
    return !is_dead_state(*es) && (!node || !node->is_member());
}

clk::time_point gossiper::get_expire_time_for_endpoint(locator::host_id id) const noexcept {
    /* default expire_time is A_VERY_LONG_TIME */
    auto it = _expire_time_endpoint_map.find(id);
    if (it == _expire_time_endpoint_map.end()) {
        return compute_expire_time();
    } else {
        auto stored_time = it->second;
        return stored_time;
    }
}

endpoint_state_ptr gossiper::get_endpoint_state_ptr(locator::host_id ep) const noexcept {
    auto it = _endpoint_state_map.find(ep);
    if (it == _endpoint_state_map.end()) {
        return nullptr;
    } else {
        return it->second;
    }
}

void gossiper::update_timestamp(const endpoint_state_ptr& eps) noexcept {
    const_cast<endpoint_state&>(*eps).update_timestamp();
}

const endpoint_state& gossiper::get_endpoint_state(locator::host_id ep) const {
    auto it = _endpoint_state_map.find(ep);
    if (it == _endpoint_state_map.end()) {
        throw std::out_of_range(format("ep={}", ep));
    }
    return *it->second;
}

endpoint_state& gossiper::my_endpoint_state() {
    auto id = my_host_id();
    auto ep = get_broadcast_address();
    auto it = _endpoint_state_map.find(id);
    if (it == _endpoint_state_map.end()) {
        it = _endpoint_state_map.emplace(id, make_endpoint_state_ptr({ep})).first;
    }
    return const_cast<endpoint_state&>(*it->second);
}

future<> gossiper::reset_endpoint_state_map() {
    logger.debug("Resetting endpoint state map");
    auto lock = co_await lock_endpoint_update_semaphore();
    auto version = _live_endpoints_version + 1;
    co_await container().invoke_on_all([version] (gossiper& g) {
        if (this_shard_id() == 0) {
            for (auto&& [_, es_ptr] : g._endpoint_state_map) {
                g._address_map.set_expiring(es_ptr->get_host_id());
            }
        }
        g._unreachable_endpoints.clear();
        g._live_endpoints.clear();
        g._live_endpoints_version = version;
        g._endpoint_state_map.clear();
    });
}

std::vector<locator::host_id> gossiper::get_endpoints() const {
    return _endpoint_state_map | std::views::keys | std::ranges::to<std::vector>();
}

stop_iteration gossiper::for_each_endpoint_state_until(std::function<stop_iteration(const endpoint_state&)> func) const {
    for (const auto& [node, eps] : _endpoint_state_map) {
        if (func(*eps) == stop_iteration::yes) {
            return stop_iteration::yes;
        }
    }
    return stop_iteration::no;
}

bool gossiper::is_cql_ready(const locator::host_id& endpoint) const {
    // Note:
    // - New scylla node always send application_state::RPC_READY = false when
    // the node boots and send application_state::RPC_READY = true when cql
    // server is up
    // - Old scylla node that does not support the application_state::RPC_READY
    // never has application_state::RPC_READY in the endpoint_state, we can
    // only think their cql server is up, so we return true here if
    // application_state::RPC_READY is not present
    auto eps = get_endpoint_state_ptr(endpoint);
    if (!eps) {
        logger.debug("Node {} does not have RPC_READY application_state, return is_cql_ready=true", endpoint);
        return true;
    }
    auto ready = eps->is_cql_ready();
    logger.debug("Node {}: is_cql_ready={}",  endpoint, ready);
    return ready;
}

locator::host_id gossiper::get_host_id(inet_address endpoint) const {
    auto ids = _endpoint_state_map | std::views::values | std::views::filter([endpoint] (const auto& es) { return es->get_ip() == endpoint; });

    if (std::ranges::distance(ids) == 0) {
        throw std::runtime_error(format("Could not get host_id for endpoint {}: endpoint state not found", endpoint));
    }

    // Find an entry with largest generation
    const auto& es = std::ranges::max(ids, [](const auto& ep1, const auto& ep2) { return ep1->get_heart_beat_state().get_generation() < ep2->get_heart_beat_state().get_generation(); });

    auto host_id = es->get_host_id();
    if (!host_id) {
        throw std::runtime_error(format("Host {} does not have HOST_ID application_state", endpoint));
    }
    return host_id;
}

std::optional<locator::host_id> gossiper::try_get_host_id(inet_address endpoint) const {
    std::optional<locator::host_id> host_id;
    try {
        host_id = get_host_id(endpoint);
    } catch (std::runtime_error&) {}
    return host_id;
}


std::optional<gms::inet_address> gossiper::get_node_ip(locator::host_id host_id) const {
    if (auto it = _endpoint_state_map.find(host_id); it != _endpoint_state_map.end()) {
        return {it->second->get_ip()};
    } else {
        return {};
    }
}

std::optional<endpoint_state> gossiper::get_state_for_version_bigger_than(locator::host_id for_endpoint, version_type version) const {
    std::optional<endpoint_state> reqd_endpoint_state;
    auto es = get_endpoint_state_ptr(for_endpoint);
    if (es) {
        auto& eps = *es;
        /*
             * Here we try to include the Heart Beat state only if it is
             * greater than the version passed in. It might happen that
             * the heart beat version maybe lesser than the version passed
             * in and some application state has a version that is greater
             * than the version passed in. In this case we also send the old
             * heart beat and throw it away on the receiver if it is redundant.
            */
        auto local_hb_version = eps.get_heart_beat_state().get_heart_beat_version();
        if (local_hb_version > version) {
            reqd_endpoint_state.emplace(eps.get_heart_beat_state(), eps.get_ip());
            logger.trace("local heartbeat version {} greater than {} for {}", local_hb_version, version, for_endpoint);
        }
        /* Accumulate all application states whose versions are greater than "version" variable */
        for (auto& entry : eps.get_application_state_map()) {
            auto& value = entry.second;
            if (value.version() > version) {
                if (!reqd_endpoint_state) {
                    reqd_endpoint_state.emplace(eps.get_heart_beat_state(), eps.get_ip());
                }
                auto& key = entry.first;
                logger.trace("Adding state of {}, {}: {}" , for_endpoint, key, value.value());
                reqd_endpoint_state->add_application_state(key, value);
            }
        }
    }
    return reqd_endpoint_state;
}

std::strong_ordering gossiper::compare_endpoint_startup(locator::host_id addr1, locator::host_id addr2) const {
    auto ep1 = get_endpoint_state_ptr(addr1);
    auto ep2 = get_endpoint_state_ptr(addr2);
    if (!ep1 || !ep2) {
        auto err = format("Can not get endpoint_state for {} or {}", addr1, addr2);
        logger.warn("{}", err);
        throw std::runtime_error(err);
    }
    return ep1->get_heart_beat_state().get_generation() <=> ep2->get_heart_beat_state().get_generation();
}

sstring gossiper::get_rpc_address(const locator::host_id& endpoint) const {
    auto* v = get_application_state_ptr(endpoint, gms::application_state::RPC_ADDRESS);
    if (v) {
        return v->value();
    }
    return fmt::to_string(endpoint);
}

void gossiper::update_timestamp_for_nodes(const std::map<inet_address, endpoint_state>& map) {
    for (const auto& x : map) {
        const locator::host_id& endpoint = try_get_host_id(x.first).value_or(locator::host_id{});
        const endpoint_state& remote_endpoint_state = x.second;
        auto local_endpoint_state = get_endpoint_state_ptr(endpoint);
        if (local_endpoint_state) {
            bool update = false;
            auto local_generation = local_endpoint_state->get_heart_beat_state().get_generation();
            auto remote_generation = remote_endpoint_state.get_heart_beat_state().get_generation();
            if (remote_generation > local_generation) {
                update = true;
            } else if (remote_generation == local_generation) {
                auto local_version = get_max_endpoint_state_version(*local_endpoint_state);
                auto remote_version = remote_endpoint_state.get_heart_beat_state().get_heart_beat_version();
                if (remote_version > local_version) {
                    update = true;
                }
            }
            if (update) {
                logger.trace("Updated timestamp for node {}", endpoint);
                update_timestamp(local_endpoint_state);
            }
        }
    }
}

future<> gossiper::notify_nodes_on_up(std::unordered_set<locator::host_id> dsts) {
    co_await coroutine::parallel_for_each(dsts, [this] (locator::host_id dst) -> future<>  {
        if (dst != _gcfg.host_id) {
            try {
                auto generation = my_endpoint_state().get_heart_beat_state().get_generation();
                co_await send_echo(dst, std::chrono::seconds(10), generation.value(), true);
            } catch (...) {
                logger.warn("Failed to notify node {} that I am UP: {}", dst, std::current_exception());
            }
        }
    });
}

void gossiper::mark_alive(endpoint_state_ptr node) {
    auto id = node->get_host_id();
    auto addr = node->get_ip();
    // Enter the _background_msg gate so stop() would wait on it
    auto inserted = _pending_mark_alive_endpoints.insert(id).second;
    if (inserted) {
        // The node is not in the _pending_mark_alive_endpoints
        logger.debug("Mark Node {}/{} alive with EchoMessage", id, addr);
    } else {
        // We are in the progress of marking this node alive
        logger.debug("Node {}/{} is being marked as up, ignoring duplicated mark alive operation", id, addr);
        return;
    }

    // unmark addr as pending on exception or after background continuation completes
    auto unmark_pending = deferred_action([this, id, g = shared_from_this()] () noexcept {
        _pending_mark_alive_endpoints.erase(id);
    });

    if (_address_map.find(id) != addr) {
        // We are here because id has now different ip but we
        // try to ping the old one
        return;
    }
    auto generation = my_endpoint_state().get_heart_beat_state().get_generation();
    // Enter the _background_msg gate so stop() would wait on it
    auto gh = _background_msg.hold();
    logger.debug("Sending a EchoMessage to {}/{}, with generation_number={}", id, addr, generation);
    (void) send_echo(id, std::chrono::seconds(15), generation.value(), false).then([this, id] {
        logger.trace("Got EchoMessage Reply");
        return real_mark_alive(id);
    }).handle_exception([addr, gh = std::move(gh), unmark_pending = std::move(unmark_pending), id] (auto ep) {
        logger.warn("Fail to send EchoMessage to {}/{}: {}", id, addr, ep);
    });
}

future<> gossiper::real_mark_alive(locator::host_id host_id) {
    auto permit = co_await lock_endpoint(host_id, null_permit_id);

    // After sending echo message, the Node might not be in the
    // _endpoint_state_map anymore, use the reference of local_state
    // might cause user-after-free
    auto es = get_endpoint_state_ptr(host_id);
    if (!es) {
        logger.info("Node {} is not in endpoint_state_map anymore", host_id);
        co_return;
    }

    // Do not mark a node with status shutdown as UP.
    auto status = sstring(get_gossip_status(*es));
    if (status == sstring(versioned_value::SHUTDOWN)) {
        logger.warn("Skip marking node {} with status = {} as UP", host_id, status);
        co_return;
    }

    logger.debug("Mark Node {} alive after EchoMessage", host_id);

    // prevents do_status_check from racing us and evicting if it was down > A_VERY_LONG_TIME
    update_timestamp(es);

    logger.debug("removing expire time for endpoint : {}", host_id);
    bool was_live = false;
    co_await mutate_live_and_unreachable_endpoints([addr = host_id, &was_live] (live_and_unreachable_endpoints& data) {
        data.unreachable.erase(addr);
        auto [it_, inserted] = data.live.insert(addr);
        was_live = !inserted;
    });
    _expire_time_endpoint_map.erase(host_id);
    if (was_live) {
        co_return;
    }

    if (_endpoints_to_talk_with.empty()) {
        _endpoints_to_talk_with.push_back({host_id});
    } else {
        _endpoints_to_talk_with.front().push_back(host_id);
    }

    auto addr = es->get_ip();

    logger.info("InetAddress {}/{} is now UP, status = {}", host_id, addr, status);

    co_await _subscribers.for_each([addr, host_id, es, pid = permit.id()] (shared_ptr<i_endpoint_state_change_subscriber> subscriber) -> future<> {
        co_await subscriber->on_alive(addr, host_id, es, pid);
        logger.trace("Notified {}", fmt::ptr(subscriber.get()));
    });
}

future<> gossiper::mark_dead(locator::host_id addr, endpoint_state_ptr state, permit_id pid) {
    logger.trace("marking as down {}", addr);
    verify_permit(addr, pid);

    co_await mutate_live_and_unreachable_endpoints([addr = state->get_host_id()] (live_and_unreachable_endpoints& data) {
        data.live.erase(addr);
        data.unreachable[addr] = now();
    });
    logger.info("InetAddress {} is now DOWN, status = {}", addr, get_gossip_status(*state));
    co_await do_on_dead_notifications(state->get_ip(), std::move(state), pid);
}

future<> gossiper::handle_major_state_change(endpoint_state eps, permit_id pid, bool shadow_round) {
    auto ep = eps.get_host_id();
    verify_permit(ep, pid);

    endpoint_state_ptr eps_old = get_endpoint_state_ptr(ep);

    if (!is_dead_state(eps) && !shadow_round) {
        if (_endpoint_state_map.contains(ep))  {
            logger.info("Node {} has restarted, now UP, status = {}", ep, get_gossip_status(eps));
        } else {
            logger.debug("Node {} is now part of the cluster, status = {}", ep, get_gossip_status(eps));
        }
    }
    logger.trace("Adding endpoint state for {}, status = {}", ep, get_gossip_status(eps));
    co_await replicate(eps, pid);

    if (shadow_round) {
        co_return;
    }

    if (eps_old) {
        // the node restarted: it is up to the subscriber to take whatever action is necessary
        co_await _subscribers.for_each([ep, eps_old, pid] (shared_ptr<i_endpoint_state_change_subscriber> subscriber) {
            return subscriber->on_restart(eps_old->get_ip(), ep, eps_old, pid);
        });
    }

    auto ep_state = get_endpoint_state_ptr(ep);
    if (!ep_state) {
        throw std::out_of_range(format("ep={}", ep));
    }
    if (!is_dead_state(*ep_state)) {
        mark_alive(ep_state);
    } else {
        logger.debug("Not marking {} alive due to dead state {}", ep, get_gossip_status(eps));
        co_await mark_dead(ep, ep_state, pid);
    }

    co_await _subscribers.for_each([ep, ep_state, pid] (shared_ptr<i_endpoint_state_change_subscriber> subscriber) {
        return subscriber->on_join(ep_state->get_ip(), ep, ep_state, pid);
    });

    // check this at the end so nodes will learn about the endpoint
    if (is_shutdown(*ep_state)) {
        co_await mark_as_shutdown(ep, pid);
    }
}

bool gossiper::is_dead_state(const endpoint_state& eps) const {
    return std::ranges::any_of(DEAD_STATES, [state = get_gossip_status(eps)](const auto& deadstate) { return state == deadstate; });
}

bool gossiper::is_shutdown(const locator::host_id& endpoint) const {
    return get_gossip_status(endpoint) == versioned_value::SHUTDOWN;
}

bool gossiper::is_shutdown(const endpoint_state& eps) const {
    return get_gossip_status(eps) == versioned_value::SHUTDOWN;
}

bool gossiper::is_normal(const locator::host_id& endpoint) const {
    return get_gossip_status(endpoint) == versioned_value::STATUS_NORMAL;
}

bool gossiper::is_left(const locator::host_id& endpoint) const {
    auto status = get_gossip_status(endpoint);
    return status == versioned_value::STATUS_LEFT || status == versioned_value::REMOVED_TOKEN;
}

bool gossiper::is_normal_ring_member(const locator::host_id& endpoint) const {
    auto status = get_gossip_status(endpoint);
    return status == versioned_value::STATUS_NORMAL || status == versioned_value::SHUTDOWN;
}

bool gossiper::is_silent_shutdown_state(const endpoint_state& ep_state) const{
    return std::ranges::any_of(SILENT_SHUTDOWN_STATES, [state = get_gossip_status(ep_state)](const auto& deadstate) { return state == deadstate; });
}

future<> gossiper::apply_new_states(endpoint_state local_state, const endpoint_state& remote_state, permit_id pid, bool shadow_round) {
    // don't SCYLLA_ASSERT here, since if the node restarts the version will go back to zero
    //int oldVersion = local_state.get_heart_beat_state().get_heart_beat_version();
    auto host_id = local_state.get_host_id();

    verify_permit(host_id, pid);

    if (!shadow_round) {
        local_state.set_heart_beat_state_and_update_timestamp(remote_state.get_heart_beat_state());
    }
    // if (logger.isTraceEnabled()) {
    //     logger.trace("Updating heartbeat state version to {} from {} for {} ...",
    //     local_state.get_heart_beat_state().get_heart_beat_version(), oldVersion, addr);
    // }

    application_state_map changed;
    auto&& remote_map = remote_state.get_application_state_map();

    std::exception_ptr ep;
    try {
        // we need to make two loops here, one to apply, then another to notify,
        // this way all states in an update are present and current when the notifications are received
        for (const auto& remote_entry : remote_map) {
            const auto& remote_key = remote_entry.first;
            const auto& remote_value = remote_entry.second;
            auto remote_gen = remote_state.get_heart_beat_state().get_generation();
            auto local_gen = local_state.get_heart_beat_state().get_generation();
            if(remote_gen != local_gen) {
                auto err = format("Remote generation {} != local generation {}", remote_gen, local_gen);
                logger.warn("{}", err);
                throw std::runtime_error(err);
            }

            const versioned_value* local_val = local_state.get_application_state_ptr(remote_key);
            if (!local_val || remote_value.version() > local_val->version()) {
                changed.emplace(remote_key, remote_value);
                local_state.add_application_state(remote_key, remote_value);
            }
        }
    } catch (...) {
        ep = std::current_exception();
    }

    auto addr = local_state.get_ip();
    // We must replicate endpoint states before listeners run.
    // Exceptions during replication will cause abort because node's state
    // would be inconsistent across shards. Changes listeners depend on state
    // being replicated to all shards.
    co_await replicate(std::move(local_state), pid);

    if (shadow_round) {
        co_return;
    }

    // Exceptions thrown from listeners will result in abort because that could leave the node in a bad
    // state indefinitely. Unless the value changes again, we wouldn't retry notifications.
    // Some values are set only once, so listeners would never be re-run.
    // Listeners should decide which failures are non-fatal and swallow them.
    try {
        co_await do_on_change_notifications(addr, host_id, changed, pid);
    } catch (...) {
        auto msg = format("Gossip change listener failed: {}", std::current_exception());
        if (_abort_source.abort_requested()) {
            logger.warn("{}. Ignored", msg);
        } else {
            on_fatal_internal_error(logger, msg);
        }
    }

    maybe_rethrow_exception(std::move(ep));
}

future<> gossiper::do_on_change_notifications(inet_address addr, locator::host_id id, const gms::application_state_map& states, permit_id pid) const {
    co_await _subscribers.for_each([&] (shared_ptr<i_endpoint_state_change_subscriber> subscriber) {
        // Once _abort_source is aborted, don't attempt to process any further notifications
        // because that would violate monotonicity due to partially failed notification.
        _abort_source.check();
        return subscriber->on_change(addr, id, states, pid);
    });
}

future<> gossiper::do_on_dead_notifications(inet_address addr, endpoint_state_ptr state, permit_id pid) const {
    co_await _subscribers.for_each([addr, state = std::move(state), pid] (shared_ptr<i_endpoint_state_change_subscriber> subscriber) {
        return subscriber->on_dead(addr, state->get_host_id(), state, pid);
    });
}

void gossiper::request_all(gossip_digest& g_digest,
    utils::chunked_vector<gossip_digest>& delta_gossip_digest_list, generation_type remote_generation) const {
    /* We are here since we have no data for this endpoint locally so request everything. */
    delta_gossip_digest_list.emplace_back(g_digest.get_endpoint(), remote_generation);
    logger.trace("request_all for {}", g_digest.get_endpoint());
}

void gossiper::send_all(gossip_digest& g_digest,
    std::map<inet_address, endpoint_state>& delta_ep_state_map,
    version_type max_remote_version) const {
    auto ep = g_digest.get_endpoint();
    auto id = try_get_host_id(ep);
    logger.trace("send_all(): ep={}/{}, version > {}", id, ep, max_remote_version);
    if (id) {
        auto local_ep_state_ptr = get_state_for_version_bigger_than(*id, max_remote_version);
        if (local_ep_state_ptr) {
            delta_ep_state_map.emplace(ep, *local_ep_state_ptr);
        }
    }
}

void gossiper::examine_gossiper(utils::chunked_vector<gossip_digest>& g_digest_list,
    utils::chunked_vector<gossip_digest>& delta_gossip_digest_list,
    std::map<inet_address, endpoint_state>& delta_ep_state_map) const {
    for (gossip_digest& g_digest : g_digest_list) {
        auto remote_generation = g_digest.get_generation();
        auto max_remote_version = g_digest.get_max_version();
        /* Get state associated with the end point in digest */
        auto&& ep = g_digest.get_endpoint();
        auto id = try_get_host_id(ep);
        auto es = get_endpoint_state_ptr(id.value_or(locator::host_id{}));
        /* Here we need to fire a GossipDigestAckMessage. If we have some
             * data associated with this endpoint locally then we follow the
             * "if" path of the logic. If we have absolutely nothing for this
             * endpoint we need to request all the data for this endpoint.
             */
        if (es) {
            const endpoint_state& ep_state_ptr = *es;
            auto local_generation = ep_state_ptr.get_heart_beat_state().get_generation();
            /* get the max version of all keys in the state associated with this endpoint */
            auto max_local_version = get_max_endpoint_state_version(ep_state_ptr);
            logger.trace("examine_gossiper(): ep={}, remote={}.{}, local={}.{}", ep,
                remote_generation, max_remote_version, local_generation, max_local_version);
            if (remote_generation == local_generation && max_remote_version == max_local_version) {
                continue;
            }

            if (remote_generation > local_generation) {
                /* we request everything from the gossiper */
                request_all(g_digest, delta_gossip_digest_list, remote_generation);
            } else if (remote_generation < local_generation) {
                /* send all data with generation = localgeneration and version > 0 */
                send_all(g_digest, delta_ep_state_map, version_type());
            } else if (remote_generation == local_generation) {
                /*
                 * If the max remote version is greater then we request the
                 * remote endpoint send us all the data for this endpoint
                 * with version greater than the max version number we have
                 * locally for this endpoint.
                 *
                 * If the max remote version is lesser, then we send all
                 * the data we have locally for this endpoint with version
                 * greater than the max remote version.
                 */
                if (max_remote_version > max_local_version) {
                    logger.trace("examine_gossiper(): requesting version > {} from {}", max_local_version, g_digest.get_endpoint());
                    delta_gossip_digest_list.emplace_back(g_digest.get_endpoint(), remote_generation, max_local_version);
                } else if (max_remote_version < max_local_version) {
                    /* send all data with generation = localgeneration and version > max_remote_version */
                    send_all(g_digest, delta_ep_state_map, max_remote_version);
                }
            }
        } else {
            /* We are here since we have no data for this endpoint locally so request everything. */
            request_all(g_digest, delta_gossip_digest_list, remote_generation);
        }
    }
}

future<> gossiper::start_gossiping(gms::generation_type generation_nbr, application_state_map preload_local_states) {
    auto permit = co_await lock_endpoint(my_host_id(), null_permit_id);

    build_seeds_list();
    if (_gcfg.force_gossip_generation() > 0) {
        generation_nbr = gms::generation_type(_gcfg.force_gossip_generation());
        logger.warn("Use the generation number provided by user: generation = {}", generation_nbr);
    }
    endpoint_state local_state = my_endpoint_state();
    local_state.set_heart_beat_state_and_update_timestamp(heart_beat_state(generation_nbr));
    for (auto& entry : preload_local_states) {
        local_state.add_application_state(entry.first, entry.second);
    }

    co_await utils::get_local_injector().inject("gossiper_publish_local_state_pause", utils::wait_for_message(5min));

    co_await replicate(local_state, permit.id());

    logger.info("Gossip started with local state: {}", local_state);
    _enabled = true;
    _nr_run = 0;
    _scheduled_gossip_task.arm(INTERVAL);
    if (!_background_msg.is_closed()) {
        co_await _background_msg.close();
    }
    _background_msg = seastar::named_gate("gossiper");
    /* Ensure all shards have enabled gossip before starting the failure detector loop */
    co_await container().invoke_on_all([] (gms::gossiper& g) {
        g._enabled = true;
    });
    co_await container().invoke_on(0, [] (gms::gossiper& g) {
        g._failure_detector_loop_done = g.failure_detector_loop();
    });
}

future<gossiper::generation_for_nodes>
gossiper::get_generation_for_nodes(std::unordered_set<locator::host_id> nodes) const {
    generation_for_nodes ret;
    for (const auto& node : nodes) {
        auto es = get_endpoint_state_ptr(node);
        if (es) {
            auto current_generation_number = es->get_heart_beat_state().get_generation();
            ret.emplace(node, current_generation_number);
        } else {
            return make_exception_future<generation_for_nodes>(
                    std::runtime_error(format("Can not find generation number for node={}", node)));
        }
    }
    return make_ready_future<generation_for_nodes>(std::move(ret));
}

future<> gossiper::advertise_to_nodes(generation_for_nodes advertise_to_nodes) {
    return container().invoke_on_all([advertise_to_nodes = std::move(advertise_to_nodes)] (auto& g) {
        g._advertise_to_nodes = advertise_to_nodes;
    });
}

future<> gossiper::do_shadow_round(std::unordered_set<gms::inet_address> nodes, mandatory is_mandatory) {
    nodes.erase(get_broadcast_address());
    gossip_get_endpoint_states_request request{{
        gms::application_state::STATUS,
        gms::application_state::HOST_ID,
        gms::application_state::TOKENS,
        gms::application_state::DC,
        gms::application_state::RACK,
        gms::application_state::SUPPORTED_FEATURES,
        gms::application_state::SNITCH_NAME}};
    logger.info("Gossip shadow round started with nodes={}", nodes);
    std::unordered_set<gms::inet_address> nodes_talked;
    auto start_time = clk::now();
    std::list<gms::gossip_get_endpoint_states_response> responses;

    for (;;) {
        size_t nodes_down = 0;
        co_await coroutine::parallel_for_each(nodes, [this, &request, &responses, &nodes_talked, &nodes_down] (gms::inet_address node) -> future<> {
            logger.debug("Sent get_endpoint_states request to {}, request={}", node, request.application_states);
            try {
                auto response = co_await ser::gossip_rpc_verbs::send_gossip_get_endpoint_states(&_messaging, msg_addr(node), netw::messaging_service::clock_type::now() + std::chrono::seconds(5), request);

                logger.debug("Got get_endpoint_states response from {}, response={}", node, response.endpoint_state_map);
                responses.push_back(std::move(response));
                nodes_talked.insert(node);

                utils::get_local_injector().inject("stop_during_gossip_shadow_round", [] { std::raise(SIGSTOP); });
            } catch (seastar::rpc::unknown_verb_error&) {
                auto err = format("Node {} does not support get_endpoint_states verb", node);
                logger.error("{}", err);
                throw std::runtime_error{err};
            } catch (seastar::rpc::timeout_error&) {
                nodes_down++;
                logger.warn("The get_endpoint_states verb to node {} timed out", node);
            } catch (seastar::rpc::closed_error&) {
                nodes_down++;
                logger.warn("Node {} is down for get_endpoint_states verb", node);
            }
        });

        for (auto& response : responses) {
            try {
                co_await apply_state_locally_in_shadow_round(std::move(response.endpoint_state_map));
            } catch (const std::exception& exception) {
                logger.warn("Error while applying node state {}", exception.what());
            }
        }
        if (!nodes_talked.empty()) {
            break;
        }
        if (nodes_down == nodes.size() && !is_mandatory) {
            logger.warn("All nodes={} are down for get_endpoint_states verb. Skip ShadowRound.", nodes);
            break;
        }
        if (clk::now() > start_time + std::chrono::milliseconds(_gcfg.shadow_round_ms)) {
            throw std::runtime_error(fmt::format("Unable to gossip with any nodes={} (ShadowRound).", nodes));
        }
        sleep_abortable(std::chrono::seconds(1), _abort_source).get();
        logger.info("Connect nodes={} again ... ({} seconds passed)",
                nodes, std::chrono::duration_cast<std::chrono::seconds>(clk::now() - start_time).count());
        if (!nodes_talked.empty()) {
            break;
        }
        if (nodes_down == nodes.size() && !is_mandatory) {
            logger.warn("All nodes={} are down for get_endpoint_states verb. Skip ShadowRound.", nodes);
            break;
        }
        if (clk::now() > start_time + std::chrono::milliseconds(_gcfg.shadow_round_ms)) {
            throw std::runtime_error(fmt::format("Unable to gossip with any nodes={} (ShadowRound).", nodes));
        }
        sleep_abortable(std::chrono::seconds(1), _abort_source).get();
        logger.info("Connect nodes={} again ... ({} seconds passed)",
                nodes, std::chrono::duration_cast<std::chrono::seconds>(clk::now() - start_time).count());
    }
    logger.info("Gossip shadow round finished with nodes_talked={}", nodes_talked);
}

void gossiper::build_seeds_list() {
    for (inet_address seed : get_seeds() ) {
        if (seed == get_broadcast_address()) {
            continue;
        }
        _seeds.emplace(seed);
    }
}

future<> gossiper::add_saved_endpoint(locator::host_id host_id, gms::loaded_endpoint_state st, permit_id pid) {
    if (host_id == my_host_id()) {
        logger.debug("Attempt to add self as saved endpoint");
        co_return;
    }
    const auto& ep = st.endpoint;
    if (!host_id) {
        on_internal_error(logger, format("Attempt to add {} with null host_id as saved endpoint", ep));
    }
    if (ep == inet_address{}) {
        on_internal_error(logger, format("Attempt to add {} with null inet_address as saved endpoint", host_id));
    }
    if (ep == get_broadcast_address()) {
        on_internal_error(logger, format("Attempt to add {} with broadcast_address {} as saved endpoint", host_id, ep));
    }

    auto permit = co_await lock_endpoint(host_id, pid);

    //preserve any previously known, in-memory data about the endpoint (such as DC, RACK, and so on)
    auto ep_state = endpoint_state(ep);
    auto es = get_endpoint_state_ptr(host_id);
    if (es) {
        if (es->get_heart_beat_state().get_generation()) {
            auto msg = fmt::format("Attempted to add saved endpoint {} after endpoint_state was already established with gossip: {}, at {}", ep, es->get_heart_beat_state(), current_backtrace());
            on_internal_error(logger, msg);
        }
        ep_state = *es;
        logger.debug("not replacing a previous ep_state for {}, but reusing it: {}", ep, ep_state);
        ep_state.update_timestamp();
    }
    // It's okay to use the local version generator for the loaded application state values
    // As long as the endpoint_state has zero generation.
    // It will get updated as a whole by handle_major_state_change
    // via do_apply_state_locally when (remote_generation > local_generation)
    const auto tmptr = get_token_metadata_ptr();
    ep_state.add_application_state(gms::application_state::HOST_ID, versioned_value::host_id(host_id));
    auto tokens = tmptr->get_tokens(host_id);
    if (!tokens.empty()) {
        std::unordered_set<dht::token> tokens_set(tokens.begin(), tokens.end());
        ep_state.add_application_state(gms::application_state::TOKENS, versioned_value::tokens(tokens_set));
    }
    if (st.opt_dc_rack) {
        ep_state.add_application_state(gms::application_state::DC, gms::versioned_value::datacenter(st.opt_dc_rack->dc));
        ep_state.add_application_state(gms::application_state::RACK, gms::versioned_value::datacenter(st.opt_dc_rack->rack));
    }
    auto generation = ep_state.get_heart_beat_state().get_generation();
    co_await replicate(std::move(ep_state), permit.id());
    _unreachable_endpoints[host_id] = now();
    logger.trace("Adding saved endpoint {} {}", ep, generation);
}

future<> gossiper::add_local_application_state(application_state state, versioned_value value) {
    application_state_map tmp;
    tmp.emplace(std::pair(std::move(state), std::move(value)));
    return add_local_application_state(std::move(tmp));
}

// Depends on:
// - on_change callbacks
// #2894. Similar to origin fix, but relies on non-interruptability to ensure we apply
// values "in order".
//
// NOTE: having the values being actual versioned values here is sort of pointless, because
// we overwrite the version to ensure the set is monotonic. However, it does not break anything,
// and changing this tends to spread widely (see versioned_value::factory), so that can be its own
// change later, if needed.
// Retaining the slightly broken signature is also consistent with origin. Hooray.
//
future<> gossiper::add_local_application_state(application_state_map states) {
    if (states.empty()) {
        co_return;
    }
    try {
        co_await container().invoke_on(0, [&] (gossiper& gossiper) mutable -> future<> {
            inet_address ep_addr = gossiper.get_broadcast_address();
            auto ep_id = gossiper.my_host_id();
            // for symmetry with other apply, use endpoint lock for our own address.
            auto permit = co_await gossiper.lock_endpoint(ep_id, null_permit_id);
            auto ep_state_before = gossiper.get_endpoint_state_ptr(ep_id);
            if (!ep_state_before) {
                auto err = fmt::format("endpoint_state_map does not contain endpoint = {}, application_states = {}",
                                  ep_addr, states);
                co_await coroutine::return_exception(std::runtime_error(err));
            }

            auto local_state = *ep_state_before;
            for (auto& p : states) {
                auto& state = p.first;
                auto& value = p.second;
                // Notifications may have taken some time, so preventively raise the version
                // of the new value, otherwise it could be ignored by the remote node
                // if another value with a newer version was received in the meantime:
                value = versioned_value::clone_with_higher_version(value);
                // Add to local application state
                local_state.add_application_state(state, value);
            }

            // It is OK to replicate the new endpoint_state
            // after all application states were modified as a batch.
            // We guarantee that the on_change notifications
            // will be called in the order given by `states` anyhow.
            co_await gossiper.replicate(std::move(local_state), permit.id());

            // fire "on change" notifications:
            // now we might defer again, so this could be reordered. But we've
            // ensured the whole set of values are monotonically versioned and
            // applied to endpoint state.
            co_await gossiper.do_on_change_notifications(ep_addr, gossiper.my_host_id(), states, permit.id());
        });
    } catch (...) {
        logger.warn("Fail to apply application_state: {}", std::current_exception());
    }
}

future<> gossiper::do_stop_gossiping() {
    // Don't rely on is_enabled() since it
    // also considers _abort_source and return false
    // before _enabled is set to false down below.
    if (!_enabled) {
        logger.info("gossip is already stopped");
        co_return;
    }
    auto my_ep_state = get_this_endpoint_state_ptr();
    if (my_ep_state) {
        logger.info("My status = {}", get_gossip_status(*my_ep_state));
    }
    if (my_ep_state && !is_silent_shutdown_state(*my_ep_state)) {
        auto local_generation = my_ep_state->get_heart_beat_state().get_generation();
        logger.info("Announcing shutdown");
        co_await add_local_application_state(application_state::STATUS, versioned_value::shutdown(true));
        auto live_endpoints = _live_endpoints;
        for (locator::host_id id : live_endpoints) {
            logger.info("Sending a GossipShutdown to {} with generation {}", id, local_generation);
            try {
                co_await ser::gossip_rpc_verbs::send_gossip_shutdown(&_messaging, id, get_broadcast_address(), local_generation.value());
                logger.trace("Got GossipShutdown Reply");
            } catch (...) {
                logger.warn("Fail to send GossipShutdown to {}: {}", id, std::current_exception());
            }
        }
        co_await sleep(std::chrono::milliseconds(_gcfg.shutdown_announce_ms));
    } else {
        logger.warn("No local state or state is in silent shutdown, not announcing shutdown");
    }
    logger.info("Disable and wait for gossip loop started");
    // Set disable flag and cancel the timer makes sure gossip loop will not be scheduled
    co_await container().invoke_on_all([] (gms::gossiper& g) {
        g._enabled = false;
    });
    _scheduled_gossip_task.cancel();
    // Take the semaphore makes sure existing gossip loop is finished
    auto units = co_await get_units(_callback_running, 1);
    co_await container().invoke_on(0, [] (auto& g) {
        // #21159
        // gossiper::shutdown can be called from more than once place - both 
        // storage_service::isolate and normal gossip service stop. The former is
        // waited for in storage_service::stop, but if we, as was done in cql_test_env,
        // call shutdown independently, we could still end up here twite, and not hit 
        // the _enabled guard (because we do waiting things before setting it, and setting it
        // is also waiting). However, making sure we don't leave an invalid future 
        // here should ensure even if we reenter this method in such as way, we don't crash.
        return std::exchange(g._failure_detector_loop_done, make_ready_future<>());
    });
    logger.info("Gossip is now stopped");
}

future<> gossiper::start() {
    init_messaging_service_handler();
    return make_ready_future();
}

future<> gossiper::shutdown() {
    if (!_background_msg.is_closed()) {
        co_await _background_msg.close();
    }
    if (this_shard_id() == 0) {
        co_await do_stop_gossiping();
    }
}

future<> gossiper::stop() {
    co_await shutdown();
    co_await uninit_messaging_service_handler();
}

bool gossiper::is_enabled() const {
    return _enabled && !_abort_source.abort_requested();
}

void gossiper::add_expire_time_for_endpoint(locator::host_id endpoint, clk::time_point expire_time) {
    auto now_ = now();
    auto diff = std::chrono::duration_cast<std::chrono::seconds>(expire_time - now_).count();
    logger.info("Node {} will be removed from gossip at [{:%Y-%m-%d %T}]: (expire = {}, now = {}, diff = {} seconds)",
            endpoint, fmt::localtime(clk::to_time_t(expire_time)), expire_time.time_since_epoch().count(),
            now_.time_since_epoch().count(), diff);
    _expire_time_endpoint_map[endpoint] = expire_time;
}

clk::time_point gossiper::compute_expire_time() {
    return now() + A_VERY_LONG_TIME;
}

bool gossiper::is_alive(locator::host_id id) const {
    if (id == my_host_id()) {
        return true;
    }

    bool is_alive = _live_endpoints.contains(id);

#ifndef SCYLLA_BUILD_MODE_RELEASE
    // Live endpoints must always have a valid endpoint_state.
    // Verify that in testing mode to reduce the overhead in production.
    if (is_alive && !get_endpoint_state_ptr(id)) {
        on_internal_error(logger, fmt::format("Node {} is alive but has no endpoint state", id));
    }
#endif

    return is_alive;
}

future<> gossiper::wait_alive_helper(noncopyable_function<std::vector<locator::host_id>()> get_nodes, std::chrono::milliseconds timeout) {
    auto start_time = std::chrono::steady_clock::now();
    for (;;) {
        auto nodes = get_nodes();
        std::vector<locator::host_id> live_nodes;
        for (const auto& node: nodes) {
            auto es = get_endpoint_state_ptr(node);
            if (es) {
                size_t nr_alive = co_await container().map_reduce0([node = es->get_host_id()] (gossiper& g) -> size_t {
                    return g.is_alive(node) ? 1 : 0;
                }, 0, std::plus<size_t>());
                logger.debug("Marked node={} as alive on {} out of {} shards", node, nr_alive, smp::count);
                if (nr_alive == smp::count) {
                    live_nodes.push_back(node);
                }
            }
        }
        logger.debug("Waited for marking node as up, replace_nodes={}, live_nodes={}", nodes, live_nodes);
        if (live_nodes.size() == nodes.size()) {
            break;
        }
        if (std::chrono::steady_clock::now() > timeout + start_time) {
            throw std::runtime_error(fmt::format("Failed to mark node as alive in {} ms, nodes={}, live_nodes={}",
                    timeout.count(), nodes, live_nodes));
        }
        co_await sleep_abortable(std::chrono::milliseconds(100), _abort_source);
    }
}

// Needed for legacy (node_ops) mode only)
future<> gossiper::wait_alive(std::vector<gms::inet_address> nodes, std::chrono::milliseconds timeout) {
    auto ids = nodes | std::views::transform([this] (auto ip) { return get_host_id(ip); }) | std::ranges::to<std::vector>();
    return wait_alive(std::move(ids), timeout);
}

future<> gossiper::wait_alive(std::vector<locator::host_id> nodes, std::chrono::milliseconds timeout) {
    return wait_alive_helper([nodes = std::move(nodes)] { return nodes; }, timeout);
}

future<> gossiper::wait_alive(noncopyable_function<std::vector<locator::host_id>()> get_nodes, std::chrono::milliseconds timeout) {
    return wait_alive_helper(std::move(get_nodes), timeout);
}

future<> gossiper::wait_for_live_nodes_to_show_up(size_t n) {
    logger::rate_limit rate_limit{std::chrono::seconds{5}};
    // Account for gossip slowness. 3 minutes is probably overkill but we don't want flaky tests.
    constexpr auto timeout_delay = std::chrono::minutes{3};
    auto timeout = gossiper::clk::now() + timeout_delay;
    while (get_live_members().size() < n) {
        if (timeout <= gossiper::clk::now()) {
            auto err = ::format("Timed out waiting for {} live nodes to show up in gossip", n);
            logger.error("{}", err);
            throw std::runtime_error{std::move(err)};
        }

        logger.log(log_level::info, rate_limit,
                   "Waiting for {} live nodes to show up in gossip, currently {} present...",
                   n, get_live_members().size());
        co_await sleep_abortable(std::chrono::milliseconds(10), _abort_source);
    }
    logger.info("Live nodes seen in gossip: {}", get_live_members());
}

const versioned_value* gossiper::get_application_state_ptr(locator::host_id endpoint, application_state appstate) const noexcept {
    auto eps = get_endpoint_state_ptr(std::move(endpoint));
    if (!eps) {
        return nullptr;
    }
    return eps->get_application_state_ptr(appstate);
}

sstring gossiper::get_application_state_value(locator::host_id endpoint, application_state appstate) const {
    auto v = get_application_state_ptr(endpoint, appstate);
    if (!v) {
        return {};
    }
    return v->value();
}

/**
 * This method is used to mark a node as shutdown; that is it gracefully exited on its own and told us about it
 * @param endpoint endpoint that has shut itself down
 */
future<> gossiper::mark_as_shutdown(const locator::host_id& endpoint, permit_id pid) {
    verify_permit(endpoint, pid);
    auto es = get_endpoint_state_ptr(endpoint);
    if (es) {
        auto ep_state = *es;
        ep_state.add_application_state(application_state::STATUS, versioned_value::shutdown(true));
        ep_state.get_heart_beat_state().force_highest_possible_version_unsafe();
        co_await replicate(std::move(ep_state), pid);
        co_await mark_dead(endpoint, get_endpoint_state_ptr(endpoint), pid);
    }
}

void gossiper::force_newer_generation() {
    auto& eps = my_endpoint_state();
    eps.get_heart_beat_state().force_newer_generation_unsafe();
}

static std::string_view do_get_gossip_status(const gms::versioned_value* app_state) noexcept {
    if (!app_state) {
        return gms::versioned_value::STATUS_UNKNOWN;
    }
    const std::string_view value = app_state->value();
    const auto pos = value.find(',');
    if (value.empty() || !pos) {
        return gms::versioned_value::STATUS_UNKNOWN;
    }
    // npos allowed (full value)
    return value.substr(0, pos);
}

std::string_view gossiper::get_gossip_status(const endpoint_state& ep_state) const noexcept {
    return do_get_gossip_status(ep_state.get_application_state_ptr(application_state::STATUS));
}

std::string_view gossiper::get_gossip_status(const locator::host_id& endpoint) const noexcept {
    return do_get_gossip_status(get_application_state_ptr(endpoint, application_state::STATUS));
}

future<> gossiper::wait_for_gossip(std::chrono::milliseconds initial_delay, std::optional<int32_t> force_after) const {
    static constexpr std::chrono::milliseconds GOSSIP_SETTLE_POLL_INTERVAL_MS{1000};
    static constexpr int32_t GOSSIP_SETTLE_POLL_SUCCESSES_REQUIRED = 3;

    if (force_after && *force_after == 0) {
        logger.warn("Skipped to wait for gossip to settle by user request since skip_wait_for_gossip_to_settle is set zero. Do not use this in production!");
        co_return;
    }

    int32_t total_polls = 0;
    int32_t num_okay = 0;
    auto ep_size = _endpoint_state_map.size();

    auto delay = initial_delay;

    co_await sleep_abortable(GOSSIP_SETTLE_MIN_WAIT_MS, _abort_source);
    while (num_okay < GOSSIP_SETTLE_POLL_SUCCESSES_REQUIRED) {
        co_await sleep_abortable(delay, _abort_source);
        delay = GOSSIP_SETTLE_POLL_INTERVAL_MS;

        auto current_size = _endpoint_state_map.size();
        total_polls++;
        if (current_size == ep_size && _msg_processing == 0) {
            logger.debug("Gossip looks settled");
            num_okay++;
        } else {
            logger.info("Gossip not settled after {} polls.", total_polls);
            num_okay = 0;
        }
        ep_size = current_size;
        if (force_after && *force_after > 0 && total_polls > *force_after) {
            logger.warn("Gossip not settled but startup forced by skip_wait_for_gossip_to_settle. Gossp total polls: {}", total_polls);
            break;
        }
    }
    if (total_polls > GOSSIP_SETTLE_POLL_SUCCESSES_REQUIRED) {
        logger.info("Gossip settled after {} extra polls; proceeding", total_polls - GOSSIP_SETTLE_POLL_SUCCESSES_REQUIRED);
    } else {
        logger.info("No gossip backlog; proceeding");
    }
}

future<> gossiper::wait_for_gossip_to_settle() const {
    auto force_after = _gcfg.skip_wait_for_gossip_to_settle;
    if (force_after != 0) {
        co_await wait_for_gossip(GOSSIP_SETTLE_MIN_WAIT_MS, force_after);
    }
}

future<> gossiper::wait_for_range_setup() const {
    logger.info("Waiting for pending range setup...");
    auto ring_delay = std::chrono::milliseconds(_gcfg.ring_delay_ms);
    auto force_after = _gcfg.skip_wait_for_gossip_to_settle;
    return wait_for_gossip(ring_delay, force_after);
}

bool gossiper::is_safe_for_bootstrap(inet_address endpoint) const {
    // We allow to bootstrap a new node in only two cases:
    // 1) The node is a completely new node and no state in gossip at all
    // 2) The node has state in gossip and it is already removed from the
    // cluster either by nodetool decommission or nodetool removenode
    bool allowed = true;
    auto host_id = try_get_host_id(endpoint);
    if (!host_id) {
        logger.debug("is_safe_for_bootstrap: node={}, status=no state in gossip, allowed_to_bootstrap={}", endpoint, allowed);
        return allowed;
    }
    auto eps = get_endpoint_state_ptr(*host_id);
    if (!eps) {
        logger.debug("is_safe_for_bootstrap: node={}, status=no state in gossip, allowed_to_bootstrap={}", endpoint, allowed);
        return allowed;
    }
    auto status = get_gossip_status(*eps);
    std::unordered_set<std::string_view> allowed_statuses{
        versioned_value::STATUS_LEFT,
        versioned_value::REMOVED_TOKEN,
    };
    allowed = allowed_statuses.contains(status);
    logger.debug("is_safe_for_bootstrap: node={}, status={}, allowed_to_bootstrap={}", endpoint, status, allowed);
    return allowed;
}

bool gossiper::is_safe_for_restart(locator::host_id host_id) const {
    // Reject to restart a node in case:
    // *) if the node has been removed from the cluster by nodetool decommission or
    //    nodetool removenode
    std::unordered_set<std::string_view> not_allowed_statuses{
        versioned_value::STATUS_LEFT,
        versioned_value::REMOVED_TOKEN,
    };
    bool allowed = true;
    for (auto& x : _endpoint_state_map) {
        auto node = x.first;
        try {
            auto status = get_gossip_status(node);
            logger.debug("is_safe_for_restart: node with host_id={}, status={}", node, status);
            if (host_id == node && not_allowed_statuses.contains(status)) {
                allowed = false;
                logger.error("is_safe_for_restart: node with host_id={}, status={}", node, status);
                break;
            }
        } catch (...) {
            logger.info("is_safe_for_restart: node={} doesn't not have status or host_id yet in gossip", node);
        }
    }
    return allowed;
}

std::set<sstring> gossiper::get_supported_features(locator::host_id endpoint) const {
    auto app_state = get_application_state_ptr(endpoint, application_state::SUPPORTED_FEATURES);
    if (!app_state) {
        return {};
    }
    return feature_service::to_feature_set(app_state->value());
}

std::set<sstring> gossiper::get_supported_features(const std::unordered_map<locator::host_id, sstring>& loaded_peer_features, ignore_features_of_local_node ignore_local_node) const {
    std::unordered_map<locator::host_id, std::set<sstring>> features_map;
    std::set<sstring> common_features;

    for (auto& x : loaded_peer_features) {
        auto features = feature_service::to_feature_set(x.second);
        if (features.empty()) {
            logger.warn("Loaded empty features for peer node {}", x.first);
        } else {
            features_map.emplace(x.first, std::move(features));
        }
    }

    for (auto& x : _endpoint_state_map) {
        auto host_id = x.second->get_host_id();
        auto features = get_supported_features(host_id);
        if (ignore_local_node && host_id == my_host_id()) {
            logger.debug("Ignore SUPPORTED_FEATURES of local node: features={}", features);
            continue;
        }
        if (features.empty()) {
            auto it = loaded_peer_features.find(host_id);
            if (it != loaded_peer_features.end()) {
                logger.info("Node {} does not contain SUPPORTED_FEATURES in gossip, using features saved in system table, features={}", host_id, feature_service::to_feature_set(it->second));
            } else {
                logger.warn("Node {} does not contain SUPPORTED_FEATURES in gossip or system table", host_id);
            }
        } else {
            // Replace the features with live info
            features_map[host_id] = std::move(features);
        }
    }

    if (ignore_local_node) {
        features_map.erase(my_host_id());
    }

    if (!features_map.empty()) {
        common_features = features_map.begin()->second;
    }

    for (auto& x : features_map) {
        auto& features = x.second;
        std::set<sstring> result;
        std::set_intersection(features.begin(), features.end(),
                common_features.begin(), common_features.end(),
                std::inserter(result, result.end()));
        common_features = std::move(result);
    }
    common_features.erase("");
    return common_features;
}

void gossiper::check_knows_remote_features(std::set<std::string_view>& local_features, const std::unordered_map<locator::host_id, sstring>& loaded_peer_features) const {
    auto local_endpoint = get_broadcast_address();
    auto common_features = get_supported_features(loaded_peer_features, ignore_features_of_local_node::yes);
    if (boost::range::includes(local_features, common_features)) {
        logger.info("Feature check passed. Local node {}/{} features = {}, Remote common_features = {}",
                local_endpoint, my_host_id(), local_features, common_features);
    } else {
        throw std::runtime_error(fmt::format("Feature check failed. This node can not join the cluster because it does not understand the feature. Local node {} features = {}, Remote common_features = {}", local_endpoint, local_features, common_features));
    }
}

void gossiper::check_snitch_name_matches(sstring local_snitch_name) const {
    for (const auto& [address, state] : _endpoint_state_map) {
        const auto remote_snitch_name = state->get_application_state_ptr(application_state::SNITCH_NAME);
        if (!remote_snitch_name) {
            continue;
        }

        if (remote_snitch_name->value() != local_snitch_name) {
            throw std::runtime_error(format("Snitch check failed. This node cannot join the cluster because it uses {} and not {}", local_snitch_name, remote_snitch_name->value()));
        }
    }
}

int gossiper::get_down_endpoint_count() const noexcept {
    return _endpoint_state_map.size() - get_up_endpoint_count();
}

int gossiper::get_up_endpoint_count() const noexcept {
    return std::ranges::count_if(_endpoint_state_map | std::views::values, [this] (const endpoint_state_ptr& es) {
        return is_alive(es->get_host_id());
    });
}

void gossiper::append_endpoint_state(std::stringstream& ss, const endpoint_state& state) {
    ss << "  generation:" << state.get_heart_beat_state().get_generation() << "\n";
    ss << "  heartbeat:" << state.get_heart_beat_state().get_heart_beat_version() << "\n";
    for (const auto& entry : state.get_application_state_map()) {
        auto& app_state = entry.first;
        auto& versioned_val = entry.second;
        if (app_state == application_state::TOKENS) {
            continue;
        }
        fmt::print(ss, "  {}:{}:{}\n", app_state, versioned_val.version(), versioned_val.value());
    }
    const auto& app_state_map = state.get_application_state_map();
    if (app_state_map.contains(application_state::TOKENS)) {
        ss << "  TOKENS:" << app_state_map.at(application_state::TOKENS).version() << ":<hidden>\n";
    } else {
        ss << "  TOKENS: not present" << "\n";
    }
}

locator::token_metadata_ptr gossiper::get_token_metadata_ptr() const noexcept {
    return _shared_token_metadata.get();
}

} // namespace gms

auto fmt::formatter<gms::loaded_endpoint_state>::format(const gms::loaded_endpoint_state& st, fmt::format_context& ctx) const -> decltype(ctx.out()) {
    return fmt::format_to(ctx.out(), "{{ endpoint={} dc={} rack={} tokens={} }}", st.endpoint,
            st.opt_dc_rack ? st.opt_dc_rack->dc : "",
            st.opt_dc_rack ? st.opt_dc_rack->rack : "",
            st.tokens);
}
