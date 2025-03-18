/*
 * Copyright (C) 2025-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.0 and Apache-2.0)
 */

#pragma once

#include "cql_test_env.hh"
#include "locator/topology.hh"
#include "gms/inet_address.hh"
#include "service/topology_mutation.hh"
#include "service/topology_state_machine.hh"
#include "service/raft/raft_group0_client.hh"
#include "locator/host_id.hh"
#include "test/lib/log.hh"
#include "version.hh"

#include <atomic>


/// Modifies topology inside a given cql_test_env.
/// Local node's membership is not affected, but it belongs to a different DC than those produced by this builder.
///
/// Creating the builder locks the topology state machine so there are no concurrent topology operations
/// and load balancing.
/// The built topology is not removed when the builder is destroyed and the state machine is left locked.
///
/// All methods expect to be run in a seastar thread.
///
/// Examples usage:
///
///     topology_builder topo(e);
///     auto host1 = topo.add_node(); // dc1 rack1
///     auto host2 = topo.add_node(); // dc1 rack1
///     topo.start_new_dc();
///     auto host3 = topo.add_node(); // dc2 rack1
///     auto host4 = topo.add_node(); // dc2 rack1
///     topo.start_new_rack();
///     auto host5 = topo.add_node(); // dc2 rack2
///     auto host6 = topo.add_node(); // dc2 rack2
///
class topology_builder {
public:
    using inet_address = locator::inet_address;
    using endpoint_dc_rack = locator::endpoint_dc_rack;
private:
    cql_test_env& _env;
    int _nr_nodes = 0;
    int _rack_id;
    sstring _dc;
    sstring _rack;
private:
    inet_address make_node_address(int n) {
        assert(n > 0);
        int a = n % 256;
        n /= 256;
        int b = n % 256;
        n /= 256;
        assert(n < 256);
        return inet_address(fmt::format("10.{}.{}.{}", n, b, a));
    }

    // Locks the topology to prevent concurrent topology operations and load balancing.
    // Setting transition_state to "lock" blocks background load-balancing which could interfere with the test
    // and prevents errors from load_topology_state() complaining about nodes in transition with no transition state.
    void lock_topology() {
        abort_source as;
        auto& client = _env.get_raft_group0_client();
        service::topology& topo = _env.get_topology_state_machine().local()._topology;

        while (true) {
            if (topo.tstate && *topo.tstate == service::topology::transition_state::lock) {
                testlog.info("Topology is locked");
                return;
            }

            auto guard = client.start_operation(as).get();

            if (topo.tstate) {
                testlog.info("Waiting for topology state machine to be idle");
                release_guard(std::move(guard));
                _env.get_topology_state_machine().local().await_not_busy().get();
                testlog.info("Woken up");
                continue;
            }

            service::topology_change change({service::topology_mutation_builder(guard.write_timestamp())
                                                 .set_transition_state(service::topology::transition_state::lock)
                                                 .build()});
            service::group0_command g0_cmd = client.prepare_command(std::move(change), guard, "locking topology");
            try {
                client.add_entry(std::move(g0_cmd), std::move(guard), as).get();
            } catch (service::group0_concurrent_modification&) {
                testlog.info("Concurrent modification detected while locking topology, retrying");
            }
        }
    }
public:
    topology_builder(cql_test_env& e)
        : _env(e)
    {
        start_new_dc();
        lock_topology();
    }

    // Returns a new token from some sequence of unique tokens.
    // Uniqueness is in the scope of the process, not just this object.
    dht::token new_token() {
        static std::atomic<int64_t> next_token = 1;
        return dht::token(next_token.fetch_add(1));
    }

    // Returns the name of the currently built DC.
    const sstring& dc() const {
        return _dc;
    }

    // Returns location of the currently built rack.
    endpoint_dc_rack rack() const {
        return {_dc, _rack};
    }

    // Starts building a new rack in the current DC.
    // Returns location of the new rack.
    endpoint_dc_rack start_new_rack() {
        _rack_id++;
        _rack = fmt::format("rack{}", _rack_id);
        return rack();
    }

    // Starts building a new DC.
    // DC is named uniquely in the scope of the process, not just this object.
    endpoint_dc_rack start_new_dc() {
        static std::atomic<int> next_id = 1;
        _dc = fmt::format("dc{}", next_id.fetch_add(1));
        _rack_id = 0;
        return start_new_rack();
    }

    locator::host_id add_node(service::node_state state = service::node_state::normal,
                              unsigned shard_count = 1,
                              std::optional<endpoint_dc_rack> rack_override = {})
    {
        ++_nr_nodes;

        auto ip = make_node_address(_nr_nodes);
        auto id = locator::host_id(utils::UUID_gen::get_time_UUID());
        auto dc_rack = rack_override.value_or(rack());
        dht::token token = new_token();
        std::unordered_set<dht::token> tokens({token});

        abort_source as;
        auto& client = _env.get_raft_group0_client();

        while (true) {
            auto guard = client.start_operation(as).get();

            service::topology_mutation_builder builder(guard.write_timestamp());
            builder.with_node(raft::server_id(id.uuid()))
                    .set("datacenter", dc_rack.dc)
                    .set("rack", dc_rack.rack)
                    .set("node_state", state)
                    .set("shard_count", (uint32_t) shard_count)
                    .set("cleanup_status", service::cleanup_status::clean)
                    .set("release_version", version::release())
                    .set("num_tokens", (uint32_t) 1)
                    .set("tokens_string", fmt::format("{}", token))
                    .set("tokens", tokens)
                    .set("supported_features", std::set<sstring>())
                    .set("request_id", utils::UUID())
                    .set("ignore_msb", (uint32_t) 0);
            service::topology_change change({builder.build()});
            service::group0_command g0_cmd = client.prepare_command(std::move(change), guard,
                                                                    format("adding node {} to topology", id));
            testlog.info("Adding node {}/{} dc={} rack={} to topology", id, ip, dc_rack.dc, dc_rack.rack);
            try {
                client.add_entry(std::move(g0_cmd), std::move(guard), as).get();
                break;
            } catch (service::group0_concurrent_modification&) {
                testlog.warn("Concurrent modification detected, retrying");
            }
        }
        return id;
    }

    void set_node_state(locator::host_id id, service::node_state state) {
        abort_source as;
        auto& client = _env.get_raft_group0_client();
        while (true) {
            auto guard = client.start_operation(as).get();
            service::topology_mutation_builder builder(guard.write_timestamp());
            builder.with_node(raft::server_id(id.uuid()))
                    .set("node_state", state);
            service::topology_change change({builder.build()});
            service::group0_command g0_cmd = client.prepare_command(std::move(change), guard,
                                                                    format("node {} state={}", id, state));
            testlog.info("Changing node {} state={}", id, state);
            try {
                client.add_entry(std::move(g0_cmd), std::move(guard), as).get();
                break;
            } catch (service::group0_concurrent_modification&) {
                testlog.warn("Concurrent modification detected, retrying");
            }
        }
    }
};
