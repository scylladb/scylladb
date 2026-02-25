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
#include "service/tablet_allocator_fwd.hh"
#include "locator/host_id.hh"
#include "locator/tablets.hh"
#include "test/lib/log.hh"
#include "version.hh"

#include <atomic>

struct shared_load_stats {
    locator::load_stats stats;

    locator::load_stats_ptr get() const {
        return make_lw_shared(stats);
    }

    void set_size(table_id table, size_t size_in_bytes) {
        stats.tables[table].size_in_bytes = size_in_bytes;
    }

    void set_split_ready_seq_number(table_id table, size_t seq_number) {
        stats.tables[table].split_ready_seq_number = seq_number;
    }

    void set_capacity(locator::host_id host, size_t capacity) {
        stats.capacity[host] = capacity;
        stats.tablet_stats[host].effective_capacity = capacity;
    }

    void set_tablet_size(locator::host_id host, const locator::range_based_tablet_id& rb_tid, uint64_t tablet_size) {
        stats.tablet_stats[host].tablet_sizes[rb_tid.table][rb_tid.range] = tablet_size;
    }

    void set_default_tablet_sizes(locator::token_metadata_ptr tmptr) {
        for (auto&& [table, tmap_ptr] : tmptr->tablets().all_tables_ungrouped()) {
            tmap_ptr->for_each_tablet([&] (locator::tablet_id tid, const locator::tablet_info& tinfo) -> future<> {
                locator::range_based_tablet_id rb_tid {table, tmap_ptr->get_token_range(tid)};
                for (auto& replica : tinfo.replicas) {
                    if (!stats.get_tablet_size(replica.host, rb_tid)) {
                        stats.tablet_stats[replica.host].tablet_sizes[table][rb_tid.range] = service::default_target_tablet_size;
                    }
                }
                return make_ready_future<>();
            }).get();
        }
    }
};

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
    int _dc_id;
    int _rack_id;
    sstring _dc;
    sstring _rack;
    shared_load_stats _load_stats;
    std::vector<locator::host_id> _hosts;
    std::unordered_map<locator::host_id, gms::inet_address> _host_addresses;
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
        _rack = fmt::format("rack{}{:c}", _dc_id, 'a' + _rack_id++);
        return rack();
    }

    // Starts building a new rack in the current DC.
    // Returns location of the new rack.
    endpoint_dc_rack start_new_rack(sstring rack_name) {
        _rack = std::move(rack_name);
        return rack();
    }

    // Starts building a new DC.
    // DC is named uniquely in the scope of the process, not just this object.
    endpoint_dc_rack start_new_dc() {
        static std::atomic<int> next_id = 1;
        _dc_id = next_id.fetch_add(1);
        _dc = fmt::format("dc{}", _dc_id);
        _rack_id = 0;
        return start_new_rack();
    }

    // Starts building a new DC.
    endpoint_dc_rack start_new_dc(endpoint_dc_rack dc_and_rack) {
        _dc = dc_and_rack.dc;
        _rack = dc_and_rack.rack;
        return rack();
    }

    locator::load_stats_ptr get_load_stats() const {
        return _load_stats.get();
    }

    shared_load_stats& get_shared_load_stats() {
        return _load_stats;
    }

    /// Returns total cluster's storage capacity in bytes.
    uint64_t get_capacity() const {
        uint64_t cap = 0;
        auto stats = get_load_stats();
        for (auto h : _hosts) {
            cap += stats->capacity.at(h);
        }
        return cap;
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
        _load_stats.set_capacity(id, service::default_target_tablet_size * shard_count);

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
        _hosts.push_back(id);
        _host_addresses.emplace(id, ip);
        return id;
    }

    locator::host_id add_i4i_2xlarge(endpoint_dc_rack rack) {
        auto h = add_node(service::node_state::normal, 7, rack);
        get_shared_load_stats().set_capacity(h, 1'875'000'000'000);
        return h;
    }

    locator::host_id add_i4i_large(endpoint_dc_rack rack) {
        auto h = add_node(service::node_state::normal, 2, rack);
        get_shared_load_stats().set_capacity(h, 468'000'000'000);
        return h;
    }

    void modify_group0(std::function<void(service::group0_guard&, utils::chunked_vector<canonical_mutation>&)> func) {
        abort_source as;
        auto& client = _env.get_raft_group0_client();
        while (true) {
            auto guard = client.start_operation(as).get();
            utils::chunked_vector<canonical_mutation> muts;
            func(guard, muts);
            service::topology_change change({std::move(muts)});
            service::group0_command g0_cmd = client.prepare_command(std::move(change), guard, "modify_topology()");
            try {
                client.add_entry(std::move(g0_cmd), std::move(guard), as).get();
                break;
            } catch (service::group0_concurrent_modification&) {
                testlog.warn("Concurrent modification detected, retrying");
            }
        }
    }

    void modify_topology(std::function<void(service::topology_mutation_builder&)> func) {
        modify_group0([&] (service::group0_guard& guard, utils::chunked_vector<canonical_mutation>& muts) {
            service::topology_mutation_builder builder(guard.write_timestamp());
            func(builder);
            muts.emplace_back(builder.build());
        });
    }

    void pause_rf_change_request(utils::UUID new_elem) {
        abort_source as;
        auto& client = _env.get_raft_group0_client();
        while (true) {
            auto guard = client.start_operation(as).get();
            service::topology_mutation_builder builder(guard.write_timestamp());
            builder.pause_rf_change_request(new_elem);
            service::topology_change change({builder.build()});
            service::group0_command g0_cmd = client.prepare_command(std::move(change), guard,
                                                                    "setting ongoing RF change data");
            try {
                client.add_entry(std::move(g0_cmd), std::move(guard), as).get();
                break;
            } catch (service::group0_concurrent_modification&) {
                testlog.warn("Concurrent modification detected, retrying");
            }
        }
    }

    void resume_rf_change_request(const std::unordered_set<utils::UUID>& current_queue, utils::UUID elem_to_remove) {
        abort_source as;
        auto& client = _env.get_raft_group0_client();
        while (true) {
            auto guard = client.start_operation(as).get();
            service::topology_mutation_builder builder(guard.write_timestamp());
            builder.resume_rf_change_request(current_queue, elem_to_remove);
            service::topology_change change({builder.build()});
            service::group0_command g0_cmd = client.prepare_command(std::move(change), guard,
                                                                    "setting ongoing RF change data");
            try {
                client.add_entry(std::move(g0_cmd), std::move(guard), as).get();
                break;
            } catch (service::group0_concurrent_modification&) {
                testlog.warn("Concurrent modification detected, retrying");
            }
        }
    }

    void set_node_state(locator::host_id id, service::node_state state) {
        modify_topology([&](service::topology_mutation_builder& builder) {
            testlog.info("Changing node {} state={}", id, state);
            builder.with_node(raft::server_id(id.uuid()))
                   .set("node_state", state);
        });
    }

    void add_draining_request(locator::host_id id) {
        modify_group0([&](service::group0_guard& guard, utils::chunked_vector<canonical_mutation>& muts) {
            auto& topo = _env.local_db().get_shared_token_metadata().get()->get_topology();
            auto req = topo.get_node(id).is_excluded() ? service::topology_request::remove : service::topology_request::leave;

            service::topology_mutation_builder builder(guard.write_timestamp());
            builder.with_node(raft::server_id(id.uuid()))
                    .set("topology_request", req)
                    .set("request_id", guard.new_group0_state_id());
            muts.emplace_back(builder.build());

            service::topology_request_tracking_mutation_builder rtbuilder(guard.new_group0_state_id(),
                                                                 _env.local_db().features().topology_requests_type_column);
            rtbuilder.set("initiating_host", raft::server_id(topo.my_host_id().uuid()))
                    .set("done", false);
            rtbuilder.set("request_type", req);
            muts.emplace_back(rtbuilder.build());

            testlog.info("Adding {} request for node {}", req, id);
        });
    }

    const std::vector<locator::host_id>& hosts() const {
        return _hosts;
    }

    const std::unordered_map<locator::host_id, gms::inet_address>& host_addresses() const {
        return _host_addresses;
    }
};
