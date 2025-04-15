/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <algorithm>
#include <seastar/core/reactor.hh>
#include <seastar/core/app-template.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/thread.hh>
#include <seastar/rpc/rpc_types.hh>
#include "inet_address_vectors.hh"
#include "seastarx.hh"

#include <seastar/testing/perf_tests.hh>
#include <seastar/testing/test_runner.hh>

#include "locator/token_metadata.hh"
#include "test/lib/log.hh"
#include "test/lib/random_utils.hh"

struct sort_by_proximity_topology {
    static constexpr size_t DCS = 1;
    static constexpr size_t RACKS_PER_DC = 3;
    static constexpr size_t NODES_PER_RACK = 3;
    static constexpr size_t NODES = DCS * RACKS_PER_DC * NODES_PER_RACK;
    semaphore sem{1};
    std::optional<locator::shared_token_metadata> stm;
    std::unordered_map<size_t, std::unordered_map<size_t, host_id_vector_replica_set>> nodes;
    std::vector<host_id_vector_replica_set> replica_sets;

    sort_by_proximity_topology()
    {
        locator::token_metadata::config tm_cfg;
        gms::inet_address my_address("localhost");
        tm_cfg.topo_cfg.this_endpoint = my_address;
        tm_cfg.topo_cfg.this_cql_address = my_address;
        tm_cfg.topo_cfg.this_host_id = locator::host_id{utils::UUID(0, 1)};
        tm_cfg.topo_cfg.local_dc_rack = locator::endpoint_dc_rack::default_location;

        stm.emplace([this] () noexcept { return get_units(sem, 1); }, tm_cfg);

        unsigned i = 1;
        stm->mutate_token_metadata_for_test([&] (locator::token_metadata& tm) {
            auto& topology = tm.get_topology();
            for (size_t dc = 0; dc < DCS; ++dc) {
                for (size_t rack = 0; rack < RACKS_PER_DC; ++rack) {
                    for (size_t node = 0; node < NODES_PER_RACK; ++node, ++i) {
                        auto id = locator::host_id{utils::UUID(0, i)};
                        nodes[dc][rack].emplace_back(id);
                        topology.add_or_update_endpoint(id,
                                locator::endpoint_dc_rack{format("dc{}", dc), format("rack{}", rack)},
                                locator::node::state::normal);
                    }
                }
            }
            auto seed = tests::random::get_int<locator::topology::random_engine_type::result_type>();
            topology.seed_random_engine(seed);
        });
        auto num_replica_sets = std::pow(NODES_PER_RACK, RACKS_PER_DC);
        replica_sets.reserve(num_replica_sets);
        std::array<size_t, RACKS_PER_DC> node_in_dc;
        std::ranges::fill(node_in_dc, 0);
        std::array<size_t, RACKS_PER_DC> end_node_in_dc;
        std::ranges::fill(end_node_in_dc, 0);
        do {
            host_id_vector_replica_set replicas;
            replicas.reserve(DCS * RACKS_PER_DC);
            for (size_t dc = 0; dc < DCS; ++dc) {
                for (size_t rack = 0; rack < RACKS_PER_DC; ++rack) {
                    replicas.emplace_back(nodes[dc][rack][node_in_dc[rack]]);
                }
            }
            replica_sets.emplace_back(std::move(replicas));
            for (size_t rack = 0; rack < RACKS_PER_DC; ++rack) {
                if (++node_in_dc[rack] < NODES_PER_RACK) {
                    break;
                }
                node_in_dc[rack] = 0;
            }
        } while (node_in_dc != end_node_in_dc);
        assert(replica_sets.size() == num_replica_sets);
    }
};

PERF_TEST_F(sort_by_proximity_topology, perf_sort_by_proximity)
{
    const auto& topology = stm->get()->get_topology();
    size_t iterations = 0;
    for (const auto& [dc, racks] : nodes) {
        for (const auto& [rack, rack_nodes] : racks) {
            for (auto node : rack_nodes) {
                for (auto& replicas : replica_sets) {
                    topology.do_sort_by_proximity(node, replicas);
                    iterations++;
                }
            }
        }
    }
    return iterations;
}
