/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

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
    static constexpr size_t NODES_PER_RACK = 1;
    static constexpr size_t NODES = DCS * RACKS_PER_DC * NODES_PER_RACK;
    semaphore sem{1};
    std::optional<locator::shared_token_metadata> stm;
    host_id_vector_replica_set nodes;
    size_t iter_idx = 0;

    sort_by_proximity_topology()
    {
        nodes.reserve(NODES);
        std::generate_n(std::back_inserter(nodes), NODES, [i = 0u]() mutable {
            return locator::host_id{utils::UUID(0, ++i)};
        });

        locator::token_metadata::config tm_cfg;
        gms::inet_address my_address("localhost");
        tm_cfg.topo_cfg.this_endpoint = my_address;
        tm_cfg.topo_cfg.this_cql_address = my_address;
        tm_cfg.topo_cfg.this_host_id = nodes[0];
        tm_cfg.topo_cfg.local_dc_rack = locator::endpoint_dc_rack::default_location;

        stm.emplace([this] () noexcept { return get_units(sem, 1); }, tm_cfg);

        unsigned i = 0;
        stm->mutate_token_metadata_for_test([&] (locator::token_metadata& tm) {
            auto& topology = tm.get_topology();
            for (size_t dc = 1; dc <= DCS; ++dc) {
                for (size_t rack = 1; rack <= RACKS_PER_DC; ++rack) {
                    for (size_t node = 1; node <= NODES_PER_RACK; ++node, ++i) {
                        topology.add_or_update_endpoint(nodes[i],
                                gms::inet_address((127u << 24) | i + 1),
                                locator::endpoint_dc_rack{format("dc{}", dc), format("rack{}", rack)},
                                locator::node::state::normal);
                    }
                }
            }
            auto seed = tests::random::get_int<locator::topology::random_engine_type::result_type>();
            topology.seed_random_engine(seed);
        });
    }
};

PERF_TEST_F(sort_by_proximity_topology, perf_sort_by_proximity)
{
    const auto& topology = stm->get()->get_topology();
    topology.do_sort_by_proximity(nodes[iter_idx], nodes);
    if (++iter_idx >= NODES) {
        iter_idx = 0;
    }
}
