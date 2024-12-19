/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
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
#include "test/lib/topology.hh"
#include "test/lib/log.hh"
#include "test/lib/random_utils.hh"

struct sort_by_proximity_topology {
    static constexpr size_t NODES = 15;
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

        std::unordered_map<sstring, size_t> datacenters = {
            { "dc1", 5 },
            { "dc2", 5 },
            { "dc3", 5 },
        };

        stm->mutate_token_metadata_for_test([&] (locator::token_metadata& tm) {
            auto seed = tests::random::get_int<locator::topology::random_engine_type::result_type>();
            auto& topology = tm.get_topology();
            topology.seed_random_engine(seed);
            tests::generate_topology(topology, datacenters, nodes);
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
