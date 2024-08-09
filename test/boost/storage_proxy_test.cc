/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */


#include <fmt/ranges.h>
#include <seastar/core/thread.hh>
#include "test/lib/scylla_test_case.hh"

#include "test/lib/cql_test_env.hh"
#include "service/storage_proxy.hh"
#include "query_ranges_to_vnodes.hh"
#include "schema/schema_builder.hh"

// Returns random keys sorted in ring order.
// The schema must have a single bytes_type partition key column.
static std::vector<dht::ring_position> make_ring(schema_ptr s, int n_keys) {
    std::vector<dht::ring_position> ring;
    for (int i = 0; i < 10; ++i) {
        auto pk = partition_key::from_single_value(*s, to_bytes(format("key{:d}", i)));
        ring.emplace_back(dht::decorate_key(*s, pk));
    }
    std::sort(ring.begin(), ring.end(), dht::ring_position_less_comparator(*s));
    return ring;
}

SEASTAR_TEST_CASE(test_get_restricted_ranges) {
    return do_with_cql_env_thread([](cql_test_env& e) {
        auto s = schema_builder("ks", "cf")
                .with_column("pk", bytes_type, column_kind::partition_key)
                .with_column("v", bytes_type, column_kind::regular_column)
                .build();

        std::vector<dht::ring_position> ring = make_ring(s, 10);

        auto check = [&s](locator::token_metadata_ptr tmptr, dht::partition_range input,
                          dht::partition_range_vector expected) {
            query_ranges_to_vnodes_generator ranges_to_vnodes(locator::make_splitter(tmptr), s, {input});
            auto actual = ranges_to_vnodes(1000);
            if (!std::equal(actual.begin(), actual.end(), expected.begin(), [&s](auto&& r1, auto&& r2) {
                return r1.equal(r2, dht::ring_position_comparator(*s));
            })) {
                BOOST_FAIL(format("Ranges differ, expected {} but got {}", expected, actual));
            }
        };

        {
            // Ring with minimum token
            auto tmptr = locator::make_token_metadata_ptr(locator::token_metadata::config{});
            const auto host_id = locator::host_id{utils::UUID(0, 1)};
            tmptr->update_topology(host_id, locator::endpoint_dc_rack{"dc1", "rack1"}, locator::node::state::normal);
            tmptr->update_normal_tokens(std::unordered_set<dht::token>({dht::minimum_token()}), host_id).get();

            check(tmptr, dht::partition_range::make_singular(ring[0]), {
                    dht::partition_range::make_singular(ring[0])
            });

            check(tmptr, dht::partition_range({ring[2]}, {ring[3]}), {
                    dht::partition_range({ring[2]}, {ring[3]})
            });
        }

        {
            auto tmptr = locator::make_token_metadata_ptr(locator::token_metadata::config{});
            const auto id1 = locator::host_id{utils::UUID(0, 1)};
            const auto id2 = locator::host_id{utils::UUID(0, 2)};
            tmptr->update_topology(id1, locator::endpoint_dc_rack{"dc1", "rack1"}, locator::node::state::normal);
            tmptr->update_normal_tokens(std::unordered_set<dht::token>({ring[2].token()}), id1).get();
            tmptr->update_topology(id2, locator::endpoint_dc_rack{"dc1", "rack1"}, locator::node::state::normal);
            tmptr->update_normal_tokens(std::unordered_set<dht::token>({ring[5].token()}), id2).get();

            check(tmptr, dht::partition_range::make_singular(ring[0]), {
                    dht::partition_range::make_singular(ring[0])
            });

            check(tmptr, dht::partition_range::make_singular(ring[2]), {
                    dht::partition_range::make_singular(ring[2])
            });

            check(tmptr, dht::partition_range({{dht::ring_position::ending_at(ring[2].token()), false}}, {ring[3]}), {
                    dht::partition_range({{dht::ring_position::ending_at(ring[2].token()), false}}, {ring[3]})
            });

            check(tmptr, dht::partition_range({ring[3]}, {ring[4]}), {
                dht::partition_range({ring[3]}, {ring[4]})
            });

            check(tmptr, dht::partition_range({ring[2]}, {ring[3]}), {
                dht::partition_range({ring[2]}, {dht::ring_position::ending_at(ring[2].token())}),
                dht::partition_range({{dht::ring_position::ending_at(ring[2].token()), false}}, {ring[3]})
            });

            check(tmptr, dht::partition_range({{ring[2], false}}, {ring[3]}), {
                dht::partition_range({{ring[2], false}}, {dht::ring_position::ending_at(ring[2].token())}),
                dht::partition_range({{dht::ring_position::ending_at(ring[2].token()), false}}, {ring[3]})
            });
        }
    });
}

SEASTAR_THREAD_TEST_CASE(test_split_stats) {
    auto ep1 = gms::inet_address("127.0.0.1");
    auto sg1 = create_scheduling_group("apa1", 100).get();
    auto sg2 = create_scheduling_group("apa2", 100).get();

    std::optional<service::storage_proxy_stats::split_stats> stats1, stats2;

    // pretending to be abstract_write_response_handler type. 
    // created in various scheduling groups, in which they 
    // instantiate group-local split_stats.
    with_scheduling_group(sg1, [&] {
        stats1.emplace("tuta", "nils", "en nils", "nilsa", true);
    }).get();

    with_scheduling_group(sg2, [&] {
        stats2.emplace("tuta", "nils", "en nils", "nilsa", true);
    }).get();

    // simulating the calling of storage_proxy::on_down, from gossip
    // on node dropping out. If inside a write operation, we'll pick up
    // write handlers and to "timeout_cb" on them, which in turn might
    // call get_ep_stat, which eventually calls register_metrics for 
    // the DC written to.
    // Point being is that either the above should not happen, or 
    // split_stats should be resilient to being called from different
    // scheduling group.
    stats1->register_metrics_for("DC1", ep1);
    stats2->register_metrics_for("DC1", ep1);
}
