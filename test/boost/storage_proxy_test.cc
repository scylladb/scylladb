/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */


#include <seastar/core/thread.hh>
#include "test/lib/scylla_test_case.hh"
#include "query-result-writer.hh"

#include "test/lib/cql_test_env.hh"
#include "test/lib/mutation_source_test.hh"
#include "test/lib/result_set_assertions.hh"
#include "service/storage_proxy.hh"
#include "query_ranges_to_vnodes.hh"
#include "partition_slice_builder.hh"
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
    return do_with_cql_env([](cql_test_env& e) {
        return seastar::async([] {
            auto s = schema_builder("ks", "cf")
                    .with_column("pk", bytes_type, column_kind::partition_key)
                    .with_column("v", bytes_type, column_kind::regular_column)
                    .build();

            std::vector<dht::ring_position> ring = make_ring(s, 10);

            auto check = [&s](locator::token_metadata_ptr tmptr, dht::partition_range input,
                              dht::partition_range_vector expected) {
                query_ranges_to_vnodes_generator ranges_to_vnodes(tmptr, s, {input});
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
                tmptr->update_topology(gms::inet_address("10.0.0.1"), {"dc1", "rack1"});
                tmptr->update_normal_tokens(std::unordered_set<dht::token>({dht::minimum_token()}), gms::inet_address("10.0.0.1")).get();

                check(tmptr, dht::partition_range::make_singular(ring[0]), {
                        dht::partition_range::make_singular(ring[0])
                });

                check(tmptr, dht::partition_range({ring[2]}, {ring[3]}), {
                        dht::partition_range({ring[2]}, {ring[3]})
                });
            }

            {
                auto tmptr = locator::make_token_metadata_ptr(locator::token_metadata::config{});
                tmptr->update_topology(gms::inet_address("10.0.0.1"), {"dc1", "rack1"});
                tmptr->update_normal_tokens(std::unordered_set<dht::token>({ring[2].token()}), gms::inet_address("10.0.0.1")).get();
                tmptr->update_topology(gms::inet_address("10.0.0.2"), {"dc1", "rack1"});
                tmptr->update_normal_tokens(std::unordered_set<dht::token>({ring[5].token()}), gms::inet_address("10.0.0.2")).get();

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
    });
}
