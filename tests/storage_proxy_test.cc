/*
 * Copyright (C) 2015 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */


#include <seastar/core/thread.hh>
#include <seastar/tests/test-utils.hh>
#include "query-result-writer.hh"

#include "tests/cql_test_env.hh"
#include "tests/mutation_source_test.hh"
#include "tests/result_set_assertions.hh"
#include "service/storage_proxy.hh"
#include "partition_slice_builder.hh"
#include "schema_builder.hh"

// Returns random keys sorted in ring order.
// The schema must have a single bytes_type partition key column.
static std::vector<dht::ring_position> make_ring(schema_ptr s, int n_keys) {
    std::vector<dht::ring_position> ring;
    for (int i = 0; i < 10; ++i) {
        auto pk = partition_key::from_single_value(*s, to_bytes(sprint("key%d", i)));
        ring.emplace_back(dht::global_partitioner().decorate_key(*s, pk));
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

            auto check = [&s](locator::token_metadata& tm, dht::partition_range input,
                              dht::partition_range_vector expected) {
                auto actual = service::get_restricted_ranges(tm, *s, input);
                if (!std::equal(actual.begin(), actual.end(), expected.begin(), [&s](auto&& r1, auto&& r2) {
                    return r1.equal(r2, dht::ring_position_comparator(*s));
                })) {
                    BOOST_FAIL(sprint("Ranges differ, expected %s but got %s", expected, actual));
                }
            };

            {
                // Ring with minimum token
                locator::token_metadata tm;
                tm.update_normal_token(dht::minimum_token(), {"10.0.0.1"});

                check(tm, dht::partition_range::make_singular(ring[0]), {
                        dht::partition_range::make_singular(ring[0])
                });

                check(tm, dht::partition_range({ring[2]}, {ring[3]}), {
                        dht::partition_range({ring[2]}, {ring[3]})
                });
            }

            {
                locator::token_metadata tm;
                tm.update_normal_token(ring[2].token(), {"10.0.0.1"});
                tm.update_normal_token(ring[5].token(), {"10.0.0.2"});

                check(tm, dht::partition_range::make_singular(ring[0]), {
                        dht::partition_range::make_singular(ring[0])
                });

                check(tm, dht::partition_range::make_singular(ring[2]), {
                        dht::partition_range::make_singular(ring[2])
                });

                check(tm, dht::partition_range({{dht::ring_position::ending_at(ring[2].token()), false}}, {ring[3]}), {
                        dht::partition_range({{dht::ring_position::ending_at(ring[2].token()), false}}, {ring[3]})
                });

                check(tm, dht::partition_range({ring[3]}, {ring[4]}), {
                    dht::partition_range({ring[3]}, {ring[4]})
                });

                check(tm, dht::partition_range({ring[2]}, {ring[3]}), {
                    dht::partition_range({ring[2]}, {dht::ring_position::ending_at(ring[2].token())}),
                    dht::partition_range({{dht::ring_position::ending_at(ring[2].token()), false}}, {ring[3]})
                });

                check(tm, dht::partition_range({{ring[2], false}}, {ring[3]}), {
                    dht::partition_range({{ring[2], false}}, {dht::ring_position::ending_at(ring[2].token())}),
                    dht::partition_range({{dht::ring_position::ending_at(ring[2].token()), false}}, {ring[3]})
                });
            }
        });
    });
}
