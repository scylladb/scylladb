/*
 * Copyright (C) 2018 ScyllaDB
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
#include <seastar/util/bool_class.hh>

#include "mutation_fragment.hh"
#include "mutation_source_test.hh"
#include "flat_mutation_reader.hh"
#include "multishard_writer.hh"
#include "tests/cql_test_env.hh"

struct generate_error_tag { };
using generate_error = bool_class<generate_error_tag>;


constexpr unsigned many_partitions() {
    return
#ifndef SEASTAR_DEBUG
	300
#else
	10
#endif
	;
}

SEASTAR_TEST_CASE(test_multishard_writer) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        auto test_random_streams = [] (random_mutation_generator&& gen, size_t partition_nr, generate_error error = generate_error::no) {
            for (auto i = 0; i < 3; i++) {
                auto muts = gen(partition_nr);
                std::vector<size_t> shards_before(smp::count, 0);
                std::vector<size_t> shards_after(smp::count, 0);

                for (auto& m : muts) {
                    auto shard = dht::global_partitioner().shard_of(m.token());
                    shards_before[shard]++;
                }
                schema_ptr s = gen.schema();
                auto source_reader = partition_nr > 0 ? flat_mutation_reader_from_mutations(muts) : make_empty_flat_reader(s);
                size_t partitions_received = distribute_reader_and_consume_on_shards(s,
                    dht::global_partitioner(),
                    std::move(source_reader),
                    [&shards_after, error] (flat_mutation_reader reader) mutable {
                        if (error) {
                            return make_exception_future<>(std::runtime_error("Failed to write"));
                        }
                        return repeat([&shards_after, reader = std::move(reader), error] () mutable {
                            return reader(db::no_timeout).then([&shards_after, error] (mutation_fragment_opt mf_opt) mutable {
                                if (mf_opt) {
                                    if (mf_opt->is_partition_start()) {
                                        auto shard = dht::global_partitioner().shard_of(mf_opt->as_partition_start().key().token());
                                        BOOST_REQUIRE_EQUAL(shard, engine().cpu_id());
                                        shards_after[shard]++;
                                    }
                                    return make_ready_future<stop_iteration>(stop_iteration::no);
                                } else {
                                    return make_ready_future<stop_iteration>(stop_iteration::yes);
                                }
                            });
                        });
                    }
                ).get0();
                BOOST_REQUIRE_EQUAL(partitions_received, partition_nr);
                BOOST_REQUIRE_EQUAL(shards_after, shards_before);
            }
        };

        test_random_streams(random_mutation_generator(random_mutation_generator::generate_counters::no, local_shard_only::no), 0);
        test_random_streams(random_mutation_generator(random_mutation_generator::generate_counters::yes, local_shard_only::no), 0);

        test_random_streams(random_mutation_generator(random_mutation_generator::generate_counters::no, local_shard_only::no), 1);
        test_random_streams(random_mutation_generator(random_mutation_generator::generate_counters::yes, local_shard_only::no), 1);

        test_random_streams(random_mutation_generator(random_mutation_generator::generate_counters::no, local_shard_only::no), many_partitions());
        test_random_streams(random_mutation_generator(random_mutation_generator::generate_counters::yes, local_shard_only::no), many_partitions());

        try {
            test_random_streams(random_mutation_generator(random_mutation_generator::generate_counters::no, local_shard_only::no), many_partitions(), generate_error::yes);
            BOOST_ASSERT(false);
        } catch (...) {
        }

        try {
            test_random_streams(random_mutation_generator(random_mutation_generator::generate_counters::yes, local_shard_only::no), many_partitions(), generate_error::yes);
            BOOST_ASSERT(false);
        } catch (...) {
        }
    });
}
