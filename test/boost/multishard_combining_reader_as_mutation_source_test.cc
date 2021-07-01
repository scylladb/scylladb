/*
 * Copyright (C) 2015-present ScyllaDB
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


#include <experimental/source_location>

#include <boost/range/irange.hpp>
#include <boost/range/adaptor/uniqued.hpp>

#include <seastar/core/thread.hh>

#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>
#include "test/lib/test_services.hh"
#include "test/lib/mutation_source_test.hh"
#include "test/lib/cql_test_env.hh"
#include "test/lib/dummy_sharder.hh"
#include "test/lib/reader_lifecycle_policy.hh"
#include "test/lib/log.hh"

#include "dht/sharder.hh"
#include "mutation_reader.hh"
#include "schema_registry.hh"
#include "service/priority_manager.hh"

// Best run with SMP >= 2
SEASTAR_THREAD_TEST_CASE(test_multishard_combining_reader_as_mutation_source) {
    if (smp::count < 2) {
        std::cerr << "Cannot run test " << get_name() << " with smp::count < 2" << std::endl;
        return;
    }

    // It has to be a container that does not invalidate pointers
    std::list<dummy_sharder> keep_alive_sharder;

    do_with_cql_env_thread([&] (cql_test_env& env) -> future<> {
        auto make_populate = [&] (bool evict_paused_readers, bool single_fragment_buffer) {
            return [&, evict_paused_readers, single_fragment_buffer] (schema_ptr s, const std::vector<mutation>& mutations) mutable {
                // We need to group mutations that have the same token so they land on the same shard.
                std::map<dht::token, std::vector<frozen_mutation>> mutations_by_token;

                for (const auto& mut : mutations) {
                    mutations_by_token[mut.token()].push_back(freeze(mut));
                }

                dummy_sharder sharder(s->get_sharder(), mutations_by_token);

                auto merged_mutations = boost::copy_range<std::vector<std::vector<frozen_mutation>>>(mutations_by_token | boost::adaptors::map_values);

                auto remote_memtables = make_lw_shared<std::vector<foreign_ptr<lw_shared_ptr<memtable>>>>();
                for (unsigned shard = 0; shard < sharder.shard_count(); ++shard) {
                    auto remote_mt = smp::submit_to(shard, [shard, gs = global_schema_ptr(s), &merged_mutations, sharder] {
                        auto s = gs.get();
                        auto mt = make_lw_shared<memtable>(s);

                        for (unsigned i = shard; i < merged_mutations.size(); i += sharder.shard_count()) {
                            for (auto& mut : merged_mutations[i]) {
                                mt->apply(mut.unfreeze(s));
                            }
                        }

                        return make_foreign(mt);
                    }).get0();
                    remote_memtables->emplace_back(std::move(remote_mt));
                }
                keep_alive_sharder.push_back(sharder);

                return mutation_source([&, remote_memtables, evict_paused_readers, single_fragment_buffer] (schema_ptr s,
                        reader_permit permit,
                        const dht::partition_range& range,
                        const query::partition_slice& slice,
                        const io_priority_class& pc,
                        tracing::trace_state_ptr trace_state,
                        streamed_mutation::forwarding fwd_sm,
                        mutation_reader::forwarding fwd_mr) mutable {
                    auto factory = [remote_memtables, single_fragment_buffer] (
                            schema_ptr s,
                            reader_permit permit,
                            const dht::partition_range& range,
                            const query::partition_slice& slice,
                            const io_priority_class& pc,
                            tracing::trace_state_ptr trace_state,
                            mutation_reader::forwarding fwd_mr) {
                            auto reader = remote_memtables->at(this_shard_id())->make_flat_reader(s, std::move(permit), range, slice, pc, std::move(trace_state),
                                    streamed_mutation::forwarding::no, fwd_mr);
                            if (single_fragment_buffer) {
                                reader.set_max_buffer_size(1);
                            }
                            return reader;
                    };

                    auto lifecycle_policy = seastar::make_shared<test_reader_lifecycle_policy>(std::move(factory), evict_paused_readers);
                    auto mr = make_multishard_combining_reader_for_tests(keep_alive_sharder.back(), std::move(lifecycle_policy), s,
                            std::move(permit), range, slice, pc, trace_state, fwd_mr);
                    if (fwd_sm == streamed_mutation::forwarding::yes) {
                        return make_forwardable(std::move(mr));
                    }
                    return mr;
                });
            };
        };

        testlog.info("run_mutation_source_tests(evict_readers=false, single_fragment_buffer=false)");
        run_mutation_source_tests(make_populate(false, false));

        testlog.info("run_mutation_source_tests(evict_readers=true, single_fragment_buffer=false)");
        run_mutation_source_tests(make_populate(true, false));

        testlog.info("run_mutation_source_tests(evict_readers=true, single_fragment_buffer=true)");
        run_mutation_source_tests(make_populate(true, true));

        return make_ready_future<>();
    }).get();
}
