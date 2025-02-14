/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */


#include <seastar/core/thread.hh>

#undef SEASTAR_TESTING_MAIN
#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>
#include "test/lib/mutation_source_test.hh"
#include "test/lib/cql_test_env.hh"
#include "test/lib/dummy_sharder.hh"
#include "test/lib/reader_lifecycle_policy.hh"


#include "schema/schema_registry.hh"
#include "readers/forwardable_v2.hh"

BOOST_AUTO_TEST_SUITE(multishard_combining_reader_as_mutation_source_test)

// It has to be a container that does not invalidate pointers
static std::list<dummy_sharder> keep_alive_sharder;

class evicting_semaphore_factory : public test_reader_lifecycle_policy::semaphore_factory {
    bool _evict_paused_readers;
public:
    explicit evicting_semaphore_factory(bool evict_paused_readers) : _evict_paused_readers(evict_paused_readers) { }
    virtual lw_shared_ptr<reader_concurrency_semaphore> create(sstring name) override {
        if (!_evict_paused_readers) {
            return test_reader_lifecycle_policy::semaphore_factory::create(std::move(name));
        }
        // Create with no memory, so all inactive reads are immediately evicted.
        return make_lw_shared<reader_concurrency_semaphore>(reader_concurrency_semaphore::for_tests{}, std::move(name), 1, 0);
    }
};

static auto make_populate(bool evict_paused_readers, bool single_fragment_buffer) {
    return [evict_paused_readers, single_fragment_buffer] (schema_ptr s, const std::vector<mutation>& mutations, gc_clock::time_point) mutable {
        // We need to group mutations that have the same token so they land on the same shard.
        std::map<dht::token, std::vector<frozen_mutation>> mutations_by_token;

        for (const auto& mut : mutations) {
            mutations_by_token[mut.token()].push_back(freeze(mut));
        }

        dummy_sharder sharder(s->get_sharder(), mutations_by_token);

        auto merged_mutations = mutations_by_token | std::views::values | std::ranges::to<std::vector>();

        auto remote_memtables = make_lw_shared<std::vector<foreign_ptr<lw_shared_ptr<replica::memtable>>>>();
        for (unsigned shard = 0; shard < sharder.shard_count(); ++shard) {
            auto remote_mt = smp::submit_to(shard, [shard, gs = global_schema_ptr(s), &merged_mutations, sharder] {
                auto s = gs.get();
                auto mt = make_lw_shared<replica::memtable>(s);

                for (unsigned i = shard; i < merged_mutations.size(); i += sharder.shard_count()) {
                    for (auto& mut : merged_mutations[i]) {
                        mt->apply(mut.unfreeze(s));
                    }
                }

                return make_foreign(mt);
            }).get();
            remote_memtables->emplace_back(std::move(remote_mt));
        }
        keep_alive_sharder.push_back(sharder);

        return mutation_source([&, remote_memtables, evict_paused_readers, single_fragment_buffer] (schema_ptr s,
                reader_permit permit,
                const dht::partition_range& range,
                const query::partition_slice& slice,
                tracing::trace_state_ptr trace_state,
                streamed_mutation::forwarding fwd_sm,
                mutation_reader::forwarding fwd_mr) mutable {
            auto factory = [remote_memtables, single_fragment_buffer] (
                    schema_ptr s,
                    reader_permit permit,
                    const dht::partition_range& range,
                    const query::partition_slice& slice,
                    tracing::trace_state_ptr trace_state,
                    mutation_reader::forwarding fwd_mr) {
                    auto reader = remote_memtables->at(this_shard_id())->make_flat_reader(s, std::move(permit), range, slice, std::move(trace_state),
                            streamed_mutation::forwarding::no, fwd_mr);
                    if (single_fragment_buffer) {
                        reader.set_max_buffer_size(1);
                    }
                    return reader;
            };

            auto lifecycle_policy = seastar::make_shared<test_reader_lifecycle_policy>(std::move(factory), std::make_unique<evicting_semaphore_factory>(evict_paused_readers));
            auto mr = make_multishard_combining_reader_v2_for_tests(keep_alive_sharder.back(), std::move(lifecycle_policy), s,
                    std::move(permit), range, slice, trace_state, fwd_mr);
            if (fwd_sm == streamed_mutation::forwarding::yes) {
                return make_forwardable(std::move(mr));
            }
            return mr;
        });
    };
}

// Best run with SMP >= 2
SEASTAR_THREAD_TEST_CASE(test_multishard_combining_reader) {
    if (smp::count < 2) {
        std::cerr << "Cannot run test " << get_name() << " with smp::count < 2" << std::endl;
        return;
    }

    do_with_cql_env_thread([&] (cql_test_env& env) -> future<> {
        run_mutation_source_tests(make_populate(false, false));
        return make_ready_future<>();
    }).get();
}

SEASTAR_THREAD_TEST_CASE(test_multishard_combining_reader_evict_paused) {
    if (smp::count < 2) {
        std::cerr << "Cannot run test " << get_name() << " with smp::count < 2" << std::endl;
        return;
    }

    do_with_cql_env_thread([&] (cql_test_env& env) -> future<> {
        run_mutation_source_tests(make_populate(true, false));
        return make_ready_future<>();
    }).get();
}

// Single fragment buffer tests are extremely slow, so the
// run_mutation_source_tests execution is split

SEASTAR_THREAD_TEST_CASE(test_multishard_combining_reader_with_tiny_buffer) {
    if (smp::count < 2) {
        std::cerr << "Cannot run test " << get_name() << " with smp::count < 2" << std::endl;
        return;
    }

    do_with_cql_env_thread([&] (cql_test_env& env) -> future<> {
        run_mutation_source_tests_plain(make_populate(true, true), true);
        return make_ready_future<>();
    }).get();
}

SEASTAR_THREAD_TEST_CASE(test_multishard_combining_reader_with_tiny_buffer_reverse) {
    if (smp::count < 2) {
        std::cerr << "Cannot run test " << get_name() << " with smp::count < 2" << std::endl;
        return;
    }

    do_with_cql_env_thread([&] (cql_test_env& env) -> future<> {
        run_mutation_source_tests_reverse(make_populate(true, true), true);
        return make_ready_future<>();
    }).get();
}

BOOST_AUTO_TEST_SUITE_END()
