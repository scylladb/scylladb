/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "replica/memtable.hh"
#include "readers/from_fragments_v2.hh"
#include "readers/upgrading_consumer.hh"
#include "repair/hash.hh"
#include "repair/row.hh"
#include "repair/writer.hh"
#include "repair/reader.hh"
#include "repair/row_level.hh"
#include "test/lib/mutation_source_test.hh"
#include "test/lib/random_utils.hh"
#include "test/lib/cql_test_env.hh"
#include "service/storage_proxy.hh"
#include "test/lib/reader_concurrency_semaphore.hh"
#include "test/lib/scylla_test_case.hh"
#include "readers/mutation_fragment_v1_stream.hh"

// Helper mutation_fragment_queue that stores the received stream of
// mutation_fragments in a passed in deque of mutation_fragment_v2.
// This allows easy reader construction to verify what was sent to the queue
class test_mutation_fragment_queue_impl : public mutation_fragment_queue::impl, enable_lw_shared_from_this<test_mutation_fragment_queue_impl> {
    std::deque<mutation_fragment_v2>& _fragments;
public:
    test_mutation_fragment_queue_impl(std::deque<mutation_fragment_v2>& fragments)
        : mutation_fragment_queue::impl()
        , _fragments(fragments)
    {}

    virtual future<> push(mutation_fragment_v2 mf) override {
        _fragments.push_back(std::move(mf));
        return make_ready_future();
    }

    virtual void abort(std::exception_ptr ep) override {}

    virtual void push_end_of_stream() override {}
};

mutation_fragment_queue make_test_mutation_fragment_queue(schema_ptr s, reader_permit permit, std::deque<mutation_fragment_v2>& fragments) {
    return mutation_fragment_queue(std::move(s), std::move(permit), seastar::make_shared<test_mutation_fragment_queue_impl>(fragments));
}

// repair_writer::impl abstracts away underlying writer that will receive
// mutation fragments sent to the underlying queue. This implementation
// receives the queue as its dependency and delegates all related work
// to the queue.
class test_repair_writer_impl : public repair_writer::impl {
    mutation_fragment_queue _queue;
public:
    test_repair_writer_impl(mutation_fragment_queue queue)
        : _queue(std::move(queue))
    {}

    virtual future<> wait_for_writer_done() override {
        return make_ready_future();
    }

    virtual mutation_fragment_queue& queue() override {
        return _queue;
    }

    virtual void create_writer(lw_shared_ptr<repair_writer> writer) override {
        // Empty implementation. The queue received in constructor
        // should already contain a fully initialised consumer.
    }
};

// Creates a helper repair_writer object that will fill in the passed in
// deque of fragments as it receives repair_rows during flush
lw_shared_ptr<repair_writer> make_test_repair_writer(schema_ptr schema, reader_permit permit, std::deque<mutation_fragment_v2>& fragments) {
    mutation_fragment_queue mq = make_test_mutation_fragment_queue(schema, permit, fragments);
    return make_lw_shared<repair_writer>(std::move(schema), std::move(permit), std::make_unique<test_repair_writer_impl>(std::move(mq)));
}

repair_rows_on_wire make_random_repair_rows_on_wire(random_mutation_generator& gen, schema_ptr s, reader_permit permit, lw_shared_ptr<replica::memtable> m) {
    repair_rows_on_wire input;
    std::vector<mutation> muts = gen(100);

    for (mutation& mut : muts) {
        partition_key pk = mut.key();
        auto m2 = make_lw_shared<replica::memtable>(s);
        m->apply(mut);
        m2->apply(mut);
        auto reader = mutation_fragment_v1_stream(m2->make_flat_reader(s, permit));
        std::list<frozen_mutation_fragment> mfs;
        reader.consume_pausable([s, &mfs](mutation_fragment mf) {
            if ((mf.is_partition_start() && !mf.as_partition_start().partition_tombstone()) || mf.is_end_of_partition()) {
                // Stream of mutations coming from the wire doesn't contain partition_end
                // fragments. partition_start can be sent only if it contains a tombstone.
                return stop_iteration::no;
            }

            mfs.push_back(freeze(*s, std::move(mf)));
            return stop_iteration::no;
        }).get();
        input.push_back(partition_key_and_mutation_fragments(pk, std::move(mfs)));
    }
    return input;
};

SEASTAR_TEST_CASE(flush_repair_rows_on_wire_to_sstable) {
    // The basic premise of repairing is applying missing mutations from other nodes
    // to the current one and vice versa. The missing mutations are passed on the
    // wire in the form of repair_rows_on_wire objects.
    //
    // This test exercises the path of receiving rows on wire and flushing them
    // to disk. repair_rows_on_wire is optimised for wire transfer and not for
    // internal manipulation of the data and writing to disk, so they are converted
    // to a friendlier representation of std::list<repair_row>. Such list is then
    // flushed to disk.
    //
    // The test generates a random stream of mutations, converts them to repair_rows_on_wire,
    // converts them to std::list<repair_row> and verifies that if they were flushed
    // to disk, they would produce the original stream.
    return seastar::async([&] {
        tests::reader_concurrency_semaphore_wrapper semaphore;
        reader_permit permit = semaphore.make_permit();
        random_mutation_generator gen{random_mutation_generator::generate_counters::no};
        schema_ptr s = gen.schema();
        auto m = make_lw_shared<replica::memtable>(s);
        repair_rows_on_wire input = make_random_repair_rows_on_wire(gen, s, permit, m);
        std::deque<mutation_fragment_v2> fragments;
        lw_shared_ptr<repair_writer> writer = make_test_repair_writer(s, permit, fragments);
        uint64_t seed = tests::random::get_int<uint64_t>();
        std::list<repair_row> repair_rows = to_repair_rows_list(std::move(input), s, seed, repair_master::yes, permit, repair_hasher(seed, s)).get();
        flush_rows(s, repair_rows, writer);
        writer->wait_for_writer_done().get();
        compare_readers(*s, m->make_flat_reader(s, permit), make_flat_mutation_reader_from_fragments(s, permit, std::move(fragments)));
    });
}

SEASTAR_TEST_CASE(test_reader_with_different_strategies) {
    // The test generates random mutations and persists them into the database.
    // It then tries to read them back with different repair_reader read_strategies.
    // Two cases are considered - when remote_sharder is different from
    // the local one and when it's the same.
    // In the first case we compare the output of multishard_split and multishard_filter
    // readers, in the second case we exercise the local read strategy and
    // compare its output to both multishard_split and multishard_filter.

    return do_with_cql_env([] (cql_test_env& e) -> future<> {
        random_mutation_generator gen{random_mutation_generator::generate_counters::no, local_shard_only::no};
        co_await e.db().invoke_on_all([gs = global_schema_ptr(gen.schema())](replica::database& db) -> future<> {
            co_await db.add_column_family_and_make_directory(gs.get());
            db.find_column_family(gs.get()).mark_ready_for_writes();
        });
        auto& cf = e.local_db().find_column_family(gen.schema());
        const auto& local_sharder = cf.schema()->get_sharder();

        const auto token_max = std::numeric_limits<int64_t>::max();
        const auto token_min = std::numeric_limits<int64_t>::min();
        auto min_token = token_max;
        auto max_token = token_min;
        {
            auto mutations = gen(100);
            for (const auto& m: mutations) {
                const auto t = m.token().raw();
                min_token = std::min(min_token, t);
                max_token = std::max(max_token, t);
            }
            auto& storage_proxy = e.get_storage_proxy().local();
            co_await storage_proxy.mutate_locally(std::move(mutations), tracing::trace_state_ptr());
        }

        auto do_check = [&](const dht::sharder& remote_sharder,
            repair_reader::read_strategy strategy1, repair_reader::read_strategy strategy2) -> future<>
        {
            const auto& s = *cf.schema();
            // local strategy can read only from the current shard
            const auto remote_shard = strategy1 == repair_reader::read_strategy::local || strategy2 == repair_reader::read_strategy::local
                ? this_shard_id()
                : tests::random::get_int(remote_sharder.shard_count() - 1);
            const auto random_range = std::invoke([&] {
                const auto left = tests::random::get_int(token_min, max_token);
                const auto right = tests::random::get_int(left == token_max ? token_max : left + 1, token_max);
                return dht::token_range::make(
                    {dht::token::from_int64(left), tests::random::with_probability(0.5)},
                    {dht::token::from_int64(right), tests::random::with_probability(0.5)});
            });
            auto read_all = [&](repair_reader::read_strategy strategy) -> future<std::vector<mutation_fragment>> {
                auto reader = repair_reader(e.db(), cf, cf.schema(), make_reader_permit(e),
                    random_range, remote_sharder, remote_shard, 0, strategy, gc_clock::now());
                std::vector<mutation_fragment> result;
                while (auto mf = co_await reader.read_mutation_fragment()) {
                    result.push_back(std::move(*mf));
                }
                co_await reader.on_end_of_stream();
                co_await reader.close();
                co_return result;
            };
            auto dump = [&](std::ostream& target, const std::vector<mutation_fragment>& fragments) {
                for (const auto& f: fragments) {
                    ::fmt::print(target, "\n{}", mutation_fragment::printer(s, f));
                }
            };

            const auto data1 = co_await read_all(strategy1);
            const auto data2 = co_await read_all(strategy2);
            std::optional<sstring> mismatch;
            if (data1.size() != data2.size()) {
                mismatch = ::format("size1 {} != size2 {}", data1.size(), data2.size());
            } else {
                for (unsigned i = 0; i < data1.size(); ++i) {
                    const auto& mf1 = data1[i];
                    const auto& mf2 = data2[i];
                    if (!mf1.equal(s, mf2)) {
                        mismatch = ::format("{} != {}",
                            mutation_fragment::printer(s, mf1), mutation_fragment::printer(s, mf2));
                        break;
                    }
                }
            }
            if (mismatch) {
                std::cout << "data1:";
                dump(std::cout, data1);
                std::cout << "\ndata2:";
                dump(std::cout, data2);
                std::cout << std::endl;
                BOOST_FAIL(::format("s1={}, s2={}, mismatch={}", strategy1, strategy2, *mismatch));
            }
        };

        co_await do_check(dht::sharder(local_sharder.shard_count() + 1, local_sharder.sharding_ignore_msb()),
            repair_reader::read_strategy::multishard_split,
            repair_reader::read_strategy::multishard_filter);
        co_await do_check(local_sharder,
            repair_reader::read_strategy::local,
            repair_reader::read_strategy::multishard_filter);
        co_await do_check(local_sharder,
            repair_reader::read_strategy::local,
            repair_reader::read_strategy::multishard_split);
    });
}
