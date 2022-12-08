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
#include "repair/row_level.hh"
#include "test/lib/mutation_source_test.hh"
#include "test/lib/random_utils.hh"
#include "test/lib/reader_concurrency_semaphore.hh"
#include <seastar/testing/test_case.hh>
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
        reader.consume_pausable([&input, s, &mfs](mutation_fragment mf) {
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

