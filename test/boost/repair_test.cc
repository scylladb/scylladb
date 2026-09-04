/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "replica/memtable.hh"
#include "readers/from_fragments.hh"
#include "bytes_ostream.hh"
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
#include <boost/lexical_cast.hpp>
#undef SEASTAR_TESTING_MAIN
#include <seastar/testing/test_case.hh>
#include <seastar/util/short_streams.hh>
#include "test/lib/sstable_utils.hh"
#include "readers/mutation_fragment_v1_stream.hh"
#include "schema/schema_registry.hh"
#include "utils/chunked_vector.hh"
#include "repair/incremental.hh"
#include "sstables/exceptions.hh"

BOOST_AUTO_TEST_SUITE(repair_test)

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
    utils::chunked_vector<mutation> muts = gen(100);

    for (mutation& mut : muts) {
        partition_key pk = mut.key();
        auto m2 = make_memtable(s, {mut}).get();
        m->apply(mut);
        auto reader = mutation_fragment_v1_stream(m2->make_mutation_reader(s, permit));
        auto close_reader = deferred_close(reader);
        utils::chunked_vector<frozen_mutation_fragment> mfs;
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
        compare_readers(*s, m->make_mutation_reader(s, permit), make_mutation_reader_from_fragments(s, permit, std::move(fragments)));
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
            co_await db.add_column_family_and_make_directory(gs.get(), replica::database::is_new_cf::yes);
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

        auto do_check = [&](const dht::static_sharder& remote_sharder,
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
                    random_range, remote_sharder, remote_shard, 0, strategy, gc_clock::now(), incremental_repair_meta(),
                    e.db_config().repair_multishard_reader_buffer_hint_size(),
                    e.db_config().repair_multishard_reader_enable_read_ahead());
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

        co_await do_check(dht::static_sharder(local_sharder.shard_count() + 1, local_sharder.sharding_ignore_msb()),
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

static future<> corrupt_data_component(sstables::shared_sstable sst) {
    auto f = co_await open_file_dma(sstables::test(sst).filename(component_type::Data).native(), open_flags::wo);
    const auto align = f.memory_dma_alignment();
    const auto len = f.disk_write_dma_alignment();
    auto wbuf = seastar::temporary_buffer<char>::aligned(align, len);
    std::fill(wbuf.get_write(), wbuf.get_write() + len, 0xba);
    co_await f.dma_write(0, wbuf.get(), len);
    co_await f.close();
}

static future<> run_repair_reader_corruption_test(random_mutation_generator::compress compress, const sstring& expected_error_msg) {
    return do_with_cql_env([=](cql_test_env& e) -> future<> {
        sstables::scoped_no_abort_on_malformed_sstable_error no_abort;
        random_mutation_generator gen{random_mutation_generator::generate_counters::no, local_shard_only::no,
            random_mutation_generator::generate_uncompactable::no, std::nullopt, "ks", "cf", compress};

        co_await e.db().invoke_on_all([gs = global_schema_ptr(gen.schema())](replica::database& db) -> future<> {
            co_await db.add_column_family_and_make_directory(gs.get(), replica::database::is_new_cf::yes);
        });

        auto& cf = e.local_db().find_column_family(gen.schema());
        const auto& local_sharder = cf.schema()->get_sharder();

        auto mutations = gen(30);
        auto& storage_proxy = e.get_storage_proxy().local();
        co_await storage_proxy.mutate_locally(std::move(mutations), tracing::trace_state_ptr());

        co_await cf.flush();

        auto sstables = cf.get_sstables();
        BOOST_REQUIRE_GT(sstables->size(), 0);
        auto sst = *sstables->begin();

        co_await corrupt_data_component(sst);

        bool caught_expected_error = false;
        auto test_range = dht::token_range::make_open_ended_both_sides();
        auto reader = repair_reader(e.db(), cf, cf.schema(), make_reader_permit(e),
            test_range, local_sharder, 0, 0, repair_reader::read_strategy::local,
            gc_clock::now(), incremental_repair_meta(),
            e.db_config().repair_multishard_reader_buffer_hint_size(),
            e.db_config().repair_multishard_reader_enable_read_ahead());

        try {
            while (auto mf = co_await reader.read_mutation_fragment()) {
                // Read until error occurs
            }
            co_await reader.on_end_of_stream();
        } catch (const malformed_sstable_exception& e) {
            caught_expected_error = (sstring(e.what()).find(expected_error_msg) != sstring::npos);
        }

        co_await reader.close();

        BOOST_REQUIRE(caught_expected_error);
    });
}

SEASTAR_TEST_CASE(test_repair_reader_checksum_mismatch_compressed) {
    return run_repair_reader_corruption_test(
        random_mutation_generator::compress::yes,
        "failed checksum"
    );
}

SEASTAR_TEST_CASE(test_repair_reader_checksum_mismatch_uncompressed) {
    return run_repair_reader_corruption_test(
        random_mutation_generator::compress::no,
        "failed checksum"
    );
}

SEASTAR_TEST_CASE(repair_rows_size_considers_external_memory) {
    return seastar::async([&] {
        tests::reader_concurrency_semaphore_wrapper semaphore;
        reader_permit permit = semaphore.make_permit();
        random_mutation_generator gen{random_mutation_generator::generate_counters::no};
        schema_ptr s = gen.schema();
        auto m = make_lw_shared<replica::memtable>(s);
        auto r = *make_random_repair_rows_on_wire(gen, s, permit, m).begin();
        uint64_t seed = tests::random::get_int<uint64_t>();

        auto& frozen_mf = *r.get_mutation_fragments().begin();
        auto mf = make_lw_shared<mutation_fragment>(frozen_mf.unfreeze(*s, permit));
        auto dk_ptr = make_lw_shared<const decorated_key_with_hash>(*s, dht::decorate_key(*s, r.get_key()), seed);
        position_in_partition pos(mf->position());
        repair_sync_boundary boundary{dk_ptr->dk, pos};
        auto fmf_size = frozen_mf.representation().size();

        // Test that frozen mutation fragment memory is counted.
        repair_row row{frozen_mf, std::nullopt, nullptr, std::nullopt, is_dirty_on_master::no, nullptr};
        BOOST_REQUIRE_EQUAL(row.size(), fmf_size + sizeof(repair_row));

        // Test that mutation fragment memory is counted.
        repair_row row_with_mf{frozen_mf, std::nullopt, nullptr, std::nullopt, is_dirty_on_master::no, mf};
        BOOST_REQUIRE_EQUAL(row_with_mf.size(), fmf_size + mf->memory_usage() + sizeof(repair_row));

        // Test that boundary memory is counted.
        repair_row row_with_boundary{frozen_mf, pos, dk_ptr, std::nullopt, is_dirty_on_master::no, nullptr};
        BOOST_REQUIRE_EQUAL(row_with_boundary.size(), fmf_size + boundary.pk.external_memory_usage() + boundary.position.external_memory_usage() + sizeof(repair_row));
    });
}

SEASTAR_TEST_CASE(test_tablet_token_range_count) {
    {
        // Simple case: one large range covers a smaller one
        utils::chunked_vector<tablet_token_range> r1 = {{10, 20}};
        utils::chunked_vector<tablet_token_range> r2 = {{0, 100}};
        BOOST_REQUIRE(co_await count_finished_tablets(r1, r2) == 1);
    }
    {
        // r2 ranges overlap and should merge to cover r1
        // r2: [0, 50] + [40, 100] -> merges to [0, 100]
        // r1: [10, 90] should be covered
        utils::chunked_vector<tablet_token_range> r1 = {{10, 90}};
        utils::chunked_vector<tablet_token_range> r2 = {{0, 50}, {40, 100}};
        BOOST_REQUIRE(co_await count_finished_tablets(r1, r2) == 1);
    }
    {
        // r2 ranges are adjacent (contiguous) and should merge
        // r2: [0, 10] + [11, 20] -> merges to [0, 20]
        // r1: [5, 15] should be covered
        utils::chunked_vector<tablet_token_range> r1 = {{5, 15}};
        utils::chunked_vector<tablet_token_range> r2 = {{0, 10}, {11, 20}};
        BOOST_REQUIRE(co_await count_finished_tablets(r1, r2) == 1);
    }
    {
        // r1 overlaps r2 but is not FULLY contained
        // r2: [0, 10]
        // r1: [5, 15] (Ends too late), [ -5, 5 ] (Starts too early)
        utils::chunked_vector<tablet_token_range> r1 = {{5, 15}, {-5, 5}};
        utils::chunked_vector<tablet_token_range> r2 = {{0, 10}};
        BOOST_REQUIRE(co_await count_finished_tablets(r1, r2) == 0);
    }
    {
        // A single merged range in r2 covers multiple distinct ranges in r1
        utils::chunked_vector<tablet_token_range> r1 = {{10, 20}, {30, 40}, {50, 60}};
        utils::chunked_vector<tablet_token_range> r2 = {{0, 100}};
        BOOST_REQUIRE(co_await count_finished_tablets(r1, r2) == 3);
    }
    {
        // Inputs are provided in random order, ensuring the internal sort works
        utils::chunked_vector<tablet_token_range> r1 = {{50, 60}, {10, 20}};
        utils::chunked_vector<tablet_token_range> r2 = {{50, 100}, {0, 40}};
        // r2 merges effectively to [0, 40] and [50, 100]
        // Both r1 items are covered
        BOOST_REQUIRE(co_await count_finished_tablets(r1, r2) == 2);
    }
    {
        utils::chunked_vector<tablet_token_range> r1 = {{10, 20}};
        utils::chunked_vector<tablet_token_range> r2_empty = {};
        utils::chunked_vector<tablet_token_range> r1_empty = {};
        utils::chunked_vector<tablet_token_range> r2 = {{0, 100}};

        BOOST_REQUIRE(co_await count_finished_tablets(r1, r2_empty) == 0);
        BOOST_REQUIRE(co_await count_finished_tablets(r1_empty, r2) == 0);
    }
}

// --- send_full_set_rpc_stream_batched wire-batching coverage ---
//
// batch_hashes_for_wire/batch_rows_for_wire are pure grouping functions (no rpc::sink,
// no I/O) extracted specifically so these boundary conditions can be pinned down exactly,
// which a full-cluster functional test cannot easily force (e.g. "exactly hash_batch_max_count
// hashes", "a row big enough to trip the byte cap before the count cap").

static repair_hash_set make_sequential_hashes(size_t n) {
    repair_hash_set hashes;
    for (size_t i = 0; i < n; i++) {
        hashes.insert(repair_hash(i));
    }
    return hashes;
}

static std::vector<repair_hash> flatten(const std::vector<std::vector<repair_hash>>& batches) {
    std::vector<repair_hash> flat;
    for (auto& b : batches) {
        flat.insert(flat.end(), b.begin(), b.end());
    }
    return flat;
}

SEASTAR_TEST_CASE(test_batch_hashes_for_wire_empty) {
    return seastar::async([&] {
        repair_hash_set hashes;
        auto batches = batch_hashes_for_wire(hashes).get();
        BOOST_REQUIRE(batches.empty());
    });
}

SEASTAR_TEST_CASE(test_batch_hashes_for_wire_single) {
    return seastar::async([&] {
        auto hashes = make_sequential_hashes(1);
        auto batches = batch_hashes_for_wire(hashes).get();
        BOOST_REQUIRE_EQUAL(batches.size(), 1u);
        BOOST_REQUIRE_EQUAL(batches[0].size(), 1u);
    });
}

SEASTAR_TEST_CASE(test_batch_hashes_for_wire_exact_boundary) {
    return seastar::async([&] {
        // Exactly hash_batch_max_count hashes must produce ONE full batch, not a full
        // batch plus an empty trailing one.
        auto hashes = make_sequential_hashes(hash_batch_max_count);
        auto batches = batch_hashes_for_wire(hashes).get();
        BOOST_REQUIRE_EQUAL(batches.size(), 1u);
        BOOST_REQUIRE_EQUAL(batches[0].size(), hash_batch_max_count);
    });
}

SEASTAR_TEST_CASE(test_batch_hashes_for_wire_one_over_boundary) {
    return seastar::async([&] {
        auto hashes = make_sequential_hashes(hash_batch_max_count + 1);
        auto batches = batch_hashes_for_wire(hashes).get();
        BOOST_REQUIRE_EQUAL(batches.size(), 2u);
        BOOST_REQUIRE_EQUAL(batches[0].size(), hash_batch_max_count);
        BOOST_REQUIRE_EQUAL(batches[1].size(), 1u);
    });
}

SEASTAR_TEST_CASE(test_batch_hashes_for_wire_one_under_boundary) {
    return seastar::async([&] {
        auto hashes = make_sequential_hashes(hash_batch_max_count - 1);
        auto batches = batch_hashes_for_wire(hashes).get();
        BOOST_REQUIRE_EQUAL(batches.size(), 1u);
        BOOST_REQUIRE_EQUAL(batches[0].size(), hash_batch_max_count - 1);
    });
}

SEASTAR_TEST_CASE(test_batch_hashes_for_wire_multiple_batches_plus_remainder) {
    return seastar::async([&] {
        const size_t n = 2 * hash_batch_max_count + 5;
        auto hashes = make_sequential_hashes(n);
        auto batches = batch_hashes_for_wire(hashes).get();
        BOOST_REQUIRE_EQUAL(batches.size(), 3u);
        BOOST_REQUIRE_EQUAL(batches[0].size(), hash_batch_max_count);
        BOOST_REQUIRE_EQUAL(batches[1].size(), hash_batch_max_count);
        BOOST_REQUIRE_EQUAL(batches[2].size(), 5u);

        // Order is preserved end-to-end, matching the set's own ascending iteration order.
        std::vector<repair_hash> expected(hashes.begin(), hashes.end());
        BOOST_REQUIRE(flatten(batches) == expected);
    });
}

static frozen_mutation_fragment make_dummy_frozen_mutation_fragment(size_t size) {
    bytes_ostream bo;
    std::string data(size, 'x');
    bo.write(data.data(), data.size());
    return frozen_mutation_fragment(std::move(bo));
}

// A minimal, schema-less wire row carrying one mutation fragment of a controlled byte
// size, for pinning down batch_rows_for_wire's count/byte-cap boundaries precisely.
// Not a valid row for anything that would try to unfreeze() it.
static repair_row_on_wire make_dummy_wire_row(size_t fragment_size) {
    utils::chunked_vector<frozen_mutation_fragment> mfs;
    mfs.push_back(make_dummy_frozen_mutation_fragment(fragment_size));
    return repair_row_on_wire(std::move(mfs));
}

static repair_rows_on_wire make_dummy_wire_rows(size_t n, size_t fragment_size) {
    repair_rows_on_wire rows;
    for (size_t i = 0; i < n; i++) {
        rows.push_back(make_dummy_wire_row(fragment_size));
    }
    return rows;
}

SEASTAR_TEST_CASE(test_batch_rows_for_wire_empty) {
    return seastar::async([&] {
        repair_rows_on_wire rows;
        auto batches = batch_rows_for_wire(rows).get();
        BOOST_REQUIRE(batches.empty());
    });
}

SEASTAR_TEST_CASE(test_batch_rows_for_wire_count_cap_boundaries) {
    return seastar::async([&] {
        // Tiny rows, far under the byte cap: only the count cap should be in play.
        {
            auto rows = make_dummy_wire_rows(row_batch_max_count, 4);
            auto batches = batch_rows_for_wire(rows).get();
            BOOST_REQUIRE_EQUAL(batches.size(), 1u);
            BOOST_REQUIRE_EQUAL(batches[0].size(), row_batch_max_count);
        }
        {
            auto rows = make_dummy_wire_rows(row_batch_max_count + 1, 4);
            auto batches = batch_rows_for_wire(rows).get();
            BOOST_REQUIRE_EQUAL(batches.size(), 2u);
            BOOST_REQUIRE_EQUAL(batches[0].size(), row_batch_max_count);
            BOOST_REQUIRE_EQUAL(batches[1].size(), 1u);
        }
        {
            auto rows = make_dummy_wire_rows(row_batch_max_count - 1, 4);
            auto batches = batch_rows_for_wire(rows).get();
            BOOST_REQUIRE_EQUAL(batches.size(), 1u);
            BOOST_REQUIRE_EQUAL(batches[0].size(), row_batch_max_count - 1);
        }
    });
}

SEASTAR_TEST_CASE(test_batch_rows_for_wire_byte_cap_triggers_before_count_cap) {
    return seastar::async([&] {
        // Each row is a quarter of row_batch_max_bytes, so the byte cap should split
        // into batches of exactly 4 rows -- nowhere near row_batch_max_count (32).
        const size_t fragment_size = row_batch_max_bytes / 4;
        auto rows = make_dummy_wire_rows(3 * 4, fragment_size);
        auto batches = batch_rows_for_wire(rows).get();
        BOOST_REQUIRE_EQUAL(batches.size(), 3u);
        for (auto& b : batches) {
            BOOST_REQUIRE_EQUAL(b.size(), 4u);
        }
    });
}

SEASTAR_TEST_CASE(test_batch_rows_for_wire_byte_cap_never_overshoots) {
    return seastar::async([&] {
        // Each row is just over half the cap, so two of them would overshoot it - the
        // batch must flush BEFORE adding the second row, not after.
        auto rows = make_dummy_wire_rows(5, row_batch_max_bytes / 2 + 1);
        auto batches = batch_rows_for_wire(rows).get();
        BOOST_REQUIRE_EQUAL(batches.size(), 5u);
        for (auto& b : batches) {
            BOOST_REQUIRE_EQUAL(b.size(), 1u);
        }
    });
}

SEASTAR_TEST_CASE(test_batch_rows_for_wire_single_oversized_row) {
    return seastar::async([&] {
        // A single row bigger than row_batch_max_bytes on its own must still be sent --
        // it gets its own batch rather than being dropped or looping forever.
        auto rows = make_dummy_wire_rows(1, row_batch_max_bytes * 2);
        auto batches = batch_rows_for_wire(rows).get();
        BOOST_REQUIRE_EQUAL(batches.size(), 1u);
        BOOST_REQUIRE_EQUAL(batches[0].size(), 1u);
    });
}

SEASTAR_TEST_CASE(test_batch_rows_for_wire_total_row_count_preserved) {
    return seastar::async([&] {
        // Mix of small and large rows: every input row must show up exactly once,
        // regardless of which cap ends up governing each batch.
        repair_rows_on_wire rows;
        for (size_t i = 0; i < 50; i++) {
            rows.push_back(make_dummy_wire_row(i % 3 == 0 ? row_batch_max_bytes / 2 : 8));
        }
        auto batches = batch_rows_for_wire(rows).get();
        size_t total = 0;
        for (auto& b : batches) {
            total += b.size();
        }
        BOOST_REQUIRE_EQUAL(total, 50u);
    });
}

// --- diff-detect algorithm selection coverage ---
//
// select_diff_detect_algorithm is the pure negotiation logic behind
// get_common_diff_detect_algorithm, exposed so the vnode/tablet gating (the one
// invariant that must never regress: [[vnode-repair-legacy-dont-touch]]) can be
// pinned down without a full cluster.

SEASTAR_TEST_CASE(test_select_diff_detect_algorithm_vnode_never_selects_batched) {
    return seastar::async([&] {
        using algo = row_level_diff_detect_algorithm;
        std::vector<std::vector<algo>> nodes_algorithms = {
            {algo::send_full_set, algo::send_full_set_rpc_stream, algo::send_full_set_rpc_stream_batched},
            {algo::send_full_set, algo::send_full_set_rpc_stream, algo::send_full_set_rpc_stream_batched},
        };
        // Even though every peer advertises batched support, a vnode (non-tablet) table
        // must never select it -- this is allow_batched=false, the caller's contract for
        // !cf.uses_tablets().
        auto selected = select_diff_detect_algorithm(nodes_algorithms, false);
        BOOST_REQUIRE(selected == algo::send_full_set_rpc_stream);
    });
}

SEASTAR_TEST_CASE(test_select_diff_detect_algorithm_tablet_picks_batched_when_supported) {
    return seastar::async([&] {
        using algo = row_level_diff_detect_algorithm;
        std::vector<std::vector<algo>> nodes_algorithms = {
            {algo::send_full_set, algo::send_full_set_rpc_stream, algo::send_full_set_rpc_stream_batched},
            {algo::send_full_set, algo::send_full_set_rpc_stream, algo::send_full_set_rpc_stream_batched},
        };
        auto selected = select_diff_detect_algorithm(nodes_algorithms, true);
        BOOST_REQUIRE(selected == algo::send_full_set_rpc_stream_batched);
    });
}

SEASTAR_TEST_CASE(test_select_diff_detect_algorithm_mixed_version_peer_excludes_batched) {
    return seastar::async([&] {
        using algo = row_level_diff_detect_algorithm;
        // One peer is an older binary that never advertises the batched algorithm at all
        // (the rolling-upgrade scenario) -- it must be excluded from the common set
        // regardless of allow_batched, since the peer genuinely doesn't support it.
        std::vector<std::vector<algo>> nodes_algorithms = {
            {algo::send_full_set, algo::send_full_set_rpc_stream, algo::send_full_set_rpc_stream_batched},
            {algo::send_full_set, algo::send_full_set_rpc_stream},
        };
        auto selected = select_diff_detect_algorithm(nodes_algorithms, true);
        BOOST_REQUIRE(selected == algo::send_full_set_rpc_stream);
    });
}

SEASTAR_TEST_CASE(test_select_diff_detect_algorithm_no_peers_falls_back_to_local_best) {
    return seastar::async([&] {
        using algo = row_level_diff_detect_algorithm;
        std::vector<std::vector<algo>> nodes_algorithms = {};
        BOOST_REQUIRE(select_diff_detect_algorithm(nodes_algorithms, true) == algo::send_full_set_rpc_stream_batched);
        BOOST_REQUIRE(select_diff_detect_algorithm(nodes_algorithms, false) == algo::send_full_set_rpc_stream);
    });
}

SEASTAR_TEST_CASE(test_select_diff_detect_algorithm_no_common_algorithm_throws) {
    return seastar::async([&] {
        using algo = row_level_diff_detect_algorithm;
        std::vector<std::vector<algo>> nodes_algorithms = {
            {},
        };
        BOOST_REQUIRE_THROW(select_diff_detect_algorithm(nodes_algorithms, true), std::runtime_error);
    });
}

BOOST_AUTO_TEST_SUITE_END()
