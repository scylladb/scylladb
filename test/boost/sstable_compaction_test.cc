/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <iterator>
#include <fmt/ranges.h>
#include <seastar/core/sstring.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/align.hh>
#include <seastar/core/aligned_buffer.hh>
#include <seastar/util/closeable.hh>
#include <seastar/util/short_streams.hh>
#include <seastar/core/coroutine.hh>

#include "sstables/generation_type.hh"
#include "sstables/sstables.hh"
#include "sstables/compress.hh"
#include "compaction/compaction.hh"
#undef SEASTAR_TESTING_MAIN
#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>
#include "schema/schema.hh"
#include "schema/schema_builder.hh"
#include "replica/database.hh"
#include "compaction/leveled_manifest.hh"
#include "sstables/metadata_collector.hh"
#include "sstables/sstable_writer.hh"
#include <memory>
#include "test/boost/sstable_test.hh"
#include <seastar/core/seastar.hh>
#include <seastar/core/do_with.hh>
#include "compaction/compaction_manager.hh"
#include "test/lib/tmpdir.hh"
#include "interval.hh"
#include "partition_slice_builder.hh"
#include "compaction/time_window_compaction_strategy.hh"
#include "compaction/leveled_compaction_strategy.hh"
#include "compaction/incremental_backlog_tracker.hh"
#include "compaction/size_tiered_backlog_tracker.hh"
#include "test/lib/mutation_assertions.hh"
#include "counters.hh"
#include "test/lib/simple_schema.hh"
#include "replica/memtable-sstable.hh"
#include "test/lib/mutation_reader_assertions.hh"
#include "test/lib/sstable_run_based_compaction_strategy_for_tests.hh"
#include "test/lib/random_schema.hh"
#include "mutation/mutation_compactor.hh"
#include "db/config.hh"
#include "mutation_writer/partition_based_splitting_writer.hh"
#include "compaction/table_state.hh"
#include "mutation/mutation_rebuilder.hh"
#include "mutation/mutation_source_metadata.hh"
#include "mutation/mutation_partition.hh"

#include <stdio.h>
#include <ftw.h>
#include <unistd.h>
#include <boost/icl/interval_map.hpp>
#include <boost/lexical_cast.hpp>
#include "test/lib/test_services.hh"
#include "test/lib/cql_test_env.hh"
#include "test/lib/reader_concurrency_semaphore.hh"
#include "test/lib/sstable_utils.hh"
#include "test/lib/random_utils.hh"
#include "test/lib/key_utils.hh"
#include "test/lib/test_utils.hh"
#include "test/lib/eventually.hh"
#include "readers/from_mutations_v2.hh"
#include "readers/from_fragments_v2.hh"
#include "readers/combined.hh"
#include "utils/assert.hh"
#include "utils/pretty_printers.hh"

BOOST_AUTO_TEST_SUITE(sstable_compaction_test)

namespace fs = std::filesystem;

using namespace sstables;
using compress_sstable = tests::random_schema_specification::compress_sstable;

static const sstring some_keyspace("ks");
static const sstring some_column_family("cf");

atomic_cell make_atomic_cell(data_type dt, bytes_view value, uint32_t ttl = 0, uint32_t expiration = 0) {
    if (ttl) {
        return atomic_cell::make_live(*dt, 0, value,
            gc_clock::time_point(gc_clock::duration(expiration)), gc_clock::duration(ttl));
    } else {
        return atomic_cell::make_live(*dt, 0, value);
    }
}

////////////////////////////////  Test basic compaction support

// open_sstables() opens several generations of the same sstable, returning,
// after all the tables have been open, their vector.
static future<std::vector<sstables::shared_sstable>> open_sstables(test_env& env, schema_ptr s, sstring dir, std::vector<sstables::generation_type::int_t> gen_values) {
    auto generations = generations_from_values(gen_values);
    std::vector<sstables::shared_sstable> ret;
    co_await coroutine::parallel_for_each(generations, [&env, &ret, &dir, s] (auto generation) -> future<> {
        auto sst = co_await env.reusable_sst(s, dir, generation);
        ret.push_back(std::move(sst));
    });
    co_return ret;
}

// mutation_reader for sstable keeping all the required objects alive.
static mutation_reader sstable_reader(shared_sstable sst, schema_ptr s, reader_permit permit) {
    return sst->as_mutation_source().make_reader_v2(s, std::move(permit), query::full_partition_range, s->full_slice());
}

class strategy_control_for_test : public strategy_control {
    bool _has_ongoing_compaction;
    std::optional<std::vector<shared_sstable>> _candidates_opt;
public:
    explicit strategy_control_for_test(bool has_ongoing_compaction, std::optional<std::vector<shared_sstable>> candidates) noexcept
        : _has_ongoing_compaction(has_ongoing_compaction)
        , _candidates_opt(candidates) {}

    bool has_ongoing_compaction(table_state& table_s) const noexcept override {
        return _has_ongoing_compaction;
    }

    std::vector<sstables::shared_sstable> candidates(table_state& t) const override {
        return _candidates_opt.value_or(*t.main_sstable_set().all() | std::ranges::to<std::vector>());
    }

    std::vector<sstables::frozen_sstable_run> candidates_as_runs(table_state& t) const override {
        return t.main_sstable_set().all_sstable_runs();
    }
};

static std::unique_ptr<strategy_control> make_strategy_control_for_test(bool has_ongoing_compaction, std::optional<std::vector<shared_sstable>> candidates = std::nullopt) {
    return std::make_unique<strategy_control_for_test>(has_ongoing_compaction, std::move(candidates));
}

template <typename CompactionStrategy>
requires requires(CompactionStrategy cs, table_state& t, strategy_control& c) {
    { cs.get_sstables_for_compaction(t, c) } -> std::same_as<sstables::compaction_descriptor>;
}
static compaction_descriptor get_sstables_for_compaction(CompactionStrategy& cs, table_state& t, std::vector<shared_sstable> candidates) {
    auto control = make_strategy_control_for_test(false, std::move(candidates));
    return cs.get_sstables_for_compaction(t, *control);
}

static void assert_table_sstable_count(table_for_tests& t, size_t expected_count) {
    testlog.info("sstable_set_size={}, live_sstable_count={}, expected={}", t->get_sstables()->size(), t->get_stats().live_sstable_count, expected_count);
    BOOST_REQUIRE(t->get_sstables()->size() == expected_count);
    BOOST_REQUIRE(uint64_t(t->get_stats().live_sstable_count) == expected_count);
}

SEASTAR_TEST_CASE(compaction_manager_basic_test) {
  return test_env::do_with_async([] (test_env& env) {
    BOOST_REQUIRE(smp::count == 1);
    auto s = schema_builder(some_keyspace, some_column_family)
                .with_column("p1", utf8_type, column_kind::partition_key)
                .with_column("c1", utf8_type, column_kind::clustering_key)
                .with_column("r1", int32_type)
                .build();
    auto cf = env.make_table_for_tests(s);
    auto& cm = cf->get_compaction_manager();
    auto close_cf = deferred_stop(cf);
    cf->set_compaction_strategy(sstables::compaction_strategy_type::size_tiered);
    auto sst_gen = env.make_sst_factory(s);

    auto idx = std::vector<unsigned long>({1, 2, 3, 4});
    for (auto i : idx) {
        // create 4 sstables of similar size to be compacted later on.

        const column_definition& r1_col = *s->get_column_definition("r1");

        sstring k = "key" + to_sstring(i);
        auto key = partition_key::from_exploded(*s, {to_bytes(k)});
        auto c_key = clustering_key::from_exploded(*s, {to_bytes("abc")});

        mutation m(s, key);
        m.set_clustered_cell(c_key, r1_col, make_atomic_cell(int32_type, int32_type->decompose(1)));

        auto sst = make_sstable_containing(sst_gen, {std::move(m)});
        column_family_test(cf).add_sstable(sst).get();
    }

    BOOST_REQUIRE(cf->sstables_count() == idx.size());
    cf->trigger_compaction();
    // wait for submitted job to finish and there is no pending tasks
    do_until([&cm] {
        return (cm.get_stats().completed_tasks > 0 &&
                cm.get_stats().pending_tasks == 0);
    }, [] {
        return sleep(std::chrono::milliseconds(100));
    }).wait();
    // test no more running compactions
    BOOST_CHECK_EQUAL(cm.get_stats().active_tasks, 0);
    // test compaction successfully finished
    BOOST_CHECK_EQUAL(cm.get_stats().completed_tasks, 1);
    BOOST_CHECK_EQUAL(cm.get_stats().errors, 0);

    // expect sstables of cf to be compacted.
    BOOST_CHECK_EQUAL(cf->sstables_count(), 1);
  });
}

SEASTAR_TEST_CASE(compact) {
    return sstables::test_env::do_with_async([] (sstables::test_env& env) {
        BOOST_REQUIRE(smp::count == 1);
        // The "compaction" sstable was created with the following schema:
        // CREATE TABLE compaction (
        //        name text,
        //        age int,
        //        height int,
        //        PRIMARY KEY (name)
        //);
        auto builder = schema_builder("tests", "compaction")
            .with_column("name", utf8_type, column_kind::partition_key)
            .with_column("age", int32_type)
            .with_column("height", int32_type);
        builder.set_comment("Example table for compaction");
        builder.set_gc_grace_seconds(std::numeric_limits<int32_t>::max());
        auto s = builder.build();
        auto cf = env.make_table_for_tests(s);
        auto close_cf = deferred_stop(cf);

        auto sstables = open_sstables(env, s, "test/resource/sstables/compaction", {1,2,3}).get();
        std::vector<shared_sstable> new_sstables;
        auto new_sstable = [&] {
            auto sst = env.make_sstable(cf->schema());
            new_sstables.push_back(sst);
            return sst;
        };
        compact_sstables(env, sstables::compaction_descriptor(std::move(sstables)), cf, new_sstable).get();
        // Verify that the compacted sstable has the right content. We expect to see:
        //  name  | age | height
        // -------+-----+--------
        //  jerry |  40 |    170
        //    tom |  20 |    180
        //   john |  20 |   deleted
        //   nadav - deleted partition
        BOOST_REQUIRE_EQUAL(new_sstables.size(), 1);
        auto sst = env.reusable_sst(s, new_sstables[0]).get();
        auto reader = sstable_reader(sst, s, env.make_reader_permit());
        auto close_reader = deferred_close(reader);
        auto verify_mutation = [&] (std::function<void(mutation_opt)> verify) {
            std::invoke(verify, read_mutation_from_mutation_reader(reader).get());
        };
        verify_mutation([&] (mutation_opt m) {
            BOOST_REQUIRE(m);
            BOOST_REQUIRE(m->key().equal(*s, partition_key::from_singular(*s, data_value(sstring("jerry")))));
            BOOST_REQUIRE(!m->partition().partition_tombstone());
            auto rows = m->partition().clustered_rows();
            BOOST_REQUIRE(rows.calculate_size() == 1);
            auto &row = rows.begin()->row();
            BOOST_REQUIRE(!row.deleted_at());
            auto &cells = row.cells();
            auto& cdef1 = *s->get_column_definition("age");
            auto& cdef2 = *s->get_column_definition("height");
            BOOST_REQUIRE(cells.cell_at(cdef1.id).as_atomic_cell(cdef1).value() == managed_bytes({0,0,0,40}));
            BOOST_REQUIRE(cells.cell_at(cdef2.id).as_atomic_cell(cdef2).value() == managed_bytes({0,0,0,(int8_t)170}));
        });
        verify_mutation([&] (mutation_opt m) {
            BOOST_REQUIRE(m);
            BOOST_REQUIRE(m->key().equal(*s, partition_key::from_singular(*s, data_value(sstring("tom")))));
            BOOST_REQUIRE(!m->partition().partition_tombstone());
            auto rows = m->partition().clustered_rows();
            BOOST_REQUIRE(rows.calculate_size() == 1);
            auto &row = rows.begin()->row();
            BOOST_REQUIRE(!row.deleted_at());
            auto &cells = row.cells();
            auto& cdef1 = *s->get_column_definition("age");
            auto& cdef2 = *s->get_column_definition("height");
            BOOST_REQUIRE(cells.cell_at(cdef1.id).as_atomic_cell(cdef1).value() == managed_bytes({0,0,0,20}));
            BOOST_REQUIRE(cells.cell_at(cdef2.id).as_atomic_cell(cdef2).value() == managed_bytes({0,0,0,(int8_t)180}));
        });
        verify_mutation([&] (mutation_opt m) {
            BOOST_REQUIRE(m);
            BOOST_REQUIRE(m->key().equal(*s, partition_key::from_singular(*s, data_value(sstring("john")))));
            BOOST_REQUIRE(!m->partition().partition_tombstone());
            auto rows = m->partition().clustered_rows();
            BOOST_REQUIRE(rows.calculate_size() == 1);
            auto &row = rows.begin()->row();
            BOOST_REQUIRE(!row.deleted_at());
            auto &cells = row.cells();
            auto& cdef1 = *s->get_column_definition("age");
            auto& cdef2 = *s->get_column_definition("height");
            BOOST_REQUIRE(cells.cell_at(cdef1.id).as_atomic_cell(cdef1).value() == managed_bytes({0,0,0,20}));
            BOOST_REQUIRE(cells.find_cell(cdef2.id) == nullptr);
        });
        verify_mutation([&] (mutation_opt m) {
            BOOST_REQUIRE(m);
            BOOST_REQUIRE(m->key().equal(*s, partition_key::from_singular(*s, data_value(sstring("nadav")))));
            BOOST_REQUIRE(m->partition().partition_tombstone());
            auto rows = m->partition().clustered_rows();
            BOOST_REQUIRE(rows.calculate_size() == 0);
        });
        verify_mutation([&] (mutation_opt m) {
            BOOST_REQUIRE(!m);
        });
    });

    // verify that the compacted sstable look like
}

static std::vector<sstables::shared_sstable> get_candidates_for_leveled_strategy(replica::column_family& cf) {
    std::vector<sstables::shared_sstable> candidates;
    candidates.reserve(cf.sstables_count());
    for (auto sstables = cf.get_sstables(); auto& entry : *sstables) {
        candidates.push_back(entry);
    }
    return candidates;
}

struct compact_sstables_result {
    std::vector<sstables::shared_sstable> input_sstables;
    std::vector<sstables::shared_sstable> output_sstables;
};

// Return vector of sstables generated by compaction. Only relevant for leveled one.
static future<compact_sstables_result> compact_sstables(test_env& env, std::vector<sstables::shared_sstable> sstables_to_compact, size_t create_sstables,
        uint64_t min_sstable_size, compaction_strategy_type strategy) {
    BOOST_REQUIRE(smp::count == 1);
    return seastar::async(
            [&env, sstables = std::move(sstables_to_compact), create_sstables, min_sstable_size, strategy] () mutable {
        schema_builder builder(some_keyspace, some_column_family);
        builder.with_column("p1", utf8_type, column_kind::partition_key);
        builder.with_column("c1", utf8_type, column_kind::clustering_key);
        builder.with_column("r1", utf8_type);
        builder.set_compressor_params(compression_parameters::no_compression());
        builder.set_min_compaction_threshold(4);
        auto s = builder.build(schema_builder::compact_storage::no);
        auto sst_gen = env.make_sst_factory(s);

        auto cf = env.make_table_for_tests(s);
        auto stop_cf = deferred_stop(cf);

        std::vector<sstables::shared_sstable> created;

        if (create_sstables) {
            for (unsigned i = 0; i < create_sstables; i++) {
                const column_definition& r1_col = *s->get_column_definition("r1");

                auto sst = sst_gen();
                sstring k = "key" + to_sstring(sst->generation());
                auto key = partition_key::from_exploded(*s, {to_bytes(k)});
                auto c_key = clustering_key::from_exploded(*s, {to_bytes("abc")});

                mutation m(s, key);
                m.set_clustered_cell(c_key, r1_col, make_atomic_cell(utf8_type, bytes(min_sstable_size, 'a')));

                sst = make_sstable_containing(sst, {std::move(m)});
                sstables.push_back(sst);
            }
        }

        auto new_sstable = [&] {
            auto sst = sst_gen();
            created.push_back(sst);
            return sst;
        };

        if (strategy == compaction_strategy_type::size_tiered) {
            // Calling function that will return a list of sstables to compact based on size-tiered strategy.
            int min_threshold = cf->schema()->min_compaction_threshold();
            int max_threshold = cf->schema()->max_compaction_threshold();
            auto sstables_to_compact = sstables::size_tiered_compaction_strategy::most_interesting_bucket(sstables, min_threshold, max_threshold);
            // We do expect that all candidates were selected for compaction (in this case).
            BOOST_REQUIRE(sstables_to_compact.size() == sstables.size());
            (void)compact_sstables(env, sstables::compaction_descriptor(std::move(sstables_to_compact)), cf, new_sstable).get();
        } else if (strategy == compaction_strategy_type::leveled) {
            std::vector<sstables::shared_sstable> candidates;
            candidates.reserve(sstables.size());
            for (auto& sst : sstables) {
                BOOST_REQUIRE(sst->get_sstable_level() == 0);
                BOOST_REQUIRE(sst->data_size() >= min_sstable_size);
                candidates.push_back(sst);
            }
            sstables::size_tiered_compaction_strategy_options stcs_options;
            leveled_manifest manifest = leveled_manifest::create(cf.as_table_state(), candidates, 1, stcs_options);
            std::vector<std::optional<dht::decorated_key>> last_compacted_keys(leveled_manifest::MAX_LEVELS);
            std::vector<int> compaction_counter(leveled_manifest::MAX_LEVELS);
            auto candidate = manifest.get_compaction_candidates(last_compacted_keys, compaction_counter);
            BOOST_REQUIRE(candidate.sstables.size() == sstables.size());
            BOOST_REQUIRE(candidate.level == 1);
            BOOST_REQUIRE(candidate.max_sstable_bytes == 1024*1024);

            (void)compact_sstables(env, sstables::compaction_descriptor(std::move(candidate.sstables),
                candidate.level, 1024*1024), cf, new_sstable).get();
        } else {
            throw std::runtime_error("unexpected strategy");
        }

        return compact_sstables_result{std::move(sstables), std::move(created)};
    });
}

static future<compact_sstables_result> create_and_compact_sstables(test_env& env, size_t create_sstables) {
    uint64_t min_sstable_size = 50;
    auto res = co_await compact_sstables(env, {}, create_sstables, min_sstable_size, compaction_strategy_type::size_tiered);
    // size tiered compaction will output at most one sstable, let's SCYLLA_ASSERT that.
    BOOST_REQUIRE(res.output_sstables.size() == 1);
    co_return res;
}

static future<compact_sstables_result> compact_sstables(test_env& env, std::vector<sstables::shared_sstable> sstables_to_compact) {
    uint64_t min_sstable_size = 50;
    auto res = co_await compact_sstables(env, std::move(sstables_to_compact), 0, min_sstable_size, compaction_strategy_type::size_tiered);
    // size tiered compaction will output at most one sstable, let's SCYLLA_ASSERT that.
    BOOST_REQUIRE(res.output_sstables.size() == 1);
    co_return res;
}

static future<> check_compacted_sstables(test_env& env, compact_sstables_result res) {
    return seastar::async([&env, res = std::move(res)] {
        auto s = schema_builder(some_keyspace, some_column_family)
                .with_column("p1", utf8_type, column_kind::partition_key)
                .with_column("c1", utf8_type, column_kind::clustering_key)
                .with_column("r1", utf8_type)
                .build();
        BOOST_REQUIRE_EQUAL(res.output_sstables.size(), 1);
        auto sst = env.reusable_sst(s, res.output_sstables[0]).get();
        auto reader = sstable_reader(sst, s, env.make_reader_permit()); // reader holds sst and s alive.
        auto close_reader = deferred_close(reader);
        std::vector<partition_key> keys;

        while (auto m = read_mutation_from_mutation_reader(reader).get()) {
            keys.push_back(m->key());
        }

        // keys from compacted sstable aren't ordered lexographically,
        // thus we must read all keys into a vector, sort the vector
        // lexographically, then proceed with the comparison.
        std::sort(keys.begin(), keys.end(), partition_key::less_compare(*s));
        BOOST_REQUIRE_EQUAL(keys.size(), res.input_sstables.size());

        auto generations = res.input_sstables
            | std::views::transform([] (const sstables::shared_sstable& sst) { return sst->generation(); })
            | std::ranges::to<std::vector<sstables::generation_type>>();
        for (auto& k : keys) {
            bool found = false;
            for (auto it = generations.begin(); it != generations.end(); ++it) {
                sstring original_k = "key" + to_sstring(*it);
                found = k.equal(*s, partition_key::from_singular(*s, data_value(original_k)));
                if (found) {
                    generations.erase(it);
                    break;
                }
            }
            BOOST_REQUIRE(found);
        }
    });
}

SEASTAR_TEST_CASE(compact_02) {
    // NOTE: generations 18 to 38 are used here.

    // This tests size-tiered compaction strategy by creating 4 sstables of
    // similar size and compacting them to create a new tier.
    // The process above is repeated 4 times until you have 4 compacted
    // sstables of similar size. Then you compact these 4 compacted sstables,
    // and make sure that you have all partition keys.
    // By the way, automatic compaction isn't tested here, instead the
    // strategy algorithm that selects candidates for compaction.

    return test_env::do_with_async([] (test_env& env) {
        std::vector<sstables::shared_sstable> all_input_sstables;
        std::vector<sstables::shared_sstable> compacted;

        auto compact_and_verify = [&] (size_t count) mutable{
            // Compact `count` sstables into 1 using size-tiered strategy to select sstables.
            // E.g.: generations 18, 19, 20 and 21 will be compacted into generation 22.
            auto res = create_and_compact_sstables(env, count).get();
            std::copy(res.input_sstables.begin(), res.input_sstables.end(), std::back_inserter(all_input_sstables));
            compacted.emplace_back(res.output_sstables[0]);
            // Check that generation 22 contains all keys of generations 18, 19, 20 and 21.
            check_compacted_sstables(env, std::move(res)).get();
        };

        static constexpr size_t num_rounds = 4;
        static constexpr size_t sstables_in_round = 4;
        for (unsigned i = 0; i < num_rounds; ++i) {
            compact_and_verify(sstables_in_round);
        }

        // In this step, we compact 4 compacted sstables.
        auto res = compact_sstables(env, std::move(compacted)).get();
        res.input_sstables = std::move(all_input_sstables);
        // Check that the compacted sstable contains all keys.
        check_compacted_sstables(env, std::move(res)).get();
    });
}

template <typename ExceptionType>
static void compact_corrupted_by_compression_mode(const std::string& tname,
        compress_sstable compress,
        sstables::compaction_type_options&& options,
        const sstring& error_msg)
{
    test_env::do_with_async([&] (test_env& env) {
        auto compact = [&] (schema_ptr schema, std::vector<shared_sstable> to_compact) {
            auto sst_gen = env.make_sst_factory(schema);
            auto cf = env.make_table_for_tests(schema);
            auto stop_cf = deferred_stop(cf);
            for (auto&& sst : to_compact) {
                column_family_test(cf).add_sstable(sst).get();
            }
            sstables::compaction_descriptor desc(to_compact,
                    sstables::compaction_descriptor::default_level,
                    sstables::compaction_descriptor::default_max_sstable_bytes,
                    run_id::create_random_id(),
                    options);
            desc.sharder = &schema->get_sharder(); // needed to support resharding compaction
            compact_sstables(env, desc, cf, sst_gen).get();
        };
        auto test_failing_compact = [&compact] (schema_ptr schema, std::vector<shared_sstable> to_compact, const sstring& compaction_err_msg, const sstring& integrity_err_msg) {
            try {
                compact(schema, to_compact);
                BOOST_FAIL("expecting compaction to fail with exception");
            } catch (const ExceptionType& e) {
                std::string what = e.what();
                BOOST_REQUIRE(what.find(compaction_err_msg) != std::string::npos);
                BOOST_REQUIRE(what.find(integrity_err_msg) != std::string::npos);
            }
        };

        auto random_spec = tests::make_random_schema_specification(
                tname,
                std::uniform_int_distribution<size_t>(1, 4),
                std::uniform_int_distribution<size_t>(2, 4),
                std::uniform_int_distribution<size_t>(2, 8),
                std::uniform_int_distribution<size_t>(2, 8),
                compress);
        auto random_schema = tests::random_schema{tests::random::get_int<uint32_t>(), *random_spec};
        auto schema = random_schema.schema();

        testlog.info("Random schema:\n{}", random_schema.cql());

        testlog.info("Compacting {}compressed SSTable with invalid checksums", compress ? "" : "un");

        const auto muts = tests::generate_random_mutations(random_schema, 2).get();
        auto sst = make_sstable_containing(env.make_sstable(schema), muts);
        {
            auto f = open_file_dma(sstables::test(sst).filename(component_type::Data).native(), open_flags::wo).get();
            auto close_f = deferred_close(f);
            const auto wbuf_align = f.memory_dma_alignment();
            const auto wbuf_len = f.disk_write_dma_alignment();
            auto wbuf = seastar::temporary_buffer<char>::aligned(wbuf_align, wbuf_len);
            std::fill(wbuf.get_write(), wbuf.get_write() + wbuf_len, 0xba);
            f.dma_write(0, wbuf.get(), wbuf_len).get();
        }
        test_failing_compact(schema, {sst}, error_msg, "failed checksum");

        testlog.info("Compacting {}compressed SSTable with invalid digest", compress ? "" : "un");

        sst = make_sstable_containing(env.make_sstable(schema), muts);
        {
            auto f = open_file_dma(sstables::test(sst).filename(component_type::Digest).native(), open_flags::rw).get();
            auto stream = make_file_input_stream(f);
            auto close_stream = deferred_close(stream);
            auto digest_str = util::read_entire_stream_contiguous(stream).get();
            auto digest = boost::lexical_cast<uint32_t>(digest_str);
            auto new_digest = to_sstring<bytes>(digest + 1); // a random invalid digest
            f.dma_write(0, new_digest.c_str(), new_digest.size()).get();
        }
        test_failing_compact(schema, {sst}, error_msg, "Digest mismatch");
    }).get();
}

template <typename ExceptionType>
static void compact_corrupted(const std::string& tname, sstables::compaction_type_options&& options, const sstring& error_msg) {
    for (const auto& compress : {compress_sstable::no, compress_sstable::yes}) {
        compact_corrupted_by_compression_mode<ExceptionType>(tname, compress, std::move(options), error_msg);
    }
}

SEASTAR_THREAD_TEST_CASE(compact_with_corrupted_sstable_regular) {
    compact_corrupted<sstables::malformed_sstable_exception>(get_name(),
            sstables::compaction_type_options::make_regular(),
            "Failed to read partition from SSTable");
}
SEASTAR_THREAD_TEST_CASE(compact_with_corrupted_sstable_scrub) {
    using scrub_mode = sstables::compaction_type_options::scrub::mode;
    compact_corrupted<sstables::compaction_aborted_exception>(get_name(),
            sstables::compaction_type_options::make_scrub(scrub_mode::segregate),
            "scrub compaction failed due to unrecoverable error: sstables::malformed_sstable_exception");
}
SEASTAR_THREAD_TEST_CASE(compact_with_corrupted_sstable_cleanup) {
    compact_corrupted<sstables::malformed_sstable_exception>(get_name(),
            sstables::compaction_type_options::make_cleanup(),
            "Failed to read partition from SSTable");
}
SEASTAR_THREAD_TEST_CASE(compact_with_corrupted_sstable_reshape) {
    compact_corrupted<sstables::malformed_sstable_exception>(get_name(),
            sstables::compaction_type_options::make_reshape(),
            "Failed to read partition from SSTable");
}
SEASTAR_THREAD_TEST_CASE(compact_with_corrupted_sstable_reshard) {
    compact_corrupted<sstables::malformed_sstable_exception>(get_name(),
            sstables::compaction_type_options::make_reshard(),
            "Failed to read partition from SSTable");
}
SEASTAR_THREAD_TEST_CASE(compact_with_corrupted_sstable_split) {
    auto classify_fn = [] (dht::token t) -> mutation_writer::token_group_id { return 1; };
    compact_corrupted<sstables::malformed_sstable_exception>(get_name(),
            sstables::compaction_type_options::make_split(classify_fn),
            "Failed to read partition from SSTable");
}

// Leveled compaction strategy tests

// NOTE: must run in a thread.
static sstables::shared_sstable add_sstable_for_leveled_test(test_env& env, lw_shared_ptr<replica::column_family> cf, uint64_t fake_data_size,
                                         uint32_t sstable_level, const partition_key& first_key, const partition_key& last_key, int64_t max_timestamp = 0) {
    auto sst = env.make_sstable(cf->schema());
    sstables::test(sst).set_values_for_leveled_strategy(fake_data_size, sstable_level, max_timestamp, first_key, last_key);
    SCYLLA_ASSERT(sst->data_size() == fake_data_size);
    SCYLLA_ASSERT(sst->get_sstable_level() == sstable_level);
    SCYLLA_ASSERT(sst->get_stats_metadata().max_timestamp == max_timestamp);
    column_family_test(cf).add_sstable(sst).get();
    return sst;
}

// NOTE: must run in a thread.
static shared_sstable add_sstable_for_overlapping_test(test_env& env, lw_shared_ptr<replica::column_family> cf,
        const partition_key& first_key, const partition_key& last_key, stats_metadata stats = {}) {
    auto sst = env.make_sstable(cf->schema());
    sstables::test(sst).set_values(std::move(first_key), std::move(last_key), std::move(stats));
    column_family_test(cf).add_sstable(sst).get();
    return sst;
}
static shared_sstable sstable_for_overlapping_test(test_env& env, const schema_ptr& schema,
        const partition_key& first_key, const partition_key& last_key, uint32_t level = 0) {
    auto sst = env.make_sstable(schema);
    sstables::test(sst).set_values_for_leveled_strategy(1 /* size */, level, 0 /* max_timestamp */, first_key, last_key);
    return sst;
}

// ranges: [a,b] and [c,d]
// returns true if token ranges overlap.
static bool key_range_overlaps(table_for_tests& cf, const dht::decorated_key& a, const dht::decorated_key& b,
        const dht::decorated_key& c, const dht::decorated_key& d) {
    auto range1 = dht::partition_range::make({a, true}, {b, true});
    auto range2 = dht::partition_range::make({c, true}, {d, true});
    return range1.overlaps(range2, dht::ring_position_comparator(*cf.schema()));
}

static bool sstable_overlaps(const lw_shared_ptr<replica::column_family>& cf, sstables::shared_sstable candidate1, sstables::shared_sstable candidate2) {
    auto range1 = wrapping_interval<dht::token>::make(candidate1->get_first_decorated_key()._token, candidate1->get_last_decorated_key()._token);
    auto range2 = wrapping_interval<dht::token>::make(candidate2->get_first_decorated_key()._token, candidate2->get_last_decorated_key()._token);
    return range1.overlaps(range2, dht::token_comparator());
}

SEASTAR_TEST_CASE(leveled_01) {
  BOOST_REQUIRE_EQUAL(smp::count, 1);
  return test_env::do_with_async([] (test_env& env) {
    auto cf = env.make_table_for_tests();
    auto stop_cf = deferred_stop(cf);

    const auto keys = tests::generate_partition_keys(50, cf.schema());
    const auto& min_key = keys.front();
    const auto& max_key = keys.back();
    auto max_sstable_size_in_mb = 1;
    auto max_sstable_size = max_sstable_size_in_mb*1024*1024;

    // Creating two sstables which key range overlap.
    auto sst1 = add_sstable_for_leveled_test(env, cf, max_sstable_size, /*level*/0, min_key.key(), max_key.key());
    BOOST_REQUIRE(cf->get_sstables()->size() == 1);

    auto sst2 = add_sstable_for_leveled_test(env, cf, max_sstable_size, /*level*/0, keys[1].key(), max_key.key());
    BOOST_REQUIRE(cf->get_sstables()->size() == 2);

    BOOST_REQUIRE(key_range_overlaps(cf, min_key, max_key, keys[1], max_key) == true);
    BOOST_REQUIRE(sstable_overlaps(cf, sst1, sst2) == true);

    auto candidates = get_candidates_for_leveled_strategy(*cf);
    sstables::size_tiered_compaction_strategy_options stcs_options;
    leveled_manifest manifest = leveled_manifest::create(cf.as_table_state(), candidates, max_sstable_size_in_mb, stcs_options);
    BOOST_REQUIRE(manifest.get_level_size(0) == 2);
    std::vector<std::optional<dht::decorated_key>> last_compacted_keys(leveled_manifest::MAX_LEVELS);
    std::vector<int> compaction_counter(leveled_manifest::MAX_LEVELS);
    auto candidate = manifest.get_compaction_candidates(last_compacted_keys, compaction_counter);
    BOOST_REQUIRE(candidate.sstables.size() == 2);
    BOOST_REQUIRE(candidate.level == 1);

    std::unordered_set<sstables::shared_sstable> expected = { sst1, sst2 };
    for (auto& sst : candidate.sstables) {
        BOOST_REQUIRE(expected.erase(sst));
        BOOST_REQUIRE(sst->get_sstable_level() == 0);
    }
    BOOST_REQUIRE(expected.empty());
  });
}

SEASTAR_TEST_CASE(leveled_02) {
  BOOST_REQUIRE_EQUAL(smp::count, 1);
  return test_env::do_with_async([] (test_env& env) {
    auto cf = env.make_table_for_tests();
    auto stop_cf = deferred_stop(cf);

    const auto keys = tests::generate_partition_keys(50, cf.schema());
    const auto& min_key = keys.front();
    const auto& max_key = keys.back();
    auto max_sstable_size_in_mb = 1;
    auto max_sstable_size = max_sstable_size_in_mb*1024*1024;

    // Generation 1 will overlap only with generation 2.
    // Remember that for level0, leveled strategy prefer choosing older sstables as candidates.

    auto sst1 = add_sstable_for_leveled_test(env, cf, max_sstable_size, /*level*/0, min_key.key(), keys[10].key());
    BOOST_REQUIRE(cf->get_sstables()->size() == 1);

    auto sst2 = add_sstable_for_leveled_test(env, cf, max_sstable_size, /*level*/0, min_key.key(), keys[20].key());
    BOOST_REQUIRE(cf->get_sstables()->size() == 2);

    auto sst3 = add_sstable_for_leveled_test(env, cf, max_sstable_size, /*level*/0, keys[30].key(), max_key.key());
    BOOST_REQUIRE(cf->get_sstables()->size() == 3);

    BOOST_REQUIRE(key_range_overlaps(cf, min_key, keys[10], min_key, keys[20]) == true);
    BOOST_REQUIRE(key_range_overlaps(cf, min_key, keys[20], keys[30], max_key) == false);
    BOOST_REQUIRE(key_range_overlaps(cf, min_key, keys[10], keys[30], max_key) == false);
    BOOST_REQUIRE(sstable_overlaps(cf, sst1, sst2) == true);
    BOOST_REQUIRE(sstable_overlaps(cf, sst2, sst1) == true);
    BOOST_REQUIRE(sstable_overlaps(cf, sst1, sst3) == false);
    BOOST_REQUIRE(sstable_overlaps(cf, sst2, sst3) == false);

    auto candidates = get_candidates_for_leveled_strategy(*cf);
    sstables::size_tiered_compaction_strategy_options stcs_options;
    leveled_manifest manifest = leveled_manifest::create(cf.as_table_state(), candidates, max_sstable_size_in_mb, stcs_options);
    BOOST_REQUIRE(manifest.get_level_size(0) == 3);
    std::vector<std::optional<dht::decorated_key>> last_compacted_keys(leveled_manifest::MAX_LEVELS);
    std::vector<int> compaction_counter(leveled_manifest::MAX_LEVELS);
    auto candidate = manifest.get_compaction_candidates(last_compacted_keys, compaction_counter);
    BOOST_REQUIRE(candidate.sstables.size() == 3);
    BOOST_REQUIRE(candidate.level == 1);

    std::unordered_set<sstables::shared_sstable> expected = { sst1, sst2, sst3 };
    for (auto& sst : candidate.sstables) {
        BOOST_REQUIRE(expected.erase(sst));
        BOOST_REQUIRE(sst->get_sstable_level() == 0);
    }
    BOOST_REQUIRE(expected.empty());
  });
}

SEASTAR_TEST_CASE(leveled_03) {
  BOOST_REQUIRE_EQUAL(smp::count, 1);
  return test_env::do_with_async([] (test_env& env) {
    auto cf = env.make_table_for_tests();
    auto stop_cf = deferred_stop(cf);

    const auto keys = tests::generate_partition_keys(50, cf.schema());
    const auto& min_key = keys.front();
    const auto& max_key = keys.back();

    // Creating two sstables of level 0 which overlap
    auto sst1 = add_sstable_for_leveled_test(env, cf, /*data_size*/1024*1024, /*level*/0, min_key.key(), keys[10].key());
    auto sst2 = add_sstable_for_leveled_test(env, cf, /*data_size*/1024*1024, /*level*/0, min_key.key(), keys[20].key());
    // Creating a sstable of level 1 which overlap with two sstables above.
    auto sst3 = add_sstable_for_leveled_test(env, cf, /*data_size*/1024*1024, /*level*/1, min_key.key(), keys[30].key());
    // Creating a sstable of level 1 which doesn't overlap with any sstable.
    auto sst4 = add_sstable_for_leveled_test(env, cf, /*data_size*/1024*1024, /*level*/1, keys[40].key(), max_key.key());

    BOOST_REQUIRE(cf->get_sstables()->size() == 4);

    BOOST_REQUIRE(key_range_overlaps(cf, min_key, keys[10], min_key, keys[20]) == true);
    BOOST_REQUIRE(key_range_overlaps(cf, min_key, keys[10], min_key, keys[30]) == true);
    BOOST_REQUIRE(key_range_overlaps(cf, min_key, keys[20], min_key, keys[30]) == true);
    BOOST_REQUIRE(key_range_overlaps(cf, min_key, keys[10], keys[40], max_key) == false);
    BOOST_REQUIRE(key_range_overlaps(cf, min_key, keys[30], keys[40], max_key) == false);
    BOOST_REQUIRE(sstable_overlaps(cf, sst1, sst2) == true);
    BOOST_REQUIRE(sstable_overlaps(cf, sst1, sst3) == true);
    BOOST_REQUIRE(sstable_overlaps(cf, sst2, sst3) == true);
    BOOST_REQUIRE(sstable_overlaps(cf, sst1, sst4) == false);
    BOOST_REQUIRE(sstable_overlaps(cf, sst2, sst4) == false);
    BOOST_REQUIRE(sstable_overlaps(cf, sst3, sst4) == false);

    auto max_sstable_size_in_mb = 1;
    auto candidates = get_candidates_for_leveled_strategy(*cf);
    sstables::size_tiered_compaction_strategy_options stcs_options;
    leveled_manifest manifest = leveled_manifest::create(cf.as_table_state(), candidates, max_sstable_size_in_mb, stcs_options);
    BOOST_REQUIRE(manifest.get_level_size(0) == 2);
    BOOST_REQUIRE(manifest.get_level_size(1) == 2);
    std::vector<std::optional<dht::decorated_key>> last_compacted_keys(leveled_manifest::MAX_LEVELS);
    std::vector<int> compaction_counter(leveled_manifest::MAX_LEVELS);
    auto candidate = manifest.get_compaction_candidates(last_compacted_keys, compaction_counter);
    BOOST_REQUIRE(candidate.sstables.size() == 3);
    BOOST_REQUIRE(candidate.level == 1);

    std::unordered_set<sstables::shared_sstable> expected = { sst1, sst2, sst3 };
    for (auto& sst : candidate.sstables) {
        BOOST_REQUIRE(expected.erase(sst));
    }
    BOOST_REQUIRE(expected.empty());
  });
}

SEASTAR_TEST_CASE(leveled_04) {
  BOOST_REQUIRE_EQUAL(smp::count, 1);
  return test_env::do_with_async([] (test_env& env) {
    auto cf = env.make_table_for_tests();
    auto stop_cf = deferred_stop(cf);

    const auto keys = tests::generate_partition_keys(50, cf.schema());
    const auto& min_key = keys.front();
    const auto& max_key = keys.back();

    auto max_sstable_size_in_mb = 1;
    auto max_sstable_size_in_bytes = max_sstable_size_in_mb*1024*1024;

    // add 1 level-0 sstable to cf.
    auto sst1 = add_sstable_for_leveled_test(env, cf, /*data_size*/max_sstable_size_in_bytes, /*level*/0, min_key.key(), max_key.key());

    // create two big sstables in level1 to force leveled compaction on it.
    auto max_bytes_for_l1 = leveled_manifest::max_bytes_for_level(1, max_sstable_size_in_bytes);
    // NOTE: SSTables in level1 cannot overlap.
    auto sst2 = add_sstable_for_leveled_test(env, cf, /*data_size*/max_bytes_for_l1, /*level*/1, min_key.key(), keys[25].key());
    auto sst3 = add_sstable_for_leveled_test(env, cf, /*data_size*/max_bytes_for_l1, /*level*/1, keys[26].key(), max_key.key());

    // Create SSTable in level2 that overlaps with the ones in level1,
    // so compaction in level1 will select overlapping sstables in
    // level2.
    auto sst4 = add_sstable_for_leveled_test(env, cf, /*data_size*/max_sstable_size_in_bytes, /*level*/2, min_key.key(), max_key.key());

    BOOST_REQUIRE(cf->get_sstables()->size() == 4);

    BOOST_REQUIRE(key_range_overlaps(cf, min_key, max_key, min_key, max_key) == true);
    BOOST_REQUIRE(sstable_overlaps(cf, sst1, sst2) == true);
    BOOST_REQUIRE(sstable_overlaps(cf, sst1, sst3) == true);
    BOOST_REQUIRE(sstable_overlaps(cf, sst2, sst3) == false);
    BOOST_REQUIRE(sstable_overlaps(cf, sst3, sst4) == true);
    BOOST_REQUIRE(sstable_overlaps(cf, sst2, sst4) == true);

    auto candidates = get_candidates_for_leveled_strategy(*cf);
    sstables::size_tiered_compaction_strategy_options stcs_options;
    leveled_manifest manifest = leveled_manifest::create(cf.as_table_state(), candidates, max_sstable_size_in_mb, stcs_options);
    BOOST_REQUIRE(manifest.get_level_size(0) == 1);
    BOOST_REQUIRE(manifest.get_level_size(1) == 2);
    BOOST_REQUIRE(manifest.get_level_size(2) == 1);

    // checks scores; used to determine the level of compaction to proceed with.
    auto level1_score = (double) manifest.get_total_bytes(manifest.get_level(1)) / (double) manifest.max_bytes_for_level(1);
    BOOST_REQUIRE(level1_score > 1.001);
    auto level2_score = (double) manifest.get_total_bytes(manifest.get_level(2)) / (double) manifest.max_bytes_for_level(2);
    BOOST_REQUIRE(level2_score < 1.001);

    std::vector<std::optional<dht::decorated_key>> last_compacted_keys(leveled_manifest::MAX_LEVELS);
    std::vector<int> compaction_counter(leveled_manifest::MAX_LEVELS);
    auto candidate = manifest.get_compaction_candidates(last_compacted_keys, compaction_counter);
    BOOST_REQUIRE(candidate.sstables.size() == 2);
    BOOST_REQUIRE(candidate.level == 2);

    std::set<unsigned long> levels = { 1, 2 };
    for (auto& sst : candidate.sstables) {
        BOOST_REQUIRE(levels.contains(sst->get_sstable_level()));
        levels.erase(sst->get_sstable_level());
    }
    BOOST_REQUIRE(levels.empty());
  });
}

SEASTAR_TEST_CASE(leveled_05) {
    // NOTE: Generations from 48 to 51 are used here.
    return test_env::do_with_async([] (test_env& env) {
        static constexpr size_t sstables_in_round = 2;

        // Check compaction code with leveled strategy. In this test, two sstables of level 0 will be created.
        auto res = compact_sstables(env, {}, sstables_in_round, 1024*1024, compaction_strategy_type::leveled).get();
        BOOST_REQUIRE_EQUAL(res.input_sstables.size(), sstables_in_round);

        for (const auto& sst : res.output_sstables) {
            BOOST_REQUIRE(sst->data_size() >= 1024*1024);
        }
    });
}

SEASTAR_TEST_CASE(leveled_06) {
    // Test that we can compact a single L1 compaction into an empty L2.
  return test_env::do_with_async([] (test_env& env) {
    auto cf = env.make_table_for_tests();
    auto stop_cf = deferred_stop(cf);

    auto max_sstable_size_in_mb = 1;
    auto max_sstable_size_in_bytes = max_sstable_size_in_mb*1024*1024;

    auto max_bytes_for_l1 = leveled_manifest::max_bytes_for_level(1, max_sstable_size_in_bytes);
    // Create fake sstable that will be compacted into L2.
    const auto key = tests::generate_partition_key(cf.schema());
    auto sst1 = add_sstable_for_leveled_test(env, cf, /*data_size*/max_bytes_for_l1*2, /*level*/1, key.key(), key.key());
    BOOST_REQUIRE(cf->get_sstables()->size() == 1);

    auto candidates = get_candidates_for_leveled_strategy(*cf);
    sstables::size_tiered_compaction_strategy_options stcs_options;
    leveled_manifest manifest = leveled_manifest::create(cf.as_table_state(), candidates, max_sstable_size_in_mb, stcs_options);
    BOOST_REQUIRE(manifest.get_level_size(0) == 0);
    BOOST_REQUIRE(manifest.get_level_size(1) == 1);
    BOOST_REQUIRE(manifest.get_level_size(2) == 0);

    std::vector<std::optional<dht::decorated_key>> last_compacted_keys(leveled_manifest::MAX_LEVELS);
    std::vector<int> compaction_counter(leveled_manifest::MAX_LEVELS);
    auto candidate = manifest.get_compaction_candidates(last_compacted_keys, compaction_counter);
    BOOST_REQUIRE(candidate.level == 2);
    BOOST_REQUIRE(candidate.sstables.size() == 1);
    auto& sst = (candidate.sstables)[0];
    BOOST_REQUIRE(sst->get_sstable_level() == 1);
    BOOST_REQUIRE(sst == sst1);
  });
}

SEASTAR_TEST_CASE(leveled_07) {
  return test_env::do_with_async([] (test_env& env) {
    auto cf = env.make_table_for_tests();
    auto stop_cf = deferred_stop(cf);

    const auto key = tests::generate_partition_key(cf.schema());
    for (auto i = 0; i < leveled_manifest::MAX_COMPACTING_L0*2; i++) {
        add_sstable_for_leveled_test(env, cf, 1024*1024, /*level*/0, key.key(), key.key(), i /* max timestamp */);
    }
    auto candidates = get_candidates_for_leveled_strategy(*cf);
    sstables::size_tiered_compaction_strategy_options stcs_options;
    leveled_manifest manifest = leveled_manifest::create(cf.as_table_state(), candidates, 1, stcs_options);
    std::vector<std::optional<dht::decorated_key>> last_compacted_keys(leveled_manifest::MAX_LEVELS);
    std::vector<int> compaction_counter(leveled_manifest::MAX_LEVELS);
    auto desc = manifest.get_compaction_candidates(last_compacted_keys, compaction_counter);
    BOOST_REQUIRE(desc.level == 1);
    BOOST_REQUIRE(desc.sstables.size() == leveled_manifest::MAX_COMPACTING_L0);
    // check that strategy returns the oldest sstables
    for (auto& sst : desc.sstables) {
        BOOST_REQUIRE(sst->get_stats_metadata().max_timestamp < leveled_manifest::MAX_COMPACTING_L0);
    }
  });
}

SEASTAR_TEST_CASE(leveled_invariant_fix) {
  return test_env::do_with_async([] (test_env& env) {
    auto cf = env.make_table_for_tests();
    auto stop_cf = deferred_stop(cf);

    auto sstables_no = cf.schema()->max_compaction_threshold();
    auto keys = tests::generate_partition_keys(sstables_no, cf.schema());
    auto min_key = keys.front();
    auto max_key = keys.back();
    auto sstable_max_size = 1024*1024;

    // add non overlapping with min token to be discarded by strategy
    auto sst0 = add_sstable_for_leveled_test(env, cf, sstable_max_size, /*level*/1, min_key.key(), min_key.key());

    std::unordered_set<sstables::shared_sstable> expected;
    for (auto i = 1; i < sstables_no-1; i++) {
        expected.insert(add_sstable_for_leveled_test(env, cf, sstable_max_size, /*level*/1, keys[i].key(), keys[i].key()));
    }
    // add large token span sstable into level 1, which overlaps with all sstables added in loop above.
    expected.insert(add_sstable_for_leveled_test(env, cf, sstable_max_size, 1, keys[1].key(), max_key.key()));

    auto candidates = get_candidates_for_leveled_strategy(*cf);
    sstables::size_tiered_compaction_strategy_options stcs_options;
    leveled_manifest manifest = leveled_manifest::create(cf.as_table_state(), candidates, 1, stcs_options);
    std::vector<std::optional<dht::decorated_key>> last_compacted_keys(leveled_manifest::MAX_LEVELS);
    std::vector<int> compaction_counter(leveled_manifest::MAX_LEVELS);

    auto candidate = manifest.get_compaction_candidates(last_compacted_keys, compaction_counter);
    BOOST_REQUIRE(candidate.level == 1);
    BOOST_REQUIRE(candidate.sstables.size() == size_t(sstables_no-1));
    BOOST_REQUIRE(std::ranges::all_of(candidate.sstables, [&] (auto& sst) {
        return expected.erase(sst);
    }));
    BOOST_REQUIRE(expected.empty());
  });
}

SEASTAR_TEST_CASE(leveled_stcs_on_L0) {
  return test_env::do_with_async([] (test_env& env) {
    schema_builder builder(some_keyspace, some_column_family);
    builder.with_column("p1", utf8_type, column_kind::partition_key);
    builder.set_min_compaction_threshold(4);
    auto s = builder.build(schema_builder::compact_storage::no);

    auto cf = env.make_table_for_tests(s);
    auto stop_cf = deferred_stop(cf);

    const auto keys = tests::generate_partition_keys(1, cf.schema());
    auto sstable_max_size_in_mb = 1;
    auto l0_sstables_no = s->min_compaction_threshold();
    // we don't want level 0 to be worth promoting.
    auto l0_sstables_size = (sstable_max_size_in_mb*1024*1024)/(l0_sstables_no+1);

    add_sstable_for_leveled_test(env, cf, sstable_max_size_in_mb*1024*1024, /*level*/1, keys[0].key(), keys[0].key());

    std::unordered_set<sstables::shared_sstable> expected;
    for (auto gen = 0; gen < l0_sstables_no; gen++) {
        expected.insert(add_sstable_for_leveled_test(env, cf, l0_sstables_size, /*level*/0, keys[0].key(), keys[0].key()));
    }
    auto candidates = get_candidates_for_leveled_strategy(*cf);
    BOOST_REQUIRE(candidates.size() == size_t(l0_sstables_no+1));
    BOOST_REQUIRE(cf->get_sstables()->size() == size_t(l0_sstables_no+1));

    std::vector<std::optional<dht::decorated_key>> last_compacted_keys(leveled_manifest::MAX_LEVELS);
    std::vector<int> compaction_counter(leveled_manifest::MAX_LEVELS);
    sstables::size_tiered_compaction_strategy_options stcs_options;

    {
        leveled_manifest manifest = leveled_manifest::create(cf.as_table_state(), candidates, sstable_max_size_in_mb, stcs_options);
        BOOST_REQUIRE(!manifest.worth_promoting_L0_candidates(manifest.get_level(0)));
        auto candidate = manifest.get_compaction_candidates(last_compacted_keys, compaction_counter);
        BOOST_REQUIRE(candidate.level == 0);
        BOOST_REQUIRE(candidate.sstables.size() == size_t(l0_sstables_no));
        BOOST_REQUIRE(std::ranges::all_of(candidate.sstables, [&] (auto& sst) {
            return expected.erase(sst);
        }));
        BOOST_REQUIRE(expected.empty());
    }
    {
        candidates.resize(2);
        leveled_manifest manifest = leveled_manifest::create(cf.as_table_state(), candidates, sstable_max_size_in_mb, stcs_options);
        auto candidate = manifest.get_compaction_candidates(last_compacted_keys, compaction_counter);
        BOOST_REQUIRE(candidate.level == 0);
        BOOST_REQUIRE(candidate.sstables.empty());
    }
  });
}

SEASTAR_TEST_CASE(overlapping_starved_sstables_test) {
  return test_env::do_with_async([] (test_env& env) {
    auto cf = env.make_table_for_tests();
    auto stop_cf = deferred_stop(cf);

    const auto keys = tests::generate_partition_keys(5, cf.schema());
    auto min_key = keys.front();
    auto max_sstable_size_in_mb = 1;
    auto max_sstable_size_in_bytes = max_sstable_size_in_mb*1024*1024;

    // we compact 2 sstables: 0->2 in L1 and 0->1 in L2, and rely on strategy
    // to bring a sstable from level 3 that theoretically wasn't compacted
    // for many rounds and won't introduce an overlap.
    auto max_bytes_for_l1 = leveled_manifest::max_bytes_for_level(1, max_sstable_size_in_bytes);
    add_sstable_for_leveled_test(env, cf, max_bytes_for_l1*1.1, /*level*/1, min_key.key(), keys[2].key());
    add_sstable_for_leveled_test(env, cf, max_sstable_size_in_bytes, /*level*/2, min_key.key(), keys[1].key());
    add_sstable_for_leveled_test(env, cf, max_sstable_size_in_bytes, /*level*/3, min_key.key(), keys[1].key());

    std::vector<std::optional<dht::decorated_key>> last_compacted_keys(leveled_manifest::MAX_LEVELS);
    std::vector<int> compaction_counter(leveled_manifest::MAX_LEVELS);
    // make strategy think that level 3 wasn't compacted for many rounds
    compaction_counter[3] = leveled_manifest::NO_COMPACTION_LIMIT+1;

    auto candidates = get_candidates_for_leveled_strategy(*cf);
    sstables::size_tiered_compaction_strategy_options stcs_options;
    leveled_manifest manifest = leveled_manifest::create(cf.as_table_state(), candidates, max_sstable_size_in_mb, stcs_options);
    auto candidate = manifest.get_compaction_candidates(last_compacted_keys, compaction_counter);
    BOOST_REQUIRE(candidate.level == 2);
    BOOST_REQUIRE(candidate.sstables.size() == 3);
  });
}

SEASTAR_TEST_CASE(check_overlapping) {
  return test_env::do_with_async([] (test_env& env) {
    auto cf = env.make_table_for_tests();
    auto stop_cf = deferred_stop(cf);

    const auto keys = tests::generate_partition_keys(4, cf.schema());
    const auto& min_key = keys.front();
    const auto& max_key = keys.back();

    auto sst1 = add_sstable_for_overlapping_test(env, cf, min_key.key(), keys[1].key());
    auto sst2 = add_sstable_for_overlapping_test(env, cf, min_key.key(), keys[2].key());
    auto sst3 = add_sstable_for_overlapping_test(env, cf, keys[3].key(), max_key.key());
    auto sst4 = add_sstable_for_overlapping_test(env, cf, min_key.key(), max_key.key());
    BOOST_REQUIRE(cf->get_sstables()->size() == 4);

    std::vector<shared_sstable> compacting = { sst1, sst2 };
    std::vector<shared_sstable> uncompacting = { sst3, sst4 };

    auto overlapping_sstables = leveled_manifest::overlapping(*cf.schema(), compacting, uncompacting);
    BOOST_REQUIRE(overlapping_sstables.size() == 1);
    BOOST_REQUIRE(overlapping_sstables.front() == sst4);
  });
}

SEASTAR_TEST_CASE(tombstone_purge_test) {
    BOOST_REQUIRE(smp::count == 1);
    return test_env::do_with_async([] (test_env& env) {
        // In a column family with gc_grace_seconds set to 0, check that a tombstone
        // is purged after compaction.
        auto builder = schema_builder("tests", "tombstone_purge")
                .with_column("id", utf8_type, column_kind::partition_key)
                .with_column("value", int32_type);
        builder.set_gc_grace_seconds(0);
        auto s = builder.build();

        auto sst_gen = env.make_sst_factory(s);

        auto compact = [&, s] (std::vector<shared_sstable> all, std::vector<shared_sstable> to_compact) -> std::vector<shared_sstable> {
            auto cf = env.make_table_for_tests(s);
            auto stop_cf = deferred_stop(cf);
            for (auto&& sst : all) {
                column_family_test(cf).add_sstable(sst).get();
            }
            return compact_sstables(env, sstables::compaction_descriptor(to_compact), cf, sst_gen).get().new_sstables;
        };

        auto next_timestamp = [] {
            static thread_local api::timestamp_type next = 1;
            return next++;
        };

        auto make_insert = [&] (partition_key key) {
            mutation m(s, key);
            auto timestamp = next_timestamp();
            testlog.info("make_insert: key={} timestamp={}", dht::decorate_key(*s, key), timestamp);
            m.set_clustered_cell(clustering_key::make_empty(), bytes("value"), data_value(int32_t(1)), timestamp);
            return m;
        };

        auto make_expiring = [&] (partition_key key, int ttl) {
            mutation m(s, key);
            auto timestamp = next_timestamp();
            testlog.info("make_expliring: key={} ttl={} timestamp={}", dht::decorate_key(*s, key), ttl, timestamp);
            m.set_clustered_cell(clustering_key::make_empty(), bytes("value"), data_value(int32_t(1)),
                timestamp, gc_clock::duration(ttl));
            return m;
        };

        auto make_delete = [&] (partition_key key, gc_clock::time_point deletion_time = gc_clock::now()) {
            mutation m(s, key);
            tombstone tomb(next_timestamp(), deletion_time);
            testlog.info("make_delete: {}", tomb);
            m.partition().apply(tomb);
            return m;
        };

        auto assert_that_produces_dead_cell = [&] (auto& sst, partition_key& key) {
            auto reader = make_lw_shared<mutation_reader>(sstable_reader(sst, s, env.make_reader_permit()));
            read_mutation_from_mutation_reader(*reader).then([reader, s, &key] (mutation_opt m) {
                BOOST_REQUIRE(m);
                BOOST_REQUIRE(m->key().equal(*s, key));
                auto rows = m->partition().clustered_rows();
                BOOST_REQUIRE_EQUAL(rows.calculate_size(), 1);
                auto& row = rows.begin()->row();
                auto& cells = row.cells();
                BOOST_REQUIRE_EQUAL(cells.size(), 1);
                auto& cdef = *s->get_column_definition("value");
                BOOST_REQUIRE(!cells.cell_at(cdef.id).as_atomic_cell(cdef).is_live());
                return (*reader)();
            }).then([reader, s] (mutation_fragment_v2_opt m) {
                BOOST_REQUIRE(!m);
            }).finally([reader] {
                return reader->close();
            }).get();
        };

        auto alpha = partition_key::from_exploded(*s, {to_bytes("alpha")});
        auto beta = partition_key::from_exploded(*s, {to_bytes("beta")});

        auto ttl = 10;

        {
            auto mut1 = make_insert(alpha);
            auto mut2 = make_insert(beta);
            auto mut3 = make_delete(alpha);

            std::vector<shared_sstable> sstables = {
                    make_sstable_containing(sst_gen, {mut1, mut2}),
                    make_sstable_containing(sst_gen, {mut3})
            };

            forward_jump_clocks(std::chrono::seconds(ttl));

            auto result = compact(sstables, sstables);
            BOOST_REQUIRE_EQUAL(1, result.size());

            assert_that(sstable_reader(result[0], s, env.make_reader_permit()))
                    .produces(mut2)
                    .produces_end_of_stream();
        }

        {
            auto mut1 = make_insert(alpha);
            auto mut2 = make_insert(alpha);
            auto mut3 = make_delete(alpha);

            auto sst1 = make_sstable_containing(sst_gen, {mut1});
            auto sst2 = make_sstable_containing(sst_gen, {mut2, mut3});

            forward_jump_clocks(std::chrono::seconds(ttl));

            auto result = compact({sst1, sst2}, {sst2});
            BOOST_REQUIRE_EQUAL(1, result.size());

            assert_that(sstable_reader(result[0], s, env.make_reader_permit()))
                    .produces(mut3)
                    .produces_end_of_stream();
        }

        {
            auto mut1 = make_insert(alpha);
            auto mut2 = make_delete(alpha);
            auto mut3 = make_insert(beta);
            auto mut4 = make_insert(alpha);

            auto sst1 = make_sstable_containing(sst_gen, {mut1, mut2, mut3});
            auto sst2 = make_sstable_containing(sst_gen, {mut4});

            forward_jump_clocks(std::chrono::seconds(ttl));

            auto result = compact({sst1, sst2}, {sst1});
            BOOST_REQUIRE_EQUAL(1, result.size());

            assert_that(sstable_reader(result[0], s, env.make_reader_permit()))
                    .produces(mut3)
                    .produces_end_of_stream();
        }

        {
            auto mut1 = make_insert(alpha);
            auto mut2 = make_delete(alpha);
            auto mut3 = make_insert(beta);
            auto mut4 = make_insert(beta);

            auto sst1 = make_sstable_containing(sst_gen, {mut1, mut2, mut3});
            auto sst2 = make_sstable_containing(sst_gen, {mut4});

            forward_jump_clocks(std::chrono::seconds(ttl));

            auto result = compact({sst1, sst2}, {sst1});
            BOOST_REQUIRE_EQUAL(1, result.size());

            assert_that(sstable_reader(result[0], s, env.make_reader_permit()))
                    .produces(mut3)
                    .produces_end_of_stream();
        }

        {
            // check that expired cell will not be purged if it will resurrect overwritten data.
            auto mut1 = make_insert(alpha);
            auto mut2 = make_expiring(alpha, ttl);

            auto sst1 = make_sstable_containing(sst_gen, {mut1});
            auto sst2 = make_sstable_containing(sst_gen, {mut2});

            forward_jump_clocks(std::chrono::seconds(ttl));

            auto result = compact({sst1, sst2}, {sst2});
            BOOST_REQUIRE_EQUAL(1, result.size());
            assert_that_produces_dead_cell(result[0], alpha);

            result = compact({sst1, sst2}, {sst1, sst2});
            BOOST_REQUIRE_EQUAL(0, result.size());
        }
        {
            auto mut1 = make_insert(alpha);
            auto mut2 = make_expiring(beta, ttl);

            auto sst1 = make_sstable_containing(sst_gen, {mut1});
            auto sst2 = make_sstable_containing(sst_gen, {mut2});

            forward_jump_clocks(std::chrono::seconds(ttl));

            auto result = compact({sst1, sst2}, {sst2});
            BOOST_REQUIRE_EQUAL(0, result.size());
        }
        {
            auto mut1 = make_insert(alpha);
            auto mut2 = make_expiring(alpha, ttl);
            auto mut3 = make_insert(beta);

            auto sst1 = make_sstable_containing(sst_gen, {mut1});
            auto sst2 = make_sstable_containing(sst_gen, {mut2, mut3});

            forward_jump_clocks(std::chrono::seconds(ttl));

            auto result = compact({sst1, sst2}, {sst1, sst2});
            BOOST_REQUIRE_EQUAL(1, result.size());
            assert_that(sstable_reader(result[0], s, env.make_reader_permit()))
                    .produces(mut3)
                    .produces_end_of_stream();
        }
        {
            // We use int32_t for representing a timestamp in seconds since the
            // UNIX epoch. This timestamp "local_deletion_time" (ldt for short)
            // notes the time at which a tombstone was created. It is used for
            // purging the tombstone after gc_grace_seconds.
            //
            // If ldt is greater than INT32_MAX (2147483647), then it cannot be
            // represented using a int32_t. probably more importantly, it
            // represents a time point too far in the future -- after 19 Jan 2028.
            // so the tombstone would practically live with us forever. so, the
            // sstable writer just caps it to INT32_MAX - 1 when reading a
            // tombstone with a TTL after this timepoint. and we also consider
            // it as the indication of a problem and report it using the metrics
            // named "scylla_sstables_capped_tombstone_deletion_time" which
            // notes the total number of tombstones whose deletion_time breaches
            // the limit.
            //
            // This test verifies that the metrics reflecting the number of
            // tombstones with far-into-the-future ldts by inserting tombstones
            // with a ldt greater than the date.
            auto deletion_time = gc_clock::from_time_t(sstables::max_deletion_time + 1);
            auto sst1 = make_sstable_containing(sst_gen,
                                                {make_insert(alpha),
                                                 make_delete(alpha, deletion_time)},
                                                validate::no);
            auto result = compact({sst1}, {sst1});
            BOOST_CHECK_EQUAL(1, sstables_stats::get_shard_stats().capped_tombstone_deletion_time);
        }
        {
            // Verify that old live data inhibit tombstone_gc of partition tombstone
            auto mut1 = make_insert(alpha);
            auto mut2 = make_delete(alpha);
            auto mut3 = make_insert(beta);

            auto sst1 = make_sstable_containing(sst_gen, {mut1});
            auto sst2 = make_sstable_containing(sst_gen, {mut2, mut3});

            forward_jump_clocks(std::chrono::seconds(1));

            auto result = compact({sst1, sst2}, {sst2});
            BOOST_REQUIRE_EQUAL(1, result.size());
            assert_that(sstable_reader(result[0], s, env.make_reader_permit()))
                    .produces(mut3)
                    .produces(mut2)
                    .produces_end_of_stream();
        }
        {
            // Verify that old deleted data do not inhibit tombstone_gc of partition tombstone
            auto mut1 = make_delete(alpha);
            auto mut2 = make_delete(alpha);
            auto mut3 = make_insert(beta);

            auto sst1 = make_sstable_containing(sst_gen, {mut1});
            auto sst2 = make_sstable_containing(sst_gen, {mut2, mut3});

            forward_jump_clocks(std::chrono::seconds(1));

            auto result = compact({sst1, sst2}, {sst2});
            BOOST_REQUIRE_EQUAL(1, result.size());
            assert_that(sstable_reader(result[0], s, env.make_reader_permit()))
                    .produces(mut3)
                    .produces_end_of_stream();
        }
        {
            // Verify that old live data inhibit tombstone_gc of expired cell
            auto mut1 = make_insert(alpha);
            auto mut2 = make_expiring(alpha, ttl);

            auto sst1 = make_sstable_containing(sst_gen, {mut1});
            auto sst2 = make_sstable_containing(sst_gen, {mut2});

            forward_jump_clocks(std::chrono::seconds(ttl));

            auto result = compact({sst1, sst2}, {sst2});
            BOOST_REQUIRE_EQUAL(1, result.size());
            assert_that_produces_dead_cell(result[0], alpha);
        }
        {
            // Verify that old deleted data do not inhibit tombstone_gc of expired cell
            auto mut1 = make_delete(alpha);
            auto mut2 = make_expiring(alpha, ttl);

            auto sst1 = make_sstable_containing(sst_gen, {mut1});
            auto sst2 = make_sstable_containing(sst_gen, {mut2});

            forward_jump_clocks(std::chrono::seconds(ttl));

            auto result = compact({sst1, sst2}, {sst2});
            BOOST_REQUIRE_EQUAL(0, result.size());
        }
    });
}

SEASTAR_TEST_CASE(mv_tombstone_purge_test) {
    BOOST_REQUIRE(smp::count == 1);
    return test_env::do_with_async([] (test_env& env) {
        // In a column family with gc_grace_seconds set to 0, check that a tombstone
        // is purged after compaction.
        auto builder = schema_builder("tests", "tombstone_purge")
                .with_column("id", utf8_type, column_kind::partition_key)
                .with_column("ck", int32_type, column_kind::clustering_key)
                .with_column("value", int32_type);
        builder.set_gc_grace_seconds(0);
        auto s = builder.build();

        auto sst_gen = env.make_sst_factory(s);

        auto compact = [&, s] (std::vector<shared_sstable> all, std::vector<shared_sstable> to_compact) -> std::vector<shared_sstable> {
            auto cf = env.make_table_for_tests(s);
            auto stop_cf = deferred_stop(cf);
            for (auto&& sst : all) {
                column_family_test(cf).add_sstable(sst).get();
            }
            return compact_sstables(env, sstables::compaction_descriptor(to_compact), cf, sst_gen).get().new_sstables;
        };

        auto next_timestamp = [] {
            static thread_local api::timestamp_type next = 1;
            return next++;
        };

        auto make_insert = [&] (partition_key key, int32_t ck = 0, int32_t value = 1, std::optional<api::timestamp_type> timestamp = std::nullopt, std::optional<api::timestamp_type> created_at = std::nullopt) {
            mutation m(s, key);
            if (!timestamp) {
                created_at = timestamp = next_timestamp();
            } else if (!created_at) {
                created_at = next_timestamp();
            }
            auto c_key = clustering_key::from_single_value(*s, int32_type->decompose(data_value(ck)));
            testlog.info("make_insert: key={} ck={} timestamp={} created_at={}", dht::decorate_key(*s, key), ck, *timestamp, *created_at);
            m.set_clustered_cell(
                    c_key,
                    bytes("value"),
                    data_value(value),
                    *timestamp);
            m.partition().clustered_row(*s, c_key).apply(row_marker(*created_at));
            return m;
        };

        auto make_delete_row = [&] (partition_key key, int32_t ck = 0, gc_clock::time_point deletion_time = gc_clock::now(), std::optional<api::timestamp_type> deleted_at = std::nullopt) {
            mutation m(s, key);
            if (!deleted_at) {
                deleted_at = next_timestamp();
            }
            shadowable_tombstone shadowable(*deleted_at, deletion_time);
            auto c_key = clustering_key::from_single_value(*s, int32_type->decompose(data_value(ck)));
            testlog.info("make_delete_row: key={} ck={} shadowable_tombstone={}", dht::decorate_key(*s, key), ck, shadowable);
            m.partition().clustered_row(*s, c_key).apply(shadowable);
            return m;
        };

        auto alpha = partition_key::from_exploded(*s, {to_bytes("alpha")});

        {
            // Simulate materialized views update
            // We expect mut2 to delete mut1, and be purged
            // Since the shadowable tombstone in mut4 is ignored as it is dead
            // and insert in mut5 has higher timestamp.
            // This will leave only the insert in mut3 when compacting mut1-3 together.
            //
            // cql commands that reproduce the following mutation:
            //  create keyspace ks with replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1};
            //  use ks;
            //  create table base_table (id text primary key, ck int, value int);
            //  create materialized view mv as select id, ck, value from base_table where id is not null and ck is not null primary key (id, ck);
            //
            //  insert into base_table (id, ck, value) values ('alpha', 1, 1) using timestamp 1;
            auto mut1 = make_insert(alpha, 1, 1, api::timestamp_type(1), api::timestamp_type(1));
            //  insert into base_table (id, ck) values ('alpha', 2) using timestamp 2;
            auto mut2 = make_delete_row(alpha, 1, gc_clock::now(), api::timestamp_type(1));
            auto mut3 = make_insert(alpha, 2, 1, api::timestamp_type(1), api::timestamp_type(2));
            //  insert into base_table (id, ck) values ('alpha', 3) using timestamp 3;
            auto mut4 = make_delete_row(alpha, 2, gc_clock::now(), api::timestamp_type(2));
            auto mut5 = make_insert(alpha, 3, 1, api::timestamp_type(1), api::timestamp_type(3));

            auto sst1 = make_sstable_containing(sst_gen, {mut1});
            auto sst2 = make_sstable_containing(sst_gen, {mut2, mut3});
            auto sst3 = make_sstable_containing(sst_gen, {mut4, mut5});

            forward_jump_clocks(std::chrono::seconds(1));

            auto result = compact({sst1, sst2, sst3}, {sst1, sst2});
            BOOST_REQUIRE_EQUAL(1, result.size());
            assert_that(sstable_reader(result[0], s, env.make_reader_permit()))
                    .produces(mut3)
                    .produces_end_of_stream();
        }
    });
}

SEASTAR_TEST_CASE(sstable_rewrite) {
    BOOST_REQUIRE(smp::count == 1);
    return test_env::do_with_async([] (test_env& env) {
        auto s = schema_builder(some_keyspace, some_column_family)
                .with_column("p1", utf8_type, column_kind::partition_key)
                .with_column("c1", utf8_type, column_kind::clustering_key)
                .with_column("r1", utf8_type)
                .build();
        auto sst_gen = env.make_sst_factory(s);

        const column_definition& r1_col = *s->get_column_definition("r1");

        auto key_for_this_shard = tests::generate_partition_keys(1, s);
        auto c_key = clustering_key::from_exploded(*s, {to_bytes("c1")});
        mutation mut(s, key_for_this_shard[0]);
        mut.set_clustered_cell(c_key, r1_col, make_atomic_cell(utf8_type, bytes("a")));

        auto sstp = make_sstable_containing(sst_gen, {std::move(mut)});
        auto key = key_for_this_shard[0];
        std::vector<sstables::shared_sstable> new_tables;
        auto creator = [&] {
            auto sst = sst_gen();
            new_tables.emplace_back(sst);
            return sst;
        };
        auto cf = env.make_table_for_tests(s);
        auto stop_cf = deferred_stop(cf);
        std::vector<shared_sstable> sstables;
        sstables.push_back(std::move(sstp));

        compact_sstables(env, sstables::compaction_descriptor(std::move(sstables)), cf, creator).get();
        BOOST_REQUIRE(new_tables.size() == 1);
        auto newsst = new_tables[0];
        auto reader = sstable_reader(newsst, s, env.make_reader_permit());
        auto close_reader = deferred_close(reader);
        auto m = reader().get();
        BOOST_REQUIRE(m);
        BOOST_REQUIRE(m->is_partition_start());
        BOOST_REQUIRE(m->as_partition_start().key().equal(*s, key));
        reader.next_partition().get();
        m = reader().get();
        BOOST_REQUIRE(!m);
    });
}


SEASTAR_TEST_CASE(test_sstable_max_local_deletion_time_2) {
    // Create sstable A with 5x column with TTL 100 and 1x column with TTL 1000
    // Create sstable B with tombstone for column in sstable A with TTL 1000.
    // Compact them and expect that maximum deletion time is that of column with TTL 100.
    return test_env::do_with_async([] (test_env& env) {
            for (auto version : writable_sstable_versions) {
                schema_builder builder(some_keyspace, some_column_family);
                builder.with_column("p1", utf8_type, column_kind::partition_key);
                builder.with_column("c1", utf8_type, column_kind::clustering_key);
                builder.with_column("r1", utf8_type);
                schema_ptr s = builder.build(schema_builder::compact_storage::no);
                auto cf = env.make_table_for_tests(s);
                auto close_cf = deferred_stop(cf);
                auto sst_gen = env.make_sst_factory(s, version);
                auto mt = make_lw_shared<replica::memtable>(s);
                auto now = gc_clock::now();
                int32_t last_expiry = 0;
                auto add_row = [&now, &mt, &s, &last_expiry](mutation &m, bytes column_name, uint32_t ttl) {
                    auto c_key = clustering_key::from_exploded(*s, {column_name});
                    last_expiry = (now + gc_clock::duration(ttl)).time_since_epoch().count();
                    m.set_clustered_cell(c_key, *s->get_column_definition("r1"),
                                         make_atomic_cell(utf8_type, bytes(""), ttl, last_expiry));
                    mt->apply(std::move(m));
                };

                mutation m(s, partition_key::from_exploded(*s, {to_bytes("deletetest")}));
                for (auto i = 0; i < 5; i++) {
                    add_row(m, to_bytes("deletecolumn" + to_sstring(i)), 100);
                }
                add_row(m, to_bytes("todelete"), 1000);
                auto sst1 = make_sstable_containing(sst_gen, mt);
                BOOST_REQUIRE(last_expiry == sst1->get_stats_metadata().max_local_deletion_time);

                mt = make_lw_shared<replica::memtable>(s);
                m = mutation(s, partition_key::from_exploded(*s, {to_bytes("deletetest")}));
                tombstone tomb(api::new_timestamp(), now);
                m.partition().apply_delete(*s, clustering_key::from_exploded(*s, {to_bytes("todelete")}), tomb);
                mt->apply(std::move(m));
                auto sst2 = make_sstable_containing(sst_gen, mt);
                BOOST_REQUIRE(now.time_since_epoch().count() == sst2->get_stats_metadata().max_local_deletion_time);

                auto creator = sst_gen;
                auto info = compact_sstables(env, sstables::compaction_descriptor({sst1, sst2}), cf, creator).get();
                BOOST_REQUIRE(info.new_sstables.size() == 1);
                BOOST_REQUIRE(((now + gc_clock::duration(100)).time_since_epoch().count()) ==
                              info.new_sstables.front()->get_stats_metadata().max_local_deletion_time);
            }
    });
}

static stats_metadata build_stats(int64_t min_timestamp, int64_t max_timestamp, int32_t max_local_deletion_time) {
    stats_metadata stats = {};
    stats.min_timestamp = min_timestamp;
    stats.max_timestamp = max_timestamp;
    stats.max_local_deletion_time = max_local_deletion_time;
    return stats;
}

SEASTAR_TEST_CASE(get_fully_expired_sstables_test) {
  return test_env::do_with_async([] (test_env& env) {
    const auto keys = tests::generate_partition_keys(4, table_for_tests::make_default_schema());
    const auto& min_key = keys.front();
    const auto& max_key = keys.back();

    auto t0 = gc_clock::from_time_t(1).time_since_epoch().count();
    auto t1 = gc_clock::from_time_t(10).time_since_epoch().count();
    auto t2 = gc_clock::from_time_t(15).time_since_epoch().count();
    auto t3 = gc_clock::from_time_t(20).time_since_epoch().count();
    auto t4 = gc_clock::from_time_t(30).time_since_epoch().count();

    {
        auto cf = env.make_table_for_tests();
        auto close_cf = deferred_stop(cf);

        auto sst1 = add_sstable_for_overlapping_test(env, cf, min_key.key(), keys[1].key(), build_stats(t0, t1, t1));
        auto sst2 = add_sstable_for_overlapping_test(env, cf, min_key.key(), keys[2].key(), build_stats(t0, t1, std::numeric_limits<int32_t>::max()));
        auto sst3 = add_sstable_for_overlapping_test(env, cf, min_key.key(), max_key.key(), build_stats(t3, t4, std::numeric_limits<int32_t>::max()));
        std::vector<sstables::shared_sstable> compacting = { sst1, sst2 };
        auto expired = get_fully_expired_sstables(cf.as_table_state(), compacting, /*gc before*/gc_clock::from_time_t(15) + cf->schema()->gc_grace_seconds());
        BOOST_REQUIRE(expired.size() == 0);
    }

    {
        auto cf = env.make_table_for_tests();
        auto close_cf = deferred_stop(cf);

        auto sst1 = add_sstable_for_overlapping_test(env, cf, min_key.key(), keys[1].key(), build_stats(t0, t1, t1));
        auto sst2 = add_sstable_for_overlapping_test(env, cf, min_key.key(), keys[2].key(), build_stats(t2, t3, std::numeric_limits<int32_t>::max()));
        auto sst3 = add_sstable_for_overlapping_test(env, cf, min_key.key(), max_key.key(), build_stats(t3, t4, std::numeric_limits<int32_t>::max()));
        std::vector<sstables::shared_sstable> compacting = { sst1, sst2 };
        auto expired = get_fully_expired_sstables(cf.as_table_state(), compacting, /*gc before*/gc_clock::from_time_t(25) + cf->schema()->gc_grace_seconds());
        BOOST_REQUIRE(expired.size() == 1);
        auto expired_sst = *expired.begin();
        BOOST_REQUIRE(expired_sst == sst1);
    }
  });
}

SEASTAR_TEST_CASE(compaction_with_fully_expired_table) {
    return test_env::do_with_async([] (test_env& env) {
        auto builder = schema_builder("la", "cf")
            .with_column("pk", utf8_type, column_kind::partition_key)
            .with_column("ck1", utf8_type, column_kind::clustering_key)
            .with_column("r1", int32_type);

        builder.set_gc_grace_seconds(0);
        auto s = builder.build();

        auto key = partition_key::from_exploded(*s, {to_bytes("key1")});
        auto c_key = clustering_key_prefix::from_exploded(*s, {to_bytes("c1")});
        auto sst_gen = env.make_sst_factory(s);

        mutation m(s, key);
        tombstone tomb(api::new_timestamp(), gc_clock::now() - std::chrono::seconds(3600));
        m.partition().apply_delete(*s, c_key, tomb);
        auto sst = make_sstable_containing(sst_gen, {std::move(m)});

        auto cf = env.make_table_for_tests(s);
        auto close_cf = deferred_stop(cf);

        auto ssts = std::vector<shared_sstable>{ sst };

        auto expired = get_fully_expired_sstables(cf.as_table_state(), ssts, gc_clock::now());
        BOOST_REQUIRE(expired.size() == 1);
        auto expired_sst = *expired.begin();
        BOOST_REQUIRE(expired_sst == sst);

        auto ret = compact_sstables(env, sstables::compaction_descriptor(ssts), cf, sst_gen).get();
        BOOST_REQUIRE(ret.new_sstables.empty());
        BOOST_REQUIRE(ret.stats.end_size == 0);
    });
}

SEASTAR_TEST_CASE(time_window_strategy_time_window_tests) {
    using namespace std::chrono;

    api::timestamp_type tstamp1 = duration_cast<microseconds>(milliseconds(1451001601000L)).count(); // 2015-12-25 @ 00:00:01, in milliseconds
    api::timestamp_type tstamp2 = duration_cast<microseconds>(milliseconds(1451088001000L)).count(); // 2015-12-26 @ 00:00:01, in milliseconds
    api::timestamp_type low_hour = duration_cast<microseconds>(milliseconds(1451001600000L)).count(); // 2015-12-25 @ 00:00:00, in milliseconds


    // A 1 hour window should round down to the beginning of the hour
    BOOST_REQUIRE(time_window_compaction_strategy::get_window_lower_bound(duration_cast<seconds>(hours(1)), tstamp1) == low_hour);

    // A 1 minute window should round down to the beginning of the hour
    BOOST_REQUIRE(time_window_compaction_strategy::get_window_lower_bound(duration_cast<seconds>(minutes(1)), tstamp1) == low_hour);

    // A 1 day window should round down to the beginning of the hour
    BOOST_REQUIRE(time_window_compaction_strategy::get_window_lower_bound(duration_cast<seconds>(hours(24)), tstamp1) == low_hour);

    // The 2 day window of 2015-12-25 + 2015-12-26 should round down to the beginning of 2015-12-25
    BOOST_REQUIRE(time_window_compaction_strategy::get_window_lower_bound(duration_cast<seconds>(hours(24*2)), tstamp2) == low_hour);

    return make_ready_future<>();
}

SEASTAR_TEST_CASE(time_window_strategy_ts_resolution_check) {
  return test_env::do_with([] (test_env& env) {
    auto ts = 1451001601000L; // 2015-12-25 @ 00:00:01, in milliseconds
    auto ts_in_ms = std::chrono::milliseconds(ts);
    auto ts_in_us = std::chrono::duration_cast<std::chrono::microseconds>(ts_in_ms);

    auto s = schema_builder("tests", "time_window_strategy")
            .with_column("id", utf8_type, column_kind::partition_key)
            .with_column("value", int32_type).build();

    const auto key = tests::generate_partition_key(s);

    {
        std::map<sstring, sstring> opts = { { time_window_compaction_strategy_options::TIMESTAMP_RESOLUTION_KEY, "MILLISECONDS" }, };
        time_window_compaction_strategy_options options(opts);

        auto sst = env.make_sstable(s);
        sstables::test(sst).set_values(key.key(), key.key(), build_stats(ts_in_ms.count(), ts_in_ms.count(), std::numeric_limits<int32_t>::max()));

        auto ret = time_window_compaction_strategy::get_buckets({ sst }, options);
        auto expected = time_window_compaction_strategy::get_window_lower_bound(options.get_sstable_window_size(), ts_in_us.count());

        BOOST_REQUIRE(ret.second == expected);
    }

    {
        std::map<sstring, sstring> opts = { { time_window_compaction_strategy_options::TIMESTAMP_RESOLUTION_KEY, "MICROSECONDS" }, };
        time_window_compaction_strategy_options options(opts);

        auto sst = env.make_sstable(s);
        sstables::test(sst).set_values(key.key(), key.key(), build_stats(ts_in_us.count(), ts_in_us.count(), std::numeric_limits<int32_t>::max()));

        auto ret = time_window_compaction_strategy::get_buckets({ sst }, options);
        auto expected = time_window_compaction_strategy::get_window_lower_bound(options.get_sstable_window_size(), ts_in_us.count());

        BOOST_REQUIRE(ret.second == expected);
    }
    return make_ready_future<>();
  });
}

SEASTAR_TEST_CASE(time_window_strategy_correctness_test) {
    using namespace std::chrono;

    return test_env::do_with_async([] (test_env& env) {
        auto builder = schema_builder("tests", "time_window_strategy")
                .with_column("id", utf8_type, column_kind::partition_key)
                .with_column("value", int32_type);
        builder.set_compaction_strategy(sstables::compaction_strategy_type::time_window);
        auto s = builder.build();

        auto make_insert = [&] (partition_key key, api::timestamp_type t) {
            mutation m(s, key);
            m.set_clustered_cell(clustering_key::make_empty(), bytes("value"), data_value(int32_t(1)), t);
            return m;
        };

        api::timestamp_type tstamp = api::timestamp_clock::now().time_since_epoch().count();
        api::timestamp_type tstamp2 = tstamp - duration_cast<microseconds>(seconds(2L * 3600L)).count();

        std::vector<shared_sstable> sstables;

        // create 5 sstables
        for (api::timestamp_type t = 0; t < 3; t++) {
            auto key = partition_key::from_exploded(*s, {to_bytes("key" + to_sstring(t))});
            auto mut = make_insert(std::move(key), t);
            sstables.push_back(make_sstable_containing(env.make_sstable(s), {std::move(mut)}));
        }
        // Decrement the timestamp to simulate a timestamp in the past hour
        for (api::timestamp_type t = 3; t < 5; t++) {
            // And add progressively more cells into each sstable
            auto key = partition_key::from_exploded(*s, {to_bytes("key" + to_sstring(t))});
            auto mut = make_insert(std::move(key), t);
            sstables.push_back(make_sstable_containing(env.make_sstable(s), {std::move(mut)}));
        }

        std::map<sstring, sstring> options;
        time_window_compaction_strategy twcs(options);
        std::map<api::timestamp_type, std::vector<shared_sstable>> buckets;
        auto cf = env.make_table_for_tests(s);
        auto close_cf = deferred_stop(cf);
        auto control = make_strategy_control_for_test(false);

        // We'll put 3 sstables into the newest bucket
        for (api::timestamp_type i = 0; i < 3; i++) {
            auto bound = time_window_compaction_strategy::get_window_lower_bound(duration_cast<seconds>(hours(1)), tstamp);
            buckets[bound].push_back(sstables[i]);
        }

        auto now = api::timestamp_clock::now().time_since_epoch().count();
        auto new_bucket = twcs.newest_bucket(cf.as_table_state(), *control, buckets, 4, 32,
            time_window_compaction_strategy::get_window_lower_bound(duration_cast<seconds>(hours(1)), now));
        // incoming bucket should not be accepted when it has below the min threshold SSTables
        BOOST_REQUIRE(new_bucket.empty());

        now = api::timestamp_clock::now().time_since_epoch().count();
        new_bucket = twcs.newest_bucket(cf.as_table_state(), *control, buckets, 2, 32,
            time_window_compaction_strategy::get_window_lower_bound(duration_cast<seconds>(hours(1)), now));
        // incoming bucket should be accepted when it is larger than the min threshold SSTables
        BOOST_REQUIRE(!new_bucket.empty());

        // And 2 into the second bucket (1 hour back)
        for (api::timestamp_type i = 3; i < 5; i++) {
            auto bound = time_window_compaction_strategy::get_window_lower_bound(duration_cast<seconds>(hours(1)), tstamp2);
            buckets[bound].push_back(sstables[i]);
        }

        // "an sstable with a single value should have equal min/max timestamps"
        for (auto& sst : sstables) {
            BOOST_REQUIRE(sst->get_stats_metadata().min_timestamp == sst->get_stats_metadata().max_timestamp);
        }

        // Test trim
        auto num_sstables = 40;
        for (int r = 5; r < num_sstables; r++) {
            auto key = partition_key::from_exploded(*s, {to_bytes("key" + to_sstring(r))});
            std::vector<mutation> mutations;
            for (int i = 0 ; i < r ; i++) {
                mutations.push_back(make_insert(key, tstamp + r));
            }
            sstables.push_back(make_sstable_containing(env.make_sstable(s), std::move(mutations)));
        }

        // Reset the buckets, overfill it now
        for (int i = 0 ; i < 40; i++) {
            auto bound = time_window_compaction_strategy::get_window_lower_bound(duration_cast<seconds>(hours(1)),
                sstables[i]->get_stats_metadata().max_timestamp);
            buckets[bound].push_back(sstables[i]);
        }

        now = api::timestamp_clock::now().time_since_epoch().count();
        new_bucket = twcs.newest_bucket(cf.as_table_state(), *control, buckets, 4, 32,
            time_window_compaction_strategy::get_window_lower_bound(duration_cast<seconds>(hours(1)), now));
        // new bucket should be trimmed to max threshold of 32
        BOOST_REQUIRE(new_bucket.size() == size_t(32));
    });
}

// Check that TWCS will only perform size-tiered on the current window and also
// the past windows that were already previously compacted into a single SSTable.
SEASTAR_TEST_CASE(time_window_strategy_size_tiered_behavior_correctness) {
    using namespace std::chrono;

    return test_env::do_with_async([] (test_env& env) {
        auto builder = schema_builder("tests", "time_window_strategy")
                .with_column("id", utf8_type, column_kind::partition_key)
                .with_column("value", int32_type);
        builder.set_compaction_strategy(sstables::compaction_strategy_type::time_window);
        auto s = builder.build();

        auto sst_gen = env.make_sst_factory(s);

        auto make_insert = [&] (partition_key key, api::timestamp_type t) {
            mutation m(s, key);
            m.set_clustered_cell(clustering_key::make_empty(), bytes("value"), data_value(int32_t(1)), t);
            return m;
        };

        std::map<sstring, sstring> options;
        time_window_compaction_strategy twcs(options);
        std::map<api::timestamp_type, std::vector<shared_sstable>> buckets; // windows
        int min_threshold = 4;
        int max_threshold = 32;
        auto window_size = duration_cast<seconds>(hours(1));

        auto add_new_sstable_to_bucket = [&] (api::timestamp_type ts, api::timestamp_type window_ts) {
            auto key = partition_key::from_exploded(*s, {to_bytes("key" + to_sstring(ts))});
            auto mut = make_insert(std::move(key), ts);
            auto sst = make_sstable_containing(sst_gen, {std::move(mut)});
            auto bound = time_window_compaction_strategy::get_window_lower_bound(window_size, window_ts);
            buckets[bound].push_back(std::move(sst));
        };

        auto cf = env.make_table_for_tests(s);
        auto close_cf = deferred_stop(cf);
        auto major_compact_bucket = [&] (api::timestamp_type window_ts) {
            auto bound = time_window_compaction_strategy::get_window_lower_bound(window_size, window_ts);
            auto ret = compact_sstables(env, sstables::compaction_descriptor(std::move(buckets[bound])), cf, sst_gen).get();
            BOOST_REQUIRE(ret.new_sstables.size() == 1);
            buckets[bound] = std::move(ret.new_sstables);
        };

        api::timestamp_type current_window_ts = api::timestamp_clock::now().time_since_epoch().count();
        api::timestamp_type past_window_ts = current_window_ts - duration_cast<microseconds>(seconds(2L * 3600L)).count();

        // create 1 sstable into past time window and let the strategy know about it
        add_new_sstable_to_bucket(0, past_window_ts);

        auto now = time_window_compaction_strategy::get_window_lower_bound(window_size, past_window_ts);
        auto control = make_strategy_control_for_test(false);

        // past window cannot be compacted because it has a single SSTable
        BOOST_REQUIRE(twcs.newest_bucket(cf.as_table_state(), *control, buckets, min_threshold, max_threshold, now).size() == 0);

        // create min_threshold-1 sstables into current time window
        for (api::timestamp_type t = 0; t < min_threshold - 1; t++) {
            add_new_sstable_to_bucket(t, current_window_ts);
        }
        // add 1 sstable into past window.
        add_new_sstable_to_bucket(1, past_window_ts);

        now = time_window_compaction_strategy::get_window_lower_bound(window_size, current_window_ts);

        // past window can now be compacted into a single SSTable because it was the previous current (active) window.
        // current window cannot be compacted because it has less than min_threshold SSTables
        BOOST_REQUIRE(twcs.newest_bucket(cf.as_table_state(), *control, buckets, min_threshold, max_threshold, now).size() == 2);

        major_compact_bucket(past_window_ts);

        // now past window cannot be compacted again, because it was already compacted into a single SSTable, now it switches to STCS mode.
        BOOST_REQUIRE(twcs.newest_bucket(cf.as_table_state(), *control, buckets, min_threshold, max_threshold, now).size() == 0);

        // make past window contain more than min_threshold similar-sized SSTables, allowing it to be compacted again.
        for (api::timestamp_type t = 1; t < min_threshold; t++) {
            add_new_sstable_to_bucket(t, past_window_ts);
        }

        // now past window can be compacted again because it switched to STCS mode and has more than min_threshold SSTables.
        BOOST_REQUIRE(twcs.newest_bucket(cf.as_table_state(), *control, buckets, min_threshold, max_threshold, now).size() == size_t(min_threshold));
    });
}

static void check_min_max_column_names(const sstable_ptr& sst, std::vector<bytes> min_components, std::vector<bytes> max_components) {
    const auto& st = sst->get_stats_metadata();
    BOOST_TEST_MESSAGE(fmt::format("min {}/{} max {}/{}", st.min_column_names.elements.size(), min_components.size(), st.max_column_names.elements.size(), max_components.size()));
    BOOST_REQUIRE(st.min_column_names.elements.size() == min_components.size());
    for (auto i = 0U; i < st.min_column_names.elements.size(); i++) {
        BOOST_REQUIRE(min_components[i] == st.min_column_names.elements[i].value);
    }
    BOOST_REQUIRE(st.max_column_names.elements.size() == max_components.size());
    for (auto i = 0U; i < st.max_column_names.elements.size(); i++) {
        BOOST_REQUIRE(max_components[i] == st.max_column_names.elements[i].value);
    }
}

SEASTAR_TEST_CASE(min_max_clustering_key_test_2) {
    return test_env::do_with_async([] (test_env& env) {
        for (const auto version : writable_sstable_versions) {
            auto s = schema_builder("ks", "cf")
                      .with_column("pk", utf8_type, column_kind::partition_key)
                      .with_column("ck1", utf8_type, column_kind::clustering_key)
                      .with_column("r1", int32_type)
                      .build();
            auto cf = env.make_table_for_tests(s);
            auto close_cf = deferred_stop(cf);
            auto sst_gen = env.make_sst_factory(s, version);
            auto mt = make_lw_shared<replica::memtable>(s);
            const column_definition &r1_col = *s->get_column_definition("r1");

            for (auto j = 0; j < 8; j++) {
                auto key = partition_key::from_exploded(*s, {to_bytes("key" + to_sstring(j))});
                mutation m(s, key);
                for (auto i = 100; i < 150; i++) {
                    auto c_key = clustering_key::from_exploded(*s, {to_bytes(to_sstring(j) + "ck" + to_sstring(i))});
                    m.set_clustered_cell(c_key, r1_col, make_atomic_cell(int32_type, int32_type->decompose(1)));
                }
                mt->apply(std::move(m));
            }
            auto sst = make_sstable_containing(sst_gen, mt);
            check_min_max_column_names(sst, {"0ck100"}, {"7ck149"});

            mt = make_lw_shared<replica::memtable>(s);
            auto key = partition_key::from_exploded(*s, {to_bytes("key9")});
            mutation m(s, key);
            for (auto i = 101; i < 299; i++) {
                auto c_key = clustering_key::from_exploded(*s, {to_bytes(to_sstring(9) + "ck" + to_sstring(i))});
                m.set_clustered_cell(c_key, r1_col, make_atomic_cell(int32_type, int32_type->decompose(1)));
            }
            mt->apply(std::move(m));
            auto sst2 = make_sstable_containing(sst_gen, mt);
            check_min_max_column_names(sst2, {"9ck101"}, {"9ck298"});

            auto creator = sst_gen;
            auto info = compact_sstables(env, sstables::compaction_descriptor({sst, sst2}), cf, creator).get();
            BOOST_REQUIRE(info.new_sstables.size() == 1);
            check_min_max_column_names(info.new_sstables.front(), {"0ck100"}, {"9ck298"});
        }
    });
}

SEASTAR_TEST_CASE(size_tiered_beyond_max_threshold_test) {
  return test_env::do_with_async([] (test_env& env) {
    auto cf = env.make_table_for_tests();
    auto stop_cf = deferred_stop(cf);
    auto cs = sstables::make_compaction_strategy(sstables::compaction_strategy_type::size_tiered, cf.schema()->compaction_strategy_options());

    std::vector<sstables::shared_sstable> candidates;
    int max_threshold = cf->schema()->max_compaction_threshold();
    candidates.reserve(max_threshold+1);
    for (auto i = 0; i < (max_threshold+1); i++) { // (max_threshold+1) sstables of similar size
        auto sst = env.make_sstable(cf->schema());
        sstables::test(sst).set_data_file_size(1);
        candidates.push_back(std::move(sst));
    }
    auto desc = get_sstables_for_compaction(cs, cf.as_table_state(), std::move(candidates));
    BOOST_REQUIRE(desc.sstables.size() == size_t(max_threshold));
  });
}

SEASTAR_TEST_CASE(sstable_expired_data_ratio) {
    return test_env::do_with_async([] (test_env& env) {
        auto make_schema = [&] (std::string_view cf, sstables::compaction_strategy_type cst) {
            auto builder = schema_builder("tests", cf)
                    .with_column("p1", utf8_type, column_kind::partition_key)
                    .with_column("c1", utf8_type, column_kind::clustering_key)
                    .with_column("r1", utf8_type);
            builder.set_compaction_strategy(cst);
            return builder.build();
        };

        auto stcs_schema = make_schema("stcs", sstables::compaction_strategy_type::size_tiered);
        auto stcs_table = env.make_table_for_tests(stcs_schema);
        auto close_stcs_table = deferred_stop(stcs_table);
        auto sst_gen = env.make_sst_factory(stcs_schema);

        auto mt = make_lw_shared<replica::memtable>(stcs_schema);

        static constexpr float expired = 0.33;
        // we want number of expired keys to be ~ 1.5*sstables::TOMBSTONE_HISTOGRAM_BIN_SIZE so as to
        // test ability of histogram to return a good estimation after merging keys.
        static int total_keys = std::ceil(sstables::TOMBSTONE_HISTOGRAM_BIN_SIZE/expired)*1.5;

        auto insert_key = [&stcs_schema, &mt] (bytes k, uint32_t ttl, uint32_t expiration_time) {
            auto key = partition_key::from_exploded(*stcs_schema, {k});
            mutation m(stcs_schema, key);
            auto c_key = clustering_key::from_exploded(*stcs_schema, {to_bytes("c1")});
            m.set_clustered_cell(c_key, *stcs_schema->get_column_definition("r1"), make_atomic_cell(utf8_type, bytes("a"), ttl, expiration_time));
            mt->apply(std::move(m));
        };

        auto expired_keys = total_keys*expired;
        auto now = gc_clock::now();
        for (auto i = 0; i < expired_keys; i++) {
            // generate expiration time at different time points or only a few entries would be created in histogram
            auto expiration_time = (now - gc_clock::duration(DEFAULT_GC_GRACE_SECONDS*2+i)).time_since_epoch().count();
            insert_key(to_bytes("expired_key" + to_sstring(i)), 1, expiration_time);
        }
        auto remaining = total_keys-expired_keys;
        auto expiration_time = (now + gc_clock::duration(3600)).time_since_epoch().count();
        for (auto i = 0; i < remaining; i++) {
            insert_key(to_bytes("key" + to_sstring(i)), 3600, expiration_time);
        }
        auto sst = make_sstable_containing(sst_gen, mt);
        const auto& stats = sst->get_stats_metadata();
        BOOST_REQUIRE(stats.estimated_tombstone_drop_time.bin.size() == sstables::TOMBSTONE_HISTOGRAM_BIN_SIZE);
        auto uncompacted_size = sst->data_size();
        // Asserts that two keys are equal to within a positive delta
        tombstone_gc_state gc_state(nullptr);
        BOOST_REQUIRE(std::fabs(sst->estimate_droppable_tombstone_ratio(now, gc_state, stcs_schema) - expired) <= 0.1);
        sstable_run run;
        BOOST_REQUIRE(run.insert(sst));
        BOOST_REQUIRE(std::fabs(run.estimate_droppable_tombstone_ratio(now, gc_state, stcs_schema) - expired) <= 0.1);

        auto creator = sst_gen;
        auto info = compact_sstables(env, sstables::compaction_descriptor({ sst }), stcs_table, creator).get();
        BOOST_REQUIRE(info.new_sstables.size() == 1);
        BOOST_REQUIRE(info.new_sstables.front()->estimate_droppable_tombstone_ratio(now, gc_state, stcs_schema) == 0.0f);
        BOOST_REQUIRE_CLOSE(info.new_sstables.front()->data_size(), uncompacted_size*(1-expired), 5);

        std::map<sstring, sstring> options;
        options.emplace("tombstone_threshold", "0.3f");

        auto cs = sstables::make_compaction_strategy(sstables::compaction_strategy_type::size_tiered, options);
        // that's needed because sstable with expired data should be old enough.
        sstables::test(sst).set_data_file_write_time(db_clock::time_point::min());
        auto descriptor = get_sstables_for_compaction(cs, stcs_table.as_table_state(), { sst });
        BOOST_REQUIRE(descriptor.sstables.size() == 1);
        BOOST_REQUIRE(descriptor.sstables.front() == sst);

        // Makes sure that get_sstables_for_compaction() is called with a table_state which will provide
        // the correct LCS state.
        auto lcs_table = env.make_table_for_tests(make_schema("lcs", sstables::compaction_strategy_type::leveled));
        auto close_lcs_table = deferred_stop(lcs_table);
        cs = sstables::make_compaction_strategy(sstables::compaction_strategy_type::leveled, options);
        sst->set_sstable_level(1);
        descriptor = get_sstables_for_compaction(cs, lcs_table.as_table_state(), { sst });
        BOOST_REQUIRE(descriptor.sstables.size() == 1);
        BOOST_REQUIRE(descriptor.sstables.front() == sst);
        // make sure sstable picked for tombstone compaction removal won't be promoted or demoted.
        BOOST_REQUIRE(descriptor.sstables.front()->get_sstable_level() == 1U);

        // check tombstone compaction is disabled by default for TWCS
        auto twcs_table = env.make_table_for_tests(make_schema("twcs", sstables::compaction_strategy_type::time_window));
        auto close_twcs_table = deferred_stop(twcs_table);
        cs = sstables::make_compaction_strategy(sstables::compaction_strategy_type::time_window, {});
        descriptor = get_sstables_for_compaction(cs, twcs_table.as_table_state(), { sst });
        BOOST_REQUIRE(descriptor.sstables.size() == 0);
        cs = sstables::make_compaction_strategy(sstables::compaction_strategy_type::time_window, options);
        descriptor = get_sstables_for_compaction(cs, twcs_table.as_table_state(), { sst });
        BOOST_REQUIRE(descriptor.sstables.size() == 1);
        BOOST_REQUIRE(descriptor.sstables.front() == sst);

        // sstable with droppable ratio of 0.3 won't be included due to threshold
        {
            std::map<sstring, sstring> options;
            options.emplace("tombstone_threshold", "0.5f");
            auto cs = sstables::make_compaction_strategy(sstables::compaction_strategy_type::size_tiered, options);
            auto descriptor = get_sstables_for_compaction(cs, stcs_table.as_table_state(), { sst });
            BOOST_REQUIRE(descriptor.sstables.size() == 0);
        }
        // sstable which was recently created won't be included due to min interval
        {
            std::map<sstring, sstring> options;
            options.emplace("tombstone_compaction_interval", "3600");
            auto cs = sstables::make_compaction_strategy(sstables::compaction_strategy_type::size_tiered, options);
            sstables::test(sst).set_data_file_write_time(db_clock::now());
            auto descriptor = get_sstables_for_compaction(cs, stcs_table.as_table_state(), { sst });
            BOOST_REQUIRE(descriptor.sstables.size() == 0);
        }
        // sstable which should not be included because of droppable ratio of 0.3, will actually be included
        // because the droppable ratio check has been disabled with unchecked_tombstone_compaction set to true
        {
            std::map<sstring, sstring> options;
            options.emplace("tombstone_threshold", "0.5f");
            options.emplace("tombstone_compaction_interval", "3600");
            options.emplace("unchecked_tombstone_compaction", "true");
            auto cs = sstables::make_compaction_strategy(sstables::compaction_strategy_type::size_tiered, options);
            sstables::test(sst).set_data_file_write_time(db_clock::now() - std::chrono::seconds(7200));
            auto descriptor = get_sstables_for_compaction(cs, stcs_table.as_table_state(), { sst });
            BOOST_REQUIRE(descriptor.sstables.size() == 1);
        }
    });
}

SEASTAR_TEST_CASE(compaction_correctness_with_partitioned_sstable_set) {
    return test_env::do_with_async([] (test_env& env) {
        auto builder = schema_builder("tests", "tombstone_purge")
                .with_column("id", utf8_type, column_kind::partition_key)
                .with_column("value", int32_type);
        builder.set_gc_grace_seconds(0);
        builder.set_compaction_strategy(sstables::compaction_strategy_type::leveled);
        auto s = builder.build();

        auto sst_gen = env.make_sst_factory(s);

        auto compact = [&, s] (std::vector<shared_sstable> all) -> std::vector<shared_sstable> {
            // NEEDED for partitioned_sstable_set to actually have an effect
            std::for_each(all.begin(), all.end(), [] (auto& sst) { sst->set_sstable_level(1); });
            auto cf = env.make_table_for_tests(s);
            auto close_cf = deferred_stop(cf);
            return compact_sstables(env, sstables::compaction_descriptor(std::move(all), 0, 0 /*std::numeric_limits<uint64_t>::max()*/),
                cf, sst_gen).get().new_sstables;
        };

        auto make_insert = [&] (const dht::decorated_key& key) {
            mutation m(s, key);
            m.set_clustered_cell(clustering_key::make_empty(), bytes("value"), data_value(int32_t(1)), 1 /* ts */);
            return m;
        };

        const auto keys = tests::generate_partition_keys(4, s);
        auto mut1 = make_insert(keys[0]);
        auto mut2 = make_insert(keys[1]);
        auto mut3 = make_insert(keys[2]);
        auto mut4 = make_insert(keys[3]);

        {
            std::vector<shared_sstable> sstables = {
                    make_sstable_containing(sst_gen, {mut1, mut2}),
                    make_sstable_containing(sst_gen, {mut3, mut4})
            };

            auto result = compact(std::move(sstables));
            BOOST_REQUIRE_EQUAL(4, result.size());

            assert_that(sstable_reader(result[0], s, env.make_reader_permit()))
                    .produces(mut1)
                    .produces_end_of_stream();
            assert_that(sstable_reader(result[1], s, env.make_reader_permit()))
                    .produces(mut2)
                    .produces_end_of_stream();
            assert_that(sstable_reader(result[2], s, env.make_reader_permit()))
                    .produces(mut3)
                    .produces_end_of_stream();
            assert_that(sstable_reader(result[3], s, env.make_reader_permit()))
                    .produces(mut4)
                    .produces_end_of_stream();
        }

        {
            // with partitioned_sstable_set having an interval with exclusive lower boundary, example:
            // [mut1, mut2]
            // (mut2, mut3]
            std::vector<shared_sstable> sstables = {
                    make_sstable_containing(sst_gen, {mut1, mut2}),
                    make_sstable_containing(sst_gen, {mut2, mut3}),
                    make_sstable_containing(sst_gen, {mut3, mut4})
            };

            auto result = compact(std::move(sstables));
            BOOST_REQUIRE_EQUAL(4, result.size());

            assert_that(sstable_reader(result[0], s, env.make_reader_permit()))
                    .produces(mut1)
                    .produces_end_of_stream();
            assert_that(sstable_reader(result[1], s, env.make_reader_permit()))
                    .produces(mut2)
                    .produces_end_of_stream();
            assert_that(sstable_reader(result[2], s, env.make_reader_permit()))
                    .produces(mut3)
                    .produces_end_of_stream();
            assert_that(sstable_reader(result[3], s, env.make_reader_permit()))
                    .produces(mut4)
                    .produces_end_of_stream();
        }

        {
            // with gap between tables
            std::vector<shared_sstable> sstables = {
                    make_sstable_containing(sst_gen, {mut1, mut2}),
                    make_sstable_containing(sst_gen, {mut4, mut4})
            };

            auto result = compact(std::move(sstables));
            BOOST_REQUIRE_EQUAL(3, result.size());

            assert_that(sstable_reader(result[0], s, env.make_reader_permit()))
                    .produces(mut1)
                    .produces_end_of_stream();
            assert_that(sstable_reader(result[1], s, env.make_reader_permit()))
                    .produces(mut2)
                    .produces_end_of_stream();
            assert_that(sstable_reader(result[2], s, env.make_reader_permit()))
                    .produces(mut4)
                    .produces_end_of_stream();
        }
    });
}

SEASTAR_TEST_CASE(sstable_cleanup_correctness_test) {
    return do_with_cql_env([] (auto& e) {
        return test_env::do_with_async([&db = e.local_db()] (test_env& env) {
            auto ks_name = "ks";    // single_node_cql_env::ks_name
            auto s = schema_builder(ks_name, "correcness_test")
                    .with_column("id", utf8_type, column_kind::partition_key)
                    .with_column("value", int32_type).build();

            auto sst_gen = env.make_sst_factory(s);

            auto make_insert = [&] (dht::decorated_key key) {
                mutation m(s, std::move(key));
                m.set_clustered_cell(clustering_key::make_empty(), bytes("value"), data_value(int32_t(1)), api::timestamp_type(0));
                return m;
            };

            auto total_partitions = 10000U;
            auto local_keys = tests::generate_partition_keys(total_partitions, s);
            dht::decorated_key::less_comparator cmp(s);
            std::sort(local_keys.begin(), local_keys.end(), cmp);
            std::vector<mutation> mutations;
            for (auto i = 0U; i < total_partitions; i++) {
                mutations.push_back(make_insert(local_keys.at(i)));
            }
            auto sst = make_sstable_containing(sst_gen, mutations);
            auto run_identifier = sst->run_identifier();

            auto cf = env.make_table_for_tests(s);
            auto close_cf = deferred_stop(cf);
            cf->start();

            const auto& erm = db.find_keyspace(ks_name).get_vnode_effective_replication_map();
            auto local_ranges = compaction::make_owned_ranges_ptr(db.get_keyspace_local_ranges(erm).get());
            auto descriptor = sstables::compaction_descriptor({sst}, compaction_descriptor::default_level,
                compaction_descriptor::default_max_sstable_bytes, run_identifier, compaction_type_options::make_cleanup(), std::move(local_ranges));
            auto ret = compact_sstables(env, std::move(descriptor), cf, sst_gen).get();

            BOOST_REQUIRE(ret.new_sstables.size() == 1);
            BOOST_REQUIRE(ret.new_sstables.front()->get_estimated_key_count() >= total_partitions);
            BOOST_REQUIRE((ret.new_sstables.front()->get_estimated_key_count() - total_partitions) <= uint64_t(s->min_index_interval()));
            BOOST_REQUIRE(ret.new_sstables.front()->run_identifier() == run_identifier);

            dht::token_range_vector ranges;
            ranges.push_back(dht::token_range::make_singular(local_keys.at(0).token()));
            ranges.push_back(dht::token_range::make_singular(local_keys.at(10).token()));
            ranges.push_back(dht::token_range::make_singular(local_keys.at(100).token()));
            ranges.push_back(dht::token_range::make_singular(local_keys.at(900).token()));
            local_ranges = compaction::make_owned_ranges_ptr(std::move(ranges));
            descriptor = sstables::compaction_descriptor({sst}, compaction_descriptor::default_level,
                                            compaction_descriptor::default_max_sstable_bytes, run_identifier,
                                            compaction_type_options::make_cleanup(), std::move(local_ranges));
            ret = compact_sstables(env, std::move(descriptor), cf, sst_gen).get();
            BOOST_REQUIRE(ret.new_sstables.size() == 1);
            auto reader = ret.new_sstables[0]->as_mutation_source().make_reader_v2(s, env.make_reader_permit(), query::full_partition_range, s->full_slice());
            assert_that(std::move(reader))
                    .produces(local_keys[0])
                    .produces(local_keys[10])
                    .produces(local_keys[100])
                    .produces(local_keys[900])
                    .produces_end_of_stream();
        });
    });
}

future<> foreach_table_state_with_thread(table_for_tests& table, std::function<void(compaction::table_state&)> action) {
    return table->parallel_foreach_table_state([action] (compaction::table_state& ts) {
        return seastar::async([action, &ts] {
            action(ts);
        });
    });
}

static std::deque<mutation_fragment_v2> explode(reader_permit permit, std::vector<mutation> muts) {
    if (muts.empty()) {
        return {};
    }

    auto schema = muts.front().schema();
    std::deque<mutation_fragment_v2> frags;

    auto mr = make_mutation_reader_from_mutations_v2(schema, permit, std::move(muts));
    auto close_mr = deferred_close(mr);
    mr.consume_pausable([&frags] (mutation_fragment_v2&& mf) {
        frags.emplace_back(std::move(mf));
        return stop_iteration::no;
    }).get();

    return frags;
}

static std::deque<mutation_fragment_v2> clone(const schema& schema, reader_permit permit, const std::deque<mutation_fragment_v2>& frags) {
    std::deque<mutation_fragment_v2> cloned_frags;
    for (const auto& frag : frags) {
        cloned_frags.emplace_back(schema, permit, frag);
    }
    return cloned_frags;
}


static void verify_fragments(std::vector<sstables::shared_sstable> ssts, reader_permit permit, const std::deque<mutation_fragment_v2>& mfs) {
    auto schema = ssts.front()->get_schema();

    std::vector<mutation_reader> readers;
    readers.reserve(ssts.size());
    for (auto& sst : ssts) {
        readers.push_back(sst->as_mutation_source().make_reader_v2(schema, permit));
    }

    auto r = assert_that(make_combined_reader(schema, permit, std::move(readers)));
    for (const auto& mf : mfs) {
        testlog.trace("Expecting {}", mutation_fragment_v2::printer(*schema, mf));
        r.produces(*schema, mf);
    }
    r.produces_end_of_stream();
};

// A framework for scrub-related tests.
// Lives in a seastar thread
enum class random_schema { no, yes };
template <random_schema create_random_schema>
class scrub_test_framework {
public:
    using test_func = std::function<void(table_for_tests&, compaction::table_state&, std::vector<sstables::shared_sstable>)>;

private:
    sharded<test_env> _env;
    uint32_t _seed;
    std::unique_ptr<tests::random_schema_specification> _random_schema_spec;
    tests::random_schema _random_schema;

public:
    scrub_test_framework(compress_sstable compress)
        : _seed(tests::random::get_int<uint32_t>())
        , _random_schema_spec(tests::make_random_schema_specification(
                "scrub_test_framework",
                std::uniform_int_distribution<size_t>(2, 4),
                std::uniform_int_distribution<size_t>(2, 4),
                std::uniform_int_distribution<size_t>(2, 8),
                std::uniform_int_distribution<size_t>(2, 8),
                compress))
        , _random_schema(_seed, *_random_schema_spec)
    {
        _env.start().get();
        testlog.info("random_schema: {}", _random_schema.cql());
    }

    ~scrub_test_framework() {
        _env.stop().get();
    }

    test_env& env() { return _env.local(); }
    uint32_t seed() const { return _seed; }
    tests::random_schema& random_schema() { return _random_schema; }
    schema_ptr schema() const { return _random_schema.schema(); }

    void run(schema_ptr schema, std::deque<mutation_fragment_v2> frags, test_func func) {
        auto& env = this->env();

        const auto partition_count = std::count_if(frags.begin(), frags.end(), std::mem_fn(&mutation_fragment_v2::is_partition_start));

        auto permit = env.make_reader_permit();
        auto mr = make_mutation_reader_from_fragments(schema, permit, clone(*schema, permit, frags));

        auto close_mr = deferred_close(mr);

        auto sst = env.make_sstable(schema);
        sstable_writer_config cfg = env.manager().configure_writer();
        cfg.validation_level = mutation_fragment_stream_validation_level::partition_region; // this test violates key order on purpose

        auto wr = sst->get_writer(*schema, partition_count, cfg, encoding_stats{});
        mr.consume_in_thread(std::move(wr));

        sst->load(schema->get_sharder()).get();

        auto table = env.make_table_for_tests(schema);
        auto close_cf = deferred_stop(table);
        table->start();

        table->add_sstable_and_update_cache(sst).get();

        verify_fragments({sst}, env.make_reader_permit(), frags);

        bool found_sstable = false;
        foreach_table_state_with_thread(table, [&] (compaction::table_state& ts) {
            auto sstables = in_strategy_sstables(ts);
            if (sstables.empty()) {
                return;
            }
            BOOST_REQUIRE(sstables.size() == 1);
            BOOST_REQUIRE(sstables.front() == sst);
            found_sstable = true;

            func(table, ts, sstables);
        }).get();
        BOOST_REQUIRE(found_sstable);
    }

    void run(schema_ptr schema, std::vector<mutation> muts, test_func func) {
        run(std::move(schema), explode(env().make_reader_permit(), std::move(muts)), std::move(func));
    }
};

template <>
class scrub_test_framework<random_schema::no> {
public:
    using test_func = std::function<void(table_for_tests&, compaction::table_state&, std::vector<sstables::shared_sstable>)>;

private:
    sharded<test_env> _env;

public:
    scrub_test_framework()
    {
        _env.start().get();
    }

    ~scrub_test_framework() {
        _env.stop().get();
    }

    test_env& env() { return _env.local(); }

    void run(schema_ptr schema, shared_sstable sst, test_func func) {
        auto& env = this->env();

        auto table = env.make_table_for_tests(schema);
        auto close_cf = deferred_stop(table);
        table->start();

        table->add_sstable_and_update_cache(sst).get();

        bool found_sstable = false;
        foreach_table_state_with_thread(table, [&] (compaction::table_state& ts) {
            auto sstables = in_strategy_sstables(ts);
            if (sstables.empty()) {
                return;
            }
            BOOST_REQUIRE(sstables.size() == 1);
            BOOST_REQUIRE(sstables.front() == sst);
            found_sstable = true;

            func(table, ts, sstables);
        }).get();
        BOOST_REQUIRE(found_sstable);
    }
};

void scrub_validate_corrupted_content(compress_sstable compress) {
    scrub_test_framework<random_schema::yes> test(compress);

    auto schema = test.schema();

    auto muts = tests::generate_random_mutations(
            test.random_schema(),
            tests::uncompactible_timestamp_generator(test.seed()),
            tests::no_expiry_expiry_generator(),
            std::uniform_int_distribution<size_t>(10, 10)).get();
    std::swap(*muts.begin(), *(muts.begin() + 1));

    test.run(schema, muts, [] (table_for_tests& table, compaction::table_state& ts, std::vector<sstables::shared_sstable> sstables) {
        BOOST_REQUIRE(sstables.size() == 1);
        auto sst = sstables.front();

        sstables::compaction_type_options::scrub opts = {
            .operation_mode = sstables::compaction_type_options::scrub::mode::validate,
        };
        auto stats = table->get_compaction_manager().perform_sstable_scrub(ts, opts, tasks::task_info{}).get();

        BOOST_REQUIRE(stats.has_value());
        BOOST_REQUIRE_GT(stats->validation_errors, 0);
        BOOST_REQUIRE(sst->is_quarantined());
        BOOST_REQUIRE(in_strategy_sstables(ts).empty());
    });
}

void scrub_validate_corrupted_file(compress_sstable compress) {
    scrub_test_framework<random_schema::yes> test(compress);

    auto schema = test.schema();

    auto muts = tests::generate_random_mutations(
            test.random_schema(),
            tests::uncompactible_timestamp_generator(test.seed()),
            tests::no_expiry_expiry_generator(),
            std::uniform_int_distribution<size_t>(10, 10)).get();

    test.run(schema, muts, [] (table_for_tests& table, compaction::table_state& ts, std::vector<sstables::shared_sstable> sstables) {
        BOOST_REQUIRE(sstables.size() == 1);
        auto sst = sstables.front();

        // Corrupt the data to cause an invalid checksum.
        auto f = open_file_dma(sstables::test(sst).filename(component_type::Data).native(), open_flags::wo).get();
        const auto wbuf_align = f.memory_dma_alignment();
        const auto wbuf_len = f.disk_write_dma_alignment();
        auto wbuf = seastar::temporary_buffer<char>::aligned(wbuf_align, wbuf_len);
        std::fill(wbuf.get_write(), wbuf.get_write() + wbuf_len, 0xba);
        f.dma_write(0, wbuf.get(), wbuf_len).get();
        f.close().get();

        sstables::compaction_type_options::scrub opts = {
            .operation_mode = sstables::compaction_type_options::scrub::mode::validate,
        };
        auto stats = table->get_compaction_manager().perform_sstable_scrub(ts, opts, tasks::task_info{}).get();

        BOOST_REQUIRE(stats.has_value());
        BOOST_REQUIRE_GT(stats->validation_errors, 0);
        BOOST_REQUIRE(sst->is_quarantined());
        BOOST_REQUIRE(in_strategy_sstables(ts).empty());
    });
}

void scrub_validate_corrupted_digest(compress_sstable compress) {
    scrub_test_framework<random_schema::yes> test(compress);

    auto schema = test.schema();

    auto muts = tests::generate_random_mutations(
            test.random_schema(),
            tests::uncompactible_timestamp_generator(test.seed()),
            tests::no_expiry_expiry_generator(),
            std::uniform_int_distribution<size_t>(10, 10)).get();

    test.run(schema, muts, [] (table_for_tests& table, compaction::table_state& ts, std::vector<sstables::shared_sstable> sstables) {
        BOOST_REQUIRE(sstables.size() == 1);
        auto sst = sstables.front();

        // This test is about corrupted data with valid per-chunk checksums.
        // This kind of corruption should be detected by the digest check.
        // Triggering this is not trivial, so we corrupt the Digest file instead.
        auto f = open_file_dma(sstables::test(sst).filename(component_type::Digest).native(), open_flags::rw).get();
        auto stream = make_file_input_stream(f);
        auto close_stream = deferred_close(stream);
        auto digest_str = util::read_entire_stream_contiguous(stream).get();
        auto digest = boost::lexical_cast<uint32_t>(digest_str);
        auto new_digest = to_sstring<bytes>(digest + 1); // a random invalid digest
        f.dma_write(0, new_digest.c_str(), new_digest.size()).get();

        sstables::compaction_type_options::scrub opts = {
            .operation_mode = sstables::compaction_type_options::scrub::mode::validate,
        };
        auto stats = table->get_compaction_manager().perform_sstable_scrub(ts, opts, tasks::task_info{}).get();

        BOOST_REQUIRE(stats.has_value());
        BOOST_REQUIRE_GT(stats->validation_errors, 0);
        BOOST_REQUIRE(sst->is_quarantined());
        BOOST_REQUIRE(in_strategy_sstables(ts).empty());
    });
}

void scrub_validate_no_digest(compress_sstable compress) {
    scrub_test_framework<random_schema::yes> test(compress);

    auto schema = test.schema();

    auto muts = tests::generate_random_mutations(test.random_schema()).get();

    test.run(schema, muts, [] (table_for_tests& table, compaction::table_state& ts, std::vector<sstables::shared_sstable> sstables) {
        BOOST_REQUIRE(sstables.size() == 1);
        auto sst = sstables.front();

        // Checksum and digest checking should be orthogonal.
        // Ensure that per-chunk checksums are properly checked when digest is missing.
        sstables::test(sst).rewrite_toc_without_component(component_type::Digest);

        sstables::compaction_type_options::scrub opts = {
            .operation_mode = sstables::compaction_type_options::scrub::mode::validate,
        };
        auto stats = table->get_compaction_manager().perform_sstable_scrub(ts, opts, tasks::task_info{}).get();

        BOOST_REQUIRE(stats.has_value());
        BOOST_REQUIRE_EQUAL(stats->validation_errors, 0);
        BOOST_REQUIRE(!sst->is_quarantined());
        BOOST_REQUIRE_EQUAL(in_strategy_sstables(ts).size(), 1);
        BOOST_REQUIRE_EQUAL(in_strategy_sstables(ts).front(), sst);
        BOOST_REQUIRE(!sst->get_checksum());

        // Corrupt the data to cause an invalid checksum.
        auto f = open_file_dma(sstables::test(sst).filename(component_type::Data).native(), open_flags::wo).get();
        auto close_f = deferred_close(f);
        const auto wbuf_align = f.memory_dma_alignment();
        const auto wbuf_len = f.disk_write_dma_alignment();
        auto wbuf = seastar::temporary_buffer<char>::aligned(wbuf_align, wbuf_len);
        std::fill(wbuf.get_write(), wbuf.get_write() + wbuf_len, 0xba);
        f.dma_write(0, wbuf.get(), wbuf_len).get();

        stats = table->get_compaction_manager().perform_sstable_scrub(ts, opts, tasks::task_info{}).get();

        BOOST_REQUIRE(stats.has_value());
        BOOST_REQUIRE_GT(stats->validation_errors, 0);
        BOOST_REQUIRE(sst->is_quarantined());
        BOOST_REQUIRE(in_strategy_sstables(ts).empty());
    });
}

void scrub_validate_valid(compress_sstable compress) {
    scrub_test_framework<random_schema::yes> test(compress);

    auto schema = test.schema();

    auto muts = tests::generate_random_mutations(test.random_schema()).get();

    test.run(schema, muts, [] (table_for_tests& table, compaction::table_state& ts, std::vector<sstables::shared_sstable> sstables) {
        BOOST_REQUIRE(sstables.size() == 1);
        auto sst = sstables.front();

        sstables::compaction_type_options::scrub opts = {
            .operation_mode = sstables::compaction_type_options::scrub::mode::validate,
        };
        auto stats = table->get_compaction_manager().perform_sstable_scrub(ts, opts, tasks::task_info{}).get();

        BOOST_REQUIRE(stats.has_value());
        BOOST_REQUIRE_EQUAL(stats->validation_errors, 0);
        BOOST_REQUIRE(!sst->is_quarantined());
        BOOST_REQUIRE_EQUAL(in_strategy_sstables(ts).size(), 1);
        BOOST_REQUIRE_EQUAL(in_strategy_sstables(ts).front(), sst);
    });
}

SEASTAR_THREAD_TEST_CASE(sstable_scrub_validate_mode_test_corrupted_content) {
    for (const auto& compress : {compress_sstable::no, compress_sstable::yes}) {
        testlog.info("Validating {}compressed SSTable with content-level corruption...", compress == compress_sstable::no ? "un" : "");
        scrub_validate_corrupted_content(compress);
    }
}

SEASTAR_THREAD_TEST_CASE(sstable_scrub_validate_mode_test_corrupted_file) {
    for (const auto& compress : {compress_sstable::no, compress_sstable::yes}) {
        testlog.info("Validating {}compressed SSTable with invalid checksums...", compress == compress_sstable::no ? "un" : "");
        scrub_validate_corrupted_file(compress);
    }
}

SEASTAR_THREAD_TEST_CASE(sstable_scrub_validate_mode_test_corrupted_file_digest) {
    for (const auto& compress : {compress_sstable::no, compress_sstable::yes}) {
        testlog.info("Validating {}compressed SSTable with invalid digest...", compress == compress_sstable::no ? "un" : "");
        scrub_validate_corrupted_digest(compress);
    }
}

SEASTAR_THREAD_TEST_CASE(sstable_scrub_validate_mode_test_no_digest) {
    for (const auto& compress : {compress_sstable::no, compress_sstable::yes}) {
        testlog.info("Validating {}compressed SSTable with no digest...", compress == compress_sstable::no ? "un" : "");
        scrub_validate_no_digest(compress);
    }
}

SEASTAR_THREAD_TEST_CASE(sstable_scrub_validate_mode_test_valid_sstable) {
    for (const auto& compress : {compress_sstable::no, compress_sstable::yes}) {
        testlog.info("Validating {}compressed SSTable...", compress == compress_sstable::no ? "un" : "");
        scrub_validate_valid(compress);
    }
}

SEASTAR_THREAD_TEST_CASE(sstable_scrub_validate_mode_test_multiple_instances_uncompressed) {
#ifndef SCYLLA_ENABLE_ERROR_INJECTION
    fmt::print("Skipping test as it depends on error injection. Please run in mode where it's enabled (debug,dev).\n");
    return;
#endif
    scrub_test_framework<random_schema::yes> test(compress_sstable::no);

    auto schema = test.schema();

    auto muts = tests::generate_random_mutations(test.random_schema()).get();

    test.run(schema, muts, [] (table_for_tests& table, compaction::table_state& ts, std::vector<sstables::shared_sstable> sstables) {
        BOOST_REQUIRE(sstables.size() == 1);
        auto sst = sstables.front();

        sstables::compaction_type_options::scrub opts = {
            .operation_mode = sstables::compaction_type_options::scrub::mode::validate,
        };

        utils::get_local_injector().enable("sstable_validate/pause");

        auto scrub1 = table->get_compaction_manager().perform_sstable_scrub(ts, opts, tasks::task_info{});
        BOOST_REQUIRE(eventually_true([sst] {
            auto checksum = sst->get_checksum();
            return checksum != nullptr;
        }));
        auto checksum1 = sst->get_checksum();

        auto scrub2 = table->get_compaction_manager().perform_sstable_scrub(ts, opts, tasks::task_info{});
        BOOST_REQUIRE(eventually_true([sst] {
            auto checksum = sst->get_checksum();
            return checksum != nullptr;
        }));
        auto checksum2 = sst->get_checksum();

        // Scrub instances use the same checksum component.
        BOOST_REQUIRE(checksum1);
        BOOST_REQUIRE(checksum2);
        BOOST_REQUIRE(checksum1 == checksum2);
        checksum1.release();
        checksum2.release();

        utils::get_local_injector().receive_message("sstable_validate/pause");
        when_all_succeed(std::move(scrub1), std::move(scrub2)).get();

        BOOST_REQUIRE(!sst->is_quarantined());
        BOOST_REQUIRE_EQUAL(in_strategy_sstables(ts).size(), 1);
        BOOST_REQUIRE_EQUAL(in_strategy_sstables(ts).front(), sst);
        // Checksum component released after scrub instances terminate.
        BOOST_REQUIRE(sst->get_checksum() == nullptr);

        utils::get_local_injector().disable("sstable_validate/pause");
    });
}

// Following tests run scrub in validate mode with SSTables produced by Cassandra.
// The purpose is to verify compatibility.
//
// The SSTables live in the source tree under:
// test/resource/sstables/3.x/{uncompressed,lz4}/partition_key_with_values_of_different_types and
// test/resource/sstables/3.x/{uncompressed,lz4}/integrity_check
//
// The former are pre-existing SSTables that we use to test the valid case.
//
// The latter were tailor-made to cover the invalid case by triggering the checksum and digest checks.
// The SSTables were produced with the following schema:
//
// CREATE KEYSPACE test_ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
//
// CREATE TABLE test_ks.test_table ( pk INT,
//                                   bool_val BOOLEAN,
//                                   double_val DOUBLE,
//                                   float_val FLOAT,
//                                   int_val INT,
//                                   long_val BIGINT,
//                                   timestamp_val TIMESTAMP,
//                                   timeuuid_val TIMEUUID,
//                                   uuid_val UUID,
//                                   text_val TEXT,
//                                   PRIMARY KEY(pk))
//      WITH compression = {<compression_params>};
//
//  where <compression_params> is one of the following:
//  {'enabled': false} for the uncompressed case,
//  {'chunk_length_in_kb': '4', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'} for the compressed case.

static schema_builder make_cassandra_schema_builder() {
    return schema_builder("test_ks", "test_table")
            .with_column("pk", int32_type, column_kind::partition_key)
            .with_column("bool_val", boolean_type)
            .with_column("double_val", double_type)
            .with_column("float_val", float_type)
            .with_column("int_val", int32_type)
            .with_column("long_val", long_type)
            .with_column("timestamp_val", timestamp_type)
            .with_column("timeuuid_val", timeuuid_type)
            .with_column("uuid_val", uuid_type)
            .with_column("text_val", utf8_type);
}

void scrub_validate_cassandra_compat(const compression_parameters& cp, sstring sstable_dir,
        generation_type::int_t gen, sstable::version_types version, bool valid) {
    scrub_test_framework<random_schema::no> test;

    auto schema = make_cassandra_schema_builder()
            .set_compressor_params(cp)
            .build();
    auto sst = test.env().reusable_sst(schema, sstable_dir, gen, version).get();

    test.run(schema, sst, [valid] (table_for_tests& table, compaction::table_state& ts, std::vector<sstables::shared_sstable> sstables) {
        BOOST_REQUIRE(sstables.size() == 1);
        auto sst = sstables.front();

        using scrub = sstables::compaction_type_options::scrub;
        sstables::compaction_type_options::scrub opts = {
            .operation_mode = scrub::mode::validate,
            .quarantine_sstables = scrub::quarantine_invalid_sstables::no,
        };
        auto stats = table->get_compaction_manager().perform_sstable_scrub(ts, opts, tasks::task_info{}).get();

        BOOST_REQUIRE(stats.has_value());
        if (valid) {
            BOOST_REQUIRE_EQUAL(stats->validation_errors, 0);
        } else {
            BOOST_REQUIRE_GT(stats->validation_errors, 0);
        }
        BOOST_REQUIRE(!sst->is_quarantined());
        BOOST_REQUIRE_EQUAL(in_strategy_sstables(ts).size(), 1);
        BOOST_REQUIRE_EQUAL(in_strategy_sstables(ts).front(), sst);
        BOOST_REQUIRE(!sst->get_checksum());
    });
}

SEASTAR_THREAD_TEST_CASE(sstable_scrub_validate_mode_test_valid_sstable_cassandra_compat) {
    for (const auto& [cp, subdir] : {
            std::pair{compression_parameters::no_compression(), "uncompressed"},
            {compression_parameters(compression_parameters::algorithm::lz4), "lz4"}
        }) {
        testlog.info("Validating {}compressed SSTable from Cassandra...", cp.compression_enabled() ? "" : "un");
        scrub_validate_cassandra_compat(
                cp,
                seastar::format("test/resource/sstables/3.x/{}/partition_key_with_values_of_different_types", subdir),
                1,
                sstable::version_types::mc,
                true
        );
    }
}

SEASTAR_THREAD_TEST_CASE(sstable_scrub_validate_mode_test_corrupted_file_cassandra_compat) {
    for (const auto& [cp, subdir] : {
            std::pair{compression_parameters::no_compression(), "uncompressed"},
            {compression_parameters(compression_parameters::algorithm::lz4), "lz4"}
        }) {
        testlog.info("Validating {}compressed SSTable from Cassandra with invalid checksums...", cp.compression_enabled() ? "" : "un");
        scrub_validate_cassandra_compat(
                cp,
                seastar::format("test/resource/sstables/3.x/{}/integrity_check/invalid_checksums", subdir),
                1,
                sstable::version_types::me,
                false
        );
    }
}

SEASTAR_THREAD_TEST_CASE(sstable_scrub_validate_mode_test_corrupted_file_digest_cassandra_compat) {
    for (const auto& [cp, subdir] : {
            std::pair{compression_parameters::no_compression(), "uncompressed"},
            {compression_parameters(compression_parameters::algorithm::lz4), "lz4"}
        }) {
        testlog.info("Validating {}compressed SSTable from Cassandra with invalid digest...", cp.compression_enabled() ? "" : "un");
        scrub_validate_cassandra_compat(
                cp,
                seastar::format("test/resource/sstables/3.x/{}/integrity_check/invalid_digest", subdir),
                1,
                sstable::version_types::me,
                false
        );
    }
}

SEASTAR_TEST_CASE(sstable_validate_test) {
  return test_env::do_with_async([] (test_env& env) {
    auto schema = schema_builder("ks", get_name())
            .with_column("pk", utf8_type, column_kind::partition_key)
            .with_column("ck", int32_type, column_kind::clustering_key)
            .with_column("s", int32_type, column_kind::static_column)
            .with_column("v", int32_type).build();
    tests::reader_concurrency_semaphore_wrapper semaphore;
    auto permit = semaphore.make_permit();

    std::deque<mutation_fragment_v2> frags;

    abort_source abort;

    const auto ts = api::timestamp_type{1};
    auto local_keys = tests::generate_partition_keys(5, schema);

    auto make_partition_start = [&, schema] (unsigned pk) {
        auto dkey = local_keys.at(pk);
        return mutation_fragment_v2(*schema, permit, partition_start(std::move(dkey), {}));
    };

    auto make_partition_end = [&, schema] {
        return mutation_fragment_v2(*schema, permit, partition_end());
    };

    auto make_static_row = [&, schema] {
        auto r = row{};
        auto cdef = schema->static_column_at(0);
        auto ac = atomic_cell::make_live(*cdef.type, ts, cdef.type->decompose(data_value(1)));
        r.apply(cdef, atomic_cell_or_collection{std::move(ac)});
        return mutation_fragment_v2(*schema, permit, static_row(*schema, std::move(r)));
    };

    auto make_clustering_row = [&, schema] (unsigned i) {
        auto r = row{};
        auto cdef = schema->regular_column_at(0);
        auto ac = atomic_cell::make_live(*cdef.type, ts, cdef.type->decompose(data_value(1)));
        r.apply(cdef, atomic_cell_or_collection{std::move(ac)});
        return mutation_fragment_v2(*schema, permit,
                clustering_row(clustering_key::from_single_value(*schema, int32_type->decompose(data_value(int(i)))), {}, {}, std::move(r)));
    };

    auto make_sst = [&] (std::deque<mutation_fragment_v2> frags) {
        auto rd = make_mutation_reader_from_fragments(schema, permit, std::move(frags));
        auto config = env.manager().configure_writer();
        config.validation_level = mutation_fragment_stream_validation_level::partition_region; // this test violates key order on purpose
        return make_sstable_easy(env, std::move(rd), std::move(config), sstables::get_highest_sstable_version(), local_keys.size());
    };

    auto info = make_lw_shared<compaction_data>();

    struct error_handler {
        uint64_t& count;
        void operator()(sstring what) {
            ++count;
            testlog.trace("validation error: ", what);
        }
    };

    BOOST_TEST_MESSAGE("valid");
    {
        frags.emplace_back(make_partition_start(0));
        frags.emplace_back(make_static_row());
        frags.emplace_back(make_clustering_row(0));
        frags.emplace_back(make_clustering_row(1));
        frags.emplace_back(make_partition_end());
        frags.emplace_back(make_partition_start(2));
        frags.emplace_back(make_partition_end());

        uint64_t count = 0;
        auto sst = make_sst(std::move(frags));
        const auto errors = sst->validate(permit, abort, error_handler{count}).get();
        BOOST_REQUIRE_EQUAL(errors, 0);
        BOOST_REQUIRE_EQUAL(errors, count);
    }

    BOOST_TEST_MESSAGE("out-of-order clustering row");
    {
        frags.emplace_back(make_partition_start(0));
        frags.emplace_back(make_clustering_row(1));
        frags.emplace_back(make_clustering_row(0));
        frags.emplace_back(make_partition_end());

        uint64_t count = 0;
        auto sst = make_sst(std::move(frags));
        const auto errors = sst->validate(permit, abort, error_handler{count}).get();
        BOOST_REQUIRE_NE(errors, 0);
        BOOST_REQUIRE_EQUAL(errors, count);
    }

    BOOST_TEST_MESSAGE("out-of-order partition");
    {
        frags.emplace_back(make_partition_start(0));
        frags.emplace_back(make_clustering_row(0));
        frags.emplace_back(make_partition_end());
        frags.emplace_back(make_partition_start(2));
        frags.emplace_back(make_clustering_row(0));
        frags.emplace_back(make_partition_end());
        frags.emplace_back(make_partition_start(1));
        frags.emplace_back(make_partition_end());

        uint64_t count = 0;
        auto sst = make_sst(std::move(frags));
        const auto errors = sst->validate(permit, abort, error_handler{count}).get();
        BOOST_REQUIRE_NE(errors, 0);
        BOOST_REQUIRE_EQUAL(errors, count);
    }

    BOOST_TEST_MESSAGE("malformed_sstable_exception");
    {
        frags.emplace_back(make_partition_start(0));
        frags.emplace_back(make_clustering_row(0));
        frags.emplace_back(make_partition_end());

        uint64_t count = 0;
        auto sst = make_sst(std::move(frags));

        // Corrupt the data to cause an invalid checksum.
        auto f = open_file_dma(sstables::test(sst).filename(component_type::Data).native(), open_flags::wo).get();
        const auto wbuf_align = f.memory_dma_alignment();
        const auto wbuf_len = f.disk_write_dma_alignment();
        auto wbuf = seastar::temporary_buffer<char>::aligned(wbuf_align, wbuf_len);
        std::fill(wbuf.get_write(), wbuf.get_write() + wbuf_len, 0xba);
        f.dma_write(0, wbuf.get(), wbuf_len).get();
        f.close().get();

        auto res = sstables::validate_checksums(sst, permit).get();
        BOOST_REQUIRE(res.status == validate_checksums_status::invalid);
        BOOST_REQUIRE(res.has_digest);


        const auto errors = sst->validate(permit, abort, error_handler{count}).get();
        BOOST_REQUIRE_NE(errors, 0);
        BOOST_REQUIRE_EQUAL(errors, count);
    }
  });
}

SEASTAR_THREAD_TEST_CASE(sstable_scrub_abort_mode_test) {
    scrub_test_framework<random_schema::yes> test(compress_sstable::yes);

    auto schema = test.schema();

    auto muts = tests::generate_random_mutations(test.random_schema(), 3).get();
    std::swap(*muts.begin(), *(muts.begin() + 1));

    test.run(schema, muts, [] (table_for_tests& table, compaction::table_state& ts, std::vector<sstables::shared_sstable> sstables) {
        BOOST_REQUIRE(sstables.size() == 1);
        auto sst = sstables.front();

        testlog.info("Scrub in abort mode");

        // We expect the scrub with mode=srub::mode::abort to stop on the first invalid fragment.
        sstables::compaction_type_options::scrub opts = {};
        opts.operation_mode = sstables::compaction_type_options::scrub::mode::abort;
        BOOST_REQUIRE_THROW(table->get_compaction_manager().perform_sstable_scrub(ts, opts, tasks::task_info{}).get(), sstables::compaction_aborted_exception);

        BOOST_REQUIRE(in_strategy_sstables(ts).size() == 1);
        BOOST_REQUIRE(in_strategy_sstables(ts).front() == sst);
    });
}

SEASTAR_THREAD_TEST_CASE(sstable_scrub_skip_mode_test) {
    scrub_test_framework<random_schema::yes> test(compress_sstable::yes);

    auto schema = test.schema();

    auto corrupt_muts = tests::generate_random_mutations(
            test.random_schema(),
            tests::uncompactible_timestamp_generator(test.seed()),
            tests::no_expiry_expiry_generator(),
            std::uniform_int_distribution<size_t>(10, 10),
            std::uniform_int_distribution<size_t>(10, 20),
            std::uniform_int_distribution<size_t>(0, 0)).get();

    // prepare a corrupt fragment list, with both an ooo partition and an ooo row
    std::swap(corrupt_muts.at(0), corrupt_muts.at(1));
    auto corrupt_fragments = explode(test.env().make_reader_permit(), corrupt_muts);
    auto first_cr_index = corrupt_fragments.at(1).is_static_row() ? 2 : 1;
    auto& cr1 = corrupt_fragments.at(first_cr_index);
    auto& cr2 = corrupt_fragments.at(first_cr_index + 1);
    BOOST_REQUIRE_EQUAL(cr1.mutation_fragment_kind(), mutation_fragment_v2::kind::clustering_row);
    BOOST_REQUIRE_EQUAL(cr2.mutation_fragment_kind(), mutation_fragment_v2::kind::clustering_row);
    std::swap(cr1, cr2);

    // prepare the expected post-scrub version of "corrupt_fragments"
    std::vector<mutation> scrubbed_muts;
    scrubbed_muts.push_back(corrupt_muts.front());
    std::copy(corrupt_muts.begin() + 2, corrupt_muts.end(), std::back_inserter(scrubbed_muts));
    auto scrubbed_fragments = explode(test.env().make_reader_permit(), std::move(scrubbed_muts));
    scrubbed_fragments.erase(scrubbed_fragments.begin() + first_cr_index);

    test.run(schema, std::move(corrupt_fragments), [&] (table_for_tests& table, compaction::table_state& ts, std::vector<sstables::shared_sstable> sstables) {
        BOOST_REQUIRE(sstables.size() == 1);
        auto sst = sstables.front();

        testlog.info("Scrub in skip mode");

        // We expect the scrub with mode=srub::mode::skip to get rid of all invalid data.
        sstables::compaction_type_options::scrub opts = {};
        opts.operation_mode = sstables::compaction_type_options::scrub::mode::skip;
        table->get_compaction_manager().perform_sstable_scrub(ts, opts, tasks::task_info{}).get();

        BOOST_REQUIRE(in_strategy_sstables(ts).size() == 1);
        BOOST_REQUIRE(in_strategy_sstables(ts).front() != sst);

        verify_fragments(in_strategy_sstables(ts), test.env().make_reader_permit(), scrubbed_fragments);
    });
}

SEASTAR_THREAD_TEST_CASE(sstable_scrub_segregate_mode_test) {
    scrub_test_framework<random_schema::yes> test(compress_sstable::yes);

    auto schema = test.schema();

    auto muts = tests::generate_random_mutations(
            test.random_schema(),
            tests::uncompactible_timestamp_generator(test.seed()),
            tests::no_expiry_expiry_generator(),
            std::uniform_int_distribution<size_t>(10, 10),
            std::uniform_int_distribution<size_t>(10, 20),
            std::uniform_int_distribution<size_t>(0, 0)).get();

    // prepare a corrupt fragment list, with both an ooo partition and an ooo row
    auto corrupt_muts = muts;
    std::swap(corrupt_muts.at(0), corrupt_muts.at(1));
    auto corrupt_fragments = explode(test.env().make_reader_permit(), corrupt_muts);
    auto first_cr_index = corrupt_fragments.at(1).is_static_row() ? 2 : 1;
    auto& cr1 = corrupt_fragments.at(first_cr_index);
    auto& cr2 = corrupt_fragments.at(first_cr_index + 1);
    BOOST_REQUIRE_EQUAL(cr1.mutation_fragment_kind(), mutation_fragment_v2::kind::clustering_row);
    BOOST_REQUIRE_EQUAL(cr2.mutation_fragment_kind(), mutation_fragment_v2::kind::clustering_row);
    std::swap(cr1, cr2);

    test.run(schema, std::move(corrupt_fragments), [&] (table_for_tests& table, compaction::table_state& ts, std::vector<sstables::shared_sstable> sstables) {
        BOOST_REQUIRE(sstables.size() == 1);

        testlog.info("Scrub in segregate mode");

        // We expect the scrub with mode=srub::mode::segregate to fix all out-of-order data.
        sstables::compaction_type_options::scrub opts = {};
        opts.operation_mode = sstables::compaction_type_options::scrub::mode::segregate;
        table->get_compaction_manager().perform_sstable_scrub(ts, opts, tasks::task_info{}).get();

        testlog.info("Scrub resulted in {} sstables", in_strategy_sstables(ts).size());
        BOOST_REQUIRE(in_strategy_sstables(ts).size() > 1);
        verify_fragments(in_strategy_sstables(ts), test.env().make_reader_permit(), explode(test.env().make_reader_permit(), muts));
    });
}

SEASTAR_THREAD_TEST_CASE(sstable_scrub_quarantine_mode_test) {
    scrub_test_framework<random_schema::yes> test(compress_sstable::yes);

    auto schema = test.schema();

    auto muts = tests::generate_random_mutations(
            test.random_schema(),
            tests::uncompactible_timestamp_generator(test.seed()),
            tests::no_expiry_expiry_generator(),
            std::uniform_int_distribution<size_t>(10, 10)).get();

    auto corrupt_muts = muts;
    std::swap(corrupt_muts.at(0), corrupt_muts.at(1));
    const auto corrupt_fragments = explode(test.env().make_reader_permit(), corrupt_muts);
    const auto scrubbed_fragments = explode(test.env().make_reader_permit(), muts);

    constexpr std::array<sstables::compaction_type_options::scrub::quarantine_mode, 3> quarantine_modes = {
        sstables::compaction_type_options::scrub::quarantine_mode::include,
        sstables::compaction_type_options::scrub::quarantine_mode::exclude,
        sstables::compaction_type_options::scrub::quarantine_mode::only,
    };
    for (auto qmode : quarantine_modes) {
        testlog.info("Checking qurantine mode {}", qmode);
        test.run(schema, corrupt_muts, [&] (table_for_tests& table, compaction::table_state& ts, std::vector<sstables::shared_sstable> sstables) {
            BOOST_REQUIRE(sstables.size() == 1);
            auto sst = sstables.front();

            auto permit = test.env().make_reader_permit();

            testlog.info("Scrub in validate mode");

            // We expect the scrub with mode=scrub::mode::validate to quarantine the sstable.
            sstables::compaction_type_options::scrub opts = {};
            opts.operation_mode = sstables::compaction_type_options::scrub::mode::validate;
            table->get_compaction_manager().perform_sstable_scrub(ts, opts, tasks::task_info{}).get();

            BOOST_REQUIRE(in_strategy_sstables(ts).empty());
            BOOST_REQUIRE(sst->is_quarantined());
            verify_fragments({sst}, permit, corrupt_fragments);

            testlog.info("Scrub in segregate mode with quarantine_mode {}", qmode);

            // We expect the scrub with mode=scrub::mode::segregate to fix all out-of-order data.
            opts.operation_mode = sstables::compaction_type_options::scrub::mode::segregate;
            opts.quarantine_operation_mode = qmode;
            table->get_compaction_manager().perform_sstable_scrub(ts, opts, tasks::task_info{}).get();

            switch (qmode) {
            case sstables::compaction_type_options::scrub::quarantine_mode::include:
            case sstables::compaction_type_options::scrub::quarantine_mode::only:
                // The sstable should be found and scrubbed when scrub::quarantine_mode is scrub::quarantine_mode::{include,only}
                testlog.info("Scrub resulted in {} sstables", in_strategy_sstables(ts).size());
                BOOST_REQUIRE(in_strategy_sstables(ts).size() > 1);
                verify_fragments(in_strategy_sstables(ts), permit, scrubbed_fragments);
                break;
            case sstables::compaction_type_options::scrub::quarantine_mode::exclude:
                // The sstable should not be found when scrub::quarantine_mode is scrub::quarantine_mode::exclude
                BOOST_REQUIRE(in_strategy_sstables(ts).empty());
                BOOST_REQUIRE(sst->is_quarantined());
                verify_fragments({sst}, permit, corrupt_fragments);
                break;
            }
        });
    }
}

// Test the scrub_reader in segregate mode and segregate_by_partition together,
// as they are used in scrub compaction in segregate mode.
SEASTAR_THREAD_TEST_CASE(test_scrub_segregate_stack) {
    simple_schema ss;
    auto schema = ss.schema();
    tests::reader_concurrency_semaphore_wrapper semaphore;
    auto permit = semaphore.make_permit();

    struct expected_rows_type {
        using expected_clustering_rows_type = std::set<clustering_key, clustering_key::less_compare>;

        bool has_static_row = false;
        expected_clustering_rows_type clustering_rows;

        explicit expected_rows_type(const ::schema& s) : clustering_rows(s) { }
    };
    using expected_partitions_type = std::map<dht::decorated_key, expected_rows_type, dht::decorated_key::less_comparator>;
    expected_partitions_type expected_partitions{dht::decorated_key::less_comparator(schema)};

    std::deque<mutation_fragment_v2> all_fragments;
    size_t double_partition_end = 0;
    size_t missing_partition_end = 0;

    for (uint32_t p = 0; p < 10; ++p) {
        auto dk = ss.make_pkey(tests::random::get_int<uint32_t>(0, 8));
        auto it = expected_partitions.find(dk);

        testlog.trace("Generating data for {} partition {}", it == expected_partitions.end() ? "new" : "existing", dk);

        if (it == expected_partitions.end()) {
            auto [inserted_it, _] = expected_partitions.emplace(dk, expected_rows_type(*schema));
            it = inserted_it;
        }

        all_fragments.emplace_back(*schema, permit, partition_start(dk, {}));

        auto& expected_rows = it->second;

        for (uint32_t r = 0; r < 10; ++r) {
            const auto is_clustering_row = tests::random::get_int<unsigned>(0, 8);
            if (is_clustering_row) {
                auto ck = ss.make_ckey(tests::random::get_int<uint32_t>(0, 8));
                testlog.trace("Generating clustering row {}", ck);

                all_fragments.emplace_back(*schema, permit, ss.make_row_v2(permit, ck, "cv"));
                expected_rows.clustering_rows.insert(ck);
            } else {
                testlog.trace("Generating static row");

                all_fragments.emplace_back(*schema, permit, ss.make_static_row_v2(permit, "sv"));
                expected_rows.has_static_row = true;
            }
        }

        const auto partition_end_roll = tests::random::get_int(0, 100);
        if (partition_end_roll < 80) {
            testlog.trace("Generating partition end");
            all_fragments.emplace_back(*schema, permit, partition_end());
        } else if (partition_end_roll < 90) {
            testlog.trace("Generating double partition end");
            ++double_partition_end;
            all_fragments.emplace_back(*schema, permit, partition_end());
            all_fragments.emplace_back(*schema, permit, partition_end());
        } else {
            testlog.trace("Not generating partition end");
            ++missing_partition_end;
        }
    }

    {
        size_t rows = 0;
        for (const auto& part : expected_partitions) {
            rows += part.second.clustering_rows.size();
        }
        testlog.info("Generated {} partitions (with {} double and {} missing partition ends), {} rows and {} fragments total", expected_partitions.size(), double_partition_end, missing_partition_end, rows, all_fragments.size());
    }

    auto copy_fragments = [&schema, &semaphore] (const std::deque<mutation_fragment_v2>& frags) {
        auto permit = semaphore.make_permit();
        std::deque<mutation_fragment_v2> copied_fragments;
        for (const auto& frag : frags) {
            copied_fragments.emplace_back(*schema, permit, frag);
        }
        return copied_fragments;
    };

    std::list<std::deque<mutation_fragment_v2>> segregated_fragment_streams;

    uint64_t validation_errors = 0;
    mutation_writer::segregate_by_partition(
            make_scrubbing_reader(make_mutation_reader_from_fragments(schema, permit, std::move(all_fragments)), sstables::compaction_type_options::scrub::mode::segregate, validation_errors),
            mutation_writer::segregate_config{100000},
            [&schema, &segregated_fragment_streams] (mutation_reader rd) {
        return async([&schema, &segregated_fragment_streams, rd = std::move(rd)] () mutable {
            auto close = deferred_close(rd);
            auto& fragments = segregated_fragment_streams.emplace_back();
            while (auto mf_opt = rd().get()) {
                fragments.emplace_back(*schema, rd.permit(), *mf_opt);
            }
        });
    }).get();

    testlog.info("Segregation resulted in {} fragment streams", segregated_fragment_streams.size());

    testlog.info("Checking position monotonicity of segregated streams");
    {
        size_t i = 0;
        for (const auto& segregated_fragment_stream : segregated_fragment_streams) {
            testlog.debug("Checking position monotonicity of segregated stream #{}", i++);
            assert_that(make_mutation_reader_from_fragments(schema, permit, copy_fragments(segregated_fragment_stream)))
                    .has_monotonic_positions();
        }
    }

    testlog.info("Checking position monotonicity of re-combined stream");
    {
        std::vector<mutation_reader> readers;
        readers.reserve(segregated_fragment_streams.size());

        for (const auto& segregated_fragment_stream : segregated_fragment_streams) {
            readers.emplace_back(make_mutation_reader_from_fragments(schema, permit, copy_fragments(segregated_fragment_stream)));
        }

        assert_that(make_combined_reader(schema, permit, std::move(readers))).has_monotonic_positions();
    }

    testlog.info("Checking content of re-combined stream");
    {
        std::vector<mutation_reader> readers;
        readers.reserve(segregated_fragment_streams.size());

        for (const auto& segregated_fragment_stream : segregated_fragment_streams) {
            readers.emplace_back(make_mutation_reader_from_fragments(schema, permit, copy_fragments(segregated_fragment_stream)));
        }

        auto rd = assert_that(make_combined_reader(schema, permit, std::move(readers)));
        for (const auto& [pkey, content] : expected_partitions) {
            testlog.debug("Checking content of partition {}", pkey);
            rd.produces_partition_start(pkey);
            if (content.has_static_row) {
                rd.produces_static_row();
            }
            for (const auto& ckey : content.clustering_rows) {
                rd.produces_row_with_key(ckey);
            }
            rd.produces_partition_end();
        }
        rd.produces_end_of_stream();
    }
}

SEASTAR_THREAD_TEST_CASE(sstable_scrub_reader_test) {
    auto schema = schema_builder("ks", get_name())
            .with_column("pk", utf8_type, column_kind::partition_key)
            .with_column("ck", int32_type, column_kind::clustering_key)
            .with_column("s", int32_type, column_kind::static_column)
            .with_column("v", int32_type).build();
    tests::reader_concurrency_semaphore_wrapper semaphore;
    auto permit = semaphore.make_permit();

    std::deque<mutation_fragment_v2> corrupt_fragments;
    std::deque<mutation_fragment_v2> scrubbed_fragments;

    const auto ts = api::timestamp_type{1};
    auto local_keys = tests::generate_partition_keys(5, schema);

    auto make_partition_start = [&, schema] (unsigned pk) {
        auto dkey = local_keys.at(pk);
        return mutation_fragment_v2(*schema, permit, partition_start(std::move(dkey), {}));
    };

    auto make_static_row = [&, schema] {
        auto r = row{};
        auto cdef = schema->static_column_at(0);
        auto ac = atomic_cell::make_live(*cdef.type, ts, cdef.type->decompose(data_value(1)));
        r.apply(cdef, atomic_cell_or_collection{std::move(ac)});
        return mutation_fragment_v2(*schema, permit, static_row(*schema, std::move(r)));
    };

    auto make_clustering_row = [&, schema] (unsigned i) {
        auto r = row{};
        auto cdef = schema->regular_column_at(0);
        auto ac = atomic_cell::make_live(*cdef.type, ts, cdef.type->decompose(data_value(1)));
        r.apply(cdef, atomic_cell_or_collection{std::move(ac)});
        return mutation_fragment_v2(*schema, permit,
                clustering_row(clustering_key::from_single_value(*schema, int32_type->decompose(data_value(int(i)))), {}, {}, std::move(r)));
    };

    auto add_fragment = [&, schema] (mutation_fragment_v2 mf, bool add_to_scrubbed = true) {
        corrupt_fragments.emplace_back(mutation_fragment_v2(*schema, permit, mf));
        if (add_to_scrubbed) {
            scrubbed_fragments.emplace_back(std::move(mf));
        }
    };

    // Partition 0
    add_fragment(make_partition_start(0));
    add_fragment(make_static_row());
    add_fragment(make_clustering_row(0));
    add_fragment(make_clustering_row(2));
    add_fragment(make_clustering_row(1), false); // out-of-order clustering key
    scrubbed_fragments.emplace_back(*schema, permit, partition_end{}); // missing partition-end

    // Partition 2
    add_fragment(make_partition_start(2));
    add_fragment(make_static_row());
    add_fragment(make_clustering_row(0));
    add_fragment(make_clustering_row(1));
    add_fragment(make_static_row(), false); // out-of-order static row
    add_fragment(mutation_fragment_v2(*schema, permit, partition_end{}));

    // Partition 1 - out-of-order
    add_fragment(make_partition_start(1), false);
    add_fragment(make_static_row(), false);
    add_fragment(make_clustering_row(0), false);
    add_fragment(make_clustering_row(1), false);
    add_fragment(make_clustering_row(2), false);
    add_fragment(make_clustering_row(3), false);
    add_fragment(mutation_fragment_v2(*schema, permit, partition_end{}), false);

    // Partition 3
    add_fragment(make_partition_start(3));
    add_fragment(make_static_row());
    add_fragment(make_clustering_row(0));
    add_fragment(make_clustering_row(1));
    add_fragment(make_clustering_row(2));
    add_fragment(make_clustering_row(3));
    scrubbed_fragments.emplace_back(*schema, permit, partition_end{}); // missing partition-end - at EOS

    uint64_t validation_errors = 0;
    auto r = assert_that(make_scrubbing_reader(make_mutation_reader_from_fragments(schema, permit, std::move(corrupt_fragments)),
                compaction_type_options::scrub::mode::skip, validation_errors));
    for (const auto& mf : scrubbed_fragments) {
       testlog.info("Expecting {}", mutation_fragment_v2::printer(*schema, mf));
       r.produces(*schema, mf);
    }
    r.produces_end_of_stream();
}

SEASTAR_TEST_CASE(scrubbed_sstable_removal_test) {
    // Test to verify that scrub removes the source sstable from the table upon completion
    // https://github.com/scylladb/scylladb/issues/20030
    return test_env::do_with_async([] (test_env& env) {
        simple_schema ss;
        auto s = ss.schema();
        auto pk = ss.make_pkey();

        auto mut1 = mutation(s, pk);
        mut1.partition().apply_insert(*s, ss.make_ckey(0), ss.new_timestamp());
        auto sst = make_sstable_containing(env.make_sstable(s), {std::move(mut1)});

        auto cf = env.make_table_for_tests(s);
        auto close_cf = deferred_stop(cf);

        // add the sstable to cf's maintenance set
        cf->add_sstable_and_update_cache(sst, sstables::offstrategy::yes).get();
        auto& cf_ts = cf.as_table_state();
        auto maintenance_sst_set = cf_ts.maintenance_sstable_set();
        BOOST_REQUIRE_EQUAL(maintenance_sst_set.size(), 1);
        BOOST_REQUIRE_EQUAL(*maintenance_sst_set.all()->begin(), sst);
        // confirm main sstable_set is empty
        BOOST_REQUIRE_EQUAL(cf_ts.main_sstable_set().size(), 0);

        // Perform scrub on the table
        cf->get_compaction_manager().perform_sstable_scrub(cf_ts, {}, {}).get();

        // main set should have the resultant sst and the maintenance set should be empty now
        BOOST_REQUIRE_EQUAL(cf_ts.main_sstable_set().size(), 1);
        BOOST_REQUIRE_EQUAL(cf_ts.maintenance_sstable_set().size(), 0);

        // Now that there is an sstable in main set, perform scrub on the table
        // again to verify that the result ends up again in main sstable_set
        cf->get_compaction_manager().perform_sstable_scrub(cf_ts, {}, {}).get();
        BOOST_REQUIRE_EQUAL(cf_ts.main_sstable_set().size(), 1);
        BOOST_REQUIRE_EQUAL(cf_ts.maintenance_sstable_set().size(), 0);
    });
}

SEASTAR_TEST_CASE(sstable_run_based_compaction_test) {
    return test_env::do_with_async([] (test_env& env) {
        auto builder = schema_builder("tests", "sstable_run_based_compaction_test")
                .with_column("id", utf8_type, column_kind::partition_key)
                .with_column("value", int32_type);
        auto s = builder.build();

        auto sst_gen = env.make_sst_factory(s);

        auto cf = env.make_table_for_tests(s);
        auto close_cf = deferred_stop(cf);
        cf->start();
        cf->set_compaction_strategy(sstables::compaction_strategy_type::size_tiered);
        auto compact = [&, s] (std::vector<shared_sstable> all, auto replacer) -> std::vector<shared_sstable> {
            return compact_sstables(env, sstables::compaction_descriptor(std::move(all), 1, 0), cf, sst_gen, replacer).get().new_sstables;
        };
        auto make_insert = [&] (const dht::decorated_key& key) {
            mutation m(s, key);
            m.set_clustered_cell(clustering_key::make_empty(), bytes("value"), data_value(int32_t(1)), 1 /* ts */);
            return m;
        };

        const auto keys = tests::generate_partition_keys(16, s, local_shard_only::yes, tests::key_size{8, 8});
        std::unordered_set<shared_sstable> sstables;
        std::vector<utils::observer<sstable&>> observers;
        sstables::sstable_run_based_compaction_strategy_for_tests cs;

        auto do_replace = [&] (const std::vector<shared_sstable>& old_sstables, const std::vector<shared_sstable>& new_sstables) {
            for (auto& old_sst : old_sstables) {
                BOOST_REQUIRE(sstables.contains(old_sst));
                sstables.erase(old_sst);
            }
            for (auto& new_sst : new_sstables) {
                BOOST_REQUIRE(!sstables.contains(new_sst));
                sstables.insert(new_sst);
            }
            column_family_test(cf).rebuild_sstable_list(cf.as_table_state(), new_sstables, old_sstables).get();
            env.test_compaction_manager().propagate_replacement(cf.as_table_state(), old_sstables, new_sstables);
        };

        auto do_incremental_replace = [&] (auto old_sstables, auto new_sstables, auto& expected_sst, auto& closed_sstables_tracker) {
            // that's because each sstable will contain only 1 mutation.
            BOOST_REQUIRE_EQUAL(old_sstables.size(), 1);
            BOOST_REQUIRE_EQUAL(new_sstables.size(), 1);
            auto old_sstable = old_sstables.front();
            // check that sstable replacement follows token order
            BOOST_REQUIRE_EQUAL(*expected_sst, old_sstable->generation());
            expected_sst++;
            // check that previously released sstables were already closed
            BOOST_REQUIRE_EQUAL(*closed_sstables_tracker, old_sstable->generation());

            do_replace(old_sstables, new_sstables);

            observers.push_back(old_sstable->add_on_closed_handler([&] (sstable& sst) {
                testlog.info("Closing sstable of generation {}", sst.generation());
                closed_sstables_tracker++;
            }));

            testlog.info("Removing sstable of generation {}, refcnt: {}", old_sstable->generation(), old_sstable.use_count());
        };

        auto do_compaction = [&] (size_t expected_input, size_t expected_output) mutable -> std::vector<shared_sstable> {
            auto input_ssts = std::vector<shared_sstable>(sstables.begin(), sstables.end());
            auto desc = get_sstables_for_compaction(cs, cf.as_table_state(), std::move(input_ssts));

            // nothing to compact, move on.
            if (desc.sstables.empty()) {
                return {};
            }
            std::unordered_set<sstables::run_id> run_ids;
            bool incremental_enabled = std::any_of(desc.sstables.begin(), desc.sstables.end(), [&run_ids] (shared_sstable& sst) {
                return !run_ids.insert(sst->run_identifier()).second;
            });

            BOOST_REQUIRE_EQUAL(desc.sstables.size(), expected_input);
            auto sstable_run = desc.sstables
                | std::views::transform([] (auto& sst) { return sst->generation(); })
                | std::ranges::to<std::set<sstables::generation_type>>();
            auto expected_sst = sstable_run.begin();
            auto closed_sstables_tracker = sstable_run.begin();
            auto replacer = [&] (sstables::compaction_completion_desc desc) {
                auto old_sstables = std::move(desc.old_sstables);
                auto new_sstables = std::move(desc.new_sstables);
                BOOST_REQUIRE(expected_sst != sstable_run.end());
                if (incremental_enabled) {
                    do_incremental_replace(std::move(old_sstables), std::move(new_sstables), expected_sst, closed_sstables_tracker);
                } else {
                    do_replace(std::move(old_sstables), std::move(new_sstables));
                    expected_sst = sstable_run.end();
                }
            };

            auto result = compact(std::move(desc.sstables), replacer);
            BOOST_REQUIRE_EQUAL(expected_output, result.size());
            BOOST_REQUIRE(expected_sst == sstable_run.end());
            return result;
        };

        // Generate 4 sstable runs composed of 4 fragments each after 4 compactions.
        // All fragments non-overlapping.
        for (auto i = 0U; i < keys.size(); i++) {
            auto sst = make_sstable_containing(sst_gen, { make_insert(keys[i]) });
            sst->set_sstable_level(1);
            BOOST_REQUIRE_EQUAL(sst->get_sstable_level(), 1);
            column_family_test(cf).add_sstable(sst).get();
            sstables.insert(std::move(sst));
            do_compaction(4, 4);
        }
        BOOST_REQUIRE_EQUAL(sstables.size(), 16);

        // Generate 1 sstable run from 4 sstables runs of similar size
        auto result = do_compaction(16, 16);
        BOOST_REQUIRE_EQUAL(result.size(), 16);
        for (auto i = 0U; i < keys.size(); i++) {
            assert_that(sstable_reader(result[i], s, env.make_reader_permit()))
                .produces(make_insert(keys[i]))
                .produces_end_of_stream();
        }
    });
}

SEASTAR_TEST_CASE(compaction_strategy_aware_major_compaction_test) {
    return test_env::do_with_async([] (test_env& env) {
        auto s = schema_builder("tests", "compaction_strategy_aware_major_compaction_test")
                .with_column("id", utf8_type, column_kind::partition_key)
                .with_column("value", int32_type).build();

        auto make_insert = [&] (partition_key key) {
            mutation m(s, key);
            m.set_clustered_cell(clustering_key::make_empty(), bytes("value"), data_value(int32_t(1)), api::timestamp_type(0));
            return m;
        };

        auto alpha = partition_key::from_exploded(*s, {to_bytes("alpha")});
        auto sst = make_sstable_containing(env.make_sstable(s), {make_insert(alpha)});
        sst->set_sstable_level(2);
        auto sst2 = make_sstable_containing(env.make_sstable(s), {make_insert(alpha)});
        sst2->set_sstable_level(3);
        auto candidates = std::vector<sstables::shared_sstable>({ sst, sst2 });

        auto cf = env.make_table_for_tests();
        auto close_cf = deferred_stop(cf);

        {
            auto cs = sstables::make_compaction_strategy(sstables::compaction_strategy_type::leveled, cf.schema()->compaction_strategy_options());
            auto descriptor = cs.get_major_compaction_job(cf.as_table_state(), candidates);
            BOOST_REQUIRE(descriptor.sstables.size() == candidates.size());
            BOOST_REQUIRE(uint32_t(descriptor.level) == leveled_compaction_strategy::ideal_level_for_input(candidates, 160*1024*1024));
        }

        {
            auto cs = sstables::make_compaction_strategy(sstables::compaction_strategy_type::size_tiered, cf.schema()->compaction_strategy_options());
            auto descriptor = cs.get_major_compaction_job(cf.as_table_state(), candidates);
            BOOST_REQUIRE(descriptor.sstables.size() == candidates.size());
            BOOST_REQUIRE(descriptor.level == 0);
        }
    });
}

SEASTAR_TEST_CASE(backlog_tracker_correctness_after_changing_compaction_strategy) {
    return test_env::do_with_async([] (test_env& env) {
        auto builder = schema_builder("tests", "backlog_tracker_correctness_after_changing_compaction_strategy")
                .with_column("id", utf8_type, column_kind::partition_key)
                .with_column("value", int32_type);
        auto s = builder.build();

        auto sst_gen = env.make_sst_factory(s);

        auto cf = env.make_table_for_tests(s);
        auto close_cf = deferred_stop(cf);
        cf->set_compaction_strategy(sstables::compaction_strategy_type::leveled);

        {
            const auto keys = tests::generate_partition_keys(4, s);
            auto make_insert = [&] (const dht::decorated_key& key) {
                mutation m(s, key);
                m.set_clustered_cell(clustering_key::make_empty(), bytes("value"), data_value(int32_t(1)), 1 /* ts */);
                return m;
            };
            auto mut1 = make_insert(keys[0]);
            auto mut2 = make_insert(keys[1]);
            auto mut3 = make_insert(keys[2]);
            auto mut4 = make_insert(keys[3]);
            std::vector<shared_sstable> ssts = {
                    make_sstable_containing(sst_gen, {mut1, mut2}),
                    make_sstable_containing(sst_gen, {mut3, mut4})
            };

            for (auto& sst : ssts) {
                cf.as_table_state().get_backlog_tracker().replace_sstables({}, {sst});
            }

            // Start compaction, then stop tracking compaction, switch to TWCS, wait for compaction to finish and check for backlog.
            // That's done to SCYLLA_ASSERT backlog will work for compaction that is finished and was stopped tracking.

            auto fut = compact_sstables(env, sstables::compaction_descriptor(ssts), cf, sst_gen);

            // set_compaction_strategy() itself is responsible for transferring charges from old to new backlog tracker.
            cf->set_compaction_strategy(sstables::compaction_strategy_type::time_window);
            for (auto& sst : ssts) {
                cf.as_table_state().get_backlog_tracker().replace_sstables({}, {sst});
            }

            auto ret = fut.get();
            BOOST_REQUIRE(ret.new_sstables.size() == 1);
        }
        // triggers code that iterates through registered compactions.
        cf->get_compaction_manager().backlog();
        cf.as_table_state().get_backlog_tracker().backlog();
    });
}

SEASTAR_TEST_CASE(partial_sstable_run_filtered_out_test) {
    BOOST_REQUIRE(smp::count == 1);
    return test_env::do_with_async([] (test_env& env) {
        auto s = schema_builder("tests", "partial_sstable_run_filtered_out_test")
                .with_column("id", utf8_type, column_kind::partition_key)
                .with_column("value", int32_type).build();

        auto cf = env.make_table_for_tests(s);
        auto close_cf = deferred_stop(cf);
        cf->start();

        sstables::run_id partial_sstable_run_identifier = sstables::run_id::create_random_id();
        mutation mut(s, partition_key::from_exploded(*s, {to_bytes("alpha")}));
        mut.set_clustered_cell(clustering_key::make_empty(), bytes("value"), data_value(int32_t(1)), 0);

        sstable_writer_config sst_cfg = env.manager().configure_writer();
        sst_cfg.run_identifier = partial_sstable_run_identifier;
        auto partial_sstable_run_sst = make_sstable_easy(env, make_mutation_reader_from_mutations_v2(s, env.make_reader_permit(), std::move(mut)), sst_cfg);

        column_family_test(cf).add_sstable(partial_sstable_run_sst).get();
        column_family_test::update_sstables_known_generation(*cf, partial_sstable_run_sst->generation());

        auto generation_exists = [&cf] (sstables::generation_type generation) {
            auto sstables = cf->get_sstables();
            auto entry = std::ranges::find_if(*sstables, [generation] (shared_sstable sst) { return generation == sst->generation(); });
            return entry != sstables->end();
        };

        BOOST_REQUIRE(generation_exists(partial_sstable_run_sst->generation()));

        // register partial sstable run
        run_compaction_task(env, partial_sstable_run_identifier, cf.as_table_state(), [&cf] (sstables::compaction_data&) {
            return cf->compact_all_sstables(tasks::task_info{});
        }).get();

        // make sure partial sstable run has none of its fragments compacted.
        BOOST_REQUIRE(generation_exists(partial_sstable_run_sst->generation()));
    }, test_env_config{ .use_uuid = false });
}

// Make sure that a custom tombstone-gced-only writer will be fed with gc'able tombstone
// from the regular compaction's input sstable.
SEASTAR_TEST_CASE(purged_tombstone_consumer_sstable_test) {
    BOOST_REQUIRE(smp::count == 1);
    return test_env::do_with_async([] (test_env& env) {
        auto builder = schema_builder("tests", "purged_tombstone_consumer_sstable_test")
                .with_column("id", utf8_type, column_kind::partition_key)
                .with_column("value", int32_type);
        builder.set_gc_grace_seconds(0);
        auto s = builder.build();

        class compacting_sstable_writer_test {
            shared_sstable& _sst;
            sstable_writer _writer;
        public:
            explicit compacting_sstable_writer_test(const schema_ptr& s, shared_sstable& sst, sstables_manager& manager)
                : _sst(sst),
                  _writer(sst->get_writer(*s, 1, manager.configure_writer("test"), encoding_stats{})) {}

            void consume_new_partition(const dht::decorated_key& dk) { _writer.consume_new_partition(dk); }
            void consume(tombstone t) { _writer.consume(t); }
            stop_iteration consume(static_row&& sr, tombstone, bool) { return _writer.consume(std::move(sr)); }
            stop_iteration consume(clustering_row&& cr, row_tombstone tomb, bool) { return _writer.consume(std::move(cr)); }
            stop_iteration consume(range_tombstone_change&& rtc) { return _writer.consume(std::move(rtc)); }

            stop_iteration consume_end_of_partition() { return _writer.consume_end_of_partition(); }
            void consume_end_of_stream() { _writer.consume_end_of_stream(); _sst->open_data().get(); }
        };

        std::optional<gc_clock::time_point> gc_before;
        auto max_purgeable_ts = api::max_timestamp;
        auto is_tombstone_purgeable = [&gc_before, max_purgeable_ts](const tombstone& t) {
            bool can_gc = t.deletion_time < *gc_before;
            return t && can_gc && t.timestamp < max_purgeable_ts;
        };

        auto compact = [&] (std::vector<shared_sstable> all) -> std::pair<shared_sstable, shared_sstable> {
            auto max_purgeable_func = [max_purgeable_ts] (const dht::decorated_key& dk, is_shadowable) {
                return max_purgeable_ts;
            };

            auto non_purged = env.make_sstable(s);
            auto purged_only = env.make_sstable(s);

            auto cr = compacting_sstable_writer_test(s, non_purged, env.manager());
            auto purged_cr = compacting_sstable_writer_test(s, purged_only, env.manager());

            auto gc_now = gc_clock::now();
            gc_before = gc_now - s->gc_grace_seconds();

            auto cfc = compact_for_compaction_v2<compacting_sstable_writer_test, compacting_sstable_writer_test>(
                *s, gc_now, max_purgeable_func, tombstone_gc_state(nullptr), std::move(cr), std::move(purged_cr));

            auto cs = sstables::make_compaction_strategy(sstables::compaction_strategy_type::size_tiered, s->compaction_strategy_options());
            auto compacting = make_lw_shared<sstables::sstable_set>(cs.make_sstable_set(s));
            for (auto&& sst : all) {
                compacting->insert(std::move(sst));
            }
            auto r = compacting->make_range_sstable_reader(s,
                env.make_reader_permit(),
                query::full_partition_range,
                s->full_slice(),
                nullptr,
                ::streamed_mutation::forwarding::no,
                ::mutation_reader::forwarding::no);

            auto close_r = deferred_close(r);
            r.consume_in_thread(std::move(cfc));

            return {std::move(non_purged), std::move(purged_only)};
        };

        auto next_timestamp = [] {
            static thread_local api::timestamp_type next = 1;
            return next++;
        };

        auto make_insert = [&] (partition_key key) {
            mutation m(s, key);
            m.set_clustered_cell(clustering_key::make_empty(), bytes("value"), data_value(int32_t(1)), next_timestamp());
            return m;
        };

        auto make_delete = [&] (partition_key key) -> std::pair<mutation, tombstone> {
            mutation m(s, key);
            tombstone tomb(next_timestamp(), gc_clock::now());
            m.partition().apply(tomb);
            return {m, tomb};
        };

        auto alpha = partition_key::from_exploded(*s, {to_bytes("alpha")});
        auto beta = partition_key::from_exploded(*s, {to_bytes("beta")});

        auto ttl = 5;

        auto assert_that_produces_purged_tombstone = [&] (auto& sst, partition_key& key, tombstone tomb) {
            auto reader = make_lw_shared<mutation_reader>(sstable_reader(sst, s, env.make_reader_permit()));
            read_mutation_from_mutation_reader(*reader).then([reader, s, &key, is_tombstone_purgeable, &tomb] (mutation_opt m) {
                BOOST_REQUIRE(m);
                BOOST_REQUIRE(m->key().equal(*s, key));
                auto rows = m->partition().clustered_rows();
                BOOST_REQUIRE_EQUAL(rows.calculate_size(), 0);
                BOOST_REQUIRE(is_tombstone_purgeable(m->partition().partition_tombstone()));
                BOOST_REQUIRE(m->partition().partition_tombstone() == tomb);
                return (*reader)();
            }).then([reader, s] (mutation_fragment_v2_opt m) {
                BOOST_REQUIRE(!m);
            }).finally([reader] {
                return reader->close();
            }).get();
        };

        // gc'ed tombstone for alpha will go to gc-only consumer, whereas live data goes to regular consumer.
        {
            auto mut1 = make_insert(alpha);
            auto mut2 = make_insert(beta);
            auto [mut3, mut3_tombstone] = make_delete(alpha);

            std::vector<shared_sstable> sstables = {
                make_sstable_containing(env.make_sstable(s), {mut1, mut2}),
                make_sstable_containing(env.make_sstable(s), {mut3})
            };

            forward_jump_clocks(std::chrono::seconds(ttl));

            auto [non_purged, purged_only] = compact(std::move(sstables));

            assert_that(sstable_reader(non_purged, s, env.make_reader_permit()))
                    .produces(mut2)
                    .produces_end_of_stream();

            assert_that_produces_purged_tombstone(purged_only, alpha, mut3_tombstone);
        }
    });
}

/*  Make sure data is not resurrected.
    sstable 1 with key A and key B and key C
    sstable 2 with expired (GC'able) tombstone for key A

    use max_sstable_size = 1;

    so key A and expired tombstone for key A are compacted away.
    key B is written into a new sstable, and sstable 2 is removed.

    Need to stop compaction at this point!!!

    Result: sstable 1 is alive in the table, whereas sstable 2 is gone.

    if key A can be read from table, data was resurrected.
 */
SEASTAR_TEST_CASE(incremental_compaction_data_resurrection_test) {
    return test_env::do_with_async([] (test_env& env) {
        // In a column family with gc_grace_seconds set to 0, check that a tombstone
        // is purged after compaction.
        auto builder = schema_builder("tests", "incremental_compaction_data_resurrection_test")
                .with_column("id", utf8_type, column_kind::partition_key)
                .with_column("value", int32_type);
        builder.set_gc_grace_seconds(0);
        auto s = builder.build();

        auto sst_gen = env.make_sst_factory(s);

        auto next_timestamp = [] {
            static thread_local api::timestamp_type next = 1;
            return next++;
        };

        auto make_insert = [&] (const dht::decorated_key& key) {
            mutation m(s, key);
            m.set_clustered_cell(clustering_key::make_empty(), bytes("value"), data_value(int32_t(1)), next_timestamp());
            return m;
        };

        auto deletion_time = gc_clock::now();
        auto make_delete = [&] (const dht::decorated_key& key) {
            mutation m(s, key);
            tombstone tomb(next_timestamp(), deletion_time);
            m.partition().apply(tomb);
            return m;
        };

        const auto keys = tests::generate_partition_keys(3, s);
        const auto& alpha = keys[0];
        const auto& beta = keys[1];
        const auto& gamma = keys[2];

        auto ttl = 5;

        auto mut1 = make_insert(alpha);
        auto mut2 = make_insert(beta);
        auto mut3 = make_insert(gamma);
        auto mut1_deletion = make_delete(alpha);

        auto non_expired_sst = make_sstable_containing(sst_gen, {mut1, mut2, mut3});
        auto expired_sst = make_sstable_containing(sst_gen, {mut1_deletion});

        std::vector<shared_sstable> sstables = {
                non_expired_sst,
                expired_sst,
        };

        // make mut1_deletion gc'able.
        forward_jump_clocks(std::chrono::seconds(ttl));

        auto cf = env.make_table_for_tests(s);
        auto close_cf = deferred_stop(cf);
        cf->start();
        cf->set_compaction_strategy(sstables::compaction_strategy_type::null);

        // since we use compacting_reader expired tombstones shouldn't be read from sstables
        // so we just check there are no live (resurrected for this test) rows in partition
        auto is_partition_dead = [&s, &cf, &env] (const dht::decorated_key& key) {
            replica::column_family::const_mutation_partition_ptr mp = cf->find_partition(s, env.make_reader_permit(), key).get();
            return mp && mp->live_row_count(*s, gc_clock::time_point::max()) == 0;
        };

        cf->add_sstable_and_update_cache(non_expired_sst).get();
        BOOST_REQUIRE(!is_partition_dead(alpha));
        cf->add_sstable_and_update_cache(expired_sst).get();
        BOOST_REQUIRE(is_partition_dead(alpha));

        auto replacer = [&] (sstables::compaction_completion_desc desc) {
            auto old_sstables = std::move(desc.old_sstables);
            auto new_sstables = std::move(desc.new_sstables);
            // expired_sst is exhausted, and new sstable is written with mut 2.
            BOOST_REQUIRE_EQUAL(old_sstables.size(), 1);
            BOOST_REQUIRE(old_sstables.front() == expired_sst);
            BOOST_REQUIRE_EQUAL(new_sstables.size(), 2);
            for (auto& new_sstable : new_sstables) {
                if (new_sstable->get_max_local_deletion_time() == deletion_time) { // Skipping GC SSTable.
                    continue;
                }
                assert_that(sstable_reader(new_sstable, s, env.make_reader_permit()))
                    .produces(mut2)
                    .produces_end_of_stream();
            }
            column_family_test(cf).rebuild_sstable_list(cf.as_table_state(), new_sstables, old_sstables).get();
            // force compaction failure after sstable containing expired tombstone is removed from set.
            throw std::runtime_error("forcing compaction failure on early replacement");
        };

        // make ssts belong to same run for compaction to enable incremental approach.
        // That needs to happen after fragments were inserted into sstable_set, as they'll placed into different runs due to detected overlapping.
        auto run_id = sstables::run_id::create_random_id();
        sstables::test(non_expired_sst).set_run_identifier(run_id);
        sstables::test(expired_sst).set_run_identifier(run_id);

        bool swallowed = false;
        try {
            // The goal is to have one sstable generated for each mutation to trigger the issue.
            auto max_sstable_size = 0;
            auto result = compact_sstables(env, sstables::compaction_descriptor(sstables, 0, max_sstable_size), cf, sst_gen, replacer).get().new_sstables;
            BOOST_REQUIRE_EQUAL(2, result.size());
        } catch (...) {
            // swallow exception
            swallowed = true;
        }
        BOOST_REQUIRE(swallowed);
        // check there's no data resurrection
        BOOST_REQUIRE(is_partition_dead(alpha));
    });
}

SEASTAR_TEST_CASE(twcs_major_compaction_test) {
    // Tests that two mutations that were written a month apart are compacted
    // to two different SSTables, whereas two mutations that were written 1ms apart
    // are compacted to the same SSTable.
    return test_env::do_with_async([] (test_env& env) {
        // In a column family with gc_grace_seconds set to 0, check that a tombstone
        // is purged after compaction.
        auto builder = schema_builder("tests", "twcs_major")
                .with_column("id", utf8_type, column_kind::partition_key)
                .with_column("cl", int32_type, column_kind::clustering_key)
                .with_column("value", int32_type);
        auto s = builder.build();

        auto sst_gen = env.make_sst_factory(s);

        auto next_timestamp = [] (auto step) {
            using namespace std::chrono;
            return (api::timestamp_clock::now().time_since_epoch() - duration_cast<microseconds>(step)).count();
        };

        auto make_insert = [&] (api::timestamp_clock::duration step) {
            static thread_local int32_t value = 1;

            auto key = tests::generate_partition_key(s);

            mutation m(s, key);
            auto c_key = clustering_key::from_exploded(*s, {int32_type->decompose(value++)});
            m.set_clustered_cell(c_key, bytes("value"), data_value(int32_t(value)), next_timestamp(step));
            return m;
        };


        // Two mutations, one of them 30 days ago. Should be split when
        // compacting
        auto mut1 = make_insert(0ms);
        auto mut2 = make_insert(720h);

        // Two mutations, close together. Should end up in the same SSTable
        auto mut3 = make_insert(0ms);
        auto mut4 = make_insert(1ms);

        auto cf = env.make_table_for_tests(s);
        auto close_cf = deferred_stop(cf);
        cf->start();
        cf->set_compaction_strategy(sstables::compaction_strategy_type::time_window);

        auto original_together = make_sstable_containing(sst_gen, {mut3, mut4});

        auto ret = compact_sstables(env, sstables::compaction_descriptor({original_together}), cf, sst_gen, replacer_fn_no_op()).get();
        BOOST_REQUIRE(ret.new_sstables.size() == 1);

        auto original_apart = make_sstable_containing(sst_gen, {mut1, mut2});
        ret = compact_sstables(env, sstables::compaction_descriptor({original_apart}), cf, sst_gen, replacer_fn_no_op()).get();
        BOOST_REQUIRE(ret.new_sstables.size() == 2);
    });
}

SEASTAR_TEST_CASE(autocompaction_control_test) {
    return test_env::do_with_async([] (test_env& env) {
        auto s = schema_builder(some_keyspace, some_column_family)
                .with_column("id", utf8_type, column_kind::partition_key)
                .with_column("value", int32_type)
                .build();

        auto cf = env.make_table_for_tests(s);
        auto& cm = cf->get_compaction_manager();
        auto close_cf = deferred_stop(cf);
        cf->set_compaction_strategy(sstables::compaction_strategy_type::size_tiered);

        // no compactions done yet
        auto& ss = cm.get_stats();
        BOOST_REQUIRE(cm.get_stats().pending_tasks == 0 && cm.get_stats().active_tasks == 0 && ss.completed_tasks == 0);
        // auto compaction is enabled by default
        BOOST_REQUIRE(!cf->is_auto_compaction_disabled_by_user());
        // disable auto compaction by user
        cf->disable_auto_compaction().get();
        // check it is disabled
        BOOST_REQUIRE(cf->is_auto_compaction_disabled_by_user());

        auto make_insert = [&] (const dht::decorated_key& key) {
            mutation m(s, key);
            m.set_clustered_cell(clustering_key::make_empty(), bytes("value"), data_value(int32_t(1)), 1 /* ts */);
            return m;
        };
        auto min_threshold = cf->schema()->min_compaction_threshold();
        const auto keys = tests::generate_partition_keys(1, s);
        for (auto i = 0; i < 2 * min_threshold; ++i) {
            auto mut = make_insert(keys[0]);
            auto sst = make_sstable_containing(env.make_sstable(s), {mut});
            cf->add_sstable_and_update_cache(sst).wait();
        }

        // check compaction manager does not receive background compaction submissions
        cf->start();
        auto stop_cf = deferred_stop(*cf);
        cf->trigger_compaction();
        cm.submit(cf.as_table_state());
        BOOST_REQUIRE(cm.get_stats().pending_tasks == 0 && cm.get_stats().active_tasks == 0 && ss.completed_tasks == 0);
        // enable auto compaction
        cf->enable_auto_compaction();
        // check enabled
        BOOST_REQUIRE(!cf->is_auto_compaction_disabled_by_user());
        // trigger background compaction
        cf->trigger_compaction();
        // wait until compaction finished
        do_until([&ss] { return ss.completed_tasks > 0 && ss.pending_tasks == 0; }, [] {
            return sleep(std::chrono::milliseconds(1));
        }).wait();
        // test no more running compactions
        BOOST_REQUIRE(ss.active_tasks == 0);
        // test compaction successfully finished
        BOOST_REQUIRE(ss.errors == 0);
        BOOST_REQUIRE(ss.completed_tasks == 1);
    });
}

//
// Test that https://github.com/scylladb/scylla/issues/6472 is gone
//
SEASTAR_TEST_CASE(test_bug_6472) {
    return test_env::do_with_async([] (test_env& env) {
        auto builder = schema_builder("tests", "test_bug_6472")
                .with_column("id", utf8_type, column_kind::partition_key)
                .with_column("cl", int32_type, column_kind::clustering_key)
                .with_column("value", int32_type);
        builder.set_compaction_strategy(sstables::compaction_strategy_type::time_window);
        std::map<sstring, sstring> opts = {
            { time_window_compaction_strategy_options::COMPACTION_WINDOW_UNIT_KEY, "HOURS" },
            { time_window_compaction_strategy_options::COMPACTION_WINDOW_SIZE_KEY, "1" },
        };
        builder.set_compaction_strategy_options(std::move(opts));
        builder.set_gc_grace_seconds(0);
        auto s = builder.build();

        auto sst_gen = env.make_sst_factory(s);

        auto next_timestamp = [] (auto step) {
            using namespace std::chrono;
            return (gc_clock::now().time_since_epoch() - duration_cast<microseconds>(step)).count();
        };

        auto key = tests::generate_partition_key(s);

        auto make_expiring_cell = [&] (std::chrono::hours step) {
            static thread_local int32_t value = 1;

            mutation m(s, key);
            auto c_key = clustering_key::from_exploded(*s, {int32_type->decompose(value++)});
            m.set_clustered_cell(c_key, bytes("value"), data_value(int32_t(value)), next_timestamp(step), gc_clock::duration(step + 5s));
            return m;
        };

        auto cf = env.make_table_for_tests(s);
        auto close_cf = deferred_stop(cf);
        cf->start();

        // Make 100 expiring cells which belong to different time windows
        std::vector<mutation> muts;
        muts.reserve(101);
        for (auto i = 1; i < 101; i++) {
            muts.push_back(make_expiring_cell(std::chrono::hours(i)));
        }
        muts.push_back(make_expiring_cell(std::chrono::hours(110)));

        //
        // Reproduce issue 6472 by making an input set which causes both interposer and GC writer to be enabled
        //
        std::vector<shared_sstable> sstables_spanning_many_windows = {
            make_sstable_containing(sst_gen, muts),
            make_sstable_containing(sst_gen, muts),
        };
        sstables::run_id run_id = sstables::run_id::create_random_id();
        for (auto& sst : sstables_spanning_many_windows) {
            sstables::test(sst).set_run_identifier(run_id);
        }

        // Make sure everything we wanted expired is expired by now.
        forward_jump_clocks(std::chrono::hours(101));

        auto ret = compact_sstables(env, sstables::compaction_descriptor(sstables_spanning_many_windows), cf, sst_gen, replacer_fn_no_op()).get();
        BOOST_REQUIRE(ret.new_sstables.size() == 1);
    });
}

SEASTAR_TEST_CASE(sstable_needs_cleanup_test) {
  return test_env::do_with_async([] (test_env& env) {
    auto s = schema_builder(some_keyspace, some_column_family).with_column("p1", utf8_type, column_kind::partition_key).build();
    const auto keys = tests::generate_partition_keys(10, s);

    auto sst_gen = [&env, s] (const dht::decorated_key& first, const dht::decorated_key& last) mutable {
        return sstable_for_overlapping_test(env, s, first.key(), last.key());
    };
    auto token_range = [&] (size_t first, size_t last) -> dht::token_range {
        return dht::token_range::make(keys[first].token(), keys[last].token());
    };

    {
        auto local_ranges = { token_range(0, 9) };
        auto sst = sst_gen(keys[0], keys[9]);
        BOOST_REQUIRE(!needs_cleanup(sst, local_ranges));
    }

    {
        auto local_ranges = { token_range(0, 1), token_range(3, 4), token_range(5, 6) };

        auto sst = sst_gen(keys[0], keys[1]);
        BOOST_REQUIRE(!needs_cleanup(sst, local_ranges));

        auto sst2 = sst_gen(keys[2], keys[2]);
        BOOST_REQUIRE(needs_cleanup(sst2, local_ranges));

        auto sst3 = sst_gen(keys[0], keys[6]);
        BOOST_REQUIRE(needs_cleanup(sst3, local_ranges));

        auto sst5 = sst_gen(keys[7], keys[7]);
        BOOST_REQUIRE(needs_cleanup(sst5, local_ranges));
    }
  });
}

SEASTAR_TEST_CASE(test_twcs_partition_estimate) {
    return test_env::do_with_async([] (test_env& env) {
        auto builder = schema_builder("tests", "test_bug_6472")
                .with_column("id", utf8_type, column_kind::partition_key)
                .with_column("cl", int32_type, column_kind::clustering_key)
                .with_column("value", int32_type);
        builder.set_compaction_strategy(sstables::compaction_strategy_type::time_window);
        std::map<sstring, sstring> opts = {
            { time_window_compaction_strategy_options::COMPACTION_WINDOW_UNIT_KEY, "HOURS" },
            { time_window_compaction_strategy_options::COMPACTION_WINDOW_SIZE_KEY, "1" },
        };
        builder.set_compaction_strategy_options(std::move(opts));
        builder.set_gc_grace_seconds(0);
        auto s = builder.build();

        const auto rows_per_partition = 200;

        auto sst_gen = env.make_sst_factory(s);

        auto next_timestamp = [] (int sstable_idx, int ck_idx) {
            using namespace std::chrono;
            auto window = hours(sstable_idx * rows_per_partition + ck_idx);
            return (gc_clock::now().time_since_epoch() - duration_cast<microseconds>(window)).count();
        };

        auto keys = tests::generate_partition_keys(4, s);

        auto make_sstable = [&] (int sstable_idx) {
            static thread_local int32_t value = 1;

            auto key = keys[sstable_idx];

            mutation m(s, key);
            for (auto ck = 0; ck < rows_per_partition; ++ck) {
                auto c_key = clustering_key::from_exploded(*s, {int32_type->decompose(value++)});
                m.set_clustered_cell(c_key, bytes("value"), data_value(int32_t(value)), next_timestamp(sstable_idx, ck));
            }
            return make_sstable_containing(sst_gen, {m});
        };

        auto cf = env.make_table_for_tests(s);
        auto close_cf = deferred_stop(cf);
        cf->start();

        auto ceil_div = [] (int dividend, int divisor) { return (dividend + divisor - 1) / divisor; };

        auto estimation_test = [ceil_div] (schema_ptr s, uint64_t window_count) {
            auto cs = sstables::make_compaction_strategy(sstables::compaction_strategy_type::time_window, s->compaction_strategy_options());
            mutation_source_metadata ms_metadata{};
            const int partitions = 100;
            BOOST_REQUIRE_EQUAL(cs.adjust_partition_estimate(ms_metadata, partitions, s),
                                ceil_div(partitions, window_count));
        };
        {
            static constexpr int window_count = 20;
            builder.set_default_time_to_live(std::chrono::duration_cast<std::chrono::seconds>(std::chrono::hours(window_count)));
            auto s = builder.build();
            estimation_test(s, window_count);
        }

        {
            builder.set_default_time_to_live(0s);
            auto s = builder.build();
            estimation_test(s, time_window_compaction_strategy::max_data_segregation_window_count);
        }

        std::vector<shared_sstable> sstables_spanning_many_windows = {
            make_sstable(0),
            make_sstable(1),
            make_sstable(2),
            make_sstable(3),
        };

        auto ret = compact_sstables(env, sstables::compaction_descriptor(sstables_spanning_many_windows), cf, sst_gen, replacer_fn_no_op()).get();
        // The real test here is that we don't SCYLLA_ASSERT() in
        // sstables::prepare_summary() with the compact_sstables() call above,
        // this is only here as a sanity check.
        BOOST_REQUIRE_EQUAL(ret.new_sstables.size(), std::min(sstables_spanning_many_windows.size() * rows_per_partition,
                    sstables::time_window_compaction_strategy::max_data_segregation_window_count));
    });
}

static compaction_descriptor get_reshaping_job(sstables::compaction_strategy& cs, const std::vector<shared_sstable>& input,
                                               const schema_ptr& s, reshape_mode mode, uint64_t free_storage_space = std::numeric_limits<uint64_t>::max()) {
    reshape_config cfg {
        .mode = mode,
        .free_storage_space = free_storage_space,
    };
    return cs.get_reshaping_job(input, s, cfg);
}

SEASTAR_TEST_CASE(stcs_reshape_test) {
    return test_env::do_with_async([] (test_env& env) {
        simple_schema ss;
        auto s = ss.schema();
        std::vector<shared_sstable> sstables;
        sstables.reserve(s->max_compaction_threshold());
        const auto keys = tests::generate_partition_keys(s->max_compaction_threshold() + 2, s);
        for (auto gen = 1; gen <= s->max_compaction_threshold(); gen++) {
            auto sst = env.make_sstable(s);
            sstables::test(sst).set_data_file_size(1);
            sstables::test(sst).set_values(keys[gen - 1].key(), keys[gen + 1].key(), stats_metadata{});
            sstables.push_back(std::move(sst));
        }

        auto cs = sstables::make_compaction_strategy(sstables::compaction_strategy_type::size_tiered,
                                                    s->compaction_strategy_options());

        BOOST_REQUIRE(get_reshaping_job(cs, sstables, s, reshape_mode::strict).sstables.size());
        BOOST_REQUIRE(get_reshaping_job(cs, sstables, s, reshape_mode::relaxed).sstables.size());
    });
}

SEASTAR_TEST_CASE(lcs_reshape_test) {
    return test_env::do_with_async([] (test_env& env) {
        simple_schema ss;
        auto s = ss.schema();
        const auto keys = tests::generate_partition_keys(256, s);
        auto cs = sstables::make_compaction_strategy(sstables::compaction_strategy_type::leveled,
                                                     s->compaction_strategy_options());

        // non overlapping
        {
            std::vector <shared_sstable> sstables;
            for (auto i = 0; i < 256; i++) {
                auto sst = env.make_sstable(s);
                auto key = keys[i].key();
                sstables::test(sst).set_values_for_leveled_strategy(1 /* size */, 0 /* level */, 0 /* max ts */, key, key);
                sstables.push_back(std::move(sst));
            }

            BOOST_REQUIRE(get_reshaping_job(cs, sstables, s, reshape_mode::strict).sstables.size() == 256);
        }
        // all overlapping
        {
            std::vector <shared_sstable> sstables;
            for (auto i = 0; i < 256; i++) {
                auto sst = env.make_sstable(s);
                auto key = keys[0].key();
                sstables::test(sst).set_values_for_leveled_strategy(1 /* size */, 0 /* level */, 0 /* max ts */, key, key);
                sstables.push_back(std::move(sst));
            }

            BOOST_REQUIRE(get_reshaping_job(cs, sstables, s, reshape_mode::strict).sstables.size() == uint64_t(s->max_compaction_threshold()));
        }
        // single sstable
        {
            auto sst = env.make_sstable(s);
            auto key = keys[0].key();
            sstables::test(sst).set_values_for_leveled_strategy(1 /* size */, 0 /* level */, 0 /* max ts */, key, key);

            BOOST_REQUIRE(get_reshaping_job(cs, { sst }, s, reshape_mode::strict).sstables.size() == 0);
        }
    });
}

future<> test_twcs_interposer_on_memtable_flush(bool split_during_flush) {
    return test_env::do_with_async([split_during_flush] (test_env& env) {
        auto builder = schema_builder("tests", "test_twcs_interposer_on_flush")
                .with_column("id", utf8_type, column_kind::partition_key)
                .with_column("cl", int32_type, column_kind::clustering_key)
                .with_column("value", int32_type);
        builder.set_compaction_strategy(sstables::compaction_strategy_type::time_window);
        std::map<sstring, sstring> opts = {
            { time_window_compaction_strategy_options::COMPACTION_WINDOW_UNIT_KEY, "HOURS" },
            { time_window_compaction_strategy_options::COMPACTION_WINDOW_SIZE_KEY, "1" },
        };
        builder.set_compaction_strategy_options(std::move(opts));
        auto s = builder.build();

        auto next_timestamp = [] (auto step) {
            using namespace std::chrono;
            return (gc_clock::now().time_since_epoch() - duration_cast<microseconds>(step)).count();
        };
        auto key = tests::generate_partition_key(s);

        auto make_row = [&] (std::chrono::hours step) {
            static thread_local int32_t value = 1;

            mutation m(s, key);
            auto c_key = clustering_key::from_exploded(*s, {int32_type->decompose(value++)});
            m.set_clustered_cell(c_key, bytes("value"), data_value(int32_t(value)), next_timestamp(step));
            return m;
        };

        auto cf = env.make_table_for_tests(s);
        auto close_cf = deferred_stop(cf);
        cf->start();

        size_t target_windows_span = (split_during_flush) ? 10 : 1;
        constexpr size_t rows_per_window = 10;

        for (unsigned i = 1; i <= target_windows_span; i++) {
            for (unsigned j = 0; j < rows_per_window; j++) {
                cf->apply(make_row(std::chrono::hours(i)));
            }
        }
        cf->flush().get();

        auto expected_ssts = (split_during_flush) ? target_windows_span : 1;
        testlog.info("split_during_flush={}, actual={}, expected={}", split_during_flush, cf->get_sstables()->size(), expected_ssts);
        assert_table_sstable_count(cf, expected_ssts);
  });
}

SEASTAR_TEST_CASE(test_twcs_interposer_on_memtable_flush_split) {
    return test_twcs_interposer_on_memtable_flush(true);
}

SEASTAR_TEST_CASE(test_twcs_interposer_on_memtable_flush_no_split) {
    return test_twcs_interposer_on_memtable_flush(false);
}

SEASTAR_TEST_CASE(test_twcs_compaction_across_buckets) {
    return test_env::do_with_async([] (test_env& env) {
        auto builder = schema_builder("tests", "test_twcs_compaction_across_buckets")
                .with_column("id", utf8_type, column_kind::partition_key)
                .with_column("cl", int32_type, column_kind::clustering_key)
                .with_column("value", int32_type);
        builder.set_compaction_strategy(sstables::compaction_strategy_type::time_window);
        std::map<sstring, sstring> opts = {
                { time_window_compaction_strategy_options::COMPACTION_WINDOW_UNIT_KEY, "HOURS" },
                { time_window_compaction_strategy_options::COMPACTION_WINDOW_SIZE_KEY, "1" },
        };
        builder.set_compaction_strategy_options(std::move(opts));
        auto s = builder.build();

        auto next_timestamp = [] (std::chrono::hours step = std::chrono::hours(0)) {
            return (gc_clock::now().time_since_epoch() - std::chrono::duration_cast<std::chrono::microseconds>(step)).count();
        };

        auto sst_gen = env.make_sst_factory(s);
        auto pkey = tests::generate_partition_key(s);

        auto make_row = [&] (std::chrono::hours step) {
            static thread_local int32_t value = 1;
            mutation m(s, pkey);
            auto c_key = clustering_key::from_exploded(*s, {int32_type->decompose(value++)});
            m.set_clustered_cell(c_key, bytes("value"), data_value(int32_t(value)), next_timestamp(step));
            return m;
        };

        auto cf = env.make_table_for_tests(s);
        auto close_cf = deferred_stop(cf);

        constexpr unsigned windows = 10;

        std::vector<shared_sstable> sstables_spanning_many_windows;
        sstables_spanning_many_windows.reserve(windows + 1);

        for (unsigned w = 0; w < windows; w++) {
            sstables_spanning_many_windows.push_back(make_sstable_containing(sst_gen, {make_row(std::chrono::hours((w + 1) * 2))}));
        }
        auto deletion_mut = [&] () {
            mutation m(s, pkey);
            tombstone tomb(next_timestamp(), gc_clock::now());
            m.partition().apply(tomb);
            return m;
        }();
        sstables_spanning_many_windows.push_back(make_sstable_containing(sst_gen, {deletion_mut}));

        auto ret = compact_sstables(env, sstables::compaction_descriptor(std::move(sstables_spanning_many_windows)), cf, sst_gen, replacer_fn_no_op(), can_purge_tombstones::no).get();

        BOOST_REQUIRE(ret.new_sstables.size() == 1);
        assert_that(sstable_reader(ret.new_sstables[0], s, env.make_reader_permit()))
            .produces(deletion_mut)
            .produces_end_of_stream();
    });
}

SEASTAR_TEST_CASE(test_offstrategy_sstable_compaction) {
    return test_env::do_with_async([tmpdirs = std::vector<decltype(tmpdir())>()] (test_env& env) mutable {
        for (const auto version : writable_sstable_versions) {
            tmpdirs.push_back(tmpdir());
            auto& tmp = tmpdirs.back();
            simple_schema ss;
            auto s = ss.schema();

            auto pk = tests::generate_partition_key(s);
            auto mut = mutation(s, pk);
            ss.add_row(mut, ss.make_ckey(0), "val");

            auto cf = env.make_table_for_tests(s, tmp.path().string());
            auto close_cf = deferred_stop(cf);
            auto sst_gen = [&] () mutable {
                return env.make_sstable(cf->schema(), version);
            };

            cf->start();

            for (auto i = 0; i < cf->schema()->max_compaction_threshold(); i++) {
                auto sst = make_sstable_containing(sst_gen, {mut});
                cf->add_sstable_and_update_cache(std::move(sst), sstables::offstrategy::yes).get();
            }
            BOOST_REQUIRE(cf->perform_offstrategy_compaction(tasks::task_info{}).get());
        }
    });
}

SEASTAR_TEST_CASE(twcs_reshape_with_disjoint_set_test) {
    static constexpr unsigned disjoint_sstable_count = 256;

    return test_env::do_with_async([] (test_env& env) {
        auto builder = schema_builder("tests", "twcs_reshape_test")
                .with_column("id", utf8_type, column_kind::partition_key)
                .with_column("cl", ::timestamp_type, column_kind::clustering_key)
                .with_column("value", int32_type);
        builder.set_compaction_strategy(sstables::compaction_strategy_type::time_window);
        std::map <sstring, sstring> opts = {
                {time_window_compaction_strategy_options::COMPACTION_WINDOW_UNIT_KEY, "HOURS"},
                {time_window_compaction_strategy_options::COMPACTION_WINDOW_SIZE_KEY, "8"},
                {"min_sstable_size", "1"},
        };
        builder.set_compaction_strategy_options(std::move(opts));
        size_t min_threshold = tests::random::get_int(4, 8);
        builder.set_min_compaction_threshold(min_threshold);
        auto s = builder.build();
        auto cs = sstables::make_compaction_strategy(sstables::compaction_strategy_type::time_window, s->compaction_strategy_options());

        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> distrib(1, 3600*24);

        using namespace std::chrono;

        // Make it easier to reproduce timing-based issues by running this test multiple times.
        auto offset_duration = duration_cast<microseconds>(minutes(distrib(gen)));

        auto now = gc_clock::now().time_since_epoch() + offset_duration;
        // The twcs is configured with 8-hours time window. If the starting time
        // is not aligned with that then some buckets may get less than this
        // number of sstables in and potentially hit the minimal threshold of
        // 4 sstables. Align the starting time not to make this happen.
        auto now_in_minutes = duration_cast<minutes>(now);
        constexpr auto window_size_in_minutes = 8 * 60;
        forward_jump_clocks(minutes(window_size_in_minutes - now_in_minutes.count() % window_size_in_minutes));
        now = gc_clock::now().time_since_epoch() + offset_duration;
        SCYLLA_ASSERT(std::chrono::duration_cast<minutes>(now).count() % window_size_in_minutes == 0);

        auto next_timestamp = [now](auto step) {
            return (now + duration_cast<seconds>(step)).count();
        };

        const auto keys = tests::generate_partition_keys(disjoint_sstable_count, s);

        auto make_row = [&](unsigned token_idx, auto step) {
            static thread_local int32_t value = 1;
            auto key = keys[token_idx];

            mutation m(s, key);
            auto next_ts = next_timestamp(step);
            auto c_key = clustering_key::from_exploded(*s, {::timestamp_type->decompose(next_ts)});
            m.set_clustered_cell(c_key, bytes("value"), data_value(int32_t(value++)), next_ts);
            return m;
        };

        auto sst_gen = env.make_sst_factory(s);

        {
            // create set of 256 disjoint ssts that belong to the same time window and expect that twcs reshape allows them all to be compacted at once

            std::vector<sstables::shared_sstable> sstables;
            sstables.reserve(disjoint_sstable_count);
            for (unsigned i = 0; i < disjoint_sstable_count; i++) {
                auto sst = make_sstable_containing(sst_gen, {make_row(i, std::chrono::hours(1))});
                sstables.push_back(std::move(sst));
            }

            BOOST_REQUIRE_EQUAL(get_reshaping_job(cs, sstables, s, reshape_mode::strict).sstables.size(), disjoint_sstable_count);
        }

        {
            // create set of 256 disjoint ssts that belong to different windows and expect that twcs reshape allows them all to be compacted at once

            std::vector<sstables::shared_sstable> sstables;
            sstables.reserve(disjoint_sstable_count);
            for (unsigned i = 0; i < disjoint_sstable_count; i++) {
                auto sst = make_sstable_containing(sst_gen, {make_row(i, std::chrono::hours(i))});
                sstables.push_back(std::move(sst));
            }

            auto reshaping_count = get_reshaping_job(cs, sstables, s, reshape_mode::strict).sstables.size();
            BOOST_REQUIRE_GE(reshaping_count, disjoint_sstable_count - min_threshold + 1);
            BOOST_REQUIRE_LE(reshaping_count, disjoint_sstable_count);
        }

        {
            // create set of 256 disjoint ssts that belong to different windows with none over the threshold and expect that twcs reshape selects none of them

            std::vector<sstables::shared_sstable> sstables;
            sstables.reserve(disjoint_sstable_count);
            for (unsigned i = 0; i < disjoint_sstable_count; i++) {
                auto sst = make_sstable_containing(sst_gen, {make_row(i, std::chrono::hours(24*i))});
                sstables.push_back(std::move(sst));
                i++;
                sst = make_sstable_containing(sst_gen, {make_row(i, std::chrono::hours(24*i + 1))});
                sstables.push_back(std::move(sst));
            }

            BOOST_REQUIRE_EQUAL(get_reshaping_job(cs, sstables, s, reshape_mode::strict).sstables.size(), 0);
        }

        {
            // create set of 256 overlapping ssts that belong to the same time window and expect that twcs reshape allows only 32 to be compacted at once

            std::vector<sstables::shared_sstable> sstables;
            sstables.reserve(disjoint_sstable_count);
            for (unsigned i = 0; i < disjoint_sstable_count; i++) {
                auto sst = make_sstable_containing(sst_gen, {make_row(0, std::chrono::hours(1))});
                sstables.push_back(std::move(sst));
            }

            BOOST_REQUIRE_EQUAL(get_reshaping_job(cs, sstables, s, reshape_mode::strict).sstables.size(), uint64_t(s->max_compaction_threshold()));
        }

        {
            // create set of 64 files which size is either small or big. as STCS reshape logic reused by TWCS favor compaction of smaller files
            // first, verify that only 32 small (similar-sized) files are returned

            std::vector<mutation> mutations_for_small_files;
            mutations_for_small_files.push_back(make_row(0, std::chrono::hours(1)));

            std::vector<mutation> mutations_for_big_files;
            for (unsigned i = 0; i < keys.size(); i++) {
                mutations_for_big_files.push_back(make_row(i, std::chrono::hours(1)));
            }

            std::unordered_set<sstables::generation_type> generations_for_small_files;

            std::vector<sstables::shared_sstable> sstables;
            sstables.reserve(64);

            for (unsigned i = 0; i < 64; i++) {
                sstables::shared_sstable sst;
                //
                // intermix big and small files, to make sure STCS logic is really applied to favor similar-sized reshape jobs.
                //
                if (i % 2 == 0) {
                    sst = make_sstable_containing(sst_gen, mutations_for_small_files);
                    generations_for_small_files.insert(sst->generation());
                } else {
                    sst = make_sstable_containing(sst_gen, mutations_for_big_files);
                }
                sstables.push_back(std::move(sst));
            }

            auto check_mode_correctness = [&] (reshape_mode mode) {
                auto ret = get_reshaping_job(cs, sstables, s, mode);
                BOOST_REQUIRE_EQUAL(ret.sstables.size(), uint64_t(s->max_compaction_threshold()));
                // fail if any file doesn't belong to set of small files
                bool has_big_sized_files = std::ranges::any_of(ret.sstables, [&] (const sstables::shared_sstable& sst) {
                    return !generations_for_small_files.contains(sst->generation());
                });
                BOOST_REQUIRE(!has_big_sized_files);
            };

            check_mode_correctness(reshape_mode::strict);
            check_mode_correctness(reshape_mode::relaxed);
        }

        {
            // create set of 256 disjoint ssts that spans multiple windows (essentially what happens in off-strategy during node op)

            std::vector<sstables::shared_sstable> sstables;
            sstables.reserve(disjoint_sstable_count);
            for (auto i = 0U; i < disjoint_sstable_count; i++) {
                std::vector<mutation> muts;
                muts.reserve(5);
                for (auto j = 0; j < 5; j++) {
                    muts.push_back(make_row(i, std::chrono::hours(j * 8)));
                }
                auto sst = make_sstable_containing(sst_gen, std::move(muts));
                sstables.push_back(std::move(sst));
            }

            auto job_size = [] (auto&& sst_range) {
                return std::ranges::fold_left(sst_range | std::views::transform(std::mem_fn(&sstable::bytes_on_disk)), uint64_t(0), std::plus{});
            };
            auto free_space_for_reshaping_sstables = [&job_size] (auto&& sst_range) {
                return job_size(std::move(sst_range)) * (time_window_compaction_strategy::reshape_target_space_overhead * 100);
            };

            // all sstables can be reshaped in a single round if there's enough space
            {
                uint64_t free_space = free_space_for_reshaping_sstables(std::ranges::subrange(sstables));
                BOOST_REQUIRE(get_reshaping_job(cs, sstables, s, reshape_mode::strict, free_space).sstables.size() == sstables.size());
            }

            // only a subset can be reshaped in a single round to respect the 10% space overhead
            {
                const size_t sstables_that_fit_in_target_overhead = 10;
                uint64_t free_space = free_space_for_reshaping_sstables(std::ranges::subrange(sstables.begin(), sstables.begin() + sstables_that_fit_in_target_overhead));
                auto target_space_overhead = free_space * time_window_compaction_strategy::reshape_target_space_overhead;
                auto job = get_reshaping_job(cs, sstables, s, reshape_mode::strict, free_space);
                BOOST_REQUIRE(job.sstables.size() < sstables.size());
                BOOST_REQUIRE(job_size(std::ranges::subrange(job.sstables)) <= target_space_overhead);
            }
        }
    });
}


SEASTAR_TEST_CASE(stcs_reshape_overlapping_test) {
    static constexpr unsigned disjoint_sstable_count = 256;

    return test_env::do_with_async([] (test_env& env) {
        auto builder = schema_builder("tests", "stcs_reshape_test")
                .with_column("id", utf8_type, column_kind::partition_key)
                .with_column("cl", ::timestamp_type, column_kind::clustering_key)
                .with_column("value", int32_type);
        builder.set_compaction_strategy(sstables::compaction_strategy_type::size_tiered);
        auto s = builder.build();
        std::map<sstring, sstring> opts;
        auto cs = sstables::make_compaction_strategy(sstables::compaction_strategy_type::size_tiered, std::move(opts));

        auto keys = tests::generate_partition_keys(disjoint_sstable_count, s);

        auto make_row = [&](unsigned token_idx) {
            auto key = keys[token_idx];

            mutation m(s, key);
            auto value = 1;
            auto next_ts = 1;
            auto c_key = clustering_key::from_exploded(*s, {::timestamp_type->decompose(next_ts)});
            m.set_clustered_cell(c_key, bytes("value"), data_value(int32_t(value)), next_ts);
            return m;
        };

        auto sst_gen = env.make_sst_factory(s);

        {
            // create set of 256 disjoint ssts and expect that stcs reshape allows them all to be compacted at once

            std::vector<sstables::shared_sstable> sstables;
            sstables.reserve(disjoint_sstable_count);
            for (unsigned i = 0; i < disjoint_sstable_count; i++) {
                auto sst = make_sstable_containing(sst_gen, {make_row(i)});
                sstables.push_back(std::move(sst));
            }

            BOOST_REQUIRE(get_reshaping_job(cs, sstables, s, reshape_mode::strict).sstables.size() == disjoint_sstable_count);
        }

        {
            // create set of 256 overlapping ssts and expect that stcs reshape allows only 32 to be compacted at once

            std::vector<sstables::shared_sstable> sstables;
            sstables.reserve(disjoint_sstable_count);
            for (unsigned i = 0; i < disjoint_sstable_count; i++) {
                auto sst = make_sstable_containing(sst_gen, {make_row(0)});
                sstables.push_back(std::move(sst));
            }

            BOOST_REQUIRE(get_reshaping_job(cs, sstables, s, reshape_mode::strict).sstables.size() == uint64_t(s->max_compaction_threshold()));
        }
    });
}

// Regression test for #8432
SEASTAR_TEST_CASE(test_twcs_single_key_reader_filtering) {
    return test_env::do_with_async([] (test_env& env) {
        auto builder = schema_builder("tests", "twcs_single_key_reader_filtering")
                .with_column("pk", int32_type, column_kind::partition_key)
                .with_column("ck", int32_type, column_kind::clustering_key)
                .with_column("v", int32_type);
        builder.set_compaction_strategy(sstables::compaction_strategy_type::time_window);
        auto s = builder.build();

        auto sst_gen = env.make_sst_factory(s);

        auto make_row = [&] (int32_t pk, int32_t ck) {
            mutation m(s, partition_key::from_single_value(*s, int32_type->decompose(pk)));
            m.set_clustered_cell(clustering_key::from_single_value(*s, int32_type->decompose(ck)), to_bytes("v"), int32_t(0), api::new_timestamp());
            return m;
        };

        auto sst1 = make_sstable_containing(sst_gen, {make_row(0, 0)});
        auto sst2 = make_sstable_containing(sst_gen, {make_row(0, 1)});
        auto dkey = sst1->get_first_decorated_key();

        auto cf = env.make_table_for_tests(s);
        auto close_cf = deferred_stop(cf);
        cf->start();

        auto cs = sstables::make_compaction_strategy(sstables::compaction_strategy_type::time_window, {});

        auto set = cs.make_sstable_set(s);
        set.insert(std::move(sst1));
        set.insert(std::move(sst2));

        reader_permit permit = env.make_reader_permit();
        utils::estimated_histogram eh;
        auto pr = dht::partition_range::make_singular(dkey);

        auto slice = partition_slice_builder(*s)
                    .with_range(query::clustering_range {
                        query::clustering_range::bound { clustering_key_prefix::from_single_value(*s, int32_type->decompose(0)) },
                        query::clustering_range::bound { clustering_key_prefix::from_single_value(*s, int32_type->decompose(1)) },
                    }).build();

        auto reader = set.create_single_key_sstable_reader(
                &*cf, s, permit, eh, pr, slice,
                tracing::trace_state_ptr(), ::streamed_mutation::forwarding::no,
                ::mutation_reader::forwarding::no);
        auto close_reader = deferred_close(reader);

        auto& cf_stats = cf.cf_stats();
        auto checked_by_ck = cf_stats.sstables_checked_by_clustering_filter;
        auto surviving_after_ck = cf_stats.surviving_sstables_after_clustering_filter;

        // consume all fragments
        while (reader().get());

        // At least sst2 should be checked by the CK filter during fragment consumption and should pass.
        // With the bug in #8432, sst2 wouldn't even be checked by the CK filter since it would pass right after checking the PK filter.
        BOOST_REQUIRE_GE(cf_stats.sstables_checked_by_clustering_filter - checked_by_ck, 1);
        BOOST_REQUIRE_EQUAL(
                cf_stats.surviving_sstables_after_clustering_filter - surviving_after_ck,
                cf_stats.sstables_checked_by_clustering_filter - checked_by_ck);
    });
}

SEASTAR_TEST_CASE(max_ongoing_compaction_test) {
    return test_env::do_with_async([] (test_env& env) {
        BOOST_REQUIRE(smp::count == 1);

        auto make_schema = [] (auto idx) {
            auto builder = schema_builder("tests", std::to_string(idx))
                .with_column("id", utf8_type, column_kind::partition_key)
                .with_column("cl", int32_type, column_kind::clustering_key)
                .with_column("value", int32_type);
            builder.set_compaction_strategy(sstables::compaction_strategy_type::time_window);
            std::map <sstring, sstring> opts = {
                {time_window_compaction_strategy_options::COMPACTION_WINDOW_UNIT_KEY,                  "HOURS"},
                {time_window_compaction_strategy_options::COMPACTION_WINDOW_SIZE_KEY,                  "1"},
                {time_window_compaction_strategy_options::EXPIRED_SSTABLE_CHECK_FREQUENCY_SECONDS_KEY, "0"},
            };
            builder.set_compaction_strategy_options(std::move(opts));
            builder.set_gc_grace_seconds(0);
            return builder.build();
        };

        // makes sure all data belonging to a table falls into the same time bucket.
        auto now = gc_clock::now();
        auto next_timestamp = [&now] (auto step) {
            using namespace std::chrono;
            return (now.time_since_epoch() - duration_cast<microseconds>(step)).count();
        };
        auto make_expiring_cell = [&] (schema_ptr s, std::chrono::hours step) {
            static thread_local int32_t value = 1;

            auto key = tests::generate_partition_key(s);

            mutation m(s, key);
            auto c_key = clustering_key::from_exploded(*s, {int32_type->decompose(value++)});
            m.set_clustered_cell(c_key, bytes("value"), data_value(int32_t(value)), next_timestamp(step), gc_clock::duration(step + 5s));
            return m;
        };

        constexpr size_t num_tables = 10;
        std::vector<schema_ptr> schemas;
        std::vector<table_for_tests> tables;
        auto stop_tables = defer([&tables] {
            for (auto& t : tables) {
                t->stop().get();
            }
        });

        // Make tables
        for (unsigned idx = 0; idx < num_tables; idx++) {
            auto s = make_schema(idx);
            schemas.push_back(s);
            auto cf = env.make_table_for_tests(s);
            tables.push_back(cf);
        }

        auto sst_gen = [&] (size_t idx) mutable {
            auto t = tables[idx];
            return t->make_sstable();
        };

        auto add_single_fully_expired_sstable_to_table = [&] (auto idx) {
            auto s = schemas[idx];
            auto cf = tables[idx];
            auto muts = { make_expiring_cell(s, std::chrono::hours(1)) };
            auto sst = make_sstable_containing([&sst_gen, idx] { return sst_gen(idx); }, muts);
            column_family_test(cf).add_sstable(sst).get();
        };

        for (unsigned i = 0; i < num_tables; i++) {
            add_single_fully_expired_sstable_to_table(i);
        }

        // Make sure everything is expired
        forward_jump_clocks(std::chrono::hours(100));
        now = gc_clock::now();

        auto compact_all_tables = [&] (size_t expected_before, size_t expected_after) {
            for (auto& t : tables) {
                BOOST_REQUIRE_EQUAL(t->sstables_count(), expected_before);
                t->trigger_compaction();
            }

            size_t max_ongoing_compaction = 0;
            auto& cm = env.test_compaction_manager().get_compaction_manager();

            // wait for submitted jobs to finish.
            auto end = [&cm, &tables, expected_after] {
                return cm.get_stats().pending_tasks == 0 && cm.get_stats().active_tasks == 0
                    && std::ranges::all_of(tables, [expected_after] (auto& t) { return t->sstables_count() == expected_after; });
            };
            while (!end()) {
                if (!cm.get_stats().pending_tasks && !cm.get_stats().active_tasks) {
                    for (auto& t : tables) {
                        if (t->sstables_count()) {
                            t->trigger_compaction();
                        }
                    }
                }
                max_ongoing_compaction = std::max(cm.get_stats().active_tasks, max_ongoing_compaction);
                yield().get();
            }
            BOOST_REQUIRE(cm.get_stats().errors == 0);
            return max_ongoing_compaction;
        };

        // Allow fully expired sstables to be compacted in parallel, as they have the same weight 0 (== weightless).
        BOOST_REQUIRE_LE(compact_all_tables(1, 0), num_tables);

        auto add_sstables_to_table = [&] (auto idx, size_t num_sstables) {
            auto s = schemas[idx];
            auto cf = tables[idx];
            auto cft = column_family_test(cf);
            for (size_t i = 0; i < num_sstables; i++) {
                auto muts = { make_expiring_cell(s, std::chrono::hours(1)) };
                cft.add_sstable(make_sstable_containing([&sst_gen, idx] { return sst_gen(idx); }, muts)).get();
            }
        };

        for (size_t i = 0; i < num_tables; i++) {
            add_sstables_to_table(i, DEFAULT_MIN_COMPACTION_THRESHOLD);
        }

        // All buckets are expected to have the same weight (>0)
        // and therefore their compaction is expected to be serialized
        BOOST_REQUIRE_EQUAL(compact_all_tables(DEFAULT_MIN_COMPACTION_THRESHOLD, 1), 1);
    });
}

SEASTAR_TEST_CASE(compound_sstable_set_incremental_selector_test) {
    return test_env::do_with_async([] (test_env& env) {
        auto s = schema_builder(some_keyspace, some_column_family).with_column("p1", utf8_type, column_kind::partition_key).build();
        auto cs = sstables::make_compaction_strategy(sstables::compaction_strategy_type::leveled, s->compaction_strategy_options());
        const auto keys = tests::generate_partition_keys(8, s);

        auto new_sstable = [&] (lw_shared_ptr<sstable_set> set, size_t k0, size_t k1, uint32_t level) {
            auto key0 = keys[k0];
            auto tok0 = key0.token();
            auto key1 = keys[k1];
            auto tok1 = key1.token();
            testlog.debug("creating sstable with k[{}] token={} k[{}] token={} level={}", k0, tok0, k1, tok1, level);
            auto sst = sstable_for_overlapping_test(env, s, key0.key(), key1.key(), level);
            set->insert(sst);
            return sst;
        };

        auto check = [&] (sstable_set::incremental_selector& selector, size_t k, std::unordered_set<shared_sstable> expected_ssts) {
            const dht::decorated_key& key = keys[k];
            auto sstables = selector.select(key).sstables;
            testlog.debug("checking sstables for key[{}] token={} found={} expected={}", k, keys[k].token(), sstables.size(), expected_ssts.size());
            BOOST_REQUIRE_EQUAL(sstables.size(), expected_ssts.size());
            for (auto& sst : sstables) {
                BOOST_REQUIRE(expected_ssts.contains(sst));
                expected_ssts.erase(sst);
            }
            BOOST_REQUIRE(expected_ssts.empty());
        };

        {
            auto set1 = make_lw_shared<sstable_set>(cs.make_sstable_set(s));
            auto set2 = make_lw_shared<sstable_set>(cs.make_sstable_set(s));
            std::vector<shared_sstable> ssts;
            ssts.push_back(new_sstable(set1, 0, 1, 1));
            ssts.push_back(new_sstable(set2, 0, 1, 1));
            ssts.push_back(new_sstable(set1, 3, 4, 1));
            ssts.push_back(new_sstable(set2, 4, 4, 1));
            ssts.push_back(new_sstable(set1, 4, 5, 1));

            sstable_set compound = sstables::make_compound_sstable_set(s, { set1, set2 });
            sstable_set::incremental_selector sel = compound.make_incremental_selector();
            check(sel, 0, std::unordered_set<shared_sstable>{ssts[0], ssts[1]});
            check(sel, 1, std::unordered_set<shared_sstable>{ssts[0], ssts[1]});
            check(sel, 2, std::unordered_set<shared_sstable>{});
            check(sel, 3, std::unordered_set<shared_sstable>{ssts[2]});
            check(sel, 4, std::unordered_set<shared_sstable>{ssts[2], ssts[3], ssts[4]});
            check(sel, 5, std::unordered_set<shared_sstable>{ssts[4]});
            check(sel, 6, std::unordered_set<shared_sstable>{});
            check(sel, 7, std::unordered_set<shared_sstable>{});
        }

        {
            auto set1 = make_lw_shared<sstable_set>(cs.make_sstable_set(s));
            auto set2 = make_lw_shared<sstable_set>(cs.make_sstable_set(s));
            std::vector<shared_sstable> ssts;
            ssts.push_back(new_sstable(set1, 0, 1, 0));
            ssts.push_back(new_sstable(set2, 0, 1, 1));
            ssts.push_back(new_sstable(set1, 0, 1, 1));
            ssts.push_back(new_sstable(set2, 3, 4, 1));
            ssts.push_back(new_sstable(set1, 4, 4, 1));
            ssts.push_back(new_sstable(set2, 4, 5, 1));

            sstable_set compound = sstables::make_compound_sstable_set(s, { set1, set2 });
            sstable_set::incremental_selector sel = compound.make_incremental_selector();
            check(sel, 0, std::unordered_set<shared_sstable>{ssts[0], ssts[1], ssts[2]});
            check(sel, 1, std::unordered_set<shared_sstable>{ssts[0], ssts[1], ssts[2]});
            check(sel, 2, std::unordered_set<shared_sstable>{ssts[0]});
            check(sel, 3, std::unordered_set<shared_sstable>{ssts[0], ssts[3]});
            check(sel, 4, std::unordered_set<shared_sstable>{ssts[0], ssts[3], ssts[4], ssts[5]});
            check(sel, 5, std::unordered_set<shared_sstable>{ssts[0], ssts[5]});
            check(sel, 6, std::unordered_set<shared_sstable>{ssts[0]});
            check(sel, 7, std::unordered_set<shared_sstable>{ssts[0]});
        }

        {
            // reproduces use-after-free failure in incremental reader selector with compound set where the next position
            // returned by a set can be used after freed as selector position in another set, producing incorrect results.

            enum class strategy_param : bool {
                ICS = false,
                LCS = true,
            };

            auto incremental_selection_test = [&] (strategy_param param) {
                auto set1 = make_lw_shared<sstable_set>(sstables::make_partitioned_sstable_set(s, false));
                auto set2 = make_lw_shared<sstable_set>(sstables::make_partitioned_sstable_set(s, bool(param)));
                new_sstable(set1, 1, 1, 1);
                new_sstable(set2, 0, 2, 1);
                new_sstable(set2, 3, 3, 1);
                new_sstable(set2, 4, 4, 1);

                sstable_set compound = sstables::make_compound_sstable_set(s, { set1, set2 });
                sstable_set::incremental_selector sel = compound.make_incremental_selector();

                dht::ring_position_view pos = dht::ring_position_view::min();
                std::unordered_set<sstables::shared_sstable> sstables;
                do {
                    auto ret = sel.select(pos);
                    pos = ret.next_position;
                    sstables.insert(ret.sstables.begin(), ret.sstables.end());
                } while (!pos.is_max());

                BOOST_REQUIRE(sstables.size() == 4);
            };

            incremental_selection_test(strategy_param::ICS);
            incremental_selection_test(strategy_param::LCS);
        }
    });
}

SEASTAR_TEST_CASE(twcs_single_key_reader_through_compound_set_test) {
    return test_env::do_with_async([] (test_env& env) {
        auto builder = schema_builder("tests", "single_key_reader_through_compound_set_test")
                .with_column("id", utf8_type, column_kind::partition_key)
                .with_column("cl", ::timestamp_type, column_kind::clustering_key)
                .with_column("value", int32_type);
        builder.set_compaction_strategy(sstables::compaction_strategy_type::time_window);
        std::map <sstring, sstring> opts = {
                {time_window_compaction_strategy_options::COMPACTION_WINDOW_UNIT_KEY, "HOURS"},
                {time_window_compaction_strategy_options::COMPACTION_WINDOW_SIZE_KEY, "1"},
        };
        builder.set_compaction_strategy_options(std::move(opts));
        auto s = builder.build();
        auto cs = sstables::make_compaction_strategy(sstables::compaction_strategy_type::time_window, std::move(opts));

        auto next_timestamp = [](auto step) {
            using namespace std::chrono;
            return (gc_clock::now().time_since_epoch() + duration_cast<microseconds>(step)).count();
        };
        auto key = tests::generate_partition_key(s);

        auto make_row = [&](std::chrono::hours step) {
            static thread_local int32_t value = 1;

            mutation m(s, key);
            auto next_ts = next_timestamp(step);
            auto c_key = clustering_key::from_exploded(*s, {::timestamp_type->decompose(next_ts)});
            m.set_clustered_cell(c_key, bytes("value"), data_value(int32_t(value++)), next_ts);
            return m;
        };

        auto cf = env.make_table_for_tests(s);
        auto close_cf = deferred_stop(cf);
        cf->start();

        auto set1 = make_lw_shared<sstable_set>(cs.make_sstable_set(s));
        auto set2 = make_lw_shared<sstable_set>(cs.make_sstable_set(s));

        auto sst_gen = env.make_sst_factory(s);

        // sstables with same key but belonging to different windows
        auto sst1 = make_sstable_containing(sst_gen, {make_row(std::chrono::hours(1))});
        auto sst2 = make_sstable_containing(sst_gen, {make_row(std::chrono::hours(5))});
        BOOST_REQUIRE(sst1->get_first_decorated_key().token() == sst2->get_last_decorated_key().token());
        auto dkey = sst1->get_first_decorated_key();

        set1->insert(std::move(sst1));
        set2->insert(std::move(sst2));
        sstable_set compound = sstables::make_compound_sstable_set(s, {set1, set2});

        reader_permit permit = env.make_reader_permit();
        utils::estimated_histogram eh;
        auto pr = dht::partition_range::make_singular(dkey);

        auto reader = compound.create_single_key_sstable_reader(&*cf, s, permit, eh, pr, s->full_slice(),
                                                                tracing::trace_state_ptr(), ::streamed_mutation::forwarding::no,
                                                                ::mutation_reader::forwarding::no);
        auto close_reader = deferred_close(reader);
        auto mfopt = read_mutation_from_mutation_reader(reader).get();
        BOOST_REQUIRE(mfopt);
        mfopt = read_mutation_from_mutation_reader(reader).get();
        BOOST_REQUIRE(!mfopt);
        BOOST_REQUIRE(cf.cf_stats().clustering_filter_count > 0);
    });
}

SEASTAR_TEST_CASE(basic_ics_controller_correctness_test) {
    return test_env::do_with_async([] (test_env& env) {
        static constexpr uint64_t default_fragment_size = 1UL*1024UL*1024UL*1024UL;

        auto s = simple_schema().schema();

        auto backlog = [&] (compaction_backlog_tracker backlog_tracker, uint64_t max_fragment_size) {
            table_for_tests cf = env.make_table_for_tests();
            auto stop_cf = defer([&] { cf.stop().get(); });

            uint64_t current_sstable_size = default_fragment_size;
            uint64_t data_set_size = 0;
            static constexpr uint64_t target_data_set_size = 1000UL*1024UL*1024UL*1024UL;

            while (data_set_size < target_data_set_size) {
                auto run_identifier = sstables::run_id::create_random_id();

                auto expected_fragments = std::max(1UL, current_sstable_size / max_fragment_size);
                uint64_t fragment_size = std::max(default_fragment_size, current_sstable_size / expected_fragments);
                auto tokens = tests::generate_partition_keys(expected_fragments, s, local_shard_only::yes);

                for (auto i = 0UL; i < expected_fragments; i++) {
                    auto sst = sstable_for_overlapping_test(env, cf->schema(), tokens[i].key(), tokens[i].key());
                    sstables::test(sst).set_data_file_size(fragment_size);
                    sstables::test(sst).set_run_identifier(run_identifier);
                    backlog_tracker.replace_sstables({}, {std::move(sst)});
                }
                data_set_size += current_sstable_size;
                current_sstable_size *= 2;
            }

            return backlog_tracker.backlog();
        };

        sstables::incremental_compaction_strategy_options ics_options;
        auto ics_backlog = backlog(compaction_backlog_tracker(std::make_unique<incremental_backlog_tracker>(ics_options)), default_fragment_size);
        sstables::size_tiered_compaction_strategy_options stcs_options;
        auto stcs_backlog = backlog(compaction_backlog_tracker(std::make_unique<size_tiered_backlog_tracker>(stcs_options)), std::numeric_limits<size_t>::max());

        // don't expect ics and stcs to yield different backlogs for the same workload.
        BOOST_CHECK_CLOSE(ics_backlog, stcs_backlog, 0.0001);
    });
}

SEASTAR_TEST_CASE(test_major_does_not_miss_data_in_memtable) {
    return test_env::do_with_async([] (test_env& env) {
        auto builder = schema_builder("tests", "test_major_does_not_miss_data_in_memtable")
                .with_column("id", utf8_type, column_kind::partition_key)
                .with_column("cl", int32_type, column_kind::clustering_key)
                .with_column("value", int32_type);
        auto s = builder.build();

        auto pkey = tests::generate_partition_key(s);

        auto cf = env.make_table_for_tests(s);
        auto close_cf = deferred_stop(cf);
        auto sst_gen = [&] () mutable {
            return env.make_sstable(cf->schema());
        };

        auto row_mut = [&] () {
            static thread_local int32_t value = 1;
            mutation m(s, pkey);
            auto c_key = clustering_key::from_exploded(*s, {int32_type->decompose(value++)});
            m.set_clustered_cell(c_key, bytes("value"), data_value(int32_t(value)), gc_clock::now().time_since_epoch().count());
            return m;
        }();
        auto sst = make_sstable_containing(sst_gen, {std::move(row_mut)});
        cf->add_sstable_and_update_cache(sst).get();
        assert_table_sstable_count(cf, 1);

        auto deletion_mut = [&] () {
            mutation m(s, pkey);
            tombstone tomb(gc_clock::now().time_since_epoch().count(), gc_clock::now());
            m.partition().apply(tomb);
            return m;
        }();
        cf->apply(deletion_mut);

        cf->compact_all_sstables(tasks::task_info{}).get();
        assert_table_sstable_count(cf, 1);
        auto new_sst = *(cf->get_sstables()->begin());
        BOOST_REQUIRE(new_sst->generation() != sst->generation());
        assert_that(sstable_reader(new_sst, s, env.make_reader_permit()))
                .produces(deletion_mut)
                .produces_end_of_stream();
    });
}

future<> run_controller_test(sstables::compaction_strategy_type compaction_strategy_type) {
    return test_env::do_with_async([compaction_strategy_type] (test_env& env) {
        /////////////
        // settings
        static constexpr float disk_memory_ratio = 78.125; /* AWS I3en is ~78.125 */
        static constexpr uint64_t available_memory_per_shard = 8'000'000'000; /* AWS I3en */
        static constexpr float target_disk_usage = 0.50;

        const uint64_t available_disk_size_per_shard = disk_memory_ratio * available_memory_per_shard;
        const uint64_t available_memory = available_memory_per_shard * 0.92; /* 8% is reserved for the OS */
        const uint64_t estimated_flush_size = double(available_memory) * 0.05; /* flush threshold is 5% of available shard mem */
        const uint64_t all_tables_disk_usage = double(available_disk_size_per_shard) * target_disk_usage;

        auto as = abort_source();

        auto task_manager = tasks::task_manager({}, as);
        auto stop_task_manager = deferred_stop(task_manager);
        compaction_manager::config cfg = {
            .compaction_sched_group = { default_scheduling_group() },
            .maintenance_sched_group = { default_scheduling_group() },
            .available_memory = available_memory,
        };
        auto manager = compaction_manager(std::move(cfg), as, task_manager);
        auto stop_manager = deferred_stop(manager);

        auto add_sstable = [&env] (table_for_tests& t, uint64_t data_size, int level) {
            auto sst = env.make_sstable(t.schema());
            auto key = tests::generate_partition_key(t.schema()).key();
            sstables::test(sst).set_values_for_leveled_strategy(data_size, level, 0 /*max ts*/, key, key);
            SCYLLA_ASSERT(sst->data_size() == data_size);
            auto backlog_before = t.as_table_state().get_backlog_tracker().backlog();
            t->add_sstable_and_update_cache(sst).get();
            testlog.debug("\tNew sstable of size={} level={}; Backlog diff={};",
                          utils::pretty_printed_data_size(data_size), level,
                          t.as_table_state().get_backlog_tracker().backlog() - backlog_before);
        };

        auto create_table = [&] () {
            auto cf = utils::UUID_gen::get_time_UUID();
            simple_schema ss{"ks", fmt::to_string(cf)};
            auto s = ss.schema();

            auto t = env.make_table_for_tests(s);
            t->start();
            t->set_compaction_strategy(compaction_strategy_type);
            return t;
        };

        const int fan_out = compaction_strategy_type == sstables::compaction_strategy_type::leveled ? leveled_manifest::leveled_fan_out : 4;

        auto get_size_for_tier = [&] (int tier) -> uint64_t {
            return std::pow(fan_out, tier) * estimated_flush_size;
        };
        auto get_total_tiers = [&] (uint64_t target_size) -> unsigned {
            double inv_log_n = 1.0f / std::log(fan_out);
            return std::ceil(std::log(double(target_size) / estimated_flush_size) * inv_log_n);
        };
        auto normalize_backlog = [&] (double backlog) -> double {
            return backlog / available_memory;
        };

        struct result {
            unsigned table_count;
            uint64_t per_table_max_disk_usage;
            double normalized_backlog;
        };
        std::vector<result> results;

        std::vector<unsigned> target_table_count_s = { 1, 2, 5, 10, 20 };
        for (auto target_table_count : target_table_count_s) {
            const uint64_t per_table_max_disk_usage = std::ceil(all_tables_disk_usage / target_table_count);

            testlog.info("Creating tables, with max size={}", utils::pretty_printed_data_size(per_table_max_disk_usage));

            std::vector<table_for_tests> tables;
            uint64_t tables_total_size = 0;

            for (uint64_t t_idx = 0, available_space = all_tables_disk_usage; available_space >= estimated_flush_size; t_idx++) {
                auto target_disk_usage = std::min(available_space, per_table_max_disk_usage);
                auto tiers = get_total_tiers(target_disk_usage);

                auto t = create_table();
                for (size_t tier_idx = 0; tier_idx < tiers; tier_idx++) {
                    auto tier_size = get_size_for_tier(tier_idx);
                    if (tier_size > available_space) {
                        break;
                    }
                    int level = compaction_strategy_type == sstables::compaction_strategy_type::leveled ? tier_idx : 0;
                    add_sstable(t, tier_size, level);
                    available_space -= std::min(available_space, uint64_t(tier_size));
                }

                auto table_size = t->get_stats().live_disk_space_used;
                testlog.debug("T{}: {} tiers, with total size={}", t_idx, tiers, utils::pretty_printed_data_size(table_size));
                tables.push_back(t);
                tables_total_size += table_size;
            }
            testlog.debug("Created {} tables, with total size={}", tables.size(), utils::pretty_printed_data_size(tables_total_size));
            results.push_back(result{ tables.size(), per_table_max_disk_usage, normalize_backlog(manager.backlog()) });
            for (auto& t : tables) {
                t.stop().get();
            }
        }
        for (auto& r : results) {
            testlog.info("Tables={} with max size={} -> NormalizedBacklog={}", r.table_count, utils::pretty_printed_data_size(r.per_table_max_disk_usage), r.normalized_backlog);
            // Expect 0 backlog as tiers are all perfectly compacted
            // With LCS, the size of levels *set up by the test* can slightly exceed their target size,
            // so let's account for the microscopical amount of backlog returned.
            auto max_expected = compaction_strategy_type == sstables::compaction_strategy_type::leveled ? 0.4f : 0.0f;
            BOOST_REQUIRE(r.normalized_backlog <= max_expected);
        }
    });
}

SEASTAR_TEST_CASE(simple_backlog_controller_test_size_tiered) {
    return run_controller_test(sstables::compaction_strategy_type::size_tiered);
}

SEASTAR_TEST_CASE(simple_backlog_controller_test_time_window) {
    return run_controller_test(sstables::compaction_strategy_type::time_window);
}

SEASTAR_TEST_CASE(simple_backlog_controller_test_leveled) {
    return run_controller_test(sstables::compaction_strategy_type::leveled);
}

SEASTAR_TEST_CASE(simple_backlog_controller_test_incremental) {
    return run_controller_test(sstables::compaction_strategy_type::incremental);
}

SEASTAR_TEST_CASE(test_compaction_strategy_cleanup_method) {
    return test_env::do_with_async([] (test_env& env) {
        constexpr size_t all_files = 64;

        auto get_cleanup_jobs = [&env] (sstables::compaction_strategy_type compaction_strategy_type,
                                                    std::map<sstring, sstring> strategy_options = {},
                                                    const api::timestamp_clock::duration step_base = 0ms,
                                                    unsigned sstable_level = 0) {
            auto builder = schema_builder("tests", "test_compaction_strategy_cleanup_method")
                    .with_column("id", utf8_type, column_kind::partition_key)
                    .with_column("cl", int32_type, column_kind::clustering_key)
                    .with_column("value", int32_type);
            builder.set_compaction_strategy(compaction_strategy_type);
            builder.set_compaction_strategy_options(std::move(strategy_options));
            auto s = builder.build();

            auto _ = env.tempdir().make_sweeper();
            auto keys = tests::generate_partition_keys(all_files, s);

            auto cf = env.make_table_for_tests(s);
            auto close_cf = deferred_stop(cf);
            auto sst_gen = env.make_sst_factory(s);

            using namespace std::chrono;
            auto now = gc_clock::now().time_since_epoch() + duration_cast<microseconds>(seconds(tests::random::get_int(0, 3600*24)));
            auto next_timestamp = [&now] (microseconds step) mutable -> api::timestamp_type {
                return (now + step).count();
            };
            auto make_mutation = [&] (unsigned pkey_idx, api::timestamp_type ts) {
                mutation m(s, keys[pkey_idx]);
                auto c_key = clustering_key::from_exploded(*s, {int32_type->decompose(1)});
                m.set_clustered_cell(c_key, bytes("value"), data_value(int32_t(1)), ts);
                return m;
            };

            std::vector<sstables::shared_sstable> candidates;
            candidates.reserve(all_files);
            for (size_t i = 0; i < all_files; i++) {
                auto current_step = duration_cast<microseconds>(step_base) * i;
                auto sst = make_sstable_containing(sst_gen, {make_mutation(i, next_timestamp(current_step))});
                sst->set_sstable_level(sstable_level);
                candidates.push_back(std::move(sst));
            }

            auto strategy = cf->get_compaction_strategy();
            auto jobs = strategy.get_cleanup_compaction_jobs(cf.as_table_state(), candidates);
            return std::make_pair(std::move(candidates), std::move(jobs));
        };

        auto run_cleanup_strategy_test = [&] (sstables::compaction_strategy_type compaction_strategy_type, size_t per_job_files, auto&&... args) {
            testlog.info("Running cleanup test for strategy type {}", compaction_strategy::name(compaction_strategy_type));
            size_t target_job_count = all_files / per_job_files;
            auto [candidates, descriptors] = get_cleanup_jobs(compaction_strategy_type, std::forward<decltype(args)>(args)...);
            testlog.info("get_cleanup_jobs() returned {} descriptors; expected={}", descriptors.size(), target_job_count);
            BOOST_REQUIRE(descriptors.size() == target_job_count);
            auto generations = candidates | std::views::transform(std::mem_fn(&sstables::sstable::generation)) | std::ranges::to<std::unordered_set<generation_type>>();
            auto check_desc = [&] (const auto& desc) {
                BOOST_REQUIRE(desc.sstables.size() == per_job_files);
                for (auto& sst: desc.sstables) {
                    BOOST_REQUIRE(generations.erase(sst->generation()));
                }
            };
            for (auto& desc : descriptors) {
                check_desc(desc);
            }
        };

        // STCS: Check that 2 jobs are returned for a size tier containing 2x more files than max threshold.
        run_cleanup_strategy_test(sstables::compaction_strategy_type::size_tiered, 32);

        // Default implementation: check that it will return one job for each file
        run_cleanup_strategy_test(sstables::compaction_strategy_type::null, 1);

        // TWCS: Check that it will return one job for each time window
        std::map<sstring, sstring> twcs_opts = {
            {time_window_compaction_strategy_options::COMPACTION_WINDOW_UNIT_KEY, "HOURS"},
            {time_window_compaction_strategy_options::COMPACTION_WINDOW_SIZE_KEY, "1"},
        };
        run_cleanup_strategy_test(sstables::compaction_strategy_type::time_window, 1, std::move(twcs_opts), 1h);

        const std::map<sstring, sstring> empty_opts;
        // LCS: Check that 2 jobs are returned for all similar-sized files in level 0.
        run_cleanup_strategy_test(sstables::compaction_strategy_type::leveled, 32, empty_opts, 0ms, 0);
        // LCS: Check that 1 jobs is returned for all non-overlapping files in level 1, as incremental compaction can be employed
        // to limit memory usage and space requirement.
        run_cleanup_strategy_test(sstables::compaction_strategy_type::leveled, 64, empty_opts, 0ms, 1);

        // ICS: Check that 2 jobs are returned for a size tier containing 2x more files (single-fragment runs) than max threshold.
        run_cleanup_strategy_test(sstables::compaction_strategy_type::incremental, 32);
    });
}

SEASTAR_TEST_CASE(test_large_partition_splitting_on_compaction) {
    return test_env::do_with_async([] (test_env& env) {
        auto builder = schema_builder("tests", "test_large_partition_splitting_on_compaction")
                .with_column("id", utf8_type, column_kind::partition_key)
                .with_column("cl", int32_type, column_kind::clustering_key)
                .with_column("value", int32_type);
        builder.set_compressor_params(compression_parameters::no_compression());
        builder.set_gc_grace_seconds(0); // Don't purge any tombstone
        auto s = builder.build();

        using namespace std::chrono;
        auto next_timestamp = [] (std::chrono::seconds step = 0s) {
            return (gc_clock::now().time_since_epoch() + duration_cast<microseconds>(step)).count();
        };
        auto sst_gen = env.make_sst_factory(s);
        auto pkey = tests::generate_partition_key(s);
        auto cf = env.make_table_for_tests(s);
        auto close_cf = deferred_stop(cf);

        auto get_next_ckey = [&] {
            static thread_local int32_t row_value = 1;
            return clustering_key::from_exploded(*s, {int32_type->decompose(row_value++)});
        };

        auto make_row = [&] () {
            mutation m(s, pkey);
            auto c_key = get_next_ckey();
            // Use a step to make sure that rows aren't covered by tombstone.
            m.set_clustered_cell(c_key, bytes("value"), data_value(int32_t(0)), next_timestamp(seconds(3600)));
            return m;
        };

        auto make_open_ended_range_tombstone = [&] () {
            mutation m(s, pkey);
            tombstone tomb(api::new_timestamp(), gc_clock::now());
            auto start_key = get_next_ckey();
            auto start_bound = bound_view(start_key, bound_kind::incl_start);
            auto end_bound = bound_view::top();
            range_tombstone rt(start_bound,
                    end_bound,
                    tomb);
            m.partition().apply_delete(*s, std::move(rt));
            return m;
        };

        auto deletion_mut = [&] () {
            mutation m(s, pkey);
            tombstone tomb(next_timestamp(), gc_clock::now());
            m.partition().apply(tomb);
            return m;
        }();

        std::vector<mutation> mutations;
        static constexpr size_t rows = 20;
        mutations.reserve(1 + rows);
        mutations.push_back(std::move(deletion_mut));

        for (size_t i = 0; i < rows; i++) {
            mutations.push_back(make_row());
            mutations.push_back(make_open_ended_range_tombstone());
        }

        auto sst = make_sstable_containing(sst_gen, std::move(mutations));

        auto desc = sstables::compaction_descriptor({ sst });
        // With max_sstable_bytes of 1, we'll perform the splitting of the partition as soon as possible.
        desc.max_sstable_bytes = 1;
        desc.can_split_large_partition = true;
        // Set block size to 1, so promoted index is generated for every row written, allowing the split to happen as soon as possible.
        env.manager().set_promoted_index_block_size(1);

        auto ret = compact_sstables(env, std::move(desc), cf, sst_gen, replacer_fn_no_op(), can_purge_tombstones::no).get();

        testlog.info("Large partition splitting on compaction created {} sstables", ret.new_sstables.size());
        BOOST_REQUIRE(ret.new_sstables.size() > 1);

        sstable_run sst_run;

        std::optional<range_tombstone_entry> last_rt;
        std::optional<position_in_partition> last_pos;
        position_in_partition::tri_compare pos_tri_cmp(*s);

        for (auto& sst : ret.new_sstables) {
            sst = env.reusable_sst(sst).get();
            BOOST_REQUIRE(sst->may_have_partition_tombstones());

            auto reader = sstable_reader(sst, s, env.make_reader_permit());

            mutation_opt m = read_mutation_from_mutation_reader(reader).get();
            BOOST_REQUIRE(m);
            BOOST_REQUIRE(m->decorated_key().equal(*s, pkey));
            // ASSERT that partition tobmstone is replicated to every fragment.
            BOOST_REQUIRE(m->partition().partition_tombstone());
            auto rows = m->partition().clustered_rows();
            BOOST_REQUIRE(rows.calculate_size() >= 1);
            auto& row = rows.begin()->row();
            auto& cells = row.cells();
            BOOST_REQUIRE_EQUAL(cells.size(), 1);
            auto& cdef = *s->get_column_definition("value");
            BOOST_REQUIRE(cells.cell_at(cdef.id).as_atomic_cell(cdef).is_live());

            testlog.info("SSTable of generation {} has position range [{}, {}]", sst->generation(), sst->first_partition_first_position(), sst->last_partition_last_position());

            // Check that if we split partition with active range tombstone, check we will issue properly
            // the end bound in fragment A and re-emit it as start bound in fragment B.
            // Fragment A will contain range [r1, r2]
            // And fragment B will contain range (r2, ...]
            // assuming the split happened when last position was r2.
            auto& current_first_rt = *m->partition().row_tombstones().begin();
            if (auto previous_last_rt = std::exchange(last_rt, *m->partition().row_tombstones().rbegin())) {
                testlog.info("\tprevious last rt's end bound: {}", previous_last_rt->end_bound());
                testlog.info("\tcurrent first rt's start bound: {}", current_first_rt.start_bound());
                BOOST_REQUIRE(previous_last_rt->end_bound().prefix() == current_first_rt.start_bound().prefix());
                BOOST_REQUIRE(previous_last_rt->end_bound().kind() == bound_kind::incl_end);
                BOOST_REQUIRE(current_first_rt.start_bound().kind() == bound_kind::excl_start);
            }
            const auto& current_first_pos = sst->first_partition_first_position();
            if (auto previous_last_pos = std::exchange(last_pos, sst->last_partition_last_position())) {
                testlog.info("\tprevious last pos: {}", previous_last_pos);
                testlog.info("\tcurrent first pos: {}", current_first_pos);
                BOOST_REQUIRE(pos_tri_cmp(*previous_last_pos, current_first_pos) == 0);
            }

            BOOST_REQUIRE(!(reader)().get());

            reader.close().get();

            // CHECK that all fragments generated by compaction are disjoint.
            BOOST_REQUIRE(sst_run.insert(sst) == true);
        }

    });
}

SEASTAR_TEST_CASE(check_table_sstable_set_includes_maintenance_sstables) {
    return test_env::do_with_async([] (test_env& env) {
        simple_schema ss;
        auto s = ss.schema();
        auto pks = ss.make_pkeys(1);

        auto mut1 = mutation(s, pks[0]);
        mut1.partition().apply_insert(*s, ss.make_ckey(0), ss.new_timestamp());
        auto sst = make_sstable_containing(env.make_sstable(s), {std::move(mut1)});

        auto cf = env.make_table_for_tests(s);
        auto close_cf = deferred_stop(cf);

        cf->add_sstable_and_update_cache(sst, sstables::offstrategy::yes).get();

        BOOST_REQUIRE(cf->get_sstable_set().all()->size() == 1);
        BOOST_REQUIRE(cf->get_sstable_set().size() == 1);
    });
}

// Without commit aba475fe1d24d5c, scylla will fail miserably (either with abort or segfault; depends on the version).
SEASTAR_THREAD_TEST_CASE(compaction_manager_stop_and_drain_race_test) {
    abort_source as;

    auto cfg = compaction_manager::config{ .available_memory = 1 };
    auto task_manager = tasks::task_manager({}, as);
    auto stop_task_manager = deferred_stop(task_manager);
    auto cm = compaction_manager(cfg, as, task_manager);
    auto stop_cm = deferred_stop(cm);
    cm.enable();

    testlog.info("requesting abort");
    as.request_abort();

    testlog.info("draining compaction manager");
    cm.drain().get();

    testlog.info("stopping compaction manager");
    stop_cm.stop_now();
}

SEASTAR_TEST_CASE(test_print_shared_sstables_vector) {
    return test_env::do_with_async([] (test_env& env) {
        simple_schema ss;
        auto s = ss.schema();
        auto pks = ss.make_pkeys(2);
        auto sst_gen = env.make_sst_factory(s);

        std::vector<sstables::shared_sstable> ssts(2);

        auto mut0 = mutation(s, pks[0]);
        mut0.partition().apply_insert(*s, ss.make_ckey(0), ss.new_timestamp());
        ssts[0] = make_sstable_containing(sst_gen, {std::move(mut0)});

        auto mut1 = mutation(s, pks[1]);
        mut1.partition().apply_insert(*s, ss.make_ckey(1), ss.new_timestamp());
        ssts[1] = make_sstable_containing(sst_gen, {std::move(mut1)});

        std::string msg = seastar::format("{}", ssts);
        for (const auto& sst : ssts) {
            auto gen_str = format("{}", sst->generation());
            BOOST_REQUIRE(msg.find(gen_str) != std::string::npos);
        }
    });
}

SEASTAR_TEST_CASE(tombstone_gc_disabled_test) {
    return test_env::do_with_async([] (test_env& env) {
        auto builder = schema_builder("tests", "tombstone_purge")
                .with_column("id", utf8_type, column_kind::partition_key)
                .with_column("value", int32_type);
        builder.set_gc_grace_seconds(0);
        auto s = builder.build();

        auto sst_gen = env.make_sst_factory(s);

        auto compact = [&, s] (bool tombstone_gc_enabled, bool update_tomb_gc_during_compaction, std::vector<shared_sstable> all) -> std::vector<shared_sstable> {
            auto t = env.make_table_for_tests(s);
            auto my_sst_gen = sst_gen;
            if (update_tomb_gc_during_compaction) {
                // update tombstone_gc setting after compaction was initialized,
                // when it creates the first output SSTable, to stress the
                // ability of tombstone gc update taking immediate effect
                // even on ongoing compactions.
                my_sst_gen = [&] () -> sstables::shared_sstable {
                    t.set_tombstone_gc_enabled(tombstone_gc_enabled);
                    return sst_gen();
                };
            } else {
                t.set_tombstone_gc_enabled(tombstone_gc_enabled);
            }
            auto stop = deferred_stop(t);
            for (auto& sst : all) {
                column_family_test(t).add_sstable(sst).get();
            }
            return compact_sstables(env, sstables::compaction_descriptor(all), t, my_sst_gen).get().new_sstables;
        };

        auto next_timestamp = [] {
            static thread_local api::timestamp_type next = 1;
            return next++;
        };

        auto make_insert = [&] (partition_key key) {
            mutation m(s, key);
            m.set_clustered_cell(clustering_key::make_empty(), bytes("value"), data_value(int32_t(1)), next_timestamp());
            return m;
        };

        auto make_delete = [&] (partition_key key) {
            mutation m(s, key);
            tombstone tomb(next_timestamp(), gc_clock::now());
            m.partition().apply(tomb);
            return m;
        };

        auto alpha = partition_key::from_exploded(*s, {to_bytes("alpha")});
        auto beta = partition_key::from_exploded(*s, {to_bytes("beta")});

        auto perform_tombstone_gc_test = [&] (bool tombstone_gc_enabled) {
            auto mut1 = make_insert(alpha);
            auto mut2 = make_delete(alpha);
            auto mut3 = make_insert(beta);

            auto sst1 = make_sstable_containing(sst_gen, {mut1});
            auto sst2 = make_sstable_containing(sst_gen, {mut2, mut3});

            forward_jump_clocks(std::chrono::seconds(1));

            auto do_perform_tombstone_gc_test = [&] (bool update_tomb_gc_during_compaction) {
                auto result = compact(tombstone_gc_enabled, update_tomb_gc_during_compaction, {sst1, sst2});
                BOOST_REQUIRE_EQUAL(1, result.size());

                std::set<mutation, mutation_decorated_key_less_comparator> sorted_mut;
                sorted_mut.insert(mut2);
                sorted_mut.insert(mut3);

                auto r = assert_that(sstable_reader(result[0], s, env.make_reader_permit()));
                for (auto&& mut: sorted_mut) {
                    bool is_tombstone = bool(mut.partition().partition_tombstone());
                    // if tombstone compaction is enabled, expired tombstone is purged
                    if (is_tombstone && tombstone_gc_enabled) {
                        continue;
                    }
                    r.produces(mut);
                }
                r.produces_end_of_stream();
            };

            do_perform_tombstone_gc_test(false);
            do_perform_tombstone_gc_test(true);
        };

        perform_tombstone_gc_test(false);
        perform_tombstone_gc_test(true);
    });
}

// Check that tombstone newer than grace period won't trigger bloom filter check
// against uncompacting sstable, during compaction.
SEASTAR_TEST_CASE(compaction_optimization_to_avoid_bloom_filter_checks) {
    return test_env::do_with_async([] (test_env& env) {
        auto builder = schema_builder("tests", "tombstone_purge")
            .with_column("id", utf8_type, column_kind::partition_key)
            .with_column("value", int32_type);
        builder.set_gc_grace_seconds(10000);
        auto s = builder.build();
        auto sst_gen = env.make_sst_factory(s);

        auto compact = [&, s] (std::vector<shared_sstable> all, std::vector<shared_sstable> c) -> compaction_result {
            auto t = env.make_table_for_tests(s);
            t->disable_auto_compaction().get();
            auto stop = deferred_stop(t);
            for (auto& sst : all) {
                column_family_test(t).add_sstable(sst).get();
            }
            auto desc = sstables::compaction_descriptor(std::move(c));
            desc.enable_garbage_collection(t->get_sstable_set());
            return compact_sstables(env, std::move(desc), t, sst_gen).get();
        };

        auto make_insert = [&] (partition_key key) {
            mutation m(s, key);
            m.set_clustered_cell(clustering_key::make_empty(), bytes("value"), data_value(int32_t(1)), api::new_timestamp());
            return m;
        };
        auto make_delete = [&] (partition_key key) {
            mutation m(s, key);
            tombstone tomb(api::new_timestamp(), gc_clock::now());
            m.partition().apply(tomb);
            return m;
        };

        auto uncompacting = make_sstable_containing(sst_gen, { make_insert(partition_key::from_exploded(*s, {to_bytes("pk1")}) )});
        auto compacting = make_sstable_containing(sst_gen, { make_delete(partition_key::from_exploded(*s, {to_bytes("pk1")}) )});

        auto result = compact({uncompacting, compacting}, {compacting});
        BOOST_REQUIRE_EQUAL(1, result.new_sstables.size());
        BOOST_REQUIRE_EQUAL(0, result.stats.bloom_filter_checks);

        forward_jump_clocks(std::chrono::seconds(s->gc_grace_seconds()) + 1s);

        result = compact({uncompacting, compacting}, {compacting});
        BOOST_REQUIRE_EQUAL(1, result.new_sstables.size());
        BOOST_REQUIRE_EQUAL(1, result.stats.bloom_filter_checks);
    });
}

static future<> run_incremental_compaction_test(sstables::offstrategy offstrategy, std::function<future<>(table_for_tests&, owned_ranges_ptr)> run_compaction) {
    return test_env::do_with_async([run_compaction = std::move(run_compaction), offstrategy] (test_env& env) {
        auto builder = schema_builder("tests", "test")
                .with_column("id", utf8_type, column_kind::partition_key)
                .with_column("value", int32_type);
        builder.set_gc_grace_seconds(10000);
        builder.set_compaction_strategy(sstables::compaction_strategy_type::leveled);
        std::map<sstring, sstring> opts = {
            { "sstable_size_in_mb", "0" }, // makes sure that every mutation produces one fragment, to trigger incremental compaction
        };
        builder.set_compaction_strategy_options(std::move(opts));
        auto s = builder.build();
        auto sst_gen = env.make_sst_factory(s);

        auto make_insert = [&] (partition_key key) {
            mutation m(s, key);
            m.set_clustered_cell(clustering_key::make_empty(), bytes("value"), data_value(int32_t(1)), api::new_timestamp());
            return m;
        };

        std::vector<utils::observer<sstable&>> observers;
        std::vector<shared_sstable> ssts;
        size_t sstables_closed = 0;
        size_t sstables_closed_during_cleanup = 0;
        const size_t sstables_nr = s->max_compaction_threshold() * 2;

        dht::token_range_vector owned_token_ranges;

        std::set<mutation, mutation_decorated_key_less_comparator> merged;
        for (unsigned i = 0; i < sstables_nr * 2; i++) {
            merged.insert(make_insert(partition_key::from_exploded(*s, {to_bytes(to_sstring(i))})));
        }

        std::unordered_set<sstables::generation_type> gens; // input sstable generations
        run_id run_identifier = run_id::create_random_id();
        auto merged_it = merged.begin();
        for (unsigned i = 0; i < sstables_nr; i++) {
            auto mut1 = std::move(*merged_it);
            merged_it++;
            auto mut2 = std::move(*merged_it);
            merged_it++;
            auto sst = make_sstable_containing(sst_gen, {
                std::move(mut1),
                std::move(mut2)
            });
            sstables::test(sst).set_run_identifier(run_identifier); // in order to produce multi-fragment run.
            sst->set_sstable_level(offstrategy ? 0 : 1);

            // every sstable will be eligible for cleanup, by having both an owned and unowned token.
            owned_token_ranges.push_back(dht::token_range::make_singular(sst->get_last_decorated_key().token()));

            gens.insert(sst->generation());
            ssts.push_back(std::move(sst));
        }

        size_t last_input_sstable_count = sstables_nr;
        auto t = env.make_table_for_tests(s);
        {
            auto& cm = t->get_compaction_manager();
            auto stop = deferred_stop(t);
            t->disable_auto_compaction().get();
            const dht::token_range_vector empty_owned_ranges;
            for (auto&& sst : ssts) {
                t->add_sstable_and_update_cache(sst, offstrategy).get();
                testlog.info("run id {}, refcount = {}", sst->run_identifier(), sst.use_count());
                column_family_test::update_sstables_known_generation(*t, sst->generation());
                observers.push_back(sst->add_on_closed_handler([&] (sstable& sst) mutable {
                    auto sstables = t->get_sstables();
                    auto input_sstable_count = std::count_if(sstables->begin(), sstables->end(), [&] (const shared_sstable& sst) {
                        return gens.count(sst->generation());
                    });

                    testlog.info("Closing sstable of generation {}, table set size: {}", sst.generation(), input_sstable_count);
                    sstables_closed++;
                    if (std::cmp_less(input_sstable_count, last_input_sstable_count)) {
                        sstables_closed_during_cleanup++;
                        last_input_sstable_count = input_sstable_count;
                    }
                }));
            }
            ssts = {}; // releases references
            auto owned_ranges_ptr = make_lw_shared<const dht::token_range_vector>(std::move(owned_token_ranges));
            run_compaction(t, std::move(owned_ranges_ptr)).get();
            BOOST_REQUIRE(cm.sstables_requiring_cleanup(t->try_get_table_state_with_static_sharding()).empty());
            testlog.info("Cleanup has finished");
        }

        while (sstables_closed != sstables_nr) {
            yield().get();
        }

        testlog.info("Closed sstables {}, Closed during cleanup {}", sstables_closed, sstables_closed_during_cleanup);

        BOOST_REQUIRE(sstables_closed == sstables_nr);
        BOOST_REQUIRE(sstables_closed_during_cleanup >= sstables_nr / 2);
    }, test_env_config{ .use_uuid = false });
}

SEASTAR_TEST_CASE(cleanup_incremental_compaction_test) {
    return run_incremental_compaction_test(sstables::offstrategy::no, [] (table_for_tests& t, owned_ranges_ptr owned_ranges) -> future<> {
        return t->perform_cleanup_compaction(std::move(owned_ranges), tasks::task_info{});
    });
}

SEASTAR_TEST_CASE(offstrategy_incremental_compaction_test) {
    return run_incremental_compaction_test(sstables::offstrategy::yes, [] (table_for_tests& t, owned_ranges_ptr owned_ranges) -> future<> {
        bool performed = co_await t->perform_offstrategy_compaction(tasks::task_info{});
        BOOST_REQUIRE(performed);
    });
}

SEASTAR_TEST_CASE(cleanup_during_offstrategy_incremental_compaction_test) {
    return test_env::do_with_async([] (test_env& env) {
        auto builder = schema_builder("tests", "test")
                .with_column("id", utf8_type, column_kind::partition_key)
                .with_column("value", int32_type);
        builder.set_gc_grace_seconds(10000);
        builder.set_compaction_strategy(sstables::compaction_strategy_type::leveled);
        std::map<sstring, sstring> opts = {
            { "sstable_size_in_mb", "0" }, // makes sure that every mutation produces one fragment, to trigger incremental compaction
        };
        builder.set_compaction_strategy_options(std::move(opts));
        auto s = builder.build();
        auto sst_gen = env.make_sst_factory(s);

        auto make_insert = [&] (partition_key key) {
            mutation m(s, key);
            m.set_clustered_cell(clustering_key::make_empty(), bytes("value"), data_value(int32_t(1)), api::new_timestamp());
            return m;
        };

        std::vector<utils::observer<sstable&>> observers;
        std::vector<shared_sstable> ssts;
        size_t sstables_closed = 0;
        size_t sstables_missing_on_delete = 0;
        static constexpr size_t sstables_nr = 10;

        dht::token_range_vector owned_token_ranges;

        std::set<mutation, mutation_decorated_key_less_comparator> merged;
        for (unsigned i = 0; i < sstables_nr * 2; i++) {
            merged.insert(make_insert(partition_key::from_exploded(*s, {to_bytes(to_sstring(i))})));
        }

        std::unordered_set<sstables::generation_type> gens; // input sstable generations
        auto merged_it = merged.begin();
        for (unsigned i = 0; i < sstables_nr; i++) {
            auto mut1 = std::move(*merged_it);
            merged_it++;
            auto mut2 = std::move(*merged_it);
            merged_it++;
            auto sst = make_sstable_containing(sst_gen, {
                std::move(mut1),
                std::move(mut2)
            });
            // Force a new run_id to trigger offstrategy compaction
            sstables::test(sst).set_run_identifier(run_id::create_random_id());
            // Set level to 0 to trigger offstrategy compaction
            sst->set_sstable_level(0);

            // every sstable will be eligible for cleanup, by having both an owned and unowned token.
            owned_token_ranges.push_back(dht::token_range::make_singular(sst->get_last_decorated_key().token()));

            gens.insert(sst->generation());
            ssts.push_back(std::move(sst));
        }

        {
            auto t = env.make_table_for_tests(s);
            auto& cm = t->get_compaction_manager();
            auto stop = deferred_stop(t);
            t->disable_auto_compaction().get();
            const dht::token_range_vector empty_owned_ranges;
            for (auto&& sst : ssts) {
                testlog.info("run id {}", sst->run_identifier());
                column_family_test(t).add_sstable(sst, sstables::offstrategy::yes).get();
                observers.push_back(sst->add_on_closed_handler([&] (sstable& sst) mutable {
                    auto sstables = t->get_sstables();
                    testlog.info("Closing sstable of generation {}, table set size: {}", sst.generation(), sstables->size());
                    sstables_closed++;
                }));
                observers.push_back(sst->add_on_delete_handler([&] (sstable& sst) mutable {
                    // ATTN -- the _on_delete callback is not necessarily running in thread
                    auto missing = (::access(fmt::to_string(sst.get_filename()).c_str(), F_OK) != 0);
                    testlog.info("Deleting sstable of generation {}: missing={}", sst.generation(), missing);
                    sstables_missing_on_delete += missing;
                }));
            }
            ssts = {}; // releases references
            auto owned_ranges_ptr = make_lw_shared<const dht::token_range_vector>(std::move(owned_token_ranges));
            t->perform_cleanup_compaction(std::move(owned_ranges_ptr), tasks::task_info{}).get();
            BOOST_REQUIRE(cm.sstables_requiring_cleanup(t->try_get_table_state_with_static_sharding()).empty());
            testlog.info("Cleanup has finished");
        }

        while (sstables_closed != sstables_nr) {
            yield().get();
        }

        testlog.info("Closed sstables {}, missing on delete {}", sstables_closed, sstables_missing_on_delete);

        BOOST_REQUIRE_EQUAL(sstables_closed, sstables_nr);
        BOOST_REQUIRE_EQUAL(sstables_missing_on_delete, 0);
    });
}

future<> test_sstables_excluding_staging_correctness(test_env_config cfg) {
    return test_env::do_with_async([] (test_env& env) {
        simple_schema ss;
        auto s = ss.schema();
        auto pks = ss.make_pkeys(2);

        auto make_mut = [&] (auto pkey) {
            auto mut1 = mutation(s, pkey);
            mut1.partition().apply_insert(*s, ss.make_ckey(0), ss.new_timestamp());
            return mut1;
        };
        std::set<mutation, mutation_decorated_key_less_comparator> sorted_muts;
        sorted_muts.insert(make_mut(pks[0]));
        sorted_muts.insert(make_mut(pks[1]));

        auto t = env.make_table_for_tests(s);
        auto close_t = deferred_stop(t);

        auto sst_gen = env.make_sst_factory(s);

        auto staging_sst = make_sstable_containing(sst_gen, {*sorted_muts.begin()});
        staging_sst->change_state(sstables::sstable_state::staging).get();
        BOOST_REQUIRE(staging_sst->requires_view_building());

        auto regular_sst = make_sstable_containing(sst_gen, {*sorted_muts.rbegin()});

        t->add_sstable_and_update_cache(staging_sst).get();
        t->add_sstable_and_update_cache(regular_sst).get();

        {
            testlog.info("table::as_mutation_source_excluding_staging()");
            auto ms_excluding_staging = t->as_mutation_source_excluding_staging();
            assert_that(ms_excluding_staging.make_reader_v2(s, env.make_reader_permit(), query::full_partition_range))
                .produces(*sorted_muts.rbegin())
                .produces_end_of_stream();
        }

        {
            testlog.info("table::as_mutation_source()");
            auto ms_inclusive = t->as_mutation_source();
            assert_that(ms_inclusive.make_reader_v2(s, env.make_reader_permit(), query::full_partition_range))
                    .produces(*sorted_muts.begin())
                    .produces(*sorted_muts.rbegin())
                    .produces_end_of_stream();
        }
    }, std::move(cfg));
}

SEASTAR_TEST_CASE(test_sstables_excluding_staging_correctness_local) {
    return test_sstables_excluding_staging_correctness({});
}

SEASTAR_TEST_CASE(test_sstables_excluding_staging_correctness_s3) {
    return test_sstables_excluding_staging_correctness({ .storage = make_test_object_storage_options() });
}

// Reproducer for https://github.com/scylladb/scylladb/issues/15726.
SEASTAR_TEST_CASE(produces_optimal_filter_by_estimating_correctly_partitions_per_sstable) {
    return test_env::do_with_async([] (test_env& env) {
        auto builder = schema_builder("tests", "test")
                .with_column("id", utf8_type, column_kind::partition_key)
                .with_column("value", int32_type);
        builder.set_compressor_params(compression_parameters::no_compression());
        auto s = builder.build();
        auto sst_gen = env.make_sst_factory(s);

        auto compact = [&, s] (std::vector<shared_sstable> c, uint64_t max_size) -> compaction_result {
            auto t = env.make_table_for_tests(s);
            auto stop = deferred_stop(t);
            t->disable_auto_compaction().get();
            auto desc = sstables::compaction_descriptor(std::move(c));
            desc.max_sstable_bytes = max_size;
            return compact_sstables(env, std::move(desc), t, sst_gen).get();
        };

        auto make_insert = [&] (partition_key key) {
            mutation m(s, key);
            m.set_clustered_cell(clustering_key::make_empty(), bytes("value"), data_value(int32_t(1)), api::new_timestamp());
            return m;
        };

        const sstring shared_key_prefix = "832193982198319823hsdjahdashjdsa81923189381931829sdajidjkas812938219jdsalljdadsajk319820";

        std::vector<mutation> muts;
        constexpr int keys = 200;
        muts.reserve(keys);
        for (auto i = 0; i < keys; i++) {
            muts.push_back(make_insert(partition_key::from_exploded(*s, {to_bytes(shared_key_prefix + to_sstring(i))})));
        }
        auto sst = make_sstable_containing(sst_gen, std::move(muts));

        testlog.info("index size: {}, data_size: {}", sst->index_size(), sst->ondisk_data_size());

        uint64_t max_sstable_size = std::ceil(double(sst->ondisk_data_size()) / 10);
        auto ret = compact({sst}, max_sstable_size);

        uint64_t partitions_per_sstable = keys / ret.new_sstables.size();
        auto filter = utils::i_filter::get_filter(partitions_per_sstable, s->bloom_filter_fp_chance(), utils::filter_format::m_format);

        auto comp = ret.new_sstables.front()->get_open_info().get();

        // Filter for SSTable generated cannot be lower than the one expected
        testlog.info("filter size: actual={}, expected>={}", comp.components->filter->memory_size(), filter->memory_size());
        BOOST_REQUIRE(comp.components->filter->memory_size() >= filter->memory_size());
    });
}

SEASTAR_TEST_CASE(splitting_compaction_test) {
    return test_env::do_with_async([] (test_env& env) {
        auto builder = schema_builder("tests", "twcs_splitting")
                .with_column("id", utf8_type, column_kind::partition_key)
                .with_column("cl", int32_type, column_kind::clustering_key)
                .with_column("value", int32_type);
        builder.set_compaction_strategy(sstables::compaction_strategy_type::time_window);
        auto s = builder.build();

        auto sst_gen = env.make_sst_factory(s);

        auto next_timestamp = [] (auto step) {
            using namespace std::chrono;
            return (api::timestamp_clock::now().time_since_epoch() - duration_cast<microseconds>(step)).count();
        };

        auto make_insert = [&] (const dht::decorated_key& key, api::timestamp_clock::duration step) {
            static thread_local int32_t value = 1;

            mutation m(s, key);
            auto c_key = clustering_key::from_exploded(*s, {int32_type->decompose(value++)});
            m.set_clustered_cell(c_key, bytes("value"), data_value(int32_t(value)), next_timestamp(step));
            return m;
        };

        auto keys = tests::generate_partition_keys(100, s);
        std::vector<mutation> muts;

        muts.reserve(keys.size() * 2);
        for (auto& k : keys) {
            muts.push_back(make_insert(k, 0ms));
            muts.push_back(make_insert(k, 720h));
        }

        auto t = env.make_table_for_tests(s);
        auto close_table = deferred_stop(t);
        t->start();

        auto input = make_sstable_containing(sst_gen, std::move(muts));

        std::unordered_set<int64_t> groups;
        auto classify_fn = [&groups] (dht::token t) -> mutation_writer::token_group_id {
            auto r = dht::compaction_group_of(1, t);
            if (groups.insert(r).second) {
                testlog.info("Group {} detected!", r);
            }
            return r;
        };

        auto desc = sstables::compaction_descriptor({input});
        desc.options = compaction_type_options::make_split(classify_fn);

        auto ret = compact_sstables(env, std::move(desc), t, sst_gen, replacer_fn_no_op()).get();

        auto twcs_options = time_window_compaction_strategy_options(s->compaction_strategy_options());
        auto window_for = [&twcs_options] (api::timestamp_type timestamp) {
            return time_window_compaction_strategy::get_window_for(twcs_options, timestamp);
        };

        // Assert that data was segregated both by token and timestamp.
        for (auto& sst : ret.new_sstables) {
            testlog.info("{}: token_groups: [{}, {}], windows: [{}, {}]",
                         sst->get_filename(),
                         classify_fn(sst->get_first_decorated_key().token()),
                         classify_fn(sst->get_last_decorated_key().token()),
                         window_for(sst->get_stats_metadata().min_timestamp),
                         window_for(sst->get_stats_metadata().max_timestamp));
            BOOST_REQUIRE_EQUAL(classify_fn(sst->get_first_decorated_key().token()), classify_fn(sst->get_last_decorated_key().token()));
            BOOST_REQUIRE_EQUAL(window_for(sst->get_stats_metadata().min_timestamp), window_for(sst->get_stats_metadata().max_timestamp));
        }
        const size_t expected_output_size = 4; // 2 token groups * 2 windows.
        BOOST_REQUIRE(ret.new_sstables.size() == expected_output_size);

        auto& cm = t->get_compaction_manager();
        auto split_opt = compaction_type_options::split{classify_fn};
        auto new_ssts = cm.maybe_split_sstable(input, t.as_table_state(), split_opt).get();
        BOOST_REQUIRE(new_ssts.size() == expected_output_size);
        for (auto& sst : new_ssts) {
            // split sstables don't require further split.
            auto ssts = cm.maybe_split_sstable(sst, t.as_table_state(), split_opt).get();
            BOOST_REQUIRE(ssts.size() == 1);
            BOOST_REQUIRE(ssts.front() == sst);
        }
        // test exception propagation
        auto throwing_classifier = [&] (dht::token t) -> mutation_writer::token_group_id {
            // skip first and last token, to not trigger exception when checking if sstable needs split.
            if (t != input->get_first_decorated_key().token() && t != input->get_last_decorated_key().token()) {
                throw std::runtime_error("exception");
            }
            return classify_fn(t);
        };
        BOOST_REQUIRE_THROW(cm.maybe_split_sstable(input, t.as_table_state(), compaction_type_options::split{throwing_classifier}).get(),
                            std::runtime_error);
    });
}

BOOST_AUTO_TEST_SUITE_END()
