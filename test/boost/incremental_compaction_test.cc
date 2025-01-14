/*
 * Copyright (C) 2019 ScyllaDB
 *
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <boost/test/unit_test.hpp>
#include <memory>
#include <utility>

#include <seastar/core/sstring.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/do_with.hh>
#include <seastar/core/distributed.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>
#include "sstables/sstables.hh"
#include "compaction/incremental_compaction_strategy.hh"
#include "schema/schema.hh"
#include "replica/database.hh"
#include "compaction/compaction_manager.hh"
#include "sstable_test.hh"
#include "sstables/metadata_collector.hh"
#include "test/lib/tmpdir.hh"
#include "cell_locking.hh"
#include "test/lib/mutation_reader_assertions.hh"
#include "test/lib/key_utils.hh"
#include "service/storage_proxy.hh"
#include "test/lib/sstable_run_based_compaction_strategy_for_tests.hh"
#include "dht/i_partitioner.hh"
#include "dht/murmur3_partitioner.hh"
#include "db/large_data_handler.hh"
#include "db/config.hh"

#include "test/lib/sstable_utils.hh"
#include "test/lib/test_services.hh"

using namespace sstables;

static mutation_reader sstable_reader(reader_permit permit, shared_sstable sst, schema_ptr s) {
    return sst->as_mutation_source().make_reader_v2(s, std::move(permit), query::full_partition_range, s->full_slice());

}

class strategy_control_for_test : public strategy_control {
    bool _has_ongoing_compaction;
public:
    explicit strategy_control_for_test(bool has_ongoing_compaction) noexcept : _has_ongoing_compaction(has_ongoing_compaction) {}

    bool has_ongoing_compaction(table_state& table_s) const noexcept override {
        return _has_ongoing_compaction;
    }
    virtual std::vector<sstables::shared_sstable> candidates(table_state& t) const override {
        return boost::copy_range<std::vector<sstables::shared_sstable>>(*t.main_sstable_set().all());
    }
    virtual std::vector<sstables::frozen_sstable_run> candidates_as_runs(table_state& t) const override {
        return t.main_sstable_set().all_sstable_runs();
    }
};

static std::unique_ptr<strategy_control> make_strategy_control_for_test(bool has_ongoing_compaction) {
    return std::make_unique<strategy_control_for_test>(has_ongoing_compaction);
}

SEASTAR_TEST_CASE(incremental_compaction_test) {
    return sstables::test_env::do_with_async([&] (sstables::test_env& env) {
        auto builder = schema_builder("tests", "incremental_compaction_test")
                .with_column("id", utf8_type, column_kind::partition_key)
                .with_column("value", int32_type)
                .with_partitioner("org.apache.cassandra.dht.Murmur3Partitioner")
                .with_sharder(smp::count, 0);
        auto s = builder.build();

        auto tmp = make_lw_shared<tmpdir>();
        auto sst_gen = [&env, s, tmp] () mutable {
            auto sst = env.make_sstable(s, tmp->path().string(), env.new_generation(), sstable_version_types::md, big);
            return sst;
        };

        table_for_tests cf = env.make_table_for_tests(s, tmp->path().string());
        auto close_cf = deferred_stop(cf);
        cf->set_compaction_strategy(sstables::compaction_strategy_type::size_tiered);
        auto compact = [&, s] (std::vector<shared_sstable> all, auto replacer) -> std::vector<shared_sstable> {
            auto desc = sstables::compaction_descriptor(std::move(all), 1, 0);
            desc.enable_garbage_collection(cf->get_sstable_set());
            return compact_sstables(env, std::move(desc), cf, sst_gen, replacer).get().new_sstables;
        };
        auto make_insert = [&] (auto p) {
            auto key = p.key();
            mutation m(s, key);
            m.set_clustered_cell(clustering_key::make_empty(), bytes("value"), data_value(int32_t(1)), 1 /* ts */);
            BOOST_REQUIRE(m.decorated_key().token() == p.token());
            return m;
        };

        auto tokens = tests::generate_partition_keys(16, s, local_shard_only::yes, tests::key_size{8, 8});
        std::unordered_set<shared_sstable> sstables;
        std::vector<utils::observer<sstable&>> observers;
        sstables::sstable_run_based_compaction_strategy_for_tests cs;

        auto do_replace = [&] (const std::vector<shared_sstable>& old_sstables, const std::vector<shared_sstable>& new_sstables) {
            for (auto& old_sst : old_sstables) {
                BOOST_REQUIRE(sstables.count(old_sst));
                sstables.erase(old_sst);
            }
            for (auto& new_sst : new_sstables) {
                BOOST_REQUIRE(!sstables.count(new_sst));
                sstables.insert(new_sst);
            }
            column_family_test(cf).rebuild_sstable_list(cf.as_table_state(), new_sstables, old_sstables).get();
            env.test_compaction_manager().propagate_replacement(cf.as_table_state(), old_sstables, new_sstables);
        };

        auto do_incremental_replace = [&] (auto old_sstables, auto new_sstables, auto& expected_sst, auto& closed_sstables_tracker) {
            // that's because each sstable will contain only 1 mutation.
            BOOST_REQUIRE(old_sstables.size() == 1);
            BOOST_REQUIRE(new_sstables.size() == 1);
            auto old_sstable = old_sstables.front();
            // check that sstable replacement follows token order
            BOOST_REQUIRE(*expected_sst == old_sstable->generation());
            expected_sst++;
            // check that previously released sstables were already closed
            BOOST_REQUIRE(*closed_sstables_tracker == old_sstable->generation());

            do_replace(old_sstables, new_sstables);

            observers.push_back(old_sstable->add_on_closed_handler([&] (sstable& sst) {
                BOOST_TEST_MESSAGE(fmt::format("Closing sstable of generation {}", sst.generation()));
                closed_sstables_tracker++;
            }));

            BOOST_TEST_MESSAGE(fmt::format("Removing sstable of generation {}, refcnt: {}", old_sstables.front()->generation(), old_sstables.front().use_count()));
        };

        auto do_compaction = [&] (size_t expected_input, size_t expected_output) -> std::vector<shared_sstable> {
            auto control = make_strategy_control_for_test(false);
            auto desc = cs.get_sstables_for_compaction(cf.as_table_state(), *control);

            // nothing to compact, move on.
            if (desc.sstables.empty()) {
                return {};
            }
            std::unordered_set<sstables::run_id> run_ids;
            bool incremental_enabled = std::any_of(desc.sstables.begin(), desc.sstables.end(), [&run_ids] (shared_sstable& sst) {
                return !run_ids.insert(sst->run_identifier()).second;
            });

            BOOST_REQUIRE(desc.sstables.size() == expected_input);
            auto sstable_run = desc.sstables
                | std::views::transform([] (auto& sst) { return sst->generation(); })
                | std::ranges::to<std::set>();
            auto expected_sst = sstable_run.begin();
            auto closed_sstables_tracker = sstable_run.begin();
            auto replacer = [&] (compaction_completion_desc ccd) {
                BOOST_REQUIRE(expected_sst != sstable_run.end());
                if (incremental_enabled) {
                    do_incremental_replace(std::move(ccd.old_sstables), std::move(ccd.new_sstables), expected_sst, closed_sstables_tracker);
                } else {
                    do_replace(std::move(ccd.old_sstables), std::move(ccd.new_sstables));
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
        for (auto i = 0U; i < tokens.size(); i++) {
            auto sst = make_sstable_containing(sst_gen, { make_insert(tokens[i]) });
            sst->set_sstable_level(1);
            BOOST_REQUIRE(sst->get_sstable_level() == 1);
            column_family_test(cf).add_sstable(sst).get();
            sstables.insert(std::move(sst));
            do_compaction(4, 4);
        }
        BOOST_REQUIRE(sstables.size() == 16);

        // Generate 1 sstable run from 4 sstables runs of similar size
        auto result = do_compaction(16, 16);
        BOOST_REQUIRE(result.size() == 16);
        for (auto i = 0U; i < tokens.size(); i++) {
            assert_that(sstable_reader(env.semaphore().make_tracking_only_permit(s, "test reader", db::no_timeout, tracing::trace_state_ptr()), result[i], s))
                .produces(make_insert(tokens[i]))
                .produces_end_of_stream();
        }
    });
}

SEASTAR_THREAD_TEST_CASE(incremental_compaction_sag_test) {
    auto builder = schema_builder("tests", "incremental_compaction_test")
        .with_column("id", utf8_type, column_kind::partition_key)
        .with_column("value", int32_type);
    auto s = builder.build();

    struct sag_test {
        test_env& _env;
        mutable table_for_tests _cf;
        incremental_compaction_strategy _ics;
        const unsigned min_threshold = 4;
        const size_t data_set_size = 1'000'000'000;

        static incremental_compaction_strategy make_ics(double space_amplification_goal) {
            std::map<sstring, sstring> options;
            options.emplace(sstring("space_amplification_goal"), sstring(std::to_string(space_amplification_goal)));
            return incremental_compaction_strategy(options);
        }
        static replica::column_family::config make_table_config(test_env& env) {
            auto config = env.make_table_config();
            config.compaction_enforce_min_threshold = true;
            return config;
        }

        sag_test(test_env& env, schema_ptr s, double space_amplification_goal)
            : _env(env)
            , _cf(env.make_table_for_tests(s))
            , _ics(make_ics(space_amplification_goal))
        {
        }

        double space_amplification() const {
            auto sstables = _cf->get_sstables();
            auto total = std::ranges::fold_left(*sstables | std::views::transform(std::mem_fn(&sstable::data_size)), uint64_t(0), std::plus{});
            return double(total) / data_set_size;
        }

        shared_sstable make_sstable_with_size(size_t sstable_data_size) {
            auto sst = _env.make_sstable(_cf->schema(), "/nowhere/in/particular", _env.new_generation(), sstable_version_types::md, big);
            auto keys = tests::generate_partition_keys(2, _cf->schema(), local_shard_only::yes);
            sstables::test(sst).set_values(keys[0].key(), keys[1].key(), stats_metadata{}, sstable_data_size);
            return sst;
        }

        void populate(double target_space_amplification) {
            auto add_sstable = [this] (unsigned sst_data_size) {
                auto sst = make_sstable_with_size(sst_data_size);
                column_family_test(_cf).add_sstable(sst).get();
            };

            add_sstable(data_set_size);
            while (space_amplification() < target_space_amplification) {
                add_sstable(data_set_size / min_threshold);
            }
        }

        void run() {
            auto& table_s = _cf.as_table_state();
            auto control = make_strategy_control_for_test(false);
            for (;;) {
                auto desc = _ics.get_sstables_for_compaction(table_s, *control);
                // no more jobs, bailing out...
                if (desc.sstables.empty()) {
                    break;
                }
                auto total = std::ranges::fold_left(desc.sstables | std::views::transform(std::mem_fn(&sstable::data_size)), uint64_t(0), std::plus{});
                std::vector<shared_sstable> new_ssts = { make_sstable_with_size(std::min(total, data_set_size)) };
                column_family_test(_cf).rebuild_sstable_list(table_s, new_ssts, desc.sstables).get();
            }
        }

        future<> stop() {
            return _cf.stop();
        }
    };

    using SAG = double;
    using TABLE_INITIAL_SA = double;

    auto with_sag_test = [&] (SAG sag, TABLE_INITIAL_SA initial_sa) {
      test_env::do_with_async([&] (test_env& env) {
        sag_test test(env, s, sag);
        test.populate(initial_sa);
        BOOST_REQUIRE(test.space_amplification() >= initial_sa);
        test.run();
        BOOST_REQUIRE(test.space_amplification() <= sag);
        test.stop().get();
      }).get();
    };

    with_sag_test(SAG(1.25), TABLE_INITIAL_SA(1.5));
    with_sag_test(SAG(2), TABLE_INITIAL_SA(1.5));
    with_sag_test(SAG(1.5), TABLE_INITIAL_SA(1.75));
    with_sag_test(SAG(1.01), TABLE_INITIAL_SA(1.5));
    with_sag_test(SAG(1.5), TABLE_INITIAL_SA(1));
}

SEASTAR_TEST_CASE(basic_garbage_collection_test) {
    return test_env::do_with_async([] (test_env& env) {
        auto tmp = tmpdir();
        auto s = schema_builder("ks", "cf")
                .with_column("p1", utf8_type, column_kind::partition_key)
                .with_column("c1", utf8_type, column_kind::clustering_key)
                .with_column("r1", utf8_type)
                .build();

        static constexpr float expired = 0.33;
        // we want number of expired keys to be ~ 1.5*sstables::TOMBSTONE_HISTOGRAM_BIN_SIZE so as to
        // test ability of histogram to return a good estimation after merging keys.
        static int total_keys = std::ceil(sstables::TOMBSTONE_HISTOGRAM_BIN_SIZE/expired)*1.5;

        auto make_insert = [&] (bytes k, uint32_t ttl, uint32_t expiration_time) {
            auto key = partition_key::from_exploded(*s, {k});
            mutation m(s, key);
            auto c_key = clustering_key::from_exploded(*s, {to_bytes("c1")});
            auto live_cell = atomic_cell::make_live(*utf8_type, 0, bytes("a"), gc_clock::time_point(gc_clock::duration(expiration_time)), gc_clock::duration(ttl));
            m.set_clustered_cell(c_key, *s->get_column_definition("r1"), std::move(live_cell));
            return m;
        };
        std::vector<mutation> mutations;
        mutations.reserve(total_keys);

        auto expired_keys = total_keys*expired;
        auto now = gc_clock::now();
        for (auto i = 0; i < expired_keys; i++) {
            // generate expiration time at different time points or only a few entries would be created in histogram
            auto expiration_time = (now - gc_clock::duration(DEFAULT_GC_GRACE_SECONDS*2+i)).time_since_epoch().count();
            mutations.push_back(make_insert(to_bytes("expired_key" + to_sstring(i)), 1, expiration_time));
        }
        auto remaining = total_keys-expired_keys;
        auto expiration_time = (now + gc_clock::duration(3600)).time_since_epoch().count();
        for (auto i = 0; i < remaining; i++) {
            mutations.push_back(make_insert(to_bytes("key" + to_sstring(i)), 3600, expiration_time));
        }

        table_for_tests cf = env.make_table_for_tests(s);
        auto close_cf = deferred_stop(cf);

        auto creator = [&] {
            auto sst = env.make_sstable(s, tmp.path().string(), env.new_generation(), sstables::get_highest_sstable_version(), big);
            return sst;
        };
        auto sst = make_sstable_containing(creator, std::move(mutations));
        column_family_test(cf).add_sstable(sst).get();

        const auto& stats = sst->get_stats_metadata();
        BOOST_REQUIRE(stats.estimated_tombstone_drop_time.bin.size() == sstables::TOMBSTONE_HISTOGRAM_BIN_SIZE);
        // Asserts that two keys are equal to within a positive delta
        sstable_run run;
        // FIXME: can we ignore return value of insert()?
        (void)run.insert(sst);
        BOOST_REQUIRE(std::fabs(run.estimate_droppable_tombstone_ratio(now, cf.as_table_state().get_tombstone_gc_state(), cf.schema()) - expired) <= 0.1);

        auto cd = sstables::compaction_descriptor({ sst });
        cd.enable_garbage_collection(cf->get_sstable_set());
        auto info = compact_sstables(env, std::move(cd), cf, creator).get();
        auto uncompacted_size = sst->data_size();
        BOOST_REQUIRE(info.new_sstables.size() == 1);
        BOOST_REQUIRE(info.new_sstables.front()->estimate_droppable_tombstone_ratio(now, cf.as_table_state().get_tombstone_gc_state(), cf.schema()) == 0.0f);
        BOOST_REQUIRE_CLOSE(info.new_sstables.front()->data_size(), uncompacted_size*(1-expired), 5);
        auto control = make_strategy_control_for_test(false);

        // sstable satisfying conditions will be included
        {
            std::map<sstring, sstring> options;
            options.emplace("tombstone_threshold", "0.3f");
            // that's needed because sstable with droppable data should be old enough.
            options.emplace("tombstone_compaction_interval", "1");
            sleep(2s).get();
            auto cs = sstables::make_compaction_strategy(sstables::compaction_strategy_type::incremental, options);
            auto descriptor = cs.get_sstables_for_compaction(cf.as_table_state(), *control);
            BOOST_REQUIRE(descriptor.sstables.size() == 1);
            BOOST_REQUIRE(descriptor.sstables.front() == sst);
        }

        // sstable with droppable ratio of 0.3 won't be included due to threshold
        {
            std::map<sstring, sstring> options;
            options.emplace("tombstone_threshold", "0.5f");
            auto cs = sstables::make_compaction_strategy(sstables::compaction_strategy_type::incremental, options);
            auto descriptor = cs.get_sstables_for_compaction(cf.as_table_state(), *control);
            BOOST_REQUIRE(descriptor.sstables.size() == 0);
        }
        // sstable which was recently created won't be included due to min interval
        {
            std::map<sstring, sstring> options;
            options.emplace("tombstone_compaction_interval", "3600");
            auto cs = sstables::make_compaction_strategy(sstables::compaction_strategy_type::incremental, options);
            sstables::test(sst).set_data_file_write_time(db_clock::now());
            auto descriptor = cs.get_sstables_for_compaction(cf.as_table_state(), *control);
            BOOST_REQUIRE(descriptor.sstables.size() == 0);
        }
        // sstable which should not be included because of droppable ratio of 0.3, will actually be included
        // because the droppable ratio check has been disabled with unchecked_tombstone_compaction set to true
        {
            std::map<sstring, sstring> options;
            options.emplace("tombstone_compaction_interval", "3600");
            options.emplace("tombstone_threshold", "0.5f");
            options.emplace("unchecked_tombstone_compaction", "true");
            auto cs = sstables::make_compaction_strategy(sstables::compaction_strategy_type::incremental, options);
            sstables::test(sst).set_data_file_write_time(db_clock::now() - std::chrono::seconds(7200));
            auto descriptor = cs.get_sstables_for_compaction(cf.as_table_state(), *control);
            BOOST_REQUIRE(descriptor.sstables.size() == 1);
        }
    });
}

SEASTAR_TEST_CASE(ics_reshape_test) {
    static constexpr unsigned disjoint_sstable_count = 256;

    return test_env::do_with_async([] (test_env& env) {
        auto builder = schema_builder("tests", "ics_reshape_test")
                .with_column("id", utf8_type, column_kind::partition_key)
                .with_column("cl", ::timestamp_type, column_kind::clustering_key)
                .with_column("value", int32_type);
        builder.set_compaction_strategy(sstables::compaction_strategy_type::incremental);
        constexpr unsigned target_sstable_size_in_mb = 1000;
        std::map <sstring, sstring> opts = {
                {"sstable_size_in_mb", to_sstring(target_sstable_size_in_mb)},
        };
        auto cs = sstables::make_compaction_strategy(sstables::compaction_strategy_type::incremental, opts);
        builder.set_compaction_strategy_options(std::move(opts));
        auto s = builder.build();

        auto tokens = tests::generate_partition_keys(disjoint_sstable_count, s, local_shard_only::yes);

        auto make_row = [&](unsigned token_idx) {
            auto key = tokens[token_idx].key();

            mutation m(s, key);
            auto value = 1;
            auto next_ts = 1;
            auto c_key = clustering_key::from_exploded(*s, {::timestamp_type->decompose(next_ts)});
            m.set_clustered_cell(c_key, bytes("value"), data_value(int32_t(value)), next_ts);
            return m;
        };

        auto tmp = tmpdir();

        auto sst_gen = [&env, s, &tmp]() {
            return env.make_sstable(s, tmp.path().string(), env.new_generation(), sstables::sstable::version_types::md, big);
        };

        {
            unsigned sstable_count = s->max_compaction_threshold() * 2;

            std::vector<sstables::shared_sstable> sstables;
            sstables.reserve(sstable_count);
            for (unsigned i = 0; i < sstable_count; i++) {
                auto sst = make_sstable_containing(sst_gen, {make_row(0)});
                sstables.push_back(std::move(sst));
            }

            auto ret = cs.get_reshaping_job(sstables, s, reshape_config{.mode = reshape_mode::strict});
            BOOST_REQUIRE(ret.sstables.size() == unsigned(s->max_compaction_threshold()));
            BOOST_REQUIRE(ret.max_sstable_bytes == target_sstable_size_in_mb*1024*1024);
        }

        {
            // create set of 256 disjoint ssts and expect that stcs reshape allows them all to be compacted at once

            std::vector<sstables::shared_sstable> sstables;
            sstables.reserve(disjoint_sstable_count);
            for (unsigned i = 0; i < disjoint_sstable_count; i++) {
                auto sst = make_sstable_containing(sst_gen, {make_row(i)});
                sstables.push_back(std::move(sst));
            }

            BOOST_REQUIRE(cs.get_reshaping_job(sstables, s, reshape_config{.mode = reshape_mode::strict}).sstables.size() == disjoint_sstable_count);
        }

        {
            // create a single run of 256 sstables and expect that reshape will say there's nothing to do.

            run_id sstable_run_id = run_id::create_random_id();
            std::vector<sstables::shared_sstable> sstables;
            sstables.reserve(disjoint_sstable_count);
            for (unsigned i = 0; i < disjoint_sstable_count; i++) {
                auto sst = make_sstable_containing(sst_gen, {make_row(i)});
                sstables::test(sst).set_run_identifier(sstable_run_id);
                sstables.push_back(std::move(sst));
            }

            BOOST_REQUIRE(cs.get_reshaping_job(sstables, s, reshape_config{.mode = reshape_mode::strict}).sstables.size() == 0);
        }

        {
            // create set of 256 overlapping ssts and expect that stcs reshape allows only 32 to be compacted at once

            std::vector<sstables::shared_sstable> sstables;
            sstables.reserve(disjoint_sstable_count);
            for (unsigned i = 0; i < disjoint_sstable_count; i++) {
                auto sst = make_sstable_containing(sst_gen, {make_row(0)});
                sstables.push_back(std::move(sst));
            }

            BOOST_REQUIRE(cs.get_reshaping_job(sstables, s, reshape_config{.mode = reshape_mode::strict}).sstables.size() == uint64_t(s->max_compaction_threshold()));
        }
    });
}

SEASTAR_TEST_CASE(gc_tombstone_with_grace_seconds_test) {
    return test_env::do_with_async([](test_env &env) {
        auto gc_grace_seconds = 5;
        auto schema = schema_builder("tests", "gc_tombstone_with_grace_seconds_test")
                .with_column("id", utf8_type, column_kind::partition_key)
                .with_column("value", byte_type)
                .set_gc_grace_seconds(gc_grace_seconds).build();
        auto sst_factory = env.make_sst_factory(schema);

        auto now = gc_clock::now();
        // set the expiration time to (now - gc_grace_seconds), so that the tombstone is GC'able when compaction is run
        auto expiration_time = (now - gc_clock::duration(gc_grace_seconds)).time_since_epoch().count();
        mutation mut(schema, tests::generate_partition_key(schema, local_shard_only::yes));
        auto live_cell = atomic_cell::make_live(*byte_type, 0, to_bytes("a"), gc_clock::time_point(gc_clock::duration(expiration_time)), gc_clock::duration(1));
        mut.set_clustered_cell(clustering_key::make_empty(), *schema->get_column_definition("value"), std::move(live_cell));
        auto sst = make_sstable_containing(env.make_sst_factory(schema), {mut});

        table_for_tests cf = env.make_table_for_tests(schema);
        auto close_cf = deferred_stop(cf);
        column_family_test(cf).add_sstable(sst).get();

        std::map<sstring, sstring> options;
        // reduce tombstone_compaction_interval to make droppable data old enough for GC.
        options.emplace("tombstone_compaction_interval", "1");
        forward_jump_clocks(std::chrono::seconds{1});
        auto control = make_strategy_control_for_test(false);
        auto cs = sstables::make_compaction_strategy(sstables::compaction_strategy_type::incremental, options);
        auto descriptor = cs.get_sstables_for_compaction(cf.as_table_state(), *control);
        BOOST_REQUIRE_EQUAL(descriptor.sstables.size(), 1);
        BOOST_REQUIRE_EQUAL(descriptor.sstables.front(), sst);
    });
}
