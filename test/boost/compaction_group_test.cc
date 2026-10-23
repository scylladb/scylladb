/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <cmath>
#include <limits>

#include <seastar/core/sstring.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/aligned_buffer.hh>
#include <seastar/core/sleep.hh>
#include <seastar/util/closeable.hh>
#include <seastar/util/defer.hh>

#include "test/lib/scylla_test_case.hh"
#include "test/lib/test_services.hh"
#include "test/lib/reader_concurrency_semaphore.hh"
#include "test/lib/sstable_utils.hh"
#include "test/lib/random_utils.hh"
#include "test/lib/key_utils.hh"
#include "test/lib/eventually.hh"

#include "clocks-impl.hh"

#include "schema/schema.hh"
#include "schema/schema_builder.hh"

#include "sstables/sstables.hh"
#include "sstables/compress.hh"
#include "compaction/compaction.hh"
#include "compaction/compaction_manager.hh"
#include "replica/compaction_group.hh"

using namespace sstables;

static sstables::shared_sstable generate_sstable(schema_ptr s, std::function<shared_sstable()> sst_gen, noncopyable_function<bool(dht::token)> token_filter) {
    auto make_insert = [&] (const dht::decorated_key& key) {
        static thread_local int32_t value = 1;

        mutation m(s, key);
        auto c_key = clustering_key::from_exploded(*s, {int32_type->decompose(value++)});
        m.set_clustered_cell(c_key, bytes("value"), data_value(int32_t(value)), api::timestamp_clock::now().time_since_epoch().count());
        return m;
    };

    auto keys = tests::generate_partition_keys(100, s);
    utils::chunked_vector<mutation> muts;

    muts.reserve(keys.size());
    for (auto& k : keys) {
        if (token_filter(k.token())) {
            muts.push_back(make_insert(k));
        }
    }
    return make_sstable_containing(sst_gen, std::move(muts)).get();
}

static sstables::shared_sstable sstable_that_needs_split(schema_ptr s, std::function<shared_sstable()> sst_gen) {
    return generate_sstable(std::move(s), std::move(sst_gen), [] (dht::token) { return true; });
}

class single_compaction_group : public compaction::compaction_group_view {
private:
    schema_ptr _schema;
    sstables::sstables_manager& _sst_man;
    sstables::sstable_set _main_set;
    sstables::sstable_set _maintenance_set;
    std::vector<sstables::shared_sstable> _compacted_undeleted_sstables;
    mutable compaction::compaction_strategy _compaction_strategy;
    compaction::compaction_strategy_state _compaction_strategy_state;
    tombstone_gc_state _tombstone_gc_state;
    compaction::compaction_backlog_tracker _backlog_tracker;
    condition_variable _staging_done_condition;
    std::function<shared_sstable()> _sstable_factory;
    mutable tests::reader_concurrency_semaphore_wrapper _semaphore;
public:
    single_compaction_group(table_for_tests& t, sstables::sstables_manager& sst_man, std::function<shared_sstable()> sstable_factory)
            : _schema(t.schema())
            , _sst_man(sst_man)
            , _main_set(sstables::make_partitioned_sstable_set(_schema))
            , _maintenance_set(sstables::make_partitioned_sstable_set(_schema))
            , _compaction_strategy(compaction::make_compaction_strategy(_schema->compaction_strategy(), _schema->compaction_strategy_options()))
            , _compaction_strategy_state(compaction::compaction_strategy_state::make(_compaction_strategy))
            , _tombstone_gc_state(tombstone_gc_state::for_tests())
            , _backlog_tracker(_compaction_strategy.make_backlog_tracker())
            , _sstable_factory(std::move(sstable_factory))
    {
        t->get_compaction_manager().add(*this);
    }

    future<> stop(table_for_tests& t) {
        return t->get_compaction_manager().remove(*this);
    }

    void rebuild_main_set(std::vector<shared_sstable> to_add, std::vector<shared_sstable> to_remove) {
        for (auto& sst : to_remove) {
            _main_set.erase(sst);
        }
        for (auto& sst : to_add) {
            _main_set.insert(sst);
        }
    }

    virtual dht::token_range token_range() const noexcept override { return dht::token_range::make(dht::first_token(), dht::last_token()); }
    virtual const schema_ptr& schema() const noexcept override { return _schema; }
    virtual unsigned min_compaction_threshold() const noexcept override { return _schema->min_compaction_threshold(); }
    virtual std::optional<std::chrono::seconds> scrub_period() const noexcept override { return std::nullopt; }
    virtual bool compaction_enforce_min_threshold() const noexcept override { return false; }
    virtual future<lw_shared_ptr<const sstables::sstable_set>> main_sstable_set() const override { co_return make_lw_shared<const sstables::sstable_set>(_main_set); }
    virtual future<lw_shared_ptr<const sstables::sstable_set>> maintenance_sstable_set() const override { co_return make_lw_shared<const sstables::sstable_set>(_maintenance_set); }
    virtual lw_shared_ptr<const sstables::sstable_set> sstable_set_for_tombstone_gc(const ::schema&) const override { return make_lw_shared<const sstables::sstable_set>(_main_set); }
    virtual bool skip_memtable_for_tombstone_gc(const ::schema&) const noexcept override { return false; }
    virtual std::unordered_set<sstables::shared_sstable> fully_expired_sstables(const std::vector<sstables::shared_sstable>& sstables, gc_clock::time_point compaction_time) const override { return {}; }
    virtual const std::vector<sstables::shared_sstable>& compacted_undeleted_sstables() const noexcept override { return _compacted_undeleted_sstables; }
    virtual compaction::compaction_strategy& get_compaction_strategy() const noexcept override { return _compaction_strategy; }
    virtual compaction::compaction_strategy_state& get_compaction_strategy_state() noexcept override { return _compaction_strategy_state; }
    virtual reader_permit make_compaction_reader_permit() const override { return _semaphore.make_permit(); }
    virtual sstables::sstables_manager& get_sstables_manager() noexcept override { return _sst_man; }
    virtual sstables::shared_sstable make_sstable(sstables::sstable_state) const override { return _sstable_factory(); }
    virtual sstables::shared_sstable make_sstable(sstables::sstable_state, sstables::sstable_version_types) const override { return _sstable_factory(); }
    virtual sstables::sstable_writer_config configure_writer(sstring origin) const override { return _sst_man.configure_writer(std::move(origin)); }
    virtual api::timestamp_type min_memtable_timestamp() const override { return api::min_timestamp; }
    virtual api::timestamp_type min_memtable_live_timestamp() const override { return api::min_timestamp; }
    virtual api::timestamp_type min_memtable_live_row_marker_timestamp() const override { return api::min_timestamp; }
    virtual bool memtable_has_key(const dht::decorated_key& key) const override { return false; }
    virtual future<> on_compaction_completion(compaction::compaction_completion_desc desc, sstables::offstrategy offstrategy) override {
        testlog.info("Adding {} sstable(s), removing {} sstables", desc.new_sstables.size(), desc.old_sstables.size());
        rebuild_main_set(desc.new_sstables, desc.old_sstables);
        return make_ready_future<>();
    }
    virtual bool is_auto_compaction_disabled_by_user() const noexcept override { return false; }
    virtual bool tombstone_gc_enabled() const noexcept override { return false; }
    virtual tombstone_gc_state get_tombstone_gc_state() const noexcept override { return _tombstone_gc_state; }
    virtual compaction::compaction_backlog_tracker& get_backlog_tracker() override { return _backlog_tracker; }
    virtual const std::string get_group_id() const noexcept override { return "0"; }
    void add_compacted_bytes(uint64_t) noexcept override {}
    void add_completed_compaction(uint64_t, uint64_t) noexcept override {}
    virtual seastar::condition_variable& get_staging_done_condition() noexcept override { return _staging_done_condition; }
    dht::token_range get_token_range_after_split(const dht::token& t) const noexcept override { return dht::token_range(); }
    int64_t get_sstables_repaired_at() const noexcept override { return 0; }
};

SEASTAR_TEST_CASE(basic_compaction_group_splitting_test) {
    return test_env::do_with_async([] (test_env& env) {
        auto builder = schema_builder(this_smp_shard_count(), "tests", "compaction_group_splitting")
                .with_column("id", utf8_type, column_kind::partition_key)
                .with_column("cl", int32_type, column_kind::clustering_key)
                .with_column("value", int32_type);
        auto s = builder.build();

        auto t = env.make_table_for_tests(s);
        auto close_table = deferred_stop(t);
        t->start();

        auto sst_factory = env.make_sst_factory(s);
        auto classifier = [] (dht::token t) -> mutation_writer::token_group_id {
            return dht::compaction_group_of(1, t);
        };
        auto sstable_needs_split = [&] (const sstables::shared_sstable& sst) {
            return classifier(sst->get_first_decorated_key().token()) != classifier(sst->get_last_decorated_key().token());
        };

        auto run_test = [&] (std::vector<sstables::shared_sstable> ssts, size_t expected_output, noncopyable_function<void(const sstables::shared_sstable&)> validate) {
            auto compaction_group = std::make_unique<single_compaction_group>(t, env.manager(), sst_factory);

            compaction_group->rebuild_main_set(ssts, {});

            auto& cm = t->get_compaction_manager();
            auto expected_compaction_size = std::ranges::fold_left(ssts | std::views::transform([&] (auto& sst) {
                // sstables that doesn't need split will have compaction bypassed.
                return sstable_needs_split(sst) ? sst->bytes_on_disk() : size_t(0);
            }), int64_t(0), std::plus{});

            auto ret = cm.perform_split_compaction(*compaction_group, compaction::compaction_type_options::split{classifier}, tasks::make_empty_task_info()).get();
            BOOST_REQUIRE_EQUAL(ret->start_size, expected_compaction_size);

            BOOST_REQUIRE(compaction_group->main_sstable_set().get()->size() == expected_output);
            compaction_group->main_sstable_set().get()->for_each_sstable([&] (const sstables::shared_sstable& sst) {
                BOOST_REQUIRE(!sstable_needs_split(sst));
                validate(sst);
            });
            compaction_group->stop(t).get();
        };

        // sstable that needs split case will generate 2 sstables, one for left, another for right.
        {
            auto input = sstable_that_needs_split(s, sst_factory);
            std::unordered_set<mutation_writer::token_group_id> expected_ids { 0, 1 };
            run_test({ input }, 2, [&] (const sstables::shared_sstable& sst) {
                BOOST_REQUIRE(expected_ids.erase(classifier(sst->get_first_decorated_key().token())) == 1);
            });
            BOOST_REQUIRE(expected_ids.empty());
        }
        // sstable that doesn't need split won't actually be compacted
        {
            auto input = generate_sstable(s, sst_factory, [&] (dht::token t) { return classifier(t) == 0; });
            run_test({ input }, 1, [&] (const sstables::shared_sstable& sst) {
                BOOST_REQUIRE(sst->generation() == input->generation());
                BOOST_REQUIRE_EQUAL(0, classifier(sst->get_first_decorated_key().token()));
            });
        }

        // combination of both cases
        {
            auto input1 = sstable_that_needs_split(s, sst_factory);
            auto input2 = generate_sstable(s, sst_factory, [&] (dht::token t) { return classifier(t) == 0; });
            bool found_input2 = false;
            run_test({ input1, input2 }, 3, [&] (const sstables::shared_sstable& sst) {
                found_input2 |= sst->generation() == input2->generation();
            });
            BOOST_REQUIRE(found_input2);
        }
    });
}

static mutation_reader sstable_reader(shared_sstable sst, schema_ptr s, reader_permit permit) {
    return sst->as_mutation_source().make_mutation_reader(s, std::move(permit), query::full_partition_range, s->full_slice());
}

SEASTAR_TEST_CASE(compactions_dont_cross_group_boundary_test) {
    return test_env::do_with_async([] (test_env& env) {
        auto builder = schema_builder(this_smp_shard_count(), "tests", "compactions_dont_cross_group_boundary")
                .with_column("id", utf8_type, column_kind::partition_key)
                .with_column("cl", int32_type, column_kind::clustering_key)
                .with_column("value", int32_type);
        auto s = builder.build();

        auto t = env.make_table_for_tests(s);
        auto close_table = deferred_stop(t);
        t->start();

        // Disable auto compaction to allow us to trigger compaction manually later.
        t->disable_auto_compaction().get();

        auto is_unrepaired = [] (dht::token t) { return t.raw() % 3 == 0; };
        auto is_repairing = [] (dht::token t) { return t.raw() % 3 == 1; };
        auto is_repaired = [] (dht::token t) { return t.raw() % 3 == 2; };

        auto sst_factory = env.make_sst_factory(s);
        auto generate_sstables = [&] (std::function<bool(dht::token)> filter) {
            for (int i = 0; i < 4; i++) {
                t->add_sstable_and_update_cache(generate_sstable(s, sst_factory, filter)).get();
            }
        };
        generate_sstables(is_unrepaired);
        generate_sstables(is_repairing);
        generate_sstables(is_repaired);

        auto repair_token_classifier = [&] (dht::token t) -> replica::repair_sstable_classification {
            if (is_unrepaired(t)) {
                return replica::repair_sstable_classification::unrepaired;
            } else if (is_repairing(t)) {
                return replica::repair_sstable_classification::repairing;
            }
            return replica::repair_sstable_classification::repaired;
        };
        auto repair_sstable_classifier = [&] (const sstables::shared_sstable& sst, int64_t sstables_repaired_at) -> replica::repair_sstable_classification {
            return repair_token_classifier(sst->get_first_decorated_key().token());
        };
        t.set_repair_sstable_classifier(repair_sstable_classifier);

        for (int i = 0; i < 4; i++) {
            t->compact_all_sstables(tasks::make_empty_task_info()).get();
        }

        auto validate_sstable = [&] (const sstables::shared_sstable& sst) {
            auto reader = sstable_reader(sst, s, env.make_reader_permit()); // reader holds sst and s alive.
            auto close_reader = deferred_close(reader);

            auto expected_classification = repair_sstable_classifier(sst, 0);

            while (auto m = read_mutation_from_mutation_reader(reader).get()) {
                BOOST_REQUIRE(repair_token_classifier(m->decorated_key().token()) == expected_classification);
            }
        };
        auto all_sstables = t->get_sstables();
        for (auto& sst : *all_sstables) {
            validate_sstable(sst);
        }
    });
}

static schema_ptr make_rates_test_schema(const char* name) {
    return schema_builder(this_smp_shard_count(), "tests", name)
            .with_column("id", utf8_type, column_kind::partition_key)
            .with_column("cl", int32_type, column_kind::clustering_key)
            .with_column("value", int32_type)
            .build();
}

static mutation make_partition(schema_ptr s, const dht::decorated_key& key, int rows) {
    mutation m(s, key);
    for (int i = 0; i < rows; ++i) {
        auto c_key = clustering_key::from_exploded(*s, {int32_type->decompose(i)});
        m.set_clustered_cell(c_key, bytes("value"), data_value(int32_t(i)), api::new_timestamp());
    }
    return m;
}

// Scheduling groups of their own for regular compaction and for the other kinds, so that
// the compaction manager of the test env tells them apart. Must be made in a thread.
struct compaction_scheduling_groups {
    seastar::scheduling_group compaction = create_scheduling_group("test_compaction", 1000).get();
    seastar::scheduling_group maintenance = create_scheduling_group("test_maintenance", 200).get();

    ~compaction_scheduling_groups() {
        destroy_scheduling_group(maintenance).get();
        destroy_scheduling_group(compaction).get();
    }
    test_env_config config() const {
        return test_env_config{.compaction_scheduling_group = compaction, .maintenance_scheduling_group = maintenance};
    }
};

// The compacted bytes count the input of a regular compaction exactly once: as it runs,
// from its progress, whenever the compaction manager credits it, and when it ends, for
// the remainder.
SEASTAR_THREAD_TEST_CASE(compaction_group_credits_regular_compaction_input_once_test) {
    compaction_scheduling_groups sgs;
    test_env::do_with_async([] (test_env& env) {
        auto s = make_rates_test_schema("compaction_group_credits");
        auto t = env.make_table_for_tests(s);
        auto close_table = deferred_stop(t);
        t->start();
        t->disable_auto_compaction().get();
        auto rates = [&] { return t.as_compaction_backlog_source().rates(); };

        // Large enough for the compaction to report progress many times as it reads.
        for (const auto& key : tests::generate_partition_keys(4, s)) {
            t->apply(make_partition(s, key, 50000));
            t->flush().get();
        }
        BOOST_REQUIRE_EQUAL(t->sstables_count(), 4);
        uint64_t input_data_size = 0;
        for (const auto& sst : *t->get_sstables()) {
            input_data_size += sst->data_size();
        }

        // Credits the compaction in flight, as the compaction controller will, until it ends.
        auto& cm = t->get_compaction_manager();
        uint64_t credited_in_flight = 0;
        t->enable_auto_compaction();
        t->trigger_compaction();
        while (t->sstables_count() != 1) {
            credited_in_flight += cm.credit_compaction_progress();
            seastar::sleep(std::chrono::milliseconds(1)).get();
        }
        // It credits the rest of its input once it ends.
        cm.await_ongoing_compactions(nullptr).get();
        BOOST_REQUIRE_GT(credited_in_flight, 0);
        BOOST_REQUIRE_LE(credited_in_flight, input_data_size);
        BOOST_REQUIRE_EQUAL(rates().completed_input_bytes.amount(), input_data_size);
        BOOST_REQUIRE_EQUAL(rates().compacted_bytes.amount(), input_data_size);
        // Nothing is left to credit once it ended.
        BOOST_REQUIRE_EQUAL(cm.credit_compaction_progress(), 0);
        BOOST_REQUIRE_EQUAL(rates().compacted_bytes.amount(), input_data_size);
    }, sgs.config()).get();
}

// Whether the sstable was written by regular compaction, rather than by another kind, like
// major, or by a memtable flush.
static bool has_regular_compaction_origin(const sstables::shared_sstable& sst) {
    return sst->get_origin() == "compaction";
}

// A compaction group tracks the rates of its memtable flushes and regular compactions,
// and derives the average flush size, weighted by size, from them.
SEASTAR_TEST_CASE(compaction_group_rates_test) {
    return test_env::do_with_async([] (test_env& env) {
        auto s = make_rates_test_schema("compaction_group_rates");
        auto t = env.make_table_for_tests(s);
        auto close_table = deferred_stop(t);
        t->start();
        // Nothing gets compacted until the test says so.
        t->disable_auto_compaction().get();
        auto rates = [&] { return t.as_compaction_backlog_source().rates(); };

        auto initial = rates();
        BOOST_REQUIRE_EQUAL(initial.flushed_bytes.amount(), 0);
        BOOST_REQUIRE_EQUAL(initial.mean_flush_size, 0);
        BOOST_REQUIRE_EQUAL(initial.compacted_bytes.amount(), 0);
        BOOST_REQUIRE_EQUAL(initial.compacted_bytes_long.amount(), 0);

        // Flushes of different sizes, close enough to be compacted together later.
        auto keys = tests::generate_partition_keys(3, s);
        const int rows[] = {100, 120, 140};
        for (int i = 0; i < 3; ++i) {
            t->apply(make_partition(s, keys[i], rows[i]));
            t->flush().get();
        }
        BOOST_REQUIRE_EQUAL(t->sstables_count(), 3);

        // Rates are measured over time, so let some pass.
        BOOST_REQUIRE(eventually_true([&] { return rates().flushed_bytes.per_second() > 0; }));
        double bytes = 0;
        double squared_bytes = 0;
        for (const auto& sst : *t->get_sstables()) {
            bytes += sst->data_size();
            squared_bytes += double(sst->data_size()) * sst->data_size();
        }
        auto after_flushes = rates();
        BOOST_REQUIRE_EQUAL(after_flushes.flushed_bytes.amount(), bytes);
        BOOST_REQUIRE_CLOSE(after_flushes.mean_flush_size, squared_bytes / bytes, 1e-6);
        BOOST_REQUIRE_EQUAL(after_flushes.compacted_bytes.amount(), 0);
        // All rates cover the same interval, so the controller can compare them.
        BOOST_REQUIRE(after_flushes.compacted_bytes.interval() == after_flushes.flushed_bytes.interval());
        BOOST_REQUIRE(after_flushes.compacted_bytes_long.interval() == after_flushes.flushed_bytes.interval());

        // Regular compaction counts.
        t->enable_auto_compaction();
        t->trigger_compaction();
        BOOST_REQUIRE(eventually_true([&] { return rates().compacted_bytes.per_second() > 0; }));
        // Both windows still cover everything since the table started.
        auto compacted = rates();
        BOOST_REQUIRE_EQUAL(compacted.compacted_bytes_long.amount(), compacted.compacted_bytes.amount());
        // A compaction that ended reports what it read and wrote.
        BOOST_REQUIRE(eventually_true([&] { return rates().completed_input_bytes.amount() > 0; }));
        BOOST_REQUIRE_GT(rates().completed_output_bytes.amount(), 0);
        BOOST_REQUIRE(std::ranges::any_of(*t->get_sstables(), has_regular_compaction_origin));
    });
}

// Only regular compaction, the one the compaction controller drives, counts toward the
// compaction rate; other kinds, like major, run in the maintenance scheduling group.
SEASTAR_THREAD_TEST_CASE(compaction_group_rates_ignore_non_regular_compaction_test) {
    compaction_scheduling_groups sgs;
    test_env::do_with_async([] (test_env& env) {
        auto s = make_rates_test_schema("compaction_group_rates_ignore_major");
        auto t = env.make_table_for_tests(s);
        auto close_table = deferred_stop(t);
        t->start();
        // Keeps regular compaction from compacting the flushed sstables before major does.
        t->disable_auto_compaction().get();
        auto rates = [&] { return t.as_compaction_backlog_source().rates(); };

        auto keys = tests::generate_partition_keys(2, s);
        for (const auto& key : keys) {
            t->apply(make_partition(s, key, 10));
            t->flush().get();
        }
        t->compact_all_sstables(tasks::make_empty_task_info(), replica::table::do_flush::no).get();
        BOOST_REQUIRE_EQUAL(t->sstables_count(), 1);
        BOOST_REQUIRE(!has_regular_compaction_origin(*t->get_sstables()->begin()));

        BOOST_REQUIRE(eventually_true([&] { return rates().flushed_bytes.per_second() > 0; }));
        BOOST_REQUIRE_EQUAL(rates().compacted_bytes.amount(), 0);
    }, sgs.config()).get();
}

static int64_t to_milliseconds(db_clock::time_point tp) {
    return std::chrono::duration_cast<std::chrono::milliseconds>(tp.time_since_epoch()).count();
}

// A memtable flush records that all of the sstable's data was ingested, from the
// memtable's creation to the flush.
SEASTAR_TEST_CASE(memtable_flush_records_ingestion_metadata_test) {
    return test_env::do_with_async([] (test_env& env) {
        auto s = make_rates_test_schema("memtable_flush_ingestion_metadata");
        auto before = to_milliseconds(db_clock::now());
        auto t = env.make_table_for_tests(s);
        auto close_table = deferred_stop(t);
        t->start();
        t->disable_auto_compaction().get();

        for (const auto& key : tests::generate_partition_keys(2, s)) {
            t->apply(make_partition(s, key, 10));
            t->flush().get();
        }
        auto after = to_milliseconds(db_clock::now());

        BOOST_REQUIRE_EQUAL(t->sstables_count(), 2);
        for (const auto& sst : *t->get_sstables()) {
            auto ingestion = sst->get_ingestion_metadata();
            BOOST_REQUIRE(ingestion);
            BOOST_REQUIRE_EQUAL(ingestion->fraction, 1.0);
            BOOST_REQUIRE_LE(before, ingestion->ingested_from);
            BOOST_REQUIRE_LE(ingestion->ingested_from, ingestion->ingested_until);
            BOOST_REQUIRE_LE(ingestion->ingested_until, after);
        }
    });
}

// Compaction writes to every output the share of ingested bytes among its inputs,
// weighted by their sizes, and the period spanning theirs; inputs without ingestion
// metadata count as not ingested.
SEASTAR_TEST_CASE(compaction_propagates_ingestion_metadata_test) {
    return test_env::do_with_async([] (test_env& env) {
        auto s = make_rates_test_schema("compaction_ingestion_metadata");
        auto t = env.make_table_for_tests(s);
        auto close_table = deferred_stop(t);
        t->start();
        auto sst_gen = env.make_sst_factory(s);

        auto keys = tests::generate_partition_keys(30, s);
        auto make_input = [&] (size_t first_key, int rows, std::optional<sstables::ingestion_metadata> ingestion) {
            utils::chunked_vector<mutation> muts;
            for (size_t i = first_key; i < first_key + 10; ++i) {
                muts.push_back(make_partition(s, keys[i], rows));
            }
            auto cfg = env.manager().configure_writer();
            cfg.ingestion = ingestion;
            return make_sstable_easy(env, make_memtable(s, muts).get(), cfg);
        };
        auto flushed = make_input(0, 10, sstables::ingestion_metadata{.fraction = 1.0, .ingested_from = 1000, .ingested_until = 2000});
        auto compacted = make_input(10, 20, sstables::ingestion_metadata{.fraction = 0.5, .ingested_from = 1500, .ingested_until = 3000});
        auto repaired = make_input(20, 30, std::nullopt);

        // A small maximum sstable size makes the compaction write several outputs.
        auto compact = [&] (std::vector<sstables::shared_sstable> input) {
            constexpr uint64_t max_sstable_bytes = 1;
            auto desc = compaction::compaction_descriptor(std::move(input), compaction::compaction_descriptor::default_level, max_sstable_bytes);
            return compact_sstables(env, std::move(desc), t, sst_gen).get().new_sstables;
        };

        auto output = compact({flushed, compacted, repaired});
        BOOST_REQUIRE_GT(output.size(), 1);
        double input_bytes = flushed->data_size() + compacted->data_size() + repaired->data_size();
        auto expected_fraction = (1.0 * flushed->data_size() + 0.5 * compacted->data_size()) / input_bytes;
        for (const auto& sst : output) {
            auto ingestion = sst->get_ingestion_metadata();
            BOOST_REQUIRE(ingestion);
            BOOST_REQUIRE_CLOSE(ingestion->fraction, expected_fraction, 1e-6);
            BOOST_REQUIRE_EQUAL(ingestion->ingested_from, 1000);
            BOOST_REQUIRE_EQUAL(ingestion->ingested_until, 3000);
        }

        // Without any ingested input, the output has no ingestion metadata.
        for (const auto& sst : compact({make_input(20, 30, std::nullopt)})) {
            BOOST_REQUIRE(!sst->get_ingestion_metadata());
        }
    });
}

// The flush rate a compaction group reports, from its moving average, is close to the
// rate derived from the ingestion metadata of its sstables, which is what the controller
// can rely on when the moving average doesn't cover the data, e.g. after a restart. That
// holds through rewrites: a major compaction keeps the period its data was ingested over,
// so the rate derived from its output doesn't change.
SEASTAR_TEST_CASE(flush_rate_matches_ingestion_metadata_test) {
    return test_env::do_with_async([] (test_env& env) {
        auto s = schema_builder(this_smp_shard_count(), "tests", "flush_rate_matches_ingestion_metadata")
                .with_column("id", utf8_type, column_kind::partition_key)
                .with_column("cl", int32_type, column_kind::clustering_key)
                .with_column("value", bytes_type)
                .build();
        auto t = env.make_table_for_tests(s);
        auto close_table = deferred_stop(t);
        t->start();
        t->disable_auto_compaction().get();

        // Flush at a steady pace, moving the clock forward between flushes rather than
        // waiting. The jumps move both lowres_db_clock, which the rates use, and db_clock,
        // which the ingestion metadata uses. The offset is process-wide, so it's restored
        // for the other tests.
        auto restore_clocks = defer([offset = get_clocks_offset()] () noexcept { set_clocks_offset(offset); });
        constexpr int flushes_per_batch = 20;
        auto keys = tests::generate_partition_keys(2 * flushes_per_batch, s);
        size_t next_key = 0;
        auto flush_batch = [&] {
            for (int i = 0; i < flushes_per_batch; ++i) {
                if (next_key) {
                    forward_jump_clocks(std::chrono::seconds(10));
                }
                // Values of 1KiB, so that the per-row overhead, which a compaction can encode
                // differently than the flush did, e.g. timestamps as deltas from a different
                // minimum, is a small share of the bytes.
                mutation m(s, keys[next_key++]);
                for (int row = 0; row < 100; ++row) {
                    auto c_key = clustering_key::from_exploded(*s, {int32_type->decompose(row)});
                    m.set_clustered_cell(c_key, bytes("value"), data_value(bytes(1024, 'x')), api::new_timestamp());
                }
                t->apply(m);
                t->flush().get();
            }
        };

        struct derived_ingestion {
            double rate;
            int64_t ingested_from;
            int64_t ingested_until;
        };
        auto derive_from_sstables = [&] {
            double ingested_bytes = 0;
            auto ingested_from = std::numeric_limits<int64_t>::max();
            auto ingested_until = std::numeric_limits<int64_t>::min();
            for (const auto& sst : *t->get_sstables()) {
                auto ingestion = sst->get_ingestion_metadata();
                BOOST_REQUIRE(ingestion);
                ingested_bytes += ingestion->fraction * sst->data_size();
                ingested_from = std::min(ingested_from, ingestion->ingested_from);
                ingested_until = std::max(ingested_until, ingestion->ingested_until);
            }
            return derived_ingestion{ingested_bytes / ((ingested_until - ingested_from) / 1000.0), ingested_from, ingested_until};
        };

        // Both cover the time up to the last flush: the moving average from when the group was
        // created, the ingestion metadata from when its first memtable was, right after. They
        // differ only by the real time the test takes.
        auto check_rates_match = [&] (std::string_view when) {
            auto reported = t.as_compaction_backlog_source().rates().flushed_bytes.per_second();
            auto derived = derive_from_sstables().rate;
            testlog.info("{}: reported flush rate {} bytes/s, derived from ingestion metadata {} bytes/s", when, reported, derived);
            BOOST_REQUIRE_CLOSE(reported, derived, 1);
        };

        flush_batch();
        check_rates_match("after the first flushes");
        auto before_major = derive_from_sstables();

        // Major compaction compacts the flushed sstables into one, which keeps the period their
        // data was ingested over.
        t->compact_all_sstables(tasks::make_empty_task_info(), replica::table::do_flush::no).get();
        BOOST_REQUIRE_EQUAL(t->sstables_count(), 1);
        auto major_output = (*t->get_sstables()->begin())->get_ingestion_metadata();
        BOOST_REQUIRE(major_output);
        BOOST_REQUIRE_EQUAL(major_output->fraction, 1.0);
        BOOST_REQUIRE_EQUAL(major_output->ingested_from, before_major.ingested_from);
        BOOST_REQUIRE_EQUAL(major_output->ingested_until, before_major.ingested_until);
        check_rates_match("after major compaction");

        flush_batch();
        check_rates_match("after more flushes");
    });
}
