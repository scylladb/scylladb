/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <seastar/testing/thread_test_case.hh>
#include <seastar/testing/test_case.hh>

#include "sstables/sstable_writer.hh"
#include "test/lib/error_injection.hh"
#include "test/lib/random_schema.hh"
#include "test/lib/sstable_test_env.hh"
#include "test/lib/sstable_utils.hh"
#include "test/lib/random_utils.hh"

namespace {

static future<std::vector<sstables::shared_sstable>> get_all_sstables(compaction::compaction_group_view& t) {
    auto main_set = co_await t.main_sstable_set();
    auto maintenance_set = co_await t.maintenance_sstable_set();
    auto s = *main_set->all() | std::ranges::to<std::vector>();
    auto maintenance_sstables = maintenance_set->all();
    s.insert(s.end(), maintenance_sstables->begin(), maintenance_sstables->end());
    co_return s;
}

class automatic_scrub_test_framework {
    std::unique_ptr<sstable_compressor_factory> scf = make_sstable_compressor_factory_for_tests_in_thread();
    sharded<test_env> _env;
    tests::random_schema_specification::compress_sstable _compress;
    size_t _schema_counter;
public:
    automatic_scrub_test_framework(tests::random_schema_specification::compress_sstable compress)
        : _compress(compress)
        , _schema_counter(0)
    {
        _env.start(test_env_config(), std::ref(*scf)).get();
    }

    ~automatic_scrub_test_framework() {
        _env.stop().get();
    }

    test_env& env() { return _env.local(); }
private:
    tests::random_schema make_random_schema(uint32_t seed) {
        auto spec = tests::make_random_schema_specification(
            "automatic_scrub_test_framework_" + fmt::to_string(_schema_counter++),
            std::uniform_int_distribution<size_t>(2, 4),
            std::uniform_int_distribution<size_t>(2, 4),
            std::uniform_int_distribution<size_t>(2, 8),
            std::uniform_int_distribution<size_t>(2, 8),
            _compress
        );
        return {seed, *spec};
    }

    shared_sstable make_sstable(tests::random_schema& random_schema, uint32_t seed) {
        auto muts = tests::generate_random_mutations(
                random_schema,
                tests::uncompactible_timestamp_generator(seed),
                tests::no_expiry_expiry_generator(),
                std::uniform_int_distribution<size_t>(10, 10)).get();

        auto sst = make_sstable_containing(env().make_sstable(random_schema.schema()), std::move(muts)).get();
        return sst;
    }

    table_for_tests make_table(size_t sst_count) {
        auto seed = tests::random::get_int<uint32_t>();
        tests::random_schema random_schema = make_random_schema(seed);
        testlog.info("random_schema: {}", random_schema.cql());
        auto table = env().make_table_for_tests(random_schema.schema());
        table->start();
        table->disable_auto_compaction().get();

        for (size_t i = 0; i < sst_count; i++) {
            auto sst = make_sstable(random_schema, seed);
            table->add_sstable_and_update_cache(sst, offstrategy::no).get();
        }

        return table;
    }
public:
    using test_func = std::function<void(table_for_tests&, compaction::compaction_group_view&, std::vector<sstables::shared_sstable>)>;
    void run(size_t sst_count, test_func func) {
        run_with_many_tables(1, sst_count, [func = std::move(func)] (std::span<table_for_tests> tables, std::span<compaction::compaction_group_view*> views, std::span<std::vector<sstables::shared_sstable>> sstables) {
            func(tables.front(), *(views.front()), sstables.front());
        });
    }
    using many_tablets_test_func = std::function<void(std::span<table_for_tests>, std::span<compaction::compaction_group_view*>, std::span<std::vector<sstables::shared_sstable>>)>;
    void run_with_many_tables(size_t table_count, size_t sst_count, many_tablets_test_func func) {
#ifndef SCYLLA_ENABLE_ERROR_INJECTION
        fmt::print("Skipping test as it depends on error injection. Please run in mode where it's enabled (debug,dev).\n");
        return;
#endif
        std::vector<table_for_tests> tables;
        std::vector<compaction::compaction_group_view*> views;
        std::vector<std::vector<sstables::shared_sstable>> sstables;
        std::vector<deferred_stop<table_for_tests>> stops;

        for (size_t i = 0; i < table_count; i++) {
            tables.emplace_back(make_table(sst_count));
            views.emplace_back(&tables.back().as_compaction_group_view());
            sstables.emplace_back(get_all_sstables(*views.back()).get());
        }

        for (auto& table : tables) {
            stops.emplace_back(table);
        }

        scoped_no_abort_on_malformed_sstable_error no_abort{};
        scoped_error_injection validation_injection{"automatic_scrub_compaction_done"};
        scoped_error_injection wait_injection{"automatic_scrub_wait_for_signal"};
        scoped_error_injection found_mismatch_injection{"compaction_regular_compaction_digest_mismatch_found"};
        scoped_error_injection validation_done_injection{"compaction_regular_compaction_validation_done"};

        func(tables, views, sstables);
    }
};

future<> wait_on_enter(std::string_view name, size_t count = 1) {
    auto& injector = utils::get_local_injector();
    constexpr auto sleep_duration = std::chrono::milliseconds(100);
    while (injector.enter_count(name) < count) {
        co_await sleep(sleep_duration);
    }
}

void set_scrub_time(shared_sstable sst, db_clock::time_point timestamp) {
    auto metadata_opt = sstables::test(sst)._scylla_metadata();
    if (metadata_opt) {
        (*metadata_opt)->set_scrub_time(timestamp);
    }
}

void scylla_digests_are_superset(const sstables::scylla_metadata::components_digests& lhs, const sstables::scylla_metadata::components_digests& rhs) {
    for (const auto& [component, digest] : lhs.map) {
        auto it = rhs.map.find(component);
        BOOST_REQUIRE(it != rhs.map.end());
        BOOST_REQUIRE_EQUAL(digest, it->second);
    }
}

static void remove_scylla_component(shared_sstable sst) {
    auto test_sst = sstables::test(sst);
    test_sst.remove_component(component_type::Scylla).get();
    test_sst.rewrite_toc_without_component(component_type::Scylla);
}

SEASTAR_THREAD_TEST_CASE(sstable_auto_scrub_corrupted_ssts_with_scylla_test) {
    automatic_scrub_test_framework test(tests::random_schema_specification::compress_sstable::yes);

    auto& test_env = test.env();
    constexpr auto sst_count = 5;

    test.run(sst_count, [&test_env] (table_for_tests& table, compaction::compaction_group_view& ts, std::vector<sstables::shared_sstable> sstables) {
        auto& cm = test_env.test_compaction_manager();

        for (sstables::shared_sstable& sst : sstables) {
            set_scrub_time(sst, db_clock::from_time_t(0));
            slightly_corrupt_sstable(sst);
        }

        cm.set_scrub_period(std::chrono::seconds(3600));
        cm.trigger_auto_scrub_timer();

        wait_on_enter("automatic_scrub_compaction_done", sst_count).get();

        BOOST_REQUIRE_EQUAL(table->get_sstables()->size(), sstables.size());
        for (auto& sst : *table->get_sstables()) {
            BOOST_REQUIRE(sst->is_quarantined());
        }
    });
}

SEASTAR_THREAD_TEST_CASE(sstable_auto_scrub_only_scylla_rewritten) {
    automatic_scrub_test_framework test(tests::random_schema_specification::compress_sstable::yes);

    auto& test_env = test.env();
    constexpr auto sst_count = 1;

    test.run(sst_count, [&test_env] (table_for_tests& table, compaction::compaction_group_view& ts, std::vector<sstables::shared_sstable> sstables) {
        auto& cm = test_env.test_compaction_manager();

        BOOST_REQUIRE_EQUAL(sstables.size(), 1);
        auto& sst = sstables.front();

        set_scrub_time(sst, db_clock::from_time_t(0));

        BOOST_REQUIRE(sst->get_scylla_metadata());
        auto pre_scrub_digests = sst->get_scylla_metadata()->get_components_digests();
        BOOST_REQUIRE(pre_scrub_digests);
        auto pre_scrub = *pre_scrub_digests;

        // Enable auto scrub.
        cm.set_scrub_period(std::chrono::seconds(3600));

        cm.trigger_auto_scrub_timer();

        wait_on_enter("automatic_scrub_compaction_done", sst_count).get();

        BOOST_REQUIRE_EQUAL(table->get_sstables()->size(), sstables.size());
        auto new_sst = *table->get_sstables()->begin();

        BOOST_REQUIRE(new_sst->get_scylla_metadata());
        auto post_scrub_digests = new_sst->get_scylla_metadata()->get_components_digests();
        BOOST_REQUIRE(post_scrub_digests);

        // Component digest are the same and the new sst components match the digests.
        scylla_digests_are_superset(pre_scrub, *post_scrub_digests);
        auto validation_result = sstables::validate_checksums_and_digests(new_sst, test_env.make_reader_permit()).get();
        BOOST_REQUIRE(validation_result.status == sstables::validate_checksums_status::valid);
    });
}

SEASTAR_THREAD_TEST_CASE(sstable_auto_scrub_scrub_time_updated_with_scylla) {
    automatic_scrub_test_framework test(tests::random_schema_specification::compress_sstable::yes);

    auto& test_env = test.env();
    constexpr auto sst_count = 5;

    test.run(sst_count, [&test_env] (table_for_tests& table, compaction::compaction_group_view& ts, std::vector<sstables::shared_sstable> sstables) {
        auto& cm = test_env.test_compaction_manager();

        for (sstables::shared_sstable& sst : sstables) {
            set_scrub_time(sst, db_clock::from_time_t(0));
        }

        cm.set_scrub_period(std::chrono::seconds(3600));

        auto timestamp_before = db_clock::now();
        cm.trigger_auto_scrub_timer();

        wait_on_enter("automatic_scrub_compaction_done", sst_count).get();

        BOOST_REQUIRE_EQUAL(table->get_sstables()->size(), sstables.size());
        for (auto& sst : *table->get_sstables()) {
            BOOST_REQUIRE(sst->get_scrub_time() > timestamp_before);
        }
    });
}

SEASTAR_THREAD_TEST_CASE(sstable_auto_scrub_scrub_time_updated_without_scylla) {
    automatic_scrub_test_framework test(tests::random_schema_specification::compress_sstable::yes);

    auto& test_env = test.env();
    constexpr auto sst_count = 5;

    test.run(sst_count, [&test_env] (table_for_tests& table, compaction::compaction_group_view& ts, std::vector<sstables::shared_sstable> sstables) {
        auto& cm = test_env.test_compaction_manager();

        for (sstables::shared_sstable& sst : sstables) {
            set_scrub_time(sst, db_clock::from_time_t(0));
            sstables::test(sst).rewrite_toc_without_component(component_type::Scylla);
        }

        cm.set_scrub_period(std::chrono::seconds(3600));

        auto timestamp_before = db_clock::now();
        cm.trigger_auto_scrub_timer();

        wait_on_enter("automatic_scrub_compaction_done", sst_count).get();

        BOOST_REQUIRE_EQUAL(table->get_sstables()->size(), sstables.size());
        for (auto& sst : *table->get_sstables()) {
            BOOST_REQUIRE(sst->has_scylla_component());
            BOOST_REQUIRE(sst->get_scrub_time() > timestamp_before);
        }
    });
}

SEASTAR_THREAD_TEST_CASE(sstable_auto_scrub_scrub_time_updated_mixed) {
    automatic_scrub_test_framework test(tests::random_schema_specification::compress_sstable::yes);

    auto& test_env = test.env();
    constexpr auto sst_count = 5;

    test.run(sst_count, [&test_env] (table_for_tests& table, compaction::compaction_group_view& ts, std::vector<sstables::shared_sstable> sstables) {
        auto& cm = test_env.test_compaction_manager();

        for (sstables::shared_sstable& sst : sstables) {
            set_scrub_time(sst, db_clock::from_time_t(0));
        }

        for (size_t i = 0; i < 3; i++) {
            auto& sst = sstables[i];
            sstables::test(sst).rewrite_toc_without_component(component_type::Scylla);
        }

        cm.set_scrub_period(std::chrono::seconds(3600));

        auto timestamp_before = db_clock::now();
        cm.trigger_auto_scrub_timer();

        wait_on_enter("automatic_scrub_compaction_done", sst_count).get();

        BOOST_REQUIRE_EQUAL(table->get_sstables()->size(), sstables.size());
        for (auto& sst : *table->get_sstables()) {
            BOOST_REQUIRE(sst->has_scylla_component());
            BOOST_REQUIRE(sst->get_scrub_time() > timestamp_before);
        }
    });
}

SEASTAR_THREAD_TEST_CASE(sstable_auto_scrub_skips_validated_sstables_test) {
    automatic_scrub_test_framework test(tests::random_schema_specification::compress_sstable::yes);

    auto& test_env = test.env();
    constexpr auto sst_count = 5;

    test.run(sst_count, [&test_env] (table_for_tests& table, compaction::compaction_group_view& ts, std::vector<sstables::shared_sstable> sstables) {
        auto& cm = test_env.test_compaction_manager();

        std::vector<shared_sstable> validated, not_validated;

        for (auto [idx, sst] : std::views::enumerate(sstables)) {
            if (idx < 3) {
                set_scrub_time(sst, db_clock::from_time_t(0));
                not_validated.emplace_back(std::move(sst));
            } else {
                validated.emplace_back(std::move(sst));
            }
        }

        sstables::test(not_validated.front()).rewrite_toc_without_component(component_type::Scylla);

        auto generations_of_validated = validated
            | std::views::transform(std::mem_fn(&sstable::generation))
            | std::ranges::to<std::unordered_set>();

        auto timestamp_before = db_clock::now();

        cm.set_scrub_period(std::chrono::seconds(3600));
        cm.trigger_auto_scrub_timer();

        wait_on_enter("automatic_scrub_compaction_done", not_validated.size()).get();

        BOOST_REQUIRE_EQUAL(table->get_sstables()->size(), sstables.size());

        auto sstables_after = table->get_sstables();
        size_t newly_validated = 0;

        for (const auto& sst : *sstables_after) {
            BOOST_REQUIRE(sst->get_scrub_time());
            if (*sst->get_scrub_time() > timestamp_before) {
                newly_validated++;
            } else {
                auto it = generations_of_validated.find(sst->generation());
                BOOST_REQUIRE(it != generations_of_validated.end());
                generations_of_validated.erase(it);
            }
        }

        BOOST_REQUIRE_EQUAL(newly_validated, not_validated.size());
    });
}

SEASTAR_THREAD_TEST_CASE(sstable_auto_scrub_and_ongiong_compaction) {
    automatic_scrub_test_framework test(tests::random_schema_specification::compress_sstable::yes);

    auto& test_env = test.env();
    constexpr auto sst_count = 5;

    test.run(sst_count, [&test_env] (table_for_tests& table, compaction::compaction_group_view& ts, std::vector<sstables::shared_sstable> sstables) {
        auto& cm = test_env.test_compaction_manager();

        for (sstables::shared_sstable& sst : sstables) {
            set_scrub_time(sst, db_clock::from_time_t(0));
        }

        auto timestamp_before = db_clock::now();
        auto signal_waits_before = utils::get_local_injector().enter_count("automatic_scrub_wait_for_signal");

        size_t compacting_sstable_count = 3;
        cm.register_compacting(ts, std::span{sstables}.subspan(0, compacting_sstable_count)).wait();

        cm.set_scrub_period(std::chrono::seconds(3600));
        cm.trigger_auto_scrub_timer();

        wait_on_enter("automatic_scrub_compaction_done", sst_count - compacting_sstable_count).get();

        wait_on_enter("automatic_scrub_wait_for_signal", signal_waits_before + 1).get();

        auto ssts_after_scrub = *table->get_sstables() | std::ranges::to<std::unordered_set>();
        for (auto compacting_sst : sstables | std::views::take(compacting_sstable_count)) {
            // The compacting sstables were not selected by automatic scrub
            BOOST_REQUIRE(ssts_after_scrub.contains(compacting_sst));
        }

        cm.deregister_compacting(ts, std::span{sstables}.subspan(0, compacting_sstable_count)).wait();

        // Deregistration should cause automatic scrub to validate the sstables it missed before.
        wait_on_enter("automatic_scrub_compaction_done", sst_count).get();

        BOOST_REQUIRE_EQUAL(table->get_sstables()->size(), sstables.size());
        for (auto& sst : *table->get_sstables()) {
            auto sst_timestamp = sst->get_scrub_time();
            BOOST_REQUIRE(sst_timestamp);
            BOOST_REQUIRE(*sst_timestamp > timestamp_before);
        }
    });
}

SEASTAR_THREAD_TEST_CASE(sstable_auto_scrub_scrubs_without_timestamp) {
    automatic_scrub_test_framework test(tests::random_schema_specification::compress_sstable::yes);

    auto& test_env = test.env();
    constexpr auto sst_count = 5;

    test.run(sst_count, [&test_env] (table_for_tests& table, compaction::compaction_group_view& ts, std::vector<sstables::shared_sstable> sstables) {
        auto& cm = test_env.test_compaction_manager();

        for (sstables::shared_sstable& sst : sstables) {
            auto metadata_opt = sstables::test(sst)._scylla_metadata();
            BOOST_REQUIRE(metadata_opt);
            auto& metadata = *metadata_opt;
            metadata->data.data.erase(scylla_metadata_type::ScrubTime);
        }

        auto timestamp_before = db_clock::now();

        cm.set_scrub_period(std::chrono::seconds(3600));
        cm.trigger_auto_scrub_timer();

        wait_on_enter("automatic_scrub_compaction_done", sst_count).get();

        BOOST_REQUIRE_EQUAL(table->get_sstables()->size(), sstables.size());
        for (auto& sst : *table->get_sstables()) {
            auto scrub_time = sst->get_scrub_time();
            BOOST_REQUIRE(scrub_time);
            BOOST_REQUIRE(*scrub_time > timestamp_before);
        }
    });
}

SEASTAR_THREAD_TEST_CASE(sstable_auto_scrub_finds_corruption_no_scylla_component) {
    automatic_scrub_test_framework test(tests::random_schema_specification::compress_sstable::yes);

    auto& test_env = test.env();
    constexpr auto sst_count = 5;

    test.run(sst_count, [&test_env] (table_for_tests& table, compaction::compaction_group_view& ts, std::vector<sstables::shared_sstable> sstables) {
        auto& cm = test_env.test_compaction_manager();

        for (sstables::shared_sstable& sst : sstables) {
            remove_scylla_component(sst);
            slightly_corrupt_sstable(sst);
        }

        cm.set_scrub_period(std::chrono::seconds(3600));
        cm.trigger_auto_scrub_timer();

        wait_on_enter("automatic_scrub_compaction_done", sst_count).get();

        BOOST_REQUIRE_EQUAL(table->get_sstables()->size(), sstables.size());
        for (auto& sst : *table->get_sstables()) {
            BOOST_REQUIRE(sst->is_quarantined());
        }
    });
}

SEASTAR_THREAD_TEST_CASE(sstable_auto_validates_all_tables) {
    automatic_scrub_test_framework test(tests::random_schema_specification::compress_sstable::yes);

    auto& test_env = test.env();
    constexpr auto table_count = 5;
    constexpr auto sst_count = 5;

    test.run_with_many_tables(table_count, sst_count, [&test_env] (std::span<table_for_tests> tables, std::span<compaction::compaction_group_view*> views, std::span<std::vector<sstables::shared_sstable>> sstable_sets) {
        auto& cm = test_env.test_compaction_manager();

        for (sstables::shared_sstable& sst : std::views::join(sstable_sets)) {
            set_scrub_time(sst, db_clock::from_time_t(0));
        }

        auto timestamp_before = db_clock::now();

        cm.set_scrub_period(std::chrono::seconds(3600));
        cm.trigger_auto_scrub_timer();

        wait_on_enter("automatic_scrub_compaction_done", sst_count * table_count).get();

        for (auto [table, view, sstables] : std::views::zip(tables, views, sstable_sets)) {
            BOOST_REQUIRE(table->get_sstables());
            BOOST_REQUIRE_EQUAL(table->get_sstables()->size(), sstables.size());
            for (auto& sst : sstables) {
                BOOST_REQUIRE(sst->unlinked_at());
            }
            for (auto& sst : *table->get_sstables()) {
                auto timestamp = sst->get_scrub_time();
                BOOST_REQUIRE(timestamp);
                BOOST_REQUIRE(*timestamp > timestamp_before);
            }
        }
    });
}

// Sstables for which compaction fails should not be retried in the same scrub reevaluation period.
// This could cause a deadlock, when auto-scrub is repeatedly retrying the same sstable.
SEASTAR_THREAD_TEST_CASE(sstable_auto_scrub_skips_if_failed) {
    automatic_scrub_test_framework test(tests::random_schema_specification::compress_sstable::yes);

    auto& test_env = test.env();
    constexpr auto table_count = 2;
    constexpr auto sst_count = 5;

    test.run_with_many_tables(table_count, sst_count, [&test_env] (std::span<table_for_tests> tables, std::span<compaction::compaction_group_view*> views, std::span<std::vector<sstables::shared_sstable>> sstable_sets) {
        auto& cm = test_env.test_compaction_manager();

        for (sstables::shared_sstable& sst : std::views::join(sstable_sets)) {
            set_scrub_time(sst, db_clock::from_time_t(0));
        }

        for (auto& view : views) {
            // Table for tests registers an additional compaction group view
            // for its sstables (additional to the compaction group view
            // ceated by the column family).
            //
            // Remove this compaction group view from the compaction manager
            // for the purposes of this test. All sstables will be accessible
            // through other compaction group views.
            //
            // In this way, sstables are accessible only through one compaction
            // group view and automatic scrub will not submit them multiple times.
            cm.get_compaction_manager().remove(*view).get();
        }
        auto readd = defer([&views, &cm] mutable noexcept {
            try {
                for (auto& view : views) {
                    cm.get_compaction_manager().add(*view);
                }
            } catch (...) {
                testlog.error("Failed to re-add views because of {}", std::current_exception());
            }
        });

        auto timestamp_before = db_clock::now();

        cm.set_scrub_period(std::chrono::seconds(3600));
        cm.trigger_auto_scrub_timer();

        utils::get_local_injector().enable_on_all("automatic_scrub_compaction", true, {{"what", "throw"}}).wait();

        std::exception_ptr ex;
        try {
            wait_on_enter("automatic_scrub_compaction_done", sst_count * table_count).get();
        } catch (...) {
            ex = std::current_exception();
        }

        utils::get_local_injector().disable_on_all("automatic_scrub_compaction_fail").wait();
        if (ex) {
            std::rethrow_exception(ex);
        }

        size_t validated = 0;
        for (auto [table, view, sstables] : std::views::zip(tables, views, sstable_sets)) {
            BOOST_REQUIRE(table->get_sstables());
            BOOST_REQUIRE_EQUAL(table->get_sstables()->size(), sstables.size());
            for (auto& sst : *table->get_sstables()) {
                auto timestamp = sst->get_scrub_time();
                BOOST_REQUIRE(timestamp);
                if (*timestamp > timestamp_before) {
                    validated++;
                }
            }
        }

        BOOST_REQUIRE_EQUAL(validated, table_count * sst_count - 1);

        cm.trigger_auto_scrub_timer();
        wait_on_enter("automatic_scrub_compaction_done", sst_count * table_count + 1).get();

        for (auto [table, view, sstables] : std::views::zip(tables, views, sstable_sets)) {
            BOOST_REQUIRE(table->get_sstables());
            BOOST_REQUIRE_EQUAL(table->get_sstables()->size(), sstables.size());
            for (auto& sst : *table->get_sstables()) {
                auto timestamp = sst->get_scrub_time();
                BOOST_REQUIRE(timestamp);
                BOOST_REQUIRE(*timestamp > timestamp_before);
            }
        }
    });
}

SEASTAR_THREAD_TEST_CASE(test_scrub_time_updateable) {
    automatic_scrub_test_framework test(tests::random_schema_specification::compress_sstable::yes);

    auto& test_env = test.env();
    constexpr auto sst_count = 5;

    test.run(sst_count, [&test_env] (table_for_tests& table, compaction::compaction_group_view& ts, std::vector<sstables::shared_sstable> sstables) {
        auto& cm = test_env.test_compaction_manager();

        for (sstables::shared_sstable& sst : sstables) {
            set_scrub_time(sst, db_clock::from_time_t(0));
        }

        cm.set_scrub_time_source(3600); // enable

        auto timestamp_before = db_clock::now();
        cm.trigger_auto_scrub_timer();

        wait_on_enter("automatic_scrub_compaction_done", sst_count).get();

        BOOST_REQUIRE_EQUAL(table->get_sstables()->size(), sstables.size());
        for (auto& sst : *table->get_sstables()) {
            BOOST_REQUIRE(sst->has_scylla_component());
            BOOST_REQUIRE(sst->get_scrub_time() > timestamp_before);
        }

        for (sstables::shared_sstable& sst : sstables) {
            set_scrub_time(sst, db_clock::from_time_t(0));
        }

        cm.set_scrub_time_source(0) ;// disable

        auto enters_before = utils::get_local_injector().enter_count("automatic_scrub_wait_for_signal");

        timestamp_before = db_clock::now();
        cm.trigger_auto_scrub_timer();

        wait_on_enter("automatic_scrub_wait_for_signal", enters_before + 1).get();

        BOOST_REQUIRE_EQUAL(table->get_sstables()->size(), sstables.size());
        for (auto& sst : *table->get_sstables()) {
            BOOST_REQUIRE(sst->has_scylla_component());
            BOOST_REQUIRE(sst->get_scrub_time() < timestamp_before);
        }

        cm.set_scrub_time_source(3600); // re-enable

        enters_before = utils::get_local_injector().enter_count("automatic_scrub_wait_for_signal");

        timestamp_before = db_clock::now();
        cm.trigger_auto_scrub_timer();

        wait_on_enter("automatic_scrub_wait_for_signal", enters_before + 1).get();

        BOOST_REQUIRE_EQUAL(table->get_sstables()->size(), sstables.size());
        for (auto& sst : *table->get_sstables()) {
            BOOST_REQUIRE(sst->has_scylla_component());
            BOOST_REQUIRE(sst->get_scrub_time() > timestamp_before);
        }
    });
}

SEASTAR_THREAD_TEST_CASE(test_automatic_scrub_respects_reevaluation_during_scrub) {
    automatic_scrub_test_framework test(tests::random_schema_specification::compress_sstable::yes);

    auto& test_env = test.env();
    constexpr auto sst_count = 5;

    test.run(sst_count, [&test_env] (table_for_tests& table, compaction::compaction_group_view& ts, std::vector<sstables::shared_sstable> sstables) {
        auto& cm = test_env.test_compaction_manager();
        scoped_error_injection pause_injection{"automatic_scrub_compaction", false, {{"what", "pause"}}};

        for (const auto& sst : sstables) {
            set_scrub_time(sst, db_clock::from_time_t(0));
        }

        cm.set_scrub_period(std::chrono::seconds(3600));
        cm.trigger_auto_scrub_timer();

        for (size_t i = 0; i < sst_count - 1; i++) {
            utils::get_local_injector().receive_message("automatic_scrub_compaction");
        }
        wait_on_enter("automatic_scrub_compaction_done", sst_count - 1).get();

        for (const auto& sst : sstables) {
            set_scrub_time(sst, db_clock::from_time_t(0));
        }

        auto between_scrubs = db_clock::now();

        cm.trigger_auto_scrub_timer();

        for (size_t i = 0; i < sst_count + 1; i++) {
            utils::get_local_injector().receive_message("automatic_scrub_compaction");
        }

        wait_on_enter("automatic_scrub_compaction_done", 2 * sst_count - 1).get();

        BOOST_REQUIRE_EQUAL(table->get_sstables()->size(), sstables.size());
        size_t revalidated_sstables = 0;
        for (auto& sst : *table->get_sstables()) {
            auto timestamp = sst->get_scrub_time();
            BOOST_REQUIRE(timestamp);
            if (*timestamp > between_scrubs) {
                ++revalidated_sstables;
            }
        }
        BOOST_REQUIRE_GE(revalidated_sstables, sst_count - 1);
    });
}

SEASTAR_THREAD_TEST_CASE(test_scrub_time_persistance) {
    automatic_scrub_test_framework test(tests::random_schema_specification::compress_sstable::yes);

    auto& test_env = test.env();
    constexpr auto sst_count = 1;

    test.run(sst_count, [&test_env] (table_for_tests& table, compaction::compaction_group_view& ts, std::vector<sstables::shared_sstable> sstables) {
        auto& cm = test_env.test_compaction_manager();

        auto sst = sstables.front();

        auto metadata_opt = sstables::test(sst)._scylla_metadata();
        BOOST_REQUIRE(metadata_opt);
        auto& metadata = *metadata_opt;
        metadata->data.data.erase(scylla_metadata_type::ScrubTime);

        auto timestamp_before = db_clock::now();

        cm.set_scrub_period(std::chrono::seconds(3600));
        cm.trigger_auto_scrub_timer();

        wait_on_enter("automatic_scrub_compaction_done", sst_count).get();

        BOOST_REQUIRE(!table->get_sstables()->empty());
        sst = *table->get_sstables()->begin();

        auto timestamp = sst->get_scrub_time();
        BOOST_REQUIRE(timestamp);
        BOOST_REQUIRE(*timestamp > timestamp_before);

        auto on_disk_sst = test_env.reusable_sst(sst).get();

        auto on_disk_scrub_time = on_disk_sst->get_scrub_time();
        BOOST_REQUIRE(on_disk_scrub_time == timestamp);
    });
}

} // namespace
