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
        scoped_error_injection suspected_disk_corruption_injection{"compaction_manager_suspected_disk_corruption"};
        scoped_error_injection validation_done_injection{"compaction_regular_compaction_validation_done"};

        func(tables, views, sstables);
    }
};

} // namespace
