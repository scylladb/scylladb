/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <seastar/testing/thread_test_case.hh>
#include "readers/from_mutations.hh"
#include "seastarx.hh"

#include <seastar/testing/test_case.hh>

#include "sstables/sstable_writer.hh"
#include "test/boost/sstable_test.hh"
#include "test/lib/cql_test_env.hh"
#include "test/lib/error_injection.hh"
#include "test/lib/mutation_source_test.hh"
#include "test/lib/random_schema.hh"
#include "test/lib/simple_schema.hh"
#include "test/lib/sstable_test_env.hh"
#include "test/lib/sstable_utils.hh"
#include "test/lib/test_utils.hh"
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
public:
    using test_func = std::function<void(table_for_tests&, compaction::compaction_group_view&, std::vector<sstables::shared_sstable>)>;

private:
    std::unique_ptr<sstable_compressor_factory> scf = make_sstable_compressor_factory_for_tests_in_thread();
    sharded<test_env> _env;
    uint32_t _seed;
    std::unique_ptr<tests::random_schema_specification> _random_schema_spec;
    tests::random_schema _random_schema;
public:
    automatic_scrub_test_framework(tests::random_schema_specification::compress_sstable compress)
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
        _env.start(test_env_config(), std::ref(*scf)).get();
        testlog.info("random_schema: {}", _random_schema.cql());
    }

    ~automatic_scrub_test_framework() {
        _env.stop().get();
    }

    test_env& env() { return _env.local(); }
    uint32_t seed() const { return _seed; }
    tests::random_schema& random_schema() { return _random_schema; }
    schema_ptr schema() const { return _random_schema.schema(); }

    shared_sstable make_sstable() {
        auto muts = tests::generate_random_mutations(
                random_schema(),
                tests::uncompactible_timestamp_generator(seed()),
                tests::no_expiry_expiry_generator(),
                std::uniform_int_distribution<size_t>(10, 10)).get();
        
        auto sst = make_sstable_containing(env().make_sstable(schema()), std::move(muts)).get();
        return sst;
    }

    table_for_tests make_table(size_t sst_count, offstrategy ssts_offstrategy) {
        auto table = env().make_table_for_tests(schema());
        table->start();

        for (size_t i = 0; i < sst_count; i++) {
            auto sst = make_sstable();
            table->add_sstable_and_update_cache(sst, ssts_offstrategy).get();
        }

        return table;
    }

    void run(size_t sst_count, test_func func, offstrategy ssts_offstrategy = offstrategy::yes) {
        auto table = make_table(sst_count, ssts_offstrategy);
        auto close_cf = deferred_stop(table);

        auto& cgv = table.as_compaction_group_view();

        scoped_no_abort_on_malformed_sstable_error no_abort{};
        scoped_error_injection validation_injection{"automatic_scrub_compaction_done"};
        scoped_error_injection wait_injection{"automatic_scrub_wait_for_signal"};

        auto sstables = get_all_sstables(cgv).get();

        if (ssts_offstrategy) {
            BOOST_REQUIRE_EQUAL(sst_count, sstables.size());
        }
        
        func(table, cgv, std::move(sstables));
    }
};

} // namespace
