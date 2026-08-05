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

future<> wait_on_enter(std::string_view name, size_t count = 1, std::chrono::milliseconds timeout = std::chrono::milliseconds(5000)) {
    auto& injector = utils::get_local_injector();
    constexpr auto sleep_duration = std::chrono::milliseconds(100);
    std::chrono::milliseconds time_waited{0};
    while (injector.enter_count(name) < count) {
        if (time_waited > timeout) {
            throw std::runtime_error(fmt::format("timeout reached when waiting for {}", name));
        }
        co_await sleep(sleep_duration);
        time_waited += sleep_duration;
    }
}

static void corrupt_sstable(sstables::shared_sstable sst, component_type type = component_type::Data) {
    auto f = sstables::test(sst).open_file(type, {}, {}).get();
    auto close_f = deferred_close(f);
    const auto wbuf_align = f.memory_dma_alignment();
    const auto wbuf_len = f.size().get();
    auto wbuf = seastar::temporary_buffer<char>::aligned(wbuf_align, wbuf_len);
    std::fill(wbuf.get_write(), wbuf.get_write() + wbuf_len, 0xba);
    auto os = output_stream<char>(sstables::test(sst).get_storage().make_component_sink(*sst, type, open_flags::wo, {}).get());
    auto close_os = deferred_close(os);
    os.write(std::move(wbuf)).get();
}

void compare_non_scylla_digests(const sstables::scylla_metadata::components_digests& lhs, const sstables::scylla_metadata::components_digests& rhs) {
    BOOST_REQUIRE_EQUAL(lhs.map.size(), rhs.map.size());

    for (const auto& [component, digest] : lhs.map) {
        auto it = rhs.map.find(component);
        BOOST_REQUIRE(it != rhs.map.end());
        BOOST_REQUIRE_EQUAL(digest, it->second);
    }
}


SEASTAR_THREAD_TEST_CASE(sstable_auto_scrub_corrupted_ssts_with_scylla_test) {
    automatic_scrub_test_framework test(tests::random_schema_specification::compress_sstable::yes);

    auto& test_env = test.env();
    constexpr auto sst_count = 5;

    test.run(sst_count, [&test_env] (table_for_tests& table, compaction::compaction_group_view& ts, std::vector<sstables::shared_sstable> sstables) {
        auto& cm = test_env.test_compaction_manager();

        for (sstables::shared_sstable& sst : sstables) {
            sst->set_scrub_time(db_clock::from_time_t(0));
            corrupt_sstable(sst);
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
        
        sst->set_scrub_time(db_clock::from_time_t(0));

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
        compare_non_scylla_digests(pre_scrub, *post_scrub_digests);
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
            sst->set_scrub_time(db_clock::from_time_t(0));
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
            sst->set_scrub_time(db_clock::from_time_t(0));
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
            sst->set_scrub_time(db_clock::from_time_t(0));
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
                sst->set_scrub_time(db_clock::from_time_t(0));
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
            sst->set_scrub_time(db_clock::from_time_t(0));
        }

        auto timestamp_before = db_clock::now();
        auto signal_waits_before = utils::get_local_injector().enter_count("automatic_scrub_wait_for_signal");

        size_t compacting_sstable_count = 3;
        cm.register_compacting(ts, std::span{sstables}.subspan(0, compacting_sstable_count)).wait();
        
        cm.set_scrub_period(std::chrono::seconds(3600));
        cm.trigger_auto_scrub_timer();

        wait_on_enter("automatic_scrub_compaction_done", sst_count - compacting_sstable_count).get();

        wait_on_enter("automatic_scrub_wait_for_signal", signal_waits_before + 1).get();
        BOOST_REQUIRE_EQUAL(utils::get_local_injector().enter_count("automatic_scrub_wait_for_signal"), signal_waits_before + 1);

        auto ssts_after_scrub = *table->get_sstables() | std::ranges::to<std::unordered_set>();
        for (auto compacting_sst : sstables | std::views::take(compacting_sstable_count)) {
            // The compacting sstables were not selected by automatic scrub
            BOOST_REQUIRE(ssts_after_scrub.contains(compacting_sst));
        }

        cm.deregister_compacting(ts, std::span{sstables}.subspan(0, compacting_sstable_count)).wait();
        cm.trigger_auto_scrub_timer();
        wait_on_enter("automatic_scrub_compaction_done", compacting_sstable_count).get();
        
        BOOST_REQUIRE_EQUAL(table->get_sstables()->size(), sstables.size());
        for (auto& sst : *table->get_sstables()) {
            auto sst_timestamp = sst->get_scrub_time();
            BOOST_REQUIRE(sst_timestamp);
            BOOST_REQUIRE(*sst_timestamp > timestamp_before);
        }
    });
}

SEASTAR_THREAD_TEST_CASE(sstable_auto_scrub_table_readded_after_compaction) {
    automatic_scrub_test_framework test(tests::random_schema_specification::compress_sstable::yes);

    auto& test_env = test.env();
    constexpr auto sst_count = 5;

    test.run(sst_count, [&test_env] (table_for_tests& table, compaction::compaction_group_view& ts, std::vector<sstables::shared_sstable> sstables) {
        auto& cm = test_env.test_compaction_manager();

        // All the sstables should be validated.
        for (sstables::shared_sstable& sst : sstables) {
            sst->set_scrub_time(db_clock::from_time_t(0));
        }

        auto timestamp_before = db_clock::now();
        auto signal_waits_before = utils::get_local_injector().enter_count("automatic_scrub_wait_for_signal");
        
        utils::get_local_injector().enable("major_compaction_wait");
        auto major_compaction = cm.get_compaction_manager().perform_major_compaction(ts, tasks::task_info{});

        // Enable auto scrub. The sstables should be compacting and not be eligible
        // for automatic scrub.
        cm.set_scrub_period(std::chrono::seconds(3600));
        cm.trigger_auto_scrub_timer();
        wait_on_enter("automatic_scrub_wait_for_signal", signal_waits_before + 1).get();

        // Resume major compaction. After it finishes, it should schedule the unverified sstables
        // it owns for an automatic scrub.
        utils::get_local_injector().receive_message("major_compaction_wait");
        major_compaction.wait();
        utils::get_local_injector().disable("major_compaction_wait");

        wait_on_enter("automatic_scrub_wait_for_signal", signal_waits_before + 2).get();
        
        BOOST_REQUIRE(!table->get_sstables()->empty());
        for (auto& sst : *table->get_sstables()) {
            auto scrub_time = sst->get_scrub_time();
            BOOST_REQUIRE(scrub_time);
            BOOST_REQUIRE(*scrub_time > timestamp_before);
        }
    }, offstrategy::no);
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

} // namespace

