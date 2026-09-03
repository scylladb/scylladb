/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <boost/lexical_cast.hpp>
#include <seastar/util/short_streams.hh>
#include <seastar/util/closeable.hh>

#undef SEASTAR_TESTING_MAIN
#include <seastar/testing/thread_test_case.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/testing/test_fixture.hh>
#include "test/lib/error_injection.hh"
#include "test/lib/eventually.hh"
#include "test/lib/key_utils.hh"
#include "test/lib/log.hh"
#include "test/lib/mutation_reader_assertions.hh"
#include "test/lib/random_schema.hh"
#include "test/lib/sstable_utils.hh"
#include "test/lib/test_services.hh"
#include "test/lib/sstable_test_env.hh"
#include "test/lib/random_utils.hh"
#include "test/lib/gcs_fixture.hh"
#include "test/lib/test_utils.hh"
#include "test/lib/simple_schema.hh"
#include "sstables/sstable_writer.hh"
#include "mutation_writer/partition_based_splitting_writer.hh"
#include "readers/combined.hh"
#include "readers/from_fragments.hh"
#include "readers/from_mutations.hh"

BOOST_AUTO_TEST_SUITE(scrub_test)

using namespace sstables;
using compress_sstable = tests::random_schema_specification::compress_sstable;

future<> foreach_compaction_group_view_with_thread(table_for_tests& table, std::function<void(compaction::compaction_group_view&)> action) {
    return table->parallel_foreach_compaction_group_view([action] (compaction::compaction_group_view& ts) {
        return seastar::async([action, &ts] {
            action(ts);
        });
    });
}

static std::deque<mutation_fragment_v2> explode(reader_permit permit, utils::chunked_vector<mutation> muts) {
    if (muts.empty()) {
        return {};
    }

    auto schema = muts.front().schema();
    std::deque<mutation_fragment_v2> frags;

    auto mr = make_mutation_reader_from_mutations(schema, permit, std::move(muts));
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
        readers.push_back(sst->as_mutation_source().make_mutation_reader(schema, permit));
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
    using test_func = std::function<void(table_for_tests&, compaction::compaction_group_view&, std::vector<sstables::shared_sstable>)>;

private:
    std::unique_ptr<sstable_compressor_factory> scf = make_sstable_compressor_factory_for_tests_in_thread();
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
        _env.start(test_env_config(), std::ref(*scf)).get();
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

        // The test violates key order on purpose.
        // That's illegal with the index writer of version `ms`.
        // So we can't use this test, as it is currently written, with `ms`.
        auto version = sstable_version_types::me;
        auto sst = env.make_sstable(schema, version);
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
        foreach_compaction_group_view_with_thread(table, [&] (compaction::compaction_group_view& ts) {
            auto sstables = in_strategy_sstables(ts).get();
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

    void run(schema_ptr schema, utils::chunked_vector<mutation> muts, test_func func) {
        run(std::move(schema), explode(env().make_reader_permit(), std::move(muts)), std::move(func));
    }
};

template <>
class scrub_test_framework<random_schema::no> {
public:
    using test_func = std::function<void(table_for_tests&, compaction::compaction_group_view&, std::vector<sstables::shared_sstable>)>;

private:

    std::unique_ptr<sstable_compressor_factory> scf = make_sstable_compressor_factory_for_tests_in_thread();
    sharded<test_env> _env;

public:
    scrub_test_framework()
    {
        _env.start(test_env_config(), std::ref(*scf)).get();
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
        foreach_compaction_group_view_with_thread(table, [&] (compaction::compaction_group_view& ts) {
            auto sstables = in_strategy_sstables(ts).get();
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

void scrub_validate_corrupted_content(compress_sstable compress,
        compaction::compaction_type_options::scrub::quarantine_invalid_sstables quarantine_sstables
            = compaction::compaction_type_options::scrub::quarantine_invalid_sstables::yes) {
    scrub_test_framework<random_schema::yes> test(compress);

    auto schema = test.schema();

    auto muts = tests::generate_random_mutations(
            test.random_schema(),
            tests::uncompactible_timestamp_generator(test.seed()),
            tests::no_expiry_expiry_generator(),
            std::uniform_int_distribution<size_t>(10, 10)).get();
    std::swap(*muts.begin(), *(muts.begin() + 1));

    test.run(schema, muts, [quarantine_sstables] (table_for_tests& table, compaction::compaction_group_view& ts, std::vector<sstables::shared_sstable> sstables) {
        BOOST_REQUIRE(sstables.size() == 1);
        auto sst = sstables.front();

        compaction::compaction_type_options::scrub opts = {
            .operation_mode = compaction::compaction_type_options::scrub::mode::validate,
            .quarantine_sstables = quarantine_sstables,
        };
        auto stats = table->get_compaction_manager().perform_sstable_scrub(ts, opts, tasks::task_info{}).get();

        BOOST_REQUIRE(stats.has_value());
        BOOST_REQUIRE_GT(stats->validation_errors, 0);
        const bool expect_quarantined = quarantine_sstables == compaction::compaction_type_options::scrub::quarantine_invalid_sstables::yes;
        BOOST_REQUIRE_EQUAL(sst->is_quarantined(), expect_quarantined);
        if (expect_quarantined) {
            BOOST_REQUIRE(in_strategy_sstables(ts).get().empty());
        } else {
            BOOST_REQUIRE_EQUAL(in_strategy_sstables(ts).get().size(), 1);
            BOOST_REQUIRE_EQUAL(in_strategy_sstables(ts).get().front(), sst);
        }
    });
}

void scrub_validate_corrupted_file(compress_sstable compress, component_type component = component_type::Data,
        compaction::compaction_type_options::scrub::quarantine_invalid_sstables quarantine_sstables
            = compaction::compaction_type_options::scrub::quarantine_invalid_sstables::yes) {
    scrub_test_framework<random_schema::yes> test(compress);

    auto schema = test.schema();

    auto muts = tests::generate_random_mutations(
            test.random_schema(),
            tests::uncompactible_timestamp_generator(test.seed()),
            tests::no_expiry_expiry_generator(),
            std::uniform_int_distribution<size_t>(10, 10)).get();

    test.run(schema, muts, [component, quarantine_sstables] (table_for_tests& table, compaction::compaction_group_view& ts, std::vector<sstables::shared_sstable> sstables) {
        BOOST_REQUIRE(sstables.size() == 1);
        auto sst = sstables.front();

        // Corrupt the component file to cause an invalid checksum.
        corrupt_sstable(sst, component);

        compaction::compaction_type_options::scrub opts = {
            .operation_mode = compaction::compaction_type_options::scrub::mode::validate,
            .quarantine_sstables = quarantine_sstables,
        };
        auto stats = table->get_compaction_manager().perform_sstable_scrub(ts, opts, tasks::task_info{}).get();

        BOOST_REQUIRE(stats.has_value());
        BOOST_REQUIRE_GT(stats->validation_errors, 0);
        const bool expect_quarantined = quarantine_sstables == compaction::compaction_type_options::scrub::quarantine_invalid_sstables::yes;
        BOOST_REQUIRE_EQUAL(sst->is_quarantined(), expect_quarantined);
        if (expect_quarantined) {
            BOOST_REQUIRE(in_strategy_sstables(ts).get().empty());
        } else {
            BOOST_REQUIRE_EQUAL(in_strategy_sstables(ts).get().size(), 1);
            BOOST_REQUIRE_EQUAL(in_strategy_sstables(ts).get().front(), sst);
        }
    });
}

void scrub_validate_corrupted_digest(compress_sstable compress,
        compaction::compaction_type_options::scrub::quarantine_invalid_sstables quarantine_sstables
            = compaction::compaction_type_options::scrub::quarantine_invalid_sstables::yes) {
    scrub_test_framework<random_schema::yes> test(compress);

    auto schema = test.schema();

    auto muts = tests::generate_random_mutations(
            test.random_schema(),
            tests::uncompactible_timestamp_generator(test.seed()),
            tests::no_expiry_expiry_generator(),
            std::uniform_int_distribution<size_t>(10, 10)).get();

    test.run(schema, muts, [quarantine_sstables] (table_for_tests& table, compaction::compaction_group_view& ts, std::vector<sstables::shared_sstable> sstables) {
        BOOST_REQUIRE(sstables.size() == 1);
        auto sst = sstables.front();

        // This test is about corrupted data with valid per-chunk checksums.
        // This kind of corruption should be detected by the digest check.
        // Triggering this is not trivial, so we corrupt the Digest file instead.
        auto f = sstables::test(sst).open_file(component_type::Digest, {}, {}).get();
        auto stream = make_file_input_stream(f);
        auto close_stream = deferred_close(stream);
        auto digest_str = util::read_entire_stream_contiguous(stream).get();
        auto digest = boost::lexical_cast<uint32_t>(digest_str);
        auto new_digest = to_sstring<bytes>(digest + 1); // a random invalid digest
        auto os = output_stream<char>(sstables::test(sst).get_storage().make_component_sink(*sst, component_type::Digest, open_flags::wo, {}).get());
        auto close_os = deferred_close(os);
        os.write(std::move(new_digest)).get();
        os.flush().get();

        compaction::compaction_type_options::scrub opts = {
            .operation_mode = compaction::compaction_type_options::scrub::mode::validate,
            .quarantine_sstables = quarantine_sstables,
        };
        auto stats = table->get_compaction_manager().perform_sstable_scrub(ts, opts, tasks::task_info{}).get();

        BOOST_REQUIRE(stats.has_value());
        BOOST_REQUIRE_GT(stats->validation_errors, 0);
        const bool expect_quarantined = quarantine_sstables == compaction::compaction_type_options::scrub::quarantine_invalid_sstables::yes;
        BOOST_REQUIRE_EQUAL(sst->is_quarantined(), expect_quarantined);
        if (expect_quarantined) {
            BOOST_REQUIRE(in_strategy_sstables(ts).get().empty());
        } else {
            BOOST_REQUIRE_EQUAL(in_strategy_sstables(ts).get().size(), 1);
            BOOST_REQUIRE_EQUAL(in_strategy_sstables(ts).get().front(), sst);
        }
    });
}

void scrub_validate_no_digest(compress_sstable compress,
        compaction::compaction_type_options::scrub::quarantine_invalid_sstables quarantine_sstables
            = compaction::compaction_type_options::scrub::quarantine_invalid_sstables::yes) {
    scrub_test_framework<random_schema::yes> test(compress);

    auto schema = test.schema();

    auto muts = tests::generate_random_mutations(test.random_schema()).get();

    test.run(schema, muts, [quarantine_sstables] (table_for_tests& table, compaction::compaction_group_view& ts, std::vector<sstables::shared_sstable> sstables) {
        BOOST_REQUIRE(sstables.size() == 1);
        auto sst = sstables.front();

        // Checksum and digest checking should be orthogonal.
        // Ensure that per-chunk checksums are properly checked when digest is missing.
        sstables::test(sst).rewrite_toc_without_component(component_type::Digest);

        compaction::compaction_type_options::scrub opts = {
            .operation_mode = compaction::compaction_type_options::scrub::mode::validate,
            .quarantine_sstables = quarantine_sstables,
        };
        auto stats = table->get_compaction_manager().perform_sstable_scrub(ts, opts, tasks::task_info{}).get();

        BOOST_REQUIRE(stats.has_value());
        BOOST_REQUIRE_EQUAL(stats->validation_errors, 0);
        BOOST_REQUIRE(!sst->is_quarantined());
        BOOST_REQUIRE_EQUAL(in_strategy_sstables(ts).get().size(), 1);
        BOOST_REQUIRE_EQUAL(in_strategy_sstables(ts).get().front(), sst);
        BOOST_REQUIRE(!sst->get_checksum());

        // Corrupt the data to cause an invalid checksum.
        corrupt_sstable(sst);

        stats = table->get_compaction_manager().perform_sstable_scrub(ts, opts, tasks::task_info{}).get();

        BOOST_REQUIRE(stats.has_value());
        BOOST_REQUIRE_GT(stats->validation_errors, 0);
        const bool expect_quarantined = quarantine_sstables == compaction::compaction_type_options::scrub::quarantine_invalid_sstables::yes;
        BOOST_REQUIRE_EQUAL(sst->is_quarantined(), expect_quarantined);
        if (expect_quarantined) {
            BOOST_REQUIRE(in_strategy_sstables(ts).get().empty());
        } else {
            BOOST_REQUIRE_EQUAL(in_strategy_sstables(ts).get().size(), 1);
            BOOST_REQUIRE_EQUAL(in_strategy_sstables(ts).get().front(), sst);
        }
    });
}

void scrub_validate_valid(compress_sstable compress,
        compaction::compaction_type_options::scrub::quarantine_invalid_sstables quarantine_sstables
            = compaction::compaction_type_options::scrub::quarantine_invalid_sstables::yes) {
    scrub_test_framework<random_schema::yes> test(compress);

    auto schema = test.schema();

    auto muts = tests::generate_random_mutations(test.random_schema()).get();

    test.run(schema, muts, [quarantine_sstables] (table_for_tests& table, compaction::compaction_group_view& ts, std::vector<sstables::shared_sstable> sstables) {
        BOOST_REQUIRE(sstables.size() == 1);
        auto sst = sstables.front();

        compaction::compaction_type_options::scrub opts = {
            .operation_mode = compaction::compaction_type_options::scrub::mode::validate,
            .quarantine_sstables = quarantine_sstables,
        };
        auto stats = table->get_compaction_manager().perform_sstable_scrub(ts, opts, tasks::task_info{}).get();

        BOOST_REQUIRE(stats.has_value());
        BOOST_REQUIRE_EQUAL(stats->validation_errors, 0);
        // A valid sstable is never quarantined regardless of the quarantine_sstables flag.
        BOOST_REQUIRE(!sst->is_quarantined());
        BOOST_REQUIRE_EQUAL(in_strategy_sstables(ts).get().size(), 1);
        BOOST_REQUIRE_EQUAL(in_strategy_sstables(ts).get().front(), sst);
    });
}

SEASTAR_THREAD_TEST_CASE(sstable_scrub_validate_mode_test_corrupted_content) {
    for (const auto& compress : {compress_sstable::no, compress_sstable::yes}) {
        testlog.info("Validating {}compressed SSTable with content-level corruption...", compress == compress_sstable::no ? "un" : "");
        scrub_validate_corrupted_content(compress);
    }
}

SEASTAR_THREAD_TEST_CASE(sstable_scrub_validate_mode_test_corrupted_content_no_quarantine) {
    for (const auto& compress : {compress_sstable::no, compress_sstable::yes}) {
        testlog.info("Validating {}compressed SSTable with content-level corruption (no quarantine)...", compress == compress_sstable::no ? "un" : "");
        scrub_validate_corrupted_content(compress, compaction::compaction_type_options::scrub::quarantine_invalid_sstables::no);
    }
}

SEASTAR_THREAD_TEST_CASE(sstable_scrub_validate_mode_test_corrupted_file) {
    for (const auto& compress : {compress_sstable::no, compress_sstable::yes}) {
        testlog.info("Validating {}compressed SSTable with invalid checksums...", compress == compress_sstable::no ? "un" : "");
        scrub_validate_corrupted_file(compress);
    }
}

SEASTAR_THREAD_TEST_CASE(sstable_scrub_validate_mode_test_corrupted_file_no_quarantine) {
    for (const auto& compress : {compress_sstable::no, compress_sstable::yes}) {
        testlog.info("Validating {}compressed SSTable with invalid checksums (no quarantine)...", compress == compress_sstable::no ? "un" : "");
        scrub_validate_corrupted_file(compress, component_type::Data, compaction::compaction_type_options::scrub::quarantine_invalid_sstables::no);
    }
}

SEASTAR_THREAD_TEST_CASE(sstable_scrub_validate_mode_test_corrupted_index) {
    for (const auto& compress : {compress_sstable::no, compress_sstable::yes}) {
        testlog.info("Validating {}compressed SSTable with corrupted index...", compress == compress_sstable::no ? "un" : "");
        scrub_validate_corrupted_file(compress, component_type::Index);
    }
}

SEASTAR_THREAD_TEST_CASE(sstable_scrub_validate_mode_test_corrupted_index_no_quarantine) {
    for (const auto& compress : {compress_sstable::no, compress_sstable::yes}) {
        testlog.info("Validating {}compressed SSTable with corrupted index (no quarantine)...", compress == compress_sstable::no ? "un" : "");
        scrub_validate_corrupted_file(compress, component_type::Index, compaction::compaction_type_options::scrub::quarantine_invalid_sstables::no);
    }
}

SEASTAR_THREAD_TEST_CASE(sstable_scrub_validate_mode_test_corrupted_file_digest) {
    for (const auto& compress : {compress_sstable::no, compress_sstable::yes}) {
        testlog.info("Validating {}compressed SSTable with invalid digest...", compress == compress_sstable::no ? "un" : "");
        scrub_validate_corrupted_digest(compress);
    }
}

SEASTAR_THREAD_TEST_CASE(sstable_scrub_validate_mode_test_corrupted_file_digest_no_quarantine) {
    for (const auto& compress : {compress_sstable::no, compress_sstable::yes}) {
        testlog.info("Validating {}compressed SSTable with invalid digest (no quarantine)...", compress == compress_sstable::no ? "un" : "");
        scrub_validate_corrupted_digest(compress, compaction::compaction_type_options::scrub::quarantine_invalid_sstables::no);
    }
}

SEASTAR_THREAD_TEST_CASE(sstable_scrub_validate_mode_test_no_digest) {
    for (const auto& compress : {compress_sstable::no, compress_sstable::yes}) {
        testlog.info("Validating {}compressed SSTable with no digest...", compress == compress_sstable::no ? "un" : "");
        scrub_validate_no_digest(compress);
    }
}

SEASTAR_THREAD_TEST_CASE(sstable_scrub_validate_mode_test_no_digest_no_quarantine) {
    for (const auto& compress : {compress_sstable::no, compress_sstable::yes}) {
        testlog.info("Validating {}compressed SSTable with no digest (no quarantine)...", compress == compress_sstable::no ? "un" : "");
        scrub_validate_no_digest(compress, compaction::compaction_type_options::scrub::quarantine_invalid_sstables::no);
    }
}

SEASTAR_THREAD_TEST_CASE(sstable_scrub_validate_mode_test_valid_sstable) {
    for (const auto& compress : {compress_sstable::no, compress_sstable::yes}) {
        testlog.info("Validating {}compressed SSTable...", compress == compress_sstable::no ? "un" : "");
        scrub_validate_valid(compress);
    }
}

SEASTAR_THREAD_TEST_CASE(sstable_scrub_validate_mode_test_valid_sstable_no_quarantine) {
    for (const auto& compress : {compress_sstable::no, compress_sstable::yes}) {
        testlog.info("Validating {}compressed SSTable (no quarantine)...", compress == compress_sstable::no ? "un" : "");
        scrub_validate_valid(compress, compaction::compaction_type_options::scrub::quarantine_invalid_sstables::no);
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

    test.run(schema, muts, [] (table_for_tests& table, compaction::compaction_group_view& ts, std::vector<sstables::shared_sstable> sstables) {
        BOOST_REQUIRE(sstables.size() == 1);
        auto sst = sstables.front();

        compaction::compaction_type_options::scrub opts = {
            .operation_mode = compaction::compaction_type_options::scrub::mode::validate,
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
        BOOST_REQUIRE_EQUAL(in_strategy_sstables(ts).get().size(), 1);
        BOOST_REQUIRE_EQUAL(in_strategy_sstables(ts).get().front(), sst);
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
    return schema_builder(this_smp_shard_count(), "test_ks", "test_table")
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

    test.run(schema, sst, [valid] (table_for_tests& table, compaction::compaction_group_view& ts, std::vector<sstables::shared_sstable> sstables) {
        BOOST_REQUIRE(sstables.size() == 1);
        auto sst = sstables.front();

        using scrub = compaction::compaction_type_options::scrub;
        compaction::compaction_type_options::scrub opts = {
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
        BOOST_REQUIRE_EQUAL(in_strategy_sstables(ts).get().size(), 1);
        BOOST_REQUIRE_EQUAL(in_strategy_sstables(ts).get().front(), sst);
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

void sstable_validate_fn(test_env& env) {
    for (const auto sst_version : {sstable_version_types::me, sstable_version_types::ms}) {
        auto schema = schema_builder(this_smp_shard_count(), "ks", testing::seastar_test::get_name())
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
            return make_sstable_easy(env, std::move(rd), std::move(config), sst_version, local_keys.size());
        };

        auto info = make_lw_shared<compaction::compaction_data>();

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

        // BTI index writers won't accept out-of-order keys.
        if (has_summary_and_index(sst_version)) {
            BOOST_TEST_MESSAGE("out-of-order clustering row");
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

        // BTI index writers won't accept out-of-order keys.
        if (has_summary_and_index(sst_version)) {
            BOOST_TEST_MESSAGE("out-of-order partition");
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
            corrupt_sstable(sst);

            auto res = sstables::validate_checksums_and_digests(sst, permit).get();
            BOOST_REQUIRE(res.status == validate_checksums_status::invalid);
            BOOST_REQUIRE(res.has_digest);


            const auto errors = sst->validate(permit, abort, error_handler{count}).get();
            BOOST_REQUIRE_NE(errors, 0);
            BOOST_REQUIRE_EQUAL(errors, count);
        }
   }
}

SEASTAR_TEST_CASE(sstable_validate_test) {
    return test_env::do_with_async([](test_env& env) { sstable_validate_fn(env); });
}
SEASTAR_TEST_CASE(sstable_validate_s3_test, *boost::unit_test::precondition(tests::has_scylla_test_env)) {
    return test_env::do_with_async([](test_env& env) { sstable_validate_fn(env); },
                                   test_env_config{.storage = make_test_object_storage_options("S3")});
}
SEASTAR_FIXTURE_TEST_CASE(sstable_validate_gcs_test, gcs_fixture, *tests::check_run_test_decorator("ENABLE_GCP_STORAGE_TEST", true)) {
    return test_env::do_with_async([](test_env& env) { sstable_validate_fn(env); },
                                   test_env_config{.storage = make_test_object_storage_options("GS")});
}

SEASTAR_THREAD_TEST_CASE(sstable_scrub_abort_mode_test) {
    scrub_test_framework<random_schema::yes> test(compress_sstable::yes);

    auto schema = test.schema();

    auto muts = tests::generate_random_mutations(test.random_schema(), 3).get();
    std::swap(*muts.begin(), *(muts.begin() + 1));

    test.run(schema, muts, [] (table_for_tests& table, compaction::compaction_group_view& ts, std::vector<sstables::shared_sstable> sstables) {
        BOOST_REQUIRE(sstables.size() == 1);
        auto sst = sstables.front();

        testlog.info("Scrub in abort mode");

        // We expect the scrub with mode=srub::mode::abort to stop on the first invalid fragment.
        compaction::compaction_type_options::scrub opts = {};
        opts.operation_mode = compaction::compaction_type_options::scrub::mode::abort;
        BOOST_REQUIRE_THROW(table->get_compaction_manager().perform_sstable_scrub(ts, opts, tasks::task_info{}).get(), compaction::compaction_aborted_exception);

        BOOST_REQUIRE(in_strategy_sstables(ts).get().size() == 1);
        BOOST_REQUIRE(in_strategy_sstables(ts).get().front() == sst);
    });
}

SEASTAR_THREAD_TEST_CASE(sstable_scrub_abort_mode_malformed_sstable_test) {
    scrub_test_framework<random_schema::yes> test(compress_sstable::yes);

    auto schema = test.schema();

    auto muts = tests::generate_random_mutations(test.random_schema(), 3).get();

    test.run(schema, muts, [] (table_for_tests& table, compaction::compaction_group_view& ts, std::vector<sstables::shared_sstable> sstables) {
        sstables::scoped_no_abort_on_malformed_sstable_error no_abort;
        BOOST_REQUIRE(sstables.size() == 1);
        auto sst = sstables.front();
        corrupt_sstable(sst);

        testlog.info("Scrub in abort mode");

        // We expect the scrub with mode=scrub::mode::abort to abort scrub on invalid sstable
        compaction::compaction_type_options::scrub opts = {};
        opts.operation_mode = compaction::compaction_type_options::scrub::mode::abort;
        BOOST_REQUIRE_THROW(table->get_compaction_manager().perform_sstable_scrub(ts, opts, tasks::task_info{}).get(), compaction::compaction_aborted_exception);

        BOOST_REQUIRE(in_strategy_sstables(ts).get().size() == 1);
        BOOST_REQUIRE(in_strategy_sstables(ts).get().front() == sst);
    });
}

SEASTAR_THREAD_TEST_CASE(sstable_scrub_skip_mode_malformed_sstable_test) {
    scrub_test_framework<random_schema::yes> test(compress_sstable::yes);

    auto schema = test.schema();

    auto muts = tests::generate_random_mutations(test.random_schema(), 3).get();

    test.run(schema, muts, [] (table_for_tests& table, compaction::compaction_group_view& ts, std::vector<sstables::shared_sstable> sstables) {
        BOOST_REQUIRE(sstables.size() == 1);
        auto sst = sstables.front();
        corrupt_sstable(sst);

        testlog.info("Scrub in skip mode");

        // We expect the scrub with mode=scrub::mode::skip to remove invalid partitions or sstables
        compaction::compaction_type_options::scrub opts = {};
        opts.operation_mode = compaction::compaction_type_options::scrub::mode::skip;
        BOOST_REQUIRE_NO_THROW(table->get_compaction_manager().perform_sstable_scrub(ts, opts, tasks::task_info{}).get());

        BOOST_REQUIRE(in_strategy_sstables(ts).get().empty());
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
    utils::chunked_vector<mutation> scrubbed_muts;
    scrubbed_muts.push_back(corrupt_muts.front());
    std::copy(corrupt_muts.begin() + 2, corrupt_muts.end(), std::back_inserter(scrubbed_muts));
    auto scrubbed_fragments = explode(test.env().make_reader_permit(), std::move(scrubbed_muts));
    scrubbed_fragments.erase(scrubbed_fragments.begin() + first_cr_index);

    test.run(schema, std::move(corrupt_fragments), [&] (table_for_tests& table, compaction::compaction_group_view& ts, std::vector<sstables::shared_sstable> sstables) {
        BOOST_REQUIRE(sstables.size() == 1);
        auto sst = sstables.front();

        testlog.info("Scrub in skip mode");

        // We expect the scrub with mode=srub::mode::skip to get rid of all invalid data.
        compaction::compaction_type_options::scrub opts = {};
        opts.operation_mode = compaction::compaction_type_options::scrub::mode::skip;
        table->get_compaction_manager().perform_sstable_scrub(ts, opts, tasks::task_info{}).get();

        BOOST_REQUIRE(in_strategy_sstables(ts).get().size() == 1);
        BOOST_REQUIRE(in_strategy_sstables(ts).get().front() != sst);

        verify_fragments(in_strategy_sstables(ts).get(), test.env().make_reader_permit(), scrubbed_fragments);
    });
}

void test_sstable_scrub_segregate_mode(compaction::compaction_type_options::scrub::drop_unfixable_sstables drop_unfixable) {
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

    test.run(schema, std::move(corrupt_fragments), [&] (table_for_tests& table, compaction::compaction_group_view& ts, std::vector<sstables::shared_sstable> sstables) {
        BOOST_REQUIRE(sstables.size() == 1);

        testlog.info("Scrub in segregate mode");

        // We expect the scrub with mode=srub::mode::segregate to fix all out-of-order data.
        compaction::compaction_type_options::scrub opts = {};
        opts.operation_mode = compaction::compaction_type_options::scrub::mode::segregate;
        table->get_compaction_manager().perform_sstable_scrub(ts, opts, tasks::task_info{}).get();

        testlog.info("Scrub resulted in {} sstables", in_strategy_sstables(ts).get().size());
        BOOST_REQUIRE(in_strategy_sstables(ts).get().size() > 1);
        verify_fragments(in_strategy_sstables(ts).get(), test.env().make_reader_permit(), explode(test.env().make_reader_permit(), muts));
    });
}

SEASTAR_THREAD_TEST_CASE(sstable_scrub_segregate_mode_test) {
    test_sstable_scrub_segregate_mode(compaction::compaction_type_options::scrub::drop_unfixable_sstables::no);
    test_sstable_scrub_segregate_mode(compaction::compaction_type_options::scrub::drop_unfixable_sstables::yes);
}

SEASTAR_THREAD_TEST_CASE(sstable_scrub_segregate_mode_drop_unfixable_sstables_test) {
    scrub_test_framework<random_schema::yes> test(compress_sstable::yes);

    auto schema = test.schema();

    auto muts = tests::generate_random_mutations(test.random_schema(), 3).get();

    test.run(schema, muts, [] (table_for_tests& table, compaction::compaction_group_view& ts, std::vector<sstables::shared_sstable> sstables) {
        BOOST_REQUIRE(sstables.size() == 1);
        auto sst = sstables.front();
        corrupt_sstable(sst);

        testlog.info("Scrub in segregate mode");

        compaction::compaction_type_options::scrub opts = {};
        opts.operation_mode = compaction::compaction_type_options::scrub::mode::segregate;
        opts.drop_unfixable = compaction::compaction_type_options::scrub::drop_unfixable_sstables::yes;

        BOOST_REQUIRE_NO_THROW(table->get_compaction_manager().perform_sstable_scrub(ts, opts, tasks::task_info{}).get());

        BOOST_REQUIRE(in_strategy_sstables(ts).get().empty());
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

    constexpr std::array<compaction::compaction_type_options::scrub::quarantine_mode, 3> quarantine_modes = {
        compaction::compaction_type_options::scrub::quarantine_mode::include,
        compaction::compaction_type_options::scrub::quarantine_mode::exclude,
        compaction::compaction_type_options::scrub::quarantine_mode::only,
    };
    for (auto qmode : quarantine_modes) {
        testlog.info("Checking qurantine mode {}", qmode);
        test.run(schema, corrupt_muts, [&] (table_for_tests& table, compaction::compaction_group_view& ts, std::vector<sstables::shared_sstable> sstables) {
            BOOST_REQUIRE(sstables.size() == 1);
            auto sst = sstables.front();

            auto permit = test.env().make_reader_permit();

            testlog.info("Scrub in validate mode");

            // We expect the scrub with mode=scrub::mode::validate to quarantine the sstable.
            compaction::compaction_type_options::scrub opts = {};
            opts.operation_mode = compaction::compaction_type_options::scrub::mode::validate;
            table->get_compaction_manager().perform_sstable_scrub(ts, opts, tasks::task_info{}).get();

            BOOST_REQUIRE(in_strategy_sstables(ts).get().empty());
            BOOST_REQUIRE(sst->is_quarantined());
            verify_fragments({sst}, permit, corrupt_fragments);

            testlog.info("Scrub in segregate mode with quarantine_mode {}", qmode);

            // We expect the scrub with mode=scrub::mode::segregate to fix all out-of-order data.
            opts.operation_mode = compaction::compaction_type_options::scrub::mode::segregate;
            opts.quarantine_operation_mode = qmode;
            table->get_compaction_manager().perform_sstable_scrub(ts, opts, tasks::task_info{}).get();

            switch (qmode) {
            case compaction::compaction_type_options::scrub::quarantine_mode::include:
            case compaction::compaction_type_options::scrub::quarantine_mode::only:
                // The sstable should be found and scrubbed when scrub::quarantine_mode is scrub::quarantine_mode::{include,only}
                testlog.info("Scrub resulted in {} sstables", in_strategy_sstables(ts).get().size());
                BOOST_REQUIRE(in_strategy_sstables(ts).get().size() > 1);
                verify_fragments(in_strategy_sstables(ts).get(), permit, scrubbed_fragments);
                break;
            case compaction::compaction_type_options::scrub::quarantine_mode::exclude:
                // The sstable should not be found when scrub::quarantine_mode is scrub::quarantine_mode::exclude
                BOOST_REQUIRE(in_strategy_sstables(ts).get().empty());
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
    bool failed_to_fix_sstable = false;
    mutation_writer::segregate_by_partition(
            make_scrubbing_reader(make_mutation_reader_from_fragments(schema, permit, std::move(all_fragments)), compaction::compaction_type_options::scrub::mode::segregate,
                    validation_errors, failed_to_fix_sstable, compaction::compaction_type_options::scrub::drop_unfixable_sstables::no),
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
    auto schema = schema_builder(this_smp_shard_count(), "ks", get_name())
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
    bool failed_to_fix_sstable = false;
    auto r = assert_that(make_scrubbing_reader(make_mutation_reader_from_fragments(schema, permit, std::move(corrupt_fragments)),
                compaction::compaction_type_options::scrub::mode::skip, validation_errors, failed_to_fix_sstable, compaction::compaction_type_options::scrub::drop_unfixable_sstables::no));
    for (const auto& mf : scrubbed_fragments) {
       testlog.info("Expecting {}", mutation_fragment_v2::printer(*schema, mf));
       r.produces(*schema, mf);
    }
    r.produces_end_of_stream();
}

void scrubbed_sstable_removal_fn(test_env& env) {
    // Test to verify that scrub removes the source sstable from the table upon completion
    // https://github.com/scylladb/scylladb/issues/20030
    simple_schema ss;
    auto s = ss.schema();
    auto pk = ss.make_pkey();

    auto mut1 = mutation(s, pk);
    mut1.partition().apply_insert(*s, ss.make_ckey(0), ss.new_timestamp());
    auto sst = make_sstable_containing(env.make_sstable(s), {std::move(mut1)}).get();

    auto cf = env.make_table_for_tests(s);
    auto close_cf = deferred_stop(cf);

    // add the sstable to cf's maintenance set
    cf->add_sstable_and_update_cache(sst, sstables::offstrategy::yes).get();
    auto& cf_ts = cf.as_compaction_group_view();
    auto maintenance_sst_set = cf_ts.maintenance_sstable_set().get();
    BOOST_REQUIRE_EQUAL(maintenance_sst_set->size(), 1);
    BOOST_REQUIRE_EQUAL(*maintenance_sst_set->all()->begin(), sst);
    // confirm main sstable_set is empty
    BOOST_REQUIRE_EQUAL(cf_ts.main_sstable_set().get()->size(), 0);

    // Perform scrub on the table
    cf->get_compaction_manager().perform_sstable_scrub(cf_ts, {}, {}).get();

    // main set should have the resultant sst and the maintenance set should be empty now
    BOOST_REQUIRE_EQUAL(cf_ts.main_sstable_set().get()->size(), 1);
    BOOST_REQUIRE_EQUAL(cf_ts.maintenance_sstable_set().get()->size(), 0);

    // Now that there is an sstable in main set, perform scrub on the table
    // again to verify that the result ends up again in main sstable_set
    cf->get_compaction_manager().perform_sstable_scrub(cf_ts, {}, {}).get();
    BOOST_REQUIRE_EQUAL(cf_ts.main_sstable_set().get()->size(), 1);
    BOOST_REQUIRE_EQUAL(cf_ts.maintenance_sstable_set().get()->size(), 0);
}

SEASTAR_TEST_CASE(scrubbed_sstable_removal_test) {
    return test_env::do_with_async([](test_env& env) { scrubbed_sstable_removal_fn(env); });
}

SEASTAR_TEST_CASE(scrubbed_sstable_removal_test_s3, *boost::unit_test::precondition(tests::has_scylla_test_env)) {
    return test_env::do_with_async([](test_env& env) { scrubbed_sstable_removal_fn(env); }, test_env_config{.storage = make_test_object_storage_options("S3")});
}

SEASTAR_FIXTURE_TEST_CASE(scrubbed_sstable_removal_test_gcs, gcs_fixture, *tests::check_run_test_decorator("ENABLE_GCP_STORAGE_TEST", true)) {
    return test_env::do_with_async([](test_env& env) { scrubbed_sstable_removal_fn(env); }, test_env_config{.storage = make_test_object_storage_options("GS")});
}

// Test to verify that `scrub --validate` is not affected by a concurrent regular compaction

void compact_uncompressed_sstable_during_scrub_validate_fn(test_env& env) {
    auto s = schema_builder(this_smp_shard_count(), "unlinked_sstable_scrub_test", "t1")
        .with_column("pk", utf8_type, column_kind::partition_key)
        .with_column("ck", utf8_type, column_kind::clustering_key)
        .with_column("v", utf8_type)
        .set_compressor_params(compression_parameters::no_compression())
        .build();
    auto cf = env.make_table_for_tests(s);
    auto close_cf = deferred_stop(cf);
    cf->disable_auto_compaction().get();

    // Add 2 sstables to the column family
    api::timestamp_type timestamp = api::min_timestamp;
    for (int i = 0; i < 2; i++) {
        auto mut = mutation(s, tests::generate_partition_key(s));
        mut.partition().apply_insert(*s, tests::generate_clustering_key(s), timestamp++);
        auto sst = make_sstable_containing(env.make_sstable(s), {std::move(mut)}).get();
        cf->add_sstable_and_update_cache(std::move(sst)).get();
    }

    // Start a scrub on the table; Use an injector to pause the scrub after it has collected the sstables to be scrubbed.
    utils::get_local_injector().enable("sstable_validate/pause");
    compaction::compaction_type_options::scrub opts = {};
    opts.operation_mode = compaction::compaction_type_options::scrub::mode::validate;
    auto scrub_task = cf->get_compaction_manager().perform_sstable_scrub(cf.as_compaction_group_view(), opts, {});

    // When the scrub is paused, compact the two sstables in the table; this should not affect the scrub
    cf->get_compaction_manager().perform_major_compaction(cf.as_compaction_group_view(), {}).get();

    // Now resume the scrub and ensure it completes without error
    utils::get_local_injector().receive_message("sstable_validate/pause");
    BOOST_REQUIRE_EQUAL(scrub_task.get().value().validation_errors, 0);

    // Test the reverse case : start a compaction and pause it, then start a scrub --validate
    utils::get_local_injector().enable("major_compaction_wait");
    auto compaction_task = cf->get_compaction_manager().perform_major_compaction(cf.as_compaction_group_view(), {});
    // Perform scrub --validate while compaction is in progress
    scrub_task = cf->get_compaction_manager().perform_sstable_scrub(cf.as_compaction_group_view(), opts, {});
    // Resume compaction and ensure that it doesn't interfere with the scrub
    utils::get_local_injector().receive_message("major_compaction_wait");
    BOOST_REQUIRE_EQUAL(scrub_task.get().value().validation_errors, 0);
    compaction_task.get();
}

SEASTAR_TEST_CASE(compact_uncompressed_sstable_during_scrub_validate_test) {
#ifndef SCYLLA_ENABLE_ERROR_INJECTION
    fmt::print("Skipping test as it depends on error injection. Please run in mode where it's enabled (debug,dev).\n");
    return make_ready_future();
#endif
    return test_env::do_with_async([](test_env& env) { compact_uncompressed_sstable_during_scrub_validate_fn(env); });
}

SEASTAR_TEST_CASE(compact_uncompressed_sstable_during_scrub_validate_test_s3, *boost::unit_test::precondition(tests::has_scylla_test_env)) {
#ifndef SCYLLA_ENABLE_ERROR_INJECTION
    fmt::print("Skipping test as it depends on error injection. Please run in mode where it's enabled (debug,dev).\n");
    return make_ready_future();
#endif
    return test_env::do_with_async([](test_env& env) { compact_uncompressed_sstable_during_scrub_validate_fn(env); },
                                   test_env_config{.storage = make_test_object_storage_options("S3")});
}

SEASTAR_FIXTURE_TEST_CASE(compact_uncompressed_sstable_during_scrub_validate_test_gcs,
                          gcs_fixture,
                          *tests::check_run_test_decorator("ENABLE_GCP_STORAGE_TEST", true)) {
#ifndef SCYLLA_ENABLE_ERROR_INJECTION
    fmt::print("Skipping test as it depends on error injection. Please run in mode where it's enabled (debug,dev).\n");
    return make_ready_future();
#endif
    return test_env::do_with_async([](test_env& env) { compact_uncompressed_sstable_during_scrub_validate_fn(env); },
                                   test_env_config{.storage = make_test_object_storage_options("GS")});
}

static void test_scrub_validates_component_digests(test_env& env, sstables::component_type type) {
    scrub_test_framework<random_schema::yes> test(compress_sstable::no);

    auto schema = test.schema();

    auto muts = tests::generate_random_mutations(test.random_schema()).get();

    test.run(schema, muts, [type] (table_for_tests& table, compaction::compaction_group_view& ts, std::vector<sstables::shared_sstable> sstables) {
        BOOST_REQUIRE(sstables.size() == 1);
        auto sst = sstables.front();
        scoped_error_injection injection{"sstable_digest_mismatch_found"};

        corrupt_sstable(sst, type);

        compaction::compaction_type_options::scrub opts = {
            .operation_mode = compaction::compaction_type_options::scrub::mode::validate,
        };
        auto scrub = table->get_compaction_manager().perform_sstable_scrub(ts, opts, tasks::task_info{}).get();
        BOOST_REQUIRE(scrub);
        auto errors = scrub->validation_errors;
        BOOST_REQUIRE_NE(errors, 0);
        BOOST_REQUIRE_GT(utils::get_local_injector().enter_count_on_all("sstable_digest_mismatch_found").get(), 0);
    });
}

SEASTAR_TEST_CASE(test_scrub_validates_toc_digest) {
#ifndef SCYLLA_ENABLE_ERROR_INJECTION
    fmt::print("Skipping test as it depends on error injection. Please run in mode where it's enabled (debug,dev).\n");
    return make_ready_future();
#endif
    return test_env::do_with_async([](test_env& env) { test_scrub_validates_component_digests(env, component_type::TOC); });
}

SEASTAR_TEST_CASE(test_scrub_validates_scylla_digest) {
#ifndef SCYLLA_ENABLE_ERROR_INJECTION
    fmt::print("Skipping test as it depends on error injection. Please run in mode where it's enabled (debug,dev).\n");
    return make_ready_future();
#endif
    return test_env::do_with_async([](test_env& env) { test_scrub_validates_component_digests(env, component_type::Scylla); });
}

SEASTAR_TEST_CASE(test_scrub_validates_index_digest) {
#ifndef SCYLLA_ENABLE_ERROR_INJECTION
    fmt::print("Skipping test as it depends on error injection. Please run in mode where it's enabled (debug,dev).\n");
    return make_ready_future();
#endif
    return test_env::do_with_async([](test_env& env) { test_scrub_validates_component_digests(env, component_type::Index); });
}

SEASTAR_TEST_CASE(test_scrub_validates_statistics_digest) {
#ifndef SCYLLA_ENABLE_ERROR_INJECTION
    fmt::print("Skipping test as it depends on error injection. Please run in mode where it's enabled (debug,dev).\n");
    return make_ready_future();
#endif
    return test_env::do_with_async([](test_env& env) { test_scrub_validates_component_digests(env, component_type::Statistics); });
}

BOOST_AUTO_TEST_SUITE_END()