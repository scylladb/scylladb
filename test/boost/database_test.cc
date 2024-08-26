/*
 * Copyright (C) 2016-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */


#include <seastar/core/seastar.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/util/file.hh>

#include "test/lib/scylla_test_case.hh"
#include <seastar/testing/thread_test_case.hh>
#include <utility>
#include <fmt/ranges.h>
#include <fmt/std.h>

#include "test/lib/cql_test_env.hh"
#include "test/lib/result_set_assertions.hh"
#include "test/lib/log.hh"
#include "test/lib/random_utils.hh"
#include "test/lib/simple_schema.hh"
#include "test/lib/test_utils.hh"
#include "test/lib/key_utils.hh"

#include "replica/database.hh"
#include "utils/assert.hh"
#include "utils/lister.hh"
#include "partition_slice_builder.hh"
#include "mutation/frozen_mutation.hh"
#include "test/lib/mutation_source_test.hh"
#include "schema/schema_builder.hh"
#include "service/migration_manager.hh"
#include "sstables/sstables.hh"
#include "sstables/generation_type.hh"
#include "db/config.hh"
#include "db/commitlog/commitlog_replayer.hh"
#include "db/commitlog/commitlog.hh"
#include "test/lib/tmpdir.hh"
#include "db/data_listeners.hh"
#include "multishard_mutation_query.hh"
#include "transport/messages/result_message.hh"
#include "compaction/compaction_manager.hh"
#include "db/snapshot-ctl.hh"
#include "replica/mutation_dump.hh"

using namespace std::chrono_literals;
using namespace sstables;

class database_test {
    replica::database& _db;
public:
    explicit database_test(replica::database& db) : _db(db) { }

    reader_concurrency_semaphore& get_user_read_concurrency_semaphore() {
        return _db._read_concurrency_sem;
    }
    reader_concurrency_semaphore& get_streaming_read_concurrency_semaphore() {
        return _db._streaming_concurrency_sem;
    }
    reader_concurrency_semaphore& get_system_read_concurrency_semaphore() {
        return _db._system_read_concurrency_sem;
    }
};

static future<> apply_mutation(sharded<replica::database>& sharded_db, table_id uuid, const mutation& m, bool do_flush = false,
        db::commitlog::force_sync fs = db::commitlog::force_sync::no, db::timeout_clock::time_point timeout = db::no_timeout) {
    auto& t = sharded_db.local().find_column_family(uuid);
    auto shard = t.shard_for_reads(m.token());
    return sharded_db.invoke_on(shard, [uuid, fm = freeze(m), do_flush, fs, timeout] (replica::database& db) {
        auto& t = db.find_column_family(uuid);
        return db.apply(t.schema(), fm, tracing::trace_state_ptr(), fs, timeout).then([do_flush, &t] {
            return do_flush ? t.flush() : make_ready_future<>();
        });
    });
}

future<> do_with_cql_env_and_compaction_groups_cgs(unsigned cgs, std::function<void(cql_test_env&)> func, cql_test_config cfg = {}, thread_attributes thread_attr = {}) {
    // clean the dir before running
    if (cfg.db_config->data_file_directories.is_set()) {
        co_await recursive_remove_directory(fs::path(cfg.db_config->data_file_directories()[0]));
        co_await recursive_touch_directory(cfg.db_config->data_file_directories()[0]);
    }
    // TODO: perhaps map log2_compaction_groups into initial_tablets when creating the testing keyspace.
    co_await do_with_cql_env_thread(func, cfg, thread_attr);
}

future<> do_with_cql_env_and_compaction_groups(std::function<void(cql_test_env&)> func, cql_test_config cfg = {}, thread_attributes thread_attr = {}) {
    std::vector<unsigned> x_log2_compaction_group_values = { 0 /* 1 CG */ };
    for (auto x_log2_compaction_groups : x_log2_compaction_group_values) {
        co_await do_with_cql_env_and_compaction_groups_cgs(x_log2_compaction_groups, func, cfg, thread_attr);
    }
}

SEASTAR_TEST_CASE(test_safety_after_truncate) {
    auto cfg = make_shared<db::config>();
    cfg->auto_snapshot.set(false);
    return do_with_cql_env_and_compaction_groups([](cql_test_env& e) {
        e.execute_cql("create table ks.cf (k text, v int, primary key (k));").get();
        auto& db = e.local_db();
        sstring ks_name = "ks";
        sstring cf_name = "cf";
        auto s = db.find_schema(ks_name, cf_name);
        auto&& table = db.find_column_family(s);
        auto uuid = s->id();

        std::vector<size_t> keys_per_shard;
        std::vector<dht::partition_range_vector> pranges_per_shard;
        keys_per_shard.resize(smp::count);
        pranges_per_shard.resize(smp::count);
        for (uint32_t i = 1; i <= 1000; ++i) {
            auto pkey = partition_key::from_single_value(*s, to_bytes(fmt::format("key{}", i)));
            mutation m(s, pkey);
            m.set_clustered_cell(clustering_key_prefix::make_empty(), "v", int32_t(42), {});
            auto shard = table.shard_for_reads(m.token());
            keys_per_shard[shard]++;
            pranges_per_shard[shard].emplace_back(dht::partition_range::make_singular(dht::decorate_key(*s, std::move(pkey))));
            apply_mutation(e.db(), uuid, m).get();
        }

        auto assert_query_result = [&] (const std::vector<size_t>& expected_sizes) {
            auto max_size = std::numeric_limits<size_t>::max();
            auto cmd = query::read_command(s->id(), s->version(), partition_slice_builder(*s).build(), query::max_result_size(max_size),
                    query::tombstone_limit::max, query::row_limit(1000));
            e.db().invoke_on_all([&] (replica::database& db) -> future<> {
                auto shard = this_shard_id();
                auto s = db.find_schema(uuid);
                auto&& [result, cache_tempature] = co_await db.query(s, cmd, query::result_options::only_result(), pranges_per_shard[shard], nullptr, db::no_timeout);
                assert_that(query::result_set::from_raw_result(s, cmd.slice, *result)).has_size(expected_sizes[shard]);
            }).get();
        };
        assert_query_result(keys_per_shard);

        replica::database::truncate_table_on_all_shards(e.db(), e.get_system_keyspace(), "ks", "cf").get();

        for (auto it = keys_per_shard.begin(); it < keys_per_shard.end(); ++it) {
            *it = 0;
        }
        assert_query_result(keys_per_shard);

        e.db().invoke_on_all([&] (replica::database& db) -> future<> {
            auto cl = db.commitlog();
            auto rp = co_await db::commitlog_replayer::create_replayer(e.db(), e.get_system_keyspace());
            auto paths = co_await cl->list_existing_segments();
            co_await rp.recover(paths, db::commitlog::descriptor::FILENAME_PREFIX);
        }).get();

        assert_query_result(keys_per_shard);
        return make_ready_future<>();
    }, cfg);
}

// Reproducer for:
//   https://github.com/scylladb/scylla/issues/10421
//   https://github.com/scylladb/scylla/issues/10423
SEASTAR_TEST_CASE(test_truncate_without_snapshot_during_writes) {
    auto cfg = make_shared<db::config>();
    cfg->auto_snapshot.set(false);
    return do_with_cql_env_and_compaction_groups([] (cql_test_env& e) {
        sstring ks_name = "ks";
        sstring cf_name = "cf";
        e.execute_cql(fmt::format("create table {}.{} (k text, v int, primary key (k));", ks_name, cf_name)).get();
        auto& db = e.local_db();
        auto uuid = db.find_uuid(ks_name, cf_name);
        auto s = db.find_column_family(uuid).schema();
        int count = 0;

        auto insert_data = [&] (uint32_t begin, uint32_t end) {
            return parallel_for_each(boost::irange(begin, end), [&] (auto i) {
                auto pkey = partition_key::from_single_value(*s, to_bytes(fmt::format("key-{}", tests::random::get_int<uint64_t>())));
                mutation m(s, pkey);
                m.set_clustered_cell(clustering_key_prefix::make_empty(), "v", int32_t(42), {});
                return apply_mutation(e.db(), uuid, m, true /* do_flush */).finally([&] {
                    ++count;
                });
            });
        };

        uint32_t num_keys = 1000;

        auto f0 = insert_data(0, num_keys);
        auto f1 = do_until([&] { return std::cmp_greater_equal(count, num_keys); }, [&, ts = db_clock::now()] {
            return replica::database::truncate_table_on_all_shards(e.db(), e.get_system_keyspace(), "ks", "cf", ts, false /* with_snapshot */).then([] {
                return yield();
            });
        });
        f0.get();
        f1.get();
    }, cfg);
}

SEASTAR_TEST_CASE(test_querying_with_limits) {
    return do_with_cql_env_and_compaction_groups([](cql_test_env& e) {
            // FIXME: restore indent.
            e.execute_cql("create table ks.cf (k text, v int, primary key (k));").get();
            auto& db = e.local_db();
            auto s = db.find_schema("ks", "cf");
            auto&& table = db.find_column_family(s);
            auto uuid = s->id();
            std::vector<size_t> keys_per_shard;
            std::vector<dht::partition_range_vector> pranges_per_shard;
            keys_per_shard.resize(smp::count);
            pranges_per_shard.resize(smp::count);
            for (uint32_t i = 1; i <= 3 * smp::count; ++i) {
                auto pkey = partition_key::from_single_value(*s, to_bytes(format("key{:d}", i)));
                mutation m(s, pkey);
                m.partition().apply(tombstone(api::timestamp_type(1), gc_clock::now()));
                apply_mutation(e.db(), uuid, m).get();
                auto shard = table.shard_for_reads(m.token());
                pranges_per_shard[shard].emplace_back(dht::partition_range::make_singular(dht::decorate_key(*s, std::move(pkey))));
            }
            for (uint32_t i = 3 * smp::count; i <= 8 * smp::count; ++i) {
                auto pkey = partition_key::from_single_value(*s, to_bytes(format("key{:d}", i)));
                mutation m(s, pkey);
                m.set_clustered_cell(clustering_key_prefix::make_empty(), "v", int32_t(42), 1);
                apply_mutation(e.db(), uuid, m).get();
                auto shard = table.shard_for_reads(m.token());
                keys_per_shard[shard]++;
                pranges_per_shard[shard].emplace_back(dht::partition_range::make_singular(dht::decorate_key(*s, std::move(pkey))));
            }

            auto max_size = std::numeric_limits<size_t>::max();
            {
                auto cmd = query::read_command(s->id(), s->version(), partition_slice_builder(*s).build(), query::max_result_size(max_size),
                        query::tombstone_limit::max, query::row_limit(3));
                e.db().invoke_on_all([&] (replica::database& db) -> future<> {
                    auto shard = this_shard_id();
                    auto s = db.find_schema(uuid);
                    auto result = std::get<0>(co_await db.query(s, cmd, query::result_options::only_result(), pranges_per_shard[shard], nullptr, db::no_timeout));
                    auto expected_size = std::min<size_t>(keys_per_shard[shard], 3);
                    assert_that(query::result_set::from_raw_result(s, cmd.slice, *result)).has_size(expected_size);
                }).get();
            }

            {
                auto cmd = query::read_command(s->id(), s->version(), partition_slice_builder(*s).build(), query::max_result_size(max_size),
                        query::tombstone_limit::max, query::row_limit(query::max_rows), query::partition_limit(5));
                e.db().invoke_on_all([&] (replica::database& db) -> future<> {
                    auto shard = this_shard_id();
                    auto s = db.find_schema(uuid);
                    auto result = std::get<0>(co_await db.query(s, cmd, query::result_options::only_result(), pranges_per_shard[shard], nullptr, db::no_timeout));
                    auto expected_size = std::min<size_t>(keys_per_shard[shard], 5);
                    assert_that(query::result_set::from_raw_result(s, cmd.slice, *result)).has_size(expected_size);
                }).get();
            }

            {
                auto cmd = query::read_command(s->id(), s->version(), partition_slice_builder(*s).build(), query::max_result_size(max_size),
                        query::tombstone_limit::max, query::row_limit(query::max_rows), query::partition_limit(3));
                e.db().invoke_on_all([&] (replica::database& db) -> future<> {
                    auto shard = this_shard_id();
                    auto s = db.find_schema(uuid);
                    auto result = std::get<0>(co_await db.query(s, cmd, query::result_options::only_result(), pranges_per_shard[shard], nullptr, db::no_timeout));
                    auto expected_size = std::min<size_t>(keys_per_shard[shard], 3);
                    assert_that(query::result_set::from_raw_result(s, cmd.slice, *result)).has_size(expected_size);
                }).get();
            }
    });
}

static void test_database(void (*run_tests)(populate_fn_ex, bool), unsigned cgs) {
    do_with_cql_env_and_compaction_groups_cgs(cgs, [run_tests] (cql_test_env& e) {
        run_tests([&] (schema_ptr s, const std::vector<mutation>& partitions, gc_clock::time_point) -> mutation_source {
            auto& mm = e.migration_manager().local();
            try {
                auto group0_guard = mm.start_group0_operation().get();
                auto ts = group0_guard.write_timestamp();
                e.local_db().find_column_family(s->ks_name(), s->cf_name());
                mm.announce(service::prepare_column_family_drop_announcement(mm.get_storage_proxy(), s->ks_name(), s->cf_name(), ts).get(), std::move(group0_guard), "").get();
            } catch (const replica::no_such_column_family&) {
                // expected
            }
            auto group0_guard = mm.start_group0_operation().get();
            auto ts = group0_guard.write_timestamp();
            mm.announce(service::prepare_new_column_family_announcement(mm.get_storage_proxy(), s, ts).get(), std::move(group0_guard), "").get();
            replica::column_family& cf = e.local_db().find_column_family(s);
            auto uuid = cf.schema()->id();
            for (auto&& m : partitions) {
                apply_mutation(e.db(), uuid, m).get();
            }
            cf.flush().get();
            cf.get_row_cache().invalidate(row_cache::external_updater([] {})).get();
            return mutation_source([&] (schema_ptr s,
                    reader_permit permit,
                    const dht::partition_range& range,
                    const query::partition_slice& slice,
                    tracing::trace_state_ptr trace_state,
                    streamed_mutation::forwarding fwd,
                    mutation_reader::forwarding fwd_mr) {
                return cf.make_reader_v2(s, std::move(permit), range, slice, std::move(trace_state), fwd, fwd_mr);
            });
        }, true);
    }).get();
}

// plain cg0
SEASTAR_THREAD_TEST_CASE(test_database_with_data_in_sstables_is_a_mutation_source_plain_basic_cg0) {
    test_database(run_mutation_source_tests_plain_basic, 0);
}

SEASTAR_THREAD_TEST_CASE(test_database_with_data_in_sstables_is_a_mutation_source_plain_reader_conversion_cg0) {
    test_database(run_mutation_source_tests_plain_reader_conversion, 0);
}

SEASTAR_THREAD_TEST_CASE(test_database_with_data_in_sstables_is_a_mutation_source_plain_fragments_monotonic_cg0) {
    test_database(run_mutation_source_tests_plain_fragments_monotonic, 0);
}

SEASTAR_THREAD_TEST_CASE(test_database_with_data_in_sstables_is_a_mutation_source_plain_read_back_cg0) {
    test_database(run_mutation_source_tests_plain_read_back, 0);
}

// plain cg1
SEASTAR_THREAD_TEST_CASE(test_database_with_data_in_sstables_is_a_mutation_source_plain_basic_cg1) {
    test_database(run_mutation_source_tests_plain_basic, 1);
}

SEASTAR_THREAD_TEST_CASE(test_database_with_data_in_sstables_is_a_mutation_source_plain_reader_conversion_cg1) {
    test_database(run_mutation_source_tests_plain_reader_conversion, 1);
}

SEASTAR_THREAD_TEST_CASE(test_database_with_data_in_sstables_is_a_mutation_source_plain_fragments_monotonic_cg1) {
    test_database(run_mutation_source_tests_plain_fragments_monotonic, 1);
}

SEASTAR_THREAD_TEST_CASE(test_database_with_data_in_sstables_is_a_mutation_source_plain_read_back_cg1) {
    test_database(run_mutation_source_tests_plain_read_back, 1);
}

// reverse cg0
SEASTAR_THREAD_TEST_CASE(test_database_with_data_in_sstables_is_a_mutation_source_reverse_basic_cg0) {
    test_database(run_mutation_source_tests_reverse_basic, 0);
}

SEASTAR_THREAD_TEST_CASE(test_database_with_data_in_sstables_is_a_mutation_source_reverse_reader_conversion_cg0) {
    test_database(run_mutation_source_tests_reverse_reader_conversion, 0);
}

SEASTAR_THREAD_TEST_CASE(test_database_with_data_in_sstables_is_a_mutation_source_reverse_fragments_monotonic_cg0) {
    test_database(run_mutation_source_tests_reverse_fragments_monotonic, 0);
}

SEASTAR_THREAD_TEST_CASE(test_database_with_data_in_sstables_is_a_mutation_source_reverse_read_back_cg0) {
    test_database(run_mutation_source_tests_reverse_read_back, 0);
}

// reverse cg1
SEASTAR_THREAD_TEST_CASE(test_database_with_data_in_sstables_is_a_mutation_source_reverse_basic_cg1) {
    test_database(run_mutation_source_tests_reverse_basic, 1);
}

SEASTAR_THREAD_TEST_CASE(test_database_with_data_in_sstables_is_a_mutation_source_reverse_reader_conversion_cg1) {
    test_database(run_mutation_source_tests_reverse_reader_conversion, 1);
}

SEASTAR_THREAD_TEST_CASE(test_database_with_data_in_sstables_is_a_mutation_source_reverse_fragments_monotonic_cg1) {
    test_database(run_mutation_source_tests_reverse_fragments_monotonic, 1);
}

SEASTAR_THREAD_TEST_CASE(test_database_with_data_in_sstables_is_a_mutation_source_reverse_read_back_cg1) {
    test_database(run_mutation_source_tests_reverse_read_back, 1);
}

static void require_exist(const sstring& filename, bool should) {
    auto exists = file_exists(filename).get();
    BOOST_REQUIRE_EQUAL(exists, should);
}

static void touch_dir(const sstring& dirname) {
    recursive_touch_directory(dirname).get();
    require_exist(dirname, true);
}

static void touch_file(const sstring& filename) {
    tests::touch_file(filename).get();
    require_exist(filename, true);
}

SEASTAR_THREAD_TEST_CASE(test_distributed_loader_with_incomplete_sstables) {
    using sst = sstables::sstable;

    tmpdir data_dir;
    auto db_cfg_ptr = make_shared<db::config>();
    auto& db_cfg = *db_cfg_ptr;

    db_cfg.data_file_directories({data_dir.path().string()}, db::config::config_source::CommandLine);

    // Create incomplete sstables in test data directory
    sstring ks = "system";
    sstring cf = "peers-37f71aca7dc2383ba70672528af04d4f";
    sstring sst_dir = (data_dir.path() / std::string_view(ks) / std::string_view(cf)).string();

    auto temp_sst_dir_2 = fmt::format("{}/{}{}", sst_dir, generation_from_value(2), tempdir_extension);
    touch_dir(temp_sst_dir_2);

    auto temp_sst_dir_3 = fmt::format("{}/{}{}", sst_dir, generation_from_value(3), tempdir_extension);
    touch_dir(temp_sst_dir_3);

    auto temp_file_name = sst::filename(temp_sst_dir_3, ks, cf, sstables::get_highest_sstable_version(), generation_from_value(3), sst::format_types::big, component_type::TemporaryTOC);
    touch_file(temp_file_name);

    temp_file_name = sst::filename(sst_dir, ks, cf, sstables::get_highest_sstable_version(), generation_from_value(4), sst::format_types::big, component_type::TemporaryTOC);
    touch_file(temp_file_name);
    temp_file_name = sst::filename(sst_dir, ks, cf, sstables::get_highest_sstable_version(), generation_from_value(4), sst::format_types::big, component_type::Data);
    touch_file(temp_file_name);

    do_with_cql_env_and_compaction_groups([&sst_dir, &ks, &cf, &temp_sst_dir_2, &temp_sst_dir_3] (cql_test_env& e) {
        require_exist(temp_sst_dir_2, false);
        require_exist(temp_sst_dir_3, false);

        require_exist(sst::filename(sst_dir, ks, cf, sstables::get_highest_sstable_version(), generation_from_value(4), sst::format_types::big, component_type::TemporaryTOC), false);
        require_exist(sst::filename(sst_dir, ks, cf, sstables::get_highest_sstable_version(), generation_from_value(4), sst::format_types::big, component_type::Data), false);
    }, db_cfg_ptr).get();
}

SEASTAR_THREAD_TEST_CASE(test_distributed_loader_with_pending_delete) {
    using sst = sstables::sstable;

    tmpdir data_dir;
    auto db_cfg_ptr = make_shared<db::config>();
    auto& db_cfg = *db_cfg_ptr;

    db_cfg.data_file_directories({data_dir.path().string()}, db::config::config_source::CommandLine);

    // Create incomplete sstables in test data directory
    sstring ks = "system";
    sstring cf = "peers-37f71aca7dc2383ba70672528af04d4f";
    sstring sst_dir = (data_dir.path() / std::string_view(ks) / std::string_view(cf)).string();
    sstring pending_delete_dir = sst_dir + "/" + sstables::pending_delete_dir;

    auto write_file = [] (const sstring& file_name, const sstring& text) {
        auto f = open_file_dma(file_name, open_flags::wo | open_flags::create | open_flags::truncate).get();
        auto os = make_file_output_stream(f, file_output_stream_options{}).get();
        os.write(text).get();
        os.flush().get();
        os.close().get();
        require_exist(file_name, true);
    };

    auto component_basename = [&ks, &cf] (sstables::generation_type gen, component_type ctype) {
        return sst::component_basename(ks, cf, sstables::get_highest_sstable_version(), gen, sst::format_types::big, ctype);
    };

    auto gen_filename = [&sst_dir, &ks, &cf] (sstables::generation_type gen, component_type ctype) {
        return sst::filename(sst_dir, ks, cf, sstables::get_highest_sstable_version(), gen, sst::format_types::big, ctype);
    };

    touch_dir(pending_delete_dir);

    // Empty log file
    touch_file(pending_delete_dir + "/sstables-0-0.log");

    // Empty temporary log file
    touch_file(pending_delete_dir + "/sstables-1-1.log.tmp");

    const sstring toc_text = "TOC.txt\nData.db\n";

    sstables::sstable_generation_generator gen_generator(0);
    std::vector<sstables::generation_type> gen;
    constexpr size_t num_gens = 9;
    std::generate_n(std::back_inserter(gen), num_gens, [&] {
        // we assumes the integer-based generation identifier in this test, so disable
        // uuid_identifier here
        return gen_generator(sstables::uuid_identifiers::no);
    });

    // Regular log file with single entry
    write_file(gen_filename(gen[2], component_type::TOC), toc_text);
    touch_file(gen_filename(gen[2], component_type::Data));
    write_file(pending_delete_dir + "/sstables-2-2.log",
               component_basename(gen[2], component_type::TOC) + "\n");

    // Temporary log file with single entry
    write_file(pending_delete_dir + "/sstables-3-3.log.tmp",
               component_basename(gen[3], component_type::TOC) + "\n");

    // Regular log file with multiple entries
    write_file(gen_filename(gen[4], component_type::TOC), toc_text);
    touch_file(gen_filename(gen[4], component_type::Data));
    write_file(gen_filename(gen[5], component_type::TOC), toc_text);
    touch_file(gen_filename(gen[5], component_type::Data));
    write_file(pending_delete_dir + "/sstables-4-5.log",
               component_basename(gen[4], component_type::TOC) + "\n" +
               component_basename(gen[5], component_type::TOC) + "\n");

    // Regular log file with multiple entries and some deleted sstables
    write_file(gen_filename(gen[6], component_type::TemporaryTOC), toc_text);
    touch_file(gen_filename(gen[6], component_type::Data));
    write_file(gen_filename(gen[7], component_type::TemporaryTOC), toc_text);
    write_file(pending_delete_dir + "/sstables-6-8.log",
               component_basename(gen[6], component_type::TOC) + "\n" +
               component_basename(gen[7], component_type::TOC) + "\n" +
               component_basename(gen[8], component_type::TOC) + "\n");

    do_with_cql_env_and_compaction_groups([&] (cql_test_env& e) {
        // Empty log file
        require_exist(pending_delete_dir + "/sstables-0-0.log", false);

        // Empty temporary log file
        require_exist(pending_delete_dir + "/sstables-1-1.log.tmp", false);

        // Regular log file with single entry
        require_exist(gen_filename(gen[2], component_type::TOC), false);
        require_exist(gen_filename(gen[2], component_type::Data), false);
        require_exist(pending_delete_dir + "/sstables-2-2.log", false);

        // Temporary log file with single entry
        require_exist(pending_delete_dir + "/sstables-3-3.log.tmp", false);

        // Regular log file with multiple entries
        require_exist(gen_filename(gen[4], component_type::TOC), false);
        require_exist(gen_filename(gen[4], component_type::Data), false);
        require_exist(gen_filename(gen[5], component_type::TOC), false);
        require_exist(gen_filename(gen[5], component_type::Data), false);
        require_exist(pending_delete_dir + "/sstables-4-5.log", false);

        // Regular log file with multiple entries and some deleted sstables
        require_exist(gen_filename(gen[6], component_type::TemporaryTOC), false);
        require_exist(gen_filename(gen[6], component_type::Data), false);
        require_exist(gen_filename(gen[7], component_type::TemporaryTOC), false);
        require_exist(pending_delete_dir + "/sstables-6-8.log", false);
    }, db_cfg_ptr).get();
}

// Snapshot tests and their helpers
future<> do_with_some_data(std::vector<sstring> cf_names, std::function<future<> (cql_test_env& env)> func, shared_ptr<db::config> db_cfg_ptr = {}) {
    return seastar::async([cf_names = std::move(cf_names), func = std::move(func), db_cfg_ptr = std::move(db_cfg_ptr)] () mutable {
        lw_shared_ptr<tmpdir> tmpdir_for_data;
        if (!db_cfg_ptr) {
            tmpdir_for_data = make_lw_shared<tmpdir>();
            db_cfg_ptr = make_shared<db::config>();
            db_cfg_ptr->data_file_directories(std::vector<sstring>({ tmpdir_for_data->path().string() }));
        }
        do_with_cql_env_and_compaction_groups([cf_names = std::move(cf_names), func = std::move(func)] (cql_test_env& e) {
            for (const auto& cf_name : cf_names) {
                e.create_table([&cf_name] (std::string_view ks_name) {
                    return *schema_builder(ks_name, cf_name)
                            .with_column("p1", utf8_type, column_kind::partition_key)
                            .with_column("c1", int32_type, column_kind::clustering_key)
                            .with_column("c2", int32_type, column_kind::clustering_key)
                            .with_column("r1", int32_type)
                            .build();
                }).get();
                e.execute_cql(fmt::format("insert into {} (p1, c1, c2, r1) values ('key1', 1, 2, 3);", cf_name)).get();
                e.execute_cql(fmt::format("insert into {} (p1, c1, c2, r1) values ('key1', 2, 2, 3);", cf_name)).get();
                e.execute_cql(fmt::format("insert into {} (p1, c1, c2, r1) values ('key1', 3, 2, 3);", cf_name)).get();
                e.execute_cql(fmt::format("insert into {} (p1, c1, c2, r1) values ('key2', 4, 5, 6);", cf_name)).get();
                e.execute_cql(fmt::format("insert into {} (p1, c1, c2, r1) values ('key2', 5, 5, 6);", cf_name)).get();
                e.execute_cql(fmt::format("insert into {} (p1, c1, c2, r1) values ('key2', 6, 5, 6);", cf_name)).get();
            }

            func(e).get();
        }, db_cfg_ptr).get();
    });
}

future<> take_snapshot(sharded<replica::database>& db, bool skip_flush = false, sstring ks_name = "ks", sstring cf_name = "cf", sstring snapshot_name = "test") {
    try {
        co_await replica::database::snapshot_table_on_all_shards(db, ks_name, cf_name, snapshot_name, db::snapshot_ctl::snap_views::no, skip_flush);
    } catch (...) {
        testlog.error("Could not take snapshot for {}.{} snapshot_name={} skip_flush={}: {}",
                ks_name, cf_name, snapshot_name, skip_flush, std::current_exception());
        throw;
    }
}

future<> take_snapshot(cql_test_env& e, bool skip_flush = false) {
    return take_snapshot(e.db(), skip_flush);
}

future<> take_snapshot(cql_test_env& e, sstring ks_name, sstring cf_name, sstring snapshot_name = "test") {
    return take_snapshot(e.db(), false /* skip_flush */, std::move(ks_name), std::move(cf_name), std::move(snapshot_name));
}

SEASTAR_TEST_CASE(snapshot_works) {
    return do_with_some_data({"cf"}, [] (cql_test_env& e) {
        take_snapshot(e).get();

        std::set<sstring> expected = {
            "manifest.json",
        };

        auto& cf = e.local_db().find_column_family("ks", "cf");
        lister::scan_dir(fs::path(cf.dir()), lister::dir_entry_types::of<directory_entry_type::regular>(), [&expected] (fs::path parent_dir, directory_entry de) {
            expected.insert(de.name);
            return make_ready_future<>();
        }).get();
        // snapshot triggered a flush and wrote the data down.
        BOOST_REQUIRE_GT(expected.size(), 1);

        // all files were copied and manifest was generated
        lister::scan_dir((fs::path(cf.dir()) / sstables::snapshots_dir / "test"), lister::dir_entry_types::of<directory_entry_type::regular>(), [&expected] (fs::path parent_dir, directory_entry de) {
            expected.erase(de.name);
            return make_ready_future<>();
        }).get();

        BOOST_REQUIRE_EQUAL(expected.size(), 0);
        return make_ready_future<>();
    });
}

SEASTAR_TEST_CASE(snapshot_skip_flush_works) {
    return do_with_some_data({"cf"}, [] (cql_test_env& e) {
        take_snapshot(e, true /* skip_flush */).get();

        std::set<sstring> expected = {
            "manifest.json",
        };

        auto& cf = e.local_db().find_column_family("ks", "cf");
        lister::scan_dir(fs::path(cf.dir()), lister::dir_entry_types::of<directory_entry_type::regular>(), [&expected] (fs::path parent_dir, directory_entry de) {
            expected.insert(de.name);
            return make_ready_future<>();
        }).get();
        // Snapshot did not trigger a flush.
        // Only "manifest.json" is expected.
        BOOST_REQUIRE_EQUAL(expected.size(), 1);

        // all files were copied and manifest was generated
        lister::scan_dir((fs::path(cf.dir()) / sstables::snapshots_dir / "test"), lister::dir_entry_types::of<directory_entry_type::regular>(), [&expected] (fs::path parent_dir, directory_entry de) {
            expected.erase(de.name);
            return make_ready_future<>();
        }).get();

        BOOST_REQUIRE_EQUAL(expected.size(), 0);
        return make_ready_future<>();
    });
}

SEASTAR_TEST_CASE(snapshot_list_okay) {
    return do_with_some_data({"cf"}, [] (cql_test_env& e) {
        auto& cf = e.local_db().find_column_family("ks", "cf");
        take_snapshot(e).get();

        auto details = cf.get_snapshot_details().get();
        BOOST_REQUIRE_EQUAL(details.size(), 1);

        auto sd = details["test"];
        BOOST_REQUIRE_EQUAL(sd.live, 0);
        BOOST_REQUIRE_GT(sd.total, 0);

        lister::scan_dir(fs::path(cf.dir()), lister::dir_entry_types::of<directory_entry_type::regular>(), [] (fs::path parent_dir, directory_entry de) {
            fs::remove(parent_dir / de.name);
            return make_ready_future<>();
        }).get();

        auto sd_post_deletion = cf.get_snapshot_details().get().at("test");

        BOOST_REQUIRE_EQUAL(sd_post_deletion.total, sd_post_deletion.live);
        BOOST_REQUIRE_EQUAL(sd.total, sd_post_deletion.live);

        return make_ready_future<>();
    });
}

SEASTAR_TEST_CASE(snapshot_list_contains_dropped_tables) {
    return do_with_some_data({"cf1", "cf2", "cf3", "cf4"}, [] (cql_test_env& e) {
        e.execute_cql("DROP TABLE ks.cf1;").get();

        auto details = e.local_db().get_snapshot_details().get();
        BOOST_REQUIRE_EQUAL(details.size(), 1);
        BOOST_REQUIRE_EQUAL(details.begin()->second.size(), 1);

        const auto& sd = details.begin()->second.front().details;
        BOOST_REQUIRE_GT(sd.live, 0);
        BOOST_REQUIRE_EQUAL(sd.total, sd.live);

        take_snapshot(e, "ks", "cf2", "test2").get();
        take_snapshot(e, "ks", "cf3", "test3").get();

        details = e.local_db().get_snapshot_details().get();
        BOOST_REQUIRE_EQUAL(details.size(), 3);

        e.execute_cql("DROP TABLE ks.cf4;").get();

        details = e.local_db().get_snapshot_details().get();
        BOOST_REQUIRE_EQUAL(details.size(), 4);

        for (const auto& [name, r] : details) {
            BOOST_REQUIRE_EQUAL(r.size(), 1);
            const auto& result = r.front();
            const auto& sd = result.details;

            if (name == "test2" || name == "test3") {
                BOOST_REQUIRE_EQUAL(sd.live, 0);
                BOOST_REQUIRE_GT(sd.total, 0);
            } else {
                BOOST_REQUIRE_GT(sd.live, 0);
                BOOST_REQUIRE_EQUAL(sd.total, sd.live);
            }
        }

        return make_ready_future<>();
    });
}

SEASTAR_TEST_CASE(snapshot_list_inexistent) {
    return do_with_some_data({"cf"}, [] (cql_test_env& e) {
        auto& cf = e.local_db().find_column_family("ks", "cf");
        auto details = cf.get_snapshot_details().get();
        BOOST_REQUIRE_EQUAL(details.size(), 0);
        return make_ready_future<>();
    });
}


SEASTAR_TEST_CASE(clear_snapshot) {
    return do_with_some_data({"cf"}, [] (cql_test_env& e) {
        take_snapshot(e).get();
        auto& cf = e.local_db().find_column_family("ks", "cf");

        unsigned count = 0;
        lister::scan_dir((fs::path(cf.dir()) / sstables::snapshots_dir / "test"), lister::dir_entry_types::of<directory_entry_type::regular>(), [&count] (fs::path parent_dir, directory_entry de) {
            count++;
            return make_ready_future<>();
        }).get();
        BOOST_REQUIRE_GT(count, 1); // expect more than the manifest alone

        e.local_db().clear_snapshot("test", {"ks"}, "").get();
        count = 0;

        BOOST_REQUIRE_EQUAL(fs::exists(fs::path(cf.dir()) / sstables::snapshots_dir / "test"), false);
        return make_ready_future<>();
    });
}

SEASTAR_TEST_CASE(clear_multiple_snapshots) {
    sstring ks_name = "ks";
    sstring table_name = "cf";
    auto num_snapshots = 2;

    auto snapshot_name = [] (int idx) {
        return format("test-snapshot-{}", idx);
    };

    co_await do_with_some_data({table_name}, [&] (cql_test_env& e) {
        auto& t = e.local_db().find_column_family(ks_name, table_name);
        auto table_dir = fs::path(t.dir());
        auto snapshots_dir = table_dir / sstables::snapshots_dir;

        for (auto i = 0; i < num_snapshots; i++) {
            testlog.debug("Taking snapshot {} on {}.{}", snapshot_name(i), ks_name, table_name);
            take_snapshot(e, ks_name, table_name, snapshot_name(i)).get();
        }

        for (auto i = 0; i < num_snapshots; i++) {
            unsigned count = 0;
            testlog.debug("Verifying {}", snapshots_dir / snapshot_name(i));
            lister::scan_dir(snapshots_dir / snapshot_name(i), lister::dir_entry_types::of<directory_entry_type::regular>(), [&count] (fs::path parent_dir, directory_entry de) {
                count++;
                return make_ready_future<>();
            }).get();
            BOOST_REQUIRE_GT(count, 1); // expect more than the manifest alone
        }

        // non-existent tag
        testlog.debug("Clearing bogus tag");
        e.local_db().clear_snapshot("bogus", {ks_name}, table_name).get();
        for (auto i = 0; i < num_snapshots; i++) {
            BOOST_REQUIRE_EQUAL(fs::exists(snapshots_dir / snapshot_name(i)), true);
        }

        // clear single tag
        testlog.debug("Clearing snapshot={} of {}.{}", snapshot_name(0), ks_name, table_name);
        e.local_db().clear_snapshot(snapshot_name(0), {ks_name}, table_name).get();
        BOOST_REQUIRE_EQUAL(fs::exists(snapshots_dir / snapshot_name(0)), false);
        for (auto i = 1; i < num_snapshots; i++) {
            BOOST_REQUIRE_EQUAL(fs::exists(snapshots_dir / snapshot_name(i)), true);
        }

        // clear all tags (all tables)
        testlog.debug("Clearing all snapshots in {}", ks_name);
        e.local_db().clear_snapshot("", {ks_name}, "").get();
        for (auto i = 0; i < num_snapshots; i++) {
            BOOST_REQUIRE_EQUAL(fs::exists(snapshots_dir / snapshot_name(i)), false);
        }

        testlog.debug("Taking an extra {} of {}.{}", snapshot_name(num_snapshots), ks_name, table_name);
        take_snapshot(e, ks_name, table_name, snapshot_name(num_snapshots)).get();

        // existing snapshots expected to remain after dropping the table
        testlog.debug("Dropping table {}.{}", ks_name, table_name);
        replica::database::drop_table_on_all_shards(e.db(), e.get_system_keyspace(), ks_name, table_name).get();
        BOOST_REQUIRE_EQUAL(fs::exists(snapshots_dir / snapshot_name(num_snapshots)), true);

        // clear all tags
        testlog.debug("Clearing all snapshots in {}.{} after it had been dropped", ks_name, table_name);
        e.local_db().clear_snapshot("", {ks_name}, table_name).get();

        SCYLLA_ASSERT(!fs::exists(table_dir));

        // after all snapshots had been cleared,
        // the dropped table directory is expected to be removed.
        BOOST_REQUIRE_EQUAL(fs::exists(table_dir), false);

        return make_ready_future<>();
    });
}

SEASTAR_TEST_CASE(clear_nonexistent_snapshot) {
    // no crashes, no exceptions
    return do_with_some_data({"cf"}, [] (cql_test_env& e) {
        e.local_db().clear_snapshot("test", {"ks"}, "").get();
        return make_ready_future<>();
    });
}

SEASTAR_TEST_CASE(test_snapshot_ctl_details) {
    return do_with_some_data({"cf"}, [] (cql_test_env& e) {
        sharded<db::snapshot_ctl> sc;
        sc.start(std::ref(e.db()), std::ref(e.get_task_manager()), std::ref(e.get_sstorage_manager()), db::snapshot_ctl::config{}).get();
        auto stop_sc = deferred_stop(sc);

        auto& cf = e.local_db().find_column_family("ks", "cf");
        take_snapshot(e).get();

        auto details = cf.get_snapshot_details().get();
        BOOST_REQUIRE_EQUAL(details.size(), 1);

        auto sd = details["test"];
        BOOST_REQUIRE_EQUAL(sd.live, 0);
        BOOST_REQUIRE_GT(sd.total, 0);

        auto sc_details = sc.local().get_snapshot_details().get();
        BOOST_REQUIRE_EQUAL(sc_details.size(), 1);

        auto sc_sd_vec = sc_details["test"];
        BOOST_REQUIRE_EQUAL(sc_sd_vec.size(), 1);
        const auto &sc_sd = sc_sd_vec[0];
        BOOST_REQUIRE_EQUAL(sc_sd.ks, "ks");
        BOOST_REQUIRE_EQUAL(sc_sd.cf, "cf");
        BOOST_REQUIRE_EQUAL(sc_sd.details.live, sd.live);
        BOOST_REQUIRE_EQUAL(sc_sd.details.total, sd.total);

        lister::scan_dir(fs::path(cf.dir()), lister::dir_entry_types::of<directory_entry_type::regular>(), [] (fs::path parent_dir, directory_entry de) {
            fs::remove(parent_dir / de.name);
            return make_ready_future<>();
        }).get();

        auto sd_post_deletion = cf.get_snapshot_details().get().at("test");

        BOOST_REQUIRE_EQUAL(sd_post_deletion.total, sd_post_deletion.live);
        BOOST_REQUIRE_EQUAL(sd.total, sd_post_deletion.live);

        sc_details = sc.local().get_snapshot_details().get();
        auto sc_sd_post_deletion_vec = sc_details["test"];
        BOOST_REQUIRE_EQUAL(sc_sd_post_deletion_vec.size(), 1);
        const auto &sc_sd_post_deletion = sc_sd_post_deletion_vec[0];
        BOOST_REQUIRE_EQUAL(sc_sd_post_deletion.ks, "ks");
        BOOST_REQUIRE_EQUAL(sc_sd_post_deletion.cf, "cf");
        BOOST_REQUIRE_EQUAL(sc_sd_post_deletion.details.live, sd_post_deletion.live);
        BOOST_REQUIRE_EQUAL(sc_sd_post_deletion.details.total, sd_post_deletion.total);

        return make_ready_future<>();
    });
}

SEASTAR_TEST_CASE(test_snapshot_ctl_true_snapshots_size) {
    return do_with_some_data({"cf"}, [] (cql_test_env& e) {
        sharded<db::snapshot_ctl> sc;
        sc.start(std::ref(e.db()), std::ref(e.get_task_manager()), std::ref(e.get_sstorage_manager()), db::snapshot_ctl::config{}).get();
        auto stop_sc = deferred_stop(sc);

        auto& cf = e.local_db().find_column_family("ks", "cf");
        take_snapshot(e).get();

        auto details = cf.get_snapshot_details().get();
        BOOST_REQUIRE_EQUAL(details.size(), 1);

        auto sd = details["test"];
        BOOST_REQUIRE_EQUAL(sd.live, 0);
        BOOST_REQUIRE_GT(sd.total, 0);

        auto sc_live_size = sc.local().true_snapshots_size().get();
        BOOST_REQUIRE_EQUAL(sc_live_size, sd.live);

        lister::scan_dir(fs::path(cf.dir()), lister::dir_entry_types::of<directory_entry_type::regular>(), [] (fs::path parent_dir, directory_entry de) {
            fs::remove(parent_dir / de.name);
            return make_ready_future<>();
        }).get();

        auto sd_post_deletion = cf.get_snapshot_details().get().at("test");

        BOOST_REQUIRE_EQUAL(sd_post_deletion.total, sd_post_deletion.live);
        BOOST_REQUIRE_EQUAL(sd.total, sd_post_deletion.live);

        sc_live_size = sc.local().true_snapshots_size().get();
        BOOST_REQUIRE_EQUAL(sc_live_size, sd_post_deletion.live);

        return make_ready_future<>();
    });
}

// toppartitions_query caused a lw_shared_ptr to cross shards when moving results, #5104
SEASTAR_TEST_CASE(toppartitions_cross_shard_schema_ptr) {
    return do_with_cql_env_and_compaction_groups([] (cql_test_env& e) {
        e.execute_cql("CREATE TABLE ks.tab (id int PRIMARY KEY)").get();
        db::toppartitions_query tq(e.db(), {{"ks", "tab"}}, {}, 1s, 100, 100);
        tq.scatter().get();
        auto q = e.prepare("INSERT INTO ks.tab(id) VALUES(?)").get();
        // Generate many values to ensure crossing shards
        for (auto i = 0; i != 100; ++i) {
            e.execute_prepared(q, {cql3::raw_value::make_value(int32_type->decompose(i))}).get();
        }
        // This should trigger the bug in debug mode
        tq.gather().get();
    });
}

SEASTAR_THREAD_TEST_CASE(read_max_size) {
    do_with_cql_env_and_compaction_groups([] (cql_test_env& e) {
        e.execute_cql("CREATE TABLE test (pk text, ck int, v text, PRIMARY KEY (pk, ck));").get();
        auto id = e.prepare("INSERT INTO test (pk, ck, v) VALUES (?, ?, ?);").get();

        auto& db = e.local_db();
        auto& tab = db.find_column_family("ks", "test");
        auto s = tab.schema();

        auto pk = tests::generate_partition_key(s);
        const auto cql3_pk = cql3::raw_value::make_value(pk.key().explode().front());

        const auto value = sstring(1024, 'a');
        const auto raw_value = utf8_type->decompose(data_value(value));
        const auto cql3_value = cql3::raw_value::make_value(raw_value);

        const int num_rows = 1024;

        for (int i = 0; i != num_rows; ++i) {
            const auto cql3_ck = cql3::raw_value::make_value(int32_type->decompose(data_value(i)));
            e.execute_prepared(id, {cql3_pk, cql3_ck, cql3_value}).get();
        }

        const auto partition_ranges = std::vector<dht::partition_range>{query::full_partition_range};

        const std::vector<std::pair<sstring, std::function<future<size_t>(schema_ptr, const query::read_command&)>>> query_methods{
                {"query_mutations()", [&db, &partition_ranges] (schema_ptr s, const query::read_command& cmd) -> future<size_t> {
                    return db.query_mutations(s, cmd, partition_ranges.front(), {}, db::no_timeout).then(
                            [] (const std::tuple<reconcilable_result, cache_temperature>& res) {
                        return std::get<0>(res).memory_usage();
                    });
                }},
                {"query()", [&db, &partition_ranges] (schema_ptr s, const query::read_command& cmd) -> future<size_t> {
                    return db.query(s, cmd, query::result_options::only_result(), partition_ranges, {}, db::no_timeout).then(
                            [] (const std::tuple<lw_shared_ptr<query::result>, cache_temperature>& res) {
                        return size_t(std::get<0>(res)->buf().size());
                    });
                }},
                {"query_mutations_on_all_shards()", [&e, &partition_ranges] (schema_ptr s, const query::read_command& cmd) -> future<size_t> {
                    return query_mutations_on_all_shards(e.db(), s, cmd, partition_ranges, {}, db::no_timeout).then(
                            [] (const std::tuple<foreign_ptr<lw_shared_ptr<reconcilable_result>>, cache_temperature>& res) {
                        return std::get<0>(res)->memory_usage();
                    });
                }}
        };

        for (auto [query_method_name, query_method] : query_methods) {
            for (auto allow_short_read : {true, false}) {
                for (auto max_size : {1024u, 1024u * 1024u, 1024u * 1024u * 1024u}) {
                    const auto should_throw = max_size < (num_rows * value.size() * 2) && !allow_short_read;
                    testlog.info("checking: query_method={}, allow_short_read={}, max_size={}, should_throw={}", query_method_name, allow_short_read, max_size, should_throw);
                    auto slice = s->full_slice();
                    if (allow_short_read) {
                        slice.options.set<query::partition_slice::option::allow_short_read>();
                    } else {
                        slice.options.remove<query::partition_slice::option::allow_short_read>();
                    }
                    query::read_command cmd(s->id(), s->version(), slice, query::max_result_size(max_size), query::tombstone_limit::max);
                    try {
                        auto size = query_method(s, cmd).get();
                        // Just to ensure we are not interpreting empty results as success.
                        BOOST_REQUIRE(size != 0);
                        if (should_throw) {
                            BOOST_FAIL("Expected exception, but none was thrown.");
                        } else {
                            testlog.trace("No exception thrown, as expected.");
                        }
                    } catch (std::runtime_error& e) {
                        if (should_throw) {
                            testlog.trace("Exception thrown, as expected: {}", e.what());
                        } else {
                            BOOST_FAIL(fmt::format("Expected no exception, but caught: {}", e.what()));
                        }
                    }
                }
            }
        }
    }).get();
}

// Check that mutation queries, those that are stopped when the memory
// consumed by their results reach the local/global limit, are aborted
// instead of silently terminated when this happens.
SEASTAR_THREAD_TEST_CASE(unpaged_mutation_read_global_limit) {
    auto cfg = cql_test_config{};
    cfg.dbcfg.emplace();
    // The memory available to the result memory limiter (global limit) is
    // configured based on the available memory, so give a small amount to
    // the "node", so we don't have to work with large amount of data.
    cfg.dbcfg->available_memory = 2 * 1024 * 1024;
    do_with_cql_env_and_compaction_groups([] (cql_test_env& e) {
        e.execute_cql("CREATE TABLE test (pk text, ck int, v text, PRIMARY KEY (pk, ck));").get();
        auto id = e.prepare("INSERT INTO test (pk, ck, v) VALUES (?, ?, ?);").get();

        auto& db = e.local_db();
        auto& tab = db.find_column_family("ks", "test");
        auto s = tab.schema();

        auto pk = tests::generate_partition_key(s);
        const auto cql3_pk = cql3::raw_value::make_value(pk.key().explode().front());

        const auto value = sstring(1024, 'a');
        const auto raw_value = utf8_type->decompose(data_value(value));
        const auto cql3_value = cql3::raw_value::make_value(raw_value);

        const int num_rows = 1024;
        const auto max_size = 1024u * 1024u * 1024u;

        for (int i = 0; i != num_rows; ++i) {
            const auto cql3_ck = cql3::raw_value::make_value(int32_type->decompose(data_value(i)));
            e.execute_prepared(id, {cql3_pk, cql3_ck, cql3_value}).get();
        }

        const auto partition_ranges = std::vector<dht::partition_range>{query::full_partition_range};

        const std::vector<std::pair<sstring, std::function<future<size_t>(schema_ptr, const query::read_command&)>>> query_methods{
                {"query_mutations()", [&db, &partition_ranges] (schema_ptr s, const query::read_command& cmd) -> future<size_t> {
                    return db.query_mutations(s, cmd, partition_ranges.front(), {}, db::no_timeout).then(
                            [] (const std::tuple<reconcilable_result, cache_temperature>& res) {
                        return std::get<0>(res).memory_usage();
                    });
                }},
                {"query_mutations_on_all_shards()", [&e, &partition_ranges] (schema_ptr s, const query::read_command& cmd) -> future<size_t> {
                    return query_mutations_on_all_shards(e.db(), s, cmd, partition_ranges, {}, db::no_timeout).then(
                            [] (const std::tuple<foreign_ptr<lw_shared_ptr<reconcilable_result>>, cache_temperature>& res) {
                        return std::get<0>(res)->memory_usage();
                    });
                }}
        };

        for (auto [query_method_name, query_method] : query_methods) {
            testlog.info("checking: query_method={}", query_method_name);
            auto slice = s->full_slice();
            slice.options.remove<query::partition_slice::option::allow_short_read>();
            query::read_command cmd(s->id(), s->version(), slice, query::max_result_size(max_size), query::tombstone_limit::max);
            try {
                auto size = query_method(s, cmd).get();
                // Just to ensure we are not interpreting empty results as success.
                BOOST_REQUIRE(size != 0);
                BOOST_FAIL("Expected exception, but none was thrown.");
            } catch (std::runtime_error& e) {
                testlog.trace("Exception thrown, as expected: {}", e.what());
            }
        }
    }, std::move(cfg)).get();
}

SEASTAR_THREAD_TEST_CASE(reader_concurrency_semaphore_selection_test) {
    cql_test_config cfg;

    scheduling_group unknown_scheduling_group = create_scheduling_group("unknown", 800).get();
    auto cleanup_unknown_scheduling_group = defer([&unknown_scheduling_group] {
        destroy_scheduling_group(unknown_scheduling_group).get();
    });

    const auto user_semaphore = std::mem_fn(&database_test::get_user_read_concurrency_semaphore);
    const auto system_semaphore = std::mem_fn(&database_test::get_system_read_concurrency_semaphore);
    const auto streaming_semaphore = std::mem_fn(&database_test::get_streaming_read_concurrency_semaphore);

    std::vector<std::pair<scheduling_group, std::function<reader_concurrency_semaphore&(database_test&)>>> scheduling_group_and_expected_semaphore{
        {default_scheduling_group(), system_semaphore}
    };

    auto sched_groups = get_scheduling_groups().get();

    scheduling_group_and_expected_semaphore.emplace_back(sched_groups.compaction_scheduling_group, system_semaphore);
    scheduling_group_and_expected_semaphore.emplace_back(sched_groups.memory_compaction_scheduling_group, system_semaphore);
    scheduling_group_and_expected_semaphore.emplace_back(sched_groups.streaming_scheduling_group, streaming_semaphore);
    scheduling_group_and_expected_semaphore.emplace_back(sched_groups.statement_scheduling_group, user_semaphore);
    scheduling_group_and_expected_semaphore.emplace_back(sched_groups.memtable_scheduling_group, system_semaphore);
    scheduling_group_and_expected_semaphore.emplace_back(sched_groups.memtable_to_cache_scheduling_group, system_semaphore);
    scheduling_group_and_expected_semaphore.emplace_back(sched_groups.gossip_scheduling_group, system_semaphore);
    scheduling_group_and_expected_semaphore.emplace_back(unknown_scheduling_group, user_semaphore);

    do_with_cql_env_and_compaction_groups([&scheduling_group_and_expected_semaphore] (cql_test_env& e) {
        auto& db = e.local_db();
        database_test tdb(db);
        for (const auto& [sched_group, expected_sem_getter] : scheduling_group_and_expected_semaphore) {
            with_scheduling_group(sched_group, [&db, sched_group = sched_group, expected_sem_ptr = &expected_sem_getter(tdb)] {
                auto& sem = db.get_reader_concurrency_semaphore();
                if (&sem != expected_sem_ptr) {
                    BOOST_FAIL(fmt::format("Unexpected semaphore for scheduling group {}, expected {}, got {}", sched_group.name(), expected_sem_ptr->name(), sem.name()));
                }
            }).get();
        }
    }, std::move(cfg)).get();
}

SEASTAR_THREAD_TEST_CASE(max_result_size_for_query_selection_test) {
    cql_test_config cfg;

    cfg.db_config->max_memory_for_unlimited_query_soft_limit(1 * 1024 * 1024, utils::config_file::config_source::CommandLine);
    cfg.db_config->max_memory_for_unlimited_query_hard_limit(2 * 1024 * 1024, utils::config_file::config_source::CommandLine);

    scheduling_group unknown_scheduling_group = create_scheduling_group("unknown", 800).get();
    auto cleanup_unknown_scheduling_group = defer([&unknown_scheduling_group] {
        destroy_scheduling_group(unknown_scheduling_group).get();
    });

    const auto user_max_result_size = query::max_result_size(
            cfg.db_config->max_memory_for_unlimited_query_soft_limit(),
            cfg.db_config->max_memory_for_unlimited_query_hard_limit(),
            query::result_memory_limiter::maximum_result_size);
    const auto system_max_result_size = query::max_result_size(
            query::result_memory_limiter::unlimited_result_size,
            query::result_memory_limiter::unlimited_result_size,
            query::result_memory_limiter::maximum_result_size);
    const auto maintenance_max_result_size = system_max_result_size;

    std::vector<std::pair<scheduling_group, query::max_result_size>> scheduling_group_and_expected_max_result_size{
        {default_scheduling_group(), system_max_result_size}
    };

    auto sched_groups = get_scheduling_groups().get();

    scheduling_group_and_expected_max_result_size.emplace_back(sched_groups.compaction_scheduling_group, system_max_result_size);
    scheduling_group_and_expected_max_result_size.emplace_back(sched_groups.memory_compaction_scheduling_group, system_max_result_size);
    scheduling_group_and_expected_max_result_size.emplace_back(sched_groups.streaming_scheduling_group, maintenance_max_result_size);
    scheduling_group_and_expected_max_result_size.emplace_back(sched_groups.statement_scheduling_group, user_max_result_size);
    scheduling_group_and_expected_max_result_size.emplace_back(sched_groups.memtable_scheduling_group, system_max_result_size);
    scheduling_group_and_expected_max_result_size.emplace_back(sched_groups.memtable_to_cache_scheduling_group, system_max_result_size);
    scheduling_group_and_expected_max_result_size.emplace_back(sched_groups.gossip_scheduling_group, system_max_result_size);
    scheduling_group_and_expected_max_result_size.emplace_back(unknown_scheduling_group, user_max_result_size);

    do_with_cql_env_and_compaction_groups([&scheduling_group_and_expected_max_result_size] (cql_test_env& e) {
        auto& db = e.local_db();
        database_test tdb(db);
        for (const auto& [sched_group, expected_max_size] : scheduling_group_and_expected_max_result_size) {
            with_scheduling_group(sched_group, [&db, sched_group = sched_group, expected_max_size = expected_max_size] {
                const auto max_size = db.get_query_max_result_size();
                if (max_size != expected_max_size) {
                    BOOST_FAIL(fmt::format("Unexpected max_size for scheduling group {}, expected {{{}, {}}}, got {{{}, {}}}",
                                sched_group.name(),
                                expected_max_size.soft_limit,
                                expected_max_size.hard_limit,
                                max_size.soft_limit,
                                max_size.hard_limit));
                }
            }).get();
        }
    }, std::move(cfg)).get();
}

// Check that during a multi-page range scan:
// * semaphore mismatch is detected
// * code is exception safe w.r.t. to the mismatch exception, e.g. readers are closed properly
SEASTAR_TEST_CASE(multipage_range_scan_semaphore_mismatch) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        const auto do_abort = set_abort_on_internal_error(false);
        auto reset_abort = defer([do_abort] {
            set_abort_on_internal_error(do_abort);
        });
        e.execute_cql("CREATE TABLE ks.tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck));").get();

        auto insert_id = e.prepare("INSERT INTO ks.tbl(pk, ck, v) VALUES(?, ?, ?)").get();

        auto& db = e.local_db();
        auto& tbl = db.find_column_family("ks", "tbl");
        auto s = tbl.schema();

        auto dk = tests::generate_partition_key(tbl.schema());
        const auto pk = cql3::raw_value::make_value(managed_bytes(*dk.key().begin(*s)));
        const auto v = cql3::raw_value::make_value(int32_type->decompose(0));
        for (int32_t ck = 0; ck < 100; ++ck) {
            e.execute_prepared(insert_id, {pk, cql3::raw_value::make_value(int32_type->decompose(ck)), v}).get();
        }

        auto sched_groups = get_scheduling_groups().get();

        query::read_command cmd1(
                s->id(),
                s->version(),
                s->full_slice(),
                query::max_result_size(std::numeric_limits<uint64_t>::max(), std::numeric_limits<uint64_t>::max(), std::numeric_limits<uint64_t>::max()),
                query::tombstone_limit::max,
                query::row_limit(4),
                query::partition_limit::max,
                gc_clock::now(),
                std::nullopt,
                query_id::create_random_id(),
                query::is_first_page::yes);

        auto cmd2 = cmd1;
        auto cr = query::clustering_range::make_starting_with({clustering_key::from_single_value(*s, int32_type->decompose(3)), false});
        cmd2.slice = partition_slice_builder(*s).set_specific_ranges(query::specific_ranges(dk.key(), {cr})).build();
        cmd2.is_first_page = query::is_first_page::no;

        auto pr = dht::partition_range::make_starting_with({dk, true});
        auto prs = dht::partition_range_vector{pr};

        auto read_page = [&] (scheduling_group sg, const query::read_command& cmd) {
            with_scheduling_group(sg, [&] {
                return query_data_on_all_shards(e.db(), s, cmd, prs, query::result_options::only_result(), {}, db::no_timeout);
            }).get();
        };

        read_page(default_scheduling_group(), cmd1);
        BOOST_REQUIRE_EXCEPTION(read_page(sched_groups.statement_scheduling_group, cmd2), std::runtime_error,
                testing::exception_predicate::message_contains("semaphore mismatch detected, dropping reader"));
    });
}

// Test `upgrade_sstables` on all keyspaces (including the system keyspace).
// Refs: #9494 (https://github.com/scylladb/scylla/issues/9494)
SEASTAR_TEST_CASE(upgrade_sstables) {
    return do_with_cql_env_and_compaction_groups([] (cql_test_env& e) {
        e.db().invoke_on_all([] (replica::database& db) -> future<> {
            auto& cm = db.get_compaction_manager();
            for (auto& [ks_name, ks] : db.get_keyspaces()) {
                const auto& erm = ks.get_vnode_effective_replication_map();
                auto owned_ranges_ptr = compaction::make_owned_ranges_ptr(co_await db.get_keyspace_local_ranges(erm));
                for (auto& [cf_name, schema] : ks.metadata()->cf_meta_data()) {
                    auto& t = db.find_column_family(schema->id());
                    constexpr bool exclude_current_version = false;
                    co_await t.parallel_foreach_table_state([&] (compaction::table_state& ts) {
                        return cm.perform_sstable_upgrade(owned_ranges_ptr, ts, exclude_current_version, tasks::task_info{});
                    });
                }
            }
        }).get();
    });
}

SEASTAR_TEST_CASE(populate_from_quarantine_works) {
    auto tmpdir_for_data = make_lw_shared<tmpdir>();
    auto db_cfg_ptr = make_shared<db::config>();
    db_cfg_ptr->data_file_directories(std::vector<sstring>({ tmpdir_for_data->path().string() }));
    locator::host_id host_id;

    // populate tmpdir_for_data and
    // move a random sstable to quarantine
    co_await do_with_some_data({"cf"}, [&host_id] (cql_test_env& e) -> future<> {
        host_id = e.local_db().get_token_metadata().get_my_id();
        auto& db = e.db();
        co_await db.invoke_on_all([] (replica::database& db) {
            auto& cf = db.find_column_family("ks", "cf");
            return cf.flush();
        });
        auto shard = tests::random::get_int<unsigned>(0, smp::count);
        auto found = false;
        for (unsigned i = 0; i < smp::count && !found; i++) {
            found = co_await db.invoke_on((shard + i) % smp::count, [] (replica::database& db) -> future<bool> {
                auto& cf = db.find_column_family("ks", "cf");
                bool found = false;
                co_await cf.parallel_foreach_table_state([&] (compaction::table_state& ts) -> future<> {
                    auto sstables = in_strategy_sstables(ts);
                    if (sstables.empty()) {
                        co_return;
                    }
                    auto idx = tests::random::get_int<size_t>(0, sstables.size() - 1);
                    testlog.debug("Moving sstable #{} out of {} to quarantine", idx, sstables.size());
                    auto sst = sstables[idx];
                    co_await sst->change_state(sstables::sstable_state::quarantine);
                    found |= true;
                });
                co_return found;
            });
        }
        BOOST_REQUIRE(found);
    }, db_cfg_ptr);

    // reload the table from tmpdir_for_data and
    // verify that all rows are still there
    size_t row_count = 0;
    cql_test_config test_config(db_cfg_ptr);
    test_config.host_id = host_id;
    co_await do_with_cql_env([&row_count] (cql_test_env& e) -> future<> {
        auto res = co_await e.execute_cql("select * from ks.cf;");
        auto rows = dynamic_pointer_cast<cql_transport::messages::result_message::rows>(res);
        BOOST_REQUIRE(rows);
        row_count = rows->rs().result_set().size();
    }, std::move(test_config));
    BOOST_REQUIRE_EQUAL(row_count, 6);
}

SEASTAR_TEST_CASE(snapshot_with_quarantine_works) {
    return do_with_some_data({"cf"}, [] (cql_test_env& e) -> future<> {
        auto& db = e.db();
        co_await db.invoke_on_all([] (replica::database& db) {
            auto& cf = db.find_column_family("ks", "cf");
            return cf.flush();
        });

        std::set<sstring> expected = {
            "manifest.json",
        };

        // move a random sstable to quarantine
        auto shard = tests::random::get_int<unsigned>(0, smp::count);
        auto found = false;
        for (unsigned i = 0; i < smp::count; i++) {
            co_await db.invoke_on((shard + i) % smp::count, [&] (replica::database& db) -> future<> {
                auto& cf = db.find_column_family("ks", "cf");
                co_await cf.parallel_foreach_table_state([&] (compaction::table_state& ts) -> future<> {
                    auto sstables = in_strategy_sstables(ts);
                    if (sstables.empty()) {
                        co_return;
                    }
                    // collect all expected sstable data files
                    for (auto sst : sstables) {
                        expected.insert(sst->component_basename(sstables::component_type::Data));
                    }
                    if (std::exchange(found, true)) {
                        co_return;
                    }
                    auto idx = tests::random::get_int<size_t>(0, sstables.size() - 1);
                    auto sst = sstables[idx];
                    co_await sst->change_state(sstables::sstable_state::quarantine);
                });
            });
        }
        BOOST_REQUIRE(found);

        co_await take_snapshot(db, true /* skip_flush */);

        testlog.debug("Expected: {}", expected);

        // snapshot triggered a flush and wrote the data down.
        BOOST_REQUIRE_GT(expected.size(), 1);

        auto& cf = db.local().find_column_family("ks", "cf");

        // all files were copied and manifest was generated
        co_await lister::scan_dir((fs::path(cf.dir()) / sstables::snapshots_dir / "test"), lister::dir_entry_types::of<directory_entry_type::regular>(), [&expected] (fs::path parent_dir, directory_entry de) {
            testlog.debug("Found in snapshots: {}", de.name);
            expected.erase(de.name);
            return make_ready_future<>();
        });

        if (!expected.empty()) {
            testlog.error("Not in snapshots: {}", expected);
        }

        BOOST_REQUIRE(expected.empty());
    });
}

SEASTAR_TEST_CASE(database_drop_column_family_clears_querier_cache) {
    return do_with_cql_env_and_compaction_groups([] (cql_test_env& e) {
        e.execute_cql("create table ks.cf (k text, v int, primary key (k));").get();
        auto& db = e.local_db();
        auto& tbl = db.find_column_family("ks", "cf");

        auto op = std::optional(tbl.read_in_progress());
        auto s = tbl.schema();
        auto q = query::querier(
                tbl.as_mutation_source(),
                tbl.schema(),
                database_test(db).get_user_read_concurrency_semaphore().make_tracking_only_permit(s, "test", db::no_timeout, {}),
                query::full_partition_range,
                s->full_slice(),
                nullptr);

        auto f = replica::database::drop_table_on_all_shards(e.db(), e.get_system_keyspace(), "ks", "cf");

        // we add a querier to the querier cache while the drop is ongoing
        auto& qc = db.get_querier_cache();
        qc.insert_data_querier(query_id::create_random_id(), std::move(q), nullptr);
        BOOST_REQUIRE_EQUAL(qc.get_stats().population, 1);

        op.reset(); // this should allow the drop to finish
        f.get();

        // the drop should have cleaned up all entries belonging to that table
        BOOST_REQUIRE_EQUAL(qc.get_stats().population, 0);
    });
}

static future<> test_drop_table_with_auto_snapshot(bool auto_snapshot) {
    sstring ks_name = "ks";
    sstring table_name = format("table_with_auto_snapshot_{}", auto_snapshot ? "enabled" : "disabled");
    auto tmpdir_for_data = make_lw_shared<tmpdir>();
    auto db_cfg_ptr = make_shared<db::config>();
    db_cfg_ptr->data_file_directories(std::vector<sstring>({ tmpdir_for_data->path().string() }));
    db_cfg_ptr->auto_snapshot(auto_snapshot);

    co_await do_with_some_data({table_name}, [&] (cql_test_env& e) -> future<> {
        auto cf_dir = e.local_db().find_column_family(ks_name, table_name).dir();

        // Pass `with_snapshot=true` to drop_table_on_all
        // to allow auto_snapshot (based on the configuration above).
        // The table directory should therefore exist after the table is dropped if auto_snapshot is disabled in the configuration.
        co_await replica::database::drop_table_on_all_shards(e.db(), e.get_system_keyspace(), ks_name, table_name, true);
        auto cf_dir_exists = co_await file_exists(cf_dir);
        BOOST_REQUIRE_EQUAL(cf_dir_exists, auto_snapshot);
        co_return;
    }, db_cfg_ptr);
}

SEASTAR_TEST_CASE(drop_table_with_auto_snapshot_enabled) {
    return test_drop_table_with_auto_snapshot(true);
}

SEASTAR_TEST_CASE(drop_table_with_auto_snapshot_disabled) {
    return test_drop_table_with_auto_snapshot(false);
}

SEASTAR_TEST_CASE(drop_table_with_no_snapshot) {
    sstring ks_name = "ks";
    sstring table_name = "table_with_no_snapshot";

    co_await do_with_some_data({table_name}, [&] (cql_test_env& e) -> future<> {
        auto cf_dir = e.local_db().find_column_family(ks_name, table_name).dir();

        // Pass `with_snapshot=false` to drop_table_on_all
        // to disallow auto_snapshot.
        // The table directory should therefore not exist after the table is dropped.
        co_await replica::database::drop_table_on_all_shards(e.db(), e.get_system_keyspace(), ks_name, table_name, false);
        auto cf_dir_exists = co_await file_exists(cf_dir);
        BOOST_REQUIRE_EQUAL(cf_dir_exists, false);
        co_return;
    });
}

SEASTAR_TEST_CASE(drop_table_with_explicit_snapshot) {
    sstring ks_name = "ks";
    sstring table_name = "table_with_explicit_snapshot";

    co_await do_with_some_data({table_name}, [&] (cql_test_env& e) -> future<> {
        auto snapshot_tag = format("test-{}", db_clock::now().time_since_epoch().count());
        co_await replica::database::snapshot_table_on_all_shards(e.db(), ks_name, table_name, snapshot_tag, db::snapshot_ctl::snap_views::no, false);
        auto cf_dir = e.local_db().find_column_family(ks_name, table_name).dir();

        // With explicit snapshot and with_snapshot=false
        // dir should still be kept, regardless of the
        // with_snapshot parameter and auto_snapshot config.
        co_await replica::database::drop_table_on_all_shards(e.db(), e.get_system_keyspace(), ks_name, table_name, false);
        auto cf_dir_exists = co_await file_exists(cf_dir);
        BOOST_REQUIRE_EQUAL(cf_dir_exists, true);
        co_return;
    });
}

SEASTAR_TEST_CASE(mutation_dump_generated_schema_deterministic_id_version) {
    simple_schema s;
    auto os1 = replica::mutation_dump::generate_output_schema_from_underlying_schema(s.schema());
    auto os2 = replica::mutation_dump::generate_output_schema_from_underlying_schema(s.schema());

    BOOST_REQUIRE_EQUAL(os1->id(), os2->id());
    BOOST_REQUIRE_EQUAL(os1->version(), os2->version());

    return make_ready_future<>();
}
