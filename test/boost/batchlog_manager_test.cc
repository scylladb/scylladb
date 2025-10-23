/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */


#include <boost/test/unit_test.hpp>
#include <stdint.h>

#undef SEASTAR_TESTING_MAIN
#include <seastar/testing/test_case.hh>
#include "test/lib/cql_assertions.hh"
#include "test/lib/cql_test_env.hh"
#include "test/lib/error_injection.hh"
#include "test/lib/log.hh"

#include <seastar/core/future-util.hh>
#include <seastar/core/shared_ptr.hh>
#include "cql3/statements/batch_statement.hh"
#include "cql3/query_processor.hh"
#include "cql3/untyped_result_set.hh"
#include "db/batchlog.hh"
#include "db/batchlog_manager.hh"
#include "db/commitlog/commitlog.hh"
#include "db/config.hh"
#include "idl/frozen_schema.dist.hh"
#include "idl/frozen_schema.dist.impl.hh"
#include "message/messaging_service.hh"
#include "service/storage_proxy.hh"
#include "utils/rjson.hh"

BOOST_AUTO_TEST_SUITE(batchlog_manager_test)

static atomic_cell make_atomic_cell(data_type dt, bytes value) {
    return atomic_cell::make_live(*dt, 0, std::move(value));
};

SEASTAR_TEST_CASE(test_execute_batch) {
    return do_with_cql_env([] (auto& e) {
        auto& qp = e.local_qp();
        auto& bp =  e.batchlog_manager().local();

        return e.execute_cql("create table cf (p1 varchar, c1 int, r1 int, PRIMARY KEY (p1, c1));").discard_result().then([&qp, &e, &bp] () mutable {
            auto& db = e.local_db();
            auto s = db.find_schema("ks", "cf");
            const auto batchlog_schema = db.find_schema(db::system_keyspace::NAME, db::system_keyspace::BATCHLOG_V2);

            const column_definition& r1_col = *s->get_column_definition("r1");
            auto key = partition_key::from_exploded(*s, {to_bytes("key1")});
            auto c_key = clustering_key::from_exploded(*s, {int32_type->decompose(1)});

            mutation m(s, key);
            m.set_clustered_cell(c_key, r1_col, make_atomic_cell(int32_type, int32_type->decompose(100)));

            using namespace std::chrono_literals;

            auto version = netw::messaging_service::current_version;
            auto bm = db::get_batchlog_mutation_for(batchlog_schema, { m }, version, db_clock::now() - db_clock::duration(3h), s->id().uuid());

            return qp.proxy().mutate_locally(bm, tracing::trace_state_ptr(), db::commitlog::force_sync::no).then([&bp] () mutable {
                return bp.count_all_batches().then([](auto n) {
                    BOOST_CHECK_EQUAL(n, 1);
                }).then([&bp] () mutable {
                    return bp.do_batch_log_replay(db::batchlog_manager::post_replay_cleanup::yes);
                });
            });
        }).then([&qp] {
            return qp.execute_internal("select * from ks.cf where p1 = ? and c1 = ?;", { sstring("key1"), 1 }, cql3::query_processor::cache_internal::yes).then([](auto rs) {
                BOOST_REQUIRE(!rs->empty());
                auto i = rs->one().template get_as<int32_t>("r1");
                BOOST_CHECK_EQUAL(i, int32_t(100));
            });
        });
    });
}

future<> run_batchlog_cleanup_with_failed_batches_test(bool replay_fails, db::batchlog_manager::post_replay_cleanup cleanup) {
#ifndef SCYLLA_ENABLE_ERROR_INJECTION
    return make_ready_future<>();
#endif

    cql_test_config cfg;
    cfg.db_config->batchlog_replay_cleanup_after_replays.set_value("9999999", utils::config_file::config_source::Internal);
    cfg.batchlog_replay_timeout = 0s;
    cfg.batchlog_delay = std::chrono::hours(9999);

    return do_with_cql_env_thread([=] (cql_test_env& env) -> void {
        auto& bm = env.batchlog_manager().local();

        env.execute_cql("CREATE TABLE tbl (pk bigint PRIMARY KEY, v text)").get();

        const auto shard_count = 256;

        const uint64_t batch_count = 8;
        uint64_t failed_batches = 0;

        for (uint64_t i = 0; i != batch_count; ++i) {
            std::vector<sstring> queries;
            std::vector<std::string_view> query_views;
            for (uint64_t j = 0; j != i+2; ++j) {
                queries.emplace_back(format("INSERT INTO tbl (pk, v) VALUES ({}, 'value');", j));
                query_views.emplace_back(queries.back());
            }
            const bool fail = i % 2;
            bool injected_exception_thrown = false;

            std::optional<scoped_error_injection> error_injection;
            if (fail) {
                ++failed_batches;
                error_injection.emplace("storage_proxy_fail_send_batch");
            }
            try {
                env.execute_batch(
                        query_views,
                        cql3::statements::batch_statement::type::LOGGED,
                        std::make_unique<cql3::query_options>(db::consistency_level::ONE, std::vector<cql3::raw_value>())).get();
            } catch (std::runtime_error& ex) {
                if (fail) {
                    BOOST_REQUIRE_EQUAL(std::string(ex.what()), "Error injection: failing to send batch");
                    injected_exception_thrown = true;
                } else {
                    throw;
                }
            }
            BOOST_REQUIRE_EQUAL(injected_exception_thrown, fail);
        }

        const auto fragments_query = format("SELECT * FROM MUTATION_FRAGMENTS({}.{}) WHERE partition_region = 2 ALLOW FILTERING", db::system_keyspace::NAME, db::system_keyspace::BATCHLOG_V2);

        assert_that(env.execute_cql(format("SELECT id FROM {}.{}", db::system_keyspace::NAME, db::system_keyspace::BATCHLOG_V2)).get())
            .is_rows()
            .with_size(failed_batches);

        assert_that(env.execute_cql(fragments_query).get())
            .is_rows()
            .with_size(batch_count)
            .assert_for_columns_of_each_row([&] (columns_assertions& columns) {
                columns.with_typed_column<sstring>("mutation_source", "memtable:0");
            });

        std::optional<scoped_error_injection> error_injection;
        if (replay_fails) {
            error_injection.emplace("storage_proxy_fail_replay_batch");
        }

        bm.do_batch_log_replay(cleanup).get();

        assert_that(env.execute_cql(format("SELECT id FROM {}.{}", db::system_keyspace::NAME, db::system_keyspace::BATCHLOG_V2)).get())
            .is_rows()
            .with_size(replay_fails ? failed_batches : 0);

        assert_that(env.execute_cql(fragments_query).get())
            .is_rows()
            .with_size([&] (size_t size) {
                if (!cleanup) {
                    return size == batch_count;
                }
                if (replay_fails) {
                    // Some of the stage=0 rows might still linger, so accept a range here.
                    return size >= 2 * shard_count + failed_batches && size <= 2 * shard_count + batch_count + failed_batches;
                }
                return size >= 2 * shard_count && size <= 2 * shard_count + batch_count;
            })
            .assert_for_columns_of_each_row([&] (columns_assertions& columns) {
                columns.with_typed_column<sstring>("mutation_source", [&] (const sstring& source) {
                    return cleanup || source == "memtable:0";
                });
            });
    }, cfg);
}

SEASTAR_TEST_CASE(test_batchlog_replay_fails_no_cleanup) {
    return run_batchlog_cleanup_with_failed_batches_test(true, db::batchlog_manager::post_replay_cleanup::no);
}

SEASTAR_TEST_CASE(test_batchlog_replay_fails_with_cleanup) {
    return run_batchlog_cleanup_with_failed_batches_test(true, db::batchlog_manager::post_replay_cleanup::yes);
}

SEASTAR_TEST_CASE(test_batchlog_replay_no_cleanup) {
    return run_batchlog_cleanup_with_failed_batches_test(false, db::batchlog_manager::post_replay_cleanup::no);
}

SEASTAR_TEST_CASE(test_batchlog_replay_with_cleanup) {
    return run_batchlog_cleanup_with_failed_batches_test(false, db::batchlog_manager::post_replay_cleanup::yes);
}

SEASTAR_TEST_CASE(test_batchlog_cleanup_replay_timeout) {
#ifndef SCYLLA_ENABLE_ERROR_INJECTION
    return make_ready_future<>();
#endif

    cql_test_config cfg;
    cfg.db_config->batchlog_replay_cleanup_after_replays.set_value("9999999", utils::config_file::config_source::Internal);
    cfg.batchlog_replay_timeout = 1s;
    cfg.batchlog_delay = std::chrono::hours(9999);

    return do_with_cql_env_thread([] (cql_test_env& env) -> void {
        auto& bm = env.batchlog_manager().local();

        env.execute_cql("CREATE TABLE tbl (pk bigint PRIMARY KEY, v text)").get();

        const auto shard_count = 256;

        const uint64_t batch_count = 8;

        auto send_batches = [&] {
            for (uint64_t i = 0; i != batch_count; ++i) {
                std::vector<sstring> queries;
                std::vector<std::string_view> query_views;
                for (uint64_t j = 0; j != i+2; ++j) {
                    queries.emplace_back(format("INSERT INTO tbl (pk, v) VALUES ({}, 'value');", j));
                    query_views.emplace_back(queries.back());
                }
                try {
                    env.execute_batch(
                            query_views,
                            cql3::statements::batch_statement::type::LOGGED,
                            std::make_unique<cql3::query_options>(db::consistency_level::ONE, std::vector<cql3::raw_value>())).get();
                } catch (std::runtime_error& ex) {
                    BOOST_REQUIRE_EQUAL(std::string(ex.what()), "Error injection: failing to send batch");
                }
            }
        };

        {
            scoped_error_injection error_injection("storage_proxy_fail_send_batch");

            send_batches();
            sleep(1s).get();
            send_batches();
        }

        bm.do_batch_log_replay(db::batchlog_manager::post_replay_cleanup::yes).get();

        const auto fragments_query = format("SELECT * FROM MUTATION_FRAGMENTS({}.{}) WHERE partition_region = 2 ALLOW FILTERING", db::system_keyspace::NAME, db::system_keyspace::BATCHLOG_V2);

        assert_that(env.execute_cql(format("SELECT id FROM {}.{}", db::system_keyspace::NAME, db::system_keyspace::BATCHLOG_V2)).get())
            .is_rows()
            .with_size(batch_count); // half of the batches are too fresh to be replayed

        size_t rows = 0;
        size_t range_tombstone_changes = 0;

        assert_that(env.execute_cql(fragments_query).get())
            .is_rows()
            .with_size([&] (size_t size) {
                // half of the entries should be gone, +2*shard_count for range tombstone covering replayed entries
                return size >= batch_count + 2 * shard_count && size <= batch_count * 2 + 2 * shard_count;
            })
            .assert_for_columns_of_each_row([&] (columns_assertions& columns) {
                columns
                    .with_typed_column<sstring>("mutation_source", "memtable:0")
                    .with_typed_column<sstring>("mutation_fragment_kind", [&] (std::string_view mutation_fragment_kind) {
                        if (mutation_fragment_kind == "clustering row") {
                            ++rows;
                        } else if (mutation_fragment_kind == "range tombstone change") {
                            ++range_tombstone_changes;
                        } else {
                            return false;
                        }
                        return true;
                    });
            });

        BOOST_REQUIRE_GE(rows, batch_count);
        BOOST_REQUIRE_LE(rows, batch_count * 2);
        BOOST_REQUIRE_EQUAL(range_tombstone_changes, 2*shard_count);
    }, cfg);
}

SEASTAR_TEST_CASE(test_batchlog_replay_stage) {
#ifndef SCYLLA_ENABLE_ERROR_INJECTION
    return make_ready_future<>();
#endif

    cql_test_config cfg;
    cfg.db_config->batchlog_replay_cleanup_after_replays.set_value("9999999", utils::config_file::config_source::Internal);
    cfg.batchlog_replay_timeout = 0s;
    cfg.batchlog_delay = std::chrono::hours(9999);

    return do_with_cql_env_thread([] (cql_test_env& env) -> void {
        auto& bm = env.batchlog_manager().local();

        env.execute_cql("CREATE TABLE tbl (pk bigint PRIMARY KEY, v text)").get();

        const uint64_t batch_count = 8;

        const auto shard_count = 256;

        {
            scoped_error_injection error_injection("storage_proxy_fail_send_batch");

            for (uint64_t i = 0; i != batch_count; ++i) {
                std::vector<sstring> queries;
                std::vector<std::string_view> query_views;
                for (uint64_t j = 0; j != i+2; ++j) {
                    queries.emplace_back(format("INSERT INTO tbl (pk, v) VALUES ({}, 'value');", j));
                    query_views.emplace_back(queries.back());
                }
                try {
                    env.execute_batch(
                            query_views,
                            cql3::statements::batch_statement::type::LOGGED,
                            std::make_unique<cql3::query_options>(db::consistency_level::ONE, std::vector<cql3::raw_value>())).get();
                } catch (std::runtime_error& ex) {
                    BOOST_REQUIRE_EQUAL(std::string(ex.what()), "Error injection: failing to send batch");
                }
            }
        }

        // Ensure select ... where write_time < write_time_limit (=now) picks up all batches.
        sleep(2ms).get();

        const auto batchlog_query = format("SELECT * FROM {}.{}", db::system_keyspace::NAME, db::system_keyspace::BATCHLOG_V2);
        const auto fragments_query = format("SELECT * FROM MUTATION_FRAGMENTS({}.{}) WHERE partition_region = 2 ALLOW FILTERING", db::system_keyspace::NAME, db::system_keyspace::BATCHLOG_V2);

        std::set<utils::UUID> ids;
        std::set<db_clock::time_point> written_ats;
        assert_that(env.execute_cql(batchlog_query).get())
            .is_rows()
            .with_size(batch_count)
            .assert_for_columns_of_each_row([&] (columns_assertions& columns) {
                columns.with_typed_column<int32_t>("version", netw::messaging_service::current_version)
                    .with_typed_column<int32_t>("stage", int32_t(db::batchlog_stage::initial))
                    .with_typed_column<db_clock::time_point>("written_at", [&] (db_clock::time_point written_at) {
                        written_ats.insert(written_at);
                        return true;
                    })
                    .with_typed_column<utils::UUID>("id", [&] (utils::UUID id) {
                        ids.insert(id);
                        return true;
                    });
            });

        BOOST_REQUIRE_EQUAL(ids.size(), batch_count);
        BOOST_REQUIRE_LE(written_ats.size(), batch_count);

        auto do_replays = [&] (db::batchlog_manager::post_replay_cleanup cleanup) {
            for (unsigned i = 0; i < 3; ++i) {
                testlog.info("Replay attempt [cleanup={}] #{} - batches should be in failed_replay stage", cleanup, i);

                bm.do_batch_log_replay(cleanup).get();

                assert_that(env.execute_cql(batchlog_query).get())
                    .is_rows()
                    .with_size(batch_count)
                    .assert_for_columns_of_each_row([&] (columns_assertions& columns) {
                        columns.with_typed_column<int32_t>("version", netw::messaging_service::current_version)
                            .with_typed_column<int32_t>("stage", [&] (int32_t stage) {
                                // (0) cleanup::no  == db::batchlog_stage::initial
                                // (1) cleanup::yes == db::batchlog_stage::failed_replay
                                return stage == int32_t(bool(cleanup));
                            })
                            .with_typed_column<db_clock::time_point>("written_at", [&] (db_clock::time_point written_at) {
                                return written_ats.contains(written_at);
                            })
                            .with_typed_column<utils::UUID>("id", [&] (utils::UUID id) {
                                return ids.contains(id);
                            });
                    });

                auto fragments_results = cql3::untyped_result_set(env.execute_cql(fragments_query).get());
                if (cleanup) {
                    // Each shard has a range tombstone (shard_count * 2 range tombstone changes)
                    // There should be batch_count clustering rows, with stage=1
                    // There may be [0, batch_count] clustering rows with stage=0
                    BOOST_REQUIRE_GE(fragments_results.size(), shard_count * 2 + batch_count);
                    BOOST_REQUIRE_LE(fragments_results.size(), shard_count * 2 + batch_count * 2);
                } else {
                    BOOST_REQUIRE_EQUAL(fragments_results.size(), batch_count); // only clustering rows
                }
                for (const auto& row : fragments_results) {
                    if (row.get_as<sstring>("mutation_fragment_kind") == "range tombstone change") {
                        continue;
                    }

                    const auto id = row.get_as<utils::UUID>("id");
                    const auto stage = row.get_as<int32_t>("stage");

                    testlog.trace("Processing row for batch id={}, stage={}: ", id, stage);

                    BOOST_REQUIRE_EQUAL(row.get_as<int32_t>("version"), netw::messaging_service::current_version);
                    BOOST_REQUIRE(ids.contains(id));

                    const auto metadata = row.get_as<sstring>("metadata");
                    auto metadata_json = rjson::parse(metadata);
                    BOOST_REQUIRE(metadata_json.IsObject());

                    if (!cleanup || stage == int32_t(db::batchlog_stage::failed_replay)) {
                        BOOST_REQUIRE_NE(row.get_as<sstring>("value"), "{}");
                        BOOST_REQUIRE(!metadata_json.HasMember("tombstone"));
                        const auto value_json = rjson::parse(row.get_as<sstring>("value"));
                        BOOST_REQUIRE(value_json.IsObject());
                        BOOST_REQUIRE(value_json.HasMember("data"));
                    } else if (stage == int32_t(db::batchlog_stage::initial)) {
                        BOOST_REQUIRE_EQUAL(row.get_as<sstring>("value"), "{}"); // row should be dead -- data column shadowed by tombstone
                        if (!cleanup) {
                            BOOST_REQUIRE(metadata_json.HasMember("tombstone"));
                        }
                    } else {
                        BOOST_FAIL(format("Unexpected stage: {}", stage));
                    }
                }
            }
        };

        {
            scoped_error_injection error_injection("storage_proxy_fail_replay_batch");
            do_replays(db::batchlog_manager::post_replay_cleanup::no);
            do_replays(db::batchlog_manager::post_replay_cleanup::yes);
        }

        testlog.info("Successful replay - should remove all batches");
        bm.do_batch_log_replay(db::batchlog_manager::post_replay_cleanup::no).get();

        assert_that(env.execute_cql(batchlog_query).get())
            .is_rows()
            .is_empty();

        const auto fragment_results = cql3::untyped_result_set(env.execute_cql(fragments_query).get());
        // Each shard has a range tombstone (shard_count * 2 range tombstone changes)
        // There should be batch_count clustering rows, with stage=1
        // There may be [0, batch_count] clustering rows with stage=0
        BOOST_REQUIRE_GE(fragment_results.size(), shard_count * 2 + batch_count);
        BOOST_REQUIRE_LE(fragment_results.size(), shard_count * 2 + batch_count * 2);
        for (const auto& row : fragment_results) {
            if (row.get_as<sstring>("mutation_fragment_kind") == "range tombstone change") {
                continue;
            }

            BOOST_REQUIRE_EQUAL(row.get_as<int32_t>("version"), netw::messaging_service::current_version);
            BOOST_REQUIRE_EQUAL(row.get_as<int32_t>("stage"), int32_t(db::batchlog_stage::failed_replay));
            BOOST_REQUIRE(written_ats.contains(row.get_as<db_clock::time_point>("written_at")));
            BOOST_REQUIRE(ids.contains(row.get_as<utils::UUID>("id")));
            BOOST_REQUIRE_EQUAL(row.get_as<sstring>("value"), "{}");
        }
    }, cfg);
}

SEASTAR_TEST_CASE(test_batchlog_migrate_v1_v2) {
#ifndef SCYLLA_ENABLE_ERROR_INJECTION
    return make_ready_future<>();
#endif

    const auto batch_replay_timeout = 1s;

    cql_test_config cfg;
    cfg.db_config->batchlog_replay_cleanup_after_replays.set_value("9999999", utils::config_file::config_source::Internal);
    cfg.batchlog_replay_timeout = batch_replay_timeout;
    cfg.batchlog_delay = std::chrono::hours(9999);

    return do_with_cql_env_thread([batch_replay_timeout] (cql_test_env& env) -> void {
        auto& bm = env.batchlog_manager().local();

        env.execute_cql("CREATE TABLE tbl (pk bigint PRIMARY KEY, v text)").get();

        auto& sp = env.get_storage_proxy().local();
        auto& db = env.local_db();

        auto& tbl = db.find_column_family("ks", "tbl");
        auto tbl_schema = tbl.schema();
        auto cdef_tbl_v = tbl_schema->get_column_definition(to_bytes("v"));

        auto batchlog_v1_schema = db.find_schema(db::system_keyspace::NAME, db::system_keyspace::BATCHLOG);
        auto cdef_batchlog_v1_data = batchlog_v1_schema->get_column_definition(to_bytes("data"));

        const uint64_t batch_count = 8;

        struct batchlog {
            utils::UUID id;
            int32_t version;
            db_clock::time_point written_at;
            managed_bytes data;
        };
        std::map<utils::UUID, const batchlog> batchlogs;

        for (int64_t i = 0; i != batch_count; ++i) {
            bytes_ostream batchlog_data;
            for (int64_t j = 0; j != i+2; ++j) {
                auto key = partition_key::from_single_value(*tbl_schema, serialized(j));
                mutation m(tbl_schema, key);
                m.set_clustered_cell(clustering_key::make_empty(), *cdef_tbl_v, make_atomic_cell(utf8_type, serialized("value")));
                ser::serialize(batchlog_data, canonical_mutation(m));
            }

            const auto id = utils::UUID_gen::get_time_UUID();
            auto [it, _] = batchlogs.emplace(id, batchlog{
                    .id = id,
                    .version = netw::messaging_service::current_version,
                    .written_at = db_clock::now() - batch_replay_timeout * 10,
                    .data = std::move(batchlog_data).to_managed_bytes()});

            auto& batch = it->second;

            const auto timestamp = api::new_timestamp();
            mutation m(batchlog_v1_schema, partition_key::from_single_value(*batchlog_v1_schema, serialized(batch.id)));
            m.set_cell(clustering_key_prefix::make_empty(), to_bytes("version"), batch.version, timestamp);
            m.set_cell(clustering_key_prefix::make_empty(), to_bytes("written_at"), batch.written_at, timestamp);
            m.set_cell(clustering_key_prefix::make_empty(), *cdef_batchlog_v1_data, atomic_cell::make_live(*cdef_batchlog_v1_data->type, timestamp, std::move(batch.data)));

            sp.mutate_locally(m, tracing::trace_state_ptr(), db::commitlog::force_sync::no).get();
        }

        const auto batchlog_v1_query = format("SELECT * FROM {}.{}", db::system_keyspace::NAME, db::system_keyspace::BATCHLOG);
        const auto batchlog_v2_query = format("SELECT * FROM {}.{}", db::system_keyspace::NAME, db::system_keyspace::BATCHLOG_V2);

        // Initial state, all entries are in the v1 table.
        assert_that(env.execute_cql(batchlog_v1_query).get())
            .is_rows()
            .with_size(batch_count);

        assert_that(env.execute_cql(batchlog_v2_query).get())
            .is_rows()
            .is_empty();

        {
            scoped_error_injection error_injection("batchlog_manager_fail_migration");

            testlog.info("First replay - migration should fail, all entries stay in v1 table");
            bm.do_batch_log_replay(db::batchlog_manager::post_replay_cleanup::no).get();
        }

        assert_that(env.execute_cql(batchlog_v1_query).get())
            .is_rows()
            .with_size(batch_count);

        assert_that(env.execute_cql(batchlog_v2_query).get())
            .is_rows()
            .is_empty();

        {
            scoped_error_injection error_injection("storage_proxy_fail_replay_batch");

            testlog.info("Second replay - migration should run again and succeed, but replay of migrated entries should fail, so they should remain in the v2 table.");
            bm.do_batch_log_replay(db::batchlog_manager::post_replay_cleanup::no).get();
        }

        assert_that(env.execute_cql(batchlog_v1_query).get())
            .is_rows()
            .is_empty();

        auto results = cql3::untyped_result_set(env.execute_cql(batchlog_v2_query).get());
        BOOST_REQUIRE_EQUAL(results.size(), batch_count);

        std::set<utils::UUID> migrated_batchlog_ids;
        for (const auto& row : results) {
            BOOST_REQUIRE_EQUAL(row.get_as<int32_t>("stage"), 1);

            const auto id = row.get_as<utils::UUID>("id");
            auto it = batchlogs.find(id);
            BOOST_REQUIRE(it != batchlogs.end());
            const auto& batch = it->second;

            BOOST_REQUIRE_EQUAL(row.get_as<int32_t>("version"), batch.version);
            BOOST_REQUIRE(row.get_as<db_clock::time_point>("written_at") == batch.written_at);
            BOOST_REQUIRE_EQUAL(row.get_blob_fragmented("data"), batch.data);

            migrated_batchlog_ids.emplace(id);
        }

        BOOST_REQUIRE_EQUAL(batchlogs.size(), migrated_batchlog_ids.size());

        testlog.info("Third replay - migration is already done, replay of migrated entries should succeed, v2 table should be empty afterwards.");
        bm.do_batch_log_replay(db::batchlog_manager::post_replay_cleanup::no).get();

        assert_that(env.execute_cql(batchlog_v2_query).get())
            .is_rows()
            .is_empty();
    }, cfg);
}

BOOST_AUTO_TEST_SUITE_END()
