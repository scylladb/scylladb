/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// Tests of the choice of the format of new sstables (PLAN.md, "Format
// selection"): the `sstable_data_format` cluster config option, its
// resolution through the table, keyspace, and cluster scopes, the tables
// that ignore it, the test-only default, and the paths that convert a
// table: flush, compaction, and `upgradesstables`.

#include <set>

#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>

#include "compaction/compaction_manager.hh"
#include "db/cluster_config_manager.hh"
#include "db/config.hh"
#include "replica/database.hh"
#include "schema/schema_builder.hh"
#include "sstables/parquet/schema.hh"
#include "sstables/sstables.hh"
#include "sstables/sstables_manager.hh"
#include "sstables/version.hh"
#include "test/lib/cql_assertions.hh"
#include "test/lib/cql_test_env.hh"
#include "test/lib/eventually.hh"
#include "test/lib/exception_utils.hh"

using sstables::sstable_format_types;

namespace {

// The tests set the test-only default explicitly, so that they also pass
// when the suite runs with Parquet as the default.
cql_test_config make_config(sstring test_default = "native") {
    cql_test_config cfg;
    cfg.db_config->test_sstable_data_format_default.set(std::move(test_default));
    cfg.db_config->enable_create_table_with_compact_storage.set(true);
    return cfg;
}

// Runs a statement that changes cluster config, and waits until every shard
// sees the change. The cluster config manager refreshes its caches in the
// background.
void alter(cql_test_env& e, std::string_view statement) {
    e.execute_cql(statement).get();
    e.cluster_config_manager().local().refresh().get();
}

sstable_format_types chosen_format(cql_test_env& e, std::string_view ks, std::string_view cf) {
    auto& t = e.local_db().find_column_family(ks, cf);
    return sstables::format_of(t.get_sstables_manager().choose_sstable_version(*t.schema()));
}

void flush(cql_test_env& e, std::string_view ks, std::string_view cf) {
    e.db().invoke_on_all([ks = sstring(ks), cf = sstring(cf)] (replica::database& db) {
        return db.flush(ks, cf);
    }).get();
}

// The formats of the live sstables of a table, on all shards: "big", "pq",
// or both.
std::set<sstring> formats_on_disk(cql_test_env& e, std::string_view ks, std::string_view cf) {
    return e.db().map_reduce0([ks = sstring(ks), cf = sstring(cf)] (replica::database& db) {
        std::set<sstring> formats;
        for (const auto& sst : *db.find_column_family(ks, cf).get_sstables()) {
            formats.insert(sstables::format_of(sst->get_version()) == sstable_format_types::pq ? "pq" : "big");
        }
        return formats;
    }, std::set<sstring>{}, [] (auto a, const auto& b) {
        a.insert(b.begin(), b.end());
        return a;
    }).get();
}

const std::set<sstring> big_only{"big"};
const std::set<sstring> pq_only{"pq"};
const std::set<sstring> big_and_pq{"big", "pq"};

// The generations of the live sstables of a table, on all shards. A rewrite
// changes them.
std::set<sstables::generation_type> generations_on_disk(cql_test_env& e, std::string_view ks, std::string_view cf) {
    return e.db().map_reduce0([ks = sstring(ks), cf = sstring(cf)] (replica::database& db) {
        std::set<sstables::generation_type> generations;
        for (const auto& sst : *db.find_column_family(ks, cf).get_sstables()) {
            generations.insert(sst->generation());
        }
        return generations;
    }, std::set<sstables::generation_type>{}, [] (auto a, const auto& b) {
        a.insert(b.begin(), b.end());
        return a;
    }).get();
}

// What `nodetool upgradesstables` does for a table, without and with
// `--include-all-sstables`.
void upgrade_sstables(cql_test_env& e, std::string_view ks, std::string_view cf, bool include_all) {
    e.db().invoke_on_all([ks = sstring(ks), cf = sstring(cf), include_all] (replica::database& db) -> future<> {
        auto& t = db.find_column_family(ks, cf);
        co_await t.parallel_foreach_compaction_group_view([&] (compaction::compaction_group_view& ts) {
            return db.get_compaction_manager().perform_sstable_upgrade(nullptr, ts, !include_all, tasks::make_empty_task_info());
        });
    }).get();
}

void insert_rows(cql_test_env& e, std::string_view table, int from, int to) {
    for (int pk = from; pk < to; ++pk) {
        e.execute_cql(seastar::format("INSERT INTO {} (pk, v) VALUES ({}, {})", table, pk, pk * 10)).get();
    }
}

void require_rows(cql_test_env& e, std::string_view table, int count) {
    std::vector<std::vector<bytes_opt>> expected;
    for (int pk = 0; pk < count; ++pk) {
        expected.push_back({int32_type->decompose(pk), int32_type->decompose(pk * 10)});
    }
    assert_that(e.execute_cql(seastar::format("SELECT pk, v FROM {}", table)).get())
            .is_rows().with_rows_ignore_order(expected);
}

void create_keyspace_and_table(cql_test_env& e) {
    e.execute_cql("CREATE KEYSPACE tks WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}").get();
    e.execute_cql("CREATE TABLE tks.t (pk int PRIMARY KEY, v int)").get();
}

}

SEASTAR_TEST_CASE(test_sstable_data_format_resolution) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        create_keyspace_and_table(e);
        e.execute_cql("CREATE TABLE tks.other (pk int PRIMARY KEY, v int)").get();
        e.execute_cql("CREATE KEYSPACE tks2 WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}").get();
        e.execute_cql("CREATE TABLE tks2.t (pk int PRIMARY KEY, v int)").get();

        BOOST_REQUIRE(chosen_format(e, "tks", "t") == sstable_format_types::big);

        alter(e, "ALTER CLUSTER WITH sstable_data_format = 'parquet'");
        BOOST_REQUIRE(chosen_format(e, "tks", "t") == sstable_format_types::pq);
        BOOST_REQUIRE(chosen_format(e, "tks2", "t") == sstable_format_types::pq);

        // The keyspace scope wins over the cluster scope.
        alter(e, "ALTER KEYSPACE tks WITH sstable_data_format = 'native'");
        BOOST_REQUIRE(chosen_format(e, "tks", "t") == sstable_format_types::big);
        BOOST_REQUIRE(chosen_format(e, "tks", "other") == sstable_format_types::big);
        BOOST_REQUIRE(chosen_format(e, "tks2", "t") == sstable_format_types::pq);

        // The table scope wins over the keyspace scope.
        alter(e, "ALTER TABLE tks.t WITH sstable_data_format = 'parquet'");
        BOOST_REQUIRE(chosen_format(e, "tks", "t") == sstable_format_types::pq);
        BOOST_REQUIRE(chosen_format(e, "tks", "other") == sstable_format_types::big);

        // Removing an override falls back to the next scope.
        alter(e, "ALTER TABLE tks.t WITH sstable_data_format = NULL");
        BOOST_REQUIRE(chosen_format(e, "tks", "t") == sstable_format_types::big);
        alter(e, "ALTER KEYSPACE tks WITH sstable_data_format = NULL");
        BOOST_REQUIRE(chosen_format(e, "tks", "t") == sstable_format_types::pq);
        alter(e, "ALTER CLUSTER WITH sstable_data_format = NULL");
        BOOST_REQUIRE(chosen_format(e, "tks", "t") == sstable_format_types::big);
        BOOST_REQUIRE(chosen_format(e, "tks2", "t") == sstable_format_types::big);

        // The table scope alone, and in CREATE TABLE.
        alter(e, "ALTER TABLE tks.t WITH sstable_data_format = 'parquet'");
        BOOST_REQUIRE(chosen_format(e, "tks", "t") == sstable_format_types::pq);
        BOOST_REQUIRE(chosen_format(e, "tks", "other") == sstable_format_types::big);
        alter(e, "CREATE TABLE tks.created (pk int PRIMARY KEY, v int) WITH sstable_data_format = 'parquet'");
        BOOST_REQUIRE(chosen_format(e, "tks", "created") == sstable_format_types::pq);
    }, make_config());
}

SEASTAR_TEST_CASE(test_sstable_data_format_rejects_invalid_values) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        create_keyspace_and_table(e);
        const auto invalid = exception_predicate::message_contains("'native' or 'parquet'");
        for (const auto& value : {"'orc'", "'Parquet'", "''", "1", "true"}) {
            BOOST_REQUIRE_EXCEPTION(e.execute_cql(seastar::format("ALTER CLUSTER WITH sstable_data_format = {}", value)).get(), std::exception, invalid);
            BOOST_REQUIRE_EXCEPTION(e.execute_cql(seastar::format("ALTER KEYSPACE tks WITH sstable_data_format = {}", value)).get(), std::exception, invalid);
            BOOST_REQUIRE_EXCEPTION(e.execute_cql(seastar::format("ALTER TABLE tks.t WITH sstable_data_format = {}", value)).get(), std::exception, invalid);
        }
        e.cluster_config_manager().local().refresh().get();
        BOOST_REQUIRE(chosen_format(e, "tks", "t") == sstable_format_types::big);
    }, make_config());
}

// The option's cluster feature gates it: until every node knows the option,
// it cannot be set, and the test-only default has no effect either.
SEASTAR_TEST_CASE(test_sstable_data_format_waits_for_its_feature) {
    auto cfg = make_config("parquet");
    cfg.disabled_features.emplace("CLUSTER_CONFIG_SSTABLE_DATA_FORMAT");
    return do_with_cql_env_thread([] (cql_test_env& e) {
        create_keyspace_and_table(e);
        const auto unsupported = exception_predicate::message_contains("is not yet supported by this cluster");
        BOOST_REQUIRE_EXCEPTION(e.execute_cql("ALTER CLUSTER WITH sstable_data_format = 'parquet'").get(), std::exception, unsupported);
        BOOST_REQUIRE_EXCEPTION(e.execute_cql("ALTER KEYSPACE tks WITH sstable_data_format = 'parquet'").get(), std::exception, unsupported);
        BOOST_REQUIRE_EXCEPTION(e.execute_cql("ALTER TABLE tks.t WITH sstable_data_format = 'parquet'").get(), std::exception, unsupported);
        BOOST_REQUIRE(chosen_format(e, "tks", "t") == sstable_format_types::big);
    }, std::move(cfg));
}

SEASTAR_TEST_CASE(test_sstable_data_format_ignored_by_system_and_compact_storage_tables) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        create_keyspace_and_table(e);
        e.execute_cql("CREATE TABLE tks.compact (pk int PRIMARY KEY, v int) WITH COMPACT STORAGE").get();
        e.execute_cql("CREATE TABLE tks.dense (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH COMPACT STORAGE").get();
        alter(e, "ALTER CLUSTER WITH sstable_data_format = 'parquet'");
        alter(e, "ALTER TABLE tks.compact WITH sstable_data_format = 'parquet'");

        BOOST_REQUIRE(chosen_format(e, "tks", "t") == sstable_format_types::pq);
        BOOST_REQUIRE(chosen_format(e, "tks", "compact") == sstable_format_types::big);
        BOOST_REQUIRE(chosen_format(e, "tks", "dense") == sstable_format_types::big);
        for (const auto& [ks, cf] : {std::pair{"system", "local"}, {"system_schema", "tables"}, {"system_distributed", "cdc_streams_descriptions_v2"}, {"system_auth", "roles"}}) {
            if (!e.local_db().has_schema(ks, cf)) {
                continue;
            }
            BOOST_TEST_MESSAGE(seastar::format("checking {}.{}", ks, cf));
            BOOST_REQUIRE(chosen_format(e, ks, cf) == sstable_format_types::big);
        }

        insert_rows(e, "tks.compact", 0, 10);
        flush(e, "tks", "compact");
        BOOST_REQUIRE(formats_on_disk(e, "tks", "compact") == big_only);
        require_rows(e, "tks.compact", 10);

        flush(e, "system", "local");
        BOOST_REQUIRE(formats_on_disk(e, "system", "local") == big_only);

        // A column name whose Parquet name is longer than a reader accepts.
        auto with_column = [] (std::string name) {
            return schema_builder(this_smp_shard_count(), "tks", "long")
                    .with_column("pk", int32_type, column_kind::partition_key)
                    .with_column(to_bytes(name), int32_type)
                    .build();
        };
        BOOST_REQUIRE(sstables::sstables_manager::may_use_parquet(*with_column(std::string(sstables::parquet::max_name_size, 'a'))));
        BOOST_REQUIRE(!sstables::sstables_manager::may_use_parquet(*with_column(std::string(sstables::parquet::max_name_size + 1, 'a'))));
    }, make_config());
}

// Flush, compaction, and reads follow the option, and a table may hold
// sstables of both formats at once.
SEASTAR_TEST_CASE(test_sstable_data_format_takes_effect_on_the_next_sstable) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        create_keyspace_and_table(e);

        insert_rows(e, "tks.t", 0, 10);
        flush(e, "tks", "t");
        BOOST_REQUIRE(formats_on_disk(e, "tks", "t") == big_only);

        alter(e, "ALTER TABLE tks.t WITH sstable_data_format = 'parquet'");
        // Existing sstables stay as they are.
        BOOST_REQUIRE(formats_on_disk(e, "tks", "t") == big_only);
        insert_rows(e, "tks.t", 10, 20);
        flush(e, "tks", "t");
        BOOST_REQUIRE(formats_on_disk(e, "tks", "t") == big_and_pq);
        require_rows(e, "tks.t", 20);

        // Switching back gives the native format again.
        alter(e, "ALTER TABLE tks.t WITH sstable_data_format = 'native'");
        insert_rows(e, "tks.t", 20, 30);
        flush(e, "tks", "t");
        BOOST_REQUIRE(formats_on_disk(e, "tks", "t") == big_and_pq);
        require_rows(e, "tks.t", 30);

        // A compaction writes the format of the table's choice.
        alter(e, "ALTER TABLE tks.t WITH sstable_data_format = 'parquet'");
        e.db().invoke_on_all([] (replica::database& db) {
            return db.find_column_family("tks", "t").compact_all_sstables(tasks::make_empty_task_info());
        }).get();
        BOOST_REQUIRE(formats_on_disk(e, "tks", "t") == pq_only);
        require_rows(e, "tks.t", 30);
    }, make_config());
}

SEASTAR_TEST_CASE(test_upgradesstables_converts_both_ways) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        create_keyspace_and_table(e);
        for (int i = 0; i < 3; ++i) {
            insert_rows(e, "tks.t", i * 10, i * 10 + 10);
            flush(e, "tks", "t");
        }
        BOOST_REQUIRE(formats_on_disk(e, "tks", "t") == big_only);

        alter(e, "ALTER TABLE tks.t WITH sstable_data_format = 'parquet'");
        upgrade_sstables(e, "tks", "t", false);
        BOOST_REQUIRE(formats_on_disk(e, "tks", "t") == pq_only);
        require_rows(e, "tks.t", 30);

        // Nothing is left to convert, so a plain upgrade rewrites nothing.
        auto generations = generations_on_disk(e, "tks", "t");
        upgrade_sstables(e, "tks", "t", false);
        BOOST_REQUIRE(generations_on_disk(e, "tks", "t") == generations);

        // --include-all-sstables rewrites every sstable, in the same format.
        upgrade_sstables(e, "tks", "t", true);
        auto rewritten = generations_on_disk(e, "tks", "t");
        BOOST_REQUIRE(!rewritten.empty());
        for (const auto& g : rewritten) {
            BOOST_REQUIRE(!generations.contains(g));
        }
        BOOST_REQUIRE(formats_on_disk(e, "tks", "t") == pq_only);

        alter(e, "ALTER TABLE tks.t WITH sstable_data_format = NULL");
        upgrade_sstables(e, "tks", "t", false);
        BOOST_REQUIRE(formats_on_disk(e, "tks", "t") == big_only);
        require_rows(e, "tks.t", 30);
    }, make_config());
}

// A materialized view or a secondary index has no table scope of its own,
// and follows the table scope of its base table.
SEASTAR_TEST_CASE(test_views_and_indexes_of_a_parquet_table) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        create_keyspace_and_table(e);
        e.execute_cql("CREATE MATERIALIZED VIEW tks.mv AS SELECT pk, v FROM tks.t WHERE v IS NOT NULL AND pk IS NOT NULL PRIMARY KEY (v, pk)").get();
        e.execute_cql("CREATE INDEX t_v ON tks.t (v)").get();

        BOOST_REQUIRE_EXCEPTION(e.execute_cql("ALTER MATERIALIZED VIEW tks.mv WITH sstable_data_format = 'parquet'").get(), std::exception,
                exception_predicate::message_contains("not supported on materialized views"));

        alter(e, "ALTER TABLE tks.t WITH sstable_data_format = 'parquet'");
        BOOST_REQUIRE(chosen_format(e, "tks", "mv") == sstable_format_types::pq);
        BOOST_REQUIRE(chosen_format(e, "tks", "t_v_index") == sstable_format_types::pq);

        insert_rows(e, "tks.t", 0, 20);
        eventually([&] {
            auto rows = e.execute_cql("SELECT pk FROM tks.mv").get();
            assert_that(rows).is_rows().with_size(20);
        });
        eventually([&] {
            assert_that(e.execute_cql("SELECT pk, v FROM tks.t WHERE v = 70").get())
                    .is_rows().with_rows({{int32_type->decompose(7), int32_type->decompose(70)}});
        });
        for (const auto& cf : {"t", "mv", "t_v_index"}) {
            flush(e, "tks", cf);
            BOOST_REQUIRE(formats_on_disk(e, "tks", cf) == pq_only);
        }

        assert_that(e.execute_cql("SELECT pk FROM tks.mv WHERE v = 130").get())
                .is_rows().with_rows({{int32_type->decompose(13)}});
        assert_that(e.execute_cql("SELECT pk, v FROM tks.t WHERE v = 70").get())
                .is_rows().with_rows({{int32_type->decompose(7), int32_type->decompose(70)}});
        require_rows(e, "tks.t", 20);
    }, make_config());
}

// The test-only default makes Parquet the default for the tables that do not
// choose a format, and a stored override wins over it.
SEASTAR_TEST_CASE(test_test_only_default) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        create_keyspace_and_table(e);
        BOOST_REQUIRE(chosen_format(e, "tks", "t") == sstable_format_types::pq);
        BOOST_REQUIRE(chosen_format(e, "system", "local") == sstable_format_types::big);

        insert_rows(e, "tks.t", 0, 10);
        flush(e, "tks", "t");
        BOOST_REQUIRE(formats_on_disk(e, "tks", "t") == pq_only);
        require_rows(e, "tks.t", 10);

        alter(e, "ALTER TABLE tks.t WITH sstable_data_format = 'native'");
        BOOST_REQUIRE(chosen_format(e, "tks", "t") == sstable_format_types::big);

        // The option is live-updatable.
        e.db_config().test_sstable_data_format_default.set("native");
        e.execute_cql("CREATE TABLE tks.other (pk int PRIMARY KEY, v int)").get();
        BOOST_REQUIRE(chosen_format(e, "tks", "other") == sstable_format_types::big);
    }, make_config("parquet"));
}
