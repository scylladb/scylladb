/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */


#include <boost/test/unit_test.hpp>

#undef SEASTAR_TESTING_MAIN
#include <seastar/testing/test_case.hh>

#include <filesystem>
#include <set>

#include "test/lib/cql_test_env.hh"
#include "sstables/sstables.hh"
#include "sstables/parquet/writer_impl.hh"
#include "test/lib/cql_assertions.hh"
#include "test/lib/key_utils.hh"
#include "test/lib/sstable_utils.hh"
#include "test/lib/test_utils.hh"
#include "replica/distributed_loader.hh"
#include "mutation/mutation.hh"

BOOST_AUTO_TEST_SUITE(cql_ddl_test)

/// Check that writes interact with caching = {'enabled': X} as expected:
/// * enable: true -> data is merged into cache on memtable flush [1]
/// * enable: false -> data is not merged into cache on memtable flush
///
/// [1] Important: only partitions which are either already in the cache,
///     or are not present in underlying (disk) are merged.
future<> writes_with_caching_toggle(bool enabled) {
    return do_with_cql_env_thread([enabled] (cql_test_env& e) {
        e.execute_cql(format("CREATE TABLE ks.tbl (pk int PRIMARY KEY, v text) WITH CACHING = {{'enabled': '{}'}}", enabled)).get();

        const auto& table = e.local_db().find_column_family("ks", "tbl");
        const auto table_id = table.schema()->id();

        auto write_rows = [&, first_pk = 0] () mutable {
            sstring value(128, 'v');
            const auto cql3_value = cql3::raw_value::make_value(serialized(value));

            auto id = e.prepare("INSERT INTO ks.tbl (pk, v) VALUES (?, ?);").get();
            for (int flushes = 0; flushes < 5; flushes++) {
                for (int32_t pk = first_pk; pk < first_pk + 10; ++pk) {
                    const auto cql3_pk = cql3::raw_value::make_value(serialized(pk));
                    e.execute_prepared(id, {cql3_pk, cql3_value}).get();
                }
                replica::database::flush_table_on_all_shards(e.db(), table_id).get();
            }
            first_pk += 10;
        };

        auto check_expected_cache_content = [&] (bool cache_enabled) {
            const auto get_cache_shards_with_content = e.db().map_reduce0([] (const replica::database& db) {
                auto& t = db.find_column_family("ks", "tbl");
                return uint64_t(!t.get_row_cache().empty());
            }, uint64_t(0), std::plus<uint64_t>()).get();

            if (cache_enabled) {
                BOOST_REQUIRE_GT(get_cache_shards_with_content, 0);
            } else {
                BOOST_REQUIRE_EQUAL(get_cache_shards_with_content, 0);
            }
        };

        write_rows();
        check_expected_cache_content(enabled);

        replica::database::drop_cache_for_table_on_all_shards(e.db(), table_id).get();
        e.execute_cql(format("ALTER TABLE ks.tbl WITH CACHING = {{'enabled': '{}'}}", !enabled)).get();

        write_rows();
        check_expected_cache_content(!enabled);
    });
}

SEASTAR_TEST_CASE(test_writes_with_caching_disabled) {
    return writes_with_caching_toggle(false);
}

SEASTAR_TEST_CASE(test_writes_with_caching_enabled) {
    return writes_with_caching_toggle(true);
}

// The `parquet = {...}` table property, end to end through CQL.
//
// parquet_parameters has its own unit test for parsing and validation. What that cannot
// cover is the part that actually broke first: whether the property survives being
// *stored*. to_map() originally emitted the internal "L0" while the parser accepted
// "verbatim", so writing the property and reading it back failed its own validation --
// invisible to any test that only parses. This drives CREATE, ALTER and a schema reload.
SEASTAR_TEST_CASE(test_parquet_table_property) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("CREATE TABLE ks.pqt (pk int PRIMARY KEY, v int) "
                      "WITH parquet = {'rows_per_row_group': '5000'}").get();
        {
            auto s = e.local_db().find_schema("ks", "pqt");
            BOOST_REQUIRE_EQUAL(s->parquet_options().at("rows_per_row_group"), "5000");
        }

        // ALTER replaces the map, and a folding level exercises the round trip that broke.
        e.execute_cql("ALTER TABLE ks.pqt WITH parquet = "
                      "{'rows_per_row_group': '20000', 'metadata_folding': 'verbatim'}").get();
        {
            auto s = e.local_db().find_schema("ks", "pqt");
            BOOST_REQUIRE_EQUAL(s->parquet_options().at("rows_per_row_group"), "20000");
            BOOST_REQUIRE_EQUAL(s->parquet_options().at("metadata_folding"), "verbatim");
        }

        // Bad values must be configuration errors at DDL time, not surprises at write
        // time. Each of these is rejected for a different reason: unknown sub-option,
        // below the row-group floor where per-row-group metadata dominates, a codec the
        // writer cannot emit, and the export-only folding level that would discard write
        // times and TTLs.
        for (const char* bad : {
                "{'row_groop_rows': '5000'}",
                "{'rows_per_row_group': '100'}",
                "{'compression': 'gzip'}",
                "{'metadata_folding': 'logical'}"}) {
            BOOST_REQUIRE_THROW(
                    e.execute_cql(seastar::format(
                            "ALTER TABLE ks.pqt WITH parquet = {}", bad)).get(),
                    exceptions::configuration_exception);
        }

        // The rejected ALTERs must not have changed anything.
        auto s = e.local_db().find_schema("ks", "pqt");
        BOOST_REQUIRE_EQUAL(s->parquet_options().at("rows_per_row_group"), "20000");
    });
}

// `row_group_rows`, the pre-rename spelling of `rows_per_row_group`, through CQL.
//
// The unit test in parquet_writer_test covers the parser. What it cannot cover is the reason
// the alias exists: the property is *persisted*, so a table created before the rename has the
// old key sitting in its schema and reconstructs a parquet_parameters from it on every schema
// read and every subsequent DDL. The upgrade path is therefore "created with the old name,
// altered with the new one", and it has to work without an intermediate step.
SEASTAR_TEST_CASE(test_parquet_rows_per_row_group_alias) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        auto effective_rows = [&e] (const char* cf) {
            auto s = e.local_db().find_schema("ks", cf);
            return sstables::parquet::parquet_parameters(s->parquet_options())
                    .config().rows_per_row_group;
        };

        // Both spellings must produce the same *effective setting*, which is the only thing
        // that matters to the writer -- the stored text differs by design (see below).
        e.execute_cql("CREATE TABLE ks.pqold (pk int PRIMARY KEY, v int) "
                      "WITH parquet = {'row_group_rows': '20000'}").get();
        e.execute_cql("CREATE TABLE ks.pqnew (pk int PRIMARY KEY, v int) "
                      "WITH parquet = {'rows_per_row_group': '20000'}").get();
        BOOST_REQUIRE_EQUAL(effective_rows("pqold"), 20000u);
        BOOST_REQUIRE_EQUAL(effective_rows("pqold"), effective_rows("pqnew"));

        // The stored map echoes what the operator wrote, and DESCRIBE echoes the stored map
        // verbatim (schema.cc), so the old name survives in the schema of a table that used
        // it. That is deliberate: rewriting an operator's DDL text on their behalf is a worse
        // surprise than an old name in DESCRIBE, and the alias makes the old text keep working.
        {
            auto s = e.local_db().find_schema("ks", "pqold");
            BOOST_REQUIRE_EQUAL(s->parquet_options().at("row_group_rows"), "20000");
            BOOST_REQUIRE(!s->parquet_options().contains("rows_per_row_group"));
        }

        // DESCRIBE must round-trip: its output has to recreate the table. Rather than trust
        // that the emitted text is well-formed, feed the stored property back through CREATE
        // and check the result is the same effective setting. This is the check that would
        // have caught the "L0" vs "verbatim" bug, applied to the alias.
        {
            auto s = e.local_db().find_schema("ks", "pqold");
            e.execute_cql(seastar::format(
                    "CREATE TABLE ks.pqcopy (pk int PRIMARY KEY, v int) WITH parquet = "
                    "{{'row_group_rows': '{}'}}",
                    s->parquet_options().at("row_group_rows"))).get();
            BOOST_REQUIRE_EQUAL(effective_rows("pqcopy"), 20000u);
        }

        // The upgrade path the alias exists for: a table created with the old name is altered
        // using the new one. The ALTER replaces the map, so the old key is gone afterwards --
        // an operator can migrate the spelling, they are just never forced to.
        e.execute_cql("ALTER TABLE ks.pqold WITH parquet = "
                      "{'rows_per_row_group': '50000'}").get();
        {
            auto s = e.local_db().find_schema("ks", "pqold");
            BOOST_REQUIRE_EQUAL(s->parquet_options().at("rows_per_row_group"), "50000");
            BOOST_REQUIRE(!s->parquet_options().contains("row_group_rows"));
            BOOST_REQUIRE_EQUAL(effective_rows("pqold"), 50000u);
        }

        // ...and back the other way, because a rolled-back deployment script may still be
        // issuing the old name against a table that has already been migrated.
        e.execute_cql("ALTER TABLE ks.pqold WITH parquet = {'row_group_rows': '30000'}").get();
        BOOST_REQUIRE_EQUAL(effective_rows("pqold"), 30000u);

        // Both spellings in one map is a user error: it must be refused, not resolved by map
        // order. Also checked with equal values, where last-one-wins would look harmless.
        for (const char* bad : {
                "{'row_group_rows': '20000', 'rows_per_row_group': '40000'}",
                "{'rows_per_row_group': '40000', 'row_group_rows': '20000'}",
                "{'row_group_rows': '20000', 'rows_per_row_group': '20000'}"}) {
            BOOST_REQUIRE_THROW(
                    e.execute_cql(seastar::format(
                            "ALTER TABLE ks.pqold WITH parquet = {}", bad)).get(),
                    exceptions::configuration_exception);
        }

        // The old name is still range-checked, and the refused ALTERs changed nothing.
        BOOST_REQUIRE_THROW(
                e.execute_cql("ALTER TABLE ks.pqold WITH parquet = "
                              "{'row_group_rows': '100'}").get(),
                exceptions::configuration_exception);
        BOOST_REQUIRE_EQUAL(effective_rows("pqold"), 30000u);
    });
}

// The per-column `encoding.<col>` sub-option, end to end through CQL.
//
// The interesting cases are the rejections. An encoding that does not apply to a column's type
// cannot be honoured, and there is no good way to fail later: silently ignoring it is a setting
// that lies, and failing at write time takes the table down long after the DDL was accepted. So
// both a wrong type and a misspelled column name have to be configuration errors here.
//
// `auto` is checked too, because it is the only way to *undo* an override -- an ALTER replaces
// the whole map, but an operator scripting a change wants to name the column and cancel it.
SEASTAR_TEST_CASE(test_parquet_per_column_encoding_property) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("CREATE TABLE ks.pqe (pk int, ck text, v text, w double, "
                      "PRIMARY KEY ((pk), ck)) WITH parquet = "
                      "{'encoding.v': 'delta_byte_array', 'encoding.w': 'byte_stream_split'}")
                .get();
        {
            auto s = e.local_db().find_schema("ks", "pqe");
            BOOST_REQUIRE_EQUAL(s->parquet_options().at("encoding.v"), "delta_byte_array");
            BOOST_REQUIRE_EQUAL(s->parquet_options().at("encoding.w"), "byte_stream_split");
        }

        // Every rejection is for a different reason: an encoding that is not a member of the
        // enum, two that do not apply to the column's type, a column the table does not have,
        // and a missing column name.
        for (const char* bad : {
                "{'encoding.v': 'delta_magic'}",
                "{'encoding.v': 'delta_binary_packed'}",
                "{'encoding.v': 'byte_stream_split'}",
                "{'encoding.nosuch': 'plain'}",
                "{'encoding.': 'plain'}"}) {
            BOOST_REQUIRE_THROW(
                    e.execute_cql(seastar::format(
                            "ALTER TABLE ks.pqe WITH parquet = {}", bad)).get(),
                    exceptions::configuration_exception);
        }

        // The rejected ALTERs must have changed nothing.
        {
            auto s = e.local_db().find_schema("ks", "pqe");
            BOOST_REQUIRE_EQUAL(s->parquet_options().at("encoding.v"), "delta_byte_array");
        }

        // A clustering key is a legitimate target, and delta_binary_packed applies to an int.
        e.execute_cql("ALTER TABLE ks.pqe WITH parquet = {'encoding.ck': 'delta_byte_array'}")
                .get();
        {
            auto s = e.local_db().find_schema("ks", "pqe");
            BOOST_REQUIRE_EQUAL(s->parquet_options().at("encoding.ck"), "delta_byte_array");
            // The replaced map must not have kept the old entries.
            BOOST_REQUIRE(!s->parquet_options().contains("encoding.v"));
        }

        // `auto` is accepted and stored, so DESCRIBE keeps showing an explicit cancellation
        // rather than the setting vanishing from the schema.
        e.execute_cql("ALTER TABLE ks.pqe WITH parquet = {'encoding.ck': 'auto'}").get();
        {
            auto s = e.local_db().find_schema("ks", "pqe");
            BOOST_REQUIRE_EQUAL(s->parquet_options().at("encoding.ck"), "auto");
        }
    });
}

// storage_format actually converts on compaction, in both directions.
//
// The property has been parsed, validated and persisted for a while, but nothing acted on
// it: compaction was format-preserving, so `ALTER TABLE ... WITH storage_format =
// 'parquet'` recorded an intent that never happened. This drives the round trip that
// matters -- native to Parquet and back -- because converting *back* is the direction
// nobody thinks to check, and a table that cannot be un-converted is a trap.
SEASTAR_TEST_CASE(test_storage_format_converts_on_compaction) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("CREATE TABLE ks.conv (pk int PRIMARY KEY, v int)").get();
        auto& db = e.local_db();
        auto insert_and_flush = [&] (int base) {
            for (int i = 0; i < 20; ++i) {
                e.execute_cql(seastar::format(
                        "INSERT INTO ks.conv (pk, v) VALUES ({}, {})", base + i, i)).get();
            }
            e.db().invoke_on_all([] (replica::database& d) {
                return d.flush_all_memtables();
            }).get();
        };
        auto versions = [&] {
            std::set<sstables::sstable_version_types> out;
            auto& t = db.find_column_family("ks", "conv");
            for (auto&& sst : *t.get_sstables()) { out.insert(sst->get_version()); }
            return out;
        };

        insert_and_flush(0);
        // Flushes are never Parquet: the creator above only affects compaction outputs.
        BOOST_REQUIRE(!versions().contains(sstables::sstable_version_types::pq));

        e.execute_cql("ALTER TABLE ks.conv WITH storage_format = 'parquet'").get();
        insert_and_flush(100);
        db.find_column_family("ks", "conv").compact_all_sstables(tasks::task_info{}).get();
        BOOST_REQUIRE(versions() == std::set<sstables::sstable_version_types>{
                sstables::sstable_version_types::pq});

        // The data has to survive the conversion, not merely change format: read every
        // key back rather than trusting the format switch.
        for (int i = 0; i < 20; ++i) {
            assert_that(e.execute_cql(seastar::format(
                    "SELECT v FROM ks.conv WHERE pk = {}", 100 + i)).get())
                    .is_rows().with_size(1);
        }

        // And back again.
        e.execute_cql("ALTER TABLE ks.conv WITH storage_format = 'sstable'").get();
        insert_and_flush(200);
        db.find_column_family("ks", "conv").compact_all_sstables(tasks::task_info{}).get();
        BOOST_REQUIRE(!versions().contains(sstables::sstable_version_types::pq));
    });
}

// Every write path that creates an sstable *without* going through compaction has to honour
// storage_format, and four of them did not. Streaming, reshape, reshard and split all defaulted
// to the node's preferred native version, so a table declared 'parquet' silently accumulated
// native sstables. All four were found by grepping for creator assignments; none was caught by a
// test, which is what this case is for.
//
// The streaming creator is the one reachable from a single-node cql_test_env, and it is also the
// one an operator hits most often -- repair, bootstrap and `nodetool refresh` in load-and-stream
// mode all go through it.
SEASTAR_TEST_CASE(test_storage_format_honoured_by_streaming_writes) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("CREATE TABLE ks.strm (pk int PRIMARY KEY, v int) "
                      "WITH storage_format = 'parquet'").get();
        auto& t = e.local_db().find_column_family("ks", "strm");

        // The creator the streaming path uses, asked directly. Going through an actual repair
        // needs a second node; the contract being asserted is the creator's, and calling it is
        // the honest unit of that.
        auto sst = t.make_streaming_sstable_for_write();
        BOOST_REQUIRE(sst->get_version() == sstables::sstable_version_types::pq);

        auto staging = t.make_streaming_staging_sstable();
        BOOST_REQUIRE(staging->get_version() == sstables::sstable_version_types::pq);
        // Staging is about view building, and must not be lost to the format change.
        BOOST_REQUIRE(staging->state() == sstables::sstable_state::staging);

        // A table that has not opted in must be untouched.
        e.execute_cql("CREATE TABLE ks.strm_plain (pk int PRIMARY KEY, v int)").get();
        auto& p = e.local_db().find_column_family("ks", "strm_plain");
        BOOST_REQUIRE(p.make_streaming_sstable_for_write()->get_version()
                      != sstables::sstable_version_types::pq);

        // 'hybrid' under a size-tiered strategy streams native: streamed data has just arrived and
        // is not bottom-tier, so C1 would decline it anyway. The strategy is pinned rather than
        // left to the build's default, because this arm is only meaningful as the *counterpart* of
        // the TWCS one below -- with the default in play, a default that moved to TWCS would turn
        // this line from "hybrid+STCS streams native" into a contradiction of it.
        e.execute_cql("CREATE TABLE ks.strm_hybrid (pk int PRIMARY KEY, v int) "
                      "WITH storage_format = 'hybrid' "
                      "AND compaction = {'class': 'SizeTieredCompactionStrategy'}").get();
        auto& h = e.local_db().find_column_family("ks", "strm_hybrid");
        BOOST_REQUIRE(h.make_streaming_sstable_for_write()->get_version()
                      != sstables::sstable_version_types::pq);

        // Under TWCS, 'hybrid' means the whole table (design doc 6.4), and the streaming path is
        // one of the three callers that has to agree with the other two or the table never
        // converges. `writes_parquet_unconditionally()` has covered this case since it was
        // introduced and `streaming_version_for()` consults it, but nothing asserted the pairing
        // for TWCS: the streaming test only had a hybrid table under the default strategy, and
        // `test_twcs_hybrid_is_parquet_for_the_whole_table` only asserts the predicate. That is
        // exactly the arrangement that let the sibling defect on the reshape-on-load path live --
        // predicate right, one caller not asking it -- so this asks the caller.
        //
        // One thing this arm cannot see, established by mutating each rule in turn:
        // `streaming_version_for()` is **redundant** as the code stands. When it answers nullopt,
        // `make_streaming_sstable_for_write()` falls back to `make_sstable(state)`, which asks
        // `writes_parquet_unconditionally()` itself (`replica/table.cc:542`, the flush-path rule
        // added later for §10.7). Reverting `streaming_version_for()` to its pre-fix
        // `storage_format() == parquet` form leaves every assertion here passing. So this asserts
        // the *outcome* of the streaming path, which is what matters, but it cannot tell which of
        // the two rules produced it -- and a refactor that deleted either one would still be
        // green. The mutation that does fail this is removing the TWCS arm from
        // `writes_parquet_unconditionally()`, the rule both paths share.
        e.execute_cql("CREATE TABLE ks.strm_hybrid_twcs (pk int, ck timestamp, v int, "
                      "PRIMARY KEY (pk, ck)) WITH storage_format = 'hybrid' "
                      "AND compaction = {'class': 'TimeWindowCompactionStrategy'}").get();
        auto& ht = e.local_db().find_column_family("ks", "strm_hybrid_twcs");
        BOOST_REQUIRE(ht.make_streaming_sstable_for_write()->get_version()
                      == sstables::sstable_version_types::pq);
        BOOST_REQUIRE(ht.make_streaming_staging_sstable()->get_version()
                      == sstables::sstable_version_types::pq);

        // And TWCS on its own is not the trigger -- otherwise the arm above would pass on a build
        // that had stopped reading `storage_format` at all.
        e.execute_cql("CREATE TABLE ks.strm_plain_twcs (pk int, ck timestamp, v int, "
                      "PRIMARY KEY (pk, ck)) WITH compaction = "
                      "{'class': 'TimeWindowCompactionStrategy'}").get();
        auto& pt = e.local_db().find_column_family("ks", "strm_plain_twcs");
        BOOST_REQUIRE(pt.make_streaming_sstable_for_write()->get_version()
                      != sstables::sstable_version_types::pq);

        // Everything above reads the version off the sstable *object* the creator returned. That
        // is the creator's contract, but it is not the file, and a version mislabelled in memory
        // would carry every one of those assertions. So drive one of them all the way: write a
        // partition through the sstable the streaming creator handed back, and read the format off
        // the component that lands on disk, where local storage puts the version in the name.
        {
            auto s = ht.schema();
            auto keys = tests::generate_partition_keys(1, s);
            mutation m(s, keys[0]);
            auto ck = clustering_key::from_single_value(
                    *s, timestamp_type->decompose(data_value(db_clock::now())));
            m.set_clustered_cell(ck, to_bytes("v"), data_value(int32_t(7)), api::new_timestamp());
            make_sstable_containing(ht.make_streaming_sstable_for_write(),
                                    utils::chunked_vector<mutation>{std::move(m)}).get();

            size_t data_components = 0;
            for (const auto& entry : std::filesystem::directory_iterator(tests::table_dir(ht))) {
                auto name = entry.path().filename().string();
                if (!name.ends_with("-Data.db")) {
                    continue;
                }
                ++data_components;
                BOOST_REQUIRE_EQUAL(name.substr(0, 3), "pq-");
            }
            // Nothing else has written to this table, so a count of zero would mean the loop above
            // asserted nothing at all.
            BOOST_REQUIRE_EQUAL(data_components, 1u);
        }
    });
}

// `nodetool refresh` must write the table's format too. This is the other half of the
// reshape-on-load hole, and the wider half.
//
// The boot path -- `table_populator::process_subdir()` -- was fixed to ask
// `sstables::parquet::version_for_rewrite_on_load()`. `distributed_loader::process_upload_dir()`,
// the refresh reshard/reshape path, builds its own creator and was left asking
// `sstables_manager::get_preferred_sstable_version()`, which chooses among the *native* versions
// from config and cluster features and has never heard of `pq`. Every sstable that reshard or
// reshape rewrote while loading `upload/` therefore came out native. Unlike the boot hole, which
// only mis-formatted hybrid + TWCS, this one hit a table with an explicit
// `storage_format = 'parquet'`.
//
// Why §10.12's snapshot -> truncate -> refresh round trip did not catch it, despite looking like
// exactly this scenario: it ran `refresh` in **load-and-stream** mode, which re-streams the
// snapshot's partitions through `table::make_streaming_sstable_for_write()` rather than adopting
// its files, so it exercises the streaming creator (fixed separately) and never reaches this one.
// Its snapshot was also already `pq`, so even a plain adopt-the-files refresh would have reported
// `pq` with nothing having been rewritten. Neither arrangement is sensitive to the version this
// creator picks, which is why it passed with the bug present.
//
// This asserts on the version read back off the files the loader actually produced -- both what
// each loaded sstable reports and the `pq-` prefix local storage puts in the component names.
// Passing a creator *in* and checking the version that came out would assert this test's own
// argument; that is the trap the boot-path fix had to design around by extracting a named
// function. Here `process_upload_dir()` picks the version itself, so driving it end to end is what
// makes the choice observable.
SEASTAR_TEST_CASE(test_storage_format_honoured_by_refresh_reshape) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        // STCS is pinned rather than left to the build's default because the four sstables staged
        // below are chosen against its strict-mode reshape threshold, and because it merges them
        // into a single output instead of ICS's runs, which keeps the count check exact.
        e.execute_cql("CREATE TABLE ks.refresh_pq (pk int PRIMARY KEY, v int) "
                      "WITH storage_format = 'parquet' "
                      "AND compaction = {'class': 'SizeTieredCompactionStrategy'}").get();

        auto& t = e.local_db().find_column_family("ks", "refresh_pq");
        auto s = t.schema();

        // Versions are collected as their *names*, joined, rather than as the raw enum:
        // sstable_version_types has no operator<<, so BOOST_REQUIRE_EQUAL on it does not compile and
        // BOOST_REQUIRE on a set comparison prints neither side. A failure here should say `me != pq`
        // and not merely that a check failed.
        auto loaded_sstables = [&e] {
            std::set<sstables::sstable_version_types> versions;
            size_t count = 0;
            for (auto&& sst : *e.local_db().find_column_family("ks", "refresh_pq").get_sstables()) {
                versions.insert(sst->get_version());
                ++count;
            }
            std::string names;
            for (auto v : versions) {
                if (!names.empty()) {
                    names += ",";
                }
                names += fmt::to_string(v);
            }
            return std::pair(names, count);
        };

        // Nothing has been written yet, so every sstable counted at the end is one the loader put
        // there.
        BOOST_REQUIRE_EQUAL(loaded_sstables().second, 0u);
        BOOST_REQUIRE_EQUAL(loaded_sstables().first, "");

        // Stage four *native* sstables in `upload/`, the way restoring a backup taken before the
        // table was converted would. Four is the number that makes this test bite: STCS reshape in
        // strict mode -- which is what process_upload_dir() asks for -- only returns a job once the
        // input reaches max(min_compaction_threshold, 4). So this is the smallest staging that makes
        // the loader *rewrite* rather than adopt the files unchanged, and a version can only be got
        // wrong on a write.
        constexpr size_t num_sstables = 4;
        auto& sstm = t.get_sstables_manager();
        auto& gen = t.get_sstable_generation_generator();
        // Keys owned by this shard, so nothing needs resharding and the merge asserted below is the
        // reshape's doing rather than a side effect of shard fan-out.
        auto keys = tests::generate_partition_keys(num_sstables, s);
        for (size_t i = 0; i < num_sstables; ++i) {
            mutation m(s, keys[i]);
            m.set_clustered_cell(clustering_key::make_empty(), to_bytes("v"),
                                 data_value(int32_t(i)), api::new_timestamp());
            auto sst = sstm.make_sstable(s, t.get_storage_options(), gen(),
                                         sstables::sstable_state::upload,
                                         sstables::sstable_version_types::me);
            make_sstable_containing(sst, utils::chunked_vector<mutation>{std::move(m)}).get();
        }

        replica::distributed_loader::process_upload_dir(
                e.db(), e.view_builder(), e.view_building_worker(), "ks", "refresh_pq",
                false /* skip_cleanup */, false /* skip_reshape */).get();

        auto [versions, loaded] = loaded_sstables();

        // Guard against a vacuous pass. If reshape had not run, the four staged files would have
        // been adopted exactly as they are, and the version check below would be measuring the
        // staging loop above rather than anything process_upload_dir() decided. These two firing
        // instead of the version check is the signal that this test's *setup* has gone stale --
        // that the reshape threshold moved -- rather than that the product regressed.
        BOOST_REQUIRE_GT(loaded, 0u);
        BOOST_REQUIRE_LT(loaded, num_sstables);

        // The contract. Without the fix this reads `me`.
        BOOST_REQUIRE_EQUAL(versions, fmt::to_string(sstables::sstable_version_types::pq));

        // The same claim read off the directory rather than the sstable objects, so that a version
        // mislabelled in memory could not carry the test: local storage puts the version in the
        // component name.
        size_t data_components = 0;
        for (const auto& entry : std::filesystem::directory_iterator(tests::table_dir(t))) {
            auto name = entry.path().filename().string();
            if (!name.ends_with("-Data.db")) {
                continue;
            }
            ++data_components;
            BOOST_REQUIRE_EQUAL(name.substr(0, 3), "pq-");
        }
        BOOST_REQUIRE_EQUAL(data_components, loaded);

        // And the rows have to survive the rewrite, not merely change format.
        assert_that(e.execute_cql("SELECT pk, v FROM ks.refresh_pq").get())
                .is_rows().with_size(num_sstables);
    });
}

BOOST_AUTO_TEST_SUITE_END()
