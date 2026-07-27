/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */



#include "test/lib/cql_test_env.hh"
#include "utils/assert.hh"
#include <seastar/core/sstring.hh>
#include <fmt/ranges.h>
#include <fmt/format.h>

#include <seastar/core/future.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/testing/test_fixture.hh>

#include "db/config.hh"
#include "db/consistency_level_type.hh"
#include "db/system_distributed_keyspace.hh"
#include "sstables/object_storage_client.hh"
#include "sstables/storage.hh"
#include "sstables_loader.hh"
#include "replica/database_fwd.hh"
#include "replica/tablets.hh"
#include "replica/schema_describe_helper.hh"
#include "tasks/task_handler.hh"
#include "db/system_distributed_keyspace.hh"
#include "service/storage_proxy.hh"
#include "replica/schema_describe_helper.hh"
#include "utils/UUID_gen.hh"

#include "test/lib/test_utils.hh"
#include "test/lib/random_utils.hh"
#include "test/lib/sstable_test_env.hh"
#include "test/boost/database_test.hh"

using namespace std::string_literals;

template <>
struct fmt::formatter<db::snapshot_state> : fmt::formatter<string_view> {
    template <typename FormatContext>
    auto format(const db::snapshot_state& e, FormatContext& ctx) const {
        switch (e) {
        case db::snapshot_state::unknown: return formatter<string_view>::format("unknown", ctx);
        case db::snapshot_state::local: return formatter<string_view>::format("local", ctx);
        case db::snapshot_state::being_backed_up: return formatter<string_view>::format("being_backed_up", ctx);
        case db::snapshot_state::remote_and_local: return formatter<string_view>::format("remote_and_local", ctx);
        case db::snapshot_state::remote: return formatter<string_view>::format("remote", ctx);
        default: throw std::invalid_argument(fmt::format("Unhandled snapshot_state {}", int(e)));
        }
    }
};

template <>
struct fmt::formatter<db::snapshot_table_type> : fmt::formatter<string_view> {
    template <typename FormatContext>
    auto format(const db::snapshot_table_type& e, FormatContext& ctx) const {
        switch (e) {
        case db::snapshot_table_type::cql_table: return formatter<string_view>::format("cql_table", ctx);
        case db::snapshot_table_type::cql_view: return formatter<string_view>::format("cql_view", ctx);
        case db::snapshot_table_type::alternator_table: return formatter<string_view>::format("alternator_table", ctx);
        default: throw std::invalid_argument(fmt::format("Unhandled snapshot_table_type {}", int(e)));
        }
    }
};

template <>
struct fmt::formatter<db::snapshot_entry> : fmt::formatter<string_view> {
    template <typename FormatContext>
    auto format(const db::snapshot_entry& e, FormatContext& ctx) const {
        return fmt::format_to(ctx.out(), 
            "{{ {}, {}, {}, {}, {} }}",
            e.name, e.created_at, e.expires_at, 
            e.namespace_version, e.manifest_version
        );
    }
};

template <>
struct fmt::formatter<db::snapshot_remote_location_entry> : fmt::formatter<string_view> {
    template <typename FormatContext>
    auto format(const db::snapshot_remote_location_entry& e, FormatContext& ctx) const {
        return fmt::format_to(ctx.out(), 
            "{{ {}, {}, {}, {}, {}, {} }}",
            e.snapshot_name, e.datacenter, e.endpoint, e.bucket, e.prefix, e.state
        );
    }
};

template <>
struct fmt::formatter<db::snapshot_keyspace_entry> : fmt::formatter<string_view> {
    template <typename FormatContext>
    auto format(const db::snapshot_keyspace_entry& e, FormatContext& ctx) const {
        return fmt::format_to(ctx.out(), 
            "{{ {}, {}, {} }}",
            e.snapshot_name, e.keyspace_name, e.keyspace_schema
        );
    }
};

template <>
struct fmt::formatter<db::snapshot_table_entry> : fmt::formatter<string_view> {
    template <typename FormatContext>
    auto format(const db::snapshot_table_entry& e, FormatContext& ctx) const {
        return fmt::format_to(ctx.out(), 
            "{{ {}, {}, {}, {}, {}, {}, {} }}",
            e.snapshot_name, e.keyspace_name, e.table_name, e.table_id,
            e.type, e.base_table_id, e.table_schema
        );
    }
};

template <>
struct fmt::formatter<db::snapshot_node_entry> : fmt::formatter<string_view> {
    template <typename FormatContext>
    auto format(const db::snapshot_node_entry& e, FormatContext& ctx) const {
        return fmt::format_to(ctx.out(), 
            "{{ {}, {}, {} }}",
            e.datacenter, e.rack, e.node
        );
    }
};

SEASTAR_TEST_CASE(test_snapshot_manifests_table_api_works) {
    auto db_cfg_ptr = make_shared<db::config>();

    return do_with_cql_env([] (cql_test_env& env) -> future<> {
        auto snapshot_name = "snapshot"s;
        auto ks = "ks"s;
        auto table = "cf"s;
        auto dc = "dc1"s;
        auto rack = "r1"s;
        auto sstable_id = utils::UUID_gen::get_time_UUID();
        auto host_id = locator::host_id::create_random_id();
        auto last_token = dht::token::from_int64(100);
        auto toc_name = "me-3gdq_0bki_2cvk01yl83nj0tp5gh-big-TOC.txt"s;
        auto prefix = "some/prefix"s;
        auto num_iter = 5;

        db::snapshot_table_helper sdk(env.qp().local());
        constexpr auto cl = db::consistency_level::ONE;

        for (int i = num_iter - 1; i >= 0; --i) {
            // insert some test data into snapshot_sstables table
            auto first_token = dht::token::from_int64(i);
            co_await sdk.insert_snapshot_sstable(snapshot_name, ks, table, dc, rack
                , sstables::sstable_id(sstable_id), first_token, last_token, toc_name, prefix
                , host_id, i, db::snapshot_state::local, 0, 0, 0
                , cl);
        }
        // read it back and check if it is correct
        auto sstables = co_await sdk.get_snapshot_sstables(snapshot_name, ks, table, dc, rack, cl);

        BOOST_CHECK_EQUAL(sstables.size(), num_iter);

        for (int i = 0; i < num_iter; ++i) {
            const auto& sstable = sstables[i];
            BOOST_CHECK_EQUAL(sstable.toc_name, toc_name);
            BOOST_CHECK_EQUAL(sstable.prefix, prefix);
            BOOST_CHECK_EQUAL(sstable.first_token, dht::token::from_int64(i));
            BOOST_CHECK_EQUAL(sstable.last_token, last_token);
            BOOST_CHECK_EQUAL(sstable.sstable_id.uuid(), sstable_id);
        }

        // test token range filtering: matching range should return the sstable
        auto filtered = co_await sdk.get_snapshot_sstables(
            snapshot_name, ks, table, dc, rack, db::consistency_level::ONE,
            dht::token::from_int64(-10), dht::token::from_int64(num_iter + 1));
        BOOST_CHECK_EQUAL(filtered.size(), num_iter);

        // test token range filtering: the interval is inclusive, if start and end are equal to first_token, it should return one sstable 
        filtered = co_await sdk.get_snapshot_sstables(
            snapshot_name, ks, table, dc, rack, db::consistency_level::ONE,
            dht::token::from_int64(0), dht::token::from_int64(0));
        BOOST_CHECK_EQUAL(filtered.size(), 1);

        // test token range filtering: non-matching range should return nothing
        auto empty = co_await sdk.get_snapshot_sstables(
            snapshot_name, ks, table, dc, rack, db::consistency_level::ONE,
            dht::token::from_int64(num_iter + 10), dht::token::from_int64(num_iter + 20));
        BOOST_CHECK_EQUAL(empty.size(), 0);

        db::snapshot_entry sse {
            .name = snapshot_name,
            .created_at = db_clock::now(),
            .expires_at = db_clock::now() + 10000s,
            .namespace_version = "Moose and Bear",
            .manifest_version = "Wingdings",
        };

        db::snapshot_remote_location_entry rle {
            .snapshot_name = snapshot_name,
            .datacenter = dc,
            .endpoint = "The end of all things",
            .bucket = "A_bucket_for_fun",
            .prefix = prefix,
            .state = db::snapshot_state::local,
        };

        co_await sdk.insert_snapshot(sse, cl);

        auto q_sse = co_await sdk.get_snapshot(snapshot_name, cl);

        BOOST_CHECK(bool(q_sse));
        BOOST_CHECK_EQUAL(*q_sse, sse);

        co_await sdk.insert_snapshot_remote_locations(std::span<const db::snapshot_remote_location_entry>({ rle }), cl);

        auto q_rle = co_await sdk.get_snapshot_remote_locations(snapshot_name, cl);

        BOOST_CHECK_EQUAL(q_rle.size(), 1);
        BOOST_CHECK_EQUAL(q_rle[0], rle);

        auto keyspaces = std::views::iota(1, 4) | std::views::transform([&](int i) {
            auto name = fmt::format("{}_{}", ks, i);
            return db::snapshot_keyspace_entry {
                .snapshot_name = snapshot_name,
                .keyspace_name = name,
                .keyspace_schema = "CREATE KEYSPACE " + name,
            };
        }) | std::ranges::to<std::vector>();

        co_await sdk.insert_snapshot_keyspaces(keyspaces, cl);
        auto q_keyspaces = co_await sdk.get_snapshot_keyspaces(snapshot_name, cl);

        std::ranges::sort(q_keyspaces, {}, &db::snapshot_keyspace_entry::keyspace_name);

        BOOST_CHECK(std::ranges::equal(keyspaces, q_keyspaces));

        auto ks_1 = ks + "_1";
        auto tables = std::views::iota(1, 10) | std::views::transform([&](int i) {
            auto name = fmt::format("{}_{}", table, i);
            return db::snapshot_table_entry {
                .snapshot_name = snapshot_name,
                .keyspace_name = (i & 1) ? ks : ks_1,
                .table_name = name,
                .table_id = ::table_id::create_random_id(),
                .type = db::snapshot_table_type::cql_table,
            };
        }) | std::ranges::to<std::vector>();

        co_await sdk.insert_snapshot_tables(tables, cl);
        auto q_tables1 = co_await sdk.get_snapshot_tables(snapshot_name, {}, {}, cl);

        std::ranges::sort(q_tables1, {}, &db::snapshot_table_entry::table_name);

        BOOST_CHECK(std::ranges::equal(tables, q_tables1));

        for (auto k : { ks, ks_1 }) {
            auto q_tables2 = co_await sdk.get_snapshot_tables(snapshot_name, k, {}, cl);
            auto filtered = tables | std::views::filter([&](auto& e) {
                return e.keyspace_name == k;
            });

            std::ranges::sort(q_tables2, {}, &db::snapshot_table_entry::table_name);

            BOOST_CHECK(std::ranges::equal(filtered, q_tables2));
        }

        auto tablets = std::views::iota(1u, 100u) | std::views::transform([&](unsigned i) {
            return db::snapshot_tablet_entry {
                .tablet_id = i,
                .first_token = dht::token::from_int64(i * 1000),
                .last_token = dht::token::from_int64(i * 1000 + 999),
                .repair_time = db_clock::now(),
                .repaired_at = i
            };
        }) | std::ranges::to<std::vector>();

        co_await sdk.insert_snapshot_tablets(snapshot_name, ks, table, dc, tablets, cl);
        auto q_tablets = co_await sdk.get_snapshot_tablets(snapshot_name, ks, table, dc, cl);

        std::ranges::sort(q_tablets, {}, &db::snapshot_tablet_entry::tablet_id);
        BOOST_CHECK(std::ranges::equal(tablets, q_tablets));

auto nodes = std::views::iota(1u, 20u) | std::views::transform([&](unsigned i) {
            return db::snapshot_node_entry {
                .datacenter = dc,
                .rack = rack,
                .node = locator::host_id::create_random_id()
            };
        }) | std::ranges::to<std::vector>();

        co_await sdk.insert_snapshot_nodes(snapshot_name, nodes, cl);
        auto q_nodes = co_await sdk.get_snapshot_nodes(snapshot_name, dc, rack, cl);

        BOOST_TEST_MESSAGE(fmt::format("Baba {} | {}", nodes, q_nodes));
        std::ranges::sort(nodes, {}, &db::snapshot_node_entry::node);
        std::ranges::sort(q_nodes, {}, &db::snapshot_node_entry::node);

        BOOST_CHECK(std::ranges::equal(nodes, q_nodes));

    }, db_cfg_ptr);
}

using namespace sstables;

future<sstring> backup(cql_test_env& env, sstring endpoint, sstring bucket) {
    sharded<db::snapshot_ctl> ctl;
    co_await ctl.start(std::ref(env.db()), std::ref(env.get_storage_proxy()), std::ref(env.get_task_manager()), std::ref(env.get_sstorage_manager()), db::snapshot_ctl::config{});
    auto prefix = fmt::format("/backup-{}", utils::UUID_gen::get_time_UUID());

    auto task_id = co_await ctl.local().start_backup(endpoint, bucket, prefix, "ks", "cf", "snapshot", false);
    auto task = tasks::task_handler{env.get_task_manager().local(), task_id};
    auto status = co_await task.wait_for_task(30s);
    BOOST_REQUIRE(status.state == tasks::task_manager::task_state::done);

    co_await ctl.stop();
    co_return prefix;
}

future<> check_snapshot_sstables(cql_test_env& env) {
    auto& topology = env.get_storage_proxy().local().get_token_metadata_ptr()->get_topology();
    auto dc = topology.get_datacenter();
    auto rack = topology.get_rack();

    db::snapshot_table_helper sth(env.local_qp());
    auto sstables = co_await sth.get_snapshot_sstables("snapshot", "ks", "cf", dc, rack, db::consistency_level::ONE);

    // Check that the sstables in system_distributed.snapshot_sstables match the sstables in the snapshot directory on disk
    auto& cf = env.local_db().find_column_family("ks", "cf");
    auto tabledir = tests::table_dir(cf);
    auto snapshot_dir = tabledir / sstables::snapshots_dir / "snapshot";
    std::set<sstring> expected_sstables;
    directory_lister lister(snapshot_dir, lister::dir_entry_types::of<directory_entry_type::regular>());
    while (auto de = co_await lister.get()) {
        if (!de->name.ends_with("-TOC.txt")) {
            continue;
        }
        expected_sstables.insert(de->name);
    }

    BOOST_CHECK_EQUAL(sstables.size(), expected_sstables.size());

    for (const auto& sstable : sstables) {
        BOOST_CHECK(expected_sstables.contains(sstable.toc_name));
    }
}

SEASTAR_TEST_CASE(test_populate_snapshot_sstables_from_manifests, *boost::unit_test::precondition(tests::has_scylla_test_env)) {
    using namespace sstables;

    auto db_cfg_ptr = make_shared<db::config>();
    db_cfg_ptr->tablets_mode_for_new_keyspaces(db::tablets_mode_t::mode::enabled);
    auto storage_options = make_test_object_storage_options("S3");
    db_cfg_ptr->object_storage_endpoints(make_storage_options_config(storage_options));

    return do_with_some_data_in_thread({"cf"}, [storage_options = std::move(storage_options)] (cql_test_env& env) {
            take_snapshot(env, "ks", "cf", "snapshot").get();

            auto ep = storage_options.to_map()["endpoint"];
            auto bucket = storage_options.to_map()["bucket"];
            auto prefix = backup(env, ep, bucket).get();
            auto manifest_path = prefix + "/manifest.json";

            BOOST_REQUIRE_THROW(populate_snapshot_sstables_from_manifests(env.get_sstorage_manager().local(), env.get_system_distributed_keyspace().local(), "ks", "cf", ep, bucket, "unexpected_snapshot", {manifest_path}, db::consistency_level::ONE).get(), std::runtime_error);;

            // populate system_distributed.snapshot_sstables with the content of the snapshot manifest
            populate_snapshot_sstables_from_manifests(env.get_sstorage_manager().local(), env.get_system_distributed_keyspace().local(), "ks", "cf", ep, bucket, "snapshot", {manifest_path}, db::consistency_level::ONE).get();

            check_snapshot_sstables(env).get();
    }, false, db_cfg_ptr, 10);
}

void verify_tablet_options(cql_test_env& env, table_id tid, size_t desired_count, bool hints_expected) {
    auto schema = env.local_db().find_schema(tid);
    auto schema_desc = schema->describe(replica::make_schema_describe_helper(schema, env.local_db().as_data_dictionary()), cql3::describe_option::STMTS);
    auto create_stmt = schema_desc.create_statement.value().linearize();

    auto min_tablet_count = fmt::format("'min_tablet_count': '{}'", desired_count);
    auto max_tablet_count = fmt::format("'max_tablet_count': '{}'", desired_count);

    BOOST_CHECK_EQUAL(create_stmt.find(min_tablet_count) != sstring::npos, hints_expected);
    BOOST_CHECK_EQUAL(create_stmt.find(max_tablet_count) != sstring::npos, hints_expected);
}

SEASTAR_TEST_CASE(test_restore_alter_table_with_tablet_hints, *boost::unit_test::precondition(tests::has_scylla_test_env)) {
    auto db_cfg_ptr = make_shared<db::config>();
    db_cfg_ptr->tablets_mode_for_new_keyspaces(db::tablets_mode_t::mode::enabled);

    return do_with_some_data_in_thread({"cf"}, [] (cql_test_env& env) {
        table_id tid = env.local_db().find_uuid("ks", "cf");

        auto token_metadata = env.local_db().get_token_metadata_ptr();
        auto& tmap = token_metadata->tablets().get_tablet_map(tid);
        auto desired_count = tmap.tablet_count();

        verify_tablet_options(env, tid, desired_count, false);

        auto schema = env.local_db().find_schema(tid);

        env.get_storage_service().local().alter_table_with_tablet_hints(tid, desired_count, desired_count).get();

        verify_tablet_options(env, tid, desired_count, true);
    }, false, db_cfg_ptr, 10);
}
