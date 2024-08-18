/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */


#include "test/lib/scylla_test_case.hh"

#include <fmt/ranges.h>
#include "db/config.hh"
#include "locator/tablets.hh"
#include "sstables/sstable_set_impl.hh"
#include "sstables/shared_sstable.hh"
#include "sstables/sstable_set.hh"
#include "sstables/sstables.hh"
#include "sstable_test.hh"
#include "test/lib/cql_test_env.hh"
#include "test/lib/simple_schema.hh"
#include "test/lib/sstable_utils.hh"
#include "readers/from_mutations_v2.hh"

using namespace sstables;

static sstables::sstable_set make_sstable_set(schema_ptr schema, lw_shared_ptr<sstable_list> all = {}, bool use_level_metadata = true) {
    auto ret = sstables::sstable_set(std::make_unique<partitioned_sstable_set>(schema, use_level_metadata));
    for (auto& sst : *all) {
        ret.insert(sst);
    }
    return ret;
}

SEASTAR_TEST_CASE(test_sstables_sstable_set_read_modify_write) {
    return test_env::do_with_async([] (test_env& env) {
        simple_schema ss;
        auto s = ss.schema();

        auto pk = tests::generate_partition_key(s);
        auto mut = mutation(s, pk);
        ss.add_row(mut, ss.make_ckey(0), "val");

        auto mr = make_mutation_reader_from_mutations_v2(s, env.make_reader_permit(), mut);
        sstable_writer_config cfg = env.manager().configure_writer("");
        auto sst1 = make_sstable_easy(env, std::move(mr), cfg);

        auto ss1 = make_lw_shared<sstables::sstable_set>(make_sstable_set(ss.schema(), make_lw_shared<sstable_list>({sst1})));
        BOOST_REQUIRE_EQUAL(ss1->all()->size(), 1);

        // Test that a random sstable_origin is stored and retrieved properly.
        mr = make_mutation_reader_from_mutations_v2(s, env.make_reader_permit(), mut);
        auto sst2 = make_sstable_easy(env, std::move(mr), cfg);

        auto ss2 = make_lw_shared<sstables::sstable_set>(*ss1);
        ss2->insert(sst2);
        BOOST_REQUIRE_EQUAL(ss2->all()->size(), 2);
        BOOST_REQUIRE_EQUAL(ss1->all()->size(), 1);
    });
}

SEASTAR_TEST_CASE(test_time_series_sstable_set_read_modify_write) {
    return test_env::do_with_async([] (test_env& env) {
        simple_schema ss;
        auto s = ss.schema();

        auto pk = tests::generate_partition_key(s);
        auto mut = mutation(s, pk);
        ss.add_row(mut, ss.make_ckey(0), "val");
        sstable_writer_config cfg = env.manager().configure_writer("");

        auto mr = make_mutation_reader_from_mutations_v2(s, env.make_reader_permit(), mut);
        auto sst1 = make_sstable_easy(env, std::move(mr), cfg);

        auto ss1 = make_lw_shared<time_series_sstable_set>(ss.schema(), true);
        ss1->insert(sst1);
        BOOST_REQUIRE_EQUAL(ss1->all()->size(), 1);

        // Test that a random sstable_origin is stored and retrieved properly.
        mr = make_mutation_reader_from_mutations_v2(s, env.make_reader_permit(), mut);
        auto sst2 = make_sstable_easy(env, std::move(mr), cfg);

        auto ss2 = make_lw_shared<time_series_sstable_set>(*ss1);
        ss2->insert(sst2);
        BOOST_REQUIRE_EQUAL(ss2->all()->size(), 2);
        BOOST_REQUIRE_EQUAL(ss1->all()->size(), 1);

        std::set<sstables::shared_sstable> in_set;
        ss2->for_each_sstable_gently_until([&] (sstables::shared_sstable sst) {
            in_set.insert(sst);
            return make_ready_future<stop_iteration>(false);
        }).get();
        BOOST_REQUIRE(in_set == std::set<sstables::shared_sstable>({sst1, sst2}));

        auto lookup_sst = [&] (sstables::shared_sstable sst) {
            bool found = false;
            ss2->for_each_sstable_gently_until([&] (sstables::shared_sstable cur) {
                found = (cur == sst);
                return make_ready_future<stop_iteration>(found);
            }).get();
            return found;
        };
        BOOST_REQUIRE(lookup_sst(sst1));
        BOOST_REQUIRE(lookup_sst(sst2));
    });
}

SEASTAR_TEST_CASE(test_time_series_sstable_set_bytes_on_disk) {
    return test_env::do_with_async([] (test_env& env) {
        simple_schema ss;
        auto s = ss.schema();

        auto pk = tests::generate_partition_key(s);
        auto mut = mutation(s, pk);
        ss.add_row(mut, ss.make_ckey(0), "val");
        sstable_writer_config cfg = env.manager().configure_writer("");

        auto mr = make_mutation_reader_from_mutations_v2(s, env.make_reader_permit(), mut);
        auto sst1 = make_sstable_easy(env, std::move(mr), cfg);
        auto size1 = sst1->bytes_on_disk();

        auto ss1 = make_lw_shared<sstable_set>(std::make_unique<time_series_sstable_set>(ss.schema(), true));
        ss1->insert(sst1);
        BOOST_REQUIRE_EQUAL(ss1->bytes_on_disk(), size1);

        // Test that a random sstable_origin is stored and retrieved properly.
        mr = make_mutation_reader_from_mutations_v2(s, env.make_reader_permit(), mut);
        auto sst2 = make_sstable_easy(env, std::move(mr), cfg);
        auto size2 = sst2->bytes_on_disk();

        auto ss2 = make_lw_shared<sstable_set>(*ss1);
        BOOST_REQUIRE_EQUAL(ss2->bytes_on_disk(), ss1->bytes_on_disk());
        ss2->insert(sst2);
        BOOST_REQUIRE_EQUAL(ss2->bytes_on_disk(), size1 + size2);

        std::vector<lw_shared_ptr<sstable_set>> sets = {ss1, ss2};
        auto sst_set = make_lw_shared<sstable_set>(std::make_unique<compound_sstable_set>(s, std::move(sets)));
        BOOST_REQUIRE_EQUAL(sst_set->bytes_on_disk(), ss1->bytes_on_disk() + ss2->bytes_on_disk());
    });
}

SEASTAR_TEST_CASE(test_partitioned_sstable_set_bytes_on_disk) {
    return test_env::do_with_async([] (test_env& env) {
        simple_schema ss;
        auto s = ss.schema();

        auto pk = tests::generate_partition_key(s);
        auto mut = mutation(s, pk);
        ss.add_row(mut, ss.make_ckey(0), "val");
        sstable_writer_config cfg = env.manager().configure_writer("");

        auto mr = make_mutation_reader_from_mutations_v2(s, env.make_reader_permit(), mut);
        auto sst1 = make_sstable_easy(env, std::move(mr), cfg);
        auto size1 = sst1->bytes_on_disk();

        auto ss1 = make_lw_shared<sstable_set>(std::make_unique<partitioned_sstable_set>(ss.schema(), true));
        ss1->insert(sst1);
        BOOST_REQUIRE_EQUAL(ss1->bytes_on_disk(), size1);

        // Test that a random sstable_origin is stored and retrieved properly.
        mr = make_mutation_reader_from_mutations_v2(s, env.make_reader_permit(), mut);
        auto sst2 = make_sstable_easy(env, std::move(mr), cfg);
        auto size2 = sst2->bytes_on_disk();

        auto ss2 = make_lw_shared<sstable_set>(*ss1);
        BOOST_REQUIRE_EQUAL(ss2->bytes_on_disk(), ss1->bytes_on_disk());
        ss2->insert(sst2);
        BOOST_REQUIRE_EQUAL(ss2->bytes_on_disk(), size1 + size2);

        std::vector<lw_shared_ptr<sstable_set>> sets = {ss1, ss2};
        auto sst_set = make_lw_shared<sstable_set>(std::make_unique<compound_sstable_set>(s, std::move(sets)));
        BOOST_REQUIRE_EQUAL(sst_set->bytes_on_disk(), ss1->bytes_on_disk() + ss2->bytes_on_disk());
    });
}

SEASTAR_TEST_CASE(test_tablet_sstable_set_copy_ctor) {
    // enable tablets, to get access to tablet_storage_group_manager
    cql_test_config cfg;
    cfg.db_config->enable_tablets(true);

    return do_with_cql_env_thread([&](cql_test_env& env) {
        env.execute_cql("CREATE KEYSPACE test_tablet_sstable_set_copy_ctor"
                " WITH REPLICATION = {'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1};").get();
        env.execute_cql("CREATE TABLE test_tablet_sstable_set_copy_ctor.test (pk int PRIMARY KEY);").get();
        for (int i = 0; i < 10; i++) {
            env.execute_cql(fmt::format("INSERT INTO test_tablet_sstable_set_copy_ctor.test (pk) VALUES ({})", i)).get();
        }
        auto& cf = env.local_db().find_column_family("test_tablet_sstable_set_copy_ctor", "test");
        auto& sgm = column_family_test::get_storage_group_manager(cf);
        sgm->split_all_storage_groups().get();

        auto tablet_sstable_set = replica::make_tablet_sstable_set(cf.schema(), *sgm.get(), locator::tablet_map(8));
        auto tablet_sstable_set_copy = *tablet_sstable_set.get();
        BOOST_REQUIRE(*tablet_sstable_set->all() == *tablet_sstable_set_copy.all());
        BOOST_REQUIRE_EQUAL(tablet_sstable_set->size(), tablet_sstable_set_copy.size());
        BOOST_REQUIRE_EQUAL(tablet_sstable_set->bytes_on_disk(), tablet_sstable_set_copy.bytes_on_disk());

    }, std::move(cfg));
}
