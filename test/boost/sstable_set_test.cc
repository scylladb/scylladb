/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */


#undef SEASTAR_TESTING_MAIN
#include <seastar/testing/test_case.hh>

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
#include "service/storage_service.hh"

BOOST_AUTO_TEST_SUITE(sstable_set_test)

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
    cfg.db_config->tablets_mode_for_new_keyspaces(db::tablets_mode_t::mode::enabled);

    return do_with_cql_env_thread([&](cql_test_env& env) {
        env.execute_cql("CREATE KEYSPACE test_tablet_sstable_set_copy_ctor"
                " WITH REPLICATION = {'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1};").get();
        env.execute_cql("CREATE TABLE test_tablet_sstable_set_copy_ctor.test (pk int PRIMARY KEY);").get();
        for (int i = 0; i < 10; i++) {
            env.execute_cql(fmt::format("INSERT INTO test_tablet_sstable_set_copy_ctor.test (pk) VALUES ({})", i)).get();
        }
        auto& cf = env.local_db().find_column_family("test_tablet_sstable_set_copy_ctor", "test");
        auto& sgm = column_family_test::get_storage_group_manager(cf);
        sgm->split_all_storage_groups(tasks::task_info{}).get();

        auto tablet_sstable_set = replica::make_tablet_sstable_set(cf.schema(), *sgm.get(), locator::tablet_map(8));
        auto tablet_sstable_set_copy = *tablet_sstable_set.get();
        BOOST_REQUIRE(*tablet_sstable_set->all() == *tablet_sstable_set_copy.all());
        BOOST_REQUIRE_EQUAL(tablet_sstable_set->size(), tablet_sstable_set_copy.size());
        BOOST_REQUIRE_EQUAL(tablet_sstable_set->bytes_on_disk(), tablet_sstable_set_copy.bytes_on_disk());

    }, std::move(cfg));
}

SEASTAR_TEST_CASE(test_sstable_set_fast_forward_by_cache_reader_simulation) {
    return test_env::do_with_async([] (test_env& env) {
        simple_schema ss;
        auto s = ss.schema();

        auto pks = tests::generate_partition_keys(6, s);
        std::vector<mutation> muts;
        for (auto pk : pks) {
            auto mut = mutation(s, pk);
            ss.add_row(mut, ss.make_ckey(0), "val");
            muts.push_back(std::move(mut));
        }

        sstable_writer_config cfg = env.manager().configure_writer("");

        std::vector<sstables::shared_sstable> ssts;

        {
            auto mr = make_mutation_reader_from_mutations_v2(s, env.make_reader_permit(), {muts[0], muts[1], muts[2]});
            auto sst = make_sstable_easy(env, std::move(mr), cfg);
            testlog.info("sstable [{}, {}]", sst->get_first_decorated_key().token(), sst->get_last_decorated_key().token());
            ssts.push_back(std::move(sst));
        }

        {
            auto mr = make_mutation_reader_from_mutations_v2(s, env.make_reader_permit(), {muts[4], muts[5]});
            auto sst = make_sstable_easy(env, std::move(mr), cfg);
            testlog.info("sstable [{}, {}]", sst->get_first_decorated_key().token(), sst->get_last_decorated_key().token());
            ssts.push_back(std::move(sst));
        }
        auto set = make_lw_shared<sstable_set>(std::make_unique<partitioned_sstable_set>(ss.schema(), false));
        for (auto& sst : ssts) {
            set->insert(sst);
        }

        // simulation of full scan on range [0, 5]
        // cache reader fetches [0, 1] -> next [4]
        // [2] consumed from cache
        // fast forward to [3, 5]

        auto first_range = dht::partition_range::make({pks[0]}, {pks[1]});
        auto reader = set->make_range_sstable_reader(s, env.make_reader_permit(),
                                                     first_range,
                                                     s->full_slice(),
                                                     nullptr,
                                                     ::streamed_mutation::forwarding::no,
                                                     ::mutation_reader::forwarding::yes);
        auto close_r = deferred_close(reader);

        auto mopt = read_mutation_from_mutation_reader(reader).get();
        BOOST_REQUIRE(mopt && mopt->decorated_key().equal(*s, pks[0]));

        mopt = read_mutation_from_mutation_reader(reader).get();
        BOOST_REQUIRE(mopt && mopt->decorated_key().equal(*s, pks[1]));

        auto second_range = dht::partition_range::make({pks[3]}, {pks[5]});

        reader.fast_forward_to(second_range).get();

        mopt = read_mutation_from_mutation_reader(reader).get();
        BOOST_REQUIRE(mopt && mopt->decorated_key().equal(*s, pks[4]));

        mopt = read_mutation_from_mutation_reader(reader).get();
        BOOST_REQUIRE(mopt && mopt->decorated_key().equal(*s, pks[5]));
        // EOS
        BOOST_REQUIRE(!read_mutation_from_mutation_reader(reader).get());
    });
}

static future<> guarantee_all_tablet_replicas_on_shard0(cql_test_env& env) {
    auto& ss = env.get_storage_service().local();
    auto& stm = env.get_shared_token_metadata().local();
    auto my_host_id = ss.get_token_metadata_ptr()->get_topology().my_host_id();

    co_await ss.set_tablet_balancing_enabled(false);
    co_await stm.mutate_token_metadata([&] (locator::token_metadata& tm) -> future<> {
        tm.update_topology(my_host_id, locator::endpoint_dc_rack::default_location, locator::node::state::normal, 1);
        return make_ready_future<>();
    });
}

SEASTAR_TEST_CASE(test_tablet_sstable_set_fast_forward_across_tablet_ranges) {
    // enable tablets, to get access to tablet_storage_group_manager
    cql_test_config cfg;
    cfg.db_config->tablets_mode_for_new_keyspaces(db::tablets_mode_t::mode::enabled);

    return do_with_cql_env_thread([&](cql_test_env& env) {
        guarantee_all_tablet_replicas_on_shard0(env).get();

        env.execute_cql("CREATE KEYSPACE test_tablet_sstable_set"
                        " WITH REPLICATION = {'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1} AND TABLETS = {'enabled': true, 'initial': 2};").get();
        env.execute_cql("CREATE TABLE test_tablet_sstable_set.test (pk int PRIMARY KEY)").get();

        auto& table = env.local_db().find_column_family("test_tablet_sstable_set", "test");
        auto s = table.schema();
        auto& sgm = column_family_test::get_storage_group_manager(table);
        auto erm = table.get_effective_replication_map();
        auto& tmap = erm->get_token_metadata().tablets().get_tablet_map(s->id());

        std::unordered_map<locator::tablet_id, std::vector<dht::decorated_key>> keys_per_tablet;

        table.disable_auto_compaction().get();
        for (int i = 0; i < 10; i++) {
            env.execute_cql(fmt::format("INSERT INTO test_tablet_sstable_set.test (pk) VALUES ({})", i)).get();
            auto key = dht::decorate_key(*s, partition_key::from_singular(*s, i));
            keys_per_tablet[tmap.get_tablet_id(key.token())].push_back(key);
            // produces single-partition sstables, to stress incremental selector.
            table.flush().get();
        }

        for (auto& [_, keys] : keys_per_tablet) {
            auto cmp = dht::decorated_key::less_comparator(s);
            std::ranges::sort(keys, cmp);
        }

        auto set = replica::make_tablet_sstable_set(s, *sgm.get(), tmap);

        utils::get_local_injector().enable("enable_read_debug_log");
        testlog.info("first tablet range: {}", tmap.get_token_range(locator::tablet_id(0)));
        testlog.info("second tablet range: {}", tmap.get_token_range(locator::tablet_id(1)));

        auto& keys_for_first_tablet = keys_per_tablet.at(locator::tablet_id(0));
        auto& keys_for_second_tablet = keys_per_tablet.at(locator::tablet_id(1));

        auto create_reader = [&] (const dht::partition_range& range) {
            return set->make_range_sstable_reader(s, make_reader_permit(env),
                                           range,
                                           s->full_slice(),
                                           nullptr,
                                           ::streamed_mutation::forwarding::no,
                                           ::mutation_reader::forwarding::yes);
        };

        auto read_and_check = [&] (auto& reader, const dht::decorated_key& expected) {
            auto mopt = read_mutation_from_mutation_reader(reader).get();
            BOOST_REQUIRE(mopt && mopt->decorated_key().equal(*s, expected));
        };
        auto end_of_stream_check = [&] (auto& reader) {
            BOOST_REQUIRE(!read_mutation_from_mutation_reader(reader).get());
        };

        // simulation of full scan on tablet ranges
        // cache reader fetches range of first tablet
        // fast forward to range of second tablet
        {
            auto first_range = dht::partition_range::make({keys_for_first_tablet.front()}, {keys_for_first_tablet.back()});
            auto reader = create_reader(first_range);
            auto close_r = deferred_close(reader);

            for (auto& k : keys_for_first_tablet) {
                read_and_check(reader, k);
            }

            auto second_range = dht::partition_range::make({keys_for_second_tablet.front()}, {keys_for_second_tablet.back()});
            reader.fast_forward_to(second_range).get();

            for (auto& k: keys_for_second_tablet) {
                read_and_check(reader, k);
            }
            end_of_stream_check(reader);
        }

        // verify that fast forward will be able to create reader when the new range goes across tablet boundaries.
        {
            auto first_range = dht::partition_range::make({keys_for_first_tablet[0]}, {keys_for_first_tablet[0]});
            auto reader = create_reader(first_range);
            auto close_r = deferred_close(reader);

            for (auto& k : std::span{keys_for_first_tablet.begin(), 1}) {
                read_and_check(reader, k);
            }

            auto second_range = dht::partition_range::make({keys_for_first_tablet[1]}, {keys_for_second_tablet.back()});
            reader.fast_forward_to(second_range).get();

            for (auto& k : std::span{keys_for_first_tablet.begin() + 1, keys_for_first_tablet.size() - 1}) {
                read_and_check(reader, k);
            }
            for (auto& k: keys_for_second_tablet) {
                read_and_check(reader, k);
            }
            end_of_stream_check(reader);
        }

        // Reproduces a scenario of range scan where fast forward will overlap with next position returned by selector
        // full scan: [0, 20]
        // 1) cache reader emits [0, 10) (position 10 is cached)
        // 2) incremental selector returns 0 sstables, next position of 16 (the start of a sstable)
        // 3) fast forward to range [14, 20]
        // fast forward might expect new range to be after next position (16), but [14, 20] is before and overlaps with next position.
        // the incremental selector must be called also when new range overlaps with next position. otherwise, there's chance of
        // missing data.
        {
            auto first_token = tmap.get_first_token(locator::tablet_id(0));
            auto first_range = dht::partition_range::make({dht::ring_position::starting_at(first_token)},
                                                          {dht::ring_position::ending_at(first_token)});
            auto reader = create_reader(first_range);
            auto close_r = deferred_close(reader);

            end_of_stream_check(reader);

            auto& keys_for_second_tablet = keys_per_tablet.at(locator::tablet_id(1));
            auto second_range = dht::partition_range::make({dht::ring_position::starting_at(dht::next_token(first_token))},
                                                           {keys_for_second_tablet.back()});

            reader.fast_forward_to(second_range).get();

            for (auto& k : keys_for_first_tablet) {
                read_and_check(reader, k);
            }
            for (auto& k: keys_for_second_tablet) {
                read_and_check(reader, k);
            }
            end_of_stream_check(reader);
        }

    }, std::move(cfg));
}

BOOST_AUTO_TEST_SUITE_END()
