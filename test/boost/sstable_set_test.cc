/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */


#include <seastar/testing/test_case.hh>

#include "sstables/sstable_set_impl.hh"
#include "sstables/shared_sstable.hh"
#include "sstables/sstable_set.hh"
#include "sstables/sstables.hh"
#include "test/lib/simple_schema.hh"
#include "test/lib/sstable_utils.hh"
#include "readers/from_mutations_v2.hh"
#include "test/lib/tmpdir.hh"

using namespace sstables;

static sstables::sstable_set make_sstable_set(schema_ptr schema, lw_shared_ptr<sstable_list> all = {}, bool use_level_metadata = true) {
    auto ret = sstables::sstable_set(std::make_unique<partitioned_sstable_set>(schema, use_level_metadata), schema);
    for (auto& sst : *all) {
        ret.insert(sst);
    }
    return ret;
}

SEASTAR_TEST_CASE(test_sstables_sstable_set_read_modify_write) {
    return test_setup::do_with_tmp_directory([] (test_env& env, sstring tmpdir_path) {
        simple_schema ss;
        auto s = ss.schema();
        fs::path tmp(tmpdir_path);

        auto pk = ss.make_pkey(make_local_key(s));
        auto mut = mutation(s, pk);
        ss.add_row(mut, ss.make_ckey(0), "val");
        int gen = 1;

        auto mr = make_flat_mutation_reader_from_mutations_v2(s, env.make_reader_permit(), {mut});
        sstable_writer_config cfg = env.manager().configure_writer("");
        auto sst1 = make_sstable_easy(env, tmp, std::move(mr), cfg, gen++);

        auto ss1 = make_lw_shared<sstables::sstable_set>(make_sstable_set(ss.schema(), make_lw_shared<sstable_list>({sst1})));
        BOOST_REQUIRE_EQUAL(ss1->all()->size(), 1);

        // Test that a random sstable_origin is stored and retrieved properly.
        mr = make_flat_mutation_reader_from_mutations_v2(s, env.make_reader_permit(), {mut});
        auto sst2 = make_sstable_easy(env, tmp, std::move(mr), cfg, gen++);

        auto ss2 = make_lw_shared<sstables::sstable_set>(*ss1);
        ss2->insert(sst2);
        BOOST_REQUIRE_EQUAL(ss2->all()->size(), 2);
        BOOST_REQUIRE_EQUAL(ss1->all()->size(), 1);

        return make_ready_future<>();
    });
}

SEASTAR_TEST_CASE(test_time_series_sstable_set_read_modify_write) {
    return test_env::do_with_async([] (test_env& env) {
        tmpdir tmpdir;
        const auto& tmp = tmpdir.path();
        simple_schema ss;
        auto s = ss.schema();

        auto pk = ss.make_pkey(make_local_key(s));
        auto mut = mutation(s, pk);
        ss.add_row(mut, ss.make_ckey(0), "val");
        int gen = 1;
        sstable_writer_config cfg = env.manager().configure_writer("");

        auto mr = make_flat_mutation_reader_from_mutations_v2(s, env.make_reader_permit(), {mut});
        auto sst1 = make_sstable_easy(env, tmp, std::move(mr), cfg, gen++);

        auto ss1 = make_lw_shared<time_series_sstable_set>(ss.schema());
        ss1->insert(sst1);
        BOOST_REQUIRE_EQUAL(ss1->all()->size(), 1);

        // Test that a random sstable_origin is stored and retrieved properly.
        mr = make_flat_mutation_reader_from_mutations_v2(s, env.make_reader_permit(), {mut});
        auto sst2 = make_sstable_easy(env, tmp, std::move(mr), cfg, gen++);

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
