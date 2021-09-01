/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */


#include <seastar/testing/test_case.hh>

#include "sstables/sstable_set_impl.hh"
#include "sstables/sstable_set.hh"
#include "sstables/sstables.hh"
#include "test/lib/simple_schema.hh"

static sstables::sstable_set make_sstable_set(schema_ptr schema, lw_shared_ptr<sstable_list> all = {}, bool use_level_metadata = true) {
    return sstables::sstable_set(std::make_unique<partitioned_sstable_set>(schema, std::move(all), use_level_metadata), schema);
}

static shared_sstable make_sstable_easy(test_env& env, const fs::path& dir, flat_mutation_reader mr, sstable_writer_config cfg, unsigned long gen) {
    schema_ptr s = mr.schema();
    auto sst = env.make_sstable(s, dir.string(), gen);
    sst->write_components(std::move(mr), 0, s, cfg, encoding_stats{}).get();
    sst->load().get();
    return sst;
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

        auto mr = flat_mutation_reader_from_mutations(env.make_reader_permit(), {mut});
        sstable_writer_config cfg = env.manager().configure_writer("");
        auto sst1 = make_sstable_easy(env, tmp, std::move(mr), cfg, gen++);

        auto ss1 = make_lw_shared<sstables::sstable_set>(make_sstable_set(ss.schema(), make_lw_shared<sstable_list>({sst1})));
        BOOST_REQUIRE_EQUAL(ss1->all()->size(), 1);

        // Test that a random sstable_origin is stored and retrieved properly.
        mr = flat_mutation_reader_from_mutations(env.make_reader_permit(), {mut});
        auto sst2 = make_sstable_easy(env, tmp, std::move(mr), cfg, gen++);

        auto ss2 = make_lw_shared<sstables::sstable_set>(*ss1);
        ss2->insert(sst2);
        BOOST_REQUIRE_EQUAL(ss2->all()->size(), 2);
        BOOST_REQUIRE_EQUAL(ss1->all()->size(), 1);

        return make_ready_future<>();
    });
}

SEASTAR_TEST_CASE(test_time_series_sstable_set_read_modify_write) {
    return test_setup::do_with_tmp_directory([] (test_env& env, sstring tmpdir_path) {
        simple_schema ss;
        auto s = ss.schema();
        fs::path tmp(tmpdir_path);

        auto pk = ss.make_pkey(make_local_key(s));
        auto mut = mutation(s, pk);
        ss.add_row(mut, ss.make_ckey(0), "val");
        int gen = 1;
        sstable_writer_config cfg = env.manager().configure_writer("");

        auto mr = flat_mutation_reader_from_mutations(env.make_reader_permit(), {mut});
        auto sst1 = make_sstable_easy(env, tmp, std::move(mr), cfg, gen++);

        auto ss1 = make_lw_shared<time_series_sstable_set>(ss.schema());
        ss1->insert(sst1);
        BOOST_REQUIRE_EQUAL(ss1->all()->size(), 1);

        // Test that a random sstable_origin is stored and retrieved properly.
        mr = flat_mutation_reader_from_mutations(env.make_reader_permit(), {mut});
        auto sst2 = make_sstable_easy(env, tmp, std::move(mr), cfg, gen++);

        auto ss2 = make_lw_shared<time_series_sstable_set>(*ss1);
        ss2->insert(sst2);
        BOOST_REQUIRE_EQUAL(ss2->all()->size(), 2);
        BOOST_REQUIRE_EQUAL(ss1->all()->size(), 1);

        return make_ready_future<>();
    });
}
