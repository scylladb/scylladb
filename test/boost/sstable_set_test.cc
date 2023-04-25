/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */


#include "test/lib/scylla_test_case.hh"

#include "sstables/sstable_set_impl.hh"
#include "sstables/shared_sstable.hh"
#include "sstables/sstable_set.hh"
#include "sstables/sstables.hh"
#include "test/lib/simple_schema.hh"
#include "test/lib/sstable_utils.hh"
#include "test/lib/flat_mutation_reader_assertions.hh"
#include "readers/from_mutations_v2.hh"
#include "replica/compaction_group.hh"

using namespace sstables;
using namespace replica;

static sstables::sstable_set make_sstable_set(schema_ptr schema, lw_shared_ptr<sstable_list> all = {}, bool use_level_metadata = true) {
    auto ret = sstables::sstable_set(std::make_unique<partitioned_sstable_set>(schema, use_level_metadata), schema);
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

        auto mr = make_flat_mutation_reader_from_mutations_v2(s, env.make_reader_permit(), {mut});
        sstable_writer_config cfg = env.manager().configure_writer("");
        auto sst1 = make_sstable_easy(env, std::move(mr), cfg);

        auto ss1 = make_lw_shared<sstables::sstable_set>(make_sstable_set(ss.schema(), make_lw_shared<sstable_list>({sst1})));
        BOOST_REQUIRE_EQUAL(ss1->all()->size(), 1);

        // Test that a random sstable_origin is stored and retrieved properly.
        mr = make_flat_mutation_reader_from_mutations_v2(s, env.make_reader_permit(), {mut});
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

        auto mr = make_flat_mutation_reader_from_mutations_v2(s, env.make_reader_permit(), {mut});
        auto sst1 = make_sstable_easy(env, std::move(mr), cfg);

        auto ss1 = make_lw_shared<time_series_sstable_set>(ss.schema(), true);
        ss1->insert(sst1);
        BOOST_REQUIRE_EQUAL(ss1->all()->size(), 1);

        // Test that a random sstable_origin is stored and retrieved properly.
        mr = make_flat_mutation_reader_from_mutations_v2(s, env.make_reader_permit(), {mut});
        auto sst2 = make_sstable_easy(env, std::move(mr), cfg);

        auto ss2 = make_lw_shared<time_series_sstable_set>(*ss1);
        ss2->insert(sst2);
        BOOST_REQUIRE_EQUAL(ss2->all()->size(), 2);
        BOOST_REQUIRE_EQUAL(ss1->all()->size(), 1);
    });
}

// stresses the following scenario:
// 1: cg (compaction group) 0 holds range [0, N]
// 2: write sstable A that span the range [0, N]
// 3: split cg 0 into cg 0 and c1, both referencing sstable A
//      cg 0 holds range [0, N/2)
//      cg 1 holds range [N/2, N]
// 4: full scan table
//      cg_sstable_set should read from each cg only the range that belongs to it,
//      even though cg 0 may *temporarily* hold a sstable which data also belongs to cg 1.
SEASTAR_TEST_CASE(test_compaction_group_sstable_set_correctness_after_split) {
    return test_env::do_with_async([] (test_env& env) {
        simple_schema ss;
        auto s = ss.schema();

        static constexpr unsigned total_keys = 100;

        std::vector<mutation> muts;
        muts.reserve(total_keys);
        auto dkeys = tests::generate_partition_keys(total_keys, s);
        for (auto& dk : dkeys) {
            auto mut = mutation(s, dk);
            ss.add_row(mut, ss.make_ckey(0), "val");
            muts.push_back(std::move(mut));
        }
        sstable_writer_config cfg = env.manager().configure_writer("");

        auto mr = make_flat_mutation_reader_from_mutations_v2(s, env.make_reader_permit(), muts);
        auto sst = make_sstable_easy(env, std::move(mr), cfg);
        auto sst_list = make_lw_shared<sstable_list>({ sst });

        const unsigned x_log2_compaction_groups = 1;

        using sstable_set_t = std::pair<const dht::token_range, lw_shared_ptr<sstables::sstable_set>>;
        std::vector<sstable_set_t> sets;

        auto ranges = dht::split_token_range_msb(x_log2_compaction_groups);
        BOOST_REQUIRE(ranges.size() == 2);

        sets.push_back(std::make_pair(ranges[0], make_lw_shared<sstables::sstable_set>(make_sstable_set(s, sst_list))));
        sets.push_back(std::make_pair(ranges[1], make_lw_shared<sstables::sstable_set>(make_sstable_set(s, sst_list))));

        auto cg_sstable_set = make_lw_shared<sstables::sstable_set>(std::make_unique<compaction_group::sstable_set>(s, std::move(sets)), s);

        {
            auto r = cg_sstable_set->make_range_sstable_reader(s,
                                                               env.make_reader_permit(),
                                                               query::full_partition_range,
                                                               s->full_slice(),
                                                               default_priority_class(),
                                                               nullptr,
                                                               ::streamed_mutation::forwarding::no,
                                                               ::mutation_reader::forwarding::no);

            auto rd = assert_that(std::move(r));

            for (const auto& mut : muts) {
                rd.produces(mut);
            }
            rd.produces_end_of_stream();
        }

        {
            auto table = env.make_table_for_tests(s);
            auto close_table = deferred_stop(table);
            for (auto k = 0; k < total_keys; k++) {
                utils::estimated_histogram eh;
                auto pr = dht::partition_range::make_singular(dkeys[k]);

                auto r = cg_sstable_set->create_single_key_sstable_reader(&*table,
                                                                          s,
                                                                          env.make_reader_permit(),
                                                                          eh,
                                                                          pr,
                                                                          s->full_slice(),
                                                                          default_priority_class(),
                                                                          nullptr,
                                                                          ::streamed_mutation::forwarding::no,
                                                                          ::mutation_reader::forwarding::no);

                auto rd = assert_that(std::move(r));
                rd.produces(muts[k]);
                rd.produces_end_of_stream();

                // assert that the read touched only once the SSTable shared by the two compaction groups.
                BOOST_REQUIRE(eh.get(0) == 1);
                BOOST_REQUIRE(eh.get(1) == 0);
            }
        }
    });
}
