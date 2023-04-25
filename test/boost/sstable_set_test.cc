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
#include "test/lib/test_services.hh"
#include "test/lib/random_utils.hh"
#include "readers/from_mutations_v2.hh"
#include "dht/i_partitioner.hh"
#include "dht/ring_position.hh"
#include "dht/token.hh"

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

        auto mr = make_flat_mutation_reader_from_mutations_v2(s, env.make_reader_permit(), mut);
        sstable_writer_config cfg = env.manager().configure_writer("");
        auto sst1 = make_sstable_easy(env, std::move(mr), cfg);

        auto ss1 = make_lw_shared<sstables::sstable_set>(make_sstable_set(ss.schema(), make_lw_shared<sstable_list>({sst1})));
        BOOST_REQUIRE_EQUAL(ss1->all()->size(), 1);

        // Test that a random sstable_origin is stored and retrieved properly.
        mr = make_flat_mutation_reader_from_mutations_v2(s, env.make_reader_permit(), mut);
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

        auto mr = make_flat_mutation_reader_from_mutations_v2(s, env.make_reader_permit(), mut);
        auto sst1 = make_sstable_easy(env, std::move(mr), cfg);

        auto ss1 = make_lw_shared<time_series_sstable_set>(ss.schema(), true);
        ss1->insert(sst1);
        BOOST_REQUIRE_EQUAL(ss1->all()->size(), 1);

        // Test that a random sstable_origin is stored and retrieved properly.
        mr = make_flat_mutation_reader_from_mutations_v2(s, env.make_reader_permit(), mut);
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

        auto mr = make_flat_mutation_reader_from_mutations_v2(s, env.make_reader_permit(), mut);
        auto sst1 = make_sstable_easy(env, std::move(mr), cfg);
        auto size1 = sst1->bytes_on_disk();

        auto ss1 = make_lw_shared<sstable_set>(std::make_unique<time_series_sstable_set>(ss.schema(), true));
        ss1->insert(sst1);
        BOOST_REQUIRE_EQUAL(ss1->bytes_on_disk(), size1);

        // Test that a random sstable_origin is stored and retrieved properly.
        mr = make_flat_mutation_reader_from_mutations_v2(s, env.make_reader_permit(), mut);
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

        auto mr = make_flat_mutation_reader_from_mutations_v2(s, env.make_reader_permit(), mut);
        auto sst1 = make_sstable_easy(env, std::move(mr), cfg);
        auto size1 = sst1->bytes_on_disk();

        auto ss1 = make_lw_shared<sstable_set>(std::make_unique<partitioned_sstable_set>(ss.schema(), true));
        ss1->insert(sst1);
        BOOST_REQUIRE_EQUAL(ss1->bytes_on_disk(), size1);

        // Test that a random sstable_origin is stored and retrieved properly.
        mr = make_flat_mutation_reader_from_mutations_v2(s, env.make_reader_permit(), mut);
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

namespace dht {
    dht::partition_range make_partition_range(const dht::token_range& tr) {
        return dht::partition_range(
            wrapping_interval<dht::token>::transform_bound(tr.start(), [] (const dht::token& token) {
                return dht::ring_position::starting_at(token);
            }),
            wrapping_interval<dht::token>::transform_bound(tr.end(), [] (const dht::token& token) {
                return dht::ring_position::ending_at(token);
            }),
            tr.is_singular()
        );
    }
}

SEASTAR_TEST_CASE(test_table_sstable_test_range_sstable_reader) {
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

        for (auto x_log2_compaction_groups : {0, 1, 2, 3}) {
            auto cfg = replica::table::config{
                .x_log2_compaction_groups = x_log2_compaction_groups,
            };
            auto table = env.make_table_for_tests(s, cfg);
            auto close_table = deferred_stop(table);
            table->start();

            auto sst = table.make_sstable();
            auto mr = make_flat_mutation_reader_from_mutations_v2(s, env.make_reader_permit(), muts);
            auto wr_cfg = env.manager().configure_writer("");
            sst->write_components(std::move(mr), total_keys, s, wr_cfg, encoding_stats{}).get();
            sst->load(s->get_sharder()).get();
            table->add_sstable_and_update_cache(sst).get();

            auto table_sstable_set = table.make_table_sstable_set();
            struct expected_muts {
                dht::partition_range pr;
                std::vector<mutation*> muts;
            };
            std::vector<expected_muts> expected;

            auto get_expected_mutations = [&] (const dht::token_range& r) {
                std::vector<mutation*> expected_muts;
                auto cmp = dht::token_comparator{};
                for (auto& m : muts) {
                    if (r.contains(m.decorated_key().token(), cmp)) {
                        expected_muts.push_back(&m);
                    }
                }
                return expected_muts;
            };

            // Test empty range
            expected.emplace_back(dht::partition_range::make_singular({dht::token{}, partition_key::make_empty()}), std::vector<mutation*>{});

            // Test full range
            expected.emplace_back(query::full_partition_range, boost::copy_range<std::vector<mutation*>>(muts | boost::adaptors::transformed([] (mutation& m) { return &m; })));

            // Test power-of-two ranges
            auto ranges = dht::split_token_range_msb(x_log2_compaction_groups);
            BOOST_REQUIRE(ranges.size() == 1 << x_log2_compaction_groups);
            for (const auto& r : ranges) {
                expected.emplace_back(dht::make_partition_range(r), get_expected_mutations(r));
            }

            // Test random singular range
            auto* mp = &muts[tests::random::get_int(total_keys - 1)];
            expected.emplace_back(dht::partition_range::make_singular(dht::ring_position(mp->decorated_key())), std::vector<mutation*>({ mp }));

            // Test random range
            auto start_token = dht::token::get_random_token();
            auto end_token = dht::token::get_random_token();
            while (end_token <= start_token) {
                end_token = dht::token::get_random_token();
            }
            auto start_bound = dht::token_range::bound(start_token, tests::random::get_bool());
            auto end_bound = dht::token_range::bound(end_token, tests::random::get_bool());
            dht::token_range r;
            switch (tests::random::get_int(0, 2)) {
            case 0:
                r = dht::token_range::make_starting_with(start_bound);
                break;
            case 1:
                r = dht::token_range::make_ending_with(end_bound);
                break;
            default:
                r = dht::token_range::make(start_bound, end_bound);
                break;
            }
            expected.emplace_back(dht::make_partition_range(r), get_expected_mutations(r));

            for (const auto& [pr, muts] : expected) {
                testlog.debug("asserting range_sstable_reader for {}", pr);
                auto r = table_sstable_set->make_range_sstable_reader(s,
                        env.make_reader_permit(),
                        pr,
                        s->full_slice(),
                        nullptr,
                        ::streamed_mutation::forwarding::no,
                        ::mutation_reader::forwarding::no);

                auto rd = assert_that(std::move(r));

                for (const auto* mp : muts) {
                    rd.produces(*mp);
                }
                rd.produces_end_of_stream();
            }
        }
    });
}
