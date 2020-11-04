/*
 * Copyright (C) 2020 ScyllaDB
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

#include <random>
#include <seastar/core/sstring.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/align.hh>
#include <seastar/core/aligned_buffer.hh>
#include "sstables/compaction.hh"
#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>
#include "test/boost/sstable_test.hh"
#include <seastar/core/seastar.hh>
#include <seastar/core/do_with.hh>
#include "sstables/compaction_strategy_impl.hh"
#include "sstables/date_tiered_compaction_strategy.hh"
#include "sstables/time_window_compaction_strategy.hh"
#include "sstables/leveled_compaction_strategy.hh"
#include "sstables/sstable_set.hh"
#include "sstables/sstable_set_impl.hh"

using namespace sstables;

static const sstring some_keyspace("ks");
static const sstring some_column_family("cf");

static shared_sstable sstable_for_overlapping_test(test_env& env, const schema_ptr& schema, int64_t gen, sstring first_key, sstring last_key, uint32_t level = 0) {
    auto sst = env.make_sstable(schema, "", gen, la, big);
    sstables::test(sst).set_values_for_leveled_strategy(0, level, 0, std::move(first_key), std::move(last_key));
    return sst;
}

SEASTAR_TEST_CASE(simple_versioned_sstable_set_test) {
    return test_env::do_with([] (test_env& env) {
        auto s = make_shared_schema({}, some_keyspace, some_column_family,
            {{"p1", utf8_type}}, {}, {}, {}, utf8_type);
        auto key_and_token_pair = token_generation_for_current_shard(8);
        auto decorated_keys = boost::copy_range<std::vector<dht::decorated_key>>(
                key_and_token_pair | boost::adaptors::transformed([&s] (const std::pair<sstring, dht::token>& key_and_token) {
                    auto value = bytes(reinterpret_cast<const signed char*>(key_and_token.first.data()), key_and_token.first.size());
                    auto pk = sstables::key::from_bytes(value).to_partition_key(*s);
                    return dht::decorate_key(*s, std::move(pk));
                }));
        struct snapshot_and_selections {
            std::optional<sstable_set> ssts;
            std::vector<std::unordered_set<shared_sstable>> selections;
            std::unordered_map<utils::UUID, std::unordered_set<shared_sstable>> runs;
            std::unordered_set<shared_sstable> all;
            snapshot_and_selections(sstable_set&& ssts, const std::vector<std::unordered_set<shared_sstable>>& selections,
                const std::unordered_map<utils::UUID, std::unordered_set<shared_sstable>>& runs, const std::unordered_set<shared_sstable>& all)
                : ssts(std::move(ssts)), selections(selections), runs(runs), all(all) { }
        };
        auto check = [&decorated_keys] (snapshot_and_selections& version) {
            for (int j = 0; j < 8; j++) {
                auto sel = version.ssts->select(dht::partition_range::make_singular(decorated_keys[j]));
                BOOST_REQUIRE_EQUAL(sel.size(), version.selections[j].size());
                for (auto& sst : sel) {
                    BOOST_REQUIRE(version.selections[j].contains(sst));
                }
            }
            for (auto& [uuid, run] : version.runs) {
                if (run.empty()) {
                    continue;
                }
                std::vector<sstable_run> runs = version.ssts->select_sstable_runs({*run.begin()});
                // only one sstable -> only one run
                BOOST_REQUIRE_EQUAL(runs[0].all().size(), run.size());
                for (auto& sst : runs[0].all()) {
                    BOOST_REQUIRE(run.contains(sst));
                }
            }
            BOOST_REQUIRE_EQUAL(version.ssts->all()->size(), version.all.size());
            for (auto& sst : *version.ssts->all()) {
                BOOST_REQUIRE(version.all.contains(sst));
            }
        };
        // check that selecting from older snapshots of an sstable_set gives correct results.
        {
            auto cs = sstables::make_compaction_strategy(sstables::compaction_strategy_type::leveled, s->compaction_strategy_options());
            std::optional<sstable_set> set = cs.make_sstable_set(s);
            std::vector<std::unordered_set<shared_sstable>> current_selections(8);
            std::unordered_map<utils::UUID, std::unordered_set<shared_sstable>> current_runs;
            std::unordered_set<shared_sstable> current_all;

            std::vector<snapshot_and_selections> versions;
            versions.reserve(20);
            int token = 0;
            for (int i = 0; i < 20; i++) {
                for (int j = 0; j < 10; j++) {
                    auto sst = sstable_for_overlapping_test(env, s, i, key_and_token_pair[token].first, key_and_token_pair[token].first, 1);
                    set->insert(sst);
                    current_selections[token].insert(sst);
                    current_runs[sst->run_identifier()].insert(sst);
                    current_all.insert(sst);
                    ++token %= 8;
                }
                for (int j = 0; j < 5; j++) {
                    auto sst = *set->all()->begin();
                    set->erase(sst);
                    for (auto& sel : current_selections) {
                        // actually erases only from one
                        sel.erase(sst);
                    }
                    current_runs[sst->run_identifier()].erase(sst);
                    current_all.erase(sst);
                }
                versions.emplace_back(std::move(*set), current_selections, current_runs, current_all);
                set = versions.back().ssts;
            }

            for (unsigned i : {15, 12, 6, 9, 19, 14, 4, 5, 13, 16, 2, 7, 0, 1, 10, 11, 3, 17, 8, 18}) {
                check(versions[i]);
                // by removing the reference (by overwriting) we test if it doesn't have influence on results on other snapshots
                versions[i].ssts.reset();
            }
        }
        return make_ready_future<>();
    });
}

SEASTAR_TEST_CASE(sstable_list_test) {
    return test_env::do_with([] (test_env& env) {
        auto s = make_shared_schema({}, some_keyspace, some_column_family,
            {{"p1", utf8_type}}, {}, {}, {}, utf8_type);
        auto key_and_token_pair = token_generation_for_current_shard(8);
        auto decorated_keys = boost::copy_range<std::vector<dht::decorated_key>>(
                key_and_token_pair | boost::adaptors::transformed([&s] (const std::pair<sstring, dht::token>& key_and_token) {
                    auto value = bytes(reinterpret_cast<const signed char*>(key_and_token.first.data()), key_and_token.first.size());
                    auto pk = sstables::key::from_bytes(value).to_partition_key(*s);
                    return dht::decorate_key(*s, std::move(pk));
                }));


        struct list_and_sstables {
            std::optional<sstable_list> list;
            std::unordered_set<shared_sstable> sstables_in_list;

            list_and_sstables(sstable_list&& sstl, std::unordered_set<shared_sstable> sstables_in_list)
                : list(std::move(sstl)), sstables_in_list(sstables_in_list) { }
        };
        auto check = [] (list_and_sstables& version) {
            BOOST_REQUIRE_EQUAL(version.list->size(), version.sstables_in_list.size());
            for (auto& sst : *version.list) {
                BOOST_REQUIRE(version.list->contains(sst));
            }
        };

        {
            auto cs = sstables::make_compaction_strategy(sstables::compaction_strategy_type::leveled, s->compaction_strategy_options());
            std::optional<sstable_set> set = cs.make_sstable_set(s);
            std::optional<sstable_list> l = *set->all();
            std::unordered_set<shared_sstable> sstables_in_list;


            std::vector<list_and_sstables> versions;
            versions.reserve(20);
            int token = 0;
            for (int i = 0; i < 20; i++) {
                for (int j = 0; j < 10; j++) {
                    auto sst = sstable_for_overlapping_test(env, s, i, key_and_token_pair[token].first, key_and_token_pair[token].first, 1);
                    l->insert(sst);
                    sstables_in_list.insert(sst);
                    ++token %= 8;
                }
                for (int j = 0; j < 5; j++) {
                    auto sst = *l->begin();
                    l->erase(sst);
                    sstables_in_list.erase(sst);
                }
                versions.emplace_back(std::move(*l), sstables_in_list);
                l = versions.back().list;
            }

            for (unsigned i : {15, 12, 6, 9, 19, 14, 4, 5, 13, 16, 2, 7, 0, 1, 10, 11, 3, 17, 8, 18}) {
                check(versions[i]);
                // by removing the reference (by overwriting) we test if it doesn't have influence on results on other snapshots
                versions[i].list.reset();
            }
        }
        return make_ready_future<>();
    });
}

SEASTAR_TEST_CASE(sstable_set_version_merging_test) {
    return test_env::do_with([] (test_env& env) {
        auto s = make_shared_schema({}, some_keyspace, some_column_family,
            {{"p1", utf8_type}}, {}, {}, {}, utf8_type);
        auto cs = sstables::make_compaction_strategy(sstables::compaction_strategy_type::leveled, s->compaction_strategy_options());
        auto key_and_token_pair = token_generation_for_current_shard(2);
        std::optional<sstable_set> set = cs.make_sstable_set(s);
        std::optional<sstable_list> list = *set->all();
        // set -> list
        BOOST_REQUIRE_EQUAL(&set->all()->version(), list->version().get_previous_version());
        std::optional<sstable_list> list2 = *set->all();
        // set -> list
        //     -> list2
        BOOST_REQUIRE_EQUAL(&set->all()->version(), list->version().get_previous_version());
        BOOST_REQUIRE_EQUAL(&set->all()->version(), list2->version().get_previous_version());
        auto sst = sstable_for_overlapping_test(env, s, 1, key_and_token_pair[0].first, key_and_token_pair[0].first, 1);
        set->insert(sst);
        // set' -> list
        //      -> list2
        //      -> set
        BOOST_REQUIRE_EQUAL(set->all()->version().get_previous_version(), list->version().get_previous_version());
        BOOST_REQUIRE_EQUAL(set->all()->version().get_previous_version(), list2->version().get_previous_version());
        BOOST_REQUIRE_NE(&set->all()->version(), list->version().get_previous_version());
        sst = sstable_for_overlapping_test(env, s, 1, key_and_token_pair[1].first, key_and_token_pair[1].first, 1);
        set->insert(sst);
        // set' -> list
        //      -> list2
        //      -> set
        BOOST_REQUIRE_EQUAL(set->all()->version().get_previous_version(), list->version().get_previous_version());
        BOOST_REQUIRE_EQUAL(set->all()->version().get_previous_version(), list2->version().get_previous_version());
        BOOST_REQUIRE_NE(&set->all()->version(), list->version().get_previous_version());
        std::optional<sstable_list> list3 = list;
        // set' -> list -> list3
        //      -> list2
        //      -> set
        BOOST_REQUIRE_EQUAL(&list->version(), list3->version().get_previous_version());
        list.reset();
        // set' -> list3
        //      -> list2
        //      -> set
        BOOST_REQUIRE_EQUAL(set->all()->version().get_previous_version(), list3->version().get_previous_version());
        BOOST_REQUIRE_EQUAL(list2->version().get_previous_version(), list3->version().get_previous_version());
        set.reset();
        // set' -> list3
        //      -> list2
        BOOST_REQUIRE_EQUAL(list2->version().get_previous_version(), list3->version().get_previous_version());
        std::optional<sstable_list> list4 = list3;
        std::optional<sstable_list> list5 = list3;
        // set' -> list3 -> list4
        //               -> list5
        //      -> list2
        BOOST_REQUIRE_EQUAL(&list3->version(), list4->version().get_previous_version());
        BOOST_REQUIRE_EQUAL(&list3->version(), list5->version().get_previous_version());
        list3.reset();
        // set' -> list3' -> list4
        //                -> list5
        //      -> list2
        BOOST_REQUIRE_NE(list2->version().get_previous_version(), list4->version().get_previous_version());
        BOOST_REQUIRE_EQUAL(list4->version().get_previous_version(), list5->version().get_previous_version());
        list4.reset();
        // set' -> list5
        //      -> list2
        BOOST_REQUIRE_EQUAL(list2->version().get_previous_version(), list5->version().get_previous_version());
        list2.reset();
        // list5
        BOOST_REQUIRE_EQUAL(nullptr, list5->version().get_previous_version());
        return make_ready_future<>();
    });
}

SEASTAR_TEST_CASE(sstable_set_propagate_erased_sstables_erased_in_last_descendant_test) {
    return test_env::do_with([] (test_env& env) {
        auto s = make_shared_schema({}, some_keyspace, some_column_family,
            {{"p1", utf8_type}}, {}, {}, {}, utf8_type);
        auto cs = sstables::make_compaction_strategy(sstables::compaction_strategy_type::leveled, s->compaction_strategy_options());
        auto key_and_token_pair = token_generation_for_current_shard(1);
        {
            std::optional<sstable_set> set = cs.make_sstable_set(s);
            auto sst = sstable_for_overlapping_test(env, s, 1, key_and_token_pair[0].first, key_and_token_pair[0].first, 1);
            bool is_sstable_removed = false;
            utils::observer<sstable&> observer = sst->add_on_closed_handler([&] (sstable& sst) {
                is_sstable_removed = true;
            });
            set->insert(sst);
            std::optional<sstable_set> set2 = set;
            std::optional<sstable_set> set3 = set;
            // set -> set2
            //     -> set3
            set.reset();
            set2->erase(sst);
            set3->erase(sst);
            sst = nullptr;
            BOOST_REQUIRE(is_sstable_removed);
        }
        {
            std::optional<sstable_set> set = cs.make_sstable_set(s);
            auto sst = sstable_for_overlapping_test(env, s, 1, key_and_token_pair[0].first, key_and_token_pair[0].first, 1);
            bool is_sstable_removed = false;
            utils::observer<sstable&> observer = sst->add_on_closed_handler([&] (sstable& sst) {
                is_sstable_removed = true;
            });
            set->insert(sst);
            std::optional<sstable_set> set2 = set;
            std::optional<sstable_set> set3 = set;
            std::optional<sstable_set> set4 = set;
            // set -> set2
            //     -> set3
            //     -> set4
            set.reset();
            set2->erase(sst);
            set3.reset();
            set4->erase(sst);
            sst = nullptr;
            BOOST_REQUIRE(is_sstable_removed);
        }
        {
            std::optional<sstable_set> set = cs.make_sstable_set(s);
            auto sst = sstable_for_overlapping_test(env, s, 1, key_and_token_pair[0].first, key_and_token_pair[0].first, 1);
            bool is_sstable_removed = false;
            utils::observer<sstable&> observer = sst->add_on_closed_handler([&] (sstable& sst) {
                is_sstable_removed = true;
            });
            set->insert(sst);
            std::optional<sstable_set> set2 = set;
            std::optional<sstable_set> set3 = set;
            std::optional<sstable_set> set4 = set3;
            std::optional<sstable_set> set5 = set3;
            // set -> set2
            //     -> set3 -> set4
            //             -> set5
            set.reset();
            set2->erase(sst);
            set3.reset();
            set4->erase(sst);
            set5->erase(sst);
            sst = nullptr;
            BOOST_REQUIRE(is_sstable_removed);
        }
        return make_ready_future<>();
    });
}

SEASTAR_TEST_CASE(sstable_set_propagate_erased_sstables_remove_reference_to_ancestor_test) {
    return test_env::do_with([] (test_env& env) {
        auto s = make_shared_schema({}, some_keyspace, some_column_family,
            {{"p1", utf8_type}}, {}, {}, {}, utf8_type);
        auto cs = sstables::make_compaction_strategy(sstables::compaction_strategy_type::leveled, s->compaction_strategy_options());
        auto key_and_token_pair = token_generation_for_current_shard(1);
        {
            std::optional<sstable_set> set = cs.make_sstable_set(s);
            auto sst = sstable_for_overlapping_test(env, s, 1, key_and_token_pair[0].first, key_and_token_pair[0].first, 1);
            bool is_sstable_removed = false;
            utils::observer<sstable&> observer = sst->add_on_closed_handler([&] (sstable& sst) {
                is_sstable_removed = true;
            });
            set->insert(sst);
            std::optional<sstable_set> set2 = set;
            // set -> set2
            set2->erase(sst);
            sst = nullptr;
            BOOST_REQUIRE(!is_sstable_removed);
            set.reset();
            BOOST_REQUIRE(is_sstable_removed);
        }
        {
            std::optional<sstable_set> set = cs.make_sstable_set(s);
            auto sst = sstable_for_overlapping_test(env, s, 1, key_and_token_pair[0].first, key_and_token_pair[0].first, 1);
            bool is_sstable_removed = false;
            utils::observer<sstable&> observer = sst->add_on_closed_handler([&] (sstable& sst) {
                is_sstable_removed = true;
            });
            set->insert(sst);
            std::optional<sstable_set> set2 = set;
            std::optional<sstable_set> set3 = set;
            // set -> set2
            //     -> set3
            set2->erase(sst);
            set3->erase(sst);
            sst = nullptr;
            BOOST_REQUIRE(!is_sstable_removed);
            set.reset();
            BOOST_REQUIRE(is_sstable_removed);
        }
        {
            std::optional<sstable_set> set = cs.make_sstable_set(s);
            auto sst = sstable_for_overlapping_test(env, s, 1, key_and_token_pair[0].first, key_and_token_pair[0].first, 1);
            bool is_sstable_removed = false;
            utils::observer<sstable&> observer = sst->add_on_closed_handler([&] (sstable& sst) {
                is_sstable_removed = true;
            });
            set->insert(sst);
            std::optional<sstable_set> set2 = set;
            std::optional<sstable_set> set3 = set;
            std::optional<sstable_set> set4 = set;
            // set -> set2
            //     -> set3
            //     -> set4
            set2->erase(sst);
            set3.reset();
            set4->erase(sst);
            sst = nullptr;
            BOOST_REQUIRE(!is_sstable_removed);
            set.reset();
            BOOST_REQUIRE(is_sstable_removed);
        }
        {
            std::optional<sstable_set> set = cs.make_sstable_set(s);
            auto sst = sstable_for_overlapping_test(env, s, 1, key_and_token_pair[0].first, key_and_token_pair[0].first, 1);
            bool is_sstable_removed = false;
            utils::observer<sstable&> observer = sst->add_on_closed_handler([&] (sstable& sst) {
                is_sstable_removed = true;
            });
            set->insert(sst);
            std::optional<sstable_set> set2 = set;
            std::optional<sstable_set> set3 = set;
            std::optional<sstable_set> set4 = set3;
            std::optional<sstable_set> set5 = set3;
            // set -> set2
            //     -> set3 -> set4
            //             -> set5
            set.reset();
            set2->erase(sst);
            set4->erase(sst);
            set5->erase(sst);
            sst = nullptr;
            BOOST_REQUIRE(!is_sstable_removed);
            set3.reset();
            BOOST_REQUIRE(is_sstable_removed);
        }
        return make_ready_future<>();
    });
}


SEASTAR_TEST_CASE(sstable_set_propagate_erased_sstables_remove_reference_to_descendant_test) {
    return test_env::do_with([] (test_env& env) {
        auto s = make_shared_schema({}, some_keyspace, some_column_family,
            {{"p1", utf8_type}}, {}, {}, {}, utf8_type);
        auto cs = sstables::make_compaction_strategy(sstables::compaction_strategy_type::leveled, s->compaction_strategy_options());
        auto key_and_token_pair = token_generation_for_current_shard(1);
        {
            std::optional<sstable_set> set = cs.make_sstable_set(s);
            auto sst = sstable_for_overlapping_test(env, s, 1, key_and_token_pair[0].first, key_and_token_pair[0].first, 1);
            bool is_sstable_removed = false;
            utils::observer<sstable&> observer = sst->add_on_closed_handler([&] (sstable& sst) {
                is_sstable_removed = true;
            });
            set->insert(sst);
            std::optional<sstable_set> set2 = set;
            std::optional<sstable_set> set3 = set;
            // set -> set2
            //     -> set3
            set.reset();
            set2->erase(sst);
            sst = nullptr;
            BOOST_REQUIRE(!is_sstable_removed);
            set3.reset();
            BOOST_REQUIRE(is_sstable_removed);
        }
        {
            std::optional<sstable_set> set = cs.make_sstable_set(s);
            auto sst = sstable_for_overlapping_test(env, s, 1, key_and_token_pair[0].first, key_and_token_pair[0].first, 1);
            bool is_sstable_removed = false;
            utils::observer<sstable&> observer = sst->add_on_closed_handler([&] (sstable& sst) {
                is_sstable_removed = true;
            });
            set->insert(sst);
            std::optional<sstable_set> set2 = set;
            std::optional<sstable_set> set3 = set;
            std::optional<sstable_set> set4 = set;
            // set -> set2
            //     -> set3
            //     -> set4
            set.reset();
            set2->erase(sst);
            set3->erase(sst);
            sst = nullptr;
            BOOST_REQUIRE(!is_sstable_removed);
            set4.reset();
            BOOST_REQUIRE(is_sstable_removed);
        }
        {
            std::optional<sstable_set> set = cs.make_sstable_set(s);
            auto sst = sstable_for_overlapping_test(env, s, 1, key_and_token_pair[0].first, key_and_token_pair[0].first, 1);
            bool is_sstable_removed = false;
            utils::observer<sstable&> observer = sst->add_on_closed_handler([&] (sstable& sst) {
                is_sstable_removed = true;
            });
            set->insert(sst);
            std::optional<sstable_set> set2 = set;
            std::optional<sstable_set> set3 = set;
            std::optional<sstable_set> set4 = set3;
            std::optional<sstable_set> set5 = set3;
            // set -> set2
            //     -> set3 -> set4
            //             -> set5
            set.reset();
            set2->erase(sst);
            set3.reset();
            set4->erase(sst);
            sst = nullptr;
            BOOST_REQUIRE(!is_sstable_removed);
            set5.reset();
            BOOST_REQUIRE(is_sstable_removed);
        }
        return make_ready_future<>();
    });
}

class simple_sstable_set {
    std::unique_ptr<sstable_set_impl> _impl;
    schema_ptr _schema;
    // used to support column_family::get_sstable(), which wants to return an sstable_list
    // that has a reference somewhere
    lw_shared_ptr<std::unordered_set<shared_sstable>> _all;
    std::unordered_map<utils::UUID, sstable_run> _all_runs;
public:
    ~simple_sstable_set() = default;

    simple_sstable_set(std::unique_ptr<sstable_set_impl> impl, schema_ptr schema)
        : _impl(std::move(impl))
        , _schema(std::move(schema))
        , _all(make_lw_shared<std::unordered_set<shared_sstable>>()) {
    }

    simple_sstable_set(const simple_sstable_set& x)
        : _impl(x._impl->clone())
        , _schema(x._schema)
        , _all(make_lw_shared<std::unordered_set<shared_sstable>>(*x._all))
        , _all_runs(x._all_runs) {
    }

    simple_sstable_set(simple_sstable_set&& x) noexcept = default;

    simple_sstable_set& operator=(const simple_sstable_set& x) {
        if (this != &x) {
            auto tmp = simple_sstable_set(x);
            *this = std::move(tmp);
        }
        return *this;
    }

    simple_sstable_set& operator=(simple_sstable_set&&) noexcept = default;

    std::vector<shared_sstable> select(const dht::partition_range& range) const {
        return _impl->select(range);
    }

    // Return all runs which contain any of the input sstables.
    std::vector<sstable_run> select_sstable_runs(const std::vector<shared_sstable>& sstables) const {
        auto run_ids = boost::copy_range<std::unordered_set<utils::UUID>>(sstables | boost::adaptors::transformed(std::mem_fn(&sstable::run_identifier)));
        return boost::copy_range<std::vector<sstable_run>>(run_ids | boost::adaptors::transformed([this] (utils::UUID run_id) {
            return _all_runs.at(run_id);
        }));
    }

    lw_shared_ptr<std::unordered_set<shared_sstable>> all() const { return _all; }

    void insert(shared_sstable sst) {
        _impl->insert(sst);
        _all->insert(sst);
        _all_runs[sst->run_identifier()].insert(sst);
    }

    void erase(shared_sstable sst) {
        _impl->erase(sst);
        _all->erase(sst);
        _all_runs[sst->run_identifier()].erase(sst);
    }
};

SEASTAR_TEST_CASE(sstable_set_random_walk_test) {
    return test_env::do_with([] (test_env& env) {
        auto rand = std::default_random_engine();
        auto op_gen = std::uniform_int_distribution<unsigned>(0, 7);
        auto nr_dist = std::geometric_distribution<size_t>(0.7);
        auto s = make_shared_schema({}, some_keyspace, some_column_family,
            {{"p1", utf8_type}}, {}, {}, {}, utf8_type);
        auto leveled_cs = leveled_compaction_strategy(s->compaction_strategy_options());
        std::vector<sstable_set> sstable_sets;
        std::vector<simple_sstable_set> simple_sstable_sets;
        sstable_sets.emplace_back(leveled_cs.make_sstable_set(s), s);
        simple_sstable_sets.emplace_back(leveled_cs.make_sstable_set(s), s);
        auto key_and_token_pair = token_generation_for_current_shard(1000);
        std::vector<shared_sstable> sstables(1000);
        for (int i = 0; i < 1000; i++) {
            sstables[i] = sstable_for_overlapping_test(env, s, i, key_and_token_pair[i].first, key_and_token_pair[i].first, i);
        }
        for (auto i = 0; i != 100000; i++) {
            auto op = op_gen(rand);
            auto u = std::uniform_int_distribution<size_t>(0, sstable_sets.size() - 1);
            auto idx = u(rand);
            switch (op) {
            case 0: {
                // delete
                if (sstable_sets.size() > 1) {
                    sstable_sets.erase(sstable_sets.begin() + idx);
                    simple_sstable_sets.erase(simple_sstable_sets.begin() + idx);
                    break;
                }
                // if we can't remove the version, let's create one
                [[fallthrough]];
            }
            case 1: {
                // copy
                if (sstable_sets.size() < 100) {
                    sstable_sets.emplace_back(sstable_sets[idx]);
                    simple_sstable_sets.emplace_back(simple_sstable_sets[idx]);
                    for (auto& sst : *simple_sstable_sets.back().all()) {
                        BOOST_REQUIRE(sstable_sets.back().all()->contains(sst));
                    }
                }
                break;
            }
            default:
                // modify
                auto sst_u = std::uniform_int_distribution<size_t>(0, 999);
                auto sst_idx = sst_u(rand);
                if (simple_sstable_sets[idx].all()->contains(sstables[sst_idx])) {
                    sstable_sets[idx].erase(sstables[sst_idx]);
                    simple_sstable_sets[idx].erase(sstables[sst_idx]);
                    BOOST_REQUIRE(!sstable_sets[idx].all()->contains(sstables[sst_idx]));
                    BOOST_REQUIRE(!sstable_sets[idx].all()->contains(sstables[sst_idx]));
                } else {
                    sstable_sets[idx].insert(sstables[sst_idx]);
                    simple_sstable_sets[idx].insert(sstables[sst_idx]);
                    BOOST_REQUIRE(sstable_sets[idx].all()->contains(sstables[sst_idx]));
                    BOOST_REQUIRE(sstable_sets[idx].all()->contains(sstables[sst_idx]));
                }
            }
            for (int j = 0; j < sstable_sets.size(); j++) {
                BOOST_REQUIRE_EQUAL(sstable_sets[j].all()->size(), simple_sstable_sets[j].all()->size());
            }
        }
        return make_ready_future<>();
    });
}
