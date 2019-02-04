/*
 * Copyright (C) 2018 ScyllaDB
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

#include "multishard_mutation_query.hh"
#include "schema_registry.hh"
#include "tests/cql_test_env.hh"
#include "tests/eventually.hh"
#include "tests/cql_assertions.hh"
#include "tests/mutation_assertions.hh"
#include "tests/test_table.hh"

#include <seastar/testing/thread_test_case.hh>

#include <experimental/source_location>

static uint64_t aggregate_querier_cache_stat(distributed<database>& db, uint64_t query::querier_cache::stats::*stat) {
    return map_reduce(boost::irange(0u, smp::count), [stat, &db] (unsigned shard) {
        return db.invoke_on(shard, [stat] (database& local_db) {
            auto& stats = local_db.get_querier_cache_stats();
            return stats.*stat;
        });
    }, 0, std::plus<size_t>()).get0();
}

static void check_cache_population(distributed<database>& db, size_t queriers,
        std::experimental::source_location sl = std::experimental::source_location::current()) {
    BOOST_TEST_MESSAGE(format("{}() called from {}() {}:{:d}", __FUNCTION__, sl.function_name(), sl.file_name(), sl.line()));

    parallel_for_each(boost::irange(0u, smp::count), [queriers, &db] (unsigned shard) {
        return db.invoke_on(shard, [queriers] (database& local_db) {
            auto& stats = local_db.get_querier_cache_stats();
            BOOST_REQUIRE_EQUAL(stats.population, queriers);
        });
    }).get0();
}

static void require_eventually_empty_caches(distributed<database>& db,
        std::experimental::source_location sl = std::experimental::source_location::current()) {
    BOOST_TEST_MESSAGE(format("{}() called from {}() {}:{:d}", __FUNCTION__, sl.function_name(), sl.file_name(), sl.line()));

    auto aggregated_population_is_zero = [&] () mutable {
        return aggregate_querier_cache_stat(db, &query::querier_cache::stats::population) == 0;
    };
    BOOST_REQUIRE(eventually_true(aggregated_population_is_zero));
}

// Best run with SMP>=2
SEASTAR_THREAD_TEST_CASE(test_abandoned_read) {
    do_with_cql_env([] (cql_test_env& env) -> future<> {
        using namespace std::chrono_literals;

        env.db().invoke_on_all([] (database& db) {
            db.set_querier_cache_entry_ttl(2s);
        }).get();

        auto [s, _] = test::create_test_table(env);
        (void)_;

        auto cmd = query::read_command(s->id(), s->version(), s->full_slice(), 7, gc_clock::now(), std::nullopt, query::max_partitions,
                utils::make_random_uuid(), true);

        query_mutations_on_all_shards(env.db(), s, cmd, {query::full_partition_range}, nullptr, std::numeric_limits<uint64_t>::max()).get();

        check_cache_population(env.db(), 1);

        sleep(2s).get();

        require_eventually_empty_caches(env.db());

        return make_ready_future<>();
    }).get();
}

static std::vector<mutation> read_all_partitions_one_by_one(distributed<database>& db, schema_ptr s, std::vector<dht::decorated_key> pkeys) {
    const auto& partitioner = dht::global_partitioner();
    std::vector<mutation> results;
    results.reserve(pkeys.size());

    for (const auto& pkey : pkeys) {
        const auto res = db.invoke_on(partitioner.shard_of(pkey.token()), [gs = global_schema_ptr(s), &pkey] (database& db) {
            return async([s = gs.get(), &pkey, &db] () mutable {
                auto accounter = db.get_result_memory_limiter().new_mutation_read(std::numeric_limits<size_t>::max()).get0();
                const auto cmd = query::read_command(s->id(), s->version(), s->full_slice(), query::max_rows);
                const auto range = dht::partition_range::make_singular(pkey);
                return make_foreign(std::make_unique<reconcilable_result>(
                    db.query_mutations(std::move(s), cmd, range, std::move(accounter), nullptr).get0()));
            });
        }).get0();

        BOOST_REQUIRE_EQUAL(res->partitions().size(), 1);
        results.emplace_back(res->partitions().front().mut().unfreeze(s));
    }

    return results;
}

using stateful_query = bool_class<class stateful>;

static std::pair<std::vector<mutation>, size_t>
read_all_partitions_with_paged_scan(distributed<database>& db, schema_ptr s, size_t page_size, stateful_query is_stateful,
        const std::function<void(size_t)>& page_hook) {
    const auto max_size = std::numeric_limits<uint64_t>::max();
    const auto query_uuid = is_stateful ? utils::make_random_uuid() : utils::UUID{};
    std::vector<mutation> results;
    auto cmd = query::read_command(s->id(), s->version(), s->full_slice(), page_size, gc_clock::now(), std::nullopt, query::max_partitions,
            query_uuid, true);

    // First page is special, needs to have `is_first_page` set.
    {
        auto res = query_mutations_on_all_shards(db, s, cmd, {query::full_partition_range}, nullptr, max_size).get0();
        for (auto& part : res->partitions()) {
            results.emplace_back(part.mut().unfreeze(s));
        }
        cmd.is_first_page = false;
    }

    size_t nrows = page_size;
    unsigned npages = 0;

    // Rest of the pages.
    // Loop until a page turns up with less rows than the limit.
    while (nrows == page_size) {
        page_hook(npages);
        if (is_stateful) {
            BOOST_REQUIRE(aggregate_querier_cache_stat(db, &query::querier_cache::stats::lookups) >= npages);
        }

        const auto& last_pkey = results.back().decorated_key();
        const auto& last_ckey = results.back().partition().clustered_rows().rbegin()->key();
        auto pkrange = dht::partition_range::make_starting_with(dht::partition_range::bound(last_pkey, true));
        auto ckrange = query::clustering_range::make_starting_with(query::clustering_range::bound(last_ckey, false));
        if (const auto& sr = cmd.slice.get_specific_ranges(); sr) {
            cmd.slice.clear_range(*s, sr->pk());
        }
        cmd.slice.set_range(*s, last_pkey.key(), {ckrange});

        auto res = query_mutations_on_all_shards(db, s, cmd, {pkrange}, nullptr, max_size).get0();

        if (res->partitions().empty()) {
            nrows = 0;
        } else {
            auto it = res->partitions().begin();
            auto end = res->partitions().end();
            auto first_mut = it->mut().unfreeze(s);

            nrows = first_mut.live_row_count();

            // The first partition of the new page may overlap with the last
            // partition of the last page.
            if (results.back().decorated_key().equal(*s, first_mut.decorated_key())) {
                results.back().apply(std::move(first_mut));
            } else {
                results.emplace_back(std::move(first_mut));
            }
            ++it;
            for (;it != end; ++it) {
                auto mut = it->mut().unfreeze(s);
                nrows += mut.live_row_count();
                results.emplace_back(std::move(mut));
            }
        }

        ++npages;
    }

    return std::pair(results, npages);
}

void check_results_are_equal(std::vector<mutation>& results1, std::vector<mutation>& results2) {
    BOOST_REQUIRE_EQUAL(results1.size(), results2.size());

    auto mut_less = [] (const mutation& a, const mutation& b) {
        return a.decorated_key().less_compare(*a.schema(), b.decorated_key());
    };
    boost::sort(results1, mut_less);
    boost::sort(results2, mut_less);
    for (unsigned i = 0; i < results1.size(); ++i) {
        BOOST_TEST_MESSAGE(format("Comparing mutation #{:d}", i));
        assert_that(results2[i]).is_equal_to(results1[i]);
    }
}

// Best run with SMP>=2
SEASTAR_THREAD_TEST_CASE(test_read_all) {
    do_with_cql_env([] (cql_test_env& env) -> future<> {
        using namespace std::chrono_literals;

        env.db().invoke_on_all([] (database& db) {
            db.set_querier_cache_entry_ttl(2s);
        }).get();

        auto [s, pkeys] = test::create_test_table(env);

        // First read all partition-by-partition (not paged).
        auto results1 = read_all_partitions_one_by_one(env.db(), s, pkeys);

        // Then do a paged range-query, with reader caching
        auto results2 = read_all_partitions_with_paged_scan(env.db(), s, 4, stateful_query::yes, [&] (size_t) {
            check_cache_population(env.db(), 1);
            BOOST_REQUIRE_EQUAL(aggregate_querier_cache_stat(env.db(), &query::querier_cache::stats::drops), 0);
            BOOST_REQUIRE_EQUAL(aggregate_querier_cache_stat(env.db(), &query::querier_cache::stats::misses), 0);
        }).first;

        check_results_are_equal(results1, results2);

        BOOST_REQUIRE_EQUAL(aggregate_querier_cache_stat(env.db(), &query::querier_cache::stats::drops), 0);
        BOOST_REQUIRE_EQUAL(aggregate_querier_cache_stat(env.db(), &query::querier_cache::stats::misses), 0);
        BOOST_REQUIRE_EQUAL(aggregate_querier_cache_stat(env.db(), &query::querier_cache::stats::time_based_evictions), 0);
        BOOST_REQUIRE_EQUAL(aggregate_querier_cache_stat(env.db(), &query::querier_cache::stats::resource_based_evictions), 0);
        BOOST_REQUIRE_EQUAL(aggregate_querier_cache_stat(env.db(), &query::querier_cache::stats::memory_based_evictions), 0);

        require_eventually_empty_caches(env.db());

        // Then do a paged range-query, without reader caching
        auto results3 = read_all_partitions_with_paged_scan(env.db(), s, 4, stateful_query::no, [&] (size_t) {
            check_cache_population(env.db(), 0);
        }).first;

        check_results_are_equal(results1, results3);

        return make_ready_future<>();
    }).get();
}

// Best run with SMP>=2
SEASTAR_THREAD_TEST_CASE(test_evict_a_shard_reader_on_each_page) {
    do_with_cql_env([] (cql_test_env& env) -> future<> {
        using namespace std::chrono_literals;

        env.db().invoke_on_all([] (database& db) {
            db.set_querier_cache_entry_ttl(2s);
        }).get();

        auto [s, pkeys] = test::create_test_table(env);

        // First read all partition-by-partition (not paged).
        auto results1 = read_all_partitions_one_by_one(env.db(), s, pkeys);

        // Then do a paged range-query
        auto [results2, npages] = read_all_partitions_with_paged_scan(env.db(), s, 4, stateful_query::yes, [&] (size_t page) {
            check_cache_population(env.db(), 1);

            env.db().invoke_on(page % smp::count, [&] (database& db) {
                db.get_querier_cache().evict_one();
            }).get();

            BOOST_REQUIRE_EQUAL(aggregate_querier_cache_stat(env.db(), &query::querier_cache::stats::drops), 0);
            BOOST_REQUIRE_EQUAL(aggregate_querier_cache_stat(env.db(), &query::querier_cache::stats::misses), page);
        });

        check_results_are_equal(results1, results2);

        BOOST_REQUIRE_EQUAL(aggregate_querier_cache_stat(env.db(), &query::querier_cache::stats::drops), 0);
        BOOST_REQUIRE_EQUAL(aggregate_querier_cache_stat(env.db(), &query::querier_cache::stats::time_based_evictions), 0);
        BOOST_REQUIRE_EQUAL(aggregate_querier_cache_stat(env.db(), &query::querier_cache::stats::resource_based_evictions), npages);
        BOOST_REQUIRE_EQUAL(aggregate_querier_cache_stat(env.db(), &query::querier_cache::stats::memory_based_evictions), 0);

        require_eventually_empty_caches(env.db());

        return make_ready_future<>();
    }).get();
}
