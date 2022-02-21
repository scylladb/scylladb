/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "multishard_mutation_query.hh"
#include "schema_registry.hh"
#include "db/config.hh"
#include "partition_slice_builder.hh"
#include "serializer_impl.hh"
#include "query-result-set.hh"
#include "test/lib/cql_test_env.hh"
#include "test/lib/eventually.hh"
#include "test/lib/cql_assertions.hh"
#include "test/lib/mutation_assertions.hh"
#include "test/lib/test_table.hh"
#include "test/lib/log.hh"
#include "test/lib/test_utils.hh"
#include "test/lib/random_utils.hh"

#include <seastar/testing/thread_test_case.hh>

#include <experimental/source_location>

#include <boost/range/algorithm/sort.hpp>

const sstring KEYSPACE_NAME = "multishard_mutation_query_test";

static uint64_t aggregate_querier_cache_stat(distributed<replica::database>& db, uint64_t query::querier_cache::stats::*stat) {
    return map_reduce(boost::irange(0u, smp::count), [stat, &db] (unsigned shard) {
        return db.invoke_on(shard, [stat] (replica::database& local_db) {
            auto& stats = local_db.get_querier_cache_stats();
            return stats.*stat;
        });
    }, 0, std::plus<size_t>()).get0();
}

static void check_cache_population(distributed<replica::database>& db, size_t queriers,
        std::experimental::source_location sl = std::experimental::source_location::current()) {
    testlog.info("{}() called from {}() {}:{:d}", __FUNCTION__, sl.function_name(), sl.file_name(), sl.line());

    parallel_for_each(boost::irange(0u, smp::count), [queriers, &db] (unsigned shard) {
        return db.invoke_on(shard, [queriers] (replica::database& local_db) {
            auto& stats = local_db.get_querier_cache_stats();
            tests::require_equal(stats.population, queriers);
        });
    }).get0();
}

static void require_eventually_empty_caches(distributed<replica::database>& db,
        std::experimental::source_location sl = std::experimental::source_location::current()) {
    testlog.info("{}() called from {}() {}:{:d}", __FUNCTION__, sl.function_name(), sl.file_name(), sl.line());

    auto aggregated_population_is_zero = [&] () mutable {
        return aggregate_querier_cache_stat(db, &query::querier_cache::stats::population) == 0;
    };
    tests::require(eventually_true(aggregated_population_is_zero));
}

// Best run with SMP>=2
SEASTAR_THREAD_TEST_CASE(test_abandoned_read) {
    do_with_cql_env_thread([] (cql_test_env& env) -> future<> {
        using namespace std::chrono_literals;

        env.db().invoke_on_all([] (replica::database& db) {
            db.set_querier_cache_entry_ttl(1s);
        }).get();

        auto [s, _] = test::create_test_table(env, KEYSPACE_NAME, "test_abandoned_read");
        (void)_;

        auto cmd = query::read_command(s->id(), s->version(), s->full_slice(), 7, gc_clock::now(), std::nullopt, query::max_partitions,
                utils::make_random_uuid(), query::is_first_page::yes, query::max_result_size(query::result_memory_limiter::unlimited_result_size), 0);

        query_mutations_on_all_shards(env.db(), s, cmd, {query::full_partition_range}, nullptr, db::no_timeout).get();

        check_cache_population(env.db(), 1);

        sleep(2s).get();

        require_eventually_empty_caches(env.db());

        return make_ready_future<>();
    }).get();
}

static std::vector<mutation> read_all_partitions_one_by_one(distributed<replica::database>& db, schema_ptr s, std::vector<dht::decorated_key> pkeys,
        const query::partition_slice& slice) {
    const auto& sharder = s->get_sharder();
    std::vector<mutation> results;
    results.reserve(pkeys.size());

    for (const auto& pkey : pkeys) {
        const auto res = db.invoke_on(sharder.shard_of(pkey.token()), [gs = global_schema_ptr(s), &pkey, &slice] (replica::database& db) {
            return async([s = gs.get(), &pkey, &slice, &db] () mutable {
                const auto cmd = query::read_command(s->id(), s->version(), slice,
                        query::max_result_size(query::result_memory_limiter::unlimited_result_size));
                const auto range = dht::partition_range::make_singular(pkey);
                return make_foreign(std::make_unique<reconcilable_result>(
                    std::get<0>(db.query_mutations(std::move(s), cmd, range, nullptr, db::no_timeout).get0())));
            });
        }).get0();

        tests::require_equal(res->partitions().size(), 1u);
        results.emplace_back(res->partitions().front().mut().unfreeze(s));
    }

    return results;
}

static std::vector<mutation> read_all_partitions_one_by_one(distributed<replica::database>& db, schema_ptr s, std::vector<dht::decorated_key> pkeys) {
    return read_all_partitions_one_by_one(db, s, pkeys, s->full_slice());
}

using stateful_query = bool_class<class stateful>;

template <typename ResultBuilder>
static std::pair<typename ResultBuilder::end_result_type, size_t>
read_partitions_with_generic_paged_scan(distributed<replica::database>& db, schema_ptr s, uint32_t page_size, uint64_t max_size, stateful_query is_stateful,
        const dht::partition_range_vector& original_ranges, const query::partition_slice& slice, const std::function<void(size_t)>& page_hook = {}) {
    const auto query_uuid = is_stateful ? utils::make_random_uuid() : utils::UUID{};
    ResultBuilder res_builder(s, slice);
    auto cmd = query::read_command(s->id(), s->version(), slice, page_size, gc_clock::now(), std::nullopt, query::max_partitions, query_uuid,
            query::is_first_page::yes, query::max_result_size(max_size), 0);

    bool has_more = true;

    auto ranges = std::make_unique<dht::partition_range_vector>(original_ranges);

    // First page is special, needs to have `is_first_page` set.
    {
        auto res = ResultBuilder::query(db, s, cmd, *ranges, nullptr, db::no_timeout);
        has_more = res_builder.add(*res);
        cmd.is_first_page = query::is_first_page::no;
    }

    if (!has_more) {
        return std::pair(std::move(res_builder).get_end_result(), 1);
    }

    unsigned npages = 0;

    const auto cmp = dht::ring_position_comparator(*s);

    // Rest of the pages. Loop until an empty page turns up. Not very
    // sophisticated but simple and safe.
    while (has_more) {
        if (page_hook) {
            page_hook(npages);
        }

        ++npages;

        // Force freeing the vector to avoid hiding any bugs related to storing
        // references to the ranges vector (which is not alive between pages in
        // real life).
        ranges = std::make_unique<dht::partition_range_vector>(original_ranges);

        while (!ranges->front().contains(res_builder.last_pkey(), cmp)) {
            ranges->erase(ranges->begin());
        }
        assert(!ranges->empty());

        const auto pkrange_begin_inclusive = res_builder.last_ckey() && res_builder.last_pkey_rows() < slice.partition_row_limit();

        ranges->front() = dht::partition_range(dht::partition_range::bound(res_builder.last_pkey(), pkrange_begin_inclusive), ranges->front().end());

        if (res_builder.last_ckey()) {
            auto ckranges = cmd.slice.default_row_ranges();
            query::trim_clustering_row_ranges_to(*s, ckranges, *res_builder.last_ckey(), slice.is_reversed());
            cmd.slice.clear_range(*s, res_builder.last_pkey().key());
            cmd.slice.clear_ranges();
            cmd.slice.set_range(*s, res_builder.last_pkey().key(), std::move(ckranges));
        }

        auto res = ResultBuilder::query(db, s, cmd, *ranges, nullptr, db::no_timeout);

        if (is_stateful) {
            tests::require(aggregate_querier_cache_stat(db, &query::querier_cache::stats::lookups) >= npages);
        }

        has_more = res_builder.add(*res);
    }

    return std::pair(std::move(res_builder).get_end_result(), npages);
}

template <typename ResultBuilder>
static std::pair<typename ResultBuilder::end_result_type, size_t>
read_partitions_with_generic_paged_scan(distributed<replica::database>& db, schema_ptr s, uint32_t page_size, uint64_t max_size, stateful_query is_stateful,
        const dht::partition_range& range, const query::partition_slice& slice, const std::function<void(size_t)>& page_hook = {}) {
    dht::partition_range_vector ranges{range};
    return read_partitions_with_generic_paged_scan<ResultBuilder>(db, std::move(s), page_size, max_size, is_stateful, ranges, slice, page_hook);
}

class mutation_result_builder {
public:
    using end_result_type = std::vector<mutation>;

private:
    schema_ptr _s;
    const query::partition_slice& _slice;
    std::vector<mutation> _results;
    std::optional<dht::decorated_key> _last_pkey;
    std::optional<clustering_key> _last_ckey;
    uint64_t _last_pkey_rows = 0;

private:
    std::optional<clustering_key> last_ckey_of(const mutation& mut) const {
        if (mut.partition().clustered_rows().empty()) {
            return std::nullopt;
        }
        if (_slice.is_reversed()) {
            return mut.partition().clustered_rows().begin()->key();
        } else {
            return mut.partition().clustered_rows().rbegin()->key();
        }
    }

public:
    static foreign_ptr<lw_shared_ptr<reconcilable_result>> query(
            distributed<replica::database>& db,
            schema_ptr s,
            const query::read_command& cmd,
            const dht::partition_range_vector& ranges,
            tracing::trace_state_ptr trace_state,
            db::timeout_clock::time_point timeout) {
        return std::get<0>(query_mutations_on_all_shards(db, std::move(s), cmd, ranges, std::move(trace_state), timeout).get());
    }

    explicit mutation_result_builder(schema_ptr s, const query::partition_slice& slice) : _s(std::move(s)), _slice(slice) { }

    bool add(const reconcilable_result& res) {
        auto it = res.partitions().begin();
        auto end = res.partitions().end();
        if (it == end) {
            return false;
        }

        auto first_mut = it->mut().unfreeze(_s);

        _last_pkey = first_mut.decorated_key();
        _last_ckey = last_ckey_of(first_mut);

        // The first partition of the new page may overlap with the last
        // partition of the last page.
        if (!_results.empty() && _results.back().decorated_key().equal(*_s, first_mut.decorated_key())) {
            _last_pkey_rows += it->row_count();
            _results.back().apply(std::move(first_mut));
        } else {
            _last_pkey_rows = it->row_count();
            _results.emplace_back(std::move(first_mut));
        }
        ++it;
        for (;it != end; ++it) {
            auto mut = it->mut().unfreeze(_s);
            _last_pkey = mut.decorated_key();
            _last_pkey_rows = it->row_count();
            _last_ckey = last_ckey_of(mut);
            _results.emplace_back(std::move(mut));
        }

        return true;
    }

    const dht::decorated_key& last_pkey() const { return _last_pkey.value(); }
    const clustering_key* last_ckey() const { return _last_ckey ? &*_last_ckey : nullptr; }
    uint64_t last_pkey_rows() const { return _last_pkey_rows; }

    end_result_type get_end_result() && {
        return std::move(_results);
    }
};

class data_result_builder {
public:
    using end_result_type = query::result_set;

private:
    schema_ptr _s;
    const query::partition_slice& _slice;
    std::vector<query::result_set_row> _rows;
    std::optional<dht::decorated_key> _last_pkey;
    std::optional<clustering_key> _last_ckey;
    uint64_t _last_pkey_rows = 0;

    template <typename Key>
    Key extract_key(const query::result_set_row& row, const schema::const_iterator_range_type& key_cols) const {
        std::vector<bytes> exploded;
        for (const auto& cdef : key_cols) {
            exploded.push_back(row.get_data_value(cdef.name_as_text())->serialize_nonnull());
        }
        return Key::from_exploded(*_s, exploded);
    }

    dht::decorated_key extract_pkey(const query::result_set_row& row) const {
        return dht::decorate_key(*_s, extract_key<partition_key>(row, _s->partition_key_columns()));
    }

    clustering_key extract_ckey(const query::result_set_row& row) const {
        return extract_key<clustering_key>(row, _s->clustering_key_columns());
    }

public:
    static foreign_ptr<lw_shared_ptr<query::result>> query(
            distributed<replica::database>& db,
            schema_ptr s,
            const query::read_command& cmd,
            const dht::partition_range_vector& ranges,
            tracing::trace_state_ptr trace_state,
            db::timeout_clock::time_point timeout) {
        return std::get<0>(query_data_on_all_shards(db, std::move(s), cmd, ranges, query::result_options::only_result(), std::move(trace_state), timeout).get());
    }

    explicit data_result_builder(schema_ptr s, const query::partition_slice& slice) : _s(std::move(s)), _slice(slice) { }

    bool add(const query::result& raw_res) {
        auto res = query::result_set::from_raw_result(_s, _slice, raw_res);
        if (res.rows().empty()) {
            return false;
        }
        for (const auto& row : res.rows()) {
            _rows.push_back(row);
            _last_ckey = extract_ckey(row);
            auto last_pkey = extract_pkey(row);
            if (_last_pkey && last_pkey.equal(*_s, *_last_pkey)) {
                ++_last_pkey_rows;
            } else {
                _last_pkey = std::move(last_pkey);
                _last_pkey_rows = 1;
            }
        }
        return true;
    }

    const dht::decorated_key& last_pkey() const { return _last_pkey.value(); }
    const clustering_key* last_ckey() const { return _last_ckey ? &*_last_ckey : nullptr; }
    uint64_t last_pkey_rows() const { return _last_pkey_rows; }

    end_result_type get_end_result() && {
        return query::result_set(_s, std::move(_rows));
    }
};

static std::pair<std::vector<mutation>, size_t>
read_partitions_with_paged_scan(distributed<replica::database>& db, schema_ptr s, uint32_t page_size, uint64_t max_size, stateful_query is_stateful,
        const dht::partition_range& range, const query::partition_slice& slice, const std::function<void(size_t)>& page_hook = {}) {
    return read_partitions_with_generic_paged_scan<mutation_result_builder>(db, std::move(s), page_size, max_size, is_stateful, range, slice, page_hook);
}

static std::pair<std::vector<mutation>, size_t>
read_all_partitions_with_paged_scan(distributed<replica::database>& db, schema_ptr s, uint32_t page_size, stateful_query is_stateful,
        const std::function<void(size_t)>& page_hook) {
    return read_partitions_with_paged_scan(db, s, page_size, std::numeric_limits<uint64_t>::max(), is_stateful, query::full_partition_range,
            s->full_slice(), page_hook);
}

void check_results_are_equal(std::vector<mutation>& results1, std::vector<mutation>& results2) {
    tests::require_equal(results1.size(), results2.size());

    auto mut_less = [] (const mutation& a, const mutation& b) {
        return a.decorated_key().less_compare(*a.schema(), b.decorated_key());
    };
    boost::sort(results1, mut_less);
    boost::sort(results2, mut_less);
    for (unsigned i = 0; i < results1.size(); ++i) {
        testlog.trace("Comparing mutation #{:d}", i);
        assert_that(results2[i]).is_equal_to(results1[i]);
    }
}

// Best run with SMP>=2
SEASTAR_THREAD_TEST_CASE(test_read_all) {
    do_with_cql_env_thread([] (cql_test_env& env) -> future<> {
        using namespace std::chrono_literals;

        env.db().invoke_on_all([] (replica::database& db) {
            db.set_querier_cache_entry_ttl(2s);
        }).get();

        auto [s, pkeys] = test::create_test_table(env, KEYSPACE_NAME, "test_read_all");

        // First read all partition-by-partition (not paged).
        auto results1 = read_all_partitions_one_by_one(env.db(), s, pkeys);

        // Then do a paged range-query, with reader caching
        auto results2 = read_all_partitions_with_paged_scan(env.db(), s, 4, stateful_query::yes, [&] (size_t) {
            check_cache_population(env.db(), 1);
            tests::require_equal(aggregate_querier_cache_stat(env.db(), &query::querier_cache::stats::drops), 0u);
            tests::require_equal(aggregate_querier_cache_stat(env.db(), &query::querier_cache::stats::misses), 0u);
        }).first;

        check_results_are_equal(results1, results2);

        tests::require_equal(aggregate_querier_cache_stat(env.db(), &query::querier_cache::stats::drops), 0u);
        tests::require_equal(aggregate_querier_cache_stat(env.db(), &query::querier_cache::stats::misses), 0u);
        tests::require_equal(aggregate_querier_cache_stat(env.db(), &query::querier_cache::stats::time_based_evictions), 0u);
        tests::require_equal(aggregate_querier_cache_stat(env.db(), &query::querier_cache::stats::resource_based_evictions), 0u);

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
SEASTAR_THREAD_TEST_CASE(test_read_all_multi_range) {
    do_with_cql_env_thread([] (cql_test_env& env) -> future<> {
        using namespace std::chrono_literals;

        env.db().invoke_on_all([] (replica::database& db) {
            db.set_querier_cache_entry_ttl(2s);
        }).get();

        auto [s, pkeys] = test::create_test_table(env, KEYSPACE_NAME, "test_read_all");

        const auto limit = std::numeric_limits<uint64_t>::max();

        const auto slice = s->full_slice();

        for (const auto step : {1ul, pkeys.size() / 4u, pkeys.size() / 2u}) {

            dht::partition_range_vector ranges;
            ranges.push_back(dht::partition_range::make_ending_with({*pkeys.begin(), false}));
            const auto max_r = pkeys.size() - 1;
            for (unsigned r = 0; r < max_r; r += step) {
                ranges.push_back(dht::partition_range::make({pkeys.at(r), true}, {pkeys.at(std::min(max_r, r + step)), false}));
            }
            ranges.push_back(dht::partition_range::make_starting_with({*pkeys.rbegin(), true}));

            unsigned i = 0;

            testlog.debug("Scan with step={}, ranges={}", step, ranges);

            // Keep indent the same to reduce white-space noise
            for (const auto page_size : {1, 4, 8, 19, 100}) {
            for (const auto stateful : {stateful_query::no, stateful_query::yes}) {
                testlog.debug("[scan #{}]: page_size={}, stateful={}", i++, page_size, stateful);

                // First read all partition-by-partition (not paged).
                auto expected_results = read_all_partitions_one_by_one(env.db(), s, pkeys);

                auto results = read_partitions_with_generic_paged_scan<mutation_result_builder>(env.db(), s, page_size, limit, stateful, ranges, slice, [&] (size_t) {
                    tests::require_equal(aggregate_querier_cache_stat(env.db(), &query::querier_cache::stats::drops), 0u);
                }).first;

                check_results_are_equal(expected_results, results);

                tests::require_equal(aggregate_querier_cache_stat(env.db(), &query::querier_cache::stats::drops), 0u);
                tests::require_equal(aggregate_querier_cache_stat(env.db(), &query::querier_cache::stats::time_based_evictions), 0u);
                tests::require_equal(aggregate_querier_cache_stat(env.db(), &query::querier_cache::stats::resource_based_evictions), 0u);
            }}
        }

        require_eventually_empty_caches(env.db());

        return make_ready_future<>();
    }).get();
}

// Best run with SMP>=2
SEASTAR_THREAD_TEST_CASE(test_read_with_partition_row_limits) {
    do_with_cql_env_thread([this] (cql_test_env& env) -> future<> {
        using namespace std::chrono_literals;

        env.db().invoke_on_all([] (replica::database& db) {
            db.set_querier_cache_entry_ttl(2s);
        }).get();

        auto [s, pkeys] = test::create_test_table(env, KEYSPACE_NAME, get_name(), 2, 10);

        unsigned i = 0;

        // Keep indent the same to reduce white-space noise
        for (const auto partition_row_limit : {1ul, 4ul, 8ul, query::partition_max_rows}) {
        for (const auto page_size : {1, 4, 8, 19}) {
        for (const auto stateful : {stateful_query::no, stateful_query::yes}) {
            testlog.debug("[scan #{}]: partition_row_limit={}, page_size={}, stateful={}", i++, partition_row_limit, page_size, stateful);

            const auto slice = partition_slice_builder(*s, s->full_slice())
                .reversed()
                .with_partition_row_limit(partition_row_limit)
                .build();

            // First read all partition-by-partition (not paged).
            auto results1 = read_all_partitions_one_by_one(env.db(), s, pkeys);

            // Then do a paged range-query, with reader caching
            auto results2 = read_all_partitions_with_paged_scan(env.db(), s, 4, stateful_query::yes, [&] (size_t) {
                check_cache_population(env.db(), 1);
                tests::require_equal(aggregate_querier_cache_stat(env.db(), &query::querier_cache::stats::drops), 0u);
                tests::require_equal(aggregate_querier_cache_stat(env.db(), &query::querier_cache::stats::misses), 0u);
            }).first;

            check_results_are_equal(results1, results2);

            tests::require_equal(aggregate_querier_cache_stat(env.db(), &query::querier_cache::stats::drops), 0u);
            tests::require_equal(aggregate_querier_cache_stat(env.db(), &query::querier_cache::stats::misses), 0u);
            tests::require_equal(aggregate_querier_cache_stat(env.db(), &query::querier_cache::stats::time_based_evictions), 0u);
            tests::require_equal(aggregate_querier_cache_stat(env.db(), &query::querier_cache::stats::resource_based_evictions), 0u);
        } } }

        return make_ready_future<>();
    }).get();
}

// Best run with SMP>=2
SEASTAR_THREAD_TEST_CASE(test_evict_a_shard_reader_on_each_page) {
    do_with_cql_env_thread([] (cql_test_env& env) -> future<> {
        using namespace std::chrono_literals;

        env.db().invoke_on_all([] (replica::database& db) {
            db.set_querier_cache_entry_ttl(2s);
        }).get();

        auto [s, pkeys] = test::create_test_table(env, KEYSPACE_NAME, "test_evict_a_shard_reader_on_each_page");

        // First read all partition-by-partition (not paged).
        auto results1 = read_all_partitions_one_by_one(env.db(), s, pkeys);

        // Then do a paged range-query
        auto [results2, npages] = read_all_partitions_with_paged_scan(env.db(), s, 4, stateful_query::yes, [&] (size_t page) {
            check_cache_population(env.db(), 1);

            env.db().invoke_on(page % smp::count, [&] (replica::database& db) {
                return db.get_querier_cache().evict_one();
            }).get();

            tests::require_equal(aggregate_querier_cache_stat(env.db(), &query::querier_cache::stats::drops), 0u);
            tests::require_equal(aggregate_querier_cache_stat(env.db(), &query::querier_cache::stats::misses), page);
        });

        check_results_are_equal(results1, results2);

        tests::require_equal(aggregate_querier_cache_stat(env.db(), &query::querier_cache::stats::drops), 0u);
        tests::require_equal(aggregate_querier_cache_stat(env.db(), &query::querier_cache::stats::time_based_evictions), 0u);
        tests::require_equal(aggregate_querier_cache_stat(env.db(), &query::querier_cache::stats::resource_based_evictions), npages);

        require_eventually_empty_caches(env.db());

        return make_ready_future<>();
    }).get();
}

// Best run with SMP>=2
SEASTAR_THREAD_TEST_CASE(test_read_reversed) {
    do_with_cql_env_thread([this] (cql_test_env& env) -> future<> {
        using namespace std::chrono_literals;

        auto& db = env.db();

        auto [s, pkeys] = test::create_test_table(env, KEYSPACE_NAME, get_name(), 4, 8);

        unsigned i = 0;

        // Keep indent the same to reduce white-space noise
        for (const auto partition_row_limit : {1ul, 4ul, 8ul, query::partition_max_rows}) {
        for (const auto page_size : {1, 4, 8, 19}) {
        for (const auto stateful : {stateful_query::no, stateful_query::yes}) {
            testlog.debug("[scan #{}]: partition_row_limit={}, page_size={}, stateful={}", i++, partition_row_limit, page_size, stateful);

            const auto slice = partition_slice_builder(*s, s->full_slice())
                .reversed()
                .with_partition_row_limit(partition_row_limit)
                .build();

            // First read all partition-by-partition (not paged).
            auto expected_results = read_all_partitions_one_by_one(env.db(), s, pkeys, slice);

            auto [mutation_results, _np1] = read_partitions_with_generic_paged_scan<mutation_result_builder>(db, s, page_size, std::numeric_limits<uint64_t>::max(), stateful,
                    query::full_partition_range, slice);

            check_results_are_equal(expected_results, mutation_results);

            auto [data_results, _np2] = read_partitions_with_generic_paged_scan<data_result_builder>(db, s, page_size, std::numeric_limits<uint64_t>::max(), stateful,
                    query::full_partition_range, slice);

            std::vector<query::result_set_row> expected_rows;
            for (const auto& mut : expected_results) {
                auto rs = query::result_set(mut);
                // mut re-sorts rows into forward order, so we have to reverse them again here
                std::copy(rs.rows().rbegin(), rs.rows().rend(), std::back_inserter(expected_rows));
            }
            auto expected_data_results = query::result_set(s, std::move(expected_rows));

            BOOST_REQUIRE_EQUAL(data_results, expected_data_results);

            tests::require_equal(aggregate_querier_cache_stat(db, &query::querier_cache::stats::drops), 0u);
            tests::require_equal(aggregate_querier_cache_stat(db, &query::querier_cache::stats::time_based_evictions), 0u);
            tests::require_equal(aggregate_querier_cache_stat(db, &query::querier_cache::stats::resource_based_evictions), 0u);

        } } }

        require_eventually_empty_caches(env.db());

        return make_ready_future<>();
    }).get();
}

namespace {

class buffer_ostream {
    size_t _size_remaining;
    bytes _buf;
    bytes::value_type* _pos;

public:
    explicit buffer_ostream(size_t size)
        : _size_remaining(size)
        , _buf(bytes::initialized_later{}, size)
        , _pos(_buf.data()) {
    }
    void write(bytes_view v) {
        if (!_size_remaining) {
            return;
        }
        const auto write_size = std::min(_size_remaining, v.size());
        std::copy_n(v.data(), write_size, _pos);
        _pos += write_size;
        _size_remaining -= write_size;
    }
    void write(const char* ptr, size_t size) {
        write(bytes_view(reinterpret_cast<const bytes_view::value_type*>(ptr), size));
    }
    bytes detach() && {
        return std::move(_buf);
    }
};

template <typename T>
static size_t calculate_serialized_size(const T& v) {
    struct {
        size_t _size = 0;
        void write(bytes_view v) { _size += v.size(); }
        void write(const char*, size_t size) { _size += size; }
    } os;
    ser::serialize(os, v);
    return os._size;
}

struct blob_header {
    uint32_t size;
    bool includes_pk;
    bool has_ck;
    bool includes_ck;
};

} // anonymous namespace

namespace ser {

template <>
struct serializer<blob_header> {
    template <typename Input>
    static blob_header read(Input& in) {
        blob_header head;
        head.size = ser::deserialize(in, boost::type<int>{});
        head.includes_pk = ser::deserialize(in, boost::type<bool>{});
        head.has_ck = ser::deserialize(in, boost::type<bool>{});
        head.includes_ck = ser::deserialize(in, boost::type<bool>{});
        return head;
    }
    template <typename Output>
    static void write(Output& out, blob_header head) {
        ser::serialize(out, head.size);
        ser::serialize(out, head.includes_pk);
        ser::serialize(out, head.has_ck);
        ser::serialize(out, head.includes_ck);
    }
    template <typename Input>
    static void skip(Input& in) {
        ser::skip(in, boost::type<int>{});
        ser::skip(in, boost::type<bool>{});
        ser::skip(in, boost::type<bool>{});
        ser::skip(in, boost::type<bool>{});
    }
};

} // namespace ser

namespace {

const uint32_t min_blob_size = sizeof(blob_header);

static bytes make_payload(const schema& schema, size_t size, const partition_key& pk, const clustering_key* const ck) {
    assert(size >= min_blob_size);

    blob_header head;
    head.size = size;

    size_t size_needed = min_blob_size;

    auto pk_components = pk.explode(schema);
    size_needed += calculate_serialized_size(pk_components);

    head.includes_pk = size_needed <= size;
    head.has_ck = bool(ck);

    auto ck_components = std::vector<bytes>{};
    if (ck) {
        ck_components = ck->explode(schema);
        size_needed += calculate_serialized_size(ck_components);
    }
    head.includes_ck = ck && size_needed <= size;

    auto buf_os = buffer_ostream(size);
    ser::serialize(buf_os, head);
    if (head.includes_pk) {
        ser::serialize(buf_os, pk_components);
    }
    if (ck && head.includes_ck) {
        ser::serialize(buf_os, ck_components);
    }

    return std::move(buf_os).detach();
}

static bool validate_payload(const schema& schema, atomic_cell_value_view payload_view, const partition_key& pk, const clustering_key* const ck) {
    auto istream = fragmented_memory_input_stream(fragment_range(payload_view).begin(), payload_view.size());
    auto head = ser::deserialize(istream, boost::type<blob_header>{});

    const size_t actual_size = payload_view.size();

    if (head.size != actual_size) {
        testlog.error("Validating payload for pk={}, ck={} failed, sizes differ: stored={}, actual={}", pk, seastar::lazy_deref(ck), head.size,
                actual_size);
        return false;
    }

    if (!head.includes_pk) {
        return true;
    }

    auto stored_pk = partition_key::from_exploded(schema, ser::deserialize(istream, boost::type<std::vector<bytes>>{}));
    if (!stored_pk.equal(schema, pk)) {
        testlog.error("Validating payload for pk={}, ck={} failed, pks differ: stored={}, actual={}", pk, seastar::lazy_deref(ck), stored_pk, pk);
        return false;
    }

    if (bool(ck) != head.has_ck) {
        const auto stored = head.has_ck ? "clustering" : "static";
        const auto actual = ck ? "clustering" : "static";
        testlog.error("Validating payload for pk={}, ck={} failed, row types differ: stored={}, actual={}", pk, seastar::lazy_deref(ck), stored, actual);
        return false;
    }

    if (!ck || !head.has_ck || !head.includes_ck) {
        return true;
    }

    auto stored_ck = clustering_key::from_exploded(schema, ser::deserialize(istream, boost::type<std::vector<bytes>>{}));
    if (!stored_ck.equal(schema, *ck)) {
        testlog.error("Validating payload for pk={}, ck={} failed, cks differ: stored={}, actual={}", pk, seastar::lazy_deref(ck), stored_ck, *ck);
        return false;
    }

    return true;
}

template <typename RandomEngine>
static test::population_description create_fuzzy_test_table(cql_test_env& env, RandomEngine& rnd_engine) {
    testlog.info("Generating combinations...");

    const std::optional<std::uniform_int_distribution<int>> static_row_configurations[] = {
        std::nullopt,
        std::uniform_int_distribution<int>(min_blob_size,   100),
        std::uniform_int_distribution<int>(          101, 1'000),
    };

    const std::uniform_int_distribution<int> clustering_row_count_configurations[] = {
        std::uniform_int_distribution<int>( 0,   2),
        std::uniform_int_distribution<int>( 3,  49),
        std::uniform_int_distribution<int>(50, 999),
    };

    const std::uniform_int_distribution<int> clustering_row_configurations[] = {
        std::uniform_int_distribution<int>(     min_blob_size, min_blob_size + 10),
        std::uniform_int_distribution<int>(min_blob_size + 11,                200),
        std::uniform_int_distribution<int>(               201,              1'000),
    };

    // std::pair(count, size)
    const std::pair<std::uniform_int_distribution<int>, std::uniform_int_distribution<int>> range_deletion_configurations[] = {
        std::pair(std::uniform_int_distribution<int>(0, 1), std::uniform_int_distribution<int>(1, 2)),
        std::pair(std::uniform_int_distribution<int>(1, 2), std::uniform_int_distribution<int>(1, 3)),
        std::pair(std::uniform_int_distribution<int>(1, 2), std::uniform_int_distribution<int>(4, 7)),
        std::pair(std::uniform_int_distribution<int>(3, 9), std::uniform_int_distribution<int>(2, 9)),
        std::pair(std::uniform_int_distribution<int>(30, 99), std::uniform_int_distribution<int>(1, 2)),
    };

    std::vector<test::partition_configuration> partition_configs;

    auto count_for = [] (size_t s, size_t cc, size_t cs, size_t d) {
#ifdef DEBUG
        const auto tier1_count = 1;
        const auto tier2_count = 1;
        const auto tier3_count = 0;
#else
        const auto tier1_count = 100;
        const auto tier2_count = 10;
        const auto tier3_count = 2;
#endif
        if (s > 1 || cc > 1 || cs > 1 || d > 1) {
            return tier3_count;
        }
        if (s > 0 || cc > 0 || cs > 0 || d > 0) {
            return tier2_count;
        }
        return tier1_count;
    };

    // Generate (almost) all combinations of the above.
    for (size_t s = 0; s < std::size(static_row_configurations); ++s) {
        for (size_t cc = 0; cc < std::size(clustering_row_count_configurations); ++cc) {
            // We don't want to generate partitions that are too huge.
            // So don't allow loads of large rows. This is achieved by
            // omitting from the last (largest) size configurations when the
            // larger count configurations are reached.
            const size_t omit_from_end = std::max(0, int(cc) - 1);
            for (size_t cs = 0; cs < std::size(clustering_row_configurations) - omit_from_end; ++cs) {
                for (size_t d = 0; d < std::size(range_deletion_configurations); ++d) {
                    const auto count = count_for(s, cc, cs, d);
                    const auto [range_delete_count_config, range_delete_size_config] = range_deletion_configurations[d];
                    partition_configs.push_back(test::partition_configuration{
                            static_row_configurations[s],
                            clustering_row_count_configurations[cc],
                            clustering_row_configurations[cs],
                            range_delete_count_config,
                            range_delete_size_config,
                            count});
                }
            }
        }
    }

    auto config_distribution = std::uniform_int_distribution<int>(0, 1);
    const auto extreme_static_row_dist = std::uniform_int_distribution<int>(1'001, 10'000);
    const auto extreme_clustering_row_count_dist = std::uniform_int_distribution<int>(1'000, 10'000);
    const auto extreme_clustering_row_dist = std::uniform_int_distribution<int>(1'001, 10'000);

    // Extreme cases, just one of each.
    const auto [range_delete_count_config, range_delete_size_config] = range_deletion_configurations[config_distribution(rnd_engine)];
    partition_configs.push_back(test::partition_configuration{
            extreme_static_row_dist,
            clustering_row_count_configurations[config_distribution(rnd_engine)],
            clustering_row_configurations[config_distribution(rnd_engine)],
            range_delete_count_config,
            range_delete_size_config,
            1});
    partition_configs.push_back(test::partition_configuration{
            static_row_configurations[config_distribution(rnd_engine)],
            extreme_clustering_row_count_dist,
            clustering_row_configurations[config_distribution(rnd_engine)],
            range_delete_count_config,
            range_delete_size_config,
            1});
    partition_configs.push_back(test::partition_configuration{
            static_row_configurations[config_distribution(rnd_engine)],
            clustering_row_count_configurations[config_distribution(rnd_engine)],
            extreme_clustering_row_dist,
            range_delete_count_config,
            range_delete_size_config,
            1});

    const auto partition_count = boost::accumulate(partition_configs, size_t(0),
            [] (size_t c, const test::partition_configuration& part_config) { return c + part_config.count; });

    testlog.info("Done. Generated {} combinations, {} partitions in total.", partition_configs.size(), partition_count);

    return test::create_test_table(env, KEYSPACE_NAME, "fuzzy_test", rnd_engine(), std::move(partition_configs), &make_payload);
}

template <typename RandomEngine>
static nonwrapping_range<int> generate_range(RandomEngine& rnd_engine, int start, int end, bool allow_open_ended_start = true) {
    assert(start < end);

    std::uniform_int_distribution<int> defined_bound_dist(0, 7);
    std::uniform_int_distribution<int8_t> inclusive_dist(0, 1);
    std::uniform_int_distribution<int> bound_dist(start, end);

    const auto open_lower_bound = allow_open_ended_start && !defined_bound_dist(rnd_engine);
    const auto open_upper_bound = !defined_bound_dist(rnd_engine);

    if (open_lower_bound || open_upper_bound) {
        const auto bound = bound_dist(rnd_engine);
        if (open_lower_bound) {
            return nonwrapping_range<int>::make_ending_with(
                    nonwrapping_range<int>::bound(bound, inclusive_dist(rnd_engine)));
        }
        return nonwrapping_range<int>::make_starting_with(
                nonwrapping_range<int>::bound(bound, inclusive_dist(rnd_engine)));
    }

    const auto b1 = bound_dist(rnd_engine);
    const auto b2 = bound_dist(rnd_engine);
    if (b1 == b2) {
        return nonwrapping_range<int>::make_starting_with(
                nonwrapping_range<int>::bound(b1, inclusive_dist(rnd_engine)));
    }
    return nonwrapping_range<int>::make(
                nonwrapping_range<int>::bound(std::min(b1, b2), inclusive_dist(rnd_engine)),
                nonwrapping_range<int>::bound(std::max(b1, b2), inclusive_dist(rnd_engine)));
}

template <typename RandomEngine>
static query::clustering_row_ranges
generate_clustering_ranges(RandomEngine& rnd_engine, const schema& schema, const std::vector<test::partition_description>& part_descs) {
    std::vector<clustering_key> all_cks;
    std::set<clustering_key, clustering_key::less_compare> all_cks_sorted{clustering_key::less_compare(schema)};

    for (const auto& part_desc : part_descs) {
        all_cks_sorted.insert(part_desc.live_rows.cbegin(), part_desc.live_rows.cend());
        all_cks_sorted.insert(part_desc.dead_rows.cbegin(), part_desc.dead_rows.cend());
    }
    all_cks.reserve(all_cks_sorted.size());
    all_cks.insert(all_cks.end(), all_cks_sorted.cbegin(), all_cks_sorted.cend());

    query::clustering_row_ranges clustering_key_ranges;

    int start = 0;
    const int end = all_cks.size() - 1;

    do {
        auto clustering_index_range = generate_range(rnd_engine, start, end, start == 0);
        if (clustering_index_range.end()) {
            start = clustering_index_range.end()->value() + clustering_index_range.end()->is_inclusive();
        } else {
            start = end;
        }

        clustering_key_ranges.emplace_back(clustering_index_range.transform([&all_cks] (int i) {
            return all_cks.at(i);
        }));
    } while (start < end);

    return clustering_key_ranges;
}

struct expected_partition {
    const dht::decorated_key& dkey;
    bool has_static_row;
    std::vector<clustering_key> live_rows;
    std::vector<clustering_key> dead_rows;
    query::clustering_row_ranges range_tombstones;
};

static std::vector<expected_partition>
slice_partitions(const schema& schema, const std::vector<test::partition_description>& partitions,
        const nonwrapping_range<int>& partition_index_range, const query::partition_slice& slice) {
    const auto& sb = partition_index_range.start();
    const auto& eb = partition_index_range.end();
    auto it = sb ? partitions.cbegin() + sb->value() + !sb->is_inclusive() : partitions.cbegin();
    const auto end = eb ? partitions.cbegin() + eb->value() + eb->is_inclusive() : partitions.cend();

    const auto tri_cmp = clustering_key::tri_compare(schema);
    const auto& row_ranges = slice.default_row_ranges();

    std::vector<expected_partition> sliced_partitions;
    for (;it != end; ++it) {
        auto sliced_live_rows = test::slice_keys(schema, it->live_rows, row_ranges);
        auto sliced_dead_rows = test::slice_keys(schema, it->dead_rows, row_ranges);

        query::clustering_row_ranges overlapping_range_tombstones;
        std::copy_if(it->range_tombstones.cbegin(), it->range_tombstones.cend(), std::back_inserter(overlapping_range_tombstones),
                [&] (const query::clustering_range& rt) {
                    return std::any_of(row_ranges.cbegin(), row_ranges.cend(), [&] (const query::clustering_range& r) {
                        return rt.overlaps(r, clustering_key::tri_compare(schema));
                    });
                });

        if (sliced_live_rows.empty() && sliced_dead_rows.empty() && overlapping_range_tombstones.empty() && !it->has_static_row) {
            continue;
        }
        sliced_partitions.emplace_back(expected_partition{it->dkey, it->has_static_row, std::move(sliced_live_rows), std::move(sliced_dead_rows),
                std::move(overlapping_range_tombstones)});
    }
    return sliced_partitions;
}

static void
validate_result_size(size_t i, schema_ptr schema, const std::vector<mutation>& results, const std::vector<expected_partition>& expected_partitions) {
    if (results.size() == expected_partitions.size()) {
        return;
    }

    auto expected = std::set<dht::decorated_key, dht::decorated_key::less_comparator>(dht::decorated_key::less_comparator(schema));
    for (const auto& p : expected_partitions) {
       expected.insert(p.dkey);
    }

    auto actual = std::set<dht::decorated_key, dht::decorated_key::less_comparator>(dht::decorated_key::less_comparator(schema));
    for (const auto& m: results) {
       actual.insert(m.decorated_key());
    }

    if (results.size() > expected_partitions.size()) {
        std::vector<dht::decorated_key> diff;
        std::set_difference(actual.cbegin(), actual.cend(), expected.cbegin(), expected.cend(), std::back_inserter(diff),
                dht::decorated_key::less_comparator(schema));
        testlog.error("[scan#{}]: got {} more partitions than expected, extra partitions: {}", i, diff.size(), diff);
        tests::fail(format("Got {} more partitions than expected", diff.size()));
    } else if (results.size() < expected_partitions.size()) {
        std::vector<dht::decorated_key> diff;
        std::set_difference(expected.cbegin(), expected.cend(), actual.cbegin(), actual.cend(), std::back_inserter(diff),
                dht::decorated_key::less_comparator(schema));
        testlog.error("[scan#{}]: got {} less partitions than expected, missing partitions: {}", i, diff.size(), diff);
        tests::fail(format("Got {} less partitions than expected", diff.size()));
    }
}

[[nodiscard]] static bool validate_row(const schema& s, const partition_key& pk, const clustering_key* const ck, column_kind kind, const row& r) {
    bool OK = true;
    const auto& cdef = s.column_at(kind, 0);
    if (auto* cell = r.find_cell(0)) {
        OK &= tests::check(validate_payload(s, cell->as_atomic_cell(cdef).value(), pk, ck));
    }
    return OK;
}

[[nodiscard]] static bool validate_static_row(const schema& s, const partition_key& pk, const row& sr) {
    return validate_row(s, pk, {}, column_kind::static_column, sr);
}

[[nodiscard]] static bool validate_regular_row(const schema& s, const partition_key& pk, const clustering_key& ck, const deletable_row& dr) {
    return validate_row(s, pk, &ck, column_kind::regular_column, dr.cells());
}

struct pkey_with_schema {
    const dht::decorated_key& key;
    const schema& s;
    bool operator==(const pkey_with_schema& pk) const {
        return key.equal(s, pk.key);
    }
};

static std::ostream& operator<<(std::ostream& os, const pkey_with_schema& pk) {
    os << pk.key;
    return os;
}

struct ckey_with_schema {
    const clustering_key& key;
    const schema& s;
    bool operator==(const ckey_with_schema& ck) const {
        return key.equal(s, ck.key);
    }
};

static std::ostream& operator<<(std::ostream& os, const ckey_with_schema& ck) {
    os << ck.key;
    return os;
}

struct with_schema_wrapper {
    const schema& s;

    pkey_with_schema operator()(const dht::decorated_key& pkey) const {
        return pkey_with_schema{pkey, s};
    }
    ckey_with_schema operator()(const clustering_key& ckey) const {
        return ckey_with_schema{ckey, s};
    }
};

static void validate_result(size_t i, const mutation& result_mut, const expected_partition& expected_part) {
    testlog.trace("[scan#{}]: validating {}: has_static_row={}, live_rows={}, dead_rows={}", i, expected_part.dkey, expected_part.has_static_row,
            expected_part.live_rows, expected_part.dead_rows);

    auto& schema = *result_mut.schema();
    const auto wrapper = with_schema_wrapper{schema};

    bool OK = true;

    tests::require_equal(result_mut.partition().static_row().empty(), !expected_part.has_static_row);
    OK &= validate_static_row(schema, expected_part.dkey.key(), result_mut.partition().static_row().get());

    const auto& res_rows = result_mut.partition().clustered_rows();
    auto res_it = res_rows.begin();
    const auto res_end = res_rows.end();

    auto exp_live_it = expected_part.live_rows.cbegin();
    const auto exp_live_end = expected_part.live_rows.cend();

    auto exp_dead_it = expected_part.dead_rows.cbegin();
    const auto exp_dead_end = expected_part.dead_rows.cend();

    for (; res_it != res_end && (exp_live_it != exp_live_end || exp_dead_it != exp_dead_end); ++res_it) {
        const bool is_live = res_it->row().is_live(schema);

        // Check that we have remaining expected rows of the respective liveness.
        if (is_live) {
            tests::require(exp_live_it != exp_live_end);
        } else {
            tests::require(exp_dead_it != exp_dead_end);
        }

        testlog.trace("[scan#{}]: validating {}/{}: is_live={}", i, expected_part.dkey, res_it->key(), is_live);

        if (is_live) {
            OK &= tests::check_equal(wrapper(res_it->key()), wrapper(*exp_live_it++));
        } else {
            // FIXME: Only a fraction of the dead rows is present in the result.
            if (!res_it->key().equal(schema, *exp_dead_it)) {
                testlog.trace("[scan#{}]: validating {}/{}: dead row in the result is not the expected one: {} != {}", i, expected_part.dkey,
                        res_it->key(), res_it->key(), *exp_dead_it);
            }

            // The dead row is not the one we expected it to be. Check that at
            // least that it *is* among the expected dead rows.
            if (!res_it->key().equal(schema, *exp_dead_it)) {
                auto it = std::find_if(exp_dead_it, exp_dead_end, [&] (const clustering_key& key) {
                    return key.equal(schema, res_it->key());
                });
                OK &= tests::check(it != exp_dead_it);

                testlog.trace("[scan#{}]: validating {}/{}: skipped over {} expected dead rows", i, expected_part.dkey,
                        res_it->key(), std::distance(exp_dead_it, it));
                exp_dead_it = it;
            }
            ++exp_dead_it;
        }
        OK &= validate_regular_row(schema, expected_part.dkey.key(), res_it->key(), res_it->row());
    }

    // We don't want to call res_rows.calculate_size() as it has linear complexity.
    // Instead, check that after iterating through the results and expected
    // results in lock-step, both have reached the end.
    OK &= tests::check(res_it == res_end);
    if (res_it != res_end) {
        testlog.error("[scan#{}]: validating {} failed: result contains unexpected trailing rows: {}", i, expected_part.dkey,
                boost::copy_range<std::vector<clustering_key>>(
                        boost::iterator_range<mutation_partition::rows_type::const_iterator>(res_it, res_end)
                        | boost::adaptors::transformed([] (const rows_entry& e) { return e.key(); })));
    }

    OK &= tests::check(exp_live_it == exp_live_end);
    if (exp_live_it != exp_live_end) {
        testlog.error("[scan#{}]: validating {} failed: {} expected live rows missing from result", i, expected_part.dkey,
                std::distance(exp_live_it, exp_live_end));
    }

    // FIXME: see note about dead rows above.
    if (exp_dead_it != exp_dead_end) {
        testlog.trace("[scan#{}]: validating {}: {} expected dead rows missing from result", i, expected_part.dkey,
                std::distance(exp_dead_it, exp_dead_end));
    }

    tests::require(OK);
}

struct fuzzy_test_config {
    uint32_t seed;
    std::chrono::seconds timeout;
    unsigned concurrency;
    unsigned scans;
};

static void
run_fuzzy_test_scan(size_t i, fuzzy_test_config cfg, distributed<replica::database>& db, schema_ptr schema,
        const std::vector<test::partition_description>& part_descs) {
    const auto seed = cfg.seed + (i + 1) * this_shard_id();
    auto rnd_engine = std::mt19937(seed);

    auto partition_index_range = generate_range(rnd_engine, 0, part_descs.size() - 1);
    auto partition_range = partition_index_range.transform([&part_descs] (int i) {
        return dht::ring_position(part_descs[i].dkey);
    });

    const auto partition_slice = partition_slice_builder(*schema)
        .with_ranges(generate_clustering_ranges(rnd_engine, *schema, part_descs))
        .with_option<query::partition_slice::option::allow_short_read>()
        .build();

    const auto is_stateful = stateful_query(std::uniform_int_distribution<int>(0, 3)(rnd_engine));

    testlog.debug("[scan#{}]: seed={}, is_stateful={}, prange={}, ckranges={}", i, seed, is_stateful, partition_range,
            partition_slice.default_row_ranges());

    const auto [results, npages] = read_partitions_with_paged_scan(db, schema, 1000, 1024, is_stateful, partition_range, partition_slice);

    const auto expected_partitions = slice_partitions(*schema, part_descs, partition_index_range, partition_slice);

    validate_result_size(i, schema, results, expected_partitions);

    const auto wrapper = with_schema_wrapper{*schema};
    auto exp_it = expected_partitions.cbegin();
    auto res_it = results.cbegin();
    while (res_it != results.cend() && exp_it != expected_partitions.cend()) {
        tests::require_equal(wrapper(res_it->decorated_key()), wrapper(exp_it->dkey));
        validate_result(i, *res_it++, *exp_it++);
    }

    testlog.trace("[scan#{}]: validated all partitions, both the expected and actual partition list should be exhausted now", i);
    tests::require(res_it == results.cend() && exp_it == expected_partitions.cend());
}

future<> run_concurrently(size_t count, size_t concurrency, noncopyable_function<future<>(size_t)> func) {
    return do_with(std::move(func), gate(), semaphore(concurrency), std::exception_ptr(),
            [count] (noncopyable_function<future<>(size_t)>& func, gate& gate, semaphore& sem, std::exception_ptr& error) {
        for (size_t i = 0; i < count; ++i) {
            // Future is waited on indirectly (via `gate`).
            (void)with_gate(gate, [&func, &sem, &error, i] {
                return with_semaphore(sem, 1, [&func, &error, i] {
                    if (error) {
                        testlog.trace("Skipping func({}) due to previous func() returning with error", i);
                        return make_ready_future<>();
                    }
                    testlog.trace("Invoking func({})", i);
                    auto f = func(i).handle_exception([&error, i] (std::exception_ptr e) {
                        testlog.debug("func({}) invocation returned with error: {}", i, e);
                        error = std::move(e);
                    });
                    return f;
                });
            });
        }
        return gate.close().then([&error] {
            if (error) {
                testlog.trace("Run failed, returning with error: {}", error);
                return make_exception_future<>(std::move(error));
            }
            testlog.trace("Run succeeded");
            return make_ready_future<>();
        });
    });
}

static future<>
run_fuzzy_test_workload(fuzzy_test_config cfg, distributed<replica::database>& db, schema_ptr schema,
        const std::vector<test::partition_description>& part_descs) {
    return run_concurrently(cfg.scans, cfg.concurrency, [cfg, &db, schema = std::move(schema), &part_descs] (size_t i) {
        return seastar::async([i, cfg, &db, schema, &part_descs] () mutable {
            run_fuzzy_test_scan(i, cfg, db, std::move(schema), part_descs);
        });
    });
}

} // namespace

SEASTAR_THREAD_TEST_CASE(fuzzy_test) {
    auto db_cfg = make_shared<db::config>();
    db_cfg->enable_commitlog(false);

    do_with_cql_env_thread([] (cql_test_env& env) -> future<> {
        // REPLACE RANDOM SEED HERE.
        const auto seed = tests::random::get_int<uint32_t>();
        testlog.info("fuzzy test seed: {}", seed);

        auto rnd_engine = std::mt19937(seed);

        auto pop_desc = create_fuzzy_test_table(env, rnd_engine);

#if defined DEBUG
        auto cfg = fuzzy_test_config{seed, std::chrono::seconds{8}, 1, 1};
#elif defined DEVEL
        auto cfg = fuzzy_test_config{seed, std::chrono::seconds{2}, 8, 4};
#else
        auto cfg = fuzzy_test_config{seed, std::chrono::seconds{2}, 16, 256};
#endif

        testlog.info("Running test workload with configuration: seed={}, timeout={}s, concurrency={}, scans={}", cfg.seed, cfg.timeout.count(),
                cfg.concurrency, cfg.scans);

        const auto& partitions = pop_desc.partitions;
        smp::invoke_on_all([cfg, db = &env.db(), gs = global_schema_ptr(pop_desc.schema), &partitions] {
            return run_fuzzy_test_workload(cfg, *db, gs.get(), partitions);
        }).handle_exception([seed] (std::exception_ptr e) {
            testlog.error("Test workload failed with exception {}."
                    " To repeat this particular run, replace the random seed of the test, with that of this run ({})."
                    " Look for `REPLACE RANDOM SEED HERE` in the source of the test.",
                    e,
                    seed);
            // Fail the test on any exception.
            BOOST_FAIL("Test run finished with exception");
        }).get();

        return make_ready_future<>();
    }, db_cfg).get();
}
