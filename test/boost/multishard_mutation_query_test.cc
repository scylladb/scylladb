/*
 * Copyright (C) 2018-present ScyllaDB
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
#include "db/config.hh"
#include "partition_slice_builder.hh"
#include "serializer_impl.hh"
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
    testlog.info("{}() called from {}() {}:{:d}", __FUNCTION__, sl.function_name(), sl.file_name(), sl.line());

    parallel_for_each(boost::irange(0u, smp::count), [queriers, &db] (unsigned shard) {
        return db.invoke_on(shard, [queriers] (database& local_db) {
            auto& stats = local_db.get_querier_cache_stats();
            tests::require_equal(stats.population, queriers);
        });
    }).get0();
}

static void require_eventually_empty_caches(distributed<database>& db,
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

        env.db().invoke_on_all([] (database& db) {
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

static std::vector<mutation> read_all_partitions_one_by_one(distributed<database>& db, schema_ptr s, std::vector<dht::decorated_key> pkeys) {
    const auto& sharder = s->get_sharder();
    std::vector<mutation> results;
    results.reserve(pkeys.size());

    for (const auto& pkey : pkeys) {
        const auto res = db.invoke_on(sharder.shard_of(pkey.token()), [gs = global_schema_ptr(s), &pkey] (database& db) {
            return async([s = gs.get(), &pkey, &db] () mutable {
                const auto cmd = query::read_command(s->id(), s->version(), s->full_slice(),
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

using stateful_query = bool_class<class stateful>;

static std::pair<std::vector<mutation>, size_t>
read_partitions_with_paged_scan(distributed<database>& db, schema_ptr s, uint32_t page_size, uint64_t max_size, stateful_query is_stateful,
        const dht::partition_range& range, const query::partition_slice& slice, const std::function<void(size_t)>& page_hook = {}) {
    const auto query_uuid = is_stateful ? utils::make_random_uuid() : utils::UUID{};
    std::vector<mutation> results;
    auto cmd = query::read_command(s->id(), s->version(), slice, page_size, gc_clock::now(), std::nullopt, query::max_partitions, query_uuid,
            query::is_first_page::yes, query::max_result_size(max_size), 0);

    bool has_more = true;

    // First page is special, needs to have `is_first_page` set.
    {
        auto res = std::get<0>(query_mutations_on_all_shards(db, s, cmd, {range}, nullptr, db::no_timeout).get0());
        for (auto& part : res->partitions()) {
            auto mut = part.mut().unfreeze(s);
            results.emplace_back(std::move(mut));
        }
        cmd.is_first_page = query::is_first_page::no;
        has_more = !res->partitions().empty();
    }

    if (!has_more) {
        return std::pair(results, 1);
    }

    unsigned npages = 0;

    const auto last_ckey_of = [] (const mutation& mut) -> std::optional<clustering_key> {
        if (mut.partition().clustered_rows().empty()) {
            return std::nullopt;
        }
        return mut.partition().clustered_rows().rbegin()->key();
    };

    auto last_pkey = results.back().decorated_key();
    auto last_ckey = last_ckey_of(results.back());

    // Rest of the pages. Loop until an empty page turns up. Not very
    // sophisticated but simple and safe.
    while (has_more) {
        if (page_hook) {
            page_hook(npages);
        }

        ++npages;

        auto pkrange = dht::partition_range(dht::partition_range::bound(last_pkey, last_ckey.has_value()), range.end());

        if (last_ckey) {
            auto ckranges = cmd.slice.default_row_ranges();
            query::trim_clustering_row_ranges_to(*s, ckranges, *last_ckey);
            cmd.slice.clear_range(*s, last_pkey.key());
            cmd.slice.clear_ranges();
            cmd.slice.set_range(*s, last_pkey.key(), std::move(ckranges));
        }

        auto res = std::get<0>(query_mutations_on_all_shards(db, s, cmd, {pkrange}, nullptr, db::no_timeout).get0());

        if (is_stateful) {
            tests::require(aggregate_querier_cache_stat(db, &query::querier_cache::stats::lookups) >= npages);
        }

        if (!res->partitions().empty()) {
            auto it = res->partitions().begin();
            auto end = res->partitions().end();
            auto first_mut = it->mut().unfreeze(s);

            last_pkey = first_mut.decorated_key();
            last_ckey = last_ckey_of(first_mut);

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
                last_pkey = mut.decorated_key();
                last_ckey = last_ckey_of(mut);
                results.emplace_back(std::move(mut));
            }
        }

        has_more = !res->partitions().empty();
    }

    return std::pair(results, npages);
}

static std::pair<std::vector<mutation>, size_t>
read_all_partitions_with_paged_scan(distributed<database>& db, schema_ptr s, uint32_t page_size, stateful_query is_stateful,
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

        env.db().invoke_on_all([] (database& db) {
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
SEASTAR_THREAD_TEST_CASE(test_evict_a_shard_reader_on_each_page) {
    do_with_cql_env_thread([] (cql_test_env& env) -> future<> {
        using namespace std::chrono_literals;

        env.db().invoke_on_all([] (database& db) {
            db.set_querier_cache_entry_ttl(2s);
        }).get();

        auto [s, pkeys] = test::create_test_table(env, KEYSPACE_NAME, "test_evict_a_shard_reader_on_each_page");

        // First read all partition-by-partition (not paged).
        auto results1 = read_all_partitions_one_by_one(env.db(), s, pkeys);

        // Then do a paged range-query
        auto [results2, npages] = read_all_partitions_with_paged_scan(env.db(), s, 4, stateful_query::yes, [&] (size_t page) {
            check_cache_population(env.db(), 1);

            env.db().invoke_on(page % smp::count, [&] (database& db) {
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

static void validate_row(const schema& s, const partition_key& pk, const clustering_key* const ck, column_kind kind, const row& r) {
    const auto& cdef = s.column_at(kind, 0);
    if (auto* cell = r.find_cell(0)) {
        tests::check(validate_payload(s, cell->as_atomic_cell(cdef).value(), pk, ck));
    }
}

static void validate_static_row(const schema& s, const partition_key& pk, const row& sr) {
    validate_row(s, pk, {}, column_kind::static_column, sr);
}

static void validate_regular_row(const schema& s, const partition_key& pk, const clustering_key& ck, const deletable_row& dr) {
    validate_row(s, pk, &ck, column_kind::regular_column, dr.cells());
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

    tests::require_equal(result_mut.partition().static_row().empty(), !expected_part.has_static_row);
    validate_static_row(schema, expected_part.dkey.key(), result_mut.partition().static_row().get());

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
            tests::check_equal(wrapper(res_it->key()), wrapper(*exp_live_it++));
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
                tests::check(it != exp_dead_it);

                testlog.trace("[scan#{}]: validating {}/{}: skipped over {} expected dead rows", i, expected_part.dkey,
                        res_it->key(), std::distance(exp_dead_it, it));
                exp_dead_it = it;
            }
            ++exp_dead_it;
        }
        validate_regular_row(schema, expected_part.dkey.key(), res_it->key(), res_it->row());
    }

    // We don't want to call res_rows.calculate_size() as it has linear complexity.
    // Instead, check that after iterating through the results and expected
    // results in lock-step, both have reached the end.
    tests::check(res_it == res_end);
    if (res_it != res_end) {
        testlog.error("[scan#{}]: validating {} failed: result contains unexpected trailing rows: {}", i, expected_part.dkey,
                boost::copy_range<std::vector<clustering_key>>(
                        boost::iterator_range<mutation_partition::rows_type::const_iterator>(res_it, res_end)
                        | boost::adaptors::transformed([] (const rows_entry& e) { return e.key(); })));
    }

    tests::check(exp_live_it == exp_live_end);
    if (exp_live_it != exp_live_end) {
        testlog.error("[scan#{}]: validating {} failed: {} expected live rows missing from result", i, expected_part.dkey,
                std::distance(exp_live_it, exp_live_end));
    }

    // FIXME: see note about dead rows above.
    if (exp_dead_it != exp_dead_end) {
        testlog.trace("[scan#{}]: validating {}: {} expected dead rows missing from result", i, expected_part.dkey,
                std::distance(exp_dead_it, exp_dead_end));
    }
}

struct fuzzy_test_config {
    uint32_t seed;
    std::chrono::seconds timeout;
    unsigned concurrency;
    unsigned scans;
};

static void
run_fuzzy_test_scan(size_t i, fuzzy_test_config cfg, distributed<database>& db, schema_ptr schema,
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
run_fuzzy_test_workload(fuzzy_test_config cfg, distributed<database>& db, schema_ptr schema,
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
