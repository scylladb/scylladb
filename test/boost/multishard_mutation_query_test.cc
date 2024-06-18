/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "utils/assert.hh"
#include "multishard_mutation_query.hh"
#include "schema/schema_registry.hh"
#include "db/config.hh"
#include "db/extensions.hh"
#include "partition_slice_builder.hh"
#include "serializer_impl.hh"
#include "query-result-set.hh"
#include "mutation_query.hh"
#include "test/lib/cql_test_env.hh"
#include "test/lib/eventually.hh"
#include "test/lib/mutation_assertions.hh"
#include "test/lib/log.hh"
#include "test/lib/test_utils.hh"
#include "test/lib/random_utils.hh"
#include "test/lib/random_schema.hh"
#include "tombstone_gc_extension.hh"
#include "db/tags/extension.hh"
#include "cdc/cdc_extension.hh"
#include "db/paxos_grace_seconds_extension.hh"
#include "db/per_partition_rate_limit_extension.hh"

#include "test/lib/scylla_test_case.hh"

#include <fmt/ranges.h>
#include <boost/range/algorithm/sort.hpp>
#include <utility>

const sstring KEYSPACE_NAME = "ks";

namespace {

static cql_test_config cql_config_with_extensions() {
    auto ext = std::make_shared<db::extensions>();
    ext->add_schema_extension<db::tags_extension>(db::tags_extension::NAME);
    ext->add_schema_extension<cdc::cdc_extension>(cdc::cdc_extension::NAME);
    ext->add_schema_extension<db::paxos_grace_seconds_extension>(db::paxos_grace_seconds_extension::NAME);
    ext->add_schema_extension<tombstone_gc_extension>(tombstone_gc_extension::NAME);
    ext->add_schema_extension<db::per_partition_rate_limit_extension>(db::per_partition_rate_limit_extension::NAME);

    auto cfg = seastar::make_shared<db::config>(ext);
    return cql_test_config(cfg);
}

struct generated_table {
    schema_ptr schema;
    std::vector<dht::decorated_key> keys;
    std::vector<frozen_mutation> compacted_frozen_mutations;
};

class random_schema_specification : public tests::random_schema_specification {
    sstring _table_name;
    std::unique_ptr<tests::random_schema_specification> _underlying_spec;
public:
    random_schema_specification(sstring ks_name, sstring table_name, bool force_clustering_column)
        : tests::random_schema_specification(std::move(ks_name))
        , _table_name(std::move(table_name))
        , _underlying_spec(tests::make_random_schema_specification(
                keyspace_name(),
                std::uniform_int_distribution<size_t>(1, 4),
                std::uniform_int_distribution<size_t>(size_t(force_clustering_column), 4),
                std::uniform_int_distribution<size_t>(1, 4),
                std::uniform_int_distribution<size_t>(0, 4)))
    { }
    virtual sstring table_name(std::mt19937& engine) override { return _table_name; }
    virtual sstring udt_name(std::mt19937& engine) override { return _underlying_spec->udt_name(engine); }
    virtual std::vector<data_type> partition_key_columns(std::mt19937& engine) override { return _underlying_spec->partition_key_columns(engine); }
    virtual std::vector<data_type> clustering_key_columns(std::mt19937& engine) override { return _underlying_spec->clustering_key_columns(engine); }
    virtual std::vector<data_type> regular_columns(std::mt19937& engine) override { return _underlying_spec->regular_columns(engine); }
    virtual std::vector<data_type> static_columns(std::mt19937& engine) override { return _underlying_spec->static_columns(engine); }
};

} // anonymous namespace

static generated_table create_test_table(
        cql_test_env& env,
        uint32_t seed,
        sstring ks_name,
        sstring tbl_name,
        bool force_clustering_column,
        std::uniform_int_distribution<size_t> partitions,
        std::uniform_int_distribution<size_t> clustering_rows,
        std::uniform_int_distribution<size_t> range_tombstones,
        tests::timestamp_generator ts_gen) {
    auto random_schema_spec = std::make_unique<random_schema_specification>(ks_name, tbl_name, force_clustering_column);
    auto random_schema = tests::random_schema(seed, *random_schema_spec);

    testlog.info("\n{}", random_schema.cql());

    random_schema.create_with_cql(env).get();

    const auto mutations = tests::generate_random_mutations(
            seed,
            random_schema,
            ts_gen,
            tests::no_expiry_expiry_generator(),
            partitions,
            clustering_rows,
            range_tombstones).get();

    auto schema = random_schema.schema();

    std::vector<dht::decorated_key> keys;
    std::vector<frozen_mutation> compacted_frozen_mutations;
    keys.reserve(mutations.size());
    compacted_frozen_mutations.reserve(mutations.size());
    {
        gate write_gate;
        for (const auto& mut : mutations) {
            keys.emplace_back(mut.decorated_key());
            compacted_frozen_mutations.emplace_back(freeze(mut.compacted()));
            (void)with_gate(write_gate, [&] {
                return smp::submit_to(dht::static_shard_of(*schema, mut.decorated_key().token()), [&env, gs = global_schema_ptr(schema), mut = freeze(mut)] () mutable {
                    return env.local_db().apply(gs.get(), std::move(mut), {}, db::commitlog_force_sync::no, db::no_timeout);
                });
            });
            thread::maybe_yield();
        }
        write_gate.close().get();
    }

    return {random_schema.schema(), keys, compacted_frozen_mutations};
}

api::timestamp_type no_tombstone_timestamp_generator(std::mt19937& engine, tests::timestamp_destination destination, api::timestamp_type min_timestamp) {
    switch (destination) {
        case tests::timestamp_destination::partition_tombstone:
        case tests::timestamp_destination::row_tombstone:
        case tests::timestamp_destination::collection_tombstone:
        case tests::timestamp_destination::range_tombstone:
            return api::missing_timestamp;
        default:
            return std::uniform_int_distribution<api::timestamp_type>(min_timestamp, api::max_timestamp)(engine);
    }
}

static std::pair<schema_ptr, std::vector<dht::decorated_key>> create_test_table(cql_test_env& env, sstring ks_name,
        sstring tbl_name, int partition_count = 10 * smp::count, int row_per_partition_count = 10) {
    auto res = create_test_table(
            env,
            tests::random::get_int<uint32_t>(),
            std::move(ks_name),
            std::move(tbl_name),
            true,
            std::uniform_int_distribution<size_t>(partition_count, partition_count),
            std::uniform_int_distribution<size_t>(row_per_partition_count, row_per_partition_count),
            std::uniform_int_distribution<size_t>(0, 0),
            no_tombstone_timestamp_generator);
    return {std::move(res.schema), std::move(res.keys)};
}

static uint64_t aggregate_querier_cache_stat(distributed<replica::database>& db, uint64_t query::querier_cache::stats::*stat) {
    return map_reduce(boost::irange(0u, smp::count), [stat, &db] (unsigned shard) {
        return db.invoke_on(shard, [stat] (replica::database& local_db) {
            auto& stats = local_db.get_querier_cache_stats();
            return stats.*stat;
        });
    }, 0, std::plus<size_t>()).get();
}

static void check_cache_population(distributed<replica::database>& db, size_t queriers,
        seastar::compat::source_location sl = seastar::compat::source_location::current()) {
    testlog.info("{}() called from {}() {}:{:d}", __FUNCTION__, sl.function_name(), sl.file_name(), sl.line());

    parallel_for_each(boost::irange(0u, smp::count), [queriers, &db] (unsigned shard) {
        return db.invoke_on(shard, [queriers] (replica::database& local_db) {
            auto& stats = local_db.get_querier_cache_stats();
            tests::require_equal(stats.population, queriers);
        });
    }).get();
}

static void require_eventually_empty_caches(distributed<replica::database>& db,
        seastar::compat::source_location sl = seastar::compat::source_location::current()) {
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

        auto [s, _] = create_test_table(env, KEYSPACE_NAME, get_name());
        (void)_;

        auto cmd = query::read_command(
                s->id(),
                s->version(),
                s->full_slice(),
                query::max_result_size(query::result_memory_limiter::unlimited_result_size),
                query::tombstone_limit::max,
                query::row_limit(7),
                query::partition_limit::max,
                gc_clock::now(),
                std::nullopt,
                query_id::create_random_id(),
                query::is_first_page::yes);

        query_mutations_on_all_shards(env.db(), s, cmd, {query::full_partition_range}, nullptr, db::no_timeout).get();

        require_eventually_empty_caches(env.db());

        return make_ready_future<>();
    }, cql_config_with_extensions()).get();
}

static std::vector<mutation> read_all_partitions_one_by_one(distributed<replica::database>& db, schema_ptr s, std::vector<dht::decorated_key> pkeys,
        const query::partition_slice& slice) {
    const auto& sharder = s->get_sharder();
    std::vector<mutation> results;
    results.reserve(pkeys.size());

    for (const auto& pkey : pkeys) {
        const auto res = db.invoke_on(sharder.shard_for_reads(pkey.token()), [gs = global_schema_ptr(s), &pkey, &slice] (replica::database& db) {
            return async([s = gs.get(), &pkey, &slice, &db] () mutable {
                const auto cmd = query::read_command(s->id(), s->version(), slice,
                        query::max_result_size(query::result_memory_limiter::unlimited_result_size), query::tombstone_limit::max);
                const auto range = dht::partition_range::make_singular(pkey);
                return make_foreign(std::make_unique<reconcilable_result>(
                    std::get<0>(db.query_mutations(std::move(s), cmd, range, nullptr, db::no_timeout).get())));
            });
        }).get();

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
    const auto query_uuid = is_stateful ? query_id::create_random_id() : query_id::create_null_id();
    ResultBuilder res_builder(s, slice, page_size);
    auto cmd = query::read_command(
            s->id(),
            s->version(),
            slice,
            query::max_result_size(max_size),
            query::tombstone_limit::max,
            query::row_limit(page_size),
            query::partition_limit::max,
            gc_clock::now(),
            std::nullopt,
            query_uuid,
            query::is_first_page::yes);

    bool has_more = true;

    auto ranges = std::make_unique<dht::partition_range_vector>(original_ranges);

    // First page is special, needs to have `is_first_page` set.
    {
        auto res = ResultBuilder::query(db, s, cmd, *ranges, nullptr, db::no_timeout);
        has_more = res_builder.add(*res);
        cmd.is_first_page = query::is_first_page::no;
        if (page_hook && has_more) {
            page_hook(0);
        }
    }

    if (!has_more) {
        return std::pair(std::move(res_builder).get_end_result(), 1);
    }


    const auto cmp = dht::ring_position_comparator(*s);

    unsigned npages = 1;

    // Rest of the pages. Loop until an empty page turns up. Not very
    // sophisticated but simple and safe.
    while (has_more) {
        // Force freeing the vector to avoid hiding any bugs related to storing
        // references to the ranges vector (which is not alive between pages in
        // real life).
        ranges = std::make_unique<dht::partition_range_vector>(original_ranges);

        while (!ranges->front().contains(res_builder.last_pkey(), cmp)) {
            ranges->erase(ranges->begin());
        }
        SCYLLA_ASSERT(!ranges->empty());

        const auto pkrange_begin_inclusive = res_builder.last_ckey() && res_builder.last_pkey_rows() < slice.partition_row_limit();

        auto range_opt = ranges->front().trim_front(dht::partition_range::bound(res_builder.last_pkey(), pkrange_begin_inclusive), dht::ring_position_comparator(*s));
        if (range_opt) {
            ranges->front() = std::move(*range_opt);
        } else {
            ranges->erase(ranges->begin());
            if (ranges->empty()) {
                break;
            }
        }

        if (res_builder.last_ckey()) {
            auto ckranges = cmd.slice.default_row_ranges();
            query::trim_clustering_row_ranges_to(*s, ckranges, *res_builder.last_ckey());
            cmd.slice.clear_range(*s, res_builder.last_pkey().key());
            cmd.slice.clear_ranges();
            cmd.slice.set_range(*s, res_builder.last_pkey().key(), std::move(ckranges));
        }

        auto res = ResultBuilder::query(db, s, cmd, *ranges, nullptr, db::no_timeout);

        if (is_stateful) {
            tests::require(aggregate_querier_cache_stat(db, &query::querier_cache::stats::lookups) >= npages);
        }

        has_more = res_builder.add(*res);
        if (has_more) {
            if (page_hook) {
                page_hook(npages);
            }
            npages++;
        }
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
    uint64_t _page_size = 0;
    std::vector<mutation> _results;
    std::optional<dht::decorated_key> _last_pkey;
    std::optional<clustering_key> _last_ckey;
    uint64_t _last_pkey_rows = 0;

private:
    std::optional<clustering_key> last_ckey_of(const mutation& mut) const {
        if (mut.partition().clustered_rows().empty()) {
            return std::nullopt;
        }

        return mut.partition().clustered_rows().rbegin()->key();
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

    explicit mutation_result_builder(schema_ptr s, const query::partition_slice&, uint64_t page_size) : _s(std::move(s)), _page_size(page_size) { }

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

        return res.is_short_read() || res.row_count() >= _page_size;
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
    uint64_t _page_size = 0;
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

    explicit data_result_builder(schema_ptr s, const query::partition_slice& slice, uint64_t page_size) : _s(std::move(s)), _slice(slice), _page_size(page_size) { }

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
        return raw_res.is_short_read() || res.rows().size() >= _page_size;
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

        auto [s, pkeys] = create_test_table(env, KEYSPACE_NAME, get_name());

        // First read all partition-by-partition (not paged).
        auto results1 = read_all_partitions_one_by_one(env.db(), s, pkeys);

        uint64_t lookups = 0;
        uint64_t misses = 0;
        auto saved_readers = aggregate_querier_cache_stat(env.db(), &query::querier_cache::stats::population);

        // Then do a paged range-query, with reader caching
        auto results2 = read_all_partitions_with_paged_scan(env.db(), s, 4, stateful_query::yes, [&] (size_t page) {
            const auto new_lookups = aggregate_querier_cache_stat(env.db(), &query::querier_cache::stats::lookups);
            const auto new_misses = aggregate_querier_cache_stat(env.db(), &query::querier_cache::stats::misses);

            if (page) {
                tests::require(new_lookups > lookups);
            }

            tests::require_equal(aggregate_querier_cache_stat(env.db(), &query::querier_cache::stats::drops), 0u);
            tests::require_less_equal(new_misses - misses, smp::count - saved_readers);

            lookups = new_lookups;
            misses = new_misses;
            saved_readers = aggregate_querier_cache_stat(env.db(), &query::querier_cache::stats::population);
            tests::require_greater_equal(saved_readers, 1u);
        }).first;

        check_results_are_equal(results1, results2);

        tests::require_equal(aggregate_querier_cache_stat(env.db(), &query::querier_cache::stats::drops), 0u);
        tests::require_equal(aggregate_querier_cache_stat(env.db(), &query::querier_cache::stats::time_based_evictions), 0u);
        tests::require_equal(aggregate_querier_cache_stat(env.db(), &query::querier_cache::stats::resource_based_evictions), 0u);

        require_eventually_empty_caches(env.db());

        // Then do a paged range-query, without reader caching
        auto results3 = read_all_partitions_with_paged_scan(env.db(), s, 4, stateful_query::no, [&] (size_t) {
            check_cache_population(env.db(), 0);
        }).first;

        check_results_are_equal(results1, results3);

        return make_ready_future<>();
    }, cql_config_with_extensions()).get();
}

// Best run with SMP>=2
SEASTAR_THREAD_TEST_CASE(test_read_all_multi_range) {
    do_with_cql_env_thread([] (cql_test_env& env) -> future<> {
        using namespace std::chrono_literals;

        env.db().invoke_on_all([] (replica::database& db) {
            db.set_querier_cache_entry_ttl(2s);
        }).get();

        auto [s, pkeys] = create_test_table(env, KEYSPACE_NAME, get_name());

        const auto limit = std::numeric_limits<uint64_t>::max();

        const auto slice = s->full_slice();

        testlog.info("pkeys.size()={}", pkeys.size());

        for (const auto step : {1ul, pkeys.size() / 4u, pkeys.size() / 2u}) {
            if (!step) {
                continue;
            }

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
    }, cql_config_with_extensions()).get();
}

// Best run with SMP>=2
SEASTAR_THREAD_TEST_CASE(test_read_with_partition_row_limits) {
    do_with_cql_env_thread([] (cql_test_env& env) -> future<> {
        using namespace std::chrono_literals;

        env.db().invoke_on_all([] (replica::database& db) {
            db.set_querier_cache_entry_ttl(2s);
        }).get();

        auto [s, pkeys] = create_test_table(env, KEYSPACE_NAME, get_name(), 2, 10);

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

            uint64_t lookups = 0;
            uint64_t misses = 0;
            auto saved_readers = aggregate_querier_cache_stat(env.db(), &query::querier_cache::stats::population);

            // Then do a paged range-query, with reader caching
            auto results2 = read_all_partitions_with_paged_scan(env.db(), s, 4, stateful_query::yes, [&] (size_t page) {
                const auto new_misses = aggregate_querier_cache_stat(env.db(), &query::querier_cache::stats::misses);
                const auto new_lookups = aggregate_querier_cache_stat(env.db(), &query::querier_cache::stats::lookups);

                if (page) {
                    tests::require(new_lookups > lookups);
                }

                tests::require_equal(aggregate_querier_cache_stat(env.db(), &query::querier_cache::stats::drops), 0u);
                tests::require_equal(aggregate_querier_cache_stat(env.db(), &query::querier_cache::stats::drops), 0u);
                tests::require_less_equal(new_misses - misses, smp::count - saved_readers);

                lookups = new_lookups;
                misses = new_misses;
                saved_readers = aggregate_querier_cache_stat(env.db(), &query::querier_cache::stats::population);
                tests::require_greater_equal(saved_readers, 1u);
            }).first;

            check_results_are_equal(results1, results2);

            tests::require_equal(aggregate_querier_cache_stat(env.db(), &query::querier_cache::stats::drops), 0u);
            tests::require_equal(aggregate_querier_cache_stat(env.db(), &query::querier_cache::stats::time_based_evictions), 0u);
            tests::require_equal(aggregate_querier_cache_stat(env.db(), &query::querier_cache::stats::resource_based_evictions), 0u);
        } } }

        return make_ready_future<>();
    }, cql_config_with_extensions()).get();
}

// Best run with SMP>=2
SEASTAR_THREAD_TEST_CASE(test_evict_a_shard_reader_on_each_page) {
    do_with_cql_env_thread([] (cql_test_env& env) -> future<> {
        using namespace std::chrono_literals;

        env.db().invoke_on_all([] (replica::database& db) {
            db.set_querier_cache_entry_ttl(2s);
        }).get();

        auto [s, pkeys] = create_test_table(env, KEYSPACE_NAME, get_name());

        // First read all partition-by-partition (not paged).
        auto results1 = read_all_partitions_one_by_one(env.db(), s, pkeys);

        int64_t lookups = 0;
        uint64_t evictions = 0;

        // Then do a paged range-query
        auto [results2, npages] = read_all_partitions_with_paged_scan(env.db(), s, 4, stateful_query::yes, [&] (size_t page) {
            const auto new_lookups = aggregate_querier_cache_stat(env.db(), &query::querier_cache::stats::lookups);
            if (page) {
                tests::require(std::cmp_greater(new_lookups, lookups), seastar::compat::source_location::current());
            }
            lookups = new_lookups;

            for (unsigned shard = 0; shard < smp::count; ++shard) {
                auto evicted = smp::submit_to(shard, [&] {
                    return env.local_db().get_querier_cache().evict_one();
                }).get();
                if (evicted) {
                    ++evictions;
                    break;
                }
            }

            tests::require(aggregate_querier_cache_stat(env.db(), &query::querier_cache::stats::misses) >= page, seastar::compat::source_location::current());
            tests::require_equal(aggregate_querier_cache_stat(env.db(), &query::querier_cache::stats::drops), 0u, seastar::compat::source_location::current());
        });

        check_results_are_equal(results1, results2);

        tests::require_equal(aggregate_querier_cache_stat(env.db(), &query::querier_cache::stats::drops), 0u);
        tests::require_equal(aggregate_querier_cache_stat(env.db(), &query::querier_cache::stats::time_based_evictions), 0u);
        tests::require_equal(aggregate_querier_cache_stat(env.db(), &query::querier_cache::stats::resource_based_evictions), evictions);

        require_eventually_empty_caches(env.db());

        return make_ready_future<>();
    }, cql_config_with_extensions()).get();
}

// Best run with SMP>=2
SEASTAR_THREAD_TEST_CASE(test_read_reversed) {
    do_with_cql_env_thread([] (cql_test_env& env) -> future<> {
        using namespace std::chrono_literals;

        auto& db = env.db();

        auto [s, pkeys] = create_test_table(env, KEYSPACE_NAME, get_name(), 4, 8);
        s = s->make_reversed();

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
                std::copy(rs.rows().begin(), rs.rows().end(), std::back_inserter(expected_rows));
            }
            auto expected_data_results = query::result_set(s, std::move(expected_rows));

            BOOST_REQUIRE_EQUAL(data_results, expected_data_results);

            tests::require_equal(aggregate_querier_cache_stat(db, &query::querier_cache::stats::drops), 0u);
            tests::require_equal(aggregate_querier_cache_stat(db, &query::querier_cache::stats::time_based_evictions), 0u);
            tests::require_equal(aggregate_querier_cache_stat(db, &query::querier_cache::stats::resource_based_evictions), 0u);

        } } }

        require_eventually_empty_caches(env.db());

        return make_ready_future<>();
    }, cql_config_with_extensions()).get();
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

template <typename RandomEngine>
static interval<int> generate_range(RandomEngine& rnd_engine, int start, int end, bool allow_open_ended_start = true) {
    SCYLLA_ASSERT(start < end);

    std::uniform_int_distribution<int> defined_bound_dist(0, 7);
    std::uniform_int_distribution<int8_t> inclusive_dist(0, 1);
    std::uniform_int_distribution<int> bound_dist(start, end);

    const auto open_lower_bound = allow_open_ended_start && !defined_bound_dist(rnd_engine);
    const auto open_upper_bound = !defined_bound_dist(rnd_engine);

    if (open_lower_bound || open_upper_bound) {
        const auto bound = bound_dist(rnd_engine);
        if (open_lower_bound) {
            return interval<int>::make_ending_with(
                    interval<int>::bound(bound, inclusive_dist(rnd_engine)));
        }
        return interval<int>::make_starting_with(
                interval<int>::bound(bound, inclusive_dist(rnd_engine)));
    }

    const auto b1 = bound_dist(rnd_engine);
    const auto b2 = bound_dist(rnd_engine);
    if (b1 == b2) {
        return interval<int>::make_starting_with(
                interval<int>::bound(b1, inclusive_dist(rnd_engine)));
    }
    return interval<int>::make(
                interval<int>::bound(std::min(b1, b2), inclusive_dist(rnd_engine)),
                interval<int>::bound(std::max(b1, b2), inclusive_dist(rnd_engine)));
}

template <typename RandomEngine>
static query::clustering_row_ranges
generate_clustering_ranges(RandomEngine& rnd_engine, const schema& schema, const std::vector<mutation>& mutations) {
    if (!schema.clustering_key_size()) {
        return {};
    }

    std::vector<clustering_key> all_cks;
    std::set<clustering_key, clustering_key::less_compare> all_cks_sorted{clustering_key::less_compare(schema)};

    for (const auto& mut : mutations) {
        for (const auto& row : mut.partition().clustered_rows()) {
            all_cks_sorted.insert(row.key());
        }
        thread::maybe_yield();
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

static std::vector<mutation>
slice_partitions(const schema& schema, const std::vector<mutation>& partitions,
        const interval<int>& partition_index_range, const query::partition_slice& slice) {
    const auto& sb = partition_index_range.start();
    const auto& eb = partition_index_range.end();
    auto it = sb ? partitions.cbegin() + sb->value() + !sb->is_inclusive() : partitions.cbegin();
    const auto end = eb ? partitions.cbegin() + eb->value() + eb->is_inclusive() : partitions.cend();

    const auto& row_ranges = slice.default_row_ranges();

    std::vector<mutation> sliced_partitions;
    for (;it != end; ++it) {
        sliced_partitions.push_back(it->sliced(row_ranges));
        thread::maybe_yield();
    }
    return sliced_partitions;
}

static void
validate_result_size(size_t i, schema_ptr schema, const std::vector<mutation>& results, const std::vector<mutation>& expected_partitions) {
    if (results.size() == expected_partitions.size()) {
        return;
    }

    auto expected = std::set<dht::decorated_key, dht::decorated_key::less_comparator>(dht::decorated_key::less_comparator(schema));
    for (const auto& p : expected_partitions) {
       expected.insert(p.decorated_key());
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

struct fuzzy_test_config {
    uint32_t seed;
    std::chrono::seconds timeout;
    unsigned concurrency;
    unsigned scans;
};

static void
run_fuzzy_test_scan(size_t i, fuzzy_test_config cfg, distributed<replica::database>& db, schema_ptr schema,
        const std::vector<frozen_mutation>& frozen_mutations) {
    const auto seed = cfg.seed + (i + 1) * this_shard_id();
    auto rnd_engine = std::mt19937(seed);

    std::vector<mutation> mutations;
    for (const auto& mut : frozen_mutations) {
        mutations.emplace_back(mut.unfreeze(schema));
        thread::maybe_yield();
    }

    auto partition_index_range = generate_range(rnd_engine, 0, mutations.size() - 1);
    auto partition_range = partition_index_range.transform([&mutations] (int i) {
        return dht::ring_position(mutations[i].decorated_key());
    });

    const auto partition_slice = partition_slice_builder(*schema)
        .with_ranges(generate_clustering_ranges(rnd_engine, *schema, mutations))
        .with_option<query::partition_slice::option::allow_short_read>()
        .build();

    const auto is_stateful = stateful_query(std::uniform_int_distribution<int>(0, 3)(rnd_engine));

    testlog.debug("[scan#{}]: seed={}, is_stateful={}, prange={}, ckranges={}", i, seed, is_stateful, partition_range,
            partition_slice.default_row_ranges());

    const auto [results, npages] = read_partitions_with_paged_scan(db, schema, 1000, 1024, is_stateful, partition_range, partition_slice);

    const auto expected_partitions = slice_partitions(*schema, mutations, partition_index_range, partition_slice);

    validate_result_size(i, schema, results, expected_partitions);

    auto exp_it = expected_partitions.cbegin();
    auto res_it = results.cbegin();
    while (res_it != results.cend() && exp_it != expected_partitions.cend()) {
        assert_that(*res_it++).is_equal_to(*exp_it++);
        thread::maybe_yield();
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
        const std::vector<frozen_mutation>& frozen_mutations) {
    return run_concurrently(cfg.scans, cfg.concurrency, [cfg, &db, schema = std::move(schema), &frozen_mutations] (size_t i) {
        return seastar::async([i, cfg, &db, schema, &frozen_mutations] () mutable {
            run_fuzzy_test_scan(i, cfg, db, std::move(schema), frozen_mutations);
        });
    });
}

} // namespace

SEASTAR_THREAD_TEST_CASE(fuzzy_test) {
    auto cql_cfg = cql_config_with_extensions();
    cql_cfg.db_config->enable_commitlog(false);

    do_with_cql_env_thread([] (cql_test_env& env) -> future<> {
        // REPLACE RANDOM SEED HERE.
        const auto seed = tests::random::get_int<uint32_t>();
        testlog.info("fuzzy test seed: {}", seed);

        auto tbl = create_test_table(env, seed, "ks", get_name(), false,
#ifdef DEBUG
                std::uniform_int_distribution<size_t>(8, 32), // partitions
                std::uniform_int_distribution<size_t>(0, 100), // clustering-rows
                std::uniform_int_distribution<size_t>(0, 10), // range-tombstones
#elif DEVEL
                std::uniform_int_distribution<size_t>(16, 64), // partitions
                std::uniform_int_distribution<size_t>(0, 100), // clustering-rows
                std::uniform_int_distribution<size_t>(0, 100), // range-tombstones
#else
                std::uniform_int_distribution<size_t>(32, 64), // partitions
                std::uniform_int_distribution<size_t>(0, 1000), // clustering-rows
                std::uniform_int_distribution<size_t>(0, 1000), // range-tombstones
#endif
                tests::default_timestamp_generator());

#if defined DEBUG
        auto cfg = fuzzy_test_config{seed, std::chrono::seconds{8}, 1, 1};
#elif defined DEVEL
        auto cfg = fuzzy_test_config{seed, std::chrono::seconds{2}, 2, 4};
#else
        auto cfg = fuzzy_test_config{seed, std::chrono::seconds{2}, 4, 8};
#endif

        testlog.info("Running test workload with configuration: seed={}, timeout={}s, concurrency={}, scans={}", cfg.seed, cfg.timeout.count(),
                cfg.concurrency, cfg.scans);

        smp::invoke_on_all([cfg, db = &env.db(), gs = global_schema_ptr(tbl.schema), &compacted_frozen_mutations = tbl.compacted_frozen_mutations] {
            return run_fuzzy_test_workload(cfg, *db, gs.get(), compacted_frozen_mutations);
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
    }, cql_cfg).get();
}
