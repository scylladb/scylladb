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

#include "querier.hh"
#include "service/priority_manager.hh"
#include "tests/simple_schema.hh"

#include <seastar/core/sleep.hh>
#include <seastar/core/thread.hh>
#include <seastar/tests/test-utils.hh>

using namespace std::chrono_literals;

class dummy_result_builder {
    std::optional<dht::decorated_key> _dk;
    std::optional<clustering_key_prefix> _ck;

public:
    dummy_result_builder()
        : _dk({dht::token(), partition_key::make_empty()})
        , _ck(clustering_key_prefix::make_empty()) {
    }

    void consume_new_partition(const dht::decorated_key& dk) {
        _dk = dk;
        _ck = {};
    }
    void consume(tombstone t) {
    }
    stop_iteration consume(static_row&& sr, tombstone t, bool is_live) {
        return stop_iteration::no;
    }
    stop_iteration consume(clustering_row&& cr, row_tombstone t, bool is_live) {
        _ck = cr.key();
        return stop_iteration::no;
    }
    stop_iteration consume(range_tombstone&& rt) {
        return stop_iteration::no;
    }
    stop_iteration consume_end_of_partition() {
        return stop_iteration::no;
    }
    std::pair<std::optional<dht::decorated_key>, std::optional<clustering_key_prefix>> consume_end_of_stream() {
        return {std::move(_dk), std::move(_ck)};
    }
};

class test_querier_cache {
public:
    using bound = range_bound<std::size_t>;

    static const size_t max_reader_buffer_size = 8 * 1024;

private:
    // Actual counters updated by the cache.
    unsigned _factory_invoked{};

    // Expected value of the above counters, updated by this.
    unsigned _expected_factory_invoked{};
    querier_cache::stats _expected_stats;

    simple_schema _s;
    querier_cache _cache;
    const std::vector<mutation> _mutations;
    const mutation_source _mutation_source;

    static sstring make_value(size_t i) {
        return sprint("value%010d", i);
    }

    static std::vector<mutation> make_mutations(simple_schema& s, const noncopyable_function<sstring(size_t)>& make_value) {
        std::vector<mutation> mutations;
        mutations.reserve(10);

        for (uint32_t i = 0; i != 10; ++i) {
            auto mut = mutation(s.schema(), s.make_pkey(i));

            s.add_static_row(mut, "-");
            s.add_row(mut, s.make_ckey(0), make_value(0));
            s.add_row(mut, s.make_ckey(1), make_value(1));
            s.add_row(mut, s.make_ckey(2), make_value(2));
            s.add_row(mut, s.make_ckey(3), make_value(3));

            mutations.emplace_back(std::move(mut));
        }

        boost::sort(mutations, [] (const mutation& a, const mutation& b) {
            return a.decorated_key().tri_compare(*a.schema(), b.decorated_key()) < 0;
        });

        return mutations;
    }

    querier make_querier(const dht::partition_range& range) {
        return querier(_mutation_source,
            _s.schema(),
            range,
            _s.schema()->full_slice(),
            service::get_local_sstable_query_read_priority(),
            nullptr,
            emit_only_live_rows::no);
    }

    static utils::UUID make_cache_key(unsigned key) {
        return utils::UUID{key, 1};
    }

    const dht::decorated_key* find_key(const dht::partition_range& range, unsigned partition_offset) const {
        const auto& s = *_s.schema();
        const auto less_cmp = dht::ring_position_less_comparator(s);

        const auto begin = _mutations.begin();
        const auto end = _mutations.end();
        const auto start_position = range.start() ?
            dht::ring_position_view::for_range_start(range) :
            dht::ring_position_view(_mutations.begin()->decorated_key());

        const auto it = std::lower_bound(begin, end, start_position, [&] (const mutation& m, const dht::ring_position_view& k) {
            return less_cmp(m.ring_position(), k);
        });

        if (it == end) {
            return nullptr;
        }

        const auto dist = std::distance(it, end);
        auto& mut = *(partition_offset >= dist ? it + dist : it + partition_offset);
        return &mut.decorated_key();
    }

public:
    struct entry_info {
        unsigned key;
        dht::partition_range original_range;
        query::partition_slice original_slice;
        uint32_t row_limit;
        size_t memory_usage;

        dht::partition_range expected_range;
        query::partition_slice expected_slice;
    };

    test_querier_cache(const noncopyable_function<sstring(size_t)>& external_make_value, std::chrono::seconds entry_ttl = 24h)
        : _cache(entry_ttl)
        , _mutations(make_mutations(_s, external_make_value))
        , _mutation_source([this] (schema_ptr, const dht::partition_range& range) {
            auto rd = flat_mutation_reader_from_mutations(_mutations, range);
            rd.set_max_buffer_size(max_reader_buffer_size);
            return std::move(rd);
        }) {
    }

    explicit test_querier_cache(std::chrono::seconds entry_ttl = 24h)
        : test_querier_cache(test_querier_cache::make_value, entry_ttl) {
    }

    const simple_schema& get_simple_schema() const {
        return _s;
    }

    simple_schema& get_simple_schema() {
        return _s;
    }

    const schema_ptr get_schema() const {
        return _s.schema();
    }

    dht::partition_range make_partition_range(bound begin, bound end) const {
        return dht::partition_range::make({_mutations.at(begin.value()).decorated_key(), begin.is_inclusive()},
                {_mutations.at(end.value()).decorated_key(), end.is_inclusive()});
    }

    dht::partition_range make_singular_partition_range(std::size_t i) const {
        return dht::partition_range::make_singular(_mutations.at(i).decorated_key());
    }

    dht::partition_range make_default_partition_range() const {
        return make_partition_range({0, true}, {_mutations.size() - 1, true});
    }

    const query::partition_slice& make_default_slice() const {
        return _s.schema()->full_slice();
    }

    entry_info produce_first_page_and_save_querier(unsigned key, const dht::partition_range& range,
            const query::partition_slice& slice, uint32_t row_limit = 5) {
        const auto cache_key = make_cache_key(key);

        auto querier = make_querier(range);
        auto [dk, ck] = querier.consume_page(dummy_result_builder{}, row_limit, std::numeric_limits<uint32_t>::max(),
                gc_clock::now(), db::no_timeout).get0();
        const auto memory_usage = querier.memory_usage();
        _cache.insert(cache_key, std::move(querier), nullptr);

        // Either no keys at all (nothing read) or at least partition key.
        BOOST_REQUIRE((dk && ck) || !ck);

        // Check that the read stopped at the correct position.
        // There are 5 rows in each mutation (1 static + 4 clustering).
        const auto* expected_key = find_key(range, row_limit / 5);
        if (!expected_key) {
            BOOST_REQUIRE(!dk);
            BOOST_REQUIRE(!ck);
        } else {
            BOOST_REQUIRE(dk->equal(*_s.schema(), *expected_key));
        }

        auto expected_range = [&] {
            if (range.is_singular() || !dk) {
                return range;
            }

            return dht::partition_range(dht::partition_range::bound(*dk, true), range.end());
        }();

        auto expected_slice = [&] {
            if (!ck) {
                return slice;
            }

            auto expected_slice = slice;
            auto cr = query::clustering_range::make_starting_with({*ck, false});
            expected_slice.set_range(*_s.schema(), dk->key(), {std::move(cr)});

            return expected_slice;
        }();

        return {key, std::move(range), std::move(slice), row_limit, memory_usage, std::move(expected_range), std::move(expected_slice)};
    }

    entry_info produce_first_page_and_save_querier(unsigned key, const dht::partition_range& range, uint32_t row_limit = 5) {
        return produce_first_page_and_save_querier(key, range, make_default_slice(), row_limit);
    }

    // Singular overload
    entry_info produce_first_page_and_save_querier(unsigned key, std::size_t i, uint32_t row_limit = 5) {
        return produce_first_page_and_save_querier(key, make_singular_partition_range(i), _s.schema()->full_slice(), row_limit);
    }

    // Use the whole range
    entry_info produce_first_page_and_save_querier(unsigned key) {
        return produce_first_page_and_save_querier(key, make_default_partition_range(), _s.schema()->full_slice());
    }

    // For tests testing just one insert-lookup.
    entry_info produce_first_page_and_save_querier() {
        return produce_first_page_and_save_querier(1);
    }

    test_querier_cache& assert_cache_lookup(unsigned lookup_key,
            const schema& lookup_schema,
            const dht::partition_range& lookup_range,
            const query::partition_slice& lookup_slice) {

        _cache.lookup(make_cache_key(lookup_key), emit_only_live_rows::no, lookup_schema, lookup_range, lookup_slice, nullptr, [this, &lookup_range] {
            ++_factory_invoked;
            return make_querier(lookup_range);
        });
        BOOST_REQUIRE_EQUAL(_cache.get_stats().lookups, ++_expected_stats.lookups);
        return *this;
    }

    test_querier_cache& no_factory_invoked() {
        BOOST_REQUIRE_EQUAL(_factory_invoked, _expected_factory_invoked);
        return *this;
    }

    test_querier_cache& factory_invoked() {
        BOOST_REQUIRE_EQUAL(_factory_invoked, ++_expected_factory_invoked);
        return *this;
    }

    test_querier_cache& no_misses() {
        BOOST_REQUIRE_EQUAL(_cache.get_stats().misses, _expected_stats.misses);
        return *this;
    }

    test_querier_cache& misses() {
        BOOST_REQUIRE_EQUAL(_cache.get_stats().misses, ++_expected_stats.misses);
        return *this;
    }

    test_querier_cache& no_drops() {
        BOOST_REQUIRE_EQUAL(_cache.get_stats().drops, _expected_stats.drops);
        return *this;
    }

    test_querier_cache& drops() {
        BOOST_REQUIRE_EQUAL(_cache.get_stats().drops, ++_expected_stats.drops);
        return *this;
    }

    test_querier_cache& no_evictions() {
        BOOST_REQUIRE_EQUAL(_cache.get_stats().time_based_evictions, _expected_stats.time_based_evictions);
        BOOST_REQUIRE_EQUAL(_cache.get_stats().resource_based_evictions, _expected_stats.resource_based_evictions);
        BOOST_REQUIRE_EQUAL(_cache.get_stats().memory_based_evictions, _expected_stats.memory_based_evictions);
        return *this;
    }

    test_querier_cache& time_based_evictions() {
        BOOST_REQUIRE_EQUAL(_cache.get_stats().time_based_evictions, ++_expected_stats.time_based_evictions);
        BOOST_REQUIRE_EQUAL(_cache.get_stats().resource_based_evictions, _expected_stats.resource_based_evictions);
        BOOST_REQUIRE_EQUAL(_cache.get_stats().memory_based_evictions, _expected_stats.memory_based_evictions);
        return *this;
    }

    test_querier_cache& resource_based_evictions() {
        BOOST_REQUIRE_EQUAL(_cache.get_stats().time_based_evictions, _expected_stats.time_based_evictions);
        BOOST_REQUIRE_EQUAL(_cache.get_stats().resource_based_evictions, ++_expected_stats.resource_based_evictions);
        BOOST_REQUIRE_EQUAL(_cache.get_stats().memory_based_evictions, _expected_stats.memory_based_evictions);
        return *this;
    }

    test_querier_cache& memory_based_evictions() {
        BOOST_REQUIRE_EQUAL(_cache.get_stats().time_based_evictions, _expected_stats.time_based_evictions);
        BOOST_REQUIRE_EQUAL(_cache.get_stats().resource_based_evictions, _expected_stats.resource_based_evictions);
        BOOST_REQUIRE_EQUAL(_cache.get_stats().memory_based_evictions, ++_expected_stats.memory_based_evictions);
        return *this;
    }
};

SEASTAR_THREAD_TEST_CASE(lookup_with_wrong_key_misses) {
    test_querier_cache t;

    const auto entry = t.produce_first_page_and_save_querier();
    t.assert_cache_lookup(90, *t.get_schema(), entry.expected_range, entry.expected_slice)
        .factory_invoked()
        .misses()
        .no_drops()
        .no_evictions();
}

/*
 * Range matching tests
 */

SEASTAR_THREAD_TEST_CASE(singular_range_lookup_with_stop_at_clustering_row) {
    test_querier_cache t;

    const auto entry = t.produce_first_page_and_save_querier(1, t.make_singular_partition_range(1), 2);
    t.assert_cache_lookup(entry.key, *t.get_schema(), entry.expected_range, entry.expected_slice)
        .no_factory_invoked()
        .no_misses()
        .no_drops()
        .no_evictions();
}

SEASTAR_THREAD_TEST_CASE(singular_range_lookup_with_stop_at_static_row) {
    test_querier_cache t;

    const auto entry = t.produce_first_page_and_save_querier(1, t.make_singular_partition_range(1), 1);
    t.assert_cache_lookup(entry.key, *t.get_schema(), entry.expected_range, entry.expected_slice)
        .no_factory_invoked()
        .no_misses()
        .no_drops()
        .no_evictions();
}

SEASTAR_THREAD_TEST_CASE(lookup_with_stop_at_clustering_row) {
    test_querier_cache t;

    const auto entry = t.produce_first_page_and_save_querier(1, t.make_partition_range({1, true}, {3, false}), 3);
    t.assert_cache_lookup(entry.key, *t.get_schema(), entry.expected_range, entry.expected_slice)
        .no_factory_invoked()
        .no_misses()
        .no_drops()
        .no_evictions();
}

SEASTAR_THREAD_TEST_CASE(lookup_with_stop_at_static_row) {
    test_querier_cache t;

    const auto entry = t.produce_first_page_and_save_querier(1, t.make_partition_range({1, true}, {3, false}), 1);
    t.assert_cache_lookup(entry.key, *t.get_schema(), entry.expected_range, entry.expected_slice)
        .no_factory_invoked()
        .no_misses()
        .no_drops()
        .no_evictions();
}

/*
 * Drop tests
 */

SEASTAR_THREAD_TEST_CASE(lookup_with_original_range_drops) {
    test_querier_cache t;

    const auto entry = t.produce_first_page_and_save_querier(1);
    t.assert_cache_lookup(entry.key, *t.get_schema(), entry.original_range, entry.expected_slice)
        .factory_invoked()
        .no_misses()
        .drops()
        .no_evictions();

}

SEASTAR_THREAD_TEST_CASE(lookup_with_wrong_slice_drops) {
    test_querier_cache t;

    // Swap slices for different clustering keys.
    const auto entry1 = t.produce_first_page_and_save_querier(1, t.make_partition_range({1, false}, {3, true}), 3);
    const auto entry2 = t.produce_first_page_and_save_querier(2, t.make_partition_range({1, false}, {3, true}), 4);
    t.assert_cache_lookup(entry1.key, *t.get_schema(), entry1.expected_range, entry2.expected_slice)
        .factory_invoked()
        .no_misses()
        .drops()
        .no_evictions();
    t.assert_cache_lookup(entry2.key, *t.get_schema(), entry2.expected_range, entry1.expected_slice)
        .factory_invoked()
        .no_misses()
        .drops()
        .no_evictions();

    // Wrong slice.
    const auto entry3 = t.produce_first_page_and_save_querier(3);
    t.assert_cache_lookup(entry3.key, *t.get_schema(), entry3.expected_range, t.get_schema()->full_slice())
        .factory_invoked()
        .no_misses()
        .drops()
        .no_evictions();

    // Swap slices for stopped at clustering/static row.
    const auto entry4 = t.produce_first_page_and_save_querier(4, t.make_partition_range({1, false}, {3, true}), 1);
    const auto entry5 = t.produce_first_page_and_save_querier(5, t.make_partition_range({1, false}, {3, true}), 2);
    t.assert_cache_lookup(entry4.key, *t.get_schema(), entry4.expected_range, entry5.expected_slice)
        .factory_invoked()
        .no_misses()
        .drops()
        .no_evictions();
    t.assert_cache_lookup(entry5.key, *t.get_schema(), entry5.expected_range, entry4.expected_slice)
        .factory_invoked()
        .no_misses()
        .drops()
        .no_evictions();
}

SEASTAR_THREAD_TEST_CASE(lookup_with_different_schema_version_drops) {
    test_querier_cache t;

    auto new_schema = schema_builder(t.get_schema()).with_column("v1", utf8_type).build();

    const auto entry = t.produce_first_page_and_save_querier();
    t.assert_cache_lookup(entry.key, *new_schema, entry.expected_range, entry.expected_slice)
        .factory_invoked()
        .no_misses()
        .drops()
        .no_evictions();
}

/*
 * Eviction tests
 */

SEASTAR_THREAD_TEST_CASE(test_time_based_cache_eviction) {
    test_querier_cache t(1s);

    const auto entry1 = t.produce_first_page_and_save_querier(1);

    seastar::sleep(500ms).get();

    const auto entry2 = t.produce_first_page_and_save_querier(2);

    seastar::sleep(700ms).get();

    t.assert_cache_lookup(entry1.key, *t.get_schema(), entry1.expected_range, entry1.expected_slice)
        .factory_invoked()
        .misses()
        .no_drops()
        .time_based_evictions();

    seastar::sleep(700ms).get();

    t.assert_cache_lookup(entry2.key, *t.get_schema(), entry2.expected_range, entry2.expected_slice)
        .factory_invoked()
        .misses()
        .no_drops()
        .time_based_evictions();
}

sstring make_string_blob(size_t size) {
    const char* const letters = "abcdefghijklmnoqprsuvwxyz";
    std::random_device rd;
    std::uniform_int_distribution<size_t> dist(0, 25);

    sstring s;
    s.resize(size);

    for (size_t i = 0; i < size; ++i) {
        s[i] = letters[dist(rd)];
    }

    return s;
}

SEASTAR_THREAD_TEST_CASE(test_memory_based_cache_eviction) {
    test_querier_cache t([] (size_t) {
        const size_t blob_size = 1 << 1; // 1K
        return make_string_blob(blob_size);
    }, 24h);

    size_t i = 0;
    const auto entry = t.produce_first_page_and_save_querier(i++);

    const size_t queriers_needed_to_fill_cache = floor(querier_cache::max_queriers_memory_usage / entry.memory_usage);

    // Fill the cache but don't overflow.
    for (; i < queriers_needed_to_fill_cache; ++i) {
        t.produce_first_page_and_save_querier(i);
    }

    // Should overflow the limit and trigger the eviction of the oldest entry.
    t.produce_first_page_and_save_querier(queriers_needed_to_fill_cache);

    t.assert_cache_lookup(entry.key, *t.get_schema(), entry.expected_range, entry.expected_slice)
        .factory_invoked()
        .misses()
        .no_drops()
        .memory_based_evictions();
}
