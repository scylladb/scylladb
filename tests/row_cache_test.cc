/*
 * Copyright 2015 Cloudius Systems
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

#define BOOST_TEST_DYN_LINK

#include <boost/test/unit_test.hpp>
#include <seastar/core/sleep.hh>

#include "tests/test-utils.hh"
#include "tests/mutation_assertions.hh"
#include "tests/mutation_reader_assertions.hh"
#include "tests/mutation_source_test.hh"

#include "schema_builder.hh"
#include "row_cache.hh"
#include "core/thread.hh"
#include "memtable.hh"

using namespace std::chrono_literals;

static schema_ptr make_schema() {
    return schema_builder("ks", "cf")
        .with_column("pk", bytes_type, column_kind::partition_key)
        .with_column("v", bytes_type, column_kind::regular_column)
        .build();
}

static
mutation make_new_mutation(schema_ptr s, partition_key key) {
    mutation m(key, s);
    static thread_local int next_value = 1;
    static thread_local api::timestamp_type next_timestamp = 1;
    m.set_clustered_cell(clustering_key::make_empty(*s), "v", data_value(to_bytes(sprint("v%d", next_value++))), next_timestamp++);
    return m;
}

static
partition_key new_key(schema_ptr s) {
    static thread_local int next = 0;
    return partition_key::from_single_value(*s, to_bytes(sprint("key%d", next++)));
}

static
mutation make_new_mutation(schema_ptr s) {
    return make_new_mutation(s, new_key(s));
}

SEASTAR_TEST_CASE(test_cache_delegates_to_underlying) {
    return seastar::async([] {
        auto s = make_schema();
        auto m = make_new_mutation(s);

        cache_tracker tracker;
        row_cache cache(s, [m] (schema_ptr s, const query::partition_range&) {
            assert(m.schema() == s);
            return make_reader_returning(m);
        }, [m] (auto&&) {
            return make_key_from_mutation_reader(make_reader_returning(m));
        }, tracker);

        assert_that(cache.make_reader(s, query::full_partition_range))
            .produces(m)
            .produces_end_of_stream();
    });
}

SEASTAR_TEST_CASE(test_cache_works_after_clearing) {
    return seastar::async([] {
        auto s = make_schema();
        auto m = make_new_mutation(s);

        cache_tracker tracker;
        row_cache cache(s, [m] (schema_ptr s, const query::partition_range&) {
            assert(m.schema() == s);
            return make_reader_returning(m);
        }, [m] (auto&&) {
            return make_key_from_mutation_reader(make_reader_returning(m));
        }, tracker);

        assert_that(cache.make_reader(s, query::full_partition_range))
            .produces(m)
            .produces_end_of_stream();

        tracker.clear();

        assert_that(cache.make_reader(s, query::full_partition_range))
            .produces(m)
            .produces_end_of_stream();
    });
}

static std::vector<mutation> make_ring(schema_ptr s, int n_mutations) {
    std::vector<mutation> mutations;
    for (int i = 0; i < n_mutations; ++i) {
        mutations.push_back(make_new_mutation(s));
    }
    std::sort(mutations.begin(), mutations.end(), mutation_decorated_key_less_comparator());
    return mutations;
}

SEASTAR_TEST_CASE(test_query_of_incomplete_range_goes_to_underlying) {
    return seastar::async([] {
        auto s = make_schema();

        std::vector<mutation> mutations = make_ring(s, 3);

        auto mt = make_lw_shared<memtable>(s);
        for (auto&& m : mutations) {
            mt->apply(m);
        }

        cache_tracker tracker;
        row_cache cache(s, mt->as_data_source(), mt->as_key_source(), tracker);

        auto get_partition_range = [] (const mutation& m) {
            return query::partition_range::make_singular(query::ring_position(m.decorated_key()));
        };

        // Populate cache for first key
        assert_that(cache.make_reader(s, get_partition_range(mutations[0])))
            .produces(mutations[0])
            .produces_end_of_stream();

        // Populate cache for last key
        assert_that(cache.make_reader(s, get_partition_range(mutations[2])))
            .produces(mutations[2])
            .produces_end_of_stream();

        // Test single-key queries
        assert_that(cache.make_reader(s, get_partition_range(mutations[0])))
            .produces(mutations[0])
            .produces_end_of_stream();

        assert_that(cache.make_reader(s, get_partition_range(mutations[2])))
            .produces(mutations[2])
            .produces_end_of_stream();

        // Test range query
        assert_that(cache.make_reader(s, query::full_partition_range))
            .produces(mutations[0])
            .produces(mutations[1])
            .produces(mutations[2])
            .produces_end_of_stream();
    });
}

SEASTAR_TEST_CASE(test_single_key_queries_after_population_in_reverse_order) {
    return seastar::async([] {
        auto s = make_schema();

        auto mt = make_lw_shared<memtable>(s);

        std::vector<mutation> mutations = make_ring(s, 3);

        for (auto&& m : mutations) {
            mt->apply(m);
        }

        cache_tracker tracker;
        row_cache cache(s, mt->as_data_source(), mt->as_key_source(), tracker);

        auto get_partition_range = [] (const mutation& m) {
            return query::partition_range::make_singular(query::ring_position(m.decorated_key()));
        };

        for (int i = 0; i < 2; ++i) {
            assert_that(cache.make_reader(s, get_partition_range(mutations[2])))
                .produces(mutations[2])
                .produces_end_of_stream();

            assert_that(cache.make_reader(s, get_partition_range(mutations[1])))
                .produces(mutations[1])
                .produces_end_of_stream();

            assert_that(cache.make_reader(s, get_partition_range(mutations[0])))
                .produces(mutations[0])
                .produces_end_of_stream();
        }
    });
}

SEASTAR_TEST_CASE(test_row_cache_conforms_to_mutation_source) {
    return seastar::async([] {
        cache_tracker tracker;

        run_mutation_source_tests([&tracker](schema_ptr s, const std::vector<mutation>& mutations) -> mutation_source {
            auto mt = make_lw_shared<memtable>(s);

            for (auto&& m : mutations) {
                mt->apply(m);
            }

            auto cache = make_lw_shared<row_cache>(s, mt->as_data_source(), mt->as_key_source(), tracker);
            return [cache] (schema_ptr s, const query::partition_range& range) {
                return cache->make_reader(s, range);
            };
        });
    });
}

SEASTAR_TEST_CASE(test_eviction) {
    return seastar::async([] {
        auto s = make_schema();
        auto mt = make_lw_shared<memtable>(s);

        cache_tracker tracker;
        row_cache cache(s, mt->as_data_source(), mt->as_key_source(), tracker);

        std::vector<dht::decorated_key> keys;
        for (int i = 0; i < 100000; i++) {
            auto m = make_new_mutation(s);
            keys.emplace_back(m.decorated_key());
            cache.populate(m);
        }

        std::random_shuffle(keys.begin(), keys.end());

        for (auto&& key : keys) {
            cache.make_reader(s, query::partition_range::make_singular(key));
        }

        while (tracker.region().occupancy().used_space() > 0) {
            logalloc::shard_tracker().reclaim(100);
        }
    });
}

bool has_key(row_cache& cache, const dht::decorated_key& key) {
    auto range = query::partition_range::make_singular(key);
    auto reader = cache.make_reader(cache.schema(), range);
    auto mo = reader().get0();
    return bool(mo);
}

void verify_has(row_cache& cache, const dht::decorated_key& key) {
    BOOST_REQUIRE(has_key(cache, key));
}

void verify_does_not_have(row_cache& cache, const dht::decorated_key& key) {
    BOOST_REQUIRE(!has_key(cache, key));
}

void verify_has(row_cache& cache, const mutation& m) {
    auto range = query::partition_range::make_singular(m.decorated_key());
    auto reader = cache.make_reader(cache.schema(), range);
    auto mo = reader().get0();
    BOOST_REQUIRE(bool(mo));
    assert_that(*mo).is_equal_to(m);
}

SEASTAR_TEST_CASE(test_update) {
    return seastar::async([] {
        auto s = make_schema();
        auto cache_mt = make_lw_shared<memtable>(s);

        cache_tracker tracker;
        row_cache cache(s, cache_mt->as_data_source(), cache_mt->as_key_source(), tracker);

        BOOST_MESSAGE("Check cache miss with populate");

        int partition_count = 1000;

        // populate cache with some partitions
        std::vector<dht::decorated_key> keys_in_cache;
        for (int i = 0; i < partition_count; i++) {
            auto m = make_new_mutation(s);
            keys_in_cache.push_back(m.decorated_key());
            cache.populate(m);
        }

        // populate memtable with partitions not in cache
        auto mt = make_lw_shared<memtable>(s);
        std::vector<dht::decorated_key> keys_not_in_cache;
        for (int i = 0; i < partition_count; i++) {
            auto m = make_new_mutation(s);
            keys_not_in_cache.push_back(m.decorated_key());
            mt->apply(m);
        }

        cache.update(*mt, [] (auto&& key) {
            return partition_presence_checker_result::definitely_doesnt_exist;
        }).get();

        for (auto&& key : keys_not_in_cache) {
            verify_has(cache, key);
        }

        for (auto&& key : keys_in_cache) {
            verify_has(cache, key);
        }

        std::copy(keys_not_in_cache.begin(), keys_not_in_cache.end(), std::back_inserter(keys_in_cache));
        keys_not_in_cache.clear();

        BOOST_MESSAGE("Check cache miss with drop");

        auto mt2 = make_lw_shared<memtable>(s);

        // populate memtable with partitions not in cache
        for (int i = 0; i < partition_count; i++) {
            auto m = make_new_mutation(s);
            keys_not_in_cache.push_back(m.decorated_key());
            mt2->apply(m);
        }

        cache.update(*mt2, [] (auto&& key) {
            return partition_presence_checker_result::maybe_exists;
        }).get();

        for (auto&& key : keys_not_in_cache) {
            verify_does_not_have(cache, key);
        }

        BOOST_MESSAGE("Check cache hit with merge");

        auto mt3 = make_lw_shared<memtable>(s);

        std::vector<mutation> new_mutations;
        for (auto&& key : keys_in_cache) {
            auto m = make_new_mutation(s, key.key());
            new_mutations.push_back(m);
            mt3->apply(m);
        }

        cache.update(*mt3, [] (auto&& key) {
            return partition_presence_checker_result::maybe_exists;
        }).get();

        for (auto&& m : new_mutations) {
            verify_has(cache, m);
        }
    });
}

class throttle {
    unsigned _block_counter = 0;
    promise<> _p; // valid when _block_counter != 0, resolves when goes down to 0
public:
    future<> enter() {
        if (_block_counter) {
            promise<> p1;
            promise<> p2;

            auto f1 = p1.get_future();

            p2.get_future().then([p1 = std::move(p1), p3 = std::move(_p)] () mutable {
                p1.set_value();
                p3.set_value();
            });
            _p = std::move(p2);

            return f1;
        } else {
            return make_ready_future<>();
        }
    }

    void block() {
        ++_block_counter;
        _p = promise<>();
    }

    void unblock() {
        assert(_block_counter);
        if (--_block_counter == 0) {
            _p.set_value();
        }
    }
};

class throttled_mutation_source {
private:
    class impl : public enable_lw_shared_from_this<impl> {
        mutation_source _underlying;
        ::throttle _throttle;
    private:
        class reader : public mutation_reader::impl {
            throttle& _throttle;
            mutation_reader _reader;
        public:
            reader(throttle& t, mutation_reader r)
                    : _throttle(t)
                    , _reader(std::move(r))
            {}

            virtual future<mutation_opt> operator()() override {
                return _reader().finally([this] () {
                    return _throttle.enter();
                });
            }
        };
    public:
        impl(mutation_source underlying)
            : _underlying(std::move(underlying))
        { }

        mutation_reader make_reader(schema_ptr s, const query::partition_range& pr) {
            return make_mutation_reader<reader>(_throttle, _underlying(s, pr));
        }

        ::throttle& throttle() { return _throttle; }
    };
    lw_shared_ptr<impl> _impl;
public:
    throttled_mutation_source(mutation_source underlying)
        : _impl(make_lw_shared<impl>(std::move(underlying)))
    { }

    void block() {
        _impl->throttle().block();
    }

    void unblock() {
        _impl->throttle().unblock();
    }

    mutation_reader operator()(schema_ptr s, const query::partition_range& pr) {
        return _impl->make_reader(s, pr);
    }
};

static std::vector<mutation> updated_ring(std::vector<mutation>& mutations) {
    std::vector<mutation> result;
    for (auto&& m : mutations) {
        result.push_back(make_new_mutation(m.schema(), m.key()));
    }
    return result;
}

SEASTAR_TEST_CASE(test_cache_population_and_update_race) {
    return seastar::async([] {
        auto s = make_schema();
        std::vector<lw_shared_ptr<memtable>> memtables;
        auto memtables_data_source = [&] (schema_ptr s, const query::partition_range& pr) {
            std::vector<mutation_reader> readers;
            for (auto&& mt : memtables) {
                readers.emplace_back(mt->make_reader(s, pr));
            }
            return make_combined_reader(std::move(readers));
        };
        auto memtables_key_source = [&] (const query::partition_range& pr) {
            std::vector<key_reader> readers;
            for (auto&& mt : memtables) {
                readers.emplace_back(mt->as_key_source()(pr));
            }
            return make_combined_reader(s, std::move(readers));
        };
        throttled_mutation_source cache_source(memtables_data_source);
        cache_tracker tracker;
        row_cache cache(s, cache_source, memtables_key_source, tracker);

        auto mt1 = make_lw_shared<memtable>(s);
        memtables.push_back(mt1);
        auto ring = make_ring(s, 3);
        for (auto&& m : ring) {
            mt1->apply(m);
        }

        auto mt2 = make_lw_shared<memtable>(s);
        auto ring2 = updated_ring(ring);
        for (auto&& m : ring2) {
            mt2->apply(m);
        }

        cache_source.block();

        auto m0_range = query::partition_range::make_singular(ring[0].ring_position());
        auto rd1 = cache.make_reader(s, m0_range);
        auto rd1_result = rd1();

        auto rd2 = cache.make_reader(s);
        auto rd2_result = rd2();

        sleep(10ms).get();
        auto mt2_flushed = make_lw_shared<memtable>(s);
        mt2_flushed->apply(*mt2).get();
        memtables.push_back(mt2_flushed);

        // This update should miss on all partitions
        auto update_future = cache.update(*mt2, make_default_partition_presence_checker());

        auto rd3 = cache.make_reader(s);

        // rd2, which is in progress, should not prevent forward progress of update()
        cache_source.unblock();
        update_future.get();

        // Reads started before memtable flush should return previous value, otherwise this test
        // doesn't trigger the conditions it is supposed to protect against.
        assert_that(rd1_result.get0()).has_mutation().is_equal_to(ring[0]);

        assert_that(rd2_result.get0()).has_mutation().is_equal_to(ring[0]);
        assert_that(rd2().get0()).has_mutation().is_equal_to(ring2[1]);
        assert_that(rd2().get0()).has_mutation().is_equal_to(ring2[2]);
        assert_that(rd2().get0()).has_no_mutation();

        // Reads started after update was started but before previous populations completed
        // should already see the new data
        assert_that(std::move(rd3))
                .produces(ring2[0])
                .produces(ring2[1])
                .produces(ring2[2])
                .produces_end_of_stream();

        // Reads started after flush should see new data
        assert_that(cache.make_reader(s))
                .produces(ring2[0])
                .produces(ring2[1])
                .produces(ring2[2])
                .produces_end_of_stream();
    });
}

SEASTAR_TEST_CASE(test_invalidate) {
    return seastar::async([] {
        auto s = make_schema();
        auto mt = make_lw_shared<memtable>(s);

        cache_tracker tracker;
        row_cache cache(s, mt->as_data_source(), mt->as_key_source(), tracker);

        int partition_count = 1000;

        // populate cache with some partitions
        std::vector<dht::decorated_key> keys_in_cache;
        for (int i = 0; i < partition_count; i++) {
            auto m = make_new_mutation(s);
            keys_in_cache.push_back(m.decorated_key());
            cache.populate(m);
        }

        for (auto&& key : keys_in_cache) {
            verify_has(cache, key);
        }

        // remove a single element from cache
        auto some_element = keys_in_cache.begin() + 547;
        std::vector<dht::decorated_key> keys_not_in_cache;
        keys_not_in_cache.push_back(*some_element);
        cache.invalidate(*some_element);
        keys_in_cache.erase(some_element);

        for (auto&& key : keys_in_cache) {
            verify_has(cache, key);
        }
        for (auto&& key : keys_not_in_cache) {
            verify_does_not_have(cache, key);
        }

        // remove a range of elements
        std::sort(keys_in_cache.begin(), keys_in_cache.end(), [s] (auto& dk1, auto& dk2) {
            return dk1.less_compare(*s, dk2);
        });
        auto some_range_begin = keys_in_cache.begin() + 123;
        auto some_range_end = keys_in_cache.begin() + 423;
        auto range = query::partition_range::make(
            { *some_range_begin, true }, { *some_range_end, false }
        );
        keys_not_in_cache.insert(keys_not_in_cache.end(), some_range_begin, some_range_end);
        cache.invalidate(range);
        keys_in_cache.erase(some_range_begin, some_range_end);

        for (auto&& key : keys_in_cache) {
            verify_has(cache, key);
        }
        for (auto&& key : keys_not_in_cache) {
            verify_does_not_have(cache, key);
        }
    });
}