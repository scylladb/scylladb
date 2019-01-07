/*
 * Copyright (C) 2015 ScyllaDB
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

#include <set>
#include <boost/test/unit_test.hpp>
#include "partition_slice_builder.hh"
#include "schema_builder.hh"
#include "mutation_source_test.hh"
#include "counters.hh"
#include "simple_schema.hh"
#include "flat_mutation_reader.hh"
#include "flat_mutation_reader_assertions.hh"
#include "mutation_query.hh"
#include "mutation_rebuilder.hh"
#include "random-utils.hh"
#include "cql3/cql3_type.hh"
#include "make_random_string.hh"
#include <boost/algorithm/string/join.hpp>

// partitions must be sorted by decorated key
static void require_no_token_duplicates(const std::vector<mutation>& partitions) {
    std::experimental::optional<dht::token> last_token;
    for (auto&& p : partitions) {
        const dht::decorated_key& key = p.decorated_key();
        if (last_token && key.token() == *last_token) {
            BOOST_FAIL("token duplicate detected");
        }
        last_token = key.token();
    }
}

static api::timestamp_type new_timestamp() {
    static thread_local api::timestamp_type ts = api::min_timestamp;
    return ts++;
}

static void test_streamed_mutation_forwarding_is_consistent_with_slicing(populate_fn populate) {
    BOOST_TEST_MESSAGE(__PRETTY_FUNCTION__);

    // Generates few random mutations and row slices and verifies that using
    // fast_forward_to() over the slices gives the same mutations as using those
    // slices in partition_slice without forwarding.

    random_mutation_generator gen(random_mutation_generator::generate_counters::no);

    for (int i = 0; i < 10; ++i) {
        mutation m = gen();

        std::vector<query::clustering_range> ranges = gen.make_random_ranges(10);
        auto prange = dht::partition_range::make_singular(m.decorated_key());
        query::partition_slice full_slice = partition_slice_builder(*m.schema()).build();
        query::partition_slice slice_with_ranges = partition_slice_builder(*m.schema())
            .with_ranges(ranges)
            .build();

        BOOST_TEST_MESSAGE(sprint("ranges: %s", ranges));

        mutation_source ms = populate(m.schema(), {m});

        flat_mutation_reader sliced_reader =
            ms.make_reader(m.schema(), prange, slice_with_ranges);

        flat_mutation_reader fwd_reader =
            ms.make_reader(m.schema(), prange, full_slice, default_priority_class(), nullptr, streamed_mutation::forwarding::yes);

        stdx::optional<mutation_rebuilder> builder{};
        struct consumer {
            schema_ptr _s;
            stdx::optional<mutation_rebuilder>& _builder;
            consumer(schema_ptr s, stdx::optional<mutation_rebuilder>& builder)
                : _s(std::move(s))
                , _builder(builder) { }

            void consume_new_partition(const dht::decorated_key& dk) {
                assert(!_builder);
                _builder = mutation_rebuilder(dk, std::move(_s));
            }

            stop_iteration consume(tombstone t) {
                assert(_builder);
                return _builder->consume(t);
            }

            stop_iteration consume(range_tombstone&& rt) {
                assert(_builder);
                return _builder->consume(std::move(rt));
            }

            stop_iteration consume(static_row&& sr) {
                assert(_builder);
                return _builder->consume(std::move(sr));
            }

            stop_iteration consume(clustering_row&& cr) {
                assert(_builder);
                return _builder->consume(std::move(cr));
            }

            stop_iteration consume_end_of_partition() {
                assert(_builder);
                return stop_iteration::yes;
            }

            void consume_end_of_stream() { }
        };
        fwd_reader.consume(consumer(m.schema(), builder), db::no_timeout).get0();
        BOOST_REQUIRE(bool(builder));
        for (auto&& range : ranges) {
            BOOST_TEST_MESSAGE(sprint("fwd %s", range));
            fwd_reader.fast_forward_to(position_range(range), db::no_timeout).get();
            fwd_reader.consume(consumer(m.schema(), builder), db::no_timeout).get0();
        }
        mutation_opt fwd_m = builder->consume_end_of_stream();
        BOOST_REQUIRE(bool(fwd_m));

        mutation_opt sliced_m = read_mutation_from_flat_mutation_reader(sliced_reader, db::no_timeout).get0();
        BOOST_REQUIRE(bool(sliced_m));
        assert_that(*sliced_m).is_equal_to(*fwd_m, slice_with_ranges.row_ranges(*m.schema(), m.key()));
    }
}

static void test_streamed_mutation_forwarding_guarantees(populate_fn populate) {
    BOOST_TEST_MESSAGE(__PRETTY_FUNCTION__);

    simple_schema table;
    schema_ptr s = table.schema();

    // mutation will include odd keys
    auto contains_key = [] (int i) {
        return i % 2 == 1;
    };

    const int n_keys = 1001;
    assert(!contains_key(n_keys - 1)); // so that we can form a range with position greater than all keys

    mutation m(s, table.make_pkey());
    std::vector<clustering_key> keys;
    for (int i = 0; i < n_keys; ++i) {
        keys.push_back(table.make_ckey(i));
        if (contains_key(i)) {
            table.add_row(m, keys.back(), "value");
        }
    }

    table.add_static_row(m, "static_value");

    mutation_source ms = populate(s, std::vector<mutation>({m}));

    auto new_stream = [&ms, s, &m] () -> flat_reader_assertions {
        BOOST_TEST_MESSAGE("Creating new streamed_mutation");
        auto res = assert_that(ms.make_reader(s,
            query::full_partition_range,
            s->full_slice(),
            default_priority_class(),
            nullptr,
            streamed_mutation::forwarding::yes));
        res.produces_partition_start(m.decorated_key());
        return std::move(res);
    };

    auto verify_range = [&] (flat_reader_assertions& sm, int start, int end) {
        sm.fast_forward_to(keys[start], keys[end]);

        for (; start < end; ++start) {
            if (!contains_key(start)) {
                BOOST_TEST_MESSAGE(sprint("skip %d", start));
                continue;
            }
            sm.produces_row_with_key(keys[start]);
        }
        sm.produces_end_of_stream();
    };

    // Test cases start here

    {
        auto sm = new_stream();
        sm.produces_static_row();
        sm.produces_end_of_stream();
    }

    {
        auto sm = new_stream();
        sm.fast_forward_to(position_range(query::full_clustering_range));
        for (int i = 0; i < n_keys; ++i) {
            if (contains_key(i)) {
                sm.produces_row_with_key(keys[i]);
            }
        }
        sm.produces_end_of_stream();
    }

    {
        auto sm = new_stream();
        verify_range(sm, 0, 1);
        verify_range(sm, 1, 2);
        verify_range(sm, 2, 4);
        verify_range(sm, 7, 7);
        verify_range(sm, 7, 9);
        verify_range(sm, 11, 15);
        verify_range(sm, 21, 32);
        verify_range(sm, 132, 200);
        verify_range(sm, 300, n_keys - 1);
    }

    // Skip before EOS
    {
        auto sm = new_stream();
        sm.fast_forward_to(keys[0], keys[4]);
        sm.produces_row_with_key(keys[1]);
        sm.fast_forward_to(keys[5], keys[8]);
        sm.produces_row_with_key(keys[5]);
        sm.produces_row_with_key(keys[7]);
        sm.produces_end_of_stream();
        sm.fast_forward_to(keys[9], keys[12]);
        sm.fast_forward_to(keys[12], keys[13]);
        sm.fast_forward_to(keys[13], keys[13]);
        sm.produces_end_of_stream();
        sm.fast_forward_to(keys[13], keys[16]);
        sm.produces_row_with_key(keys[13]);
        sm.produces_row_with_key(keys[15]);
        sm.produces_end_of_stream();
    }

    {
        auto sm = new_stream();
        verify_range(sm, n_keys - 2, n_keys - 1);
    }

    {
        auto sm = new_stream();
        verify_range(sm, 0, n_keys - 1);
    }

    // Few random ranges
    std::default_random_engine rnd;
    std::uniform_int_distribution<int> key_dist{0, n_keys - 1};
    for (int i = 0; i < 10; ++i) {
        std::vector<int> indices;
        const int n_ranges = 7;
        for (int j = 0; j < n_ranges * 2; ++j) {
            indices.push_back(key_dist(rnd));
        }
        std::sort(indices.begin(), indices.end());

        auto sm = new_stream();
        for (int j = 0; j < n_ranges; ++j) {
            verify_range(sm, indices[j*2], indices[j*2 + 1]);
        }
    }
}

// Reproduces https://github.com/scylladb/scylla/issues/2733
static void test_fast_forwarding_across_partitions_to_empty_range(populate_fn populate) {
    BOOST_TEST_MESSAGE(__PRETTY_FUNCTION__);

    simple_schema table;
    schema_ptr s = table.schema();

    std::vector<mutation> partitions;

    const unsigned ckeys_per_part = 100;
    auto keys = table.make_pkeys(10);

    auto missing_key = keys[3];
    keys.erase(keys.begin() + 3);

    auto key_after_all = keys.back();
    keys.erase(keys.begin() + (keys.size() - 1));

    unsigned next_ckey = 0;

    for (auto&& key : keys) {
        mutation m(s, key);
        sstring val = make_random_string(1024);
        for (auto i : boost::irange(0u, ckeys_per_part)) {
            table.add_row(m, table.make_ckey(next_ckey + i), val);
        }
        next_ckey += ckeys_per_part;
        partitions.push_back(m);
    }

    mutation_source ms = populate(s, partitions);

    auto pr = dht::partition_range::make({keys[0]}, {keys[1]});
    auto rd = assert_that(ms.make_reader(s,
        pr,
        s->full_slice(),
        default_priority_class(),
        nullptr,
        streamed_mutation::forwarding::no,
        mutation_reader::forwarding::yes));

    rd.fill_buffer().get();
    BOOST_REQUIRE(rd.is_buffer_full()); // if not, increase n_ckeys
    rd.produces_partition_start(keys[0])
        .produces_row_with_key(table.make_ckey(0))
        .produces_row_with_key(table.make_ckey(1));
    // ...don't finish consumption to leave the reader in the middle of partition

    pr = dht::partition_range::make({missing_key}, {missing_key});
    rd.fast_forward_to(pr);

    rd.produces_end_of_stream();

    pr = dht::partition_range::make({keys[3]}, {keys[3]});
    rd.fast_forward_to(pr)
        .produces_partition_start(keys[3])
        .produces_row_with_key(table.make_ckey(ckeys_per_part * 3))
        .produces_row_with_key(table.make_ckey(ckeys_per_part * 3 + 1));

    rd.next_partition();
    rd.produces_end_of_stream();

    pr = dht::partition_range::make_starting_with({keys[keys.size() - 1]});
    rd.fast_forward_to(pr)
        .produces_partition_start(keys.back())
        .produces_row_with_key(table.make_ckey(ckeys_per_part * (keys.size() - 1)));

    // ...don't finish consumption to leave the reader in the middle of partition

    pr = dht::partition_range::make({key_after_all}, {key_after_all});
    rd.fast_forward_to(pr)
        .produces_end_of_stream();
}

static void test_streamed_mutation_slicing_returns_only_relevant_tombstones(populate_fn populate) {
    BOOST_TEST_MESSAGE(__PRETTY_FUNCTION__);

    simple_schema table;
    schema_ptr s = table.schema();

    mutation m(s, table.make_pkey());

    std::vector<clustering_key> keys;
    for (int i = 0; i < 20; ++i) {
        keys.push_back(table.make_ckey(i));
    }

    auto rt1 = table.delete_range(m, query::clustering_range::make(
        query::clustering_range::bound(keys[0], true),
        query::clustering_range::bound(keys[1], true)
    ));

    table.add_row(m, keys[2], "value");

    auto rt2 = table.delete_range(m, query::clustering_range::make(
        query::clustering_range::bound(keys[3], true),
        query::clustering_range::bound(keys[4], true)
    ));

    table.add_row(m, keys[5], "value");

    auto rt3 = table.delete_range(m, query::clustering_range::make(
        query::clustering_range::bound(keys[6], true),
        query::clustering_range::bound(keys[7], true)
    ));

    table.add_row(m, keys[8], "value");

    auto rt4 = table.delete_range(m, query::clustering_range::make(
        query::clustering_range::bound(keys[9], true),
        query::clustering_range::bound(keys[10], true)
    ));

    auto rt5 = table.delete_range(m, query::clustering_range::make(
        query::clustering_range::bound(keys[11], true),
        query::clustering_range::bound(keys[12], true)
    ));

    table.add_row(m, keys[10], "value");

    auto pr = dht::partition_range::make_singular(m.decorated_key());
    mutation_source ms = populate(s, std::vector<mutation>({m}));

    {
        auto slice = partition_slice_builder(*s)
            .with_range(query::clustering_range::make(
                query::clustering_range::bound(keys[2], true),
                query::clustering_range::bound(keys[2], true)
            ))
            .with_range(query::clustering_range::make(
                query::clustering_range::bound(keys[7], true),
                query::clustering_range::bound(keys[9], true)
            ))
            .build();

        auto rd = assert_that(ms.make_reader(s, pr, slice));

        rd.produces_partition_start(m.decorated_key());
        rd.produces_row_with_key(keys[2]);
        rd.produces_range_tombstone(rt3, slice.row_ranges(*s, m.key()));
        rd.produces_row_with_key(keys[8]);
        rd.produces_range_tombstone(rt4, slice.row_ranges(*s, m.key()));
        rd.produces_partition_end();
        rd.produces_end_of_stream();
    }

    {
        auto slice = partition_slice_builder(*s)
            .with_range(query::clustering_range::make(
                query::clustering_range::bound(keys[7], true),
                query::clustering_range::bound(keys[9], true)
            ))
            .build();

        auto rd = assert_that(ms.make_reader(s, pr, slice));

        rd.produces_partition_start(m.decorated_key())
            .produces_range_tombstone(rt3, slice.row_ranges(*s, m.key()))
            .produces_row_with_key(keys[8])
            .produces_range_tombstone(rt4, slice.row_ranges(*s, m.key()))
            .produces_partition_end()
            .produces_end_of_stream();
    }
}

static void test_streamed_mutation_forwarding_across_range_tombstones(populate_fn populate) {
    BOOST_TEST_MESSAGE(__PRETTY_FUNCTION__);

    simple_schema table;
    schema_ptr s = table.schema();

    mutation m(s, table.make_pkey());

    std::vector<clustering_key> keys;
    for (int i = 0; i < 20; ++i) {
        keys.push_back(table.make_ckey(i));
    }

    auto rt1 = table.delete_range(m, query::clustering_range::make(
        query::clustering_range::bound(keys[0], true),
        query::clustering_range::bound(keys[1], false)
    ));

    table.add_row(m, keys[2], "value");

    auto rt2 = table.delete_range(m, query::clustering_range::make(
        query::clustering_range::bound(keys[3], true),
        query::clustering_range::bound(keys[6], true)
    ));

    table.add_row(m, keys[4], "value");

    auto rt3 = table.delete_range(m, query::clustering_range::make(
        query::clustering_range::bound(keys[7], true),
        query::clustering_range::bound(keys[8], true)
    ));

    auto rt4 = table.delete_range(m, query::clustering_range::make(
        query::clustering_range::bound(keys[9], true),
        query::clustering_range::bound(keys[10], true)
    ));

    auto rt5 = table.delete_range(m, query::clustering_range::make(
        query::clustering_range::bound(keys[11], true),
        query::clustering_range::bound(keys[13], true)
    ));

    mutation_source ms = populate(s, std::vector<mutation>({m}));
    auto rd = assert_that(ms.make_reader(s,
        query::full_partition_range,
        s->full_slice(),
        default_priority_class(),
        nullptr,
        streamed_mutation::forwarding::yes));
    rd.produces_partition_start(m.decorated_key());
    rd.fast_forward_to(position_range(query::clustering_range::make(
        query::clustering_range::bound(keys[1], true),
        query::clustering_range::bound(keys[2], true)
    )));

    rd.produces_row_with_key(keys[2]);

    rd.fast_forward_to(position_range(query::clustering_range::make(
        query::clustering_range::bound(keys[4], true),
        query::clustering_range::bound(keys[8], false)
    )));

    rd.produces_range_tombstone(rt2);
    rd.produces_row_with_key(keys[4]);
    rd.produces_range_tombstone(rt3);

    rd.fast_forward_to(position_range(query::clustering_range::make(
        query::clustering_range::bound(keys[10], true),
        query::clustering_range::bound(keys[12], false)
    )));

    rd.produces_range_tombstone(rt4);
    rd.produces_range_tombstone(rt5);
    rd.produces_end_of_stream();

    rd.fast_forward_to(position_range(query::clustering_range::make(
        query::clustering_range::bound(keys[14], true),
        query::clustering_range::bound(keys[15], false)
    )));

    rd.produces_end_of_stream();

    rd.fast_forward_to(position_range(query::clustering_range::make(
        query::clustering_range::bound(keys[15], true),
        query::clustering_range::bound(keys[16], false)
    )));

    rd.produces_end_of_stream();
}

static void test_range_queries(populate_fn populate) {
    BOOST_TEST_MESSAGE("Testing range queries");

    auto s = schema_builder("ks", "cf")
        .with_column("key", bytes_type, column_kind::partition_key)
        .with_column("v", bytes_type)
        .build();

    auto make_partition_mutation = [s] (bytes key) -> mutation {
        mutation m(s, partition_key::from_single_value(*s, key));
        m.set_clustered_cell(clustering_key::make_empty(), "v", data_value(bytes("v1")), 1);
        return m;
    };

    int partition_count = 300;

    auto keys = make_local_keys(partition_count, s);

    std::vector<mutation> partitions;
    for (int i = 0; i < partition_count; ++i) {
        partitions.emplace_back(
            make_partition_mutation(to_bytes(keys[i])));
    }

    std::sort(partitions.begin(), partitions.end(), mutation_decorated_key_less_comparator());
    require_no_token_duplicates(partitions);

    dht::decorated_key key_before_all = partitions.front().decorated_key();
    partitions.erase(partitions.begin());

    dht::decorated_key key_after_all = partitions.back().decorated_key();
    partitions.pop_back();

    auto ds = populate(s, partitions);

    auto test_slice = [&] (dht::partition_range r) {
        BOOST_TEST_MESSAGE(sprint("Testing range %s", r));
        assert_that(ds.make_reader(s, r))
            .produces(slice(partitions, r))
            .produces_end_of_stream();
    };

    auto inclusive_token_range = [&] (size_t start, size_t end) {
        return dht::partition_range::make(
            {dht::ring_position::starting_at(partitions[start].token())},
            {dht::ring_position::ending_at(partitions[end].token())});
    };

    test_slice(dht::partition_range::make(
        {key_before_all, true}, {partitions.front().decorated_key(), true}));

    test_slice(dht::partition_range::make(
        {key_before_all, false}, {partitions.front().decorated_key(), true}));

    test_slice(dht::partition_range::make(
        {key_before_all, false}, {partitions.front().decorated_key(), false}));

    test_slice(dht::partition_range::make(
        {dht::ring_position::starting_at(key_before_all.token())},
        {dht::ring_position::ending_at(partitions.front().token())}));

    test_slice(dht::partition_range::make(
        {dht::ring_position::ending_at(key_before_all.token())},
        {dht::ring_position::ending_at(partitions.front().token())}));

    test_slice(dht::partition_range::make(
        {dht::ring_position::ending_at(key_before_all.token())},
        {dht::ring_position::starting_at(partitions.front().token())}));

    test_slice(dht::partition_range::make(
        {partitions.back().decorated_key(), true}, {key_after_all, true}));

    test_slice(dht::partition_range::make(
        {partitions.back().decorated_key(), true}, {key_after_all, false}));

    test_slice(dht::partition_range::make(
        {partitions.back().decorated_key(), false}, {key_after_all, false}));

    test_slice(dht::partition_range::make(
        {dht::ring_position::starting_at(partitions.back().token())},
        {dht::ring_position::ending_at(key_after_all.token())}));

    test_slice(dht::partition_range::make(
        {dht::ring_position::starting_at(partitions.back().token())},
        {dht::ring_position::starting_at(key_after_all.token())}));

    test_slice(dht::partition_range::make(
        {dht::ring_position::ending_at(partitions.back().token())},
        {dht::ring_position::starting_at(key_after_all.token())}));

    test_slice(dht::partition_range::make(
        {partitions[0].decorated_key(), false},
        {partitions[1].decorated_key(), true}));

    test_slice(dht::partition_range::make(
        {partitions[0].decorated_key(), true},
        {partitions[1].decorated_key(), false}));

    test_slice(dht::partition_range::make(
        {partitions[1].decorated_key(), true},
        {partitions[3].decorated_key(), false}));

    test_slice(dht::partition_range::make(
        {partitions[1].decorated_key(), false},
        {partitions[3].decorated_key(), true}));

    test_slice(dht::partition_range::make_ending_with(
        {partitions[3].decorated_key(), true}));

    test_slice(dht::partition_range::make_starting_with(
        {partitions[partitions.size() - 4].decorated_key(), true}));

    test_slice(inclusive_token_range(0, 0));
    test_slice(inclusive_token_range(1, 1));
    test_slice(inclusive_token_range(2, 4));
    test_slice(inclusive_token_range(127, 128));
    test_slice(inclusive_token_range(128, 128));
    test_slice(inclusive_token_range(128, 129));
    test_slice(inclusive_token_range(127, 129));
    test_slice(inclusive_token_range(partitions.size() - 1, partitions.size() - 1));

    test_slice(inclusive_token_range(0, partitions.size() - 1));
    test_slice(inclusive_token_range(0, partitions.size() - 2));
    test_slice(inclusive_token_range(0, partitions.size() - 3));
    test_slice(inclusive_token_range(0, partitions.size() - 128));

    test_slice(inclusive_token_range(1, partitions.size() - 1));
    test_slice(inclusive_token_range(2, partitions.size() - 1));
    test_slice(inclusive_token_range(3, partitions.size() - 1));
    test_slice(inclusive_token_range(128, partitions.size() - 1));
}

void test_all_data_is_read_back(populate_fn populate) {
    BOOST_TEST_MESSAGE(__PRETTY_FUNCTION__);

    for_each_mutation([&populate] (const mutation& m) mutable {
        auto ms = populate(m.schema(), {m});
        mutation copy(m);
        copy.partition().compact_for_compaction(*copy.schema(), always_gc, gc_clock::now());
        assert_that(ms.make_reader(m.schema())).produces_compacted(copy);
    });
}

void test_mutation_reader_fragments_have_monotonic_positions(populate_fn populate) {
    BOOST_TEST_MESSAGE(__PRETTY_FUNCTION__);

    for_each_mutation([] (const mutation& m) {
        auto rd = flat_mutation_reader_from_mutations({m});
        assert_that(std::move(rd)).has_monotonic_positions();
    });
}

static void test_date_tiered_clustering_slicing(populate_fn populate) {
    BOOST_TEST_MESSAGE(__PRETTY_FUNCTION__);

    simple_schema ss;

    auto s = schema_builder(ss.schema())
        .set_compaction_strategy(sstables::compaction_strategy_type::date_tiered)
        .build();

    auto pkey = ss.make_pkey();

    mutation m1(s, pkey);
    ss.add_static_row(m1, "s");
    m1.partition().apply(ss.new_tombstone());
    ss.add_row(m1, ss.make_ckey(0), "v1");

    mutation_source ms = populate(s, {m1});

    // query row outside the range of existing rows to exercise sstable clustering key filter
    {
        auto slice = partition_slice_builder(*s)
            .with_range(ss.make_ckey_range(1, 2))
            .build();
        auto prange = dht::partition_range::make_singular(pkey);
        assert_that(ms.make_reader(s, prange, slice))
            .produces(m1, slice.row_ranges(*s, pkey.key()))
            .produces_end_of_stream();
    }

    {
        auto slice = partition_slice_builder(*s)
            .with_range(query::clustering_range::make_singular(ss.make_ckey(0)))
            .build();
        auto prange = dht::partition_range::make_singular(pkey);
        assert_that(ms.make_reader(s, prange, slice))
            .produces(m1)
            .produces_end_of_stream();
    }
}

static void test_dropped_column_handling(populate_fn populate) {
    BOOST_TEST_MESSAGE(__PRETTY_FUNCTION__);
    static constexpr api::timestamp_type write_timestamp = 1525385507816568;
    schema_ptr write_schema = schema_builder("ks", "cf")
        .with_column("pk", int32_type, column_kind::partition_key)
        .with_column("ck", int32_type, column_kind::clustering_key)
        .with_column("val1", int32_type)
        .with_column("val2", int32_type)
        .build();
    schema_ptr read_schema = schema_builder("ks", "cf")
        .with_column("pk", int32_type, column_kind::partition_key)
        .with_column("ck", int32_type, column_kind::clustering_key)
        .with_column("val2", int32_type)
        .without_column("val1", int32_type, write_timestamp + 1)
        .build();
    auto val2_cdef = read_schema->get_column_definition(to_bytes("val2"));
    auto to_ck = [write_schema] (int ck) {
        return clustering_key::from_single_value(*write_schema, int32_type->decompose(ck));
    };
    auto bytes = int32_type->decompose(int32_t(0));
    auto pk = partition_key::from_single_value(*write_schema, bytes);
    auto dk = dht::global_partitioner().decorate_key(*write_schema, pk);
    mutation partition(write_schema, pk);
    auto add_row = [&partition, &to_ck, write_schema] (int ck, int v1, int v2) {
        clustering_key ckey = to_ck(ck);
        partition.partition().apply_insert(*write_schema, ckey, write_timestamp);
        partition.set_cell(ckey, "val1", data_value{v1}, write_timestamp);
        partition.set_cell(ckey, "val2", data_value{v2}, write_timestamp);
    };
    add_row(1, 101, 201);
    add_row(2, 102, 202);
    add_row(3, 103, 203);
    assert_that(populate(write_schema, {partition}).make_reader(read_schema))
        .produces_partition_start(dk)
        .produces_row(to_ck(1), {{val2_cdef, int32_type->decompose(int32_t(201))}})
        .produces_row(to_ck(2), {{val2_cdef, int32_type->decompose(int32_t(202))}})
        .produces_row(to_ck(3), {{val2_cdef, int32_type->decompose(int32_t(203))}})
        .produces_partition_end()
        .produces_end_of_stream();
}

static void test_clustering_slices(populate_fn populate) {
    BOOST_TEST_MESSAGE(__PRETTY_FUNCTION__);
    auto s = schema_builder("ks", "cf")
        .with_column("key", bytes_type, column_kind::partition_key)
        .with_column("c1", int32_type, column_kind::clustering_key)
        .with_column("c2", int32_type, column_kind::clustering_key)
        .with_column("c3", int32_type, column_kind::clustering_key)
        .with_column("v", bytes_type)
        .build();

    auto make_ck = [&] (int ck1, stdx::optional<int> ck2 = stdx::nullopt, stdx::optional<int> ck3 = stdx::nullopt) {
        std::vector<data_value> components;
        components.push_back(data_value(ck1));
        if (ck2) {
            components.push_back(data_value(ck2));
        }
        if (ck3) {
            components.push_back(data_value(ck3));
        }
        return clustering_key::from_deeply_exploded(*s, components);
    };

    auto make_pk = [&] (sstring key) {
        return dht::global_partitioner().decorate_key(*s, partition_key::from_single_value(*s, to_bytes(key)));
    };

    auto partition_count = 3;
    auto local_keys = make_local_keys(partition_count, s);
    std::vector<dht::decorated_key> keys;
    for (int i = 0; i < partition_count; ++i) {
        keys.push_back(make_pk(local_keys[i]));
    }
    std::sort(keys.begin(), keys.end(), dht::ring_position_less_comparator(*s));

    auto pk = keys[1];

    auto make_row = [&] (clustering_key k, int v) {
        mutation m(s, pk);
        m.set_clustered_cell(k, "v", data_value(bytes("v1")), v);
        return m;
    };

    auto make_delete = [&] (const query::clustering_range& r) {
        mutation m(s, pk);
        auto bv_range = bound_view::from_range(r);
        range_tombstone rt(bv_range.first, bv_range.second, tombstone(new_timestamp(), gc_clock::now()));
        m.partition().apply_delete(*s, rt);
        return m;
    };

    auto ck1 = make_ck(1, 1, 1);
    auto ck2 = make_ck(1, 1, 2);
    auto ck3 = make_ck(1, 2, 1);
    auto ck4 = make_ck(1, 2, 2);
    auto ck5 = make_ck(1, 3, 1);
    auto ck6 = make_ck(2, 1, 1);
    auto ck7 = make_ck(2, 1, 2);
    auto ck8 = make_ck(3, 1, 1);

    mutation row1 = make_row(ck1, 1);
    mutation row2 = make_row(ck2, 2);
    mutation row3 = make_row(ck3, 3);
    mutation row4 = make_row(ck4, 4);
    mutation del_1 = make_delete(query::clustering_range::make({make_ck(1, 2, 1), true}, {make_ck(2, 0, 0), true}));
    mutation row5 = make_row(ck5, 5);
    mutation del_2 = make_delete(query::clustering_range::make({make_ck(2, 1), true}, {make_ck(2), true}));
    mutation row6 = make_row(ck6, 6);
    mutation row7 = make_row(ck7, 7);
    mutation del_3 = make_delete(query::clustering_range::make({make_ck(3), true}, {make_ck(3), true}));
    mutation row8 = make_row(ck8, 8);

    mutation m = row1 + row2 + row3 + row4 + row5 + row6 + row7 + del_1 + del_2 + row8 + del_3;

    mutation_source ds = populate(s, {m});

    auto pr = dht::partition_range::make_singular(pk);

    {
        auto slice = partition_slice_builder(*s)
            .with_range(query::clustering_range::make_singular(make_ck(0)))
            .build();
        assert_that(ds.make_reader(s, pr, slice))
            .produces_eos_or_empty_mutation();
    }

    {
        auto slice = partition_slice_builder(*s)
            .build();
        auto rd = assert_that(ds.make_reader(s, pr, slice, default_priority_class(), nullptr, streamed_mutation::forwarding::yes));
        rd.produces_partition_start(pk)
          .fast_forward_to(position_range(position_in_partition::for_key(ck1), position_in_partition::after_key(ck2)))
          .produces_row_with_key(ck1)
          .produces_row_with_key(ck2)
          .produces_end_of_stream();
    }

    {
        auto slice = partition_slice_builder(*s)
            .build();
        auto rd = assert_that(ds.make_reader(s, pr, slice, default_priority_class(), nullptr, streamed_mutation::forwarding::yes));
        rd.produces_partition_start(pk)
          .produces_end_of_stream()
          .fast_forward_to(position_range(position_in_partition::for_key(ck1), position_in_partition::after_key(ck2)))
          .produces_row_with_key(ck1)
          .produces_row_with_key(ck2)
          .produces_end_of_stream();
    }
    {
        auto slice = partition_slice_builder(*s)
            .with_range(query::clustering_range::make_singular(make_ck(1)))
            .build();
        assert_that(ds.make_reader(s, pr, slice))
            .produces(row1 + row2 + row3 + row4 + row5 + del_1, slice.row_ranges(*s, pk.key()))
            .produces_end_of_stream();
    }
    {
        auto slice = partition_slice_builder(*s)
            .with_range(query::clustering_range::make_singular(make_ck(2)))
            .build();
        assert_that(ds.make_reader(s, pr, slice))
            .produces(row6 + row7 + del_1 + del_2, slice.row_ranges(*s, pk.key()))
            .produces_end_of_stream();
    }

    {
        auto slice = partition_slice_builder(*s)
            .with_range(query::clustering_range::make_singular(make_ck(1, 2)))
            .build();
        assert_that(ds.make_reader(s, pr, slice))
            .produces(row3 + row4 + del_1, slice.row_ranges(*s, pk.key()))
            .produces_end_of_stream();
    }

    {
        auto slice = partition_slice_builder(*s)
            .with_range(query::clustering_range::make_singular(make_ck(3)))
            .build();
        assert_that(ds.make_reader(s, pr, slice))
            .produces(row8 + del_3, slice.row_ranges(*s, pk.key()))
            .produces_end_of_stream();
    }

    // Test out-of-range partition keys
    {
        auto pr = dht::partition_range::make_singular(keys[0]);
        assert_that(ds.make_reader(s, pr, s->full_slice()))
            .produces_eos_or_empty_mutation();
    }
    {
        auto pr = dht::partition_range::make_singular(keys[2]);
        assert_that(ds.make_reader(s, pr, s->full_slice()))
            .produces_eos_or_empty_mutation();
    }
}

static void test_query_only_static_row(populate_fn populate) {
    simple_schema s;

    auto pkeys = s.make_pkeys(1);

    mutation m1(s.schema(), pkeys[0]);
    m1.partition().apply(s.new_tombstone());
    s.add_static_row(m1, "s1");
    s.add_row(m1, s.make_ckey(0), "v1");
    s.add_row(m1, s.make_ckey(1), "v2");

    mutation_source ms = populate(s.schema(), {m1});

    // fully populate cache
    {
        auto prange = dht::partition_range::make_ending_with(dht::ring_position(m1.decorated_key()));
        assert_that(ms.make_reader(s.schema(), prange, s.schema()->full_slice()))
            .produces(m1)
            .produces_end_of_stream();
    }

    // query just a static row
    {
        auto slice = partition_slice_builder(*s.schema())
            .with_ranges({})
            .build();
        auto prange = dht::partition_range::make_ending_with(dht::ring_position(m1.decorated_key()));
        assert_that(ms.make_reader(s.schema(), prange, slice))
            .produces(m1, slice.row_ranges(*s.schema(), m1.key()))
            .produces_end_of_stream();
    }

    // query just a static row, single-partition case
    {
        auto slice = partition_slice_builder(*s.schema())
            .with_ranges({})
            .build();
        auto prange = dht::partition_range::make_singular(m1.decorated_key());
        assert_that(ms.make_reader(s.schema(), prange, slice))
            .produces(m1, slice.row_ranges(*s.schema(), m1.key()))
            .produces_end_of_stream();
    }
}

static void test_query_no_clustering_ranges_no_static_columns(populate_fn populate) {
    simple_schema s(simple_schema::with_static::no);

    auto pkeys = s.make_pkeys(1);

    mutation m1(s.schema(), pkeys[0]);
    m1.partition().apply(s.new_tombstone());
    s.add_row(m1, s.make_ckey(0), "v1");
    s.add_row(m1, s.make_ckey(1), "v2");

    mutation_source ms = populate(s.schema(), {m1});

    {
        auto prange = dht::partition_range::make_ending_with(dht::ring_position(m1.decorated_key()));
        assert_that(ms.make_reader(s.schema(), prange, s.schema()->full_slice()))
            .produces(m1)
            .produces_end_of_stream();
    }

    // multi-partition case
    {
        auto slice = partition_slice_builder(*s.schema())
            .with_ranges({})
            .build();
        auto prange = dht::partition_range::make_ending_with(dht::ring_position(m1.decorated_key()));
        assert_that(ms.make_reader(s.schema(), prange, slice))
            .produces(m1, slice.row_ranges(*s.schema(), m1.key()))
            .produces_end_of_stream();
    }

    // single-partition case
    {
        auto slice = partition_slice_builder(*s.schema())
            .with_ranges({})
            .build();
        auto prange = dht::partition_range::make_singular(m1.decorated_key());
        assert_that(ms.make_reader(s.schema(), prange, slice))
            .produces(m1, slice.row_ranges(*s.schema(), m1.key()))
            .produces_end_of_stream();
    }
}

void test_streamed_mutation_forwarding_succeeds_with_no_data(populate_fn populate) {
    simple_schema s;
    auto cks = s.make_ckeys(6);

    auto pkey = s.make_pkey();
    mutation m(s.schema(), pkey);
    s.add_row(m, cks[0], "data");

    auto source = populate(s.schema(), {m});
    assert_that(source.make_reader(s.schema(),
                query::full_partition_range,
                s.schema()->full_slice(),
                default_priority_class(),
                nullptr,
                streamed_mutation::forwarding::yes
                ))
        .produces_partition_start(pkey)
        .produces_end_of_stream()
        .fast_forward_to(position_range(position_in_partition::for_key(cks[0]), position_in_partition::before_key(cks[1])))
        .produces_row_with_key(cks[0])
        .produces_end_of_stream()
        .fast_forward_to(position_range(position_in_partition::for_key(cks[1]), position_in_partition::before_key(cks[3])))
        .produces_end_of_stream()
        .fast_forward_to(position_range(position_in_partition::for_key(cks[4]), position_in_partition::before_key(cks[5])))
        .produces_end_of_stream()
        .next_partition()
        .produces_end_of_stream()
        .fast_forward_to(position_range(position_in_partition::for_key(cks[0]), position_in_partition::before_key(cks[1])))
        .produces_end_of_stream()
        .fast_forward_to(position_range(position_in_partition::for_key(cks[1]), position_in_partition::before_key(cks[3])))
        .produces_end_of_stream()
        .fast_forward_to(position_range(position_in_partition::for_key(cks[4]), position_in_partition::before_key(cks[5])))
        .produces_end_of_stream();
}

static
void test_slicing_with_overlapping_range_tombstones(populate_fn populate, streamed_mutation::forwarding fwd_sm) {
    simple_schema ss;
    auto s = ss.schema();

    auto rt1 = ss.make_range_tombstone(ss.make_ckey_range(1, 10));
    auto rt2 = ss.make_range_tombstone(ss.make_ckey_range(1, 5)); // rt1 + rt2 = {[1, 5], (5, 10]}

    auto key = make_local_key(s);
    mutation m1 = ss.new_mutation(key);
    m1.partition().apply_delete(*s, rt1);

    mutation m2 = ss.new_mutation(key);
    m2.partition().apply_delete(*s, rt2);
    ss.add_row(m2, ss.make_ckey(4), "v2"); // position after rt2.position() but before rt2.end_position().

    mutation_source ds = populate(s, {m1, m2});

    // upper bound ends before the row in m2, so that the raw is fetched after next fast forward.
    auto range = ss.make_ckey_range(0, 3);

    {
        auto slice = partition_slice_builder(*s).with_range(range).build();
        auto rd = ds.make_reader(s, query::full_partition_range, slice);

        auto prange = position_range(range);
        mutation result(m1.schema(), m1.decorated_key());

        rd.consume_pausable([&] (mutation_fragment&& mf) {
            if (mf.position().has_clustering_key() && !mf.range().overlaps(*s, prange.start(), prange.end())) {
                BOOST_FAIL(sprint("Received fragment which is not relevant for the slice: %s, slice: %s", mf, range));
            }
            result.partition().apply(*s, std::move(mf));
            return stop_iteration::no;
        }, db::no_timeout).get();

        assert_that(result).is_equal_to(m1 + m2, query::clustering_row_ranges({range}));
    }

    if (!fwd_sm) {
        return;
    }

    // Check fast_forward_to()
    {
        auto rd = ds.make_reader(s, query::full_partition_range, s->full_slice(), default_priority_class(),
            nullptr, streamed_mutation::forwarding::yes);

        auto prange = position_range(range);
        mutation result(m1.schema(), m1.decorated_key());

        rd.consume_pausable([&](mutation_fragment&& mf) {
            BOOST_REQUIRE(!mf.position().has_clustering_key());
            result.partition().apply(*s, std::move(mf));
            return stop_iteration::no;
        }, db::no_timeout).get();

        rd.fast_forward_to(prange, db::no_timeout).get();

        position_in_partition last_pos = position_in_partition::before_all_clustered_rows();
        auto consume_clustered = [&] (mutation_fragment&& mf) {
            position_in_partition::less_compare less(*s);
            if (less(mf.position(), last_pos)) {
                BOOST_FAIL(sprint("Out of order fragment: %s, last pos: %s", mf, last_pos));
            }
            last_pos = position_in_partition(mf.position());
            result.partition().apply(*s, std::move(mf));
            return stop_iteration::no;
        };

        rd.consume_pausable(consume_clustered, db::no_timeout).get();
        rd.fast_forward_to(position_range(prange.end(), position_in_partition::after_all_clustered_rows()), db::no_timeout).get();
        rd.consume_pausable(consume_clustered, db::no_timeout).get();

        assert_that(result).is_equal_to(m1 + m2);
    }
}

void run_mutation_reader_tests(populate_fn populate, streamed_mutation::forwarding fwd_sm) {
    test_date_tiered_clustering_slicing(populate);
    test_fast_forwarding_across_partitions_to_empty_range(populate);
    if (fwd_sm) {
        test_clustering_slices(populate);
    }
    test_mutation_reader_fragments_have_monotonic_positions(populate);
    if (fwd_sm) {
        test_streamed_mutation_forwarding_across_range_tombstones(populate);
        test_streamed_mutation_forwarding_guarantees(populate);
    }
    test_all_data_is_read_back(populate);
    test_streamed_mutation_slicing_returns_only_relevant_tombstones(populate);
    if (fwd_sm) {
        test_streamed_mutation_forwarding_is_consistent_with_slicing(populate);
    }
    test_range_queries(populate);
    test_query_only_static_row(populate);
    test_query_no_clustering_ranges_no_static_columns(populate);
    test_dropped_column_handling(populate);
}

void test_next_partition(populate_fn populate) {
    simple_schema s;
    auto pkeys = s.make_pkeys(4);

    std::vector<mutation> mutations;
    for (auto key : pkeys) {
        mutation m(s.schema(), key);
        s.add_static_row(m, "s1");
        s.add_row(m, s.make_ckey(0), "v1");
        s.add_row(m, s.make_ckey(1), "v2");
        mutations.push_back(std::move(m));
    }
    auto source = populate(s.schema(), mutations);
    assert_that(source.make_reader(s.schema()))
        .next_partition() // Does nothing before first partition
        .produces_partition_start(pkeys[0])
        .produces_static_row()
        .produces_row_with_key(s.make_ckey(0))
        .produces_row_with_key(s.make_ckey(1))
        .produces_partition_end()
        .next_partition() // Does nothing between partitions
        .produces_partition_start(pkeys[1])
        .next_partition() // Moves to next partition
        .produces_partition_start(pkeys[2])
        .produces_static_row()
        .next_partition()
        .produces_partition_start(pkeys[3])
        .produces_static_row()
        .produces_row_with_key(s.make_ckey(0))
        .next_partition()
        .produces_end_of_stream();
}

void run_flat_mutation_reader_tests(populate_fn populate, streamed_mutation::forwarding fwd_sm) {
    test_next_partition(populate);
    if (fwd_sm) {
        test_streamed_mutation_forwarding_succeeds_with_no_data(populate);
    }
    test_slicing_with_overlapping_range_tombstones(populate, fwd_sm);
}

void run_mutation_source_tests(populate_fn populate, streamed_mutation::forwarding fwd_sm) {
    run_mutation_reader_tests(populate, fwd_sm);
    run_flat_mutation_reader_tests(populate, fwd_sm);
}

struct mutation_sets {
    std::vector<std::vector<mutation>> equal;
    std::vector<std::vector<mutation>> unequal;
    mutation_sets(){}
};

static tombstone new_tombstone() {
    return { new_timestamp(), gc_clock::now() };
}

static mutation_sets generate_mutation_sets() {
    using mutations = std::vector<mutation>;
    mutation_sets result;

    {
        auto common_schema = schema_builder("ks", "test")
                .with_column("pk_col", bytes_type, column_kind::partition_key)
                .with_column("ck_col_1", bytes_type, column_kind::clustering_key)
                .with_column("ck_col_2", bytes_type, column_kind::clustering_key)
                .with_column("regular_col_1", bytes_type)
                .with_column("regular_col_2", bytes_type)
                .with_column("static_col_1", bytes_type, column_kind::static_column)
                .with_column("static_col_2", bytes_type, column_kind::static_column);

        auto s1 = common_schema
                .with_column("regular_col_1_s1", bytes_type) // will have id in between common columns
                .build();

        auto s2 = common_schema
                .with_column("regular_col_1_s2", bytes_type) // will have id in between common columns
                .build();

        auto local_keys = make_local_keys(2, s1); // use only one schema as s1 and s2 don't differ in representation.
        auto& key1 = local_keys[0];
        auto& key2 = local_keys[1];

        // Differing keys
        result.unequal.emplace_back(mutations{
            mutation(s1, partition_key::from_single_value(*s1, to_bytes(key1))),
            mutation(s2, partition_key::from_single_value(*s2, to_bytes(key2)))
        });

        auto m1 = mutation(s1, partition_key::from_single_value(*s1, to_bytes(key1)));
        auto m2 = mutation(s2, partition_key::from_single_value(*s2, to_bytes(key1)));
        result.equal.emplace_back(mutations{m1, m2});

        clustering_key ck1 = clustering_key::from_deeply_exploded(*s1, {data_value(bytes("ck1_0")), data_value(bytes("ck1_1"))});
        clustering_key ck2 = clustering_key::from_deeply_exploded(*s1, {data_value(bytes("ck2_0")), data_value(bytes("ck2_1"))});
        auto ttl = gc_clock::duration(1);

        {
            auto tomb = new_tombstone();
            m1.partition().apply(tomb);
            result.unequal.emplace_back(mutations{m1, m2});
            m2.partition().apply(tomb);
            result.equal.emplace_back(mutations{m1, m2});
        }

        {
            auto tomb = new_tombstone();
            m1.partition().apply_delete(*s1, ck2, tomb);
            result.unequal.emplace_back(mutations{m1, m2});
            m2.partition().apply_delete(*s1, ck2, tomb);
            result.equal.emplace_back(mutations{m1, m2});
        }

        {
            auto tomb = new_tombstone();
            auto key = clustering_key_prefix::from_deeply_exploded(*s1, {data_value(bytes("ck2_0"))});
            m1.partition().apply_row_tombstone(*s1, key, tomb);
            result.unequal.emplace_back(mutations{m1, m2});
            m2.partition().apply_row_tombstone(*s1, key, tomb);
            result.equal.emplace_back(mutations{m1, m2});
        }

        {
            auto ts = new_timestamp();
            m1.set_clustered_cell(ck1, "regular_col_1", data_value(bytes("regular_col_value")), ts, ttl);
            result.unequal.emplace_back(mutations{m1, m2});
            m2.set_clustered_cell(ck1, "regular_col_1", data_value(bytes("regular_col_value")), ts, ttl);
            result.equal.emplace_back(mutations{m1, m2});
        }

        {
            auto ts = new_timestamp();
            m1.set_clustered_cell(ck1, "regular_col_2", data_value(bytes("regular_col_value")), ts, ttl);
            result.unequal.emplace_back(mutations{m1, m2});
            m2.set_clustered_cell(ck1, "regular_col_2", data_value(bytes("regular_col_value")), ts, ttl);
            result.equal.emplace_back(mutations{m1, m2});
        }

        {
            auto ts = new_timestamp();
            m1.partition().apply_insert(*s1, ck2, ts);
            result.unequal.emplace_back(mutations{m1, m2});
            m2.partition().apply_insert(*s1, ck2, ts);
            result.equal.emplace_back(mutations{m1, m2});
        }

        {
            auto ts = new_timestamp();
            m1.set_clustered_cell(ck2, "regular_col_1", data_value(bytes("ck2_regular_col_1_value")), ts);
            result.unequal.emplace_back(mutations{m1, m2});
            m2.set_clustered_cell(ck2, "regular_col_1", data_value(bytes("ck2_regular_col_1_value")), ts);
            result.equal.emplace_back(mutations{m1, m2});
        }

        {
            auto ts = new_timestamp();
            m1.set_static_cell("static_col_1", data_value(bytes("static_col_value")), ts, ttl);
            result.unequal.emplace_back(mutations{m1, m2});
            m2.set_static_cell("static_col_1", data_value(bytes("static_col_value")), ts, ttl);
            result.equal.emplace_back(mutations{m1, m2});
        }

        {
            auto ts = new_timestamp();
            m1.set_static_cell("static_col_2", data_value(bytes("static_col_value")), ts);
            result.unequal.emplace_back(mutations{m1, m2});
            m2.set_static_cell("static_col_2", data_value(bytes("static_col_value")), ts);
            result.equal.emplace_back(mutations{m1, m2});
        }

        {
            m1.partition().ensure_last_dummy(*m1.schema());
            result.equal.emplace_back(mutations{m1, m2});

            m2.partition().ensure_last_dummy(*m2.schema());
            result.equal.emplace_back(mutations{m1, m2});
        }

        {
            auto ts = new_timestamp();
            m1.set_clustered_cell(ck2, "regular_col_1_s1", data_value(bytes("x")), ts);
            result.unequal.emplace_back(mutations{m1, m2});
            m2.set_clustered_cell(ck2, "regular_col_1_s2", data_value(bytes("x")), ts);
            result.unequal.emplace_back(mutations{m1, m2});
        }
    }

    static constexpr auto rmg_iterations = 10;

    {
        random_mutation_generator gen(random_mutation_generator::generate_counters::no);
        for (int i = 0; i < rmg_iterations; ++i) {
            auto m = gen();
            result.unequal.emplace_back(mutations{m, gen()}); // collision unlikely
            result.equal.emplace_back(mutations{m, m});
        }
    }

    {
        random_mutation_generator gen(random_mutation_generator::generate_counters::yes);
        for (int i = 0; i < rmg_iterations; ++i) {
            auto m = gen();
            result.unequal.emplace_back(mutations{m, gen()}); // collision unlikely
            result.equal.emplace_back(mutations{m, m});
        }
    }

    return result;
}

static const mutation_sets& get_mutation_sets() {
    static thread_local const auto ms = generate_mutation_sets();
    return ms;
}

void for_each_mutation_pair(std::function<void(const mutation&, const mutation&, are_equal)> callback) {
    auto&& ms = get_mutation_sets();
    for (auto&& mutations : ms.equal) {
        auto i = mutations.begin();
        assert(i != mutations.end());
        const mutation& first = *i++;
        while (i != mutations.end()) {
            callback(first, *i, are_equal::yes);
            ++i;
        }
    }
    for (auto&& mutations : ms.unequal) {
        auto i = mutations.begin();
        assert(i != mutations.end());
        const mutation& first = *i++;
        while (i != mutations.end()) {
            callback(first, *i, are_equal::no);
            ++i;
        }
    }
}

void for_each_mutation(std::function<void(const mutation&)> callback) {
    auto&& ms = get_mutation_sets();
    for (auto&& mutations : ms.equal) {
        for (auto&& m : mutations) {
            callback(m);
        }
    }
    for (auto&& mutations : ms.unequal) {
        for (auto&& m : mutations) {
            callback(m);
        }
    }
}

bytes make_blob(size_t blob_size) {
    static thread_local std::independent_bits_engine<std::default_random_engine, 8, uint8_t> random_bytes;
    bytes big_blob(bytes::initialized_later(), blob_size);
    for (auto&& b : big_blob) {
        b = random_bytes();
    }
    return big_blob;
};

class random_mutation_generator::impl {
    friend class random_mutation_generator;
    generate_counters _generate_counters;
    local_shard_only _local_shard_only;
    const size_t _external_blob_size = 128; // Should be enough to force use of external bytes storage
    const size_t n_blobs = 1024;
    const column_id column_count = row::max_vector_size * 2;
    std::mt19937 _gen;
    schema_ptr _schema;
    std::vector<bytes> _blobs;
    std::uniform_int_distribution<size_t> _ck_index_dist{0, n_blobs - 1};
    std::uniform_int_distribution<int> _bool_dist{0, 1};
    std::uniform_int_distribution<int> _not_dummy_dist{0, 19};

    template <typename Generator>
    static gc_clock::time_point expiry_dist(Generator& gen) {
        static thread_local std::uniform_int_distribution<int> dist(0, 2);
        return gc_clock::time_point() + std::chrono::seconds(dist(gen));
    }

    schema_ptr do_make_schema(data_type type) {
        auto builder = schema_builder("ks", "cf")
                .with_column("pk", bytes_type, column_kind::partition_key)
                .with_column("ck1", bytes_type, column_kind::clustering_key)
                .with_column("ck2", bytes_type, column_kind::clustering_key);

        // Create enough columns so that row can overflow its vector storage
        for (column_id i = 0; i < column_count; ++i) {
            {
                auto column_name = sprint("v%d", i);
                auto col_type = type == counter_type || _bool_dist(_gen) ? type : list_type_impl::get_instance(type, true);
                builder.with_column(to_bytes(column_name), col_type, column_kind::regular_column);
            }
            {
                auto column_name = sprint("s%d", i);
                builder.with_column(to_bytes(column_name), type, column_kind::static_column);
            }
        }

        return builder.build();
    }

    schema_ptr make_schema() {
        return _generate_counters ? do_make_schema(counter_type)
                                  : do_make_schema(bytes_type);
    }
public:
    explicit impl(generate_counters counters, local_shard_only lso = local_shard_only::yes) : _generate_counters(counters), _local_shard_only(lso) {
        std::random_device rd;
        // In case of errors, replace the seed with a fixed value to get a deterministic run.
        auto seed = rd();
        std::cout << "Random seed: " << seed << "\n";
        _gen = std::mt19937(seed);

        _schema = make_schema();

        auto keys = _local_shard_only ? make_local_keys(n_blobs, _schema, _external_blob_size) : make_keys(n_blobs, _schema, _external_blob_size);
        _blobs =  boost::copy_range<std::vector<bytes>>(keys | boost::adaptors::transformed([this] (sstring& k) { return to_bytes(k); }));
    }

    bytes random_blob() {
        return _blobs[std::min(_blobs.size() - 1, std::max<size_t>(0, _ck_index_dist(_gen)))];
    }

    clustering_key make_random_key() {
        return clustering_key::from_exploded(*_schema, { random_blob(), random_blob() });
    }

    clustering_key_prefix make_random_prefix() {
        std::vector<bytes> components = { random_blob() };
        if (_bool_dist(_gen)) {
            components.push_back(random_blob());
        }
        return clustering_key_prefix::from_exploded(*_schema, std::move(components));
    }

    std::vector<query::clustering_range> make_random_ranges(unsigned n_ranges) {
        std::vector<query::clustering_range> ranges;

        if (n_ranges == 0) {
            return ranges;
        }

        auto keys = std::set<clustering_key, clustering_key::less_compare>{clustering_key::less_compare(*_schema)};
        while (keys.size() < n_ranges * 2) {
            keys.insert(make_random_key());
        }

        auto i = keys.begin();

        bool open_start = _bool_dist(_gen);
        bool open_end = _bool_dist(_gen);

        if (open_start && open_end && n_ranges == 1) {
            ranges.push_back(query::clustering_range::make_open_ended_both_sides());
            return ranges;
        }

        if (open_start) {
            ranges.push_back(query::clustering_range(
                { }, { query::clustering_range::bound(*i++, _bool_dist(_gen)) }
            ));
        }

        n_ranges -= unsigned(open_start);
        n_ranges -= unsigned(open_end);

        while (n_ranges--) {
            auto start_key = *i++;
            auto end_key = *i++;
            ranges.push_back(query::clustering_range(
                { query::clustering_range::bound(start_key, _bool_dist(_gen)) },
                { query::clustering_range::bound(end_key, _bool_dist(_gen)) }
            ));
        }

        if (open_end) {
            ranges.push_back(query::clustering_range(
                { query::clustering_range::bound(*i++, _bool_dist(_gen)) }, { }
            ));
        }

        return ranges;
    }

    mutation operator()() {
        std::uniform_int_distribution<column_id> column_count_dist(1, column_count);
        std::uniform_int_distribution<column_id> column_id_dist(0, column_count - 1);
        std::uniform_int_distribution<size_t> value_blob_index_dist(0, 2);

        std::uniform_int_distribution<api::timestamp_type> timestamp_dist(api::min_timestamp, api::min_timestamp + 2); // 3 values

        auto pkey = partition_key::from_single_value(*_schema, _blobs[0]);
        mutation m(_schema, pkey);

        std::map<counter_id, std::set<int64_t>> counter_used_clock_values;
        std::vector<counter_id> counter_ids;
        std::generate_n(std::back_inserter(counter_ids), 8, counter_id::generate_random);

        auto random_counter_cell = [&] {
            std::uniform_int_distribution<size_t> shard_count_dist(1, counter_ids.size());
            std::uniform_int_distribution<int64_t> value_dist(-100, 100);
            std::uniform_int_distribution<int64_t> clock_dist(0, 20000);

            auto shard_count = shard_count_dist(_gen);
            std::set<counter_id> shards;
            for (auto i = 0u; i < shard_count; i++) {
                shards.emplace(counter_ids[shard_count_dist(_gen) - 1]);
            }

            counter_cell_builder ccb;
            for (auto&& id : shards) {
                // Make sure we don't get shards with the same id and clock
                // but different value.
                int64_t clock = clock_dist(_gen);
                while (counter_used_clock_values[id].count(clock)) {
                    clock = clock_dist(_gen);
                }
                counter_used_clock_values[id].emplace(clock);
                ccb.add_shard(counter_shard(id, value_dist(_gen), clock));
            }
            return ccb.build(timestamp_dist(_gen));
        };

        auto set_random_cells = [&] (row& r, column_kind kind) {
            auto columns_to_set = column_count_dist(_gen);
            for (column_id i = 0; i < columns_to_set; ++i) {
                auto cid = column_id_dist(_gen);
                auto& col = _schema->column_at(kind, cid);
                auto get_live_cell = [&] () -> atomic_cell_or_collection {
                    if (_generate_counters) {
                        return random_counter_cell();
                    }
                    if (col.is_atomic()) {
                        return atomic_cell::make_live(*col.type, timestamp_dist(_gen), _blobs[value_blob_index_dist(_gen)]);
                    }
                    static thread_local std::uniform_int_distribution<int> element_dist{1, 13};
                    static thread_local std::uniform_int_distribution<int64_t> uuid_ts_dist{-12219292800000L, -12219292800000L + 1000};
                    collection_type_impl::mutation m;
                    auto num_cells = element_dist(_gen);
                    m.cells.reserve(num_cells);
                    std::unordered_set<bytes> unique_cells;
                    unique_cells.reserve(num_cells);
                    auto ctype = static_pointer_cast<const collection_type_impl>(col.type);
                    for (auto i = 0; i < num_cells; ++i) {
                        auto uuid = utils::UUID_gen::min_time_UUID(uuid_ts_dist(_gen)).serialize();
                        if (unique_cells.emplace(uuid).second) {
                            m.cells.emplace_back(
                                bytes(reinterpret_cast<const int8_t*>(uuid.data()), uuid.size()),
                                atomic_cell::make_live(*ctype->value_comparator(), timestamp_dist(_gen), _blobs[value_blob_index_dist(_gen)],
                                    atomic_cell::collection_member::yes));
                        }
                    }
                    std::sort(m.cells.begin(), m.cells.end(), [] (auto&& c1, auto&& c2) {
                            return timeuuid_type->as_less_comparator()(c1.first, c2.first);
                    });
                    return ctype->serialize_mutation_form(m);
                };
                auto get_dead_cell = [&] () -> atomic_cell_or_collection{
                    if (col.is_atomic() || col.is_counter()) {
                        return atomic_cell::make_dead(timestamp_dist(_gen), expiry_dist(_gen));
                    }
                    collection_type_impl::mutation m;
                    m.tomb = tombstone(timestamp_dist(_gen), expiry_dist(_gen));
                    return static_pointer_cast<const collection_type_impl>(col.type)->serialize_mutation_form(m);

                };
                // FIXME: generate expiring cells
                auto cell = _bool_dist(_gen) ? get_live_cell() : get_dead_cell();
                r.apply(_schema->column_at(kind, cid), std::move(cell));
            }
        };

        auto random_tombstone = [&] {
            return tombstone(timestamp_dist(_gen), expiry_dist(_gen));
        };

        auto random_row_marker = [&] {
            static thread_local std::uniform_int_distribution<int> dist(0, 3);
            switch (dist(_gen)) {
                case 0: return row_marker();
                case 1: return row_marker(random_tombstone());
                case 2: return row_marker(timestamp_dist(_gen));
                case 3: return row_marker(timestamp_dist(_gen), std::chrono::seconds(1), expiry_dist(_gen));
                default: assert(0);
            }
            abort();
        };

        if (_bool_dist(_gen)) {
            m.partition().apply(random_tombstone());
        }

        m.partition().set_static_row_continuous(_bool_dist(_gen));

        set_random_cells(m.partition().static_row(), column_kind::static_column);

        auto row_count_dist = [&] (auto& gen) {
            static thread_local std::normal_distribution<> dist(32, 1.5);
            return static_cast<size_t>(std::min(100.0, std::max(0.0, dist(gen))));
        };

        size_t row_count = row_count_dist(_gen);
        for (size_t i = 0; i < row_count; ++i) {
            auto ckey = make_random_key();
            is_continuous continuous = is_continuous(_bool_dist(_gen));
            if (_not_dummy_dist(_gen)) {
                deletable_row& row = m.partition().clustered_row(*_schema, ckey, is_dummy::no, continuous);
                set_random_cells(row.cells(), column_kind::regular_column);
                row.marker() = random_row_marker();
            } else {
                m.partition().clustered_row(*_schema, position_in_partition_view::after_key(ckey), is_dummy::yes, continuous);
            }
        }

        size_t range_tombstone_count = row_count_dist(_gen);
        for (size_t i = 0; i < range_tombstone_count; ++i) {
            auto start = make_random_prefix();
            auto end = make_random_prefix();
            clustering_key_prefix::less_compare less(*_schema);
            if (less(end, start)) {
                std::swap(start, end);
            }
            m.partition().apply_row_tombstone(*_schema,
                    range_tombstone(std::move(start), std::move(end), random_tombstone()));
        }

        if (_bool_dist(_gen)) {
            m.partition().ensure_last_dummy(*_schema);
            m.partition().clustered_rows().rbegin()->set_continuous(is_continuous(_bool_dist(_gen)));
        }

        return m;
    }

    std::vector<dht::decorated_key> make_partition_keys(size_t n) {
        auto local_keys = _local_shard_only ? make_local_keys(n, _schema) : make_keys(n, _schema);
        return boost::copy_range<std::vector<dht::decorated_key>>(local_keys | boost::adaptors::transformed([this] (sstring& key) {
            auto pkey = partition_key::from_single_value(*_schema, to_bytes(key));
            return dht::global_partitioner().decorate_key(*_schema, std::move(pkey));
        }));
    }

    std::vector<mutation> operator()(size_t n) {
        auto keys = make_partition_keys(n);
        std::vector<mutation> mutations;
        for (auto&& dkey : keys) {
            auto m = operator()();
            mutations.emplace_back(_schema, std::move(dkey), std::move(m.partition()));
        }
        return mutations;
    }
};

random_mutation_generator::~random_mutation_generator() {}

random_mutation_generator::random_mutation_generator(generate_counters counters, local_shard_only lso)
    : _impl(std::make_unique<random_mutation_generator::impl>(counters, lso))
{ }

mutation random_mutation_generator::operator()() {
    return (*_impl)();
}

std::vector<mutation> random_mutation_generator::operator()(size_t n) {
    return (*_impl)(n);
}

std::vector<dht::decorated_key> random_mutation_generator::make_partition_keys(size_t n) {
    return _impl->make_partition_keys(n);
}

schema_ptr random_mutation_generator::schema() const {
    return _impl->_schema;
}

clustering_key random_mutation_generator::make_random_key() {
    return _impl->make_random_key();
}

std::vector<query::clustering_range> random_mutation_generator::make_random_ranges(unsigned n_ranges) {
    return _impl->make_random_ranges(n_ranges);
}

namespace tests::data_model {

static constexpr const api::timestamp_type previously_removed_column_timestamp = 100;
static constexpr const api::timestamp_type data_timestamp = 200;
static constexpr const api::timestamp_type column_removal_timestamp = 300;

class mutation_description {
public:
    using key = std::vector<bytes>;
    struct collection_element {
        bytes key;
        bytes value;
    };
    using collection = std::vector<collection_element>;
    using atomic_value = bytes;
    using value = std::variant<atomic_value, collection>;
    struct cell {
        sstring column_name;
        value data_value;
    };
    using row = std::vector<cell>;
    struct clustered_row {
        api::timestamp_type marker;
        row cells;
    };
    struct range_tombstone {
        key first;
        key last;
    };

private:
    key _partition_key;
    row _static_row;
    std::map<key, clustered_row> _clustered_rows;
    std::vector<range_tombstone> _range_tombstones;

private:
    static void remove_column(row& r, const sstring& name) {
        auto it = boost::range::find_if(r, [&] (const cell& c) {
            return c.column_name == name;
        });
        if (it != r.end()) {
            r.erase(it);
        }
    }

public:
    explicit mutation_description(key partition_key)
        : _partition_key(std::move(partition_key))
    { }

    void add_static_cell(const sstring& column, value v) {
        _static_row.emplace_back(cell { column, std::move(v) });
    }

    void add_clustered_cell(const key& ck, const sstring& column, value v) {
        _clustered_rows[ck].cells.emplace_back(cell { column, std::move(v) });
    }

    void add_clustered_row_marker(const key& ck) {
        _clustered_rows[ck].marker = data_timestamp;
    }

    void remove_static_column(const sstring& name) {
        remove_column(_static_row, name);
    }

    void remove_regular_column(const sstring& name) {
        for (auto& [ ckey, cr ] : _clustered_rows) {
            (void)ckey;
            remove_column(cr.cells, name);
        }
    }

    void add_range_tombstone(const key& start, const key& end) {
        _range_tombstones.emplace_back(range_tombstone { start, end });
    }

    mutation build(schema_ptr s) const {
        auto m = mutation(s, partition_key::from_exploded(*s, _partition_key));
        for (auto& [ column, value_or_collection ] : _static_row) {
            auto cdef = s->get_column_definition(utf8_type->decompose(column));
            assert(cdef);
            std::visit(make_visitor(
                [&] (const atomic_value& v) {
                    assert(cdef->is_atomic());
                    m.set_static_cell(*cdef, atomic_cell::make_live(*cdef->type, data_timestamp, v));
                },
                [&] (const collection& c) {
                    assert(!cdef->is_atomic());
                    auto ctype = static_pointer_cast<const collection_type_impl>(cdef->type);
                    collection_type_impl::mutation mut;
                    for (auto& [ key, value ] : c) {
                        mut.cells.emplace_back(key, atomic_cell::make_live(*ctype->value_comparator(), data_timestamp,
                                                                           value, atomic_cell::collection_member::yes));
                    }
                    m.set_static_cell(*cdef, ctype->serialize_mutation_form(std::move(mut)));
                }
            ), value_or_collection);
        }
        for (auto& [ ckey, cr ] : _clustered_rows) {
            auto& [ marker, cells ] = cr;
            auto ck = clustering_key::from_exploded(*s, ckey);
            for (auto& [ column, value_or_collection ] : cells) {
                auto cdef = s->get_column_definition(utf8_type->decompose(column));
                assert(cdef);
                std::visit(make_visitor(
                [&] (const atomic_value& v) {
                        assert(cdef->is_atomic());
                        m.set_clustered_cell(ck, *cdef, atomic_cell::make_live(*cdef->type, data_timestamp, v));
                    },
                [&] (const collection& c) {
                        assert(!cdef->is_atomic());
                        auto ctype = static_pointer_cast<const collection_type_impl>(cdef->type);
                        collection_type_impl::mutation mut;
                        for (auto& [ key, value ] : c) {
                            mut.cells.emplace_back(key, atomic_cell::make_live(*ctype->value_comparator(), data_timestamp,
                                                                            value, atomic_cell::collection_member::yes));
                        }
                        m.set_clustered_cell(ck, *cdef, ctype->serialize_mutation_form(std::move(mut)));
                    }
                ), value_or_collection);
            }
            if (marker != api::missing_timestamp) {
                m.partition().clustered_row(*s, ckey).apply(row_marker(marker));
            }
        }
        clustering_key::less_compare cmp(*s);
        for (auto& [ a, b ] : _range_tombstones) {
            auto start = clustering_key::from_exploded(*s, a);
            auto stop = clustering_key::from_exploded(*s, b);
            if (cmp(stop, start)) {
                std::swap(start, stop);
            }
            auto rt = ::range_tombstone(std::move(start), bound_kind::excl_start,
                                        std::move(stop), bound_kind::excl_end,
                                        tombstone(previously_removed_column_timestamp, gc_clock::time_point()));
            m.partition().apply_delete(*s, std::move(rt));
        }
        return m;
    }
};

class table_description {
public:
    using column = std::tuple<sstring, data_type>;
    struct removed_column {
        sstring name;
        data_type type;
        api::timestamp_type removal_timestamp;
    };

private:
    std::vector<column> _partition_key;
    std::vector<column> _clustering_key;
    std::vector<column> _static_columns;
    std::vector<column> _regular_columns;

    std::vector<removed_column> _removed_columns;

    std::vector<mutation_description> _mutations;

    std::vector<sstring> _change_log;

private:
    static std::vector<column>::iterator find_column(std::vector<column>& columns, const sstring& name) {
        return boost::range::find_if(columns, [&] (const column& c) {
            return std::get<sstring>(c) == name;
        });
    }

    static void add_column(std::vector<column>& columns, const sstring& name, data_type type) {
        assert(find_column(columns, name) == columns.end());
        columns.emplace_back(name, type);
    }

    void add_old_column(const sstring& name, data_type type) {
        _removed_columns.emplace_back(removed_column { name, type, previously_removed_column_timestamp });
    }

    void remove_column(std::vector<column>& columns, const sstring& name) {
        auto it = find_column(columns, name);
        assert(it != columns.end());
        _removed_columns.emplace_back(removed_column { name, std::get<data_type>(*it), column_removal_timestamp });
        columns.erase(it);
    }

    static void alter_column_type(std::vector<column>& columns, const sstring& name, data_type new_type) {
        auto it = find_column(columns, name);
        assert(it != columns.end());
        std::get<data_type>(*it) = new_type;
    }

    schema_ptr build_schema() const {
        auto sb = schema_builder("ks", "cf");
        for (auto&& [ name, type ] : _partition_key) {
            sb.with_column(utf8_type->decompose(name), type, column_kind::partition_key);
        }
        for (auto&& [ name, type ] : _clustering_key) {
            sb.with_column(utf8_type->decompose(name), type, column_kind::clustering_key);
        }
        for (auto&& [ name, type ] : _static_columns) {
            sb.with_column(utf8_type->decompose(name), type, column_kind::static_column);
        }
        for (auto&& [ name, type ] : _regular_columns) {
            sb.with_column(utf8_type->decompose(name), type);
        }

        for (auto&& [ name, type, timestamp ] : _removed_columns) {
            sb.without_column(name, type, timestamp);
        }

        return sb.build();
    }

    std::vector<mutation> build_mutations(schema_ptr s) const {
        auto ms = boost::copy_range<std::vector<mutation>>(
            _mutations | boost::adaptors::transformed([&] (const mutation_description& md) {
                return md.build(s);
            })
        );
        boost::sort(ms, mutation_decorated_key_less_comparator());
        return ms;
    }

public:
    explicit table_description(std::vector<column> partition_key, std::vector<column> clustering_key)
        : _partition_key(std::move(partition_key))
        , _clustering_key(std::move(clustering_key))
    { }

    void add_static_column(const sstring& name, data_type type) {
        _change_log.emplace_back(format("added static column \'{}\' of type \'{}\'", name, type->as_cql3_type()->to_string()));
        add_column(_static_columns, name, type);
    }

    void add_regular_column(const sstring& name, data_type type) {
        _change_log.emplace_back(format("added regular column \'{}\' of type \'{}\'", name, type->as_cql3_type()->to_string()));
        add_column(_regular_columns, name, type);
    }

    void add_old_static_column(const sstring& name, data_type type) {
        add_old_column(name, type);
    }

    void add_old_regular_column(const sstring& name, data_type type) {
        add_old_column(name, type);
    }

    void remove_static_column(const sstring& name) {
        _change_log.emplace_back(format("removed static column \'{}\'", name));
        remove_column(_static_columns, name);
        for (auto& m : _mutations) {
            m.remove_static_column(name);
        }
    }

    void remove_regular_column(const sstring& name) {
        _change_log.emplace_back(format("removed regular column \'{}\'", name));
        remove_column(_regular_columns, name);
        for (auto& m : _mutations) {
            m.remove_regular_column(name);
        }
    }

    void alter_partition_column_type(const sstring& name, data_type new_type) {
        _change_log.emplace_back(format("altered partition column \'{}\' type to \'{}\'", name, new_type->as_cql3_type()->to_string()));
        alter_column_type(_partition_key, name, new_type);
    }

    void alter_clustering_column_type(const sstring& name, data_type new_type) {
        _change_log.emplace_back(format("altered clustering column \'{}\' type to \'{}\'", name, new_type->as_cql3_type()->to_string()));
        alter_column_type(_clustering_key, name, new_type);
    }

    void alter_static_column_type(const sstring& name, data_type new_type) {
        _change_log.emplace_back(format("altered static column \'{}\' type to \'{}\'", name, new_type->as_cql3_type()->to_string()));
        alter_column_type(_static_columns, name, new_type);
    }

    void alter_regular_column_type(const sstring& name, data_type new_type) {
        _change_log.emplace_back(format("altered regular column \'{}\' type to \'{}\'", name, new_type->as_cql3_type()->to_string()));
        alter_column_type(_regular_columns, name, new_type);
    }

    void rename_partition_column(const sstring& from, const sstring& to) {
        _change_log.emplace_back(format("renamed partition column \'{}\' to \'{}\'", from, to));
        auto it = find_column(_partition_key, from);
        assert(it != _partition_key.end());
        std::get<sstring>(*it) = to;
    }
    void rename_clustering_column(const sstring& from, const sstring& to) {
        _change_log.emplace_back(format("renamed clustering column \'{}\' to \'{}\'", from, to));
        auto it = find_column(_clustering_key, from);
        assert(it != _clustering_key.end());
        std::get<sstring>(*it) = to;
    }

    std::vector<mutation_description>& unordered_mutations() { return _mutations; }
    const std::vector<mutation_description>& unordered_mutations() const { return _mutations; }

    struct table {
        sstring schema_changes_log;
        schema_ptr schema;
        std::vector<mutation> mutations;
    };
    table build() const {
        auto s = build_schema();
        return { boost::algorithm::join(_change_log, "\n"), s, build_mutations(s) };
    }
};

}

void for_each_schema_change(std::function<void(schema_ptr, const std::vector<mutation>&,
                                               schema_ptr, const std::vector<mutation>&)> fn) {
    auto map_of_int_to_int = map_type_impl::get_instance(int32_type, int32_type, true);
    auto map_of_int_to_bytes = map_type_impl::get_instance(int32_type, bytes_type, true);
    auto frozen_map_of_int_to_int = map_type_impl::get_instance(int32_type, int32_type, false);
    auto frozen_map_of_int_to_bytes = map_type_impl::get_instance(int32_type, bytes_type, false);
    auto tuple_of_int_long = tuple_type_impl::get_instance({ int32_type, long_type });
    auto tuple_of_bytes_long = tuple_type_impl::get_instance( { bytes_type, long_type });
    auto tuple_of_bytes_bytes = tuple_type_impl::get_instance( { bytes_type, bytes_type });
    auto set_of_text = set_type_impl::get_instance(utf8_type, true);
    auto set_of_bytes = set_type_impl::get_instance(bytes_type, true);
    auto udt_int_text = user_type_impl::get_instance("ks", "udt",
        { utf8_type->decompose("f1"), utf8_type->decompose("f2"), },
        { int32_type, utf8_type });
    auto udt_int_blob_long = user_type_impl::get_instance("ks", "udt",
        { utf8_type->decompose("v1"), utf8_type->decompose("v2"), utf8_type->decompose("v3"), },
        { int32_type, bytes_type, long_type });

    auto random_int32_value = [] {
        return int32_type->decompose(tests::random::get_int<int32_t>());
    };
    int32_t key_id = 0;
    auto random_partition_key = [&] () -> tests::data_model::mutation_description::key {
        return { random_int32_value(), random_int32_value(), int32_type->decompose(key_id++), };
    };
    auto random_clustering_key = [&] () -> tests::data_model::mutation_description::key {
        return {
            utf8_type->decompose(tests::random::get_sstring()),
            utf8_type->decompose(tests::random::get_sstring()),
            utf8_type->decompose(format("{}", key_id++)),
        };
    };
    auto random_map = [&] () -> tests::data_model::mutation_description::collection {
        return {
            { int32_type->decompose(1), random_int32_value() },
            { int32_type->decompose(2), random_int32_value() },
            { int32_type->decompose(3), random_int32_value() },
        };
    };
    auto random_frozen_map = [&] {
        return map_of_int_to_int->decompose(make_map_value(map_of_int_to_int, map_type_impl::native_type({
            { 1, tests::random::get_int<int32_t>() },
            { 2, tests::random::get_int<int32_t>() },
            { 3, tests::random::get_int<int32_t>() },
        })));
    };
    auto random_tuple = [&] {
        return tuple_of_int_long->decompose(make_tuple_value(tuple_of_int_long, tuple_type_impl::native_type{
            tests::random::get_int<int32_t>(), tests::random::get_int<int64_t>(),
        }));
    };
    auto random_set = [&] () -> tests::data_model::mutation_description::collection {
        return {
            { utf8_type->decompose("a"), bytes() },
            { utf8_type->decompose("b"), bytes() },
            { utf8_type->decompose("c"), bytes() },
        };
    };
    auto random_udt = [&] {
        return udt_int_text->decompose(make_user_value(udt_int_text, user_type_impl::native_type{
            tests::random::get_int<int32_t>(),
            tests::random::get_sstring(),
        }));
    };

    struct column_description {
        int id;
        data_type type;
        std::vector<data_type> alter_to;
        std::vector<std::function<tests::data_model::mutation_description::value()>> data_generators;
        data_type old_type;
    };

    auto columns = std::vector<column_description> {
        { 100, int32_type, { varint_type, bytes_type }, { [&] { return random_int32_value(); }, [&] { return bytes(); } }, uuid_type },
        { 200, map_of_int_to_int, { map_of_int_to_bytes }, { [&] { return random_map(); } }, empty_type },
        { 300, int32_type, { varint_type, bytes_type }, { [&] { return random_int32_value(); }, [&] { return bytes(); } }, empty_type },
        { 400, frozen_map_of_int_to_int, { frozen_map_of_int_to_bytes }, { [&] { return random_frozen_map(); } }, empty_type },
        { 500, tuple_of_int_long, { tuple_of_bytes_long, tuple_of_bytes_bytes }, { [&] { return random_tuple(); } }, empty_type },
        { 600, set_of_text, { set_of_bytes }, { [&] { return random_set(); } }, empty_type },
        { 700, udt_int_text, { udt_int_blob_long }, { [&] { return random_udt(); } }, empty_type },
    };
    auto static_columns = columns;
    auto regular_columns = columns;

    // Base schema
    auto s = tests::data_model::table_description({ { "pk1", int32_type }, { "pk2", int32_type }, { "pk3", int32_type }, },
                                                  { { "ck1", utf8_type }, { "ck2", utf8_type }, { "ck3", utf8_type }, });
    for (auto& sc : static_columns) {
        auto name = format("s{}", sc.id);
        s.add_static_column(name, sc.type);
        if (sc.old_type != empty_type) {
            s.add_old_static_column(name, sc.old_type);
        }
    }
    for (auto& rc : regular_columns) {
        auto name = format("r{}", rc.id);
        s.add_regular_column(name, rc.type);
        if (rc.old_type != empty_type) {
            s.add_old_regular_column(name, rc.old_type);
        }
    }

    auto max_generator_count = std::max(
        // boost::max_elements wants the iterators to be copy-assignable. The ones we get
        // from boost::adaptors::transformed aren't.
        boost::accumulate(static_columns | boost::adaptors::transformed([] (const column_description& c) {
            return c.data_generators.size();
        }), 0u, [] (size_t a, size_t b) { return std::max(a, b); }),
        boost::accumulate(regular_columns | boost::adaptors::transformed([] (const column_description& c) {
            return c.data_generators.size();
        }), 0u, [] (size_t a, size_t b) { return std::max(a, b); })
    );

    // Base data

    // Single column in a static row, nothing else
    for (auto& [id, type, alter_to, data_generators, old_type] : static_columns) {
        auto name = format("s{}", id);
        for (auto& dg : data_generators) {
            auto m = tests::data_model::mutation_description(random_partition_key());
            m.add_static_cell(name, dg());
            s.unordered_mutations().emplace_back(std::move(m));
        }
    }

    // Partition with rows each having a single column
    auto m = tests::data_model::mutation_description(random_partition_key());
    for (auto& [id, type, alter_to, data_generators, old_type] : regular_columns) {
        auto name = format("r{}", id);
        for (auto& dg : data_generators) {
            m.add_clustered_cell(random_clustering_key(), name, dg());
        }
    }
    s.unordered_mutations().emplace_back(std::move(m));

    // Absolutely everything
    for (auto i = 0u; i < max_generator_count; i++) {
        auto m = tests::data_model::mutation_description(random_partition_key());
        for (auto& [id, type, alter_to, data_generators, old_type] : static_columns) {
            auto name = format("s{}", id);
            m.add_static_cell(name, data_generators[std::min<size_t>(i, data_generators.size() - 1)]());
        }
        for (auto& [id, type, alter_to, data_generators, old_type] : regular_columns) {
            auto name = format("r{}", id);
            m.add_clustered_cell(random_clustering_key(), name, data_generators[std::min<size_t>(i, data_generators.size() - 1)]());
        }

        m.add_range_tombstone(random_clustering_key(), random_clustering_key());
        m.add_range_tombstone(random_clustering_key(), random_clustering_key());
        m.add_range_tombstone(random_clustering_key(), random_clustering_key());

        s.unordered_mutations().emplace_back(std::move(m));
    }

    // Transformations
    auto base = s.build();

    std::vector<tests::data_model::table_description::table> schemas;
    schemas.emplace_back(base);

    auto test_mutated_schemas = [&] {
        auto& [ base_change_log, base_schema, base_mutations ] = base;
        for (auto&& [ mutated_change_log, mutated_schema, mutated_mutations ] : schemas) {
            BOOST_TEST_MESSAGE(format("\nSchema change from:\n\n{}\n\nto:\n\n{}\n", base_change_log, mutated_change_log));
            fn(base_schema, base_mutations, mutated_schema, mutated_mutations);
        }
        for (auto i = 2u; i < schemas.size(); i++) {
            auto& [ base_change_log, base_schema, base_mutations ] = schemas[i - 1];
            auto& [ mutated_change_log, mutated_schema, mutated_mutations ] = schemas[i];
            BOOST_TEST_MESSAGE(format("\nSchema change from:\n\n{}\n\nto:\n\n{}\n", base_change_log, mutated_change_log));
            fn(base_schema, base_mutations, mutated_schema, mutated_mutations);
        }
        schemas.clear();
        schemas.emplace_back(base);
    };

    auto original_s = s;
     // Remove and add back all static columns
    for (auto& sc : static_columns) {
        s.remove_static_column(format("s{}", sc.id));
        schemas.emplace_back(s.build());
    }
    for (auto& sc : static_columns) {
        s.add_static_column(format("s{}", sc.id), uuid_type);
        auto mutated = s.build();
        schemas.emplace_back(s.build());
    }
    test_mutated_schemas();

    s = original_s;
    // Remove and add back all regular columns
    for (auto& rc : regular_columns) {
        s.remove_regular_column(format("r{}", rc.id));
        schemas.emplace_back(s.build());
    }
    auto temp_s = s;
    auto temp_schemas = schemas;
    for (auto& rc : regular_columns) {
        s.add_regular_column(format("r{}", rc.id), uuid_type);
        schemas.emplace_back(s.build());
    }
    test_mutated_schemas();

    s = temp_s;
    schemas = temp_schemas;
    // Add back all regular columns as collections
    for (auto& rc : regular_columns) {
        s.add_regular_column(format("r{}", rc.id), map_of_int_to_bytes);
        schemas.emplace_back(s.build());
    }
    test_mutated_schemas();

    s = temp_s;
    schemas = temp_schemas;
    // Add back all regular columns as frozen collections
    for (auto& rc : regular_columns) {
        s.add_regular_column(format("r{}", rc.id), frozen_map_of_int_to_int);
        schemas.emplace_back(s.build());
    }
    test_mutated_schemas();

    s = original_s;
    // Add more static columns
    for (auto& sc : static_columns) {
        s.add_static_column(format("s{}", sc.id + 1), uuid_type);
        schemas.emplace_back(s.build());
    }
    test_mutated_schemas();

    s = original_s;
    // Add more regular columns
    for (auto& rc : regular_columns) {
        s.add_regular_column(format("r{}", rc.id + 1), uuid_type);
        schemas.emplace_back(s.build());
    }
    test_mutated_schemas();

    s = original_s;
    // Alter column types
    for (auto& sc : static_columns) {
        for (auto& target : sc.alter_to) {
            s.alter_static_column_type(format("s{}", sc.id), target);
            schemas.emplace_back(s.build());
        }
    }
    for (auto& rc : regular_columns) {
        for (auto& target : rc.alter_to) {
            s.alter_regular_column_type(format("r{}", rc.id), target);
            schemas.emplace_back(s.build());
        }
    }
    for (auto i = 1; i <= 3; i++) {
        s.alter_clustering_column_type(format("ck{}", i), bytes_type);
        schemas.emplace_back(s.build());
    }
    for (auto i = 1; i <= 3; i++) {
        s.alter_partition_column_type(format("pk{}", i), bytes_type);
        schemas.emplace_back(s.build());
    }
    test_mutated_schemas();

    s = original_s;
    // Rename clustering key
    for (auto i = 1; i <= 3; i++) {
        s.rename_clustering_column(format("ck{}", i), format("ck{}", 100 - i));
        schemas.emplace_back(s.build());
    }
    test_mutated_schemas();

    s = original_s;
    // Rename partition key
    for (auto i = 1; i <= 3; i++) {
        s.rename_partition_column(format("pk{}", i), format("pk{}", 100 - i));
        schemas.emplace_back(s.build());
    }
    test_mutated_schemas();
}
