/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */


#include <list>
#include <random>
#include <source_location>
#include <algorithm>

#include <fmt/ranges.h>
#include <fmt/std.h>

#include <seastar/core/sleep.hh>
#include <seastar/core/do_with.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/util/closeable.hh>

#include "sstables/generation_type.hh"
#undef SEASTAR_TESTING_MAIN
#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>
#include "test/lib/eventually.hh"
#include "test/lib/mutation_assertions.hh"
#include "test/lib/mutation_reader_assertions.hh"
#include "test/lib/sstable_utils.hh"
#include "test/lib/simple_schema.hh"
#include "test/lib/mutation_source_test.hh"
#include "test/lib/cql_test_env.hh"
#include "test/lib/make_random_string.hh"
#include "test/lib/dummy_sharder.hh"
#include "test/lib/reader_lifecycle_policy.hh"
#include "test/lib/reader_concurrency_semaphore.hh"
#include "test/lib/random_utils.hh"
#include "test/lib/simple_position_reader_queue.hh"
#include "test/lib/fragment_scatterer.hh"
#include "test/lib/key_utils.hh"
#include "test/lib/test_utils.hh"

#include "dht/sharder.hh"
#include "schema/schema_builder.hh"
#include "cell_locking.hh"
#include "sstables/sstables.hh"
#include "sstables/sstable_set_impl.hh"
#include "replica/database.hh"
#include "partition_slice_builder.hh"
#include "schema/schema_registry.hh"
#include "utils/assert.hh"
#include "mutation/mutation_rebuilder.hh"

#include "readers/from_mutations_v2.hh"
#include "readers/forwardable_v2.hh"
#include "readers/from_fragments_v2.hh"
#include "readers/mutation_fragment_v1_stream.hh"
#include "readers/generating_v2.hh"
#include "readers/empty_v2.hh"
#include "readers/next_partition_adaptor.hh"
#include "readers/combined.hh"
#include "readers/compacting.hh"
#include "readers/foreign.hh"
#include "readers/filtering.hh"
#include "readers/evictable.hh"
#include "readers/queue.hh"

BOOST_AUTO_TEST_SUITE(mutation_reader_test)

static schema_ptr make_schema() {
    return schema_builder("ks", "cf")
        .with_column("pk", bytes_type, column_kind::partition_key)
        .with_column("v", bytes_type, column_kind::regular_column)
        .build();
}

SEASTAR_TEST_CASE(test_combining_two_readers_with_the_same_row) {
    return seastar::async([] {
        auto s = make_schema();
        tests::reader_concurrency_semaphore_wrapper semaphore;
        auto permit = semaphore.make_permit();

        mutation m1(s, partition_key::from_single_value(*s, "key1"));
        m1.set_clustered_cell(clustering_key::make_empty(), "v", data_value(bytes("v1")), 1);

        mutation m2(s, partition_key::from_single_value(*s, "key1"));
        m2.set_clustered_cell(clustering_key::make_empty(), "v", data_value(bytes("v2")), 2);

        assert_that(make_combined_reader(s, permit, make_mutation_reader_from_mutations_v2(s, permit, m1), make_mutation_reader_from_mutations_v2(s, permit, m2)))
            .produces(m2)
            .produces_end_of_stream();
    });
}

SEASTAR_TEST_CASE(test_combining_two_non_overlapping_readers) {
    return seastar::async([] {
        auto s = make_schema();
        tests::reader_concurrency_semaphore_wrapper semaphore;
        auto permit = semaphore.make_permit();

        mutation m1(s, partition_key::from_single_value(*s, "keyB"));
        m1.set_clustered_cell(clustering_key::make_empty(), "v", data_value(bytes("v1")), 1);

        mutation m2(s, partition_key::from_single_value(*s, "keyA"));
        m2.set_clustered_cell(clustering_key::make_empty(), "v", data_value(bytes("v2")), 2);

        auto cr = make_combined_reader(s, permit, make_mutation_reader_from_mutations_v2(s, permit, m1), make_mutation_reader_from_mutations_v2(s, permit, m2));
        assert_that(std::move(cr))
            .produces(m2)
            .produces(m1)
            .produces_end_of_stream();
    });
}

SEASTAR_TEST_CASE(test_combining_two_partially_overlapping_readers) {
    return seastar::async([] {
        auto s = make_schema();
        tests::reader_concurrency_semaphore_wrapper semaphore;
        auto permit = semaphore.make_permit();

        mutation m1(s, partition_key::from_single_value(*s, "keyA"));
        m1.set_clustered_cell(clustering_key::make_empty(), "v", data_value(bytes("v1")), 1);

        mutation m2(s, partition_key::from_single_value(*s, "keyB"));
        m2.set_clustered_cell(clustering_key::make_empty(), "v", data_value(bytes("v2")), 1);

        mutation m3(s, partition_key::from_single_value(*s, "keyC"));
        m3.set_clustered_cell(clustering_key::make_empty(), "v", data_value(bytes("v3")), 1);

        assert_that(make_combined_reader(s, permit, make_mutation_reader_from_mutations_v2(s, permit, {m1, m2}), make_mutation_reader_from_mutations_v2(s, permit, {m2, m3})))
            .produces(m1)
            .produces(m2)
            .produces(m3)
            .produces_end_of_stream();
    });
}

SEASTAR_TEST_CASE(test_combining_one_reader_with_many_partitions) {
    return seastar::async([] {
        auto s = make_schema();
        tests::reader_concurrency_semaphore_wrapper semaphore;
        auto permit = semaphore.make_permit();

        mutation m1(s, partition_key::from_single_value(*s, "keyA"));
        m1.set_clustered_cell(clustering_key::make_empty(), "v", data_value(bytes("v1")), 1);

        mutation m2(s, partition_key::from_single_value(*s, "keyB"));
        m2.set_clustered_cell(clustering_key::make_empty(), "v", data_value(bytes("v2")), 1);

        mutation m3(s, partition_key::from_single_value(*s, "keyC"));
        m3.set_clustered_cell(clustering_key::make_empty(), "v", data_value(bytes("v3")), 1);

        std::vector<mutation_reader> v;
        v.push_back(make_mutation_reader_from_mutations_v2(s, permit, {m1, m2, m3}));
        assert_that(make_combined_reader(s, permit, std::move(v), streamed_mutation::forwarding::no, mutation_reader::forwarding::no))
            .produces(m1)
            .produces(m2)
            .produces(m3)
            .produces_end_of_stream();
    });
}

SEASTAR_THREAD_TEST_CASE(combined_reader_galloping_within_partition_test) {
    simple_schema s;
    tests::reader_concurrency_semaphore_wrapper semaphore;
    auto permit = semaphore.make_permit();

    const auto pk = s.make_pkey();
    const auto ckeys = s.make_ckeys(10);

    auto make_partition = [&] (auto&& ckey_indexes) -> mutation {
        mutation mut(s.schema(), pk);
        for (auto ckey_index : ckey_indexes) {
            s.add_row(mut, ckeys[ckey_index], format("val_{:02d}", ckey_index), 1);
        }
        return mut;
    };

    std::vector<mutation_reader> v;
    v.push_back(make_mutation_reader_from_mutations_v2(s.schema(), permit, make_partition(std::views::iota(0, 5))));
    v.push_back(make_mutation_reader_from_mutations_v2(s.schema(), permit, make_partition(std::views::iota(5, 10))));
    assert_that(make_combined_reader(s.schema(), permit, std::move(v), streamed_mutation::forwarding::no, mutation_reader::forwarding::no))
        .produces(make_partition(std::views::iota(0, 10)))
        .produces_end_of_stream();
}

template<typename Collection>
mutation make_partition_with_clustering_rows(simple_schema& s, const dht::decorated_key& pkey, Collection&& ckey_nums) {
    mutation mut(s.schema(), pkey);
    for (auto i : ckey_nums) {
        s.add_row(mut, s.make_ckey(i), format("val_{:02d}", i), 1);
    }
    return mut;
}

SEASTAR_THREAD_TEST_CASE(combined_mutation_reader_galloping_over_multiple_partitions_test) {
    simple_schema s;
    tests::reader_concurrency_semaphore_wrapper semaphore;
    auto permit = semaphore.make_permit();

    const auto k = s.make_pkeys(2);

    std::vector<mutation_reader> v;
    v.push_back(make_mutation_reader_from_mutations_v2(s.schema(), permit, {
        make_partition_with_clustering_rows(s, k[0], std::views::iota(5, 10)),
        make_partition_with_clustering_rows(s, k[1], std::views::iota(0, 5))
    }));
    v.push_back(make_mutation_reader_from_mutations_v2(s.schema(), permit, {
        make_partition_with_clustering_rows(s, k[0], std::views::iota(0, 5)),
        make_partition_with_clustering_rows(s, k[1], std::views::iota(5, 10))
    }));
    assert_that(make_combined_reader(s.schema(), permit, std::move(v), streamed_mutation::forwarding::no, mutation_reader::forwarding::no))
        .produces(make_partition_with_clustering_rows(s, k[0], std::views::iota(0, 10)))
        .produces(make_partition_with_clustering_rows(s, k[1], std::views::iota(0, 10)))
        .produces_end_of_stream();
}

SEASTAR_THREAD_TEST_CASE(combined_reader_galloping_changing_multiple_partitions_test) {
    simple_schema s;
    tests::reader_concurrency_semaphore_wrapper semaphore;
    auto permit = semaphore.make_permit();

    const auto k = s.make_pkeys(2);

    std::vector<mutation_reader> v;
    v.push_back(make_mutation_reader_from_mutations_v2(s.schema(), permit, {
        make_partition_with_clustering_rows(s, k[0], std::views::iota(0, 5)),
        make_partition_with_clustering_rows(s, k[1], std::views::iota(0, 5))
    }));
    v.push_back(make_mutation_reader_from_mutations_v2(s.schema(), permit, {
        make_partition_with_clustering_rows(s, k[0], std::views::iota(5, 10)),
        make_partition_with_clustering_rows(s, k[1], std::views::iota(5, 10)),
    }));
    assert_that(make_combined_reader(s.schema(), permit, std::move(v), streamed_mutation::forwarding::no, mutation_reader::forwarding::no))
        .produces(make_partition_with_clustering_rows(s, k[0], std::views::iota(0, 10)))
        .produces(make_partition_with_clustering_rows(s, k[1], std::views::iota(0, 10)))
        .produces_end_of_stream();
}

SEASTAR_THREAD_TEST_CASE(test_combined_reader_range_tombstone_change_merging) {
    simple_schema s;
    const auto schema = s.schema();
    tests::reader_concurrency_semaphore_wrapper semaphore;
    auto permit = semaphore.make_permit();

    auto rtc = [&] (uint32_t ckey, std::optional<api::timestamp_type> ts) {
        return range_tombstone_change(
                position_in_partition::before_key(s.make_ckey(ckey)),
                ts ? tombstone(*ts, {}) : tombstone());
    };
    struct input {
        std::vector<range_tombstone_change> rtcs;
    };
    struct output {
        std::vector<range_tombstone_change> rtcs;
    };
    auto check = [&] (const char* desc, std::vector<input> in, output out, seastar::compat::source_location sl = seastar::compat::source_location::current()) {
        testlog.info("check() {} @ {}:{}", desc, sl.file_name(), sl.line());
        std::vector<mutation_reader> readers;
        for (auto& i : in) {
            std::deque<mutation_fragment_v2> fragments;
            fragments.emplace_back(*schema, permit, partition_start(s.make_pkey(0), {}));
            for (auto& rtc : i.rtcs) {
                fragments.emplace_back(*schema, permit, std::move(rtc));
            }
            fragments.emplace_back(*schema, permit, partition_end{});
            readers.emplace_back(make_mutation_reader_from_fragments(schema, permit, std::move(fragments)));
        }
        auto combined_reader = assert_that(make_combined_reader(schema, permit, std::move(readers)));
        combined_reader.produces_partition_start(s.make_pkey(0));
        for (const auto& rtc : out.rtcs) {
            combined_reader.produces_range_tombstone_change(rtc);
        }
        combined_reader.produces_partition_end();
        combined_reader.produces_end_of_stream();
    };

    check("single stream",
        {
            input{{rtc(1, 100), rtc(2, {})}}
        },
        output{{rtc(1, 100), rtc(2, {})}});

    check("two streams, two rtc with same pos",
        {
            input{{rtc(1, 100), rtc(2, {})}},
            input{{rtc(1, 200), rtc(2, {})}}
        },
        output{{rtc(1, 200), rtc(2, {})}});

    check("two streams, alternating updates to both streams",
        {
            input{{rtc(1, 100), rtc(3, 200), rtc(4, {})}},
            input{{rtc(1, 200), rtc(2, 100), rtc(4, {})}}
        },
        output{{rtc(1, 200), rtc(2, 100), rtc(3, 200), rtc(4, {})}});

    check("two streams, active tombstone terminated, fallback to overshadowed tombstone",
        {
            input{{rtc(1, 100), rtc(3, 200), rtc(4, {})}},
            input{{rtc(1, 200), rtc(2, {})}}
        },
        output{{rtc(1, 200), rtc(2, 100), rtc(3, 200), rtc(4, {})}});

    check("three streams, active tombstone terminated, fallback to overshadowed tombstones",
        {
            input{{rtc(1, 100), rtc(4, 200), rtc(5, {})}},
            input{{rtc(1, 200), rtc(3, {}), rtc(4, {})}},
            input{{rtc(1, 300), rtc(2, {})}}
        },
        output{{rtc(1, 300), rtc(2, 200), rtc(3, 100), rtc(4, 200), rtc(5, {})}});

    check("three streams, tombstones not representing timestamp upgrades are omitted",
        {
            input{{rtc(1, 100), rtc(6, {})}},
            input{{rtc(2, 100), rtc(5, {})}},
            input{{rtc(3, 100), rtc(4, {})}}
        },
        output{{rtc(1, 100), rtc(6, {})}});

    check("two streams, new tombstone has smaller timestamp",
        {
            input{{rtc(1, 200), rtc(3, {})}},
            input{{rtc(2, 100), rtc(4, {})}},
        },
        output{{rtc(1, 200), rtc(3, 100), rtc(4, {})}});
}

static mutation make_mutation_with_key(schema_ptr s, dht::decorated_key dk) {
    mutation m(s, std::move(dk));
    m.set_clustered_cell(clustering_key::make_empty(), "v", data_value(bytes("v1")), 1);
    return m;
}

static mutation make_mutation_with_key(schema_ptr s, const char* key) {
    return make_mutation_with_key(s, dht::decorate_key(*s, partition_key::from_single_value(*s, bytes(key))));
}

SEASTAR_TEST_CASE(test_filtering) {
    return seastar::async([] {
        auto s = make_schema();
        tests::reader_concurrency_semaphore_wrapper semaphore;

        auto m1 = make_mutation_with_key(s, "key1");
        auto m2 = make_mutation_with_key(s, "key2");
        auto m3 = make_mutation_with_key(s, "key3");
        auto m4 = make_mutation_with_key(s, "key4");

        // All pass
        assert_that(make_filtering_reader(make_mutation_reader_from_mutations_v2(s, semaphore.make_permit(), {m1, m2, m3, m4}),
                 [] (const dht::decorated_key& dk) { return true; }))
            .produces(m1)
            .produces(m2)
            .produces(m3)
            .produces(m4)
            .produces_end_of_stream();

        // None pass
        assert_that(make_filtering_reader(make_mutation_reader_from_mutations_v2(s, semaphore.make_permit(), {m1, m2, m3, m4}),
                 [] (const dht::decorated_key& dk) { return false; }))
            .produces_end_of_stream();

        // Trim front
        assert_that(make_filtering_reader(make_mutation_reader_from_mutations_v2(s, semaphore.make_permit(), {m1, m2, m3, m4}),
                [&] (const dht::decorated_key& dk) { return !dk.key().equal(*s, m1.key()); }))
            .produces(m2)
            .produces(m3)
            .produces(m4)
            .produces_end_of_stream();

        assert_that(make_filtering_reader(make_mutation_reader_from_mutations_v2(s, semaphore.make_permit(), {m1, m2, m3, m4}),
            [&] (const dht::decorated_key& dk) { return !dk.key().equal(*s, m1.key()) && !dk.key().equal(*s, m2.key()); }))
            .produces(m3)
            .produces(m4)
            .produces_end_of_stream();

        // Trim back
        assert_that(make_filtering_reader(make_mutation_reader_from_mutations_v2(s, semaphore.make_permit(), {m1, m2, m3, m4}),
                 [&] (const dht::decorated_key& dk) { return !dk.key().equal(*s, m4.key()); }))
            .produces(m1)
            .produces(m2)
            .produces(m3)
            .produces_end_of_stream();

        assert_that(make_filtering_reader(make_mutation_reader_from_mutations_v2(s, semaphore.make_permit(), {m1, m2, m3, m4}),
                 [&] (const dht::decorated_key& dk) { return !dk.key().equal(*s, m4.key()) && !dk.key().equal(*s, m3.key()); }))
            .produces(m1)
            .produces(m2)
            .produces_end_of_stream();

        // Trim middle
        assert_that(make_filtering_reader(make_mutation_reader_from_mutations_v2(s, semaphore.make_permit(), {m1, m2, m3, m4}),
                 [&] (const dht::decorated_key& dk) { return !dk.key().equal(*s, m3.key()); }))
            .produces(m1)
            .produces(m2)
            .produces(m4)
            .produces_end_of_stream();

        assert_that(make_filtering_reader(make_mutation_reader_from_mutations_v2(s, semaphore.make_permit(), {m1, m2, m3, m4}),
                 [&] (const dht::decorated_key& dk) { return !dk.key().equal(*s, m2.key()) && !dk.key().equal(*s, m3.key()); }))
            .produces(m1)
            .produces(m4)
            .produces_end_of_stream();
    });
}

SEASTAR_TEST_CASE(test_combining_two_readers_with_one_reader_empty) {
    return seastar::async([] {
        auto s = make_schema();
        tests::reader_concurrency_semaphore_wrapper semaphore;
        auto permit = semaphore.make_permit();
        mutation m1(s, partition_key::from_single_value(*s, "key1"));
        m1.set_clustered_cell(clustering_key::make_empty(), "v", data_value(bytes("v1")), 1);

        assert_that(make_combined_reader(s, permit, make_mutation_reader_from_mutations_v2(s, permit, m1), make_empty_flat_reader_v2(s, permit)))
            .produces(m1)
            .produces_end_of_stream();
    });
}

SEASTAR_TEST_CASE(test_combining_two_empty_readers) {
    return seastar::async([] {
        auto s = make_schema();
        tests::reader_concurrency_semaphore_wrapper semaphore;
        auto permit = semaphore.make_permit();
        assert_that(make_combined_reader(s, permit, make_empty_flat_reader_v2(s, permit), make_empty_flat_reader_v2(s, permit)))
            .produces_end_of_stream();
    });
}

SEASTAR_TEST_CASE(test_combining_one_empty_reader) {
    return seastar::async([] {
        std::vector<mutation_reader> v;
        tests::reader_concurrency_semaphore_wrapper semaphore;
        auto permit = semaphore.make_permit();
        auto s = make_schema();
        v.push_back(make_empty_flat_reader_v2(s, permit));
        assert_that(make_combined_reader(s, permit, std::move(v), streamed_mutation::forwarding::no, mutation_reader::forwarding::no))
            .produces_end_of_stream();
    });
}

std::vector<dht::decorated_key> generate_keys(schema_ptr s, int count) {
    auto keys =
        std::views::iota(0, count) | std::views::transform([s] (int key) {
            auto pk = partition_key::from_single_value(*s, int32_type->decompose(data_value(key)));
            return dht::decorate_key(*s, std::move(pk));
        }) | std::ranges::to<std::vector<dht::decorated_key>>();
    std::ranges::sort(keys, dht::decorated_key::less_comparator(s));
    return keys;
}

std::vector<dht::ring_position> to_ring_positions(const std::vector<dht::decorated_key>& keys) {
    return keys | std::views::transform([] (const dht::decorated_key& key) {
        return dht::ring_position(key);
    }) | std::ranges::to<std::vector>();
}

SEASTAR_TEST_CASE(test_fast_forwarding_combining_reader) {
    return seastar::async([] {
        auto s = make_schema();
        tests::reader_concurrency_semaphore_wrapper semaphore;

        auto keys = generate_keys(s, 7);
        auto ring = to_ring_positions(keys);

        std::vector<std::vector<mutation>> mutations {
            {
                make_mutation_with_key(s, keys[0]),
                make_mutation_with_key(s, keys[1]),
                make_mutation_with_key(s, keys[2]),
            },
            {
                make_mutation_with_key(s, keys[2]),
                make_mutation_with_key(s, keys[3]),
                make_mutation_with_key(s, keys[4]),
            },
            {
                make_mutation_with_key(s, keys[1]),
                make_mutation_with_key(s, keys[3]),
                make_mutation_with_key(s, keys[5]),
            },
            {
                make_mutation_with_key(s, keys[0]),
                make_mutation_with_key(s, keys[5]),
                make_mutation_with_key(s, keys[6]),
            },
        };

        auto make_reader = [&] (reader_permit permit, const dht::partition_range& pr) {
            return make_combined_reader(s, permit, mutations | std::views::transform([&pr, s, permit] (auto& ms) {
                return make_mutation_reader_from_mutations_v2(s, permit, {ms}, pr);
            }) | std::ranges::to<std::vector>());
        };

        auto pr = dht::partition_range::make_open_ended_both_sides();
        assert_that(make_reader(semaphore.make_permit(), pr))
            .produces(keys[0])
            .produces(keys[1])
            .produces(keys[2])
            .produces(keys[3])
            .produces(keys[4])
            .produces(keys[5])
            .produces(keys[6])
            .produces_end_of_stream();

        pr = dht::partition_range::make(ring[0], ring[0]);
            assert_that(make_reader(semaphore.make_permit(), pr))
                    .produces(keys[0])
                    .produces_end_of_stream()
                    .fast_forward_to(dht::partition_range::make(ring[1], ring[1]))
                    .produces(keys[1])
                    .produces_end_of_stream()
                    .fast_forward_to(dht::partition_range::make(ring[3], ring[4]))
                    .produces(keys[3])
            .fast_forward_to(dht::partition_range::make({ ring[4], false }, ring[5]))
                    .produces(keys[5])
                    .produces_end_of_stream()
            .fast_forward_to(dht::partition_range::make_starting_with(ring[6]))
                    .produces(keys[6])
                    .produces_end_of_stream();
    });
}

SEASTAR_THREAD_TEST_CASE(test_fast_forwarding_combining_reader_with_galloping) {
    simple_schema s;
    tests::reader_concurrency_semaphore_wrapper semaphore;
    auto permit = semaphore.make_permit();

    const auto pkeys = s.make_pkeys(7);
    const auto ckeys = s.make_ckeys(10);
    auto ring = to_ring_positions(pkeys);

    auto make_n_mutations = [&] (auto ckeys, int n) {
        std::vector<mutation> ret;
        for (int i = 0; i < n; i++) {
            ret.push_back(make_partition_with_clustering_rows(s, pkeys[i], ckeys));
        }
        return ret;
    };

    auto pr = dht::partition_range::make(ring[0], ring[0]);
    std::vector<mutation_reader> v;
    v.push_back(make_mutation_reader_from_mutations_v2(s.schema(), permit, make_n_mutations(std::views::iota(0, 5), 7), pr));
    v.push_back(make_mutation_reader_from_mutations_v2(s.schema(), permit, make_n_mutations(std::views::iota(5, 10), 7), pr));

    assert_that(make_combined_reader(s.schema(), permit, std::move(v), streamed_mutation::forwarding::no, mutation_reader::forwarding::yes))
            .produces(make_partition_with_clustering_rows(s, pkeys[0], std::views::iota(0, 10)))
            .produces_end_of_stream()
            .fast_forward_to(dht::partition_range::make(ring[1], ring[1]))
            .produces(make_partition_with_clustering_rows(s, pkeys[1], std::views::iota(0, 10)))
            .produces_end_of_stream()
            .fast_forward_to(dht::partition_range::make(ring[3], ring[4]))
            .produces(make_partition_with_clustering_rows(s, pkeys[3], std::views::iota(0, 10)))
    .fast_forward_to(dht::partition_range::make({ ring[4], false }, ring[5]))
            .produces(make_partition_with_clustering_rows(s, pkeys[5], std::views::iota(0, 10)))
            .produces_end_of_stream()
    .fast_forward_to(dht::partition_range::make_starting_with(ring[6]))
            .produces(make_partition_with_clustering_rows(s, pkeys[6], std::views::iota(0, 10)))
            .produces_end_of_stream();
}

SEASTAR_TEST_CASE(test_sm_fast_forwarding_combining_reader) {
    return seastar::async([] {
        simple_schema s;
        tests::reader_concurrency_semaphore_wrapper semaphore;
        auto permit = semaphore.make_permit();

        const auto pkeys = s.make_pkeys(4);
        const auto ckeys = s.make_ckeys(4);

        auto make_mutation = [&] (uint32_t n) {
            mutation m(s.schema(), pkeys[n]);

            int i{0};
            s.add_row(m, ckeys[i], format("val_{:d}", i));
            ++i;
            s.add_row(m, ckeys[i], format("val_{:d}", i));
            ++i;
            s.add_row(m, ckeys[i], format("val_{:d}", i));
            ++i;
            s.add_row(m, ckeys[i], format("val_{:d}", i));

            return m;
        };

        std::vector<std::vector<mutation>> readers_mutations{
            {make_mutation(0), make_mutation(1), make_mutation(2), make_mutation(3)},
            {make_mutation(0)},
            {make_mutation(2)},
        };

        std::vector<mutation_reader> readers;
        for (auto& mutations : readers_mutations) {
            readers.emplace_back(make_mutation_reader_from_mutations_v2(s.schema(), permit, mutations, streamed_mutation::forwarding::yes));
        }

        assert_that(make_combined_reader(s.schema(), permit, std::move(readers), streamed_mutation::forwarding::yes, mutation_reader::forwarding::no))
                .produces_partition_start(pkeys[0])
                .produces_end_of_stream()
                .fast_forward_to(position_range::all_clustered_rows())
                .produces_row_with_key(ckeys[0])
                .next_partition()
                .produces_partition_start(pkeys[1])
                .produces_end_of_stream()
                .fast_forward_to(position_range(position_in_partition::before_key(ckeys[2]), position_in_partition::after_key(*s.schema(), ckeys[2])))
                .produces_row_with_key(ckeys[2])
                .produces_end_of_stream()
                .fast_forward_to(position_range(position_in_partition::after_key(*s.schema(), ckeys[2]), position_in_partition::after_all_clustered_rows()))
                .produces_row_with_key(ckeys[3])
                .produces_end_of_stream()
                .next_partition()
                .produces_partition_start(pkeys[2])
                .fast_forward_to(position_range::all_clustered_rows())
                .produces_row_with_key(ckeys[0])
                .produces_row_with_key(ckeys[1])
                .produces_row_with_key(ckeys[2])
                .produces_row_with_key(ckeys[3])
                .produces_end_of_stream();
    });
}

SEASTAR_THREAD_TEST_CASE(test_sm_fast_forwarding_combining_reader_with_galloping) {
    simple_schema s;
    tests::reader_concurrency_semaphore_wrapper semaphore;
    auto permit = semaphore.make_permit();

    const auto pkeys = s.make_pkeys(3);
    const auto ckeys = s.make_ckeys(10);
    auto ring = to_ring_positions(pkeys);

    auto make_n_mutations = [&] (auto ckeys, int n) {
        std::vector<mutation> ret;
        for (int i = 0; i < n; i++) {
            ret.push_back(make_partition_with_clustering_rows(s, pkeys[i], ckeys));
        }
        return ret;
    };

    auto pr = dht::partition_range::make(ring[0], ring[0]);
    std::vector<mutation_reader> v;
    v.push_back(make_mutation_reader_from_mutations_v2(s.schema(), permit, make_n_mutations(std::views::iota(0, 5), 3), streamed_mutation::forwarding::yes));
    v.push_back(make_mutation_reader_from_mutations_v2(s.schema(), permit, make_n_mutations(std::views::iota(5, 10), 3), streamed_mutation::forwarding::yes));

    auto reader = make_combined_reader(s.schema(), permit, std::move(v), streamed_mutation::forwarding::yes, mutation_reader::forwarding::no);
    auto assertions = assert_that(std::move(reader));
    assertions.produces_partition_start(pkeys[0])
            .produces_end_of_stream()
            .fast_forward_to(position_range::all_clustered_rows())
            .produces_row_with_key(ckeys[0])
            .produces_row_with_key(ckeys[1])
            .produces_row_with_key(ckeys[2])
            .produces_row_with_key(ckeys[3])
            .next_partition()
            .produces_partition_start(pkeys[1])
            .produces_end_of_stream()
            .fast_forward_to(position_range(position_in_partition::before_key(ckeys[0]), position_in_partition::after_key(*s.schema(), ckeys[3])))
            .produces_row_with_key(ckeys[0])
            .produces_row_with_key(ckeys[1])
            .produces_row_with_key(ckeys[2])
            .produces_row_with_key(ckeys[3])
            .produces_end_of_stream()
            .fast_forward_to(position_range(position_in_partition::after_key(*s.schema(), ckeys[6]), position_in_partition::after_all_clustered_rows()))
            .produces_row_with_key(ckeys[7])
            .produces_row_with_key(ckeys[8])
            .produces_row_with_key(ckeys[9])
            .produces_end_of_stream()
            .next_partition()
            .produces_partition_start(pkeys[2])
            .fast_forward_to(position_range::all_clustered_rows());

    for (int i = 0; i < 10; i++) {
        assertions.produces_row_with_key(ckeys[i]);
    }
    assertions.produces_end_of_stream();
}

class selector_of_empty_readers : public reader_selector {
    schema_ptr _schema;
    reader_permit _permit;
    size_t _remaining;
public:
    selector_of_empty_readers(schema_ptr s, reader_permit permit, size_t count)
        : reader_selector(s, dht::ring_position_view::min(), count)
        , _schema(s)
        , _permit(std::move(permit))
        , _remaining(count) {
    }
    virtual std::vector<mutation_reader> create_new_readers(const std::optional<dht::ring_position_view>& pos) override {
        if (_remaining == 0) {
            return {};
        }
        --_remaining;
        std::vector<mutation_reader> ret;
        ret.push_back(make_empty_flat_reader_v2(_schema, _permit));
        return ret;
    }
    virtual std::vector<mutation_reader> fast_forward_to(const dht::partition_range& pr) override {
        SCYLLA_ASSERT(false); // Fast forward not supported by this reader
        return {};
    }
};

// Reproduces scylladb/scylladb#14415
SEASTAR_THREAD_TEST_CASE(test_combined_reader_with_incrementally_opened_empty_readers) {
    static constexpr size_t empty_reader_count = 10 * 1000;

    simple_schema s;
    tests::reader_concurrency_semaphore_wrapper semaphore;
    auto permit = semaphore.make_permit();

    auto reader = make_combined_reader(s.schema(), permit,
                std::make_unique<selector_of_empty_readers>(s.schema(), permit, empty_reader_count),
                streamed_mutation::forwarding::no,
                mutation_reader::forwarding::no);

    // Expect that the reader won't produce a stack overflow
    assert_that(std::move(reader))
            .produces_end_of_stream();
}

SEASTAR_TEST_CASE(combined_mutation_reader_test) {
  return sstables::test_env::do_with_async([] (sstables::test_env& env) {
    simple_schema s;

    auto pkeys = s.make_pkeys(6);
    const auto ckeys = s.make_ckeys(4);

    std::ranges::sort(pkeys, [&s] (const dht::decorated_key& a, const dht::decorated_key& b) {
        return a.less_compare(*s.schema(), b);
    });

    auto make_sstable_mutations = [&] (sstring value_prefix, unsigned ckey_index, bool static_row, std::vector<unsigned> pkey_indexes) {
        std::vector<mutation> muts;

        for (auto pkey_index : pkey_indexes) {
            muts.emplace_back(s.schema(), pkeys[pkey_index]);
            auto& mut = muts.back();
            s.add_row(mut, ckeys[ckey_index], format("{}_{:d}_val", value_prefix, ckey_index));

            if (static_row) {
                s.add_static_row(mut, format("{}_static_val", value_prefix));
            }
        }

        return muts;
    };

    std::vector<mutation> sstable_level_0_0_mutations = make_sstable_mutations("level_0_0", 0, true,  {0, 1,       4   });
    std::vector<mutation> sstable_level_1_0_mutations = make_sstable_mutations("level_1_0", 1, false, {0, 1            });
    std::vector<mutation> sstable_level_1_1_mutations = make_sstable_mutations("level_1_1", 1, false, {      2, 3      });
    std::vector<mutation> sstable_level_2_0_mutations = make_sstable_mutations("level_2_0", 2, false, {   1,       4   });
    std::vector<mutation> sstable_level_2_1_mutations = make_sstable_mutations("level_2_1", 2, false, {               5});

    const mutation expexted_mutation_0 = sstable_level_0_0_mutations[0] + sstable_level_1_0_mutations[0];
    const mutation expexted_mutation_1 = sstable_level_0_0_mutations[1] + sstable_level_1_0_mutations[1] + sstable_level_2_0_mutations[0];
    const mutation expexted_mutation_2 = sstable_level_1_1_mutations[0];
    const mutation expexted_mutation_3 = sstable_level_1_1_mutations[1];
    const mutation expexted_mutation_4 = sstable_level_0_0_mutations[2] + sstable_level_2_0_mutations[1];
    const mutation expexted_mutation_5 = sstable_level_2_1_mutations[0];

    auto sst_factory = [&, s = s.schema()] (uint32_t level) {
        auto sst = env.make_sstable(s);
        sst->set_sstable_level(level);
        return sst;
    };

    std::vector<sstables::shared_sstable> sstable_list = {
            make_sstable_containing(sst_factory(0), std::move(sstable_level_0_0_mutations)),
            make_sstable_containing(sst_factory(1), std::move(sstable_level_1_0_mutations)),
            make_sstable_containing(sst_factory(1), std::move(sstable_level_1_1_mutations)),
            make_sstable_containing(sst_factory(2), std::move(sstable_level_2_0_mutations)),
            make_sstable_containing(sst_factory(2), std::move(sstable_level_2_1_mutations)),
    };

    auto cs = sstables::make_compaction_strategy(sstables::compaction_strategy_type::leveled, {});
    auto sstable_set = make_lw_shared<sstables::sstable_set>(cs.make_sstable_set(s.schema()));

    std::vector<mutation_reader> sstable_mutation_readers;

    auto list_permit = env.make_reader_permit();
    for (auto sst : sstable_list) {
        sstable_set->insert(sst);

        sstable_mutation_readers.emplace_back(
            sst->as_mutation_source().make_reader_v2(
                s.schema(),
                list_permit,
                query::full_partition_range,
                s.schema()->full_slice(),
                nullptr,
                streamed_mutation::forwarding::no,
                mutation_reader::forwarding::no));
    }

    auto list_reader = make_combined_reader(s.schema(), list_permit,
            std::move(sstable_mutation_readers));

    auto incremental_reader = sstable_set->make_local_shard_sstable_reader(
            s.schema(),
            env.make_reader_permit(),
            query::full_partition_range,
            s.schema()->full_slice(),
            nullptr,
            streamed_mutation::forwarding::no,
            mutation_reader::forwarding::no);

    assert_that(std::move(list_reader))
        .produces(expexted_mutation_0)
        .produces(expexted_mutation_1)
        .produces(expexted_mutation_2)
        .produces(expexted_mutation_3)
        .produces(expexted_mutation_4)
        .produces(expexted_mutation_5)
        .produces_end_of_stream();

    assert_that(std::move(incremental_reader))
        .produces(expexted_mutation_0)
        .produces(expexted_mutation_1)
        .produces(expexted_mutation_2)
        .produces(expexted_mutation_3)
        .produces(expexted_mutation_4)
        .produces(expexted_mutation_5)
        .produces_end_of_stream();
  });
}

static mutation make_mutation_with_key(simple_schema& s, dht::decorated_key dk) {
    static int i{0};

    mutation m(s.schema(), std::move(dk));
    s.add_row(m, s.make_ckey(++i), format("val_{:d}", i));
    return m;
}

class dummy_incremental_selector : public reader_selector {
    // To back _selector_position.
    schema_ptr _schema;
    reader_permit _permit;
    dht::ring_position _position;
    std::vector<std::vector<mutation>> _readers_mutations;
    streamed_mutation::forwarding _fwd;
    dht::partition_range _pr;

    mutation_reader pop_reader() {
        auto muts = std::move(_readers_mutations.back());
        _readers_mutations.pop_back();
        _position = _readers_mutations.empty() ? dht::ring_position::max() : _readers_mutations.back().front().decorated_key();
        _selector_position = _position;
        return make_mutation_reader_from_mutations_v2(_schema, _permit, std::move(muts), _pr, _fwd);
    }
public:
    // readers_mutations is expected to be sorted on both levels.
    // 1) the inner vector is expected to be sorted by decorated_key.
    // 2) the outer vector is expected to be sorted by the decorated_key
    //  of its first mutation.
    dummy_incremental_selector(schema_ptr s,
            reader_permit permit,
            std::vector<std::vector<mutation>> reader_mutations,
            dht::partition_range pr = query::full_partition_range,
            streamed_mutation::forwarding fwd = streamed_mutation::forwarding::no)
        : reader_selector(s, dht::ring_position_view::min(),
                          std::accumulate(reader_mutations.begin(), reader_mutations.end(), 0, [](size_t count, const auto& readers) { return count + readers.size(); }))
        , _schema(s)
        , _permit(std::move(permit))
        , _position(dht::ring_position::min())
        , _readers_mutations(std::move(reader_mutations))
        , _fwd(fwd)
        , _pr(std::move(pr)) {
        // So we can pop the next reader off the back
        std::ranges::reverse(_readers_mutations);
    }
    virtual std::vector<mutation_reader> create_new_readers(const std::optional<dht::ring_position_view>& pos) override {
        if (_readers_mutations.empty()) {
            return {};
        }

        std::vector<mutation_reader> readers;

        if (!pos) {
            readers.emplace_back(pop_reader());
            return readers;
        }

        while (!_readers_mutations.empty() && dht::ring_position_tri_compare(*_s, _selector_position, *pos) <= 0) {
            readers.emplace_back(pop_reader());
        }
        return readers;
    }
    virtual std::vector<mutation_reader> fast_forward_to(const dht::partition_range& pr) override {
        _pr = pr;
        return create_new_readers(dht::ring_position_view::for_range_start(_pr));
    }
};

SEASTAR_TEST_CASE(reader_selector_gap_between_readers_test) {
    return seastar::async([] {
        simple_schema s;
        tests::reader_concurrency_semaphore_wrapper semaphore;
        auto pkeys = s.make_pkeys(3);

        std::ranges::sort(pkeys, [&s] (const dht::decorated_key& a, const dht::decorated_key& b) {
            return a.less_compare(*s.schema(), b);
        });

        auto mut1 = make_mutation_with_key(s, pkeys[0]);
        auto mut2a = make_mutation_with_key(s, pkeys[1]);
        auto mut2b = make_mutation_with_key(s, pkeys[1]);
        auto mut3 = make_mutation_with_key(s, pkeys[2]);
        std::vector<std::vector<mutation>> readers_mutations{
            {mut1},
            {mut2a},
            {mut2b},
            {mut3}
        };

        auto permit = semaphore.make_permit();
        auto reader = make_combined_reader(s.schema(), permit,
                std::make_unique<dummy_incremental_selector>(s.schema(), permit, std::move(readers_mutations)),
                streamed_mutation::forwarding::no,
                mutation_reader::forwarding::no);

        assert_that(std::move(reader))
            .produces_partition(mut1)
            .produces_partition(mut2a + mut2b)
            .produces_partition(mut3)
            .produces_end_of_stream();
    });
}

SEASTAR_TEST_CASE(reader_selector_overlapping_readers_test) {
    return seastar::async([] {
        simple_schema s;
        tests::reader_concurrency_semaphore_wrapper semaphore;
        auto pkeys = s.make_pkeys(4);

        std::ranges::sort(pkeys, [&s] (const dht::decorated_key& a, const dht::decorated_key& b) {
            return a.less_compare(*s.schema(), b);
        });

        auto mut1 = make_mutation_with_key(s, pkeys[0]);
        auto mut2a = make_mutation_with_key(s, pkeys[1]);
        auto mut2b = make_mutation_with_key(s, pkeys[1]);
        auto mut3a = make_mutation_with_key(s, pkeys[2]);
        auto mut3b = make_mutation_with_key(s, pkeys[2]);
        auto mut3c = make_mutation_with_key(s, pkeys[2]);
        auto mut4a = make_mutation_with_key(s, pkeys[3]);
        auto mut4b = make_mutation_with_key(s, pkeys[3]);

        tombstone tomb(100, {});
        mut2b.partition().apply(tomb);

        s.add_row(mut2a, s.make_ckey(1), "a");
        s.add_row(mut2b, s.make_ckey(2), "b");

        s.add_row(mut3a, s.make_ckey(1), "a");
        s.add_row(mut3b, s.make_ckey(2), "b");
        s.add_row(mut3c, s.make_ckey(3), "c");

        s.add_row(mut4a, s.make_ckey(1), "a");
        s.add_row(mut4b, s.make_ckey(2), "b");

        std::vector<std::vector<mutation>> readers_mutations{
            {mut1, mut2a, mut3a},
            {mut2b, mut3b},
            {mut3c, mut4a},
            {mut4b},
        };

        auto permit = semaphore.make_permit();
        auto reader = make_combined_reader(s.schema(), permit,
                std::make_unique<dummy_incremental_selector>(s.schema(), permit, std::move(readers_mutations)),
                streamed_mutation::forwarding::no,
                mutation_reader::forwarding::no);

        assert_that(std::move(reader))
            .produces_partition(mut1)
            .produces_partition(mut2a + mut2b)
            .produces_partition(mut3a + mut3b + mut3c)
            .produces_partition(mut4a + mut4b)
            .produces_end_of_stream();
    });
}

SEASTAR_TEST_CASE(reader_selector_fast_forwarding_test) {
    return seastar::async([] {
        simple_schema s;
        tests::reader_concurrency_semaphore_wrapper semaphore;
        auto pkeys = s.make_pkeys(5);

        std::ranges::sort(pkeys, [&s] (const dht::decorated_key& a, const dht::decorated_key& b) {
            return a.less_compare(*s.schema(), b);
        });

        auto mut1a = make_mutation_with_key(s, pkeys[0]);
        auto mut1b = make_mutation_with_key(s, pkeys[0]);
        auto mut2a = make_mutation_with_key(s, pkeys[1]);
        auto mut2c = make_mutation_with_key(s, pkeys[1]);
        auto mut3a = make_mutation_with_key(s, pkeys[2]);
        auto mut3d = make_mutation_with_key(s, pkeys[2]);
        auto mut4b = make_mutation_with_key(s, pkeys[3]);
        auto mut5b = make_mutation_with_key(s, pkeys[4]);
        std::vector<std::vector<mutation>> readers_mutations{
            {mut1a, mut2a, mut3a},
            {mut1b, mut4b, mut5b},
            {mut2c},
            {mut3d},
        };

        auto permit = semaphore.make_permit();
        auto reader = make_combined_reader(s.schema(), permit,
                std::make_unique<dummy_incremental_selector>(s.schema(), permit,
                        std::move(readers_mutations),
                        dht::partition_range::make_ending_with(dht::partition_range::bound(pkeys[1], false))),
                streamed_mutation::forwarding::no,
                mutation_reader::forwarding::yes);

        assert_that(std::move(reader))
            .produces_partition(mut1a + mut1b)
            .produces_end_of_stream()
            .fast_forward_to(dht::partition_range::make(dht::partition_range::bound(pkeys[2], true), dht::partition_range::bound(pkeys[3], true)))
            .produces_partition(mut3a + mut3d)
            .fast_forward_to(dht::partition_range::make_starting_with(dht::partition_range::bound(pkeys[4], true)))
            .produces_partition(mut5b)
            .produces_end_of_stream();
    });
}

static mutation compacted(const mutation& m) {
    auto result = m;
    result.partition().compact_for_compaction(*result.schema(), always_gc, result.decorated_key(), gc_clock::now(), tombstone_gc_state(nullptr));
    return result;
}

SEASTAR_TEST_CASE(test_fast_forwarding_combined_reader_is_consistent_with_slicing) {
    return sstables::test_env::do_with_async([&] (sstables::test_env& env) {
        random_mutation_generator gen(random_mutation_generator::generate_counters::no);
        auto s = gen.schema();
        auto permit = env.make_reader_permit();

        const int n_readers = 10;
        auto keys = gen.make_partition_keys(3);
        std::vector<mutation> combined;
        std::list<dht::partition_range> reader_ranges;
        std::vector<mutation_reader> readers;
        for (int i = 0; i < n_readers; ++i) {
            std::vector<mutation> muts;
            for (auto&& key : keys) {
                mutation m = compacted(gen());
                muts.push_back(mutation(s, key, std::move(m.partition())));
            }
            if (combined.empty()) {
                combined = muts;
            } else {
                int j = 0;
                for (auto&& m : muts) {
                    combined[j++].apply(m);
                }
            }
            mutation_source ds = make_sstable_containing(env.make_sstable(s), muts)->as_mutation_source();
            reader_ranges.push_back(dht::partition_range::make({keys[0]}, {keys[0]}));
            readers.push_back(ds.make_reader_v2(s,
                permit,
                reader_ranges.back(),
                s->full_slice(), nullptr,
                streamed_mutation::forwarding::yes,
                mutation_reader::forwarding::yes));
        }

        auto rd = mutation_fragment_v1_stream(make_combined_reader(s, permit, std::move(readers),
            streamed_mutation::forwarding::yes,
            mutation_reader::forwarding::yes));
        auto close_rd = deferred_close(rd);

        std::vector<query::clustering_range> ranges = gen.make_random_ranges(3);

        auto check_next_partition = [&] (const mutation& expected) {
            mutation result(expected.schema(), expected.decorated_key());

            rd.consume_pausable([&](mutation_fragment&& mf) {
                position_in_partition::less_compare less(*s);
                if (!less(mf.position(), position_in_partition_view::before_all_clustered_rows())) {
                    BOOST_FAIL(format("Received clustering fragment: {}", mutation_fragment::printer(*s, mf)));
                }
                result.partition().apply(*s, std::move(mf));
                return stop_iteration::no;
            }).get();

            for (auto&& range : ranges) {
                auto prange = position_range(range);
                rd.fast_forward_to(prange).get();
                rd.consume_pausable([&](mutation_fragment&& mf) {
                    if (!mf.relevant_for_range(*s, prange.start())) {
                        BOOST_FAIL(format("Received fragment which is not relevant for range: {}, range: {}", mutation_fragment::printer(*s, mf), prange));
                    }
                    position_in_partition::less_compare less(*s);
                    if (!less(mf.position(), prange.end())) {
                        BOOST_FAIL(format("Received fragment is out of range: {}, range: {}", mutation_fragment::printer(*s, mf), prange));
                    }
                    result.partition().apply(*s, std::move(mf));
                    return stop_iteration::no;
                }).get();
            }

            assert_that(result).is_equal_to(expected, ranges);
        };

        check_next_partition(combined[0]);
        rd.fast_forward_to(dht::partition_range::make_singular(keys[2])).get();
        check_next_partition(combined[2]);
    });
}

SEASTAR_TEST_CASE(test_combined_reader_slicing_with_overlapping_range_tombstones) {
    return sstables::test_env::do_with_async([&] (sstables::test_env& env) {
        simple_schema ss;
        auto s = ss.schema();

        auto rt1 = ss.make_range_tombstone(ss.make_ckey_range(1, 10));
        auto rt2 = ss.make_range_tombstone(ss.make_ckey_range(1, 5)); // rt1 + rt2 = {[1, 5], (5, 10]}

        mutation m1(ss.schema(), tests::generate_partition_key(ss.schema()));
        m1.partition().apply_delete(*s, rt1);
        mutation m2 = m1;
        m2.partition().apply_delete(*s, rt2);
        ss.add_row(m2, ss.make_ckey(4), "v2"); // position after rt2.position() but before rt2.end_position().

        std::vector<mutation_reader> readers;

        mutation_source ds1 = make_sstable_containing(env.make_sstable(s), {m1})->as_mutation_source();
        mutation_source ds2 = make_sstable_containing(env.make_sstable(s), {m2})->as_mutation_source();

        // upper bound ends before the row in m2, so that the raw is fetched after next fast forward.
        auto range = ss.make_ckey_range(0, 3);

        {
            auto permit = env.make_reader_permit();
            auto slice = partition_slice_builder(*s).with_range(range).build();
            readers.push_back(ds1.make_reader_v2(s, permit, query::full_partition_range, slice));
            readers.push_back(ds2.make_reader_v2(s, permit, query::full_partition_range, slice));

            auto rd = mutation_fragment_v1_stream(make_combined_reader(s, permit, std::move(readers),
                streamed_mutation::forwarding::no, mutation_reader::forwarding::no));
            auto close_rd = deferred_close(rd);

            auto prange = position_range(range);
            mutation result(m1.schema(), m1.decorated_key());

            rd.consume_pausable([&] (mutation_fragment&& mf) {
                if (mf.position().has_clustering_key() && !mf.range(*s).overlaps(*s, prange.start(), prange.end())) {
                    BOOST_FAIL(format("Received fragment which is not relevant for the slice: {}, slice: {}", mutation_fragment::printer(*s, mf), range));
                }
                result.partition().apply(*s, std::move(mf));
                return stop_iteration::no;
            }).get();

            assert_that(result).is_equal_to(m1 + m2, query::clustering_row_ranges({range}));
        }

        // Check fast_forward_to()
        {
            auto permit = env.make_reader_permit();
            readers.push_back(ds1.make_reader_v2(s, permit, query::full_partition_range, s->full_slice(),
                nullptr, streamed_mutation::forwarding::yes));
            readers.push_back(ds2.make_reader_v2(s, permit, query::full_partition_range, s->full_slice(),
                nullptr, streamed_mutation::forwarding::yes));

            auto rd = mutation_fragment_v1_stream(make_combined_reader(s, permit, std::move(readers),
                streamed_mutation::forwarding::yes, mutation_reader::forwarding::no));
            auto close_rd = deferred_close(rd);

            auto prange = position_range(range);
            mutation result(m1.schema(), m1.decorated_key());

            rd.consume_pausable([&](mutation_fragment&& mf) {
                BOOST_REQUIRE(!mf.position().has_clustering_key());
                result.partition().apply(*s, std::move(mf));
                return stop_iteration::no;
            }).get();

            rd.fast_forward_to(prange).get();

            position_in_partition last_pos = position_in_partition::before_all_clustered_rows();
            auto consume_clustered = [&] (mutation_fragment&& mf) {
                position_in_partition::less_compare less(*s);
                if (less(mf.position(), last_pos)) {
                    BOOST_FAIL(format("Out of order fragment: {}, last pos: {}", mutation_fragment::printer(*s, mf), last_pos));
                }
                last_pos = position_in_partition(mf.position());
                result.partition().apply(*s, std::move(mf));
                return stop_iteration::no;
            };

            rd.consume_pausable(consume_clustered).get();
            rd.fast_forward_to(position_range(prange.end(), position_in_partition::after_all_clustered_rows())).get();
            rd.consume_pausable(consume_clustered).get();

            assert_that(result).is_equal_to(m1 + m2);
        }
    });
}

SEASTAR_TEST_CASE(test_combined_mutation_source_is_a_mutation_source) {
    return seastar::async([] {
        tests::reader_concurrency_semaphore_wrapper semaphore;

        // Creates a mutation source which combines N mutation sources with mutation fragments spread
        // among them in a round robin fashion.
        auto make_combined_populator = [&semaphore] (int n_sources) mutable {
            return [=, &semaphore] (schema_ptr s, const std::vector<mutation>& muts) mutable {
                std::vector<lw_shared_ptr<replica::memtable>> memtables;
                for (int i = 0; i < n_sources; ++i) {
                    memtables.push_back(make_lw_shared<replica::memtable>(s));
                }

                for (auto&& m : muts) {
                    auto rd = make_mutation_reader_from_mutations_v2(s, semaphore.make_permit(), m);
                    auto close_rd = deferred_close(rd);
                    auto muts = rd.consume(fragment_scatterer(s, n_sources)).get();
                    for (int i = 0; i < n_sources; ++i) {
                        memtables[i]->apply(std::move(muts[i]));
                    }
                }

                std::vector<mutation_source> sources;
                for (auto&& mt : memtables) {
                    sources.push_back(mt->as_data_source());
                }
                return make_combined_mutation_source(std::move(sources));
            };
        };
        run_mutation_source_tests(make_combined_populator(1));
        run_mutation_source_tests(make_combined_populator(2));
        run_mutation_source_tests(make_combined_populator(3));
    });
}

// Best run with SMP >= 2
SEASTAR_THREAD_TEST_CASE(test_foreign_reader_as_mutation_source) {
    if (smp::count < 2) {
        std::cerr << "Cannot run test " << get_name() << " with smp::count < 2" << std::endl;
        return;
    }

    do_with_cql_env_thread([] (cql_test_env& env) -> future<> {
        auto populate = [&env] (schema_ptr s, const std::vector<mutation>& mutations) {
            const auto remote_shard = (this_shard_id() + 1) % smp::count;
            auto frozen_mutations =
                mutations
                | std::views::transform([] (const mutation& m) { return freeze(m); })
                | std::ranges::to<std::vector>();
            auto remote_mt = smp::submit_to(remote_shard, [s = global_schema_ptr(s), &frozen_mutations] {
                auto mt = make_lw_shared<replica::memtable>(s.get());

                for (auto& mut : frozen_mutations) {
                    mt->apply(mut, s.get());
                }

                return make_foreign(mt);
            }).get();

            auto reader_factory = [&env, remote_shard, remote_mt = std::move(remote_mt)] (schema_ptr s,
                    reader_permit permit,
                    const dht::partition_range& range,
                    const query::partition_slice& slice,
                    tracing::trace_state_ptr trace_state,
                    streamed_mutation::forwarding fwd_sm,
                    mutation_reader::forwarding fwd_mr) {
                auto remote_reader = env.db().invoke_on(remote_shard,
                        [&, s = global_schema_ptr(s), fwd_sm, fwd_mr, trace_state = tracing::global_trace_state_ptr(trace_state)] (replica::database& db) {
                    return make_foreign(std::make_unique<mutation_reader>(remote_mt->make_mutation_reader(s.get(),
                            make_reader_permit(env),
                            range,
                            slice,
                            trace_state.get(),
                            fwd_sm,
                            fwd_mr)));
                }).get();
                return make_foreign_reader(s, std::move(permit), std::move(remote_reader), fwd_sm);
            };

            auto reader_factory_ptr = make_lw_shared<decltype(reader_factory)>(std::move(reader_factory));

            return mutation_source([reader_factory_ptr] (schema_ptr s,
                    reader_permit permit,
                    const dht::partition_range& range,
                    const query::partition_slice& slice,
                    tracing::trace_state_ptr trace_state,
                    streamed_mutation::forwarding fwd_sm,
                    mutation_reader::forwarding fwd_mr) {
                return (*reader_factory_ptr)(std::move(s), std::move(permit), range, slice, std::move(trace_state), fwd_sm, fwd_mr);
            });
        };

        run_mutation_source_tests(populate);
        return make_ready_future<>();
    }).get();
}

SEASTAR_TEST_CASE(test_trim_clustering_row_ranges_to) {
    struct null { };
    struct missing { };
    struct key {
        int c0;
        std::variant<int, null, missing> c1;

        key(int c0, int c1) : c0(c0), c1(c1) { }
        key(int c0, null) : c0(c0), c1(null{}) { }
        key(int c0) : c0(c0), c1(missing{}) { }

        clustering_key to_clustering_key(const schema& s) const {
            std::vector<bytes> v;
            v.push_back(int32_type->decompose(data_value(c0)));
            std::visit(make_visitor(
                    [&v] (int c1) { v.push_back(int32_type->decompose(data_value(c1))); },
                    [&v] (null c1) { v.push_back(bytes{}); },
                    [] (missing) { }),
                    c1);
            return clustering_key::from_exploded(s, std::move(v));
        }
    };
    struct incl {
        key value;

        incl(int c0, int c1) : value(c0, c1) { }
        incl(int c0, null) : value(c0, null{}) { }
        incl(int c0) : value(c0) { }
    };
    struct excl {
        key value;

        excl(int c0, int c1) : value(c0, c1) { }
        excl(int c0, null) : value(c0, null{}) { }
        excl(int c0) : value(c0) { }
    };
    struct bound {
        key value;
        bool inclusive;

        bound(incl b) : value(b.value), inclusive(true) { }
        bound(excl b) : value(b.value), inclusive(false) { }
    };
    struct inf {
    };
    struct range {
        std::optional<bound> start;
        std::optional<bound> end;
        bool singular = false;

        range(bound s, bound e) : start(s), end(e) { }
        range(inf, bound e) : end(e) { }
        range(bound s, inf) : start(s) { }
        range(inf, inf) { }
        range(bound b) : start(b), end(b), singular(true) { }

        static std::optional<interval_bound<clustering_key>> to_bound(const schema& s, std::optional<bound> b) {
            if (b) {
                return interval_bound<clustering_key>(b->value.to_clustering_key(s), b->inclusive);
            }
            return {};
        }
        query::clustering_range to_clustering_range(const schema& s) const {
            return query::clustering_range(to_bound(s, start), to_bound(s, end), singular);
        }
    };

    const auto schema = schema_builder("ks", get_name())
            .with_column("p0", int32_type, column_kind::partition_key)
            .with_column("c0", int32_type, column_kind::clustering_key)
            .with_column("c1", int32_type, column_kind::clustering_key)
            .with_column("v1", int32_type, column_kind::regular_column)
            .build();

    const auto check = [](std::vector<range> ranges, key key, std::vector<range> output_ranges, schema_ptr schema,
            seastar::compat::source_location sl = seastar::compat::source_location::current()) {
        auto actual_ranges = ranges | std::views::transform(
                    [&] (const range& r) { return r.to_clustering_range(*schema); })
            | std::ranges::to<query::clustering_row_ranges>();

        query::trim_clustering_row_ranges_to(*schema, actual_ranges, key.to_clustering_key(*schema));

        const auto expected_ranges = output_ranges | std::views::transform(
                    [&] (const range& r) { return r.to_clustering_range(*schema); })
            | std::ranges::to<query::clustering_row_ranges>();

        if (!std::equal(actual_ranges.begin(), actual_ranges.end(), expected_ranges.begin(), expected_ranges.end(),
                    [tri_cmp = clustering_key::tri_compare(*schema)] (const query::clustering_range& a, const query::clustering_range& b) {
            return a.equal(b, tri_cmp);
        })) {
            BOOST_FAIL(fmt::format("Unexpected result\nexpected {}\ngot {}\ncalled from {}:{}", expected_ranges, actual_ranges, sl.file_name(), sl.line()));
        }
    };

    auto check_forward = [schema, &check] (std::vector<range> ranges, key key, std::vector<range> output_ranges,
            seastar::compat::source_location sl = seastar::compat::source_location::current()) {
        return check(std::move(ranges), std::move(key), std::move(output_ranges), std::move(schema), sl);
    };

    auto check_reversed = [schema = schema->make_reversed(), &check] (std::vector<range> ranges, key key, std::vector<range> output_ranges,
            seastar::compat::source_location sl = seastar::compat::source_location::current()) {
        return check(std::move(ranges), std::move(key), std::move(output_ranges), std::move(schema), sl);
    };

    // We want to check the following cases:
    //  1) Before range
    //  2) Equal to begin(range with incl begin)
    //  3) Equal to begin(range with excl begin)
    //  4) Intersect with range (excl end)
    //  5) Intersect with range (incl end)
    //  6) Intersect with range (inf end)
    //  7) Equal to end(range with incl end)
    //  8) Equal to end(range with excl end)
    //  9) After range
    // 10) Full range
    // 11) Prefix key is before range
    // 12) Prefix key is equal to prefix start of range
    // 13) Prefix key intersects with range
    // 14) Prefix key is after range

    // (1)
    check_forward(
            { {incl{1, 6}, excl{2, 3}}, {incl{3}, excl{4}}, {incl{7, 9}, incl{999, 0}} },
            {1, 0},
            { {incl{1, 6}, excl{2, 3}}, {incl{3}, excl{4}}, {incl{7, 9}, incl{999, 0}} });

    // (2)
    check_forward(
            { {incl{1, 6}, excl{2, 3}}, {incl{3}, excl{4}}, {incl{7, 9}, incl{999, 0}} },
            {1, 6},
            { {excl{1, 6}, excl{2, 3}}, {incl{3}, excl{4}}, {incl{7, 9}, incl{999, 0}} });

    // (2) - prefix
    check_forward(
            { {incl{1, 6}, excl{2, 3}}, {incl{3}, excl{4}}, {incl{7, 9}, incl{999, 0}} },
            {3, 6},
            { {excl{3, 6}, excl{4}}, {incl{7, 9}, incl{999, 0}} });

    // (3)
    check_forward(
            { {incl{1, 6}, excl{2, 3}}, {excl{2, 3}, incl{2, 4}}, {incl{3}, excl{4}}, {incl{7, 9}, incl{999, 0}} },
            {2, 3},
            { {excl{2, 3}, incl{2, 4}}, {incl{3}, excl{4}}, {incl{7, 9}, incl{999, 0}} });

    // (3) - prefix
    check_forward(
            { {incl{1, 6}, excl{2, 3}}, {excl{2, 3}, incl{2, 4}}, {excl{3}, excl{4}}, {incl{7, 9}, incl{999, 0}} },
            {3, 7},
            { {excl{3}, excl{4}}, {incl{7, 9}, incl{999, 0}} });

    // (4)
    check_forward(
            { {incl{1, 6}, excl{2, 3}}, {incl{3}, excl{4}}, {incl{7, 9}, incl{999, 0}} },
            {2, 0},
            { {excl{2, 0}, excl{2, 3}}, {incl{3}, excl{4}}, {incl{7, 9}, incl{999, 0}} });

    // (5)
    check_forward(
            { {incl{1, 6}, excl{2, 3}}, {incl{3}, excl{4}}, {incl{7, 9}, incl{999, 0}} },
            {90, 90},
            { {excl{90, 90}, incl{999, 0}} });

    // (6)
    check_forward(
            { {incl{1, 6}, excl{2, 3}}, {incl{3}, excl{4}}, {incl{7, 9}, inf{}} },
            {90, 90},
            { {excl{90, 90}, inf{}} });

    // (7)
    check_forward(
            { {incl{1, 6}, incl{2, 3}}, {incl{3}, excl{4}}, {incl{7, 9}, excl{999, 0}} },
            {2, 3},
            { {incl{3}, excl{4}}, {incl{7, 9}, excl{999, 0}} });

    // (7) - prefix
    check_forward(
            { {incl{1, 6}, incl{2, 3}}, {incl{3}, incl{4}}, {incl{7, 9}, excl{999, 0}} },
            {4, 39},
            { {excl{4, 39}, incl{4}}, {incl{7, 9}, excl{999, 0}} });

    // (8)
    check_forward(
            { {incl{1, 6}, excl{2, 3}}, {incl{3}, excl{4}}, {incl{7, 9}, excl{999, 0}} },
            {2, 3},
            { {incl{3}, excl{4}}, {incl{7, 9}, excl{999, 0}} });

    // (8) - prefix
    check_forward(
            { {incl{1, 6}, incl{2, 3}}, {incl{3}, excl{4}}, {incl{7, 9}, excl{999, 0}} },
            {4, 11},
            { {incl{7, 9}, excl{999, 0}} });

    // (9)
    check_forward(
            { {incl{1, 6}, excl{2, 3}}, {incl{3}, excl{4}}, {incl{7, 9}, excl{999, 0}} },
            {2, 4},
            { {incl{3}, excl{4}}, {incl{7, 9}, excl{999, 0}} });

    // (10)
    check_forward(
            { {inf{}, inf{}} },
            {7, 9},
            { {excl{7, 9}, inf{}} });

    // (11)
    check_forward(
            { {incl{10, 10}, excl{10, 30}} },
            {10},
            { {incl{10, 10}, excl{10, 30}} });

    // (12)
    check_forward(
            { {incl{10}, excl{10, 30}} },
            {10},
            { {incl{10, null{}}, excl{10, 30}} });

    // (13)
    check_forward(
            { {incl{9, 10}, excl{10, 30}} },
            {10},
            { {incl{10, null{}}, excl{10, 30}} });

    // (14)
    check_forward(
            { {incl{9, 10}, excl{10, 30}} },
            {11},
            { });

    // In reversed now

    // (1)
    check_reversed(
            { {incl{999, 0}, incl{7, 9}}, {excl{4}, incl{3}}, {excl{2, 3}, incl{1, 6}} },
            {999, 1},
            { {incl{999, 0}, incl{7, 9}}, {excl{4}, incl{3}}, {excl{2, 3}, incl{1, 6}} });

    // (2)
    check_reversed(
            { {incl{999, 0}, incl{7, 9}}, {excl{4}, incl{3}}, {incl{2, 4}, excl{2, 3}}, {excl{2, 3}, incl{1, 6}} },
            {2, 4},
            { {excl{2, 4}, excl{2, 3}}, {excl{2, 3}, incl{1, 6}} });

    // (2) - prefix
    check_reversed(
            { {incl{999, 0}, incl{7, 9}}, {incl{4}, incl{3}} , {incl{2, 4}, excl{2, 3}}, {excl{2, 3}, incl{1, 6}} },
            {4, 43453},
            { {excl{4, 43453}, incl{3}}, {incl{2, 4}, excl{2, 3}}, {excl{2, 3}, incl{1, 6}} });

    // (3)
    check_reversed(
            { { incl{999, 0}, incl{7, 9} }, { excl{4}, incl{3} }, { excl{2, 3}, incl{1, 6} } },
            {2, 3},
            { {excl{2, 3}, incl{1, 6}} });

    // (3) - prefix
    check_reversed(
            { {incl{999, 0}, incl{7, 9}}, {excl{4}, incl{3}}, {excl{2, 3}, incl{1, 6}} },
            {4, 3},
            { {excl{4}, incl{3}}, {excl{2, 3}, incl{1, 6}} });

    // (4)
    check_reversed(
            { {excl{999, 0}, incl{7, 9}}, {excl{4}, incl{3}}, {excl{2, 3}, incl{1, 6}} },
            {8, 0},
            { {excl{8, 0}, incl{7, 9}}, {excl{4}, incl{3}}, {excl{2, 3}, incl{1, 6}} });

    // (5)
    check_reversed(
            { {incl{999, 0}, incl{7, 9}}, {excl{4}, incl{3}}, {excl{2, 3}, incl{1, 6}} },
            {90, 90},
            { {excl{90, 90}, incl{7, 9}}, {excl{4}, incl{3}}, {excl{2, 3}, incl{1, 6}} });

    // (6)
    check_reversed(
            { {inf{}, incl{7, 9}}, {excl{4}, incl{3}}, {excl{2, 3}, inf{}} },
            {1, 90},
            { {excl{1, 90}, inf{}} });

    // (7)
    check_reversed(
            { {excl{999, 0}, incl{7, 9}}, {excl{4}, incl{3}}, {incl{2, 3}, incl{1, 6}} },
            {7, 9},
            { {excl{4}, incl{3}}, {incl{2, 3}, incl{1, 6}} });

    // (7) - prefix
    check_reversed(
            { {excl{999, 0}, incl{7, 9}}, {excl{4}, incl{3}}, {incl{2, 3}, incl{1, 6}} },
            {3, 673},
            { {excl{3, 673}, incl{3}}, {incl{2, 3}, incl{1, 6}} });

    // (8)
    check_reversed(
            { {excl{999, 0}, incl{7, 9}}, {excl{4}, incl{3}}, {excl{2, 3}, excl{1, 6}} },
            {1, 6},
            { });

    // (8) - prefix
    check_reversed(
            { {excl{999, 0}, incl{7, 9} }, {excl{4}, excl{3} }, {incl{2, 3}, incl{1, 6} } },
            {3, 673},
            { {incl{2, 3}, incl{1, 6}} });

    // (9)
    check_reversed(
            { {excl{999, 0}, incl{7, 9}}, {excl{4}, incl{3}}, {excl{2, 3}, incl{1, 6}} },
            {0, 4},
            {});

    // (10)
    check_reversed(
            { {inf{}, inf{}} },
            {7, 9},
            { {excl{7, 9}, inf{}} });

    // (11)
    check_reversed(
            { {excl{10, 30}, incl{10, 10}} },
            {11},
            { {excl{10, 30}, incl{10, 10}} });

    // (12)
    check_reversed(
            { {incl{10}, excl{9, 39}} },
            {10},
            { {incl{10, null{}}, excl{9, 39}} });

    // (13)
    check_reversed(
            { {incl{10, 30}, incl{9, 10}} },
            {10},
            { {incl{10, null{}}, incl{9, 10}} });

    // (14)
    check_reversed(
            { {excl{10, 30}, incl{9, 10}} },
            {9},
            { });

    return make_ready_future<>();
}

// Best run with SMP >= 3
SEASTAR_THREAD_TEST_CASE(test_multishard_combining_reader_reading_empty_table) {
    if (smp::count < 3) {
        std::cerr << "Cannot run test " << get_name() << " with smp::count < 2" << std::endl;
        return;
    }

    do_with_cql_env_thread([&] (cql_test_env& env) -> future<> {
        std::vector<std::atomic<bool>> shards_touched(smp::count);
        simple_schema s;

        env.execute_cql(s.cql()).get();
        auto& table = env.db().local().find_column_family(s.schema()->ks_name(), s.schema()->cf_name());
        auto erm = table.get_effective_replication_map();

        auto factory = [&shards_touched] (
                schema_ptr s,
                reader_permit permit,
                const dht::partition_range& range,
                const query::partition_slice& slice,
                tracing::trace_state_ptr trace_state,
                mutation_reader::forwarding fwd_mr) {
            shards_touched[this_shard_id()] = true;
            return make_empty_flat_reader_v2(s, std::move(permit));
        };

        assert_that(make_multishard_combining_reader_v2(
                    seastar::make_shared<test_reader_lifecycle_policy>(std::move(factory)),
                    s.schema(),
                    erm,
                    make_reader_permit(env),
                    query::full_partition_range,
                    s.schema()->full_slice()))
                .produces_end_of_stream();

        for (unsigned i = 0; i < smp::count; ++i) {
            BOOST_REQUIRE(shards_touched.at(i));
        }

        return make_ready_future<>();
    }).get();
}

// A reader that can controlled by it's "creator" after it's created.
//
// It can execute one of a set of actions on it's fill_buffer() call:
// * fill the buffer completely with generated data
// * block until the pupet master releases it
//
// It's primary purpose is to aid in testing multishard_combining_reader's
// read-ahead related corner-cases. It allows for the test code to have
// fine-grained control over which shard will fill the multishard reader's
// buffer and how much read-ahead it launches and consequently when the
// read-ahead terminates.
class puppet_reader_v2 : public mutation_reader::impl {
public:
    struct control {
        promise<> buffer_filled;
        bool destroyed = true;
        bool pending = false;
        unsigned fast_forward_to = 0;
    };

    enum class fill_buffer_action {
        fill,
        block
    };

private:
    simple_schema _s;
    control& _ctrl;
    std::vector<fill_buffer_action> _actions;
    std::vector<uint32_t> _pkeys;
    unsigned _partition_index = 0;

    bool maybe_push_next_partition() {
        if (_partition_index == _pkeys.size()) {
            _end_of_stream = true;
            return false;
        }
        push_mutation_fragment(*_s.schema(), _permit, partition_start(_s.make_pkey(_pkeys.at(_partition_index++)), {}));
        return true;
    }

    void do_fill_buffer() {
        if (!maybe_push_next_partition()) {
            return;
        }
        auto ck = uint32_t(0);
        while (!is_buffer_full()) {
            push_mutation_fragment(*_s.schema(), _permit, _s.make_row_v2(_permit, _s.make_ckey(ck++), make_random_string(2 << 5)));
        }

        push_mutation_fragment(*_s.schema(), _permit, partition_end());
    }

public:
    puppet_reader_v2(simple_schema s, reader_permit permit, control& ctrl, std::vector<fill_buffer_action> actions, std::vector<uint32_t> pkeys)
        : impl(s.schema(), std::move(permit))
        , _s(std::move(s))
        , _ctrl(ctrl)
        , _actions(std::move(actions))
        , _pkeys(std::move(pkeys)) {
        std::ranges::reverse(_actions);
        _end_of_stream = false;
        _ctrl.destroyed = false;
    }
    ~puppet_reader_v2() {
        _ctrl.destroyed = true;
    }

    virtual future<> fill_buffer() override {
        if (is_end_of_stream() || !is_buffer_empty()) {
            return make_ready_future<>();
        }
        if (_actions.empty()) {
            _end_of_stream = true;
            return make_ready_future<>();
        }

        auto action = _actions.back();
        _actions.pop_back();

        switch (action) {
        case fill_buffer_action::fill:
            do_fill_buffer();
            return make_ready_future<>();
        case fill_buffer_action::block:
            do_fill_buffer();
            _ctrl.pending = true;
            return _ctrl.buffer_filled.get_future().then([this] {
                BOOST_REQUIRE(!_ctrl.destroyed);
                _ctrl.pending = false;
                return make_ready_future<>();
            });
        }
        abort();
    }
    virtual future<> next_partition() override { return make_ready_future<>(); }
    virtual future<> fast_forward_to(const dht::partition_range& pr) override {
        ++_ctrl.fast_forward_to;
        clear_buffer();
        _end_of_stream = true;
        return make_ready_future<>();
    }
    virtual future<> fast_forward_to(position_range) override {
        return make_exception_future<>(make_backtraced_exception_ptr<std::bad_function_call>());
    }
    virtual future<> close() noexcept override {
        return make_ready_future<>();
    };
};

// Test a background pending read-ahead.
//
// Foreign reader launches a new background read-ahead (fill_buffer()) after
// each remote operation (fill_buffer() and fast_forward_to()) is completed.
// This read-ahead executes on the background and is only synchronized with
// when a next remote operation is executed. If the reader is destroyed before
// this synchronization can happen then the remote read-ahead will outlive its
// owner. Check that when the reader is closed, it waits on any background
// readhead to complete gracefully and will not cause any memory errors.
//
// Theory of operation:
// 1) Call foreign_reader::fill_buffer() -> will start read-ahead in the
//    background;
// 2) [shard 1] puppet_reader blocks the read-ahead;
// 3) Start closing the foreign_reader;
// 4) Unblock read-ahead -> the now orphan read-ahead fiber executes;
//
// Best run with smp >= 2
SEASTAR_THREAD_TEST_CASE(test_stopping_reader_with_pending_read_ahead) {
    if (smp::count < 2) {
        std::cerr << "Cannot run test " << get_name() << " with smp::count < 2" << std::endl;
        return;
    }

    do_with_cql_env_thread([] (cql_test_env& env) -> future<> {
        const auto shard_of_interest = (this_shard_id() + 1) % smp::count;
        auto s = simple_schema();
        auto remote_control_remote_reader = smp::submit_to(shard_of_interest, [&env, gs = global_simple_schema(s)] {
            using control_type = foreign_ptr<std::unique_ptr<puppet_reader_v2::control>>;
            using reader_type = foreign_ptr<std::unique_ptr<mutation_reader>>;

            auto control = make_foreign(std::make_unique<puppet_reader_v2::control>());
            auto reader = make_foreign(std::make_unique<mutation_reader>(make_mutation_reader<puppet_reader_v2>(gs.get(),
                    make_reader_permit(env),
                    *control,
                    std::vector{puppet_reader_v2::fill_buffer_action::fill, puppet_reader_v2::fill_buffer_action::block},
                    std::vector<uint32_t>{0, 1})));

            return make_ready_future<std::tuple<control_type, reader_type>>(std::tuple(std::move(control), std::move(reader)));
        }).get();

        auto& remote_control = std::get<0>(remote_control_remote_reader);
        auto& remote_reader = std::get<1>(remote_control_remote_reader);

            auto reader = make_foreign_reader(
                    s.schema(),
                    make_reader_permit(env),
                    std::move(remote_reader));

            reader.fill_buffer().get();

            BOOST_REQUIRE(!reader.is_buffer_empty());

        BOOST_REQUIRE(!smp::submit_to(shard_of_interest, [remote_control = remote_control.get()] {
            return remote_control->destroyed;
        }).get());

        bool buffer_filled = false;
        auto destroyed_after_close = reader.close().then([&] {
            // close should wait on readahead and complete
            // only after `remote_control->buffer_filled.set_value()`
            // is executed below.
            BOOST_REQUIRE(buffer_filled);
            return smp::submit_to(shard_of_interest, [remote_control = remote_control.get()] {
                return remote_control->destroyed;
            });
        });

        smp::submit_to(shard_of_interest, [remote_control = remote_control.get(), &buffer_filled] {
            buffer_filled = true;
            remote_control->buffer_filled.set_value();
        }).get();

        BOOST_REQUIRE(destroyed_after_close.get());

        return make_ready_future<>();
    }).get();
}

struct multishard_reader_for_read_ahead {
    static const unsigned min_shards = 3;
    static const unsigned blocked_shard = 2;

    mutation_reader reader;
    std::unique_ptr<dht::sharder> sharder;
    std::vector<foreign_ptr<std::unique_ptr<puppet_reader_v2::control>>> remote_controls;
    std::unique_ptr<dht::partition_range> pr;
};

multishard_reader_for_read_ahead prepare_multishard_reader_for_read_ahead_test(simple_schema& s, reader_permit permit) {
    auto remote_controls = std::vector<foreign_ptr<std::unique_ptr<puppet_reader_v2::control>>>();
    remote_controls.reserve(smp::count);
    for (unsigned i = 0; i < smp::count; ++i) {
        remote_controls.emplace_back(nullptr);
    }

    parallel_for_each(std::views::iota(0u, smp::count), [&remote_controls] (unsigned shard) mutable {
        return smp::submit_to(shard, [] {
            return make_foreign(std::make_unique<puppet_reader_v2::control>());
        }).then([shard, &remote_controls] (foreign_ptr<std::unique_ptr<puppet_reader_v2::control>>&& ctr) mutable {
            remote_controls[shard] = std::move(ctr);
        });
    }).get();

    // We need two tokens for each shard
    std::map<dht::token, unsigned> pkeys_by_tokens;
    for (unsigned i = 0; i < smp::count * 2; ++i) {
        pkeys_by_tokens.emplace(s.make_pkey(i).token(), i);
    }

    auto shard_pkeys = std::vector<std::vector<uint32_t>>(smp::count, std::vector<uint32_t>{});
    auto i = unsigned(0);
    for (auto pkey : pkeys_by_tokens | std::views::values) {
        shard_pkeys[i++ % smp::count].push_back(pkey);
    }

    auto remote_control_refs = std::vector<puppet_reader_v2::control*>();
    remote_control_refs.reserve(smp::count);
    for (auto& rc : remote_controls) {
        remote_control_refs.push_back(rc.get());
    }

    auto factory = [gs = global_simple_schema(s), remote_controls = std::move(remote_control_refs), shard_pkeys = std::move(shard_pkeys)] (
            schema_ptr,
            reader_permit permit,
            const dht::partition_range& range,
            const query::partition_slice& slice,
            tracing::trace_state_ptr trace_state,
            mutation_reader::forwarding) mutable {
        const auto shard = this_shard_id();
        auto actions = [shard] () -> std::vector<puppet_reader_v2::fill_buffer_action> {
            if (shard < multishard_reader_for_read_ahead::blocked_shard) {
                return {puppet_reader_v2::fill_buffer_action::fill};
            } else if (shard == multishard_reader_for_read_ahead::blocked_shard) {
                return {puppet_reader_v2::fill_buffer_action::block};
            } else {
                return {};
            }
        }();
        return make_mutation_reader<puppet_reader_v2>(gs.get(), permit, *remote_controls.at(shard), std::move(actions), shard_pkeys.at(shard));
    };

    auto pr = std::make_unique<dht::partition_range>(dht::partition_range::make(dht::ring_position::starting_at(pkeys_by_tokens.begin()->first),
                dht::ring_position::ending_at(pkeys_by_tokens.rbegin()->first)));

    auto sharder = std::make_unique<dummy_sharder>(s.schema()->get_sharder(), std::move(pkeys_by_tokens));
    auto reader = make_multishard_combining_reader_v2_for_tests(*sharder, seastar::make_shared<test_reader_lifecycle_policy>(std::move(factory)),
            s.schema(), permit, *pr, s.schema()->full_slice());

    return {std::move(reader), std::move(sharder), std::move(remote_controls), std::move(pr)};
}

// Regression test for #7945
SEASTAR_THREAD_TEST_CASE(test_multishard_combining_reader_custom_shard_number) {
    if (smp::count < 2) {
        std::cerr << "Cannot run test " << get_name() << " with smp::count < 2" << std::endl;
        return;
    }

    auto no_shards = smp::count - 1;

    do_with_cql_env_thread([&] (cql_test_env& env) -> future<> {
        std::vector<std::atomic<bool>> shards_touched(smp::count);
        simple_schema s;
        auto sharder = std::make_unique<dht::static_sharder>(no_shards, 0);
        auto factory = [&shards_touched] (
                schema_ptr s,
                reader_permit permit,
                const dht::partition_range& range,
                const query::partition_slice& slice,
                tracing::trace_state_ptr trace_state,
                mutation_reader::forwarding fwd_mr) {
            shards_touched[this_shard_id()] = true;
            return make_empty_flat_reader_v2(s, std::move(permit));
        };

        assert_that(make_multishard_combining_reader_v2_for_tests(
                *sharder,
                seastar::make_shared<test_reader_lifecycle_policy>(std::move(factory)),
                s.schema(),
                make_reader_permit(env),
                query::full_partition_range,
                s.schema()->full_slice()))
            .produces_end_of_stream();

        for (unsigned i = 0; i < no_shards; ++i) {
            BOOST_REQUIRE(shards_touched[i]);
        }
        BOOST_REQUIRE(!shards_touched[no_shards]);

        return make_ready_future<>();
    }).get();
}

// Regression test for #8161
SEASTAR_THREAD_TEST_CASE(test_multishard_combining_reader_only_reads_from_needed_shards) {
    if (smp::count < 2) {
        std::cerr << "Cannot run test " << get_name() << " with smp::count < 2" << std::endl;
        return;
    }

    do_with_cql_env_thread([&] (cql_test_env& env) -> future<> {
        std::vector<std::atomic<bool>> shards_touched(smp::count);
        simple_schema s;
        auto factory = [&shards_touched] (
                schema_ptr s,
                reader_permit permit,
                const dht::partition_range& range,
                const query::partition_slice& slice,
                tracing::trace_state_ptr trace_state,
                mutation_reader::forwarding fwd_mr) {
            shards_touched[this_shard_id()] = true;
            return make_empty_flat_reader_v2(s, std::move(permit));
        };

        env.execute_cql(s.cql()).get();
        auto& table = env.db().local().find_column_family(s.schema()->ks_name(), s.schema()->cf_name());
        auto erm = table.get_effective_replication_map();

        std::vector<bool> expected_shards_touched(smp::count);

        const dht::sharder& sharder = erm->get_sharder(*s.schema());
        dht::token start_token(0);
        dht::token end_token(0);
        const auto additional_shards = tests::random::get_int<unsigned>(0, smp::count - 1);

        auto shard = sharder.shard_for_reads(start_token);
        expected_shards_touched[shard] = true;

        for (auto i = 0u; i < additional_shards; ++i) {
            shard = (shard + 1) % smp::count;
            end_token = sharder.token_for_next_shard_for_reads(end_token, shard);
            expected_shards_touched[shard] = true;
        }
        const auto inclusive_end = !additional_shards || tests::random::get_bool();
        auto pr = dht::partition_range::make(
                dht::ring_position(start_token, dht::ring_position::token_bound::start),
                dht::ring_position(end_token, inclusive_end ? dht::ring_position::token_bound::end : dht::ring_position::token_bound::start));

        if (!inclusive_end) {
            expected_shards_touched[shard] = false;
        }

        testlog.info("{}: including {} additional shards out of a total of {}, with an {} end", get_name(), additional_shards, smp::count,
                inclusive_end ? "inclusive" : "exclusive");

        assert_that(make_multishard_combining_reader_v2(
                seastar::make_shared<test_reader_lifecycle_policy>(std::move(factory)),
                s.schema(),
                erm,
                make_reader_permit(env),
                pr,
                s.schema()->full_slice()))
            .produces_end_of_stream();

        for (unsigned i = 0; i < smp::count; ++i) {
            testlog.info("[{}]: {} == {}", i, shards_touched[i], expected_shards_touched[i]);
            BOOST_CHECK(shards_touched[i] == expected_shards_touched[i]);
        }

        return make_ready_future<>();
    }).get();
}

// Test a background pending read-ahead outliving the reader.
//
// The multishard reader will issue read-aheads according to its internal
// concurrency. This concurrency starts from 1 and is increased every time
// a remote reader blocks (buffer is empty) within the same fill_buffer() call.
// The read-ahead is run in the background and the fiber will not be
// synchronized with until the multishard reader reaches the shard in question
// with the normal reading. If the multishard reader is destroyed before the
// synchronization happens the fiber is orphaned. Test that the fiber is
// prepared for this possibility and doesn't attempt to read any members of any
// destroyed objects causing memory errors.
//
// Theory of operation:
// 1) First read a full buffer from shard 0;
// 2) Shard 0 has no more data so move on to shard 1; puppet reader's buffer is
//    empty -> increase concurrency to 2 because we traversed to another shard
//    in the same fill_buffer() call;
// 3) [shard 2] puppet reader -> read-ahead launched in the background but it's
//    blocked;
// 4) Reader is destroyed;
// 5) Resume the shard 2's puppet reader -> the now orphan read-ahead fiber
//    executes;
//
// Best run with smp >= 3
SEASTAR_THREAD_TEST_CASE(test_multishard_combining_reader_destroyed_with_pending_read_ahead) {
    if (smp::count < multishard_reader_for_read_ahead::min_shards) {
        std::cerr << "Cannot run test " << get_name() << " with smp::count < " << multishard_reader_for_read_ahead::min_shards << std::endl;
        return;
    }

    do_with_cql_env_thread([&] (cql_test_env& env) -> future<> {
        auto s = simple_schema();

        auto reader_sharder_remote_controls__ = prepare_multishard_reader_for_read_ahead_test(s, make_reader_permit(env));
        auto&& reader = reader_sharder_remote_controls__.reader;
        auto&& remote_controls = reader_sharder_remote_controls__.remote_controls;

        // This will read shard 0's buffer only
        reader.fill_buffer().get();
        BOOST_REQUIRE(reader.is_buffer_full());
        reader.detach_buffer();

        // This will move to shard 1 and trigger read-ahead on shard 2
        reader.fill_buffer().get();
        BOOST_REQUIRE(reader.is_buffer_full());

        // Check that shard with read-ahead is indeed blocked.
        BOOST_REQUIRE(eventually_true([&] {
            return smp::submit_to(multishard_reader_for_read_ahead::blocked_shard,
                    [control = remote_controls.at(multishard_reader_for_read_ahead::blocked_shard).get()] {
                return control->pending;
            }).get();
        }));

        // Destroy reader.
        testlog.debug("Starting to close the reader");
        auto fut = reader.close();

        parallel_for_each(std::views::iota(0u, smp::count), [&remote_controls] (unsigned shard) mutable {
            return smp::submit_to(shard, [control = remote_controls.at(shard).get()] {
                control->buffer_filled.set_value();
            });
        }).get();

        fut.get();
        testlog.debug("Reader is closed");

        BOOST_REQUIRE(eventually_true([&] {
            return map_reduce(std::views::iota(0u, smp::count), [&] (unsigned shard) {
                    return smp::submit_to(shard, [&remote_controls, shard] {
                        return remote_controls.at(shard)->destroyed;
                    });
                },
                true,
                std::logical_and<bool>()).get();
        }));

        return make_ready_future<>();
    }).get();
}

SEASTAR_THREAD_TEST_CASE(test_multishard_combining_reader_fast_forwarded_with_pending_read_ahead) {
    if (smp::count < multishard_reader_for_read_ahead::min_shards) {
        std::cerr << "Cannot run test " << get_name() << " with smp::count < " << multishard_reader_for_read_ahead::min_shards << std::endl;
        return;
    }

    do_with_cql_env_thread([&] (cql_test_env& env) -> future<> {
        auto s = simple_schema();

        auto reader_sharder_remote_controls_pr = prepare_multishard_reader_for_read_ahead_test(s, make_reader_permit(env));
        auto&& reader = reader_sharder_remote_controls_pr.reader;
        auto&& remote_controls = reader_sharder_remote_controls_pr.remote_controls;
        auto&& pr = reader_sharder_remote_controls_pr.pr;

        reader.fill_buffer().get();
        BOOST_REQUIRE(reader.is_buffer_full());
        reader.detach_buffer();

        reader.fill_buffer().get();
        BOOST_REQUIRE(reader.is_buffer_full());
        reader.detach_buffer();

        BOOST_REQUIRE(eventually_true([&] {
            return smp::submit_to(multishard_reader_for_read_ahead::blocked_shard,
                    [control = remote_controls.at(multishard_reader_for_read_ahead::blocked_shard).get()] {
                return control->pending;
            }).get();
        }));

        auto end_token = dht::token(pr->end()->value().token());
        ++end_token._data;

        auto next_pr = dht::partition_range::make_starting_with(dht::ring_position::starting_at(end_token));
        auto fut = reader.fast_forward_to(next_pr);

        smp::submit_to(multishard_reader_for_read_ahead::blocked_shard,
                [control = remote_controls.at(multishard_reader_for_read_ahead::blocked_shard).get()] {
            control->buffer_filled.set_value();
        }).get();

        fut.get();

        const auto all_shard_fast_forwarded = map_reduce(
                std::views::iota(0u, multishard_reader_for_read_ahead::blocked_shard + 1u),
                [&] (unsigned shard) {
                    return smp::submit_to(shard, [control = remote_controls.at(shard).get()] {
                        return control->fast_forward_to == 1;
                    });
                },
                true,
                std::logical_and<bool>()).get();

        BOOST_REQUIRE(all_shard_fast_forwarded);

        reader.fill_buffer().get();

        BOOST_REQUIRE(reader.is_buffer_empty());
        BOOST_REQUIRE(reader.is_end_of_stream());

        reader.close().get();

        BOOST_REQUIRE(eventually_true([&] {
            return map_reduce(std::views::iota(0u, smp::count), [&] (unsigned shard) {
                    return smp::submit_to(shard, [&remote_controls, shard] {
                        return remote_controls.at(shard)->destroyed;
                    });
                },
                true,
                std::logical_and<bool>()).get();
        }));

        return make_ready_future<>();
    }).get();
}

SEASTAR_THREAD_TEST_CASE(test_multishard_combining_reader_next_partition) {
    do_with_cql_env_thread([&] (cql_test_env& env) -> future<> {
        env.execute_cql("CREATE KEYSPACE multishard_combining_reader_next_partition_ks"
                " WITH REPLICATION = {'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1};").get();
        env.execute_cql("CREATE TABLE multishard_combining_reader_next_partition_ks.test (pk int, v int, PRIMARY KEY(pk));").get();

        const auto insert_id = env.prepare("INSERT INTO multishard_combining_reader_next_partition_ks.test (\"pk\", \"v\") VALUES (?, ?);").get();

        const auto partition_count = 1000;

        for (int pk = 0; pk < partition_count; ++pk) {
            env.execute_prepared(insert_id, {{
                    cql3::raw_value::make_value(serialized(pk)),
                    cql3::raw_value::make_value(serialized(0))}}).get();
        }

        auto schema = env.local_db().find_column_family("multishard_combining_reader_next_partition_ks", "test").schema();
        auto& partitioner = schema->get_partitioner();

        auto pkeys =
                std::views::iota(0, partition_count) |
                std::views::transform([schema, &partitioner] (int i) {
                    return partitioner.decorate_key(*schema, partition_key::from_singular(*schema, i));
                }) |
                std::ranges::to<std::vector<dht::decorated_key>>();

        // We want to test corner cases around next_partition() called when it
        // cannot be resolved with just the buffer and it has to be forwarded
        // to the correct shard reader, so set a buffer size so that only a
        // single fragment can fit into it at a time.
        const auto max_buffer_size = size_t{1};

        auto factory = [db = &env.db()] (
                schema_ptr schema,
                reader_permit permit,
                const dht::partition_range& range,
                const query::partition_slice& slice,
                tracing::trace_state_ptr trace_state,
                mutation_reader::forwarding fwd_mr) {
            auto& table = db->local().find_column_family(schema);
            auto reader = table.as_mutation_source().make_reader_v2(
                    schema,
                    std::move(permit),
                    range,
                    slice,
                    std::move(trace_state),
                    streamed_mutation::forwarding::no,
                    fwd_mr);
            reader.set_max_buffer_size(max_buffer_size);
            return reader;
        };
        auto& table = env.db().local().find_column_family(schema);
        auto reader = make_multishard_combining_reader_v2(
                seastar::make_shared<test_reader_lifecycle_policy>(std::move(factory)),
                schema,
                table.get_effective_replication_map(),
                make_reader_permit(env),
                query::full_partition_range,
                schema->full_slice());

        reader.set_max_buffer_size(max_buffer_size);

        std::ranges::sort(pkeys, [schema] (const dht::decorated_key& a, const dht::decorated_key& b) {
            return dht::ring_position_tri_compare(*schema, a, b) < 0;
        });

        testlog.info("Start test");

        auto assertions = assert_that(std::move(reader));
        for (int i = 0; i < partition_count; ++i) {
            assertions.produces(pkeys[i]);
        }
        assertions.produces_end_of_stream();

        return make_ready_future<>();
    }).get();
}

// Test the multishard streaming reader in the context it was designed to work
// in: as a mean to read data belonging to a shard according to a different
// sharding configuration.
// The reference data is provided by a filtering reader.
SEASTAR_THREAD_TEST_CASE(test_multishard_streaming_reader) {
    if (smp::count < 3) {
        std::cerr << "Cannot run test " << get_name() << " with smp::count < 3" << std::endl;
        return;
    }

    do_with_cql_env_thread([&] (cql_test_env& env) -> future<> {
        env.execute_cql("CREATE KEYSPACE multishard_streaming_reader_ks WITH REPLICATION = {'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1};").get();
        env.execute_cql("CREATE TABLE multishard_streaming_reader_ks.test (pk int, v int, PRIMARY KEY(pk));").get();

        const auto insert_id = env.prepare("INSERT INTO multishard_streaming_reader_ks.test (\"pk\", \"v\") VALUES (?, ?);").get();

        const auto partition_count = 10000;

        for (int pk = 0; pk < partition_count; ++pk) {
            env.execute_prepared(insert_id, {{
                    cql3::raw_value::make_value(serialized(pk)),
                    cql3::raw_value::make_value(serialized(0))}}).get();
        }

        auto schema = env.local_db().find_column_family("multishard_streaming_reader_ks", "test").schema();

        auto token_range = dht::token_range::make_open_ended_both_sides();
        auto partition_range = dht::to_partition_range(token_range);

        auto& local_partitioner = schema->get_sharder();
        auto remote_partitioner = dht::static_sharder(local_partitioner.shard_count() - 1, local_partitioner.sharding_ignore_msb());

        auto tested_reader = make_multishard_streaming_reader(env.db(), schema, make_reader_permit(env),
                [sharder = dht::selective_token_range_sharder(remote_partitioner, token_range, 0)] () mutable -> std::optional<dht::partition_range> {
            if (auto next = sharder.next()) {
                return dht::to_partition_range(*next);
            }
            return std::nullopt;
        }, gc_clock::now(), {}, read_ahead::yes);
        auto close_tested_reader = deferred_close(tested_reader);

        auto reader_factory = [db = &env.db()] (
                schema_ptr s,
                reader_permit permit,
                const dht::partition_range& range,
                const query::partition_slice& slice,
                tracing::trace_state_ptr trace_state,
                mutation_reader::forwarding fwd_mr) mutable {
            auto& table = db->local().find_column_family(s);
            return table.as_mutation_source().make_reader_v2(std::move(s), std::move(permit), range, slice, std::move(trace_state),
                    streamed_mutation::forwarding::no, fwd_mr);
        };
        auto& table = env.db().local().find_column_family(schema);
        auto erm = table.get_effective_replication_map();
        auto reference_reader = make_filtering_reader(
                make_multishard_combining_reader_v2(seastar::make_shared<test_reader_lifecycle_policy>(std::move(reader_factory)),
                    schema, erm, make_reader_permit(env), partition_range, schema->full_slice()),
                [&remote_partitioner] (const dht::decorated_key& pkey) {
                    return remote_partitioner.shard_of(pkey.token()) == 0;
                });
        auto close_reference_reader = deferred_close(reference_reader);

        std::vector<mutation> reference_muts;
        while (auto mut_opt = read_mutation_from_mutation_reader(reference_reader).get()) {
            reference_muts.push_back(std::move(*mut_opt));
        }

        std::vector<mutation> tested_muts;
        while (auto mut_opt = read_mutation_from_mutation_reader(tested_reader).get()) {
            tested_muts.push_back(std::move(*mut_opt));
        }

        BOOST_CHECK_EQUAL(reference_muts.size(), tested_muts.size());

        const auto min_size = std::min(reference_muts.size(), tested_muts.size());
        for (size_t i = 0; i < min_size; ++i) {
            testlog.trace("Comparing mutation {:d}/{:d}", i, min_size - 1);
            assert_that(tested_muts[i]).is_equal_to(reference_muts[i]);
        }

        return make_ready_future<>();
    }).get();
}

SEASTAR_THREAD_TEST_CASE(test_queue_reader) {
    tests::reader_concurrency_semaphore_wrapper semaphore;
    auto gen = random_mutation_generator(random_mutation_generator::generate_counters::no);

    const auto expected_muts = gen(20);

    // Simultaneous read and write
    {
        auto read_all = [] (mutation_reader& reader, std::vector<mutation>& muts) {
            return async([&reader, &muts] {
                auto close_reader = deferred_close(reader);
                while (auto mut_opt = read_mutation_from_mutation_reader(reader).get()) {
                    muts.emplace_back(std::move(*mut_opt));
                }
            });
        };

        auto write_all = [&semaphore] (queue_reader_handle_v2& handle, const std::vector<mutation>& muts) {
            return async([&] {
                auto reader = make_mutation_reader_from_mutations_v2(muts.front().schema(), semaphore.make_permit(), muts);
                auto close_reader = deferred_close(reader);
                while (auto mf_opt = reader().get()) {
                    handle.push(std::move(*mf_opt)).get();
                }
                handle.push_end_of_stream();
            });
        };

        auto actual_muts = std::vector<mutation>{};
        actual_muts.reserve(20);

        auto p = make_queue_reader_v2(gen.schema(), semaphore.make_permit());
        auto& reader = std::get<0>(p);
        auto& handle = std::get<1>(p);
        auto close_reader = deferred_close(reader);
        when_all_succeed(read_all(reader, actual_muts), write_all(handle, expected_muts)).get();
        BOOST_REQUIRE_EQUAL(actual_muts.size(), expected_muts.size());
        for (size_t i = 0; i < expected_muts.size(); ++i) {
            BOOST_REQUIRE_EQUAL(actual_muts.at(i), expected_muts.at(i));
        }
    }

    // abort() -- check that consumer is aborted
    {
        auto p = make_queue_reader_v2(gen.schema(), semaphore.make_permit());
        auto& reader = std::get<0>(p);
        auto& handle = std::get<1>(p);
        auto close_reader = deferred_close(reader);
        auto fill_buffer_fut = reader.fill_buffer();

        auto expected_reader = make_mutation_reader_from_mutations_v2(expected_muts.front().schema(), semaphore.make_permit(), expected_muts);
        auto close_expected_reader = deferred_close(expected_reader);

        handle.push(std::move(*expected_reader().get())).get();

        BOOST_REQUIRE(!fill_buffer_fut.available());

        handle.abort(std::make_exception_ptr<std::runtime_error>(std::runtime_error("error")));

        BOOST_REQUIRE_THROW(fill_buffer_fut.get(), std::runtime_error);
        BOOST_REQUIRE_THROW(handle.push(mutation_fragment_v2(*gen.schema(), semaphore.make_permit(), partition_end{})).get(), std::runtime_error);
        BOOST_REQUIRE(!reader.is_end_of_stream());
    }

    // abort() -- check that producer is aborted
    {
        auto p = make_queue_reader_v2(gen.schema(), semaphore.make_permit());
        auto& reader = std::get<0>(p);
        auto& handle = std::get<1>(p);
        auto close_reader = deferred_close(reader);
        reader.set_max_buffer_size(1);

        auto expected_reader = make_mutation_reader_from_mutations_v2(expected_muts.front().schema(), semaphore.make_permit(), expected_muts);
        auto close_expected_reader = deferred_close(expected_reader);

        auto push_fut = make_ready_future<>();
        while (push_fut.available()) {
            push_fut = handle.push(std::move(*expected_reader().get()));
        }

        BOOST_REQUIRE(!push_fut.available());

        handle.abort(std::make_exception_ptr<std::runtime_error>(std::runtime_error("error")));

        BOOST_REQUIRE_THROW(reader.fill_buffer().get(), std::runtime_error);
        BOOST_REQUIRE_THROW(push_fut.get(), std::runtime_error);
        BOOST_REQUIRE(!reader.is_end_of_stream());
    }

    // Detached handle
    {
        auto p = make_queue_reader_v2(gen.schema(), semaphore.make_permit());
        auto& reader = std::get<0>(p);
        auto& handle = std::get<1>(p);
        auto fill_buffer_fut = reader.fill_buffer();

        {
            auto throwaway_reader = std::move(reader);
            throwaway_reader.close().get();
        }

        BOOST_REQUIRE_THROW(handle.push(mutation_fragment_v2(*gen.schema(), semaphore.make_permit(), partition_end{})).get(), std::runtime_error);
        BOOST_REQUIRE_THROW(handle.push_end_of_stream(), std::runtime_error);
        BOOST_REQUIRE_NO_THROW(fill_buffer_fut.get());
    }

    // Abandoned handle aborts, move-assignment
    {
        auto p = make_queue_reader_v2(gen.schema(), semaphore.make_permit());
        auto& reader = std::get<0>(p);
        auto& handle = std::get<1>(p);
        auto close_reader = deferred_close(reader);
        auto fill_buffer_fut = reader.fill_buffer();

        auto expected_reader = make_mutation_reader_from_mutations_v2(expected_muts.front().schema(), semaphore.make_permit(), expected_muts);
        auto close_expected_reader = deferred_close(expected_reader);

        handle.push(std::move(*expected_reader().get())).get();

        BOOST_REQUIRE(!fill_buffer_fut.available());

        {
            auto p = make_queue_reader_v2(gen.schema(), semaphore.make_permit());
            auto& throwaway_reader = std::get<0>(p);
            auto& throwaway_handle = std::get<1>(p);
            auto close_throwaway_reader = deferred_close(throwaway_reader);
            // Overwrite handle
            handle = std::move(throwaway_handle);
        }

        BOOST_REQUIRE_THROW(fill_buffer_fut.get(), std::runtime_error);
        BOOST_REQUIRE_THROW(handle.push(mutation_fragment_v2(*gen.schema(), semaphore.make_permit(), partition_end{})).get(), std::runtime_error);
    }

    // Abandoned handle aborts, destructor
    {
        auto p = make_queue_reader_v2(gen.schema(), semaphore.make_permit());
        auto& reader = std::get<0>(p);
        auto& handle = std::get<1>(p);
        auto close_reader = deferred_close(reader);
        auto fill_buffer_fut = reader.fill_buffer();

        auto expected_reader = make_mutation_reader_from_mutations_v2(expected_muts.front().schema(), semaphore.make_permit(), expected_muts);
        auto close_expected_reader = deferred_close(expected_reader);

        handle.push(std::move(*expected_reader().get())).get();

        BOOST_REQUIRE(!fill_buffer_fut.available());

        {
            // Destroy handle
            queue_reader_handle_v2 throwaway_handle(std::move(handle));
        }

        BOOST_REQUIRE_THROW(fill_buffer_fut.get(), std::runtime_error);
        BOOST_REQUIRE_THROW(handle.push(mutation_fragment_v2(*gen.schema(), semaphore.make_permit(), partition_end{})).get(), std::runtime_error);
    }

    // Life-cycle, relies on ASAN for error reporting
    {
        auto p = make_queue_reader_v2(gen.schema(), semaphore.make_permit());
        auto& reader = std::get<0>(p);
        auto& handle = std::get<1>(p);
        auto close_reader = deferred_close(reader);
        {
            auto throwaway_p = make_queue_reader_v2(gen.schema(), semaphore.make_permit());
            auto& throwaway_reader = std::get<0>(throwaway_p);
            auto& throwaway_handle = std::get<1>(throwaway_p);
            auto close_throwaway_reader = deferred_close(throwaway_reader);
            // Overwrite handle
            handle = std::move(throwaway_handle);

            auto another_throwaway_p = make_queue_reader_v2(gen.schema(), semaphore.make_permit());
            auto& another_throwaway_reader = std::get<0>(another_throwaway_p);
            auto& another_throwaway_handle = std::get<1>(another_throwaway_p);
            auto close_another_throwaway_reader = deferred_close(another_throwaway_reader);

            // Overwrite with moved-from handle (move assignment operator)
            another_throwaway_handle = std::move(throwaway_handle);

            // Overwrite with moved-from handle (move constructor)
            queue_reader_handle_v2 yet_another_throwaway_handle(std::move(throwaway_handle));
        }
    }

    // push_end_of_stream() detaches handle from reader, relies on ASAN for error reporting
    {
        auto p = make_queue_reader_v2(gen.schema(), semaphore.make_permit());
        auto& reader = std::get<0>(p);
        auto& handle = std::get<1>(p);
        auto close_reader = deferred_close(reader);
        {
            auto throwaway_handle = std::move(handle);
            throwaway_handle.push_end_of_stream();
        }
    }
}

SEASTAR_THREAD_TEST_CASE(test_compacting_reader_as_mutation_source) {
    auto make_populate = [] (bool single_fragment_buffer) {
        return [single_fragment_buffer] (schema_ptr s, const std::vector<mutation>& mutations, gc_clock::time_point query_time) mutable {
            auto mt = make_memtable(s, mutations);
            return mutation_source([=] (
                    schema_ptr s,
                    reader_permit permit,
                    const dht::partition_range& range,
                    const query::partition_slice& slice,
                    tracing::trace_state_ptr trace_state,
                    streamed_mutation::forwarding fwd_sm,
                    mutation_reader::forwarding fwd_mr) mutable {
                auto source = mt->make_mutation_reader(s, std::move(permit), range, slice, std::move(trace_state), streamed_mutation::forwarding::no, fwd_mr);
                if (fwd_sm == streamed_mutation::forwarding::yes) {
                    source = make_forwardable(std::move(source));
                }
                auto mr = make_compacting_reader(std::move(source), query_time,
                        can_never_purge,
                        tombstone_gc_state(nullptr), fwd_sm);
                if (single_fragment_buffer) {
                    mr.set_max_buffer_size(1);
                }
                return mr;
            });
        };
    };

    BOOST_TEST_MESSAGE("run_mutation_source_tests(single_fragment_buffer=false)");
    run_mutation_source_tests(make_populate(false));
    BOOST_TEST_MESSAGE("run_mutation_source_tests(single_fragment_buffer=true)");
    run_mutation_source_tests(make_populate(true));
}

// Check that next_partition() in the middle of a partition works properly.
SEASTAR_THREAD_TEST_CASE(test_compacting_reader_next_partition) {
    simple_schema ss(simple_schema::with_static::no);
    const auto& schema = *ss.schema();
    tests::reader_concurrency_semaphore_wrapper semaphore;
    std::deque<mutation_fragment_v2> expected;

    auto mr = [&] () {
        auto permit = semaphore.make_permit();
        const size_t buffer_size = 1024;
        std::deque<mutation_fragment_v2> mfs;
        auto dks = ss.make_pkeys(2);
        const auto& dk0 = dks[0];
        const auto& dk1 = dks[1];

        mfs.emplace_back(*ss.schema(), permit, partition_start(dk0, tombstone{}));

        auto i = 0;
        size_t mfs_size = 0;
        while (mfs_size <= buffer_size) {
            mfs.emplace_back(*ss.schema(), permit, ss.make_row_v2(permit, ss.make_ckey(i++), "v"));
            mfs_size += mfs.back().memory_usage();
        }
        mfs.emplace_back(*ss.schema(), permit, partition_end{});

        mfs.emplace_back(*ss.schema(), permit, partition_start(dk1, tombstone{}));
        mfs.emplace_back(*ss.schema(), permit, ss.make_row_v2(permit, ss.make_ckey(0), "v"));
        mfs.emplace_back(*ss.schema(), permit, partition_end{});

        for (const auto& mf : mfs) {
            if (mf.is_partition_start()) {
                expected.emplace_back(*ss.schema(), permit, partition_start(mf.as_partition_start()));
            } else if (mf.is_clustering_row()) {
                expected.emplace_back(*ss.schema(), permit, clustering_row(*ss.schema(), mf.as_clustering_row()));
            } else { // partition-end
                expected.emplace_back(*ss.schema(), permit, partition_end{});
            }
        }

        auto mr = make_compacting_reader(make_mutation_reader_from_fragments(ss.schema(), permit, std::move(mfs)),
                gc_clock::now(),
                can_never_purge,
                tombstone_gc_state(nullptr));
        mr.set_max_buffer_size(buffer_size);

        return mr;
    }();

    auto reader_assertions = assert_that(std::move(mr));

    reader_assertions
        .produces(schema, expected[0]) // partition start
        .produces(schema, expected[1]) // first row
        .next_partition();

    auto it = expected.end() - 3;
    while (it != expected.end()) {
        reader_assertions.produces(schema, *it++);
    }
    reader_assertions.produces_end_of_stream();
}

SEASTAR_THREAD_TEST_CASE(test_compacting_reader_is_consistent_with_compaction) {
    simple_schema ss;
    schema_ptr s = ss.schema();
    tests::reader_concurrency_semaphore_wrapper semaphore;
    auto permit = semaphore.make_permit();

    auto m = ss.new_mutation("pk");

    auto r = ss.make_ckey_range(1, 2);
    auto rt = ss.new_tombstone();
    ss.delete_range(m, r, rt);

    auto p_tomb = ss.new_tombstone();
    m.partition().apply(p_tomb);

    auto read_m = [&] {
        return make_mutation_reader_from_mutations_v2(m.schema(), permit, m);
    };

    assert_that(read_m())
        .produces_partition_start(m.decorated_key(), p_tomb)
        .produces_range_tombstone_change({position_in_partition::for_range_start(r), rt})
        .produces_range_tombstone_change({position_in_partition::for_range_end(r), {}})
        .produces_partition_end();

    assert_that(make_compacting_reader(read_m(), gc_clock::time_point::min(), can_never_purge, tombstone_gc_state(nullptr)))
            .exact()
            .produces_partition_start(m.decorated_key(), p_tomb)
            .produces_partition_end();
}

SEASTAR_THREAD_TEST_CASE(test_auto_paused_evictable_reader_is_mutation_source) {
    auto make_populate = [] (schema_ptr s, const std::vector<mutation>& mutations, gc_clock::time_point query_time) {
        auto mt = make_memtable(s, mutations);
        return mutation_source([=] (
                schema_ptr s,
                reader_permit permit,
                const dht::partition_range& range,
                const query::partition_slice& slice,
                tracing::trace_state_ptr trace_state,
                streamed_mutation::forwarding fwd_sm,
                mutation_reader::forwarding fwd_mr) mutable {
            auto mr = make_auto_paused_evictable_reader_v2(mt->as_data_source(), std::move(s), permit, range, slice, std::move(trace_state), fwd_mr);
            if (fwd_sm == streamed_mutation::forwarding::yes) {
                return make_forwardable(std::move(mr));
            }
            return mr;
        });
    };

    run_mutation_source_tests(make_populate);
}

SEASTAR_THREAD_TEST_CASE(test_manual_paused_evictable_reader_is_mutation_source) {
    class maybe_pausing_reader : public mutation_reader::impl {
        mutation_reader _reader;
        std::optional<evictable_reader_handle_v2> _handle;

    private:
        void maybe_pause() {
            if (!tests::random::get_int(0, 4)) {
                _handle->pause();
            }
        }

    public:
        maybe_pausing_reader(
                replica::memtable& mt,
                schema_ptr query_schema,
                reader_permit permit,
                const dht::partition_range& pr,
                const query::partition_slice& ps,
                tracing::trace_state_ptr trace_state,
                mutation_reader::forwarding fwd_mr)
            : impl(std::move(query_schema), std::move(permit)), _reader(nullptr) {
            std::tie(_reader, _handle) = make_manually_paused_evictable_reader_v2(mt.as_data_source(), _schema, _permit, pr, ps,
                    std::move(trace_state), fwd_mr);
        }
        virtual future<> fill_buffer() override {
            return _reader.fill_buffer().then([this] {
                _end_of_stream = _reader.is_end_of_stream();
                _reader.move_buffer_content_to(*this);
            }).then([this] {
                maybe_pause();
            });
        }
        virtual future<> next_partition() override {
            clear_buffer_to_next_partition();
            if (!is_buffer_empty()) {
                return make_ready_future<>();
            }
            _end_of_stream = false;
            return _reader.next_partition();
        }
        virtual future<> fast_forward_to(const dht::partition_range& pr) override {
            clear_buffer();
            _end_of_stream = false;
            return _reader.fast_forward_to(pr).then([this] {
                maybe_pause();
            });
        }
        virtual future<> fast_forward_to(position_range pr) override {
            throw_with_backtrace<std::bad_function_call>();
        }
        virtual future<> close() noexcept override {
            return _reader.close();
        }
    };

    auto make_populate = [] (schema_ptr s, const std::vector<mutation>& mutations, gc_clock::time_point query_time) {
        auto mt = make_memtable(s, mutations);
        return mutation_source([=] (
                schema_ptr s,
                reader_permit permit,
                const dht::partition_range& range,
                const query::partition_slice& slice,
                tracing::trace_state_ptr trace_state,
                streamed_mutation::forwarding fwd_sm,
                mutation_reader::forwarding fwd_mr) mutable {
            auto mr = make_mutation_reader<maybe_pausing_reader>(*mt, s, std::move(permit), range, slice, std::move(trace_state), fwd_mr);
            if (fwd_sm == streamed_mutation::forwarding::yes) {
                return make_forwardable(std::move(mr));
            }
            return mr;
        });
    };

    run_mutation_source_tests(make_populate);
}

namespace {

std::deque<mutation_fragment_v2> copy_fragments(const schema& s, reader_permit permit, const std::deque<mutation_fragment_v2>& o) {
    std::deque<mutation_fragment_v2> buf;
    for (const auto& mf : o) {
        buf.emplace_back(s, permit, mf);
    }
    return buf;
}

mutation_reader create_evictable_reader_and_evict_after_first_buffer(
        schema_ptr schema,
        reader_permit permit,
        const dht::partition_range& prange,
        const query::partition_slice& slice,
        std::list<std::deque<mutation_fragment_v2>> buffers,
        position_in_partition_view first_buf_last_fragment_position,
        size_t max_buffer_size,
        bool detach_buffer = true) {
    class factory {
        schema_ptr _schema;
        reader_permit _permit;
        std::list<std::deque<mutation_fragment_v2>> _buffers;
        size_t _max_buffer_size;

    public:
        factory(schema_ptr schema, reader_permit permit, std::list<std::deque<mutation_fragment_v2>> buffers, size_t max_buffer_size)
            : _schema(std::move(schema))
            , _permit(std::move(permit))
            , _buffers(std::move(buffers))
            , _max_buffer_size(max_buffer_size) {
        }

        factory(const factory& o)
            : _schema(o._schema)
            , _permit(o._permit) {
            for (const auto& buf : o._buffers) {
                _buffers.emplace_back(copy_fragments(*_schema, _permit, buf));
            }
        }
        factory(factory&& o) = default;

        mutation_reader operator()(
                schema_ptr s,
                reader_permit permit,
                const dht::partition_range& range,
                const query::partition_slice& slice,
                tracing::trace_state_ptr trace_state,
                streamed_mutation::forwarding fwd_sm,
                mutation_reader::forwarding fwd_mr) {
            BOOST_REQUIRE(s == _schema);
            if (!_buffers.empty()) {
                auto buf = std::move(_buffers.front());
                _buffers.pop_front();
                auto rd = make_mutation_reader_from_fragments(_schema, std::move(permit), std::move(buf));
                rd.set_max_buffer_size(_max_buffer_size);
                return rd;
            }
            return make_empty_flat_reader_v2(_schema, std::move(permit));
        }
    };
    auto ms = mutation_source(factory(schema, permit, std::move(buffers), max_buffer_size));

    auto rd = make_auto_paused_evictable_reader_v2(
            std::move(ms),
            schema,
            permit,
            prange,
            slice,
            nullptr,
            mutation_reader::forwarding::yes);

    rd.set_max_buffer_size(max_buffer_size);

    rd.fill_buffer().get();

    const auto eq_cmp = position_in_partition::equal_compare(*schema);
    BOOST_REQUIRE(rd.is_buffer_full());
    BOOST_REQUIRE(eq_cmp(rd.buffer().back().position(), first_buf_last_fragment_position));
    BOOST_REQUIRE(!rd.is_end_of_stream());

    if (detach_buffer) {
        rd.detach_buffer();
    }

    while(permit.semaphore().try_evict_one_inactive_read());

    return rd;
}

mutation_reader create_evictable_reader_and_evict_after_first_buffer(
        schema_ptr schema,
        reader_permit permit,
        const dht::partition_range& prange,
        const query::partition_slice& slice,
        std::deque<mutation_fragment_v2> first_buffer,
        position_in_partition_view last_fragment_position,
        std::deque<mutation_fragment_v2> last_buffer,
        size_t max_buffer_size,
        bool detach_buffer = true) {
    std::list<std::deque<mutation_fragment_v2>> list;
    list.emplace_back(std::move(first_buffer));
    list.emplace_back(std::move(last_buffer));
    return create_evictable_reader_and_evict_after_first_buffer(
            std::move(schema),
            std::move(permit),
            prange,
            slice,
            std::move(list),
            last_fragment_position,
            max_buffer_size,
            detach_buffer);
}

void check_evictable_reader_validation_is_triggered(
        std::string_view test_name,
        std::string_view error_prefix, // empty str if no exception is expected
        schema_ptr schema,
        reader_permit permit,
        const dht::partition_range& prange,
        const query::partition_slice& slice,
        std::deque<mutation_fragment_v2> first_buffer,
        position_in_partition_view last_fragment_position,
        std::deque<mutation_fragment_v2> second_buffer,
        size_t max_buffer_size) {

    testlog.info("check_evictable_reader_validation_is_triggered(): checking {} test case: {}", error_prefix.empty() ? "positive" : "negative", test_name);

    auto rd = create_evictable_reader_and_evict_after_first_buffer(std::move(schema), std::move(permit), prange, slice, std::move(first_buffer),
            last_fragment_position, std::move(second_buffer), max_buffer_size);
    auto close_rd = deferred_close(rd);

    const bool fail = !error_prefix.empty();

    try {
        rd.fill_buffer().get();
    } catch (std::runtime_error& e) {
        if (fail) {
            if (error_prefix == std::string_view(e.what(), error_prefix.size())) {
                testlog.trace("Expected exception caught: {}", std::current_exception());
                return;
            } else {
                BOOST_FAIL(fmt::format("Exception with unexpected message caught: {}", std::current_exception()));
            }
        } else {
            BOOST_FAIL(fmt::format("Unexpected exception caught: {}", std::current_exception()));
        }
    }
    if (fail) {
        BOOST_FAIL(fmt::format("Expected exception not thrown"));
    }
}

}

SEASTAR_THREAD_TEST_CASE(test_evictable_reader_self_validation) {
    set_abort_on_internal_error(false);
    auto reset_on_internal_abort = defer([] {
        set_abort_on_internal_error(true);
    });

    reader_concurrency_semaphore semaphore(reader_concurrency_semaphore::no_limits{}, get_name(), reader_concurrency_semaphore::register_metrics::no);
    auto stop_sem = deferred_stop(semaphore);
    simple_schema s;
    auto permit = semaphore.make_tracking_only_permit(s.schema(), get_name(), db::no_timeout, {});

    auto pkeys = s.make_pkeys(4);
    std::ranges::sort(pkeys, dht::decorated_key::less_comparator(s.schema()));

    size_t max_buffer_size = 512;
    const int first_ck = 100;
    const int second_buffer_ck = first_ck + 100;
    const int last_ck = second_buffer_ck + 100;

    static const char partition_error_prefix[] = "validate_partition_start(): validation failed";
    static const char position_in_partition_error_prefix[] = "validate_position_in_partition(): validation failed";

    const auto prange = dht::partition_range::make(
            dht::partition_range::bound(pkeys[1], true),
            dht::partition_range::bound(pkeys[2], true));

    const auto ckrange = query::clustering_range::make(
            query::clustering_range::bound(s.make_ckey(first_ck), true),
            query::clustering_range::bound(s.make_ckey(last_ck), true));

    const auto slice = partition_slice_builder(*s.schema()).with_range(ckrange).build();

    std::deque<mutation_fragment_v2> first_buffer;
    first_buffer.emplace_back(*s.schema(), permit, partition_start{pkeys[1], {}});
    size_t mem_usage = first_buffer.back().memory_usage();
    for (int i = 0; i < second_buffer_ck; ++i) {
        first_buffer.emplace_back(*s.schema(), permit, s.make_row_v2(permit, s.make_ckey(i++), "v"));
        mem_usage += first_buffer.back().memory_usage();
    }
    max_buffer_size = mem_usage;
    auto last_fragment_position = position_in_partition(first_buffer.back().position());
    first_buffer.emplace_back(*s.schema(), permit, s.make_row_v2(permit, s.make_ckey(second_buffer_ck), "v"));

    auto make_second_buffer = [&s, permit, &max_buffer_size] (dht::decorated_key pkey, std::optional<int> first_ckey = {}) mutable {
        auto ckey = first_ckey ? *first_ckey : second_buffer_ck;
        std::deque<mutation_fragment_v2> second_buffer;
        second_buffer.emplace_back(*s.schema(), permit, partition_start{std::move(pkey), {}});
        size_t mem_usage = second_buffer.back().memory_usage();
        while (mem_usage <= max_buffer_size) {
            second_buffer.emplace_back(*s.schema(), permit, s.make_row_v2(permit, s.make_ckey(ckey++), "v"));
            mem_usage += second_buffer.back().memory_usage();
        }
        second_buffer.emplace_back(*s.schema(), permit, partition_end{});
        return second_buffer;
    };

    //
    // Continuing the same partition
    //

    check_evictable_reader_validation_is_triggered(
            "pkey < _last_pkey; pkey ∉ prange",
            partition_error_prefix,
            s.schema(),
            permit,
            prange,
            slice,
            copy_fragments(*s.schema(), permit, first_buffer),
            last_fragment_position,
            make_second_buffer(pkeys[0]),
            max_buffer_size);

    check_evictable_reader_validation_is_triggered(
            "pkey == _last_pkey",
            "",
            s.schema(),
            permit,
            prange,
            slice,
            copy_fragments(*s.schema(), permit, first_buffer),
            last_fragment_position,
            make_second_buffer(pkeys[1]),
            max_buffer_size);

    check_evictable_reader_validation_is_triggered(
            "pkey == _last_pkey; position_in_partition ∉ ckrange (<)",
            position_in_partition_error_prefix,
            s.schema(),
            permit,
            prange,
            slice,
            copy_fragments(*s.schema(), permit, first_buffer),
            last_fragment_position,
            make_second_buffer(pkeys[1], first_ck - 10),
            max_buffer_size);

    check_evictable_reader_validation_is_triggered(
            "pkey == _last_pkey; position_in_partition ∉ ckrange; position_in_partition < _next_position_in_partition",
            position_in_partition_error_prefix,
            s.schema(),
            permit,
            prange,
            slice,
            copy_fragments(*s.schema(), permit, first_buffer),
            last_fragment_position,
            make_second_buffer(pkeys[1], second_buffer_ck - 2),
            max_buffer_size);

    check_evictable_reader_validation_is_triggered(
            "pkey == _last_pkey; position_in_partition ∈ ckrange",
            "",
            s.schema(),
            permit,
            prange,
            slice,
            copy_fragments(*s.schema(), permit, first_buffer),
            last_fragment_position,
            make_second_buffer(pkeys[1], second_buffer_ck),
            max_buffer_size);

    check_evictable_reader_validation_is_triggered(
            "pkey == _last_pkey; position_in_partition ∉ ckrange (>)",
            position_in_partition_error_prefix,
            s.schema(),
            permit,
            prange,
            slice,
            copy_fragments(*s.schema(), permit, first_buffer),
            last_fragment_position,
            make_second_buffer(pkeys[1], last_ck + 10),
            max_buffer_size);

    check_evictable_reader_validation_is_triggered(
            "pkey > _last_pkey; pkey ∈ pkrange",
            "",
            s.schema(),
            permit,
            prange,
            slice,
            copy_fragments(*s.schema(), permit, first_buffer),
            last_fragment_position,
            make_second_buffer(pkeys[2]),
            max_buffer_size);

    check_evictable_reader_validation_is_triggered(
            "pkey > _last_pkey; pkey ∉ pkrange",
            partition_error_prefix,
            s.schema(),
            permit,
            prange,
            slice,
            copy_fragments(*s.schema(), permit, first_buffer),
            last_fragment_position,
            make_second_buffer(pkeys[3]),
            max_buffer_size);

    //
    // Continuing from next partition
    //

    first_buffer.clear();

    first_buffer.emplace_back(*s.schema(), permit, partition_start{pkeys[1], {}});
    mem_usage = first_buffer.back().memory_usage();
    for (int i = 0; i < second_buffer_ck; ++i) {
        first_buffer.emplace_back(*s.schema(), permit, s.make_row_v2(permit, s.make_ckey(i++), "v"));
        mem_usage += first_buffer.back().memory_usage();
    }
    first_buffer.emplace_back(*s.schema(), permit, partition_end{});
    mem_usage += first_buffer.back().memory_usage();
    last_fragment_position = position_in_partition(first_buffer.back().position());
    max_buffer_size = mem_usage;
    first_buffer.emplace_back(*s.schema(), permit, partition_start{pkeys[2], {}});

    check_evictable_reader_validation_is_triggered(
            "pkey < _last_pkey; pkey ∉ pkrange",
            partition_error_prefix,
            s.schema(),
            permit,
            prange,
            slice,
            copy_fragments(*s.schema(), permit, first_buffer),
            last_fragment_position,
            make_second_buffer(pkeys[0]),
            max_buffer_size);

    check_evictable_reader_validation_is_triggered(
            "pkey == _last_pkey",
            partition_error_prefix,
            s.schema(),
            permit,
            prange,
            slice,
            copy_fragments(*s.schema(), permit, first_buffer),
            last_fragment_position,
            make_second_buffer(pkeys[1]),
            max_buffer_size);

    check_evictable_reader_validation_is_triggered(
            "pkey > _last_pkey; pkey ∈ pkrange",
            "",
            s.schema(),
            permit,
            prange,
            slice,
            copy_fragments(*s.schema(), permit, first_buffer),
            last_fragment_position,
            make_second_buffer(pkeys[2]),
            max_buffer_size);

    check_evictable_reader_validation_is_triggered(
            "pkey > _last_pkey; pkey ∉ pkrange",
            partition_error_prefix,
            s.schema(),
            permit,
            prange,
            slice,
            copy_fragments(*s.schema(), permit, first_buffer),
            last_fragment_position,
            make_second_buffer(pkeys[3]),
            max_buffer_size);
}

SEASTAR_THREAD_TEST_CASE(test_evictable_reader_recreate_before_fast_forward_to) {
    class test_reader : public mutation_reader::impl {
        simple_schema _s;
        const std::vector<dht::decorated_key> _pkeys;
        std::vector<dht::decorated_key>::const_iterator _it;
        std::vector<dht::decorated_key>::const_iterator _end;
    private:
        void on_range_change(const dht::partition_range& pr) {
            dht::ring_position_comparator cmp(*_schema);
            _it = _pkeys.begin();
            while (_it != _pkeys.end() && !pr.contains(*_it, cmp)) {
                ++_it;
            }
            _end = _it;
            while (_end != _pkeys.end() && pr.contains(*_end, cmp)) {
                ++_end;
            }
        }
    public:
        test_reader(simple_schema s, reader_permit permit, const dht::partition_range& pr, std::vector<dht::decorated_key> pkeys)
            : impl(s.schema(), std::move(permit))
            , _s(std::move(s))
            , _pkeys(std::move(pkeys)) {
            on_range_change(pr);
        }

        virtual future<> fill_buffer() override {
            if (_it == _end) {
                _end_of_stream = true;
                return make_ready_future<>();
            }

            push_mutation_fragment(*_schema, _permit, partition_start(*_it++, {}));

            uint32_t ck = 0;
            while (!is_buffer_full()) {
                auto ckey = _s.make_ckey(ck);
                push_mutation_fragment(*_schema, _permit, _s.make_row_v2(_permit, _s.make_ckey(ck++), make_random_string(1024)));
                ++ck;
            }

            push_mutation_fragment(*_schema, _permit, partition_end());
            return make_ready_future<>();
        }
        virtual future<> next_partition() override {
            return make_ready_future<>();
        }
        virtual future<> fast_forward_to(const dht::partition_range& pr) override {
            on_range_change(pr);
            clear_buffer();
            _end_of_stream = false;
            return make_ready_future<>();
        }
        virtual future<> fast_forward_to(position_range) override {
            return make_exception_future<>(make_backtraced_exception_ptr<std::bad_function_call>());
        }
        virtual future<> close() noexcept override {
            return make_ready_future<>();
        };
    };

    reader_concurrency_semaphore semaphore(reader_concurrency_semaphore::no_limits{}, get_name(), reader_concurrency_semaphore::register_metrics::no);
    auto stop_sem = deferred_stop(semaphore);
    simple_schema s;
    auto permit = semaphore.make_tracking_only_permit(s.schema(), get_name(), db::no_timeout, {});
    auto pkeys = s.make_pkeys(6);
    std::ranges::sort(pkeys, dht::decorated_key::less_comparator(s.schema()));

    auto ms = mutation_source([&] (schema_ptr schema,
            reader_permit permit,
            const dht::partition_range& range,
            const query::partition_slice& slice,
            tracing::trace_state_ptr tr,
            streamed_mutation::forwarding fwd,
            mutation_reader::forwarding fwd_mr) {
        std::vector<dht::decorated_key> pkeys_with_data;
        bool empty = false;
        for (const auto& pkey : pkeys) {
            empty = !empty;
            if (empty) {
                pkeys_with_data.push_back(pkey);
            }
        }
        return make_mutation_reader<test_reader>(
                s,
                std::move(permit),
                range,
                std::move(pkeys_with_data));
    });

    auto pr0 = dht::partition_range::make({pkeys[0], true}, {pkeys[3], true});
    auto [reader, handle] = make_manually_paused_evictable_reader_v2(std::move(ms), s.schema(), permit, pr0, s.schema()->full_slice(),
            {}, mutation_reader::forwarding::yes);

    auto reader_assert = assert_that(std::move(reader));
    reader_assert.produces(pkeys[0]);
    reader_assert.produces(pkeys[2]);

    handle.pause();
    BOOST_REQUIRE(semaphore.try_evict_one_inactive_read());

    reader_assert.produces_end_of_stream();

    auto pr1 = dht::partition_range::make({pkeys[4], true}, {pkeys[5], true});
    reader_assert.fast_forward_to(pr1);

    // Failure will happen in the form of `on_internal_error()`.
    reader_assert.produces(pkeys[4]);
}

SEASTAR_THREAD_TEST_CASE(test_evictable_reader_drop_flags) {
    reader_concurrency_semaphore semaphore(reader_concurrency_semaphore::for_tests{}, get_name(), 1, 0);
    auto stop_sem = deferred_stop(semaphore);
    simple_schema s;
    auto permit = semaphore.make_tracking_only_permit(s.schema(), get_name(), db::no_timeout, {});

    auto pkeys = s.make_pkeys(2);
    std::sort(pkeys.begin(), pkeys.end(), [&s] (const auto& pk1, const auto& pk2) {
        return pk1.less_compare(*s.schema(), pk2);
    });
    const auto& pkey1 = pkeys[0];
    const auto& pkey2 = pkeys[1];
    const int second_buffer_ck = 10;

    struct buffer {
        simple_schema& s;
        reader_permit permit;
        std::deque<mutation_fragment_v2> frags;
        std::vector<mutation> muts;
        std::optional<mutation_rebuilder_v2> mut_rebuilder;
        size_t size = 0;
        std::optional<position_in_partition_view> last_pos;

        buffer(simple_schema& s_, reader_permit permit_, dht::decorated_key key)
            : s(s_), permit(std::move(permit_)) {
            add_partition(key);
        }
        size_t add_partition(dht::decorated_key key) {
            size += frags.emplace_back(*s.schema(), permit, partition_start{key, {}}).memory_usage();
            if (mut_rebuilder) {
                muts.emplace_back(*std::move(*mut_rebuilder).consume_end_of_stream());
            }
            mut_rebuilder.emplace(s.schema());
            mut_rebuilder->consume(partition_start{key, {}});
            return size;
        }
        size_t add_mutation_fragment(mutation_fragment_v2&& mf, bool only_to_frags = false) {
            if (!only_to_frags) {
                SCYLLA_ASSERT(mut_rebuilder);
                mut_rebuilder->consume(mutation_fragment_v2(*s.schema(), permit, mf));
            }
            size += frags.emplace_back(*s.schema(), permit, std::move(mf)).memory_usage();
            return size;
        }
        size_t add_static_row(std::optional<mutation_fragment_v2> sr = {}) {
            auto srow = sr ? std::move(*sr) : s.make_static_row_v2(permit, "s");
            return add_mutation_fragment(std::move(srow));
        }
        size_t add_clustering_row(int i, bool only_to_frags = false) {
            return add_mutation_fragment(mutation_fragment_v2(*s.schema(), permit, s.make_row_v2(permit, s.make_ckey(i), "v")), only_to_frags);
        }
        size_t add_clustering_rows(int start, int end) {
            for (int i = start; i < end; ++i) {
                add_clustering_row(i);
            }
            return size;
        }
        size_t add_partition_end() {
            size += frags.emplace_back(*s.schema(), permit, partition_end{}).memory_usage();
            return size;
        }
        void end_of_stream() {
            muts.emplace_back(*std::move(*mut_rebuilder).consume_end_of_stream());
        }
        void save_position() { last_pos = frags.back().position(); }
        void find_position(size_t buf_size) {
            size_t s = 0;
            for (const auto& frag : frags) {
                s += frag.memory_usage();
                if (s >= buf_size) {
                    last_pos = frag.position();
                    break;
                }
            }
            BOOST_REQUIRE(last_pos);
        }
    };

    auto make_reader = [&] (const buffer& first_buffer, const buffer& second_buffer, const buffer* const third_buffer, size_t max_buffer_size) {
        std::list<std::deque<mutation_fragment_v2>> buffers;
        buffers.emplace_back(copy_fragments(*s.schema(), permit, first_buffer.frags));
        buffers.emplace_back(copy_fragments(*s.schema(), permit, second_buffer.frags));
        if (third_buffer) {
            buffers.emplace_back(copy_fragments(*s.schema(), permit, third_buffer->frags));
        }
        return create_evictable_reader_and_evict_after_first_buffer(
                s.schema(),
                permit,
                query::full_partition_range,
                s.schema()->full_slice(),
                std::move(buffers),
                *first_buffer.last_pos,
                max_buffer_size,
                false);
    };

    testlog.info("Same partition, with static row");
    {
        buffer first_buffer(s, permit, pkey1);
        first_buffer.add_static_row();
        auto srow = mutation_fragment_v2(*s.schema(), permit, first_buffer.frags.back());
        const auto buf_size = first_buffer.add_clustering_rows(0, second_buffer_ck);
        first_buffer.save_position();
        first_buffer.add_clustering_row(second_buffer_ck);
        first_buffer.end_of_stream();

        buffer second_buffer(s, permit, pkey1);
        second_buffer.add_static_row(std::move(srow));
        second_buffer.add_clustering_row(second_buffer_ck);
        second_buffer.add_clustering_row(second_buffer_ck + 1);
        second_buffer.add_partition_end();
        second_buffer.end_of_stream();

        assert_that(make_reader(first_buffer, second_buffer, nullptr, buf_size))
            .has_monotonic_positions();

        assert_that(make_reader(first_buffer, second_buffer, nullptr, buf_size))
            .produces(first_buffer.muts[0] + second_buffer.muts[0])
            .produces_end_of_stream();
    }

    testlog.info("Same partition, no static row");
    {
        buffer first_buffer(s, permit, pkey1);
        const auto buf_size = first_buffer.add_clustering_rows(0, second_buffer_ck);
        first_buffer.save_position();
        first_buffer.add_clustering_row(second_buffer_ck);
        first_buffer.end_of_stream();

        buffer second_buffer(s, permit, pkey1);
        second_buffer.add_clustering_row(second_buffer_ck);
        second_buffer.add_clustering_row(second_buffer_ck + 1);
        second_buffer.add_partition_end();
        second_buffer.end_of_stream();

        assert_that(make_reader(first_buffer, second_buffer, nullptr, buf_size))
            .has_monotonic_positions();

        assert_that(make_reader(first_buffer, second_buffer, nullptr, buf_size))
            .produces(first_buffer.muts[0] + second_buffer.muts[0])
            .produces_end_of_stream();
    }

    testlog.info("Same partition as expected, no static row, next partition has static row (#8923)");
    {
        buffer second_buffer(s, permit, pkey1);
        second_buffer.add_clustering_rows(second_buffer_ck, second_buffer_ck + second_buffer_ck / 2);
        // We want to end the buffer on the partition-start below, but since a
        // partition start will be dropped from it, we have to use the size
        // without it.
        const auto buf_size = second_buffer.add_partition_end();
        second_buffer.add_partition(pkey2);
        second_buffer.add_static_row();
        auto srow = mutation_fragment_v2(*s.schema(), permit, second_buffer.frags.back());
        second_buffer.add_clustering_rows(0, 2);
        second_buffer.end_of_stream();

        buffer first_buffer(s, permit, pkey1);
        for (int i = 0; first_buffer.add_clustering_row(i) < buf_size; ++i);
        first_buffer.save_position();
        first_buffer.add_mutation_fragment(mutation_fragment_v2(*s.schema(), permit, second_buffer.frags[1]));
        first_buffer.end_of_stream();

        buffer third_buffer(s, permit, pkey2);
        third_buffer.add_static_row(std::move(srow));
        third_buffer.add_clustering_rows(0, 2);
        third_buffer.add_partition_end();
        third_buffer.end_of_stream();

        first_buffer.find_position(buf_size);

        assert_that(make_reader(first_buffer, second_buffer, &third_buffer, buf_size))
            .has_monotonic_positions();

        assert_that(make_reader(first_buffer, second_buffer, &third_buffer, buf_size))
            .produces(first_buffer.muts[0] + second_buffer.muts[0])
            .produces(second_buffer.muts[1] + third_buffer.muts[0])
            .produces_end_of_stream();
    }

    testlog.info("Next partition, with no static row");
    {
        buffer first_buffer(s, permit, pkey1);
        const auto buf_size = first_buffer.add_clustering_rows(0, second_buffer_ck);
        first_buffer.save_position();
        first_buffer.add_clustering_row(second_buffer_ck + 1, true);
        first_buffer.end_of_stream();

        buffer second_buffer(s, permit, pkey2);
        second_buffer.add_clustering_rows(0, second_buffer_ck / 2);
        second_buffer.add_partition_end();
        second_buffer.end_of_stream();

        assert_that(make_reader(first_buffer, second_buffer, nullptr, buf_size))
            .has_monotonic_positions();

        assert_that(make_reader(first_buffer, second_buffer, nullptr, buf_size))
            .produces(first_buffer.muts[0])
            .produces(second_buffer.muts[0])
            .produces_end_of_stream();
    }

    testlog.info("Next partition, with static row");
    {
        buffer first_buffer(s, permit, pkey1);
        const auto buf_size = first_buffer.add_clustering_rows(0, second_buffer_ck);
        first_buffer.save_position();
        first_buffer.add_clustering_row(second_buffer_ck + 1, true);
        first_buffer.end_of_stream();

        buffer second_buffer(s, permit, pkey2);
        second_buffer.add_static_row();
        second_buffer.add_clustering_rows(0, second_buffer_ck / 2);
        second_buffer.add_partition_end();
        second_buffer.end_of_stream();

        assert_that(make_reader(first_buffer, second_buffer, nullptr, buf_size))
            .has_monotonic_positions();

        assert_that(make_reader(first_buffer, second_buffer, nullptr, buf_size))
            .produces(first_buffer.muts[0])
            .produces(second_buffer.muts[0])
            .produces_end_of_stream();
    }
}

SEASTAR_THREAD_TEST_CASE(test_evictable_reader_non_monotonic_positions) {
    reader_concurrency_semaphore semaphore(reader_concurrency_semaphore::for_tests{}, get_name(), 1, 0);
    auto stop_sem = deferred_stop(semaphore);
    simple_schema s;
    auto schema = s.schema();
    auto permit = semaphore.make_tracking_only_permit(s.schema(), get_name(), db::no_timeout, {});

    auto pkey = s.make_pkey();
    const auto prange = dht::partition_range::make_open_ended_both_sides();

    mutation expected_mut(schema, pkey);
    std::deque<mutation_fragment_v2> frags;
    {
        mutation_rebuilder_v2 mut_builder(schema);
        auto push_mf = [&] (auto mf) {
            using mf_type = decltype(mf);
            frags.emplace_back(*schema, permit, mf_type(mf));
            mut_builder.consume(mutation_fragment_v2(*schema, permit, std::move(mf)));
        };
        push_mf(partition_start(pkey, {}));
        for (int i = 0; i < 10; ++i) {
            const auto ckey = s.make_ckey(i);
            const auto pos = i % 2 ? position_in_partition::after_key(*s.schema(), ckey) : position_in_partition::before_key(ckey);
            for (int j = 0; j < 10; ++j) {
                push_mf(range_tombstone_change(pos, tombstone(s.new_timestamp(), {})));
            }
        }
        push_mf(range_tombstone_change(position_in_partition::after_key(*s.schema(), s.make_ckey(11)), tombstone{}));
        push_mf(partition_end{});

        auto mut_opt = mut_builder.consume_end_of_stream();
        BOOST_REQUIRE(mut_opt);
        expected_mut = std::move(*mut_opt);
    }

    auto ms = mutation_source([&frags] (
            schema_ptr schema,
            reader_permit permit,
            const dht::partition_range& range,
            const query::partition_slice& slice,
            tracing::trace_state_ptr trace_state,
            streamed_mutation::forwarding fwd_sm,
            mutation_reader::forwarding fwd_mr) {
        auto reader = make_mutation_reader_from_fragments(schema, permit, copy_fragments(*schema, permit, frags), range, slice);
        reader.set_max_buffer_size(1);
        return reader;
    });
    auto reader = make_auto_paused_evictable_reader_v2(std::move(ms), schema, permit, prange, schema->full_slice(),
            nullptr, mutation_reader::forwarding::no);
    auto close_reader = deferred_close(reader);

    auto actual_mut = read_mutation_from_mutation_reader(reader).get();
    BOOST_REQUIRE(actual_mut);
    BOOST_REQUIRE(reader.is_end_of_stream());

    BOOST_REQUIRE_EQUAL(*actual_mut, expected_mut);
}

SEASTAR_THREAD_TEST_CASE(test_evictable_reader_clear_tombstone_in_discontinued_partition) {
    reader_concurrency_semaphore semaphore(reader_concurrency_semaphore::for_tests{}, get_name(), 1, 0);
    auto stop_sem = deferred_stop(semaphore);
    simple_schema s;
    auto schema = s.schema();
    auto permit = semaphore.make_tracking_only_permit(s.schema(), get_name(), db::no_timeout, {});

    auto pkeys = s.make_pkeys(2);
    std::sort(pkeys.begin(), pkeys.end(), [&s] (const auto& pk1, const auto& pk2) {
        return pk1.less_compare(*s.schema(), pk2);
    });
    const auto& pkey1 = pkeys[0];
    const auto& pkey2 = pkeys[1];

    const auto first_rtc = range_tombstone_change(position_in_partition::before_key(s.make_ckey(0)), tombstone{s.new_timestamp(), {}});
    const auto last_rtc = range_tombstone_change(position_in_partition::after_key(*s.schema(), s.make_ckey(11)), tombstone{});

    size_t buffer_size = 0;

    std::deque<mutation_fragment_v2> first_buffer;
    {
        auto push_mf = [&] (auto mf) {
            first_buffer.emplace_back(*schema, permit, std::move(mf));
        };
        push_mf(partition_start(pkey1, {}));
        buffer_size += first_buffer.back().memory_usage();
        push_mf(first_rtc);
        buffer_size += first_buffer.back().memory_usage();
        for (int i = 0; i < 5; ++i) {
            push_mf(s.make_row_v2(permit, s.make_ckey(i), "v"));
            buffer_size += first_buffer.back().memory_usage();
        }
        // Add more data after cutting buffer_size
        for (int i = 0; i < 5; ++i) {
            push_mf(s.make_row_v2(permit, s.make_ckey(i), "v"));
        }
        push_mf(last_rtc);
        push_mf(partition_end{});
        buffer_size -= first_buffer.back().memory_usage();
    }
    std::deque<mutation_fragment_v2> second_buffer;
    {
        auto push_mf = [&] (auto mf) {
            second_buffer.emplace_back(*schema, permit, std::move(mf));
        };
        push_mf(partition_start(pkey2, {}));
        push_mf(partition_end{});
    }
    std::deque<mutation_fragment_v2> empty_buffer;

    auto prange = dht::partition_range::make_open_ended_both_sides();

    auto check = [&] (const std::deque<mutation_fragment_v2>& second_buffer, const char* scenario) {
        testlog.info("check() scenario {}", scenario);
        auto ms = mutation_source([&buffer_size, &first_buffer, &second_buffer, first = true] (
                schema_ptr schema,
                reader_permit permit,
                const dht::partition_range& range,
                const query::partition_slice& slice,
                tracing::trace_state_ptr trace_state,
                streamed_mutation::forwarding fwd_sm,
                mutation_reader::forwarding fwd_mr) mutable {
            const auto& buf = first ? first_buffer : second_buffer;
            first = false;
            auto reader = make_mutation_reader_from_fragments(schema, permit, copy_fragments(*schema, permit, buf), range, slice);
            reader.set_max_buffer_size(buffer_size);
            return reader;
        });
        auto reader = make_auto_paused_evictable_reader_v2(std::move(ms), schema, permit, prange, schema->full_slice(),
                nullptr, mutation_reader::forwarding::no);
        auto close_reader = deferred_close(reader);

        reader.fill_buffer().get();
        BOOST_REQUIRE(!reader.is_end_of_stream());
        std::vector<range_tombstone_change> tombs;
        {
            auto mf_opt = reader().get();
            BOOST_REQUIRE(mf_opt->is_partition_start());
        }
        while (auto mf_opt = reader().get()) {
            if (mf_opt->is_range_tombstone_change()) {
                tombs.push_back(std::move(mf_opt->as_range_tombstone_change()));
            }
        }

        BOOST_REQUIRE_EQUAL(tombs.size(), 2);

        BOOST_REQUIRE(tombs.front().equal(*schema, first_rtc));

        auto cmp = position_in_partition::tri_compare(*schema);
        BOOST_REQUIRE(!tombs.back().tombstone());
        BOOST_REQUIRE(cmp(tombs.back().position(), last_rtc.position()) < 0);
    };

    check(second_buffer, "continue from another partition");
    check(empty_buffer, "end of stream");
}

SEASTAR_THREAD_TEST_CASE(test_evictable_reader_next_pos_is_partition_start) {
    reader_concurrency_semaphore semaphore(reader_concurrency_semaphore::for_tests{}, get_name(), 1, 0);
    auto stop_sem = deferred_stop(semaphore);
    simple_schema s;
    auto schema = s.schema();
    auto permit = semaphore.make_tracking_only_permit(s.schema(), get_name(), db::no_timeout, {});

    auto pk = s.make_pkey();
    const auto prange = dht::partition_range::make_open_ended_both_sides();

    std::deque<mutation_fragment_v2> frags;
    frags.emplace_back(*schema, permit, partition_start(pk, {}));
    for (size_t ck = 0; ck < 1000; ++ck) {
        frags.emplace_back(*schema, permit, range_tombstone_change(position_in_partition::before_key(s.make_ckey(ck)), tombstone(s.new_timestamp(), {})));
    }
    frags.emplace_back(*schema, permit, range_tombstone_change(position_in_partition::before_key(s.make_ckey(1001)), tombstone()));
    frags.emplace_back(*schema, permit, partition_end{});

    const auto max_buf_size = frags[0].memory_usage() + frags[1].memory_usage() + frags[2].memory_usage();

    auto ms = mutation_source([&frags, max_buf_size] (
            schema_ptr schema,
            reader_permit permit,
            const dht::partition_range& pr,
            const query::partition_slice& ps) {
        auto rd = make_mutation_reader_from_fragments(std::move(schema), std::move(permit), std::move(frags), pr, ps);
        rd.set_max_buffer_size(max_buf_size);
        return rd;
    });

    auto [rd, handle] = make_manually_paused_evictable_reader_v2(ms, schema, permit, prange, schema->full_slice(), {},
            mutation_reader::forwarding::no);
    auto stop_rd = deferred_close(rd);
    rd.set_max_buffer_size(max_buf_size);

    // #13491 - the reader must not consume the entire partition but a small batch of fragments based on the buffer size.
    rd.fill_buffer().get();
    rd.fill_buffer().get();
    auto buf1 = rd.detach_buffer();
    // There should be 6-7 fragments, but to avoid computing the exact number of fragments that should fit in `max_buf_size`,
    // just ensure that there are <= 10 (consuming the whole partition would give ~1000 fragments).
    BOOST_REQUIRE_LE(buf1.size(), 10);
}

struct mutation_bounds {
    std::optional<mutation> m;
    position_in_partition lower;
    position_in_partition upper;
};

static reader_bounds make_reader_bounds(
        schema_ptr s, reader_permit permit, mutation_bounds mb, streamed_mutation::forwarding fwd,
        const query::partition_slice* slice = nullptr) {
    if (!slice) {
        slice = &s->full_slice();
    }

    return reader_bounds {
        .r = mb.m ? make_mutation_reader_from_mutations_v2(s, permit, std::move(*mb.m), *slice, fwd)
                  : make_empty_flat_reader_v2(s, permit),
        .lower = std::move(mb.lower),
        .upper = std::move(mb.upper)
    };
}

struct clustering_order_merger_test_generator {
    struct scenario {
        std::vector<mutation_bounds> readers_data;
        std::vector<position_range> fwd_ranges;
    };

    schema_ptr _s;
    dht::decorated_key _pk;

    clustering_order_merger_test_generator(std::optional<dht::decorated_key> pk = std::nullopt)
        : _s(make_schema()), _pk(pk ? *pk : tests::generate_partition_key(_s))
    {}

    static schema_ptr make_schema() {
        return schema_builder("ks", "t")
            .with_column("pk", utf8_type, column_kind::partition_key)
            .with_column("ck", int32_type, column_kind::clustering_key)
            .with_column("v", int32_type, column_kind::regular_column)
            .build();
    }

    clustering_key mk_ck(int k) const {
        return clustering_key::from_single_value(*_s, int32_type->decompose(k));
    }

    position_in_partition mk_pos_for(int k) const {
        return position_in_partition::for_key(mk_ck(k));
    }

    mutation mk_mutation(const std::map<int, int>& kvs) const {
        mutation m(_s, _pk);
        auto& cdef = *_s->get_column_definition(to_bytes("v"));
        for (auto [k, v]: kvs) {
            m.set_clustered_cell(mk_ck(k), cdef,
                    atomic_cell::make_live(*cdef.type, 42, cdef.type->decompose(v)));
        }
        return m;
    }

    scenario generate_scenario(std::mt19937& engine) const {
        std::set<int> all_ks;
        std::vector<mutation_bounds> readers_data;

        auto num_readers = tests::random::get_int(1, 10, engine);
        auto num_empty_readers = tests::random::get_int(1, num_readers, engine);
        while (num_empty_readers--) {
            auto lower = -tests::random::get_int(0, 5, engine);
            auto upper = tests::random::get_int(0, 5, engine);
            readers_data.push_back(mutation_bounds{std::nullopt, mk_pos_for(lower), mk_pos_for(upper)});
            num_readers--;
        }
        while (num_readers--) {
            auto len = tests::random::get_int(0, 15, engine);
            auto ks = tests::random::random_subset<int>(100, len, engine);
            std::map<int, int> kvs;
            for (auto k: ks) {
                all_ks.insert(k);
                kvs.emplace(k, tests::random::get_int(0, 100, engine));
            }

            auto lower = (kvs.empty() ? 0 : kvs.begin()->first) - tests::random::get_int(0, 5, engine);
            auto upper = (kvs.empty() ? 0 : std::prev(kvs.end())->first) + tests::random::get_int(0, 5, engine);

            readers_data.push_back(mutation_bounds{mk_mutation(kvs), mk_pos_for(lower), mk_pos_for(upper)});
        }

        std::sort(readers_data.begin(), readers_data.end(), [less = position_in_partition::less_compare(*_s)]
                (const mutation_bounds& a, const mutation_bounds& b) { return less(a.lower, b.lower); });

        std::vector<position_in_partition> positions { position_in_partition::before_all_clustered_rows() };
        for (auto k: all_ks) {
            auto ck = mk_ck(k);
            positions.push_back(position_in_partition::before_key(ck));
            positions.push_back(position_in_partition::for_key(ck));
            positions.push_back(position_in_partition::after_key(*_s, ck));
        }
        positions.push_back(position_in_partition::after_all_clustered_rows());

        size_t num_ranges = tests::random::get_int(1ul, std::max(all_ks.size() / 3, 1ul));
        positions = tests::random::random_subset(std::move(positions), num_ranges * 2, engine);
        std::sort(positions.begin(), positions.end(), position_in_partition::less_compare(*_s));

        std::vector<position_range> fwd_ranges;
        for (size_t i = 0; i < num_ranges; ++i) {
            SCYLLA_ASSERT(2*i+1 < positions.size());
            fwd_ranges.push_back(position_range(std::move(positions[2*i]), std::move(positions[2*i+1])));
        }

        return scenario {
            std::move(readers_data),
            std::move(fwd_ranges)
        };
    }
};

SEASTAR_THREAD_TEST_CASE(test_clustering_order_merger_in_memory) {
    tests::reader_concurrency_semaphore_wrapper semaphore;
    clustering_order_merger_test_generator g;

    auto make_authority = [s = g._s, &semaphore] (std::optional<mutation> mut, streamed_mutation::forwarding fwd) {
        auto permit = semaphore.make_permit();
        if (mut) {
            return make_mutation_reader_from_mutations_v2(s, permit, std::move(*mut), fwd);
        }
        return make_empty_flat_reader_v2(s, permit);
    };

    auto make_tested = [s = g._s, &semaphore] (std::vector<mutation_bounds> ms, streamed_mutation::forwarding fwd) {
        auto permit = semaphore.make_permit();
        auto rs = std::move(ms)
                | std::views::transform([s, fwd, &permit] (auto&& mb) {
                    return make_reader_bounds(s, permit, std::move(mb), fwd);
                })
                | std::ranges::to<std::vector<reader_bounds>>();
        auto q = std::make_unique<simple_position_reader_queue>(*s, std::move(rs));
        return make_clustering_combined_reader(s, permit, fwd, std::move(q));
    };

    auto seed = tests::random::get_int<uint32_t>();
    std::cout << "test_clustering_order_merger_in_memory seed: " << seed << std::endl;
    auto engine = std::mt19937(seed);

    for (int run = 0; run < 1000; ++run) {
        auto scenario = g.generate_scenario(engine);
        auto merged = std::accumulate(scenario.readers_data.begin(), scenario.readers_data.end(),
                std::optional<mutation>{}, [&g] (std::optional<mutation> curr, const mutation_bounds& mb) {
                    if (mb.m) {
                        if (!curr) {
                            curr = mutation(g._s, g._pk);
                        }
                        *curr += *mb.m;
                    }
                    return curr;
                });

        {
            auto fwd = streamed_mutation::forwarding::no;
            compare_readers(*g._s, make_authority(merged, fwd), make_tested(scenario.readers_data, fwd));
        }

        auto fwd = streamed_mutation::forwarding::yes;
        compare_readers(*g._s, make_authority(std::move(merged), fwd),
                make_tested(std::move(scenario.readers_data), fwd), scenario.fwd_ranges);
    }

    // Test case with 0 readers
    for (auto fwd: {streamed_mutation::forwarding::no, streamed_mutation::forwarding::yes}) {
        auto r = make_clustering_combined_reader(g._s, semaphore.make_permit(), fwd,
                std::make_unique<simple_position_reader_queue>(*g._s, std::vector<reader_bounds>{}));
        assert_that(std::move(r)).produces_end_of_stream();
    }
}


static future<> do_test_clustering_order_merger_sstable_set(bool reversed) {
  return sstables::test_env::do_with_async([reversed] (sstables::test_env& env) {
    auto pkeys = tests::generate_partition_keys(2, clustering_order_merger_test_generator::make_schema());
    clustering_order_merger_test_generator g(pkeys[0]);
    auto query_schema = g._s;
    auto table_schema = g._s;

    auto query_slice = query_schema->full_slice();
    if (reversed) {
        table_schema = table_schema->make_reversed();
        query_slice.options.set(query::partition_slice::option::reversed);
        query_slice = query::native_reverse_slice_to_legacy_reverse_slice(*table_schema, std::move(query_slice));
    }

    auto make_authority = [&env, &query_schema, &query_slice] (mutation mut, streamed_mutation::forwarding fwd) {
        return make_mutation_reader_from_mutations_v2(query_schema, env.make_reader_permit(), std::move(mut), query_slice, fwd);
    };

    auto pr = dht::partition_range::make_singular(dht::ring_position(g._pk));
    auto make_tested = [&env, query_schema, pk = g._pk, &pr, &query_slice, reversed]
            (const time_series_sstable_set& sst_set,
                const std::unordered_set<sstables::generation_type>& included_gens, streamed_mutation::forwarding fwd) {
        auto permit = env.make_reader_permit();
        auto q = sst_set.make_position_reader_queue(
            [query_schema, &pr, &query_slice, fwd, permit] (sstable& sst) {
                return sst.make_reader(query_schema, permit, pr,
                                          query_slice, nullptr, fwd);
            },
            [included_gens] (const sstable& sst) { return included_gens.contains(sst.generation()); },
            pk.key(), query_schema, permit, fwd, reversed);
        return make_clustering_combined_reader(query_schema, permit, fwd, std::move(q));
    };

    auto seed = tests::random::get_int<uint32_t>();
    std::cout << "test_clustering_order_merger_sstable_set seed: " << seed << std::endl;
    auto engine = std::mt19937(seed);
    std::bernoulli_distribution dist(0.9);

    for (int run = 0; run < 100; ++run) {
        auto scenario = g.generate_scenario(engine);

        if (reversed) {
            for (auto& mb: scenario.readers_data) {
                if (mb.m) {
                    mb.m = reverse(std::move(*mb.m));
                }
            }
        }

        time_series_sstable_set sst_set(table_schema, true);
        mutation merged(table_schema, g._pk);
        std::unordered_set<sstables::generation_type> included_gens;
        auto sst_factory = env.make_sst_factory(table_schema);
        for (auto& mb: scenario.readers_data) {
            sstables::shared_sstable sst;
            if (mb.m) {
                sst = make_sstable_containing(sst_factory, {*mb.m});
                sst_set.insert(sst);
            } else {
                // We want to have an sstable that won't return any fragments when we query it
                // for our partition (not even `partition_start`). For that we create an sstable
                // with a different partition.
                auto pk = pkeys[1];
                SCYLLA_ASSERT(!pk.equal(*g._s, g._pk));

                sst = make_sstable_containing(sst_factory, {mutation(table_schema, pk)});
                sst_set.insert(sst);
            }

            if (dist(engine)) {
                included_gens.insert(sst->generation());
                if (mb.m) {
                    merged += *mb.m;
                }
            }
        }

        {
            auto fwd = streamed_mutation::forwarding::no;
            compare_readers(*query_schema, make_authority(merged, fwd), make_tested(sst_set, included_gens, fwd));
        }

        auto fwd = streamed_mutation::forwarding::yes;
        compare_readers(*query_schema, make_authority(std::move(merged), fwd),
                make_tested(sst_set, included_gens, fwd), scenario.fwd_ranges);
    }

  });
}

SEASTAR_TEST_CASE(test_clustering_order_merger_sstable_set) {
    return do_test_clustering_order_merger_sstable_set(false);
}

SEASTAR_TEST_CASE(test_clustering_order_merger_sstable_set_reversed) {
    return do_test_clustering_order_merger_sstable_set(true);
}

SEASTAR_THREAD_TEST_CASE(clustering_combined_reader_mutation_source_test) {
    // The clustering combined reader (based on `clustering_order_reader_merger`) supports only
    // single partition readers, but the mutation source test framework tests the provided source
    // on multiple partitions. Furthermore, our reader doesn't support static rows or partition tombstones.
    // We deal with this as follows:
    // 1. we process all mutations provided by the framework, extracting unsupported fragments
    //    (partition tombstones and static rows) into separate mutations, called ``bad mutations'' below.
    //    Each bad mutation is used to create a separate reader.
    // 2. The remaining mutations (``good mutations'') are sorted w.r.t their partition keys;
    //    for each partition key, we use the set of mutations with this key to create our clustering combined reader.
    //    The readers are then fed into a meta-reader `multi_partition_reader` which supports multi-partition queries,
    //    calling the provided readers appropriately as the query goes through the partition range.
    // 3. The ``bad mutation'' readers from step 1 and the reader from step 2 are combined together
    //    using the generic combined reader.

    // Multi partition reader from single partition readers.
    // precondition: readers must be nonempty (they return a partition_start)
    struct multi_partition_reader : public mutation_reader::impl {
        using container_t = std::map<dht::decorated_key, mutation_reader, dht::decorated_key::less_comparator>;

        std::reference_wrapper<const dht::partition_range> _range;

        container_t _readers;
        container_t::iterator _it;

        // invariants:
        // 1. _it = end() or _it->first is not before the current partition range (_range)
        // 2. _it->second did not return partition_end

        // did we fetch a fragment from _it? (due to invariant 2, it wasn't partition end)
        bool _inside_partition = false;

        multi_partition_reader(schema_ptr s, reader_permit permit,
                container_t readers, const dht::partition_range& range)
            : impl(std::move(s), std::move(permit))
            , _range(range), _readers(std::move(readers))
            , _it(std::partition_point(_readers.begin(), _readers.end(), [this, cmp = dht::ring_position_comparator(*_schema)]
                    (auto& r) { return _range.get().before(r.first, cmp); }))
        {
            SCYLLA_ASSERT(!_readers.empty());
        }

        virtual future<> fill_buffer() override {
            while (!is_buffer_full()) {
                auto mfo = co_await next_fragment();
                if (!mfo) {
                    _end_of_stream = true;
                    break;
                }
                push_mutation_fragment(std::move(*mfo));
            }
        }

        virtual future<> next_partition() override {
            clear_buffer_to_next_partition();
            _end_of_stream = false;
            if (is_buffer_empty()) {
                // all fragments that were in the buffer came from the same partition
                if (_inside_partition) {
                    // last fetched fragment came from _it, so all fragments previously in the buffer came from _it
                    // => current partition is _it, we need to move forward
                    //    _it might be the end of current forwarding range, but that's no problem;
                    //    in that case we'll go into eos mode until forwarded
                    SCYLLA_ASSERT(_it != _readers.end());
                    _inside_partition = false;
                    ++_it;
                } else {
                    // either no previously fetched fragment or must have come from before _it. Nothing to do
                }
            }
            return make_ready_future<>();
        }

        virtual future<> fast_forward_to(const dht::partition_range& pr) override {
            // all fragments currently in the buffer come from the current partition range
            // and pr must be strictly greater, so just clear the buffer
            clear_buffer();
            _end_of_stream = false;
            _inside_partition = false;
            _range = pr;
            // restore invariant 2
            _it = std::partition_point(_it, _readers.end(), [this, cmp = dht::ring_position_comparator(*_schema)]
                    (auto& r) { return _range.get().before(r.first, cmp); });
            co_return;
        }

        virtual future<> fast_forward_to(position_range pr) override {
            if (!_inside_partition) {
                // this should not happen - the specification of fast_forward_to says that it can only be called
                // while inside partition. But if it happens for whatever reason just do nothing
                return make_ready_future<>();
            }
            SCYLLA_ASSERT(_it != _readers.end());
            // all fragments currently in the buffer come from the current position range
            // and pr must be strictly greater, so just clear the buffer
            clear_buffer();
            _end_of_stream = false;
            return _it->second.fast_forward_to(std::move(pr));
        }

        virtual future<> close() noexcept override {
            return parallel_for_each(_readers, [] (auto& p) {
                return p.second.close();
            });
        }

        future<mutation_fragment_v2_opt> next_fragment() {
            if (_it == _readers.end() || _range.get().after(_it->first, dht::ring_position_comparator(*_schema))) {
                co_return mutation_fragment_v2_opt{};
            }

            auto mfo = co_await _it->second();
            if (mfo) {
                if (mfo->is_end_of_partition()) {
                    ++_it;
                    _inside_partition = false;
                } else {
                    _inside_partition = true;
                }
                co_return mfo;
            }

            // our readers must be sm::forwarding (invariant 2) and the range was just exhausted,
            // waiting for ff or next_partition
            co_return mutation_fragment_v2_opt{};
        }
    };

    tests::reader_concurrency_semaphore_wrapper semaphore;

    auto populate = [&semaphore] (schema_ptr s, const std::vector<mutation>& muts) {
        std::map<dht::decorated_key, std::vector<mutation_bounds>, dht::decorated_key::less_comparator>
            good_mutations{dht::decorated_key::less_comparator(s)};
        std::vector<mutation> bad_mutations;
        for (auto& m: muts) {
            auto& dk = m.decorated_key();

            std::optional<std::pair<position_in_partition, position_in_partition>> bounds;
            position_in_partition::less_compare less{*s};
            mutation_rebuilder_v2 good(m.schema());
            good.consume_new_partition(dk);
            std::optional<mutation_rebuilder_v2> bad;
            auto rd = make_mutation_reader_from_mutations_v2(s, semaphore.make_permit(), m);
            auto close_rd = deferred_close(rd);
            rd.consume_pausable([&] (mutation_fragment_v2&& mf) {
                if (mf.is_partition_start()) {
                    if (mf.as_partition_start().partition_tombstone()) {
                        bad.emplace(m.schema());
                        bad->consume(std::move(mf));
                    }
                } else if (mf.is_static_row()) {
                    if (!bad) {
                        bad.emplace(m.schema());
                        bad->consume_new_partition(dk);
                    }
                    bad->consume(std::move(mf));
                } else if (!mf.is_end_of_partition()) {
                    if (!bounds) {
                        bounds = std::pair{mf.position(), mf.position()};
                    } else {
                        bounds->second = mf.position();
                    }
                    good.consume(std::move(mf));
                }
                return stop_iteration::no;
            }).get();

            mutation_bounds mb {
                std::move(*good.consume_end_of_stream()),
                bounds ? std::move(bounds->first) : position_in_partition::before_all_clustered_rows(),
                bounds ? std::move(bounds->second) : position_in_partition::after_all_clustered_rows()
            };

            auto it = good_mutations.find(dk);
            if (it == good_mutations.end()) {
                good_mutations.emplace(dk, std::vector<mutation_bounds>{std::move(mb)});
            } else {
                it->second.push_back(std::move(mb));
            }

            if (bad) {
                bad_mutations.push_back(std::move(*bad->consume_end_of_stream()));
            }
        }

        return mutation_source([good = std::move(good_mutations), bad = std::move(bad_mutations)] (
                schema_ptr s,
                reader_permit permit,
                const dht::partition_range& range,
                const query::partition_slice& slice,
                tracing::trace_state_ptr trace_state,
                streamed_mutation::forwarding fwd_sm,
                mutation_reader::forwarding fwd_mr) {
            auto reversed = slice.is_reversed();
            std::map<dht::decorated_key, mutation_reader, dht::decorated_key::less_comparator>
                good_readers{dht::decorated_key::less_comparator(s)};
            for (auto& [k, ms]: good) {
                auto rs = ms
                        | std::views::transform([&] (auto&& mb) {
                            auto rb = make_reader_bounds(s, permit, std::move(mb), fwd_sm, &slice);
                            if (reversed) {
                                // The bounds are calculated in 'table order' (using the mutation and its schema),
                                // but we need them in 'query order' (using the query schema), so for reversed queries
                                // we need to swap and reverse them.
                                std::swap(rb.lower, rb.upper);
                                rb.lower = std::move(rb.lower).reversed();
                                rb.upper = std::move(rb.upper).reversed();
                            }
                            return rb;
                        })
                        | std::ranges::to<std::vector<reader_bounds>>();
                std::sort(rs.begin(), rs.end(), [less = position_in_partition::less_compare(*s)]
                        (const reader_bounds& a, const reader_bounds& b) { return less(a.lower, b.lower); });
                auto q = std::make_unique<simple_position_reader_queue>(*s, std::move(rs));
                good_readers.emplace(k, make_clustering_combined_reader(s, permit, fwd_sm, std::move(q)));
            }

            std::vector<mutation_reader> readers;
            for (auto& m: bad) {
                readers.push_back(make_mutation_reader_from_mutations_v2(s, permit, {m}, range, slice, fwd_sm));
            }
            readers.push_back(make_mutation_reader<multi_partition_reader>(s, permit, std::move(good_readers), range));

            return make_combined_reader(std::move(s), std::move(permit), std::move(readers), fwd_sm, fwd_mr);
        });
    };

    run_mutation_source_tests(std::move(populate));
}

// Regression test for #8445.
SEASTAR_THREAD_TEST_CASE(test_clustering_combining_of_empty_readers) {
    auto s = clustering_order_merger_test_generator::make_schema();
    tests::reader_concurrency_semaphore_wrapper semaphore;

    auto permit = semaphore.make_permit();
    std::vector<reader_bounds> rs;
    rs.push_back({
        .r = make_empty_flat_reader_v2(s, permit),
        .lower = position_in_partition::before_all_clustered_rows(),
        .upper = position_in_partition::after_all_clustered_rows()
    });
    auto r = make_clustering_combined_reader(
            s, permit, streamed_mutation::forwarding::no,
            std::make_unique<simple_position_reader_queue>(*s, std::move(rs)));
    auto close_r = deferred_close(r);

    auto mf = r().get();
    if (mf) {
        BOOST_FAIL(format("reader combined of empty readers returned fragment {}", mutation_fragment_v2::printer(*s, *mf)));
    }
}

template <typename Reader>
class closing_holder {
    Reader _rd;
public:
    closing_holder(Reader&& rd) : _rd(std::move(rd)) { }
    closing_holder(closing_holder&&) = default;
    ~closing_holder() { _rd.close().get(); }
    auto operator()() { return _rd(); }
};

SEASTAR_THREAD_TEST_CASE(test_generating_reader_v1) {
    auto populator_v1 = [] (schema_ptr s, const std::vector<mutation>& muts) {
        return mutation_source([muts] (
                schema_ptr schema,
                reader_permit permit,
                const dht::partition_range& range,
                const query::partition_slice& slice,
                tracing::trace_state_ptr trace_state,
                streamed_mutation::forwarding fwd_sm,
                mutation_reader::forwarding fwd_mr) {
            auto underlying = mutation_fragment_v1_stream
                              (make_mutation_reader_from_mutations_v2(schema, permit, squash_mutations(muts), range, slice));
            auto rd = make_next_partition_adaptor(make_generating_reader_v1(schema, permit, closing_holder(std::move(underlying))));
            if (fwd_sm == streamed_mutation::forwarding::yes) {
                return make_forwardable(std::move(rd));
            }
            return rd;
        });
    };
    run_mutation_source_tests(populator_v1, false);
}

SEASTAR_THREAD_TEST_CASE(test_generating_reader_v2) {
    auto populator_v2 = [] (schema_ptr s, const std::vector<mutation>& muts) {
        return mutation_source([muts] (
                schema_ptr schema,
                reader_permit permit,
                const dht::partition_range& range,
                const query::partition_slice& slice,
                tracing::trace_state_ptr trace_state,
                streamed_mutation::forwarding fwd_sm,
                mutation_reader::forwarding fwd_mr) {
            auto underlying = make_mutation_reader_from_mutations_v2(schema, permit, squash_mutations(muts), range, slice);
            auto rd = make_next_partition_adaptor(make_generating_reader_v2(schema, permit, closing_holder(std::move(underlying))));
            if (fwd_sm == streamed_mutation::forwarding::yes) {
                return make_forwardable(std::move(rd));
            }
            return rd;
        });
    };
    run_mutation_source_tests(populator_v2, false);
}

// Check that the multishard reader is safe to create with an admitted permit,
// i.e. a permit which already has a count resource (and memory resources).
// Create semaphroes with a single count resource, admit a permit and create a
// multishard reader with said permit and ensure this doesn't end up in a
// deadlock (timeout) when the multishard reader creates the shard reader on the
// same shard.
SEASTAR_TEST_CASE(test_multishard_reader_safe_to_create_with_admitted_permit) {
    class semaphore_factory : public test_reader_lifecycle_policy::semaphore_factory {
        std::vector<foreign_ptr<lw_shared_ptr<reader_concurrency_semaphore>>>& _semaphores;
    public:
        explicit semaphore_factory(std::vector<foreign_ptr<lw_shared_ptr<reader_concurrency_semaphore>>>& semaphores) : _semaphores(semaphores) { }
        virtual lw_shared_ptr<reader_concurrency_semaphore> create(sstring name) override {
            auto semaphore = _semaphores.at(this_shard_id()).release();
            _semaphores[this_shard_id()] = make_foreign(semaphore);
            return semaphore;
        }
        virtual future<> stop(reader_concurrency_semaphore& semaphore) override {
            return make_ready_future<>(); // NOOP, we stop the semaphore in the layer above
        }
    };

    return do_with_cql_env_thread([] (cql_test_env& env) {
        simple_schema s;

        std::vector<foreign_ptr<lw_shared_ptr<reader_concurrency_semaphore>>> semaphores;
        semaphores.resize(smp::count);
        parallel_for_each(std::views::iota(0u, smp::count), [&semaphores] (shard_id shard) {
            return smp::submit_to(shard, [&semaphores] {
                semaphores[this_shard_id()] = make_foreign(make_lw_shared<reader_concurrency_semaphore>(
                        reader_concurrency_semaphore::for_tests{},
                        seastar::format("{}:{}", get_name(), this_shard_id()),
                        1,
                        1 * 1024 * 1024));
            });
        }).get();
        auto stop_semaphores = defer([&semaphores] {
            parallel_for_each(std::views::iota(0u, smp::count), [&semaphores] (shard_id shard) {
                return smp::submit_to(shard, [&semaphores] () -> future<> {
                    auto semaphore = semaphores[this_shard_id()].release();
                    co_await semaphore->stop();
                });
            }).get();
        });

        std::map<dht::token, unsigned> pkeys_by_tokens;
        for (unsigned i = 0; i < smp::count * 2; ++i) {
            pkeys_by_tokens.emplace(s.make_pkey(i).token(), i);
        }
        auto sharder = std::make_unique<dummy_sharder>(s.schema()->get_sharder(), std::move(pkeys_by_tokens));

        auto reader_factory = [] (
                schema_ptr schema,
                reader_permit permit,
                const dht::partition_range&,
                const query::partition_slice&,
                tracing::trace_state_ptr,
                mutation_reader::forwarding) {
            return make_empty_flat_reader_v2(std::move(schema), std::move(permit));
        };

        // timeout is used to break the deadlock in case this test fails
        auto permit = semaphores.at(this_shard_id())->obtain_permit(s.schema(), "multishard_reader", 128 * 1024, db::timeout_clock::now() + 60s, {}).get();
        auto lifecycle_policy = seastar::make_shared<test_reader_lifecycle_policy>(reader_factory, std::make_unique<semaphore_factory>(semaphores));

        auto reader = make_multishard_combining_reader_v2_for_tests(*sharder, std::move(lifecycle_policy), s.schema(), std::move(permit),
                query::full_partition_range, s.schema()->full_slice());
        auto close_reader = deferred_close(reader);

        reader().get();
    });
}

class evicting_semaphore_factory : public test_reader_lifecycle_policy::semaphore_factory {
    std::vector<lw_shared_ptr<reader_concurrency_semaphore>>& _registry;
public:
    explicit evicting_semaphore_factory(std::vector<lw_shared_ptr<reader_concurrency_semaphore>>& registry) : _registry(registry) { }
    virtual lw_shared_ptr<reader_concurrency_semaphore> create(sstring name) override {
        // Create with no memory, so all inactive reads are immediately evicted.
        auto semaphore = make_lw_shared<reader_concurrency_semaphore>(reader_concurrency_semaphore::for_tests{}, std::move(name), 1, 0);
        _registry.at(this_shard_id()) = semaphore;
        return semaphore;
    }
    virtual future<> stop(reader_concurrency_semaphore& semaphore) override {
        _registry.at(this_shard_id()) = {};
        return semaphore.stop();
    }
};

SEASTAR_TEST_CASE(test_multishard_reader_buffer_hint_large_partitions) {
    return do_with_cql_env_thread([] (cql_test_env& env) {
        simple_schema ss;
        const auto& schema = ss.schema();
        const sstring value(1024, 'v');
        const size_t num_rows = 200;
        tests::reader_concurrency_semaphore_wrapper semaphore;

        const auto pkeys_by_tokens = ss.make_pkeys(2 * smp::count)
            | std::views::transform([] (const dht::decorated_key& dk) { return std::pair(dk.token(), dk); })
            | std::ranges::to<std::map<dht::token, dht::decorated_key>>();
        std::vector<std::vector<frozen_mutation>> frozen_muts(smp::count, std::vector<frozen_mutation>{});
        std::vector<lw_shared_ptr<reader_concurrency_semaphore>> semaphore_registry(smp::count, nullptr);

        unsigned i = 0;
        for (const auto& [token, dk] : pkeys_by_tokens) {
            mutation mut(schema, dk);

            for (uint32_t ck = 0; ck < num_rows; ++ck) {
                ss.add_row(mut, ss.make_ckey(ck), value);
            }

            frozen_muts[i++ % smp::count].emplace_back(mut);
        }

        size_t partition_size{0};
        size_t partition_start_size{0};
        size_t row_size{0};
        size_t partition_end_size{0};
        size_t range_tombstone_size{0};
        {
            auto reader = make_mutation_reader_from_mutations_v2(schema, semaphore.make_permit(), frozen_muts.front().front().unfreeze(schema), schema->full_slice());
            auto close_reader = deferred_close(reader);
            reader.set_max_buffer_size(1024*1024);
            reader.fill_buffer().get();
            partition_size = reader.buffer_size();

            const auto buf = reader.detach_buffer();
            BOOST_REQUIRE_GT(buf.size(), 2);
            BOOST_REQUIRE(buf[0].is_partition_start());
            BOOST_REQUIRE(buf[1].is_clustering_row());
            BOOST_REQUIRE(buf.back().is_end_of_partition());
            BOOST_REQUIRE(reader.is_end_of_stream());

            partition_start_size = buf[0].memory_usage();
            row_size = buf[1].memory_usage();
            partition_end_size = buf.back().memory_usage();

            auto rtc = mutation_fragment_v2(*schema, semaphore.make_permit(), range_tombstone_change(ss.make_ckey(0), tombstone()));
            range_tombstone_size = rtc.memory_usage();
        }

        std::vector<std::vector<size_t>> data_per_shard(smp::count, std::vector<size_t>{});
        size_t total_data{0};
        for (unsigned shard_id = 0; shard_id != smp::count; ++shard_id) {
            for (const auto& _ : frozen_muts.at(shard_id)) {
                data_per_shard.at(shard_id).push_back(partition_size);
                total_data += partition_size;
            }
        }

        auto sharder = std::make_unique<dummy_sharder>(schema->get_sharder(), std::move(pkeys_by_tokens));

        const auto reader_factory = [frozen_muts] (
                schema_ptr schema,
                reader_permit permit,
                const dht::partition_range&,
                const query::partition_slice& ps,
                tracing::trace_state_ptr,
                mutation_reader::forwarding) {
            auto muts = frozen_muts.at(this_shard_id())
                    | std::views::transform([schema] (const frozen_mutation& fm) { return fm.unfreeze(schema); })
                    | std::ranges::to<std::vector<mutation>>();
            return make_mutation_reader_from_mutations_v2(std::move(schema), std::move(permit), std::move(muts), ps);
        };

        auto run_test = [&] (size_t buffer_size, multishard_reader_buffer_hint buffer_hint,
                seastar::compat::source_location sl = seastar::compat::source_location::current()) {
            testlog.info("run_test(): buffer_size: {}, buffer_hint: {} @ {}:{}", buffer_size, bool(buffer_hint), sl.file_name(), sl.line());

            auto reader = make_multishard_combining_reader_v2_for_tests(
                    *sharder,
                    seastar::make_shared<test_reader_lifecycle_policy>(reader_factory, std::make_unique<evicting_semaphore_factory>(semaphore_registry)),
                    schema,
                    semaphore.make_permit(),
                    query::full_partition_range,
                    schema->full_slice(),
                    {},
                    mutation_reader::forwarding::no,
                    buffer_hint,
                    read_ahead::no);
            reader.set_max_buffer_size(buffer_size);
            auto close_reader = deferred_close(reader);

            reader.fill_buffer().get();

            // simulate the expected read algorithm to calculate the amount each shard should have read
            std::vector<size_t> buffer_fill_calls_per_shard(smp::count, 0);
            size_t shards_visited{0};
            {
                size_t to_read = buffer_size;
                size_t data_left = total_data;
                auto shard_data_left = data_per_shard;
                const auto shard_reader_buffer_size = buffer_hint ? buffer_size : mutation_reader::default_max_buffer_size_in_bytes();
                while (to_read > 0 && data_left > 0) {
                    for (unsigned shard_id = 0; shard_id != smp::count && to_read > 0 && data_left > 0; ++shard_id) {
                        auto& shard_data = shard_data_left[shard_id];
                        BOOST_REQUIRE(!shard_data.empty());

                        size_t current_buffer_size{0};
                        auto* current_partition = &shard_data.back();
                        bool stop_on_full_buffer = false;
                        while (to_read > 0 && data_left > 0) {
                            const auto fragment_size = *current_partition == partition_size
                                    ? partition_start_size
                                    : (*current_partition == partition_end_size ? partition_end_size : row_size);

                            current_buffer_size += fragment_size;
                            testlog.trace("fill buffer loop for shard#{}: to_read={}, current_partition={}, fragment_size={}, current_buffer_size={}", shard_id, to_read, *current_partition, fragment_size, current_buffer_size);
                            if (current_buffer_size >= shard_reader_buffer_size) {
                                ++buffer_fill_calls_per_shard.at(shard_id);
                                testlog.trace("fill buffer loop for shard#{}: finished buffer #{} with size {}", shard_id, buffer_fill_calls_per_shard.at(shard_id), current_buffer_size);
                                // After each eviction, the evictable reader will
                                // emit a range tombstone change resetting the
                                // current tombstone.
                                current_buffer_size = range_tombstone_size;
                                if (stop_on_full_buffer) {
                                    break;
                                }
                            }
                            *current_partition -= fragment_size;
                            to_read -= std::min(to_read, fragment_size);
                            data_left -= fragment_size;

                            if (!*current_partition) {
                                if (buffer_hint) {
                                    break;
                                } else {
                                    shard_data.pop_back();
                                    current_partition = &shard_data.back();
                                    stop_on_full_buffer = true;
                                }
                            }
                        }
                        if (current_buffer_size > range_tombstone_size) {
                            ++buffer_fill_calls_per_shard.at(shard_id);
                            testlog.trace("fill buffer loop for shard#{}: finished buffer #{} with size {}", shard_id, buffer_fill_calls_per_shard.at(shard_id), current_buffer_size);
                        }

                        if (!shard_data.back()) {
                            shard_data.pop_back();
                        }
                        ++shards_visited;
                    }
                }
            }

            for (unsigned shard_id = 0; shard_id != smp::count; ++shard_id) {
                testlog.trace("shard#{}", shard_id);

                if (shards_visited > shard_id) {
                    const auto reads_from_shard = buffer_fill_calls_per_shard.at(shard_id);

                    auto& shard_semaphore = semaphore_registry.at(shard_id);
                    BOOST_REQUIRE(bool(shard_semaphore));
                    BOOST_REQUIRE_EQUAL(shard_semaphore->get_stats().reads_admitted, reads_from_shard);
                    BOOST_REQUIRE_EQUAL(shard_semaphore->get_stats().permit_based_evictions, reads_from_shard);
                } else {
                    BOOST_REQUIRE(!semaphore_registry.at(shard_id));
                }
            }
        };

        run_test(mutation_reader::default_max_buffer_size_in_bytes(), multishard_reader_buffer_hint::no);
        run_test(100 * 1024, multishard_reader_buffer_hint::no);
        run_test(100 * 1024, multishard_reader_buffer_hint::yes);
        run_test(200 * 1024, multishard_reader_buffer_hint::no);
        run_test(200 * 1024, multishard_reader_buffer_hint::yes);
        run_test(400 * 1024, multishard_reader_buffer_hint::no);
        run_test(400 * 1024, multishard_reader_buffer_hint::yes);
    });
}

SEASTAR_TEST_CASE(test_multishard_reader_buffer_hint_small_partitions) {
    return do_with_cql_env_thread([] (cql_test_env& env) {
        simple_schema ss;
        const auto& schema = ss.schema();
        const sstring value(128, 'v');
        const size_t num_rows = 2;
        tests::reader_concurrency_semaphore_wrapper semaphore;

        const auto pkeys_by_tokens = ss.make_pkeys(64 * smp::count)
            | std::views::transform([] (const dht::decorated_key& dk) { return std::pair(dk.token(), dk); })
            | std::ranges::to<std::map<dht::token, dht::decorated_key>>();
        std::vector<std::vector<frozen_mutation>> frozen_muts(smp::count, std::vector<frozen_mutation>{});
        std::vector<lw_shared_ptr<reader_concurrency_semaphore>> semaphore_registry(smp::count, nullptr);

        unsigned i = 0;
        for (const auto& [token, dk] : pkeys_by_tokens) {
            mutation mut(schema, dk);

            for (uint32_t ck = 0; ck < num_rows; ++ck) {
                ss.add_row(mut, ss.make_ckey(ck), value);
            }
            frozen_muts[i++ % smp::count].emplace_back(mut);
        }

        auto sharder = std::make_unique<dummy_sharder>(schema->get_sharder(), std::move(pkeys_by_tokens));

        const auto reader_factory = [frozen_muts] (
                schema_ptr schema,
                reader_permit permit,
                const dht::partition_range&,
                const query::partition_slice& ps,
                tracing::trace_state_ptr,
                mutation_reader::forwarding) {
            auto muts = frozen_muts.at(this_shard_id())
                    | std::views::transform([schema] (const frozen_mutation& fm) { return fm.unfreeze(schema); })
                    | std::ranges::to<std::vector<mutation>>();
            return make_mutation_reader_from_mutations_v2(std::move(schema), std::move(permit), std::move(muts), ps);
        };

        auto reader = make_multishard_combining_reader_v2_for_tests(
                *sharder,
                seastar::make_shared<test_reader_lifecycle_policy>(reader_factory, std::make_unique<evicting_semaphore_factory>(semaphore_registry)),
                schema,
                semaphore.make_permit(),
                query::full_partition_range,
                schema->full_slice(),
                {},
                mutation_reader::forwarding::no,
                multishard_reader_buffer_hint::yes,
                read_ahead::no);
        auto close_reader = deferred_close(reader);

        reader.fill_buffer().get();

        for (unsigned shard_id = 0; shard_id != smp::count; ++shard_id) {
            const auto reads_from_shard = 1;

            auto& shard_semaphore = semaphore_registry.at(shard_id);
            BOOST_REQUIRE(bool(shard_semaphore));
            BOOST_REQUIRE_EQUAL(shard_semaphore->get_stats().reads_admitted, reads_from_shard);
            BOOST_REQUIRE_EQUAL(shard_semaphore->get_stats().permit_based_evictions, reads_from_shard);
        }
    });
}

BOOST_AUTO_TEST_SUITE_END()
