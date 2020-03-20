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


#include <random>
#include <experimental/source_location>

#include <boost/range/irange.hpp>
#include <boost/range/adaptor/uniqued.hpp>

#include <seastar/core/sleep.hh>
#include <seastar/core/do_with.hh>
#include <seastar/core/thread.hh>

#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>
#include "test/lib/mutation_assertions.hh"
#include "test/lib/flat_mutation_reader_assertions.hh"
#include "test/lib/tmpdir.hh"
#include "test/lib/sstable_utils.hh"
#include "test/lib/simple_schema.hh"
#include "test/lib/test_services.hh"
#include "test/lib/mutation_source_test.hh"
#include "test/lib/cql_test_env.hh"
#include "test/lib/make_random_string.hh"
#include "test/lib/dummy_partitioner.hh"
#include "test/lib/reader_lifecycle_policy.hh"

#include "dht/sharder.hh"
#include "mutation_reader.hh"
#include "schema_builder.hh"
#include "cell_locking.hh"
#include "sstables/sstables.hh"
#include "database.hh"
#include "partition_slice_builder.hh"
#include "schema_registry.hh"
#include "service/priority_manager.hh"

static schema_ptr make_schema() {
    return schema_builder("ks", "cf")
        .with_column("pk", bytes_type, column_kind::partition_key)
        .with_column("v", bytes_type, column_kind::regular_column)
        .build();
}

SEASTAR_TEST_CASE(test_combining_two_readers_with_the_same_row) {
    return seastar::async([] {
        auto s = make_schema();

        mutation m1(s, partition_key::from_single_value(*s, "key1"));
        m1.set_clustered_cell(clustering_key::make_empty(), "v", data_value(bytes("v1")), 1);

        mutation m2(s, partition_key::from_single_value(*s, "key1"));
        m2.set_clustered_cell(clustering_key::make_empty(), "v", data_value(bytes("v2")), 2);

        assert_that(make_combined_reader(s, flat_mutation_reader_from_mutations({m1}), flat_mutation_reader_from_mutations({m2})))
            .produces(m2)
            .produces_end_of_stream();
    });
}

SEASTAR_TEST_CASE(test_combining_two_non_overlapping_readers) {
    return seastar::async([] {
        auto s = make_schema();

        mutation m1(s, partition_key::from_single_value(*s, "keyB"));
        m1.set_clustered_cell(clustering_key::make_empty(), "v", data_value(bytes("v1")), 1);

        mutation m2(s, partition_key::from_single_value(*s, "keyA"));
        m2.set_clustered_cell(clustering_key::make_empty(), "v", data_value(bytes("v2")), 2);

        auto cr = make_combined_reader(s, flat_mutation_reader_from_mutations({m1}), flat_mutation_reader_from_mutations({m2}));
        assert_that(std::move(cr))
            .produces(m2)
            .produces(m1)
            .produces_end_of_stream();
    });
}

SEASTAR_TEST_CASE(test_combining_two_partially_overlapping_readers) {
    return seastar::async([] {
        auto s = make_schema();

        mutation m1(s, partition_key::from_single_value(*s, "keyA"));
        m1.set_clustered_cell(clustering_key::make_empty(), "v", data_value(bytes("v1")), 1);

        mutation m2(s, partition_key::from_single_value(*s, "keyB"));
        m2.set_clustered_cell(clustering_key::make_empty(), "v", data_value(bytes("v2")), 1);

        mutation m3(s, partition_key::from_single_value(*s, "keyC"));
        m3.set_clustered_cell(clustering_key::make_empty(), "v", data_value(bytes("v3")), 1);

        assert_that(make_combined_reader(s, flat_mutation_reader_from_mutations({m1, m2}), flat_mutation_reader_from_mutations({m2, m3})))
            .produces(m1)
            .produces(m2)
            .produces(m3)
            .produces_end_of_stream();
    });
}

SEASTAR_TEST_CASE(test_combining_one_reader_with_many_partitions) {
    return seastar::async([] {
        auto s = make_schema();

        mutation m1(s, partition_key::from_single_value(*s, "keyA"));
        m1.set_clustered_cell(clustering_key::make_empty(), "v", data_value(bytes("v1")), 1);

        mutation m2(s, partition_key::from_single_value(*s, "keyB"));
        m2.set_clustered_cell(clustering_key::make_empty(), "v", data_value(bytes("v2")), 1);

        mutation m3(s, partition_key::from_single_value(*s, "keyC"));
        m3.set_clustered_cell(clustering_key::make_empty(), "v", data_value(bytes("v3")), 1);

        std::vector<flat_mutation_reader> v;
        v.push_back(flat_mutation_reader_from_mutations({m1, m2, m3}));
        assert_that(make_combined_reader(s, std::move(v), streamed_mutation::forwarding::no, mutation_reader::forwarding::no))
            .produces(m1)
            .produces(m2)
            .produces(m3)
            .produces_end_of_stream();
    });
}

SEASTAR_THREAD_TEST_CASE(combined_reader_galloping_within_partition_test) {
    simple_schema s;

    const auto pk = s.make_pkey();
    const auto ckeys = s.make_ckeys(10);

    auto make_partition = [&] (auto&& ckey_indexes) -> mutation {
        mutation mut(s.schema(), pk);
        for (auto ckey_index : ckey_indexes) {
            s.add_row(mut, ckeys[ckey_index], format("val_{:02d}", ckey_index), 1);
        }
        return mut;
    };

    std::vector<flat_mutation_reader> v;
    v.push_back(flat_mutation_reader_from_mutations({make_partition(boost::irange(0, 5))}));
    v.push_back(flat_mutation_reader_from_mutations({make_partition(boost::irange(5, 10))}));
    assert_that(make_combined_reader(s.schema(), std::move(v), streamed_mutation::forwarding::no, mutation_reader::forwarding::no))
        .produces(make_partition(boost::irange(0, 10)))
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

    const auto k = s.make_pkeys(2);

    std::vector<flat_mutation_reader> v;
    v.push_back(flat_mutation_reader_from_mutations({
        make_partition_with_clustering_rows(s, k[0], boost::irange(5, 10)),
        make_partition_with_clustering_rows(s, k[1], boost::irange(0, 5))
    }));
    v.push_back(flat_mutation_reader_from_mutations({
        make_partition_with_clustering_rows(s, k[0], boost::irange(0, 5)),
        make_partition_with_clustering_rows(s, k[1], boost::irange(5, 10))
    }));
    assert_that(make_combined_reader(s.schema(), std::move(v), streamed_mutation::forwarding::no, mutation_reader::forwarding::no))
        .produces(make_partition_with_clustering_rows(s, k[0], boost::irange(0, 10)))
        .produces(make_partition_with_clustering_rows(s, k[1], boost::irange(0, 10)))
        .produces_end_of_stream();
}

SEASTAR_THREAD_TEST_CASE(combined_reader_galloping_changing_multiple_partitions_test) {
    simple_schema s;

    const auto k = s.make_pkeys(2);

    std::vector<flat_mutation_reader> v;
    v.push_back(flat_mutation_reader_from_mutations({
        make_partition_with_clustering_rows(s, k[0], boost::irange(0, 5)),
        make_partition_with_clustering_rows(s, k[1], boost::irange(0, 5))
    }));
    v.push_back(flat_mutation_reader_from_mutations({
        make_partition_with_clustering_rows(s, k[0], boost::irange(5, 10)),
        make_partition_with_clustering_rows(s, k[1], boost::irange(5, 10)),
    }));
    assert_that(make_combined_reader(s.schema(), std::move(v), streamed_mutation::forwarding::no, mutation_reader::forwarding::no))
        .produces(make_partition_with_clustering_rows(s, k[0], boost::irange(0, 10)))
        .produces(make_partition_with_clustering_rows(s, k[1], boost::irange(0, 10)))
        .produces_end_of_stream();
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

        auto m1 = make_mutation_with_key(s, "key1");
        auto m2 = make_mutation_with_key(s, "key2");
        auto m3 = make_mutation_with_key(s, "key3");
        auto m4 = make_mutation_with_key(s, "key4");

        // All pass
        assert_that(make_filtering_reader(flat_mutation_reader_from_mutations({m1, m2, m3, m4}),
                 [] (const dht::decorated_key& dk) { return true; }))
            .produces(m1)
            .produces(m2)
            .produces(m3)
            .produces(m4)
            .produces_end_of_stream();

        // None pass
        assert_that(make_filtering_reader(flat_mutation_reader_from_mutations({m1, m2, m3, m4}),
                 [] (const dht::decorated_key& dk) { return false; }))
            .produces_end_of_stream();

        // Trim front
        assert_that(make_filtering_reader(flat_mutation_reader_from_mutations({m1, m2, m3, m4}),
                [&] (const dht::decorated_key& dk) { return !dk.key().equal(*s, m1.key()); }))
            .produces(m2)
            .produces(m3)
            .produces(m4)
            .produces_end_of_stream();

        assert_that(make_filtering_reader(flat_mutation_reader_from_mutations({m1, m2, m3, m4}),
            [&] (const dht::decorated_key& dk) { return !dk.key().equal(*s, m1.key()) && !dk.key().equal(*s, m2.key()); }))
            .produces(m3)
            .produces(m4)
            .produces_end_of_stream();

        // Trim back
        assert_that(make_filtering_reader(flat_mutation_reader_from_mutations({m1, m2, m3, m4}),
                 [&] (const dht::decorated_key& dk) { return !dk.key().equal(*s, m4.key()); }))
            .produces(m1)
            .produces(m2)
            .produces(m3)
            .produces_end_of_stream();

        assert_that(make_filtering_reader(flat_mutation_reader_from_mutations({m1, m2, m3, m4}),
                 [&] (const dht::decorated_key& dk) { return !dk.key().equal(*s, m4.key()) && !dk.key().equal(*s, m3.key()); }))
            .produces(m1)
            .produces(m2)
            .produces_end_of_stream();

        // Trim middle
        assert_that(make_filtering_reader(flat_mutation_reader_from_mutations({m1, m2, m3, m4}),
                 [&] (const dht::decorated_key& dk) { return !dk.key().equal(*s, m3.key()); }))
            .produces(m1)
            .produces(m2)
            .produces(m4)
            .produces_end_of_stream();

        assert_that(make_filtering_reader(flat_mutation_reader_from_mutations({m1, m2, m3, m4}),
                 [&] (const dht::decorated_key& dk) { return !dk.key().equal(*s, m2.key()) && !dk.key().equal(*s, m3.key()); }))
            .produces(m1)
            .produces(m4)
            .produces_end_of_stream();
    });
}

SEASTAR_TEST_CASE(test_combining_two_readers_with_one_reader_empty) {
    return seastar::async([] {
        auto s = make_schema();
        mutation m1(s, partition_key::from_single_value(*s, "key1"));
        m1.set_clustered_cell(clustering_key::make_empty(), "v", data_value(bytes("v1")), 1);

        assert_that(make_combined_reader(s, flat_mutation_reader_from_mutations({m1}), make_empty_flat_reader(s)))
            .produces(m1)
            .produces_end_of_stream();
    });
}

SEASTAR_TEST_CASE(test_combining_two_empty_readers) {
    return seastar::async([] {
        auto s = make_schema();
        assert_that(make_combined_reader(s, make_empty_flat_reader(s), make_empty_flat_reader(s)))
            .produces_end_of_stream();
    });
}

SEASTAR_TEST_CASE(test_combining_one_empty_reader) {
    return seastar::async([] {
        std::vector<flat_mutation_reader> v;
        auto s = make_schema();
        v.push_back(make_empty_flat_reader(s));
        assert_that(make_combined_reader(s, std::move(v), streamed_mutation::forwarding::no, mutation_reader::forwarding::no))
            .produces_end_of_stream();
    });
}

std::vector<dht::decorated_key> generate_keys(schema_ptr s, int count) {
    auto keys = boost::copy_range<std::vector<dht::decorated_key>>(
        boost::irange(0, count) | boost::adaptors::transformed([s] (int key) {
            auto pk = partition_key::from_single_value(*s, int32_type->decompose(data_value(key)));
            return dht::decorate_key(*s, std::move(pk));
        }));
    return std::move(boost::range::sort(keys, dht::decorated_key::less_comparator(s)));
}

std::vector<dht::ring_position> to_ring_positions(const std::vector<dht::decorated_key>& keys) {
    return boost::copy_range<std::vector<dht::ring_position>>(keys | boost::adaptors::transformed([] (const dht::decorated_key& key) {
        return dht::ring_position(key);
    }));
}

SEASTAR_TEST_CASE(test_fast_forwarding_combining_reader) {
    return seastar::async([] {
        auto s = make_schema();

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

        auto make_reader = [&] (const dht::partition_range& pr) {
            std::vector<flat_mutation_reader> readers;
            boost::range::transform(mutations, std::back_inserter(readers), [&pr] (auto& ms) {
                return flat_mutation_reader_from_mutations({ms}, pr);
            });
            return make_combined_reader(s, std::move(readers));
        };

        auto pr = dht::partition_range::make_open_ended_both_sides();
        assert_that(make_reader(pr))
            .produces(keys[0])
            .produces(keys[1])
            .produces(keys[2])
            .produces(keys[3])
            .produces(keys[4])
            .produces(keys[5])
            .produces(keys[6])
            .produces_end_of_stream();

        pr = dht::partition_range::make(ring[0], ring[0]);
            assert_that(make_reader(pr))
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
    std::vector<flat_mutation_reader> v;
    v.push_back(flat_mutation_reader_from_mutations(make_n_mutations(boost::irange(0, 5), 7), pr));
    v.push_back(flat_mutation_reader_from_mutations(make_n_mutations(boost::irange(5, 10), 7), pr));

    assert_that(make_combined_reader(s.schema(), std::move(v), streamed_mutation::forwarding::no, mutation_reader::forwarding::yes))
            .produces(make_partition_with_clustering_rows(s, pkeys[0], boost::irange(0, 10)))
            .produces_end_of_stream()
            .fast_forward_to(dht::partition_range::make(ring[1], ring[1]))
            .produces(make_partition_with_clustering_rows(s, pkeys[1], boost::irange(0, 10)))
            .produces_end_of_stream()
            .fast_forward_to(dht::partition_range::make(ring[3], ring[4]))
            .produces(make_partition_with_clustering_rows(s, pkeys[3], boost::irange(0, 10)))
    .fast_forward_to(dht::partition_range::make({ ring[4], false }, ring[5]))
            .produces(make_partition_with_clustering_rows(s, pkeys[5], boost::irange(0, 10)))
            .produces_end_of_stream()
    .fast_forward_to(dht::partition_range::make_starting_with(ring[6]))
            .produces(make_partition_with_clustering_rows(s, pkeys[6], boost::irange(0, 10)))
            .produces_end_of_stream();
}

SEASTAR_TEST_CASE(test_sm_fast_forwarding_combining_reader) {
    return seastar::async([] {
        storage_service_for_tests ssft;
        simple_schema s;

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

        std::vector<flat_mutation_reader> readers;
        for (auto& mutations : readers_mutations) {
            readers.emplace_back(flat_mutation_reader_from_mutations(mutations, streamed_mutation::forwarding::yes));
        }

        assert_that(make_combined_reader(s.schema(), std::move(readers), streamed_mutation::forwarding::yes, mutation_reader::forwarding::no))
                .produces_partition_start(pkeys[0])
                .produces_end_of_stream()
                .fast_forward_to(position_range::all_clustered_rows())
                .produces_row_with_key(ckeys[0])
                .next_partition()
                .produces_partition_start(pkeys[1])
                .produces_end_of_stream()
                .fast_forward_to(position_range(position_in_partition::before_key(ckeys[2]), position_in_partition::after_key(ckeys[2])))
                .produces_row_with_key(ckeys[2])
                .produces_end_of_stream()
                .fast_forward_to(position_range(position_in_partition::after_key(ckeys[2]), position_in_partition::after_all_clustered_rows()))
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
    std::vector<flat_mutation_reader> v;
    v.push_back(flat_mutation_reader_from_mutations(make_n_mutations(boost::irange(0, 5), 3), streamed_mutation::forwarding::yes));
    v.push_back(flat_mutation_reader_from_mutations(make_n_mutations(boost::irange(5, 10), 3), streamed_mutation::forwarding::yes));

    auto reader = make_combined_reader(s.schema(), std::move(v), streamed_mutation::forwarding::yes, mutation_reader::forwarding::no);
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
            .fast_forward_to(position_range(position_in_partition::before_key(ckeys[0]), position_in_partition::after_key(ckeys[3])))
            .produces_row_with_key(ckeys[0])
            .produces_row_with_key(ckeys[1])
            .produces_row_with_key(ckeys[2])
            .produces_row_with_key(ckeys[3])
            .produces_end_of_stream()
            .fast_forward_to(position_range(position_in_partition::after_key(ckeys[6]), position_in_partition::after_all_clustered_rows()))
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

struct sst_factory {
    sstables::test_env env;
    schema_ptr s;
    sstring path;
    unsigned gen;
    uint32_t level;

    sst_factory(schema_ptr s, const sstring& path, unsigned gen, int level)
        : s(s)
        , path(path)
        , gen(gen)
        , level(level)
    {}

    sstables::shared_sstable operator()() {
        auto sst = env.make_sstable(s, path, gen, sstables::sstable::version_types::la, sstables::sstable::format_types::big);
        sst->set_unshared();
        //sst->set_sstable_level(level);
        sst->get_metadata_collector().sstable_level(level);

        return sst;
    }
};

SEASTAR_THREAD_TEST_CASE(combined_mutation_reader_test) {
    storage_service_for_tests ssft;

    simple_schema s;

    auto pkeys = s.make_pkeys(6);
    const auto ckeys = s.make_ckeys(4);

    boost::sort(pkeys, [&s] (const dht::decorated_key& a, const dht::decorated_key& b) {
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

    auto tmp = tmpdir();

    unsigned gen{0};
    std::vector<sstables::shared_sstable> sstable_list = {
            make_sstable_containing(sst_factory(s.schema(), tmp.path().string(), ++gen, 0), std::move(sstable_level_0_0_mutations)),
            make_sstable_containing(sst_factory(s.schema(), tmp.path().string(), ++gen, 1), std::move(sstable_level_1_0_mutations)),
            make_sstable_containing(sst_factory(s.schema(), tmp.path().string(), ++gen, 1), std::move(sstable_level_1_1_mutations)),
            make_sstable_containing(sst_factory(s.schema(), tmp.path().string(), ++gen, 2), std::move(sstable_level_2_0_mutations)),
            make_sstable_containing(sst_factory(s.schema(), tmp.path().string(), ++gen, 2), std::move(sstable_level_2_1_mutations)),
    };

    auto cs = sstables::make_compaction_strategy(sstables::compaction_strategy_type::leveled, {});
    auto sstable_set = make_lw_shared<sstables::sstable_set>(cs.make_sstable_set(s.schema()));

    std::vector<flat_mutation_reader> sstable_mutation_readers;

    for (auto sst : sstable_list) {
        sstable_set->insert(sst);

        sstable_mutation_readers.emplace_back(
            sst->as_mutation_source().make_reader(
                s.schema(),
                no_reader_permit(),
                query::full_partition_range,
                s.schema()->full_slice(),
                seastar::default_priority_class(),
                nullptr,
                streamed_mutation::forwarding::no,
                mutation_reader::forwarding::no));
    }

    auto list_reader = make_combined_reader(s.schema(),
            std::move(sstable_mutation_readers));

    auto incremental_reader = make_local_shard_sstable_reader(
            s.schema(),
            no_reader_permit(),
            sstable_set,
            query::full_partition_range,
            s.schema()->full_slice(),
            seastar::default_priority_class(),
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
}

static mutation make_mutation_with_key(simple_schema& s, dht::decorated_key dk) {
    static int i{0};

    mutation m(s.schema(), std::move(dk));
    s.add_row(m, s.make_ckey(++i), format("val_{:d}", i));
    return m;
}

class dummy_incremental_selector : public reader_selector {
    // To back _selector_position.
    dht::ring_position _position;
    std::vector<std::vector<mutation>> _readers_mutations;
    streamed_mutation::forwarding _fwd;
    dht::partition_range _pr;

    flat_mutation_reader pop_reader() {
        auto muts = std::move(_readers_mutations.back());
        _readers_mutations.pop_back();
        _position = _readers_mutations.empty() ? dht::ring_position::max() : _readers_mutations.back().front().decorated_key();
        _selector_position = _position;
        return flat_mutation_reader_from_mutations(std::move(muts), _pr, _fwd);
    }
public:
    // readers_mutations is expected to be sorted on both levels.
    // 1) the inner vector is expected to be sorted by decorated_key.
    // 2) the outer vector is expected to be sorted by the decorated_key
    //  of its first mutation.
    dummy_incremental_selector(schema_ptr s,
            std::vector<std::vector<mutation>> reader_mutations,
            dht::partition_range pr = query::full_partition_range,
            streamed_mutation::forwarding fwd = streamed_mutation::forwarding::no)
        : reader_selector(s, dht::ring_position_view::min())
        , _position(dht::ring_position::min())
        , _readers_mutations(std::move(reader_mutations))
        , _fwd(fwd)
        , _pr(std::move(pr)) {
        // So we can pop the next reader off the back
        boost::reverse(_readers_mutations);
    }
    virtual std::vector<flat_mutation_reader> create_new_readers(const std::optional<dht::ring_position_view>& pos) override {
        if (_readers_mutations.empty()) {
            return {};
        }

        std::vector<flat_mutation_reader> readers;

        if (!pos) {
            readers.emplace_back(pop_reader());
            return readers;
        }

        while (!_readers_mutations.empty() && dht::ring_position_tri_compare(*_s, _selector_position, *pos) <= 0) {
            readers.emplace_back(pop_reader());
        }
        return readers;
    }
    virtual std::vector<flat_mutation_reader> fast_forward_to(const dht::partition_range& pr, db::timeout_clock::time_point timeout) override {
        _pr = pr;
        return create_new_readers(dht::ring_position_view::for_range_start(_pr));
    }
};

SEASTAR_TEST_CASE(reader_selector_gap_between_readers_test) {
    return seastar::async([] {
        storage_service_for_tests ssft;

        simple_schema s;
        auto pkeys = s.make_pkeys(3);

        boost::sort(pkeys, [&s] (const dht::decorated_key& a, const dht::decorated_key& b) {
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

        auto reader = make_combined_reader(s.schema(),
                std::make_unique<dummy_incremental_selector>(s.schema(), std::move(readers_mutations)),
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
        storage_service_for_tests ssft;

        simple_schema s;
        auto pkeys = s.make_pkeys(4);

        boost::sort(pkeys, [&s] (const dht::decorated_key& a, const dht::decorated_key& b) {
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

        auto reader = make_combined_reader(s.schema(),
                std::make_unique<dummy_incremental_selector>(s.schema(), std::move(readers_mutations)),
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
        storage_service_for_tests ssft;

        simple_schema s;
        auto pkeys = s.make_pkeys(5);

        boost::sort(pkeys, [&s] (const dht::decorated_key& a, const dht::decorated_key& b) {
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

        auto reader = make_combined_reader(s.schema(),
                std::make_unique<dummy_incremental_selector>(s.schema(),
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

static const std::size_t new_reader_base_cost{16 * 1024};

sstables::shared_sstable create_sstable(sstables::test_env& env, simple_schema& sschema, const sstring& path) {
    std::vector<mutation> mutations;
    mutations.reserve(1 << 14);

    for (std::size_t p = 0; p < (1 << 10); ++p) {
        mutation m(sschema.schema(), sschema.make_pkey(p));
        sschema.add_static_row(m, format("{:d}_static_val", p));

        for (std::size_t c = 0; c < (1 << 4); ++c) {
            sschema.add_row(m, sschema.make_ckey(c), format("val_{:d}", c));
        }

        mutations.emplace_back(std::move(m));
        thread::yield();
    }

    return make_sstable_containing([&] {
            return env.make_sstable(sschema.schema(), path, 0, sstables::sstable::version_types::la, sstables::sstable::format_types::big);
        }
        , mutations);
}

static
sstables::shared_sstable create_sstable(sstables::test_env& env, schema_ptr s, std::vector<mutation> mutations) {
    static thread_local auto tmp = tmpdir();
    static int gen = 0;
    return make_sstable_containing([&] {
        return env.make_sstable(s, tmp.path().string(), gen++, sstables::sstable::version_types::la, sstables::sstable::format_types::big);
    }, mutations);
}

class tracking_reader : public flat_mutation_reader::impl {
    flat_mutation_reader _reader;
    std::size_t _call_count{0};
    std::size_t _ff_count{0};
public:
    tracking_reader(schema_ptr schema, reader_permit permit, lw_shared_ptr<sstables::sstable> sst)
        : impl(schema)
        , _reader(sst->read_range_rows_flat(
                        schema,
                        permit,
                        query::full_partition_range,
                        schema->full_slice(),
                        default_priority_class(),
                        tracing::trace_state_ptr(),
                        streamed_mutation::forwarding::no,
                        mutation_reader::forwarding::yes)) {
    }

    virtual future<> fill_buffer(db::timeout_clock::time_point timeout) override {
        ++_call_count;
        return _reader.fill_buffer(timeout).then([this] {
            _end_of_stream = _reader.is_end_of_stream();
            while (!_reader.is_buffer_empty()) {
                push_mutation_fragment(_reader.pop_mutation_fragment());
            }
        });
    }

    virtual void next_partition() override {
        _end_of_stream = false;
        clear_buffer_to_next_partition();
        if (is_buffer_empty()) {
            _reader.next_partition();
        }
    }

    virtual future<> fast_forward_to(const dht::partition_range& pr, db::timeout_clock::time_point timeout) override {
        ++_ff_count;
        // Don't forward this to the underlying reader, it will force us
        // to come up with meaningful partition-ranges which is hard and
        // unecessary for these tests.
        return make_ready_future<>();
    }

    virtual future<> fast_forward_to(position_range, db::timeout_clock::time_point timeout) override {
        throw std::bad_function_call();
    }

    std::size_t call_count() const {
        return _call_count;
    }

    std::size_t ff_count() const {
        return _ff_count;
    }
};

class reader_wrapper {
    flat_mutation_reader _reader;
    tracking_reader* _tracker{nullptr};
    db::timeout_clock::time_point _timeout;
public:
    reader_wrapper(
            reader_concurrency_semaphore& semaphore,
            schema_ptr schema,
            lw_shared_ptr<sstables::sstable> sst,
            db::timeout_clock::time_point timeout = db::no_timeout)
        : _reader(make_empty_flat_reader(schema))
        , _timeout(timeout)
    {
        auto ms = mutation_source([this, sst=std::move(sst)] (schema_ptr schema,
                    reader_permit permit,
                    const dht::partition_range&,
                    const query::partition_slice&,
                    const io_priority_class&,
                    tracing::trace_state_ptr,
                    streamed_mutation::forwarding,
                    mutation_reader::forwarding) {
            auto tracker_ptr = std::make_unique<tracking_reader>(std::move(schema), std::move(permit), std::move(sst));
            _tracker = tracker_ptr.get();
            return flat_mutation_reader(std::move(tracker_ptr));
        });

        _reader = make_restricted_flat_reader(semaphore, std::move(ms), schema);
    }

    reader_wrapper(
            reader_concurrency_semaphore& semaphore,
            schema_ptr schema,
            lw_shared_ptr<sstables::sstable> sst,
            db::timeout_clock::duration timeout_duration)
        : reader_wrapper(semaphore, std::move(schema), std::move(sst), db::timeout_clock::now() + timeout_duration) {
    }

    future<> operator()() {
        while (!_reader.is_buffer_empty()) {
            _reader.pop_mutation_fragment();
        }
        return _reader.fill_buffer(_timeout);
    }

    future<> fast_forward_to(const dht::partition_range& pr) {
        return _reader.fast_forward_to(pr, _timeout);
    }

    std::size_t call_count() const {
        return _tracker ? _tracker->call_count() : 0;
    }

    std::size_t ff_count() const {
        return _tracker ? _tracker->ff_count() : 0;
    }

    bool created() const {
        return bool(_tracker);
    }
};

class dummy_file_impl : public file_impl {
    virtual future<size_t> write_dma(uint64_t pos, const void* buffer, size_t len, const io_priority_class& pc) override {
        return make_ready_future<size_t>(0);
    }

    virtual future<size_t> write_dma(uint64_t pos, std::vector<iovec> iov, const io_priority_class& pc) override {
        return make_ready_future<size_t>(0);
    }

    virtual future<size_t> read_dma(uint64_t pos, void* buffer, size_t len, const io_priority_class& pc) override {
        return make_ready_future<size_t>(0);
    }

    virtual future<size_t> read_dma(uint64_t pos, std::vector<iovec> iov, const io_priority_class& pc) override {
        return make_ready_future<size_t>(0);
    }

    virtual future<> flush(void) override {
        return make_ready_future<>();
    }

    virtual future<struct stat> stat(void) override {
        return make_ready_future<struct stat>();
    }

    virtual future<> truncate(uint64_t length) override {
        return make_ready_future<>();
    }

    virtual future<> discard(uint64_t offset, uint64_t length) override {
        return make_ready_future<>();
    }

    virtual future<> allocate(uint64_t position, uint64_t length) override {
        return make_ready_future<>();
    }

    virtual future<uint64_t> size(void) override {
        return make_ready_future<uint64_t>(0);
    }

    virtual future<> close() override {
        return make_ready_future<>();
    }

    virtual subscription<directory_entry> list_directory(std::function<future<> (directory_entry de)> next) override {
        throw std::bad_function_call();
    }

    virtual future<temporary_buffer<uint8_t>> dma_read_bulk(uint64_t offset, size_t range_size, const io_priority_class& pc) override {
        temporary_buffer<uint8_t> buf(1024);

        memset(buf.get_write(), 0xff, buf.size());

        return make_ready_future<temporary_buffer<uint8_t>>(std::move(buf));
    }
};

SEASTAR_TEST_CASE(reader_restriction_file_tracking) {
    return async([&] {
        reader_concurrency_semaphore semaphore(100, 4 * 1024, get_name());
        // Testing the tracker here, no need to have a base cost.
        auto permit = semaphore.wait_admission(0, db::no_timeout).get0();

        {
            auto tracked_file = make_tracked_file(file(shared_ptr<file_impl>(make_shared<dummy_file_impl>())), permit);

            BOOST_REQUIRE_EQUAL(4 * 1024, semaphore.available_resources().memory);

            auto buf1 = tracked_file.dma_read_bulk<char>(0, 0).get0();
            BOOST_REQUIRE_EQUAL(3 * 1024, semaphore.available_resources().memory);

            auto buf2 = tracked_file.dma_read_bulk<char>(0, 0).get0();
            BOOST_REQUIRE_EQUAL(2 * 1024, semaphore.available_resources().memory);

            auto buf3 = tracked_file.dma_read_bulk<char>(0, 0).get0();
            BOOST_REQUIRE_EQUAL(1 * 1024, semaphore.available_resources().memory);

            auto buf4 = tracked_file.dma_read_bulk<char>(0, 0).get0();
            BOOST_REQUIRE_EQUAL(0 * 1024, semaphore.available_resources().memory);

            auto buf5 = tracked_file.dma_read_bulk<char>(0, 0).get0();
            BOOST_REQUIRE_EQUAL(-1 * 1024, semaphore.available_resources().memory);

            // Reassing buf1, should still have the same amount of units.
            buf1 = tracked_file.dma_read_bulk<char>(0, 0).get0();
            BOOST_REQUIRE_EQUAL(-1 * 1024, semaphore.available_resources().memory);

            // Move buf1 to the heap, so that we can safely destroy it
            auto buf1_ptr = std::make_unique<temporary_buffer<char>>(std::move(buf1));
            BOOST_REQUIRE_EQUAL(-1 * 1024, semaphore.available_resources().memory);

            buf1_ptr.reset();
            BOOST_REQUIRE_EQUAL(0 * 1024, semaphore.available_resources().memory);

            // Move tracked_file to the heap, so that we can safely destroy it.
            auto tracked_file_ptr = std::make_unique<file>(std::move(tracked_file));
            tracked_file_ptr.reset();

            // Move buf4 to the heap, so that we can safely destroy it
            auto buf4_ptr = std::make_unique<temporary_buffer<char>>(std::move(buf4));
            BOOST_REQUIRE_EQUAL(0 * 1024, semaphore.available_resources().memory);

            // Releasing buffers that overlived the tracked-file they
            // originated from should succeed.
            buf4_ptr.reset();
            BOOST_REQUIRE_EQUAL(1 * 1024, semaphore.available_resources().memory);
        }

        // All units should have been deposited back.
        REQUIRE_EVENTUALLY_EQUAL(4 * 1024, semaphore.available_resources().memory);
    });
}

SEASTAR_TEST_CASE(restricted_reader_reading) {
    return sstables::test_env::do_with_async([&] (sstables::test_env& env) {
        storage_service_for_tests ssft;
        reader_concurrency_semaphore semaphore(2, new_reader_base_cost, get_name());

        {
            simple_schema s;
            auto tmp = tmpdir();
            auto sst = create_sstable(env, s, tmp.path().string());

            auto reader1 = reader_wrapper(semaphore, s.schema(), sst);

            reader1().get();

            BOOST_REQUIRE_LE(semaphore.available_resources().count, 1);
            BOOST_REQUIRE_LE(semaphore.available_resources().memory, 0);
            BOOST_REQUIRE_EQUAL(reader1.call_count(), 1);

            auto reader2 = reader_wrapper(semaphore, s.schema(), sst);
            auto read2_fut = reader2();

            // reader2 shouldn't be allowed yet
            BOOST_REQUIRE_EQUAL(reader2.call_count(), 0);
            BOOST_REQUIRE_EQUAL(semaphore.waiters(), 1);

            auto reader3 = reader_wrapper(semaphore, s.schema(), sst);
            auto read3_fut = reader3();

            // reader3 shouldn't be allowed yet
            BOOST_REQUIRE_EQUAL(reader3.call_count(), 0);
            BOOST_REQUIRE_EQUAL(semaphore.waiters(), 2);

            // Move reader1 to the heap, so that we can safely destroy it.
            auto reader1_ptr = std::make_unique<reader_wrapper>(std::move(reader1));
            reader1_ptr.reset();

            // reader1's destruction should've freed up enough memory for
            // reader2 by now.
            REQUIRE_EVENTUALLY_EQUAL(reader2.call_count(), 1);
            read2_fut.get();

            // But reader3 should still not be allowed
            BOOST_REQUIRE_EQUAL(reader3.call_count(), 0);
            BOOST_REQUIRE_EQUAL(semaphore.waiters(), 1);

            // Move reader2 to the heap, so that we can safely destroy it.
            auto reader2_ptr = std::make_unique<reader_wrapper>(std::move(reader2));
            reader2_ptr.reset();

            // Again, reader2's destruction should've freed up enough memory
            // for reader3 by now.
            REQUIRE_EVENTUALLY_EQUAL(reader3.call_count(), 1);
            BOOST_REQUIRE_EQUAL(semaphore.waiters(), 0);
            read3_fut.get();

            {
                BOOST_REQUIRE_LE(semaphore.available_resources().memory, 0);

                // Already allowed readers should not be blocked anymore even if
                // there are no more units available.
                read3_fut = reader3();
                BOOST_REQUIRE_EQUAL(reader3.call_count(), 2);
                read3_fut.get();
            }
        }

        // All units should have been deposited back.
        REQUIRE_EVENTUALLY_EQUAL(new_reader_base_cost, semaphore.available_resources().memory);
    });
}

SEASTAR_TEST_CASE(restricted_reader_timeout) {
    return sstables::test_env::do_with_async([&] (sstables::test_env& env) {
        storage_service_for_tests ssft;
        reader_concurrency_semaphore semaphore(2, new_reader_base_cost, get_name());

        {
            simple_schema s;
            auto tmp = tmpdir();
            auto sst = create_sstable(env, s, tmp.path().string());

            auto timeout = std::chrono::duration_cast<db::timeout_clock::time_point::duration>(std::chrono::milliseconds{1});

            auto reader1 = std::make_unique<reader_wrapper>(semaphore, s.schema(), sst, timeout);
            (*reader1)().get();

            auto reader2 = std::make_unique<reader_wrapper>(semaphore, s.schema(), sst, timeout);
            auto read2_fut = (*reader2)();

            auto reader3 = std::make_unique<reader_wrapper>(semaphore, s.schema(), sst, timeout);
            auto read3_fut = (*reader3)();

            BOOST_REQUIRE_EQUAL(semaphore.waiters(), 2);

            const auto futures_failed = eventually_true([&] { return read2_fut.failed() && read3_fut.failed(); });
            BOOST_CHECK(futures_failed);

            if (futures_failed) {
                BOOST_CHECK_THROW(std::rethrow_exception(read2_fut.get_exception()), semaphore_timed_out);
                BOOST_CHECK_THROW(std::rethrow_exception(read3_fut.get_exception()), semaphore_timed_out);
            } else {
                // We need special cleanup when the test failed to avoid invalid
                // memory access.
                reader1.reset();
                BOOST_CHECK(eventually_true([&] { return read2_fut.available(); }));
                reader2.reset();
                BOOST_CHECK(eventually_true([&] { return read3_fut.available(); }));
                reader3.reset();
            }
       }

        // All units should have been deposited back.
        REQUIRE_EVENTUALLY_EQUAL(new_reader_base_cost, semaphore.available_resources().memory);
    });
}

SEASTAR_TEST_CASE(restricted_reader_max_queue_length) {
    return sstables::test_env::do_with_async([&] (sstables::test_env& env) {
        storage_service_for_tests ssft;

        reader_concurrency_semaphore semaphore(2, new_reader_base_cost, get_name(), 2);

        {
            simple_schema s;
            auto tmp = tmpdir();
            auto sst = create_sstable(env, s, tmp.path().string());

            auto reader1_ptr = std::make_unique<reader_wrapper>(semaphore, s.schema(), sst);
            (*reader1_ptr)().get();

            auto reader2_ptr = std::make_unique<reader_wrapper>(semaphore, s.schema(), sst);
            auto read2_fut = (*reader2_ptr)();

            auto reader3_ptr = std::make_unique<reader_wrapper>(semaphore, s.schema(), sst);
            auto read3_fut = (*reader3_ptr)();

            auto reader4 = reader_wrapper(semaphore, s.schema(), sst);

            BOOST_REQUIRE_EQUAL(semaphore.waiters(), 2);

            // The queue should now be full.
            BOOST_REQUIRE_THROW(reader4().get(), std::runtime_error);

            reader1_ptr.reset();
            read2_fut.get();
            reader2_ptr.reset();
            read3_fut.get();
        }

        REQUIRE_EVENTUALLY_EQUAL(new_reader_base_cost, semaphore.available_resources().memory);
    });
}

SEASTAR_TEST_CASE(restricted_reader_create_reader) {
    return sstables::test_env::do_with_async([&] (sstables::test_env& env) {
        storage_service_for_tests ssft;
        reader_concurrency_semaphore semaphore(100, new_reader_base_cost, get_name());

        {
            simple_schema s;
            auto tmp = tmpdir();
            auto sst = create_sstable(env, s, tmp.path().string());

            {
                auto reader = reader_wrapper(semaphore, s.schema(), sst);
                // This fast-forward is stupid, I know but the
                // underlying dummy reader won't care, so it's fine.
                reader.fast_forward_to(query::full_partition_range).get();

                BOOST_REQUIRE(reader.created());
                BOOST_REQUIRE_EQUAL(reader.call_count(), 0);
                BOOST_REQUIRE_EQUAL(reader.ff_count(), 1);
            }

            {
                auto reader = reader_wrapper(semaphore, s.schema(), sst);
                reader().get();

                BOOST_REQUIRE(reader.created());
                BOOST_REQUIRE_EQUAL(reader.call_count(), 1);
                BOOST_REQUIRE_EQUAL(reader.ff_count(), 0);
            }
        }

        REQUIRE_EVENTUALLY_EQUAL(new_reader_base_cost, semaphore.available_resources().memory);
    });
}

SEASTAR_TEST_CASE(test_restricted_reader_as_mutation_source) {
    return seastar::async([test_name = get_name()] {
        reader_concurrency_semaphore semaphore(100, 10 * new_reader_base_cost, test_name);

        auto make_restricted_populator = [&semaphore](schema_ptr s, const std::vector<mutation> &muts) {
            auto mt = make_lw_shared<memtable>(s);
            for (auto &&mut : muts) {
                mt->apply(mut);
            }

            auto ms = mt->as_data_source();
            return mutation_source([&semaphore, ms = std::move(ms)](schema_ptr schema,
                    reader_permit permit,
                    const dht::partition_range& range,
                    const query::partition_slice& slice,
                    const io_priority_class& pc,
                    tracing::trace_state_ptr tr,
                    streamed_mutation::forwarding fwd,
                    mutation_reader::forwarding fwd_mr) {
                return make_restricted_flat_reader(semaphore, std::move(ms), std::move(schema), range, slice, pc, tr,
                        fwd, fwd_mr);
            });
        };
        run_mutation_source_tests(make_restricted_populator);
    });
}

static mutation compacted(const mutation& m) {
    auto result = m;
    result.partition().compact_for_compaction(*result.schema(), always_gc, gc_clock::now());
    return result;
}

SEASTAR_TEST_CASE(test_fast_forwarding_combined_reader_is_consistent_with_slicing) {
    return sstables::test_env::do_with_async([&] (sstables::test_env& env) {
        storage_service_for_tests ssft;
        random_mutation_generator gen(random_mutation_generator::generate_counters::no);
        auto s = gen.schema();

        const int n_readers = 10;
        auto keys = gen.make_partition_keys(3);
        std::vector<mutation> combined;
        std::vector<flat_mutation_reader> readers;
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
            mutation_source ds = create_sstable(env, s, muts)->as_mutation_source();
            readers.push_back(ds.make_reader(s,
                no_reader_permit(),
                dht::partition_range::make({keys[0]}, {keys[0]}),
                s->full_slice(), default_priority_class(), nullptr,
                streamed_mutation::forwarding::yes,
                mutation_reader::forwarding::yes));
        }

        flat_mutation_reader rd = make_combined_reader(s, std::move(readers),
            streamed_mutation::forwarding::yes,
            mutation_reader::forwarding::yes);

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
            }, db::no_timeout).get();

            for (auto&& range : ranges) {
                auto prange = position_range(range);
                rd.fast_forward_to(prange, db::no_timeout).get();
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
                }, db::no_timeout).get();
            }

            assert_that(result).is_equal_to(expected, ranges);
        };

        check_next_partition(combined[0]);
        rd.fast_forward_to(dht::partition_range::make_singular(keys[2]), db::no_timeout).get();
        check_next_partition(combined[2]);
    });
}

SEASTAR_TEST_CASE(test_combined_reader_slicing_with_overlapping_range_tombstones) {
    return sstables::test_env::do_with_async([&] (sstables::test_env& env) {
        storage_service_for_tests ssft;
        simple_schema ss;
        auto s = ss.schema();

        auto rt1 = ss.make_range_tombstone(ss.make_ckey_range(1, 10));
        auto rt2 = ss.make_range_tombstone(ss.make_ckey_range(1, 5)); // rt1 + rt2 = {[1, 5], (5, 10]}

        mutation m1 = ss.new_mutation(make_local_key(s));
        m1.partition().apply_delete(*s, rt1);
        mutation m2 = m1;
        m2.partition().apply_delete(*s, rt2);
        ss.add_row(m2, ss.make_ckey(4), "v2"); // position after rt2.position() but before rt2.end_position().

        std::vector<flat_mutation_reader> readers;

        mutation_source ds1 = create_sstable(env, s, {m1})->as_mutation_source();
        mutation_source ds2 = create_sstable(env, s, {m2})->as_mutation_source();

        // upper bound ends before the row in m2, so that the raw is fetched after next fast forward.
        auto range = ss.make_ckey_range(0, 3);

        {
            auto slice = partition_slice_builder(*s).with_range(range).build();
            readers.push_back(ds1.make_reader(s, no_reader_permit(), query::full_partition_range, slice));
            readers.push_back(ds2.make_reader(s, no_reader_permit(), query::full_partition_range, slice));

            auto rd = make_combined_reader(s, std::move(readers),
                streamed_mutation::forwarding::no, mutation_reader::forwarding::no);

            auto prange = position_range(range);
            mutation result(m1.schema(), m1.decorated_key());

            rd.consume_pausable([&] (mutation_fragment&& mf) {
                if (mf.position().has_clustering_key() && !mf.range().overlaps(*s, prange.start(), prange.end())) {
                    BOOST_FAIL(format("Received fragment which is not relevant for the slice: {}, slice: {}", mutation_fragment::printer(*s, mf), range));
                }
                result.partition().apply(*s, std::move(mf));
                return stop_iteration::no;
            }, db::no_timeout).get();

            assert_that(result).is_equal_to(m1 + m2, query::clustering_row_ranges({range}));
        }

        // Check fast_forward_to()
        {

            readers.push_back(ds1.make_reader(s, no_reader_permit(), query::full_partition_range, s->full_slice(), default_priority_class(),
                nullptr, streamed_mutation::forwarding::yes));
            readers.push_back(ds2.make_reader(s, no_reader_permit(), query::full_partition_range, s->full_slice(), default_priority_class(),
                nullptr, streamed_mutation::forwarding::yes));

            auto rd = make_combined_reader(s, std::move(readers),
                streamed_mutation::forwarding::yes, mutation_reader::forwarding::no);

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
                    BOOST_FAIL(format("Out of order fragment: {}, last pos: {}", mutation_fragment::printer(*s, mf), last_pos));
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
    });
}

SEASTAR_TEST_CASE(test_combined_mutation_source_is_a_mutation_source) {
    return seastar::async([] {
        // Creates a mutation source which combines N mutation sources with mutation fragments spread
        // among them in a round robin fashion.
        auto make_combined_populator = [] (int n_sources) {
            return [=] (schema_ptr s, const std::vector<mutation>& muts) {
                std::vector<lw_shared_ptr<memtable>> memtables;
                for (int i = 0; i < n_sources; ++i) {
                    memtables.push_back(make_lw_shared<memtable>(s));
                }

                int source_index = 0;
                for (auto&& m : muts) {
                    flat_mutation_reader_from_mutations({m}).consume_pausable([&] (mutation_fragment&& mf) {
                        mutation mf_m(m.schema(), m.decorated_key());
                        mf_m.partition().apply(*s, mf);
                        memtables[source_index++ % memtables.size()]->apply(mf_m);
                        return stop_iteration::no;
                    }, db::no_timeout).get();
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

    do_with_cql_env([] (cql_test_env& env) -> future<> {
        auto populate = [] (schema_ptr s, const std::vector<mutation>& mutations) {
            const auto remote_shard = (engine().cpu_id() + 1) % smp::count;
            auto frozen_mutations = boost::copy_range<std::vector<frozen_mutation>>(
                mutations
                | boost::adaptors::transformed([] (const mutation& m) { return freeze(m); })
            );
            auto remote_mt = smp::submit_to(remote_shard, [s = global_schema_ptr(s), &frozen_mutations] {
                auto mt = make_lw_shared<memtable>(s.get());

                for (auto& mut : frozen_mutations) {
                    mt->apply(mut, s.get());
                }

                return make_foreign(mt);
            }).get0();

            auto reader_factory = [remote_shard, remote_mt = std::move(remote_mt)] (schema_ptr s,
                    const dht::partition_range& range,
                    const query::partition_slice& slice,
                    const io_priority_class& pc,
                    tracing::trace_state_ptr trace_state,
                    streamed_mutation::forwarding fwd_sm,
                    mutation_reader::forwarding fwd_mr) {
                auto remote_reader = smp::submit_to(remote_shard,
                        [&, s = global_schema_ptr(s), fwd_sm, fwd_mr, trace_state = tracing::global_trace_state_ptr(trace_state)] {
                    return make_foreign(std::make_unique<flat_mutation_reader>(remote_mt->make_flat_reader(s.get(),
                            range,
                            slice,
                            pc,
                            trace_state.get(),
                            fwd_sm,
                            fwd_mr)));
                }).get0();
                return make_foreign_reader(s, std::move(remote_reader), fwd_sm);
            };

            auto reader_factory_ptr = make_lw_shared<decltype(reader_factory)>(std::move(reader_factory));

            return mutation_source([reader_factory_ptr] (schema_ptr s,
                    reader_permit,
                    const dht::partition_range& range,
                    const query::partition_slice& slice,
                    const io_priority_class& pc,
                    tracing::trace_state_ptr trace_state,
                    streamed_mutation::forwarding fwd_sm,
                    mutation_reader::forwarding fwd_mr) {
                return (*reader_factory_ptr)(std::move(s), range, slice, pc, std::move(trace_state), fwd_sm, fwd_mr);
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

        static std::optional<range_bound<clustering_key>> to_bound(const schema& s, std::optional<bound> b) {
            if (b) {
                return range_bound<clustering_key>(b->value.to_clustering_key(s), b->inclusive);
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

    const auto check = [&schema] (std::vector<range> ranges, key key, std::vector<range> output_ranges, bool reversed = false,
            std::experimental::source_location sl = std::experimental::source_location::current()) {
        auto actual_ranges = boost::copy_range<query::clustering_row_ranges>(ranges | boost::adaptors::transformed(
                    [&] (const range& r) { return r.to_clustering_range(*schema); }));

        query::trim_clustering_row_ranges_to(*schema, actual_ranges, key.to_clustering_key(*schema), reversed);

        const auto expected_ranges = boost::copy_range<query::clustering_row_ranges>(output_ranges | boost::adaptors::transformed(
                    [&] (const range& r) { return r.to_clustering_range(*schema); }));

        if (!std::equal(actual_ranges.begin(), actual_ranges.end(), expected_ranges.begin(), expected_ranges.end(),
                    [tri_cmp = clustering_key::tri_compare(*schema)] (const query::clustering_range& a, const query::clustering_range& b) {
            return a.equal(b, tri_cmp);
        })) {
            BOOST_FAIL(fmt::format("Unexpected result\nexpected {}\ngot {}\ncalled from {}:{}", expected_ranges, actual_ranges, sl.file_name(), sl.line()));
        }
    };
    const auto check_reversed = [&schema, &check] (std::vector<range> ranges, key key, std::vector<range> output_ranges, bool reversed = false,
            std::experimental::source_location sl = std::experimental::source_location::current()) {
        return check(std::move(ranges), std::move(key), std::move(output_ranges), true, sl);
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
    check(
            { {incl{1, 6}, excl{2, 3}}, {incl{3}, excl{4}}, {incl{7, 9}, incl{999, 0}} },
            {1, 0},
            { {incl{1, 6}, excl{2, 3}}, {incl{3}, excl{4}}, {incl{7, 9}, incl{999, 0}} });

    // (2)
    check(
            { {incl{1, 6}, excl{2, 3}}, {incl{3}, excl{4}}, {incl{7, 9}, incl{999, 0}} },
            {1, 6},
            { {excl{1, 6}, excl{2, 3}}, {incl{3}, excl{4}}, {incl{7, 9}, incl{999, 0}} });

    // (2) - prefix
    check(
            { {incl{1, 6}, excl{2, 3}}, {incl{3}, excl{4}}, {incl{7, 9}, incl{999, 0}} },
            {3, 6},
            { {excl{3, 6}, excl{4}}, {incl{7, 9}, incl{999, 0}} });

    // (3)
    check(
            { {incl{1, 6}, excl{2, 3}}, {excl{2, 3}, incl{2, 4}}, {incl{3}, excl{4}}, {incl{7, 9}, incl{999, 0}} },
            {2, 3},
            { {excl{2, 3}, incl{2, 4}}, {incl{3}, excl{4}}, {incl{7, 9}, incl{999, 0}} });

    // (3) - prefix
    check(
            { {incl{1, 6}, excl{2, 3}}, {excl{2, 3}, incl{2, 4}}, {excl{3}, excl{4}}, {incl{7, 9}, incl{999, 0}} },
            {3, 7},
            { {excl{3}, excl{4}}, {incl{7, 9}, incl{999, 0}} });

    // (4)
    check(
            { {incl{1, 6}, excl{2, 3}}, {incl{3}, excl{4}}, {incl{7, 9}, incl{999, 0}} },
            {2, 0},
            { {excl{2, 0}, excl{2, 3}}, {incl{3}, excl{4}}, {incl{7, 9}, incl{999, 0}} });

    // (5)
    check(
            { {incl{1, 6}, excl{2, 3}}, {incl{3}, excl{4}}, {incl{7, 9}, incl{999, 0}} },
            {90, 90},
            { {excl{90, 90}, incl{999, 0}} });

    // (6)
    check(
            { {incl{1, 6}, excl{2, 3}}, {incl{3}, excl{4}}, {incl{7, 9}, inf{}} },
            {90, 90},
            { {excl{90, 90}, inf{}} });

    // (7)
    check(
            { {incl{1, 6}, incl{2, 3}}, {incl{3}, excl{4}}, {incl{7, 9}, excl{999, 0}} },
            {2, 3},
            { {incl{3}, excl{4}}, {incl{7, 9}, excl{999, 0}} });

    // (7) - prefix
    check(
            { {incl{1, 6}, incl{2, 3}}, {incl{3}, incl{4}}, {incl{7, 9}, excl{999, 0}} },
            {4, 39},
            { {excl{4, 39}, incl{4}}, {incl{7, 9}, excl{999, 0}} });

    // (8)
    check(
            { {incl{1, 6}, excl{2, 3}}, {incl{3}, excl{4}}, {incl{7, 9}, excl{999, 0}} },
            {2, 3},
            { {incl{3}, excl{4}}, {incl{7, 9}, excl{999, 0}} });

    // (8) - prefix
    check(
            { {incl{1, 6}, incl{2, 3}}, {incl{3}, excl{4}}, {incl{7, 9}, excl{999, 0}} },
            {4, 11},
            { {incl{7, 9}, excl{999, 0}} });

    // (9)
    check(
            { {incl{1, 6}, excl{2, 3}}, {incl{3}, excl{4}}, {incl{7, 9}, excl{999, 0}} },
            {2, 4},
            { {incl{3}, excl{4}}, {incl{7, 9}, excl{999, 0}} });

    // (10)
    check(
            { {inf{}, inf{}} },
            {7, 9},
            { {excl{7, 9}, inf{}} });

    // (11)
    check(
            { {incl{10, 10}, excl{10, 30}} },
            {10},
            { {incl{10, 10}, excl{10, 30}} });

    // (12)
    check(
            { {incl{10}, excl{10, 30}} },
            {10},
            { {incl{10, null{}}, excl{10, 30}} });

    // (13)
    check(
            { {incl{9, 10}, excl{10, 30}} },
            {10},
            { {incl{10, null{}}, excl{10, 30}} });

    // (14)
    check(
            { {incl{9, 10}, excl{10, 30}} },
            {11},
            { });

    // In reversed now

    // (1)
    check_reversed(
            { {incl{1, 6}, excl{2, 3}}, {incl{3}, excl{4}}, {incl{7, 9}, incl{999, 0}} },
            {999, 1},
            { {incl{1, 6}, excl{2, 3}}, {incl{3}, excl{4}}, {incl{7, 9}, incl{999, 0}} });

    // (2)
    check_reversed(
            { {incl{1, 6}, excl{2, 3}}, {excl{2, 3}, incl{2, 4}}, {incl{3}, excl{4}}, {incl{7, 9}, incl{999, 0}} },
            {2, 4},
            { {incl{1, 6}, excl{2, 3}}, {excl{2, 3}, excl{2, 4}} });

    // (2) - prefix
    check_reversed(
            { {incl{1, 6}, excl{2, 3}}, {excl{2, 3}, incl{2, 4}}, {incl{3}, incl{4}}, {incl{7, 9}, incl{999, 0}} },
            {4, 43453},
            { {incl{1, 6}, excl{2, 3}}, {excl{2, 3}, incl{2, 4}}, {incl{3}, excl{4, 43453}} });

    // (3)
    check_reversed(
            { {incl{1, 6}, excl{2, 3}}, {incl{3}, excl{4}}, {incl{7, 9}, incl{999, 0}} },
            {2, 3},
            { {incl{1, 6}, excl{2, 3}} });

    // (3) - prefix
    check_reversed(
            { {incl{1, 6}, excl{2, 3}}, {incl{3}, excl{4}}, {incl{7, 9}, incl{999, 0}} },
            {4, 3},
            { {incl{1, 6}, excl{2, 3}}, {incl{3}, excl{4}} });

    // (4)
    check_reversed(
            { {incl{1, 6}, excl{2, 3}}, {incl{3}, excl{4}}, {incl{7, 9}, excl{999, 0}} },
            {8, 0},
            { {incl{1, 6}, excl{2, 3}}, {incl{3}, excl{4}}, {incl{7, 9}, excl{8, 0}} });

    // (5)
    check_reversed(
            { {incl{1, 6}, excl{2, 3}}, {incl{3}, excl{4}}, {incl{7, 9}, incl{999, 0}} },
            {90, 90},
            { {incl{1, 6}, excl{2, 3}}, {incl{3}, excl{4}}, {incl{7, 9}, excl{90, 90}} });

    // (6)
    check_reversed(
            { {inf{}, excl{2, 3}}, {incl{3}, excl{4}}, {incl{7, 9}, inf{}} },
            {1, 90},
            { {inf{}, excl{1, 90}} });

    // (7)
    check_reversed(
            { {incl{1, 6}, incl{2, 3}}, {incl{3}, excl{4}}, {incl{7, 9}, excl{999, 0}} },
            {7, 9},
            { {incl{1, 6}, incl{2, 3}}, {incl{3}, excl{4}} });

    // (7) - prefix
    check_reversed(
            { {incl{1, 6}, incl{2, 3}}, {incl{3}, excl{4}}, {incl{7, 9}, excl{999, 0}} },
            {3, 673},
            { {incl{1, 6}, incl{2, 3}}, {incl{3}, excl{3, 673}} });

    // (8)
    check_reversed(
            { {excl{1, 6}, excl{2, 3}}, {incl{3}, excl{4}}, {incl{7, 9}, excl{999, 0}} },
            {1, 6},
            { });

    // (8) - prefix
    check_reversed(
            { {incl{1, 6}, incl{2, 3}}, {excl{3}, excl{4}}, {incl{7, 9}, excl{999, 0}} },
            {3, 673},
            { {incl{1, 6}, incl{2, 3}} });

    // (9)
    check_reversed(
            { {incl{1, 6}, excl{2, 3}}, {incl{3}, excl{4}}, {incl{7, 9}, excl{999, 0}} },
            {0, 4},
            {});

    // (10)
    check_reversed(
            { {inf{}, inf{}} },
            {7, 9},
            { {inf{}, excl{7, 9}} });

    // (11)
    check_reversed(
            { {incl{10, 10}, excl{10, 30}} },
            {11},
            { {incl{10, 10}, excl{10, 30}} });

    // (12)
    check_reversed(
            { {excl{9, 39}, incl{10}} },
            {10},
            { {excl{9, 39}, incl{10, null{}}} });

    // (13)
    check_reversed(
            { {incl{9, 10}, incl{10, 30}} },
            {10},
            { {incl{9, 10}, incl{10, null{}}} });

    // (14)
    check_reversed(
            { {incl{9, 10}, excl{10, 30}} },
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

    do_with_cql_env([] (cql_test_env& env) -> future<> {
        std::vector<std::atomic<bool>> shards_touched(smp::count);
        simple_schema s;
        auto factory = [&shards_touched] (
                schema_ptr s,
                const dht::partition_range& range,
                const query::partition_slice& slice,
                const io_priority_class& pc,
                tracing::trace_state_ptr trace_state,
                mutation_reader::forwarding fwd_mr) {
            shards_touched[engine().cpu_id()] = true;
            return make_empty_flat_reader(s);
        };

        assert_that(make_multishard_combining_reader(
                    seastar::make_shared<test_reader_lifecycle_policy>(std::move(factory)),
                    s.schema(),
                    query::full_partition_range,
                    s.schema()->full_slice(),
                    service::get_local_sstable_query_read_priority()))
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
class puppet_reader : public flat_mutation_reader::impl {
public:
    struct control {
        promise<> buffer_filled;
        bool destroyed = false;
        bool pending = false;
    };

    enum class fill_buffer_action {
        fill,
        block
    };

private:
    simple_schema _s;
    control& _ctrl;
    fill_buffer_action _action;
    std::vector<uint32_t> _pkeys;
    unsigned _partition_index = 0;

    bool maybe_push_next_partition() {
        if (_partition_index == _pkeys.size()) {
            _end_of_stream = true;
            return false;
        }
        push_mutation_fragment(partition_start(_s.make_pkey(_pkeys.at(_partition_index++)), {}));
        return true;
    }

    void do_fill_buffer() {
        auto ck = uint32_t(0);
        while (!is_buffer_full()) {
            push_mutation_fragment(_s.make_row(_s.make_ckey(ck++), make_random_string(2 << 5)));
        }

        push_mutation_fragment(partition_end());
        maybe_push_next_partition();
    }

public:
    puppet_reader(simple_schema s, control& ctrl, fill_buffer_action action, std::vector<uint32_t> pkeys)
        : impl(s.schema())
        , _s(std::move(s))
        , _ctrl(ctrl)
        , _action(action)
        , _pkeys(std::move(pkeys)) {
        if (maybe_push_next_partition()) {
            push_mutation_fragment(_s.make_row(_s.make_ckey(0), make_random_string(4)));
            push_mutation_fragment(partition_end());
        }
        maybe_push_next_partition();
    }
    ~puppet_reader() {
        _ctrl.destroyed = true;
    }

    virtual future<> fill_buffer(db::timeout_clock::time_point) override {
        if (is_end_of_stream() || !is_buffer_empty()) {
            return make_ready_future<>();
        }

        _end_of_stream = true;

        switch (_action) {
        case fill_buffer_action::fill:
            do_fill_buffer();
            return make_ready_future<>();
        case fill_buffer_action::block:
            do_fill_buffer();
            return _ctrl.buffer_filled.get_future().then([this] {
                BOOST_REQUIRE(!_ctrl.destroyed);
                return make_ready_future<>();
            });
        }
        abort();
    }
    virtual void next_partition() override { }
    virtual future<> fast_forward_to(const dht::partition_range&, db::timeout_clock::time_point) override { throw std::bad_function_call(); }
    virtual future<> fast_forward_to(position_range, db::timeout_clock::time_point) override { throw std::bad_function_call(); }
};

// Test a background pending read-ahead outliving the reader.
//
// Foreign reader launches a new background read-ahead (fill_buffer()) after
// each remote operation (fill_buffer() and fast_forward_to()) is completed.
// This read-ahead executes on the background and is only synchronized with
// when a next remote operation is executed. If the reader is destroyed before
// this synchronization can happen then the remote read-ahead will outlive its
// owner. Check that when this happens the orphan read-ahead will terminate
// gracefully and will not cause any memory errors.
//
// Theory of operation:
// 1) Call foreign_reader::fill_buffer() -> will start read-ahead in the
//    background;
// 2) [shard 1] puppet_reader blocks the read-ahead;
// 3) Destroy foreign_reader;
// 4) Unblock read-ahead -> the now orphan read-ahead fiber executes;
//
// Best run with smp >= 2
SEASTAR_THREAD_TEST_CASE(test_foreign_reader_destroyed_with_pending_read_ahead) {
    if (smp::count < 2) {
        std::cerr << "Cannot run test " << get_name() << " with smp::count < 2" << std::endl;
        return;
    }

    do_with_cql_env([] (cql_test_env& env) -> future<> {
        const auto shard_of_interest = (engine().cpu_id() + 1) % smp::count;
        auto s = simple_schema();
        auto [remote_control, remote_reader] = smp::submit_to(shard_of_interest, [gs = global_simple_schema(s)] {
            using control_type = foreign_ptr<std::unique_ptr<puppet_reader::control>>;
            using reader_type = foreign_ptr<std::unique_ptr<flat_mutation_reader>>;

            auto control = make_foreign(std::make_unique<puppet_reader::control>());
            auto reader = make_foreign(std::make_unique<flat_mutation_reader>(make_flat_mutation_reader<puppet_reader>(gs.get(),
                    *control,
                    puppet_reader::fill_buffer_action::block,
                    std::vector<uint32_t>{0, 1})));

            return make_ready_future<std::tuple<control_type, reader_type>>(std::tuple(std::move(control), std::move(reader)));
        }).get0();

        {
            auto reader = make_foreign_reader(s.schema(), std::move(remote_reader));

            reader.fill_buffer(db::no_timeout).get();

            BOOST_REQUIRE(!reader.is_buffer_empty());
        }

        BOOST_REQUIRE(!smp::submit_to(shard_of_interest, [remote_control = remote_control.get()] {
            return remote_control->destroyed;
        }).get0());

        smp::submit_to(shard_of_interest, [remote_control = remote_control.get()] {
            remote_control->buffer_filled.set_value();
        }).get0();

        BOOST_REQUIRE(eventually_true([&] {
            return smp::submit_to(shard_of_interest, [remote_control = remote_control.get()] {
                return remote_control->destroyed;
            }).get0();
        }));

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
// destoyed objects causing memory errors.
//
// Theory of operation:
// 1) First read a partition from each shard in turn;
// 2) [shard 1] puppet reader's buffer is empty -> increase concurrency to 2
//    because we traversed to another shard in the same fill_buffer() call;
// 3) [shard 2] puppet reader -> read-ahead launched in the background but it's
//    blocked;
// 4) Reader is destroyed;
// 5) Resume the shard 2's puppet reader -> the now orphan read-ahead fiber
//    executes;
//
// Best run with smp >= 2
SEASTAR_THREAD_TEST_CASE(test_multishard_combining_reader_destroyed_with_pending_read_ahead) {
    if (smp::count < 2) {
        std::cerr << "Cannot run test " << get_name() << " with smp::count < 2" << std::endl;
        return;
    }

    do_with_cql_env([] (cql_test_env& env) -> future<> {
        auto remote_controls = std::vector<foreign_ptr<std::unique_ptr<puppet_reader::control>>>();
        remote_controls.reserve(smp::count);
        for (unsigned i = 0; i < smp::count; ++i) {
            remote_controls.emplace_back(nullptr);
        }

        parallel_for_each(boost::irange(0u, smp::count), [&remote_controls] (unsigned shard) mutable {
            return smp::submit_to(shard, [] {
                return make_foreign(std::make_unique<puppet_reader::control>());
            }).then([shard, &remote_controls] (foreign_ptr<std::unique_ptr<puppet_reader::control>>&& ctr) mutable {
                remote_controls[shard] = std::move(ctr);
            });
        }).get();

        auto s = simple_schema();

        // We need two tokens for each shard
        std::map<dht::token, unsigned> pkeys_by_tokens;
        for (unsigned i = 0; i < smp::count * 2; ++i) {
            pkeys_by_tokens.emplace(s.make_pkey(i).token(), i);
        }

        auto shard_pkeys = std::vector<std::vector<uint32_t>>(smp::count, std::vector<uint32_t>{});
        auto i = unsigned(0);
        for (auto pkey : pkeys_by_tokens | boost::adaptors::map_values) {
            shard_pkeys[i++ % smp::count].push_back(pkey);
        }

        auto partitioner = dummy_partitioner(s.schema()->get_partitioner(), std::move(pkeys_by_tokens));

        auto factory = [gs = global_simple_schema(s), &remote_controls, &shard_pkeys] (
                schema_ptr,
                const dht::partition_range& range,
                const query::partition_slice& slice,
                const io_priority_class& pc,
                tracing::trace_state_ptr trace_state,
                mutation_reader::forwarding) mutable {
            const auto shard = engine().cpu_id();
            auto action = shard == 0 ? puppet_reader::fill_buffer_action::fill : puppet_reader::fill_buffer_action::block;
            return make_flat_mutation_reader<puppet_reader>(gs.get(), *remote_controls.at(shard), action, shard_pkeys.at(shard));
        };

        {
            auto schema = schema_builder(s.schema()).with_partitioner_for_tests_only(partitioner).build();
            auto reader = make_multishard_combining_reader(seastar::make_shared<test_reader_lifecycle_policy>(std::move(factory)),
                    schema, query::full_partition_range, schema->full_slice(), service::get_local_sstable_query_read_priority());
            reader.fill_buffer(db::no_timeout).get();
            BOOST_REQUIRE(reader.is_buffer_full());
        }

        parallel_for_each(boost::irange(0u, smp::count), [&remote_controls] (unsigned shard) mutable {
            return smp::submit_to(shard, [control = remote_controls.at(shard).get()] {
                control->buffer_filled.set_value();
            });
        }).get();

        BOOST_REQUIRE(eventually_true([&] {
            return map_reduce(boost::irange(0u, smp::count), [&] (unsigned shard) {
                    return smp::submit_to(shard, [&remote_controls, shard] {
                        return remote_controls.at(shard)->destroyed;
                    });
                },
                true,
                std::logical_and<bool>()).get0();
        }));

        return make_ready_future<>();
    }).get();
}

SEASTAR_THREAD_TEST_CASE(test_multishard_combining_reader_next_partition) {
    do_with_cql_env([] (cql_test_env& env) -> future<> {
        env.execute_cql("CREATE KEYSPACE multishard_combining_reader_next_partition_ks"
                " WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1};").get();
        env.execute_cql("CREATE TABLE multishard_combining_reader_next_partition_ks.test (pk int, v int, PRIMARY KEY(pk));").get();

        const auto insert_id = env.prepare("INSERT INTO multishard_combining_reader_next_partition_ks.test (\"pk\", \"v\") VALUES (?, ?);").get0();

        const auto partition_count = 1000;

        for (int pk = 0; pk < partition_count; ++pk) {
            env.execute_prepared(insert_id, {{
                    cql3::raw_value::make_value(serialized(pk)),
                    cql3::raw_value::make_value(serialized(0))}}).get();
        }

        auto schema = env.local_db().find_column_family("multishard_combining_reader_next_partition_ks", "test").schema();
        auto& partitioner = schema->get_partitioner();

        auto pkeys = boost::copy_range<std::vector<dht::decorated_key>>(
                boost::irange(0, partition_count) |
                boost::adaptors::transformed([schema, &partitioner] (int i) {
                    return partitioner.decorate_key(*schema, partition_key::from_singular(*schema, i));
                }));

        // We want to test corner cases around next_partition() called when it
        // cannot be resolved with just the buffer and it has to be forwarded
        // to the correct shard reader, so set a buffer size so that only a
        // single fragment can fit into it at a time.
        const auto max_buffer_size = size_t{1};

        auto factory = [db = &env.db(), max_buffer_size] (
                schema_ptr schema,
                const dht::partition_range& range,
                const query::partition_slice& slice,
                const io_priority_class& pc,
                tracing::trace_state_ptr trace_state,
                mutation_reader::forwarding fwd_mr) {
            auto& table = db->local().find_column_family(schema);
            auto reader = table.as_mutation_source().make_reader(
                    schema,
                    no_reader_permit(),
                    range,
                    slice,
                    service::get_local_sstable_query_read_priority(),
                    std::move(trace_state),
                    streamed_mutation::forwarding::no,
                    fwd_mr);
            reader.set_max_buffer_size(max_buffer_size);
            return reader;
        };
        auto reader = make_multishard_combining_reader(
                seastar::make_shared<test_reader_lifecycle_policy>(std::move(factory)),
                schema,
                query::full_partition_range,
                schema->full_slice(),
                service::get_local_sstable_query_read_priority());

        reader.set_max_buffer_size(max_buffer_size);

        boost::sort(pkeys, [schema] (const dht::decorated_key& a, const dht::decorated_key& b) {
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

namespace {

std::deque<mutation_fragment> make_fragments_with_non_monotonic_positions(simple_schema& s, dht::decorated_key pkey, size_t max_buffer_size,
        gc_clock::time_point tombstone_deletion_time) {
    std::deque<mutation_fragment> fragments;

    fragments.emplace_back(partition_start{std::move(pkey), {}});

    int i = 0;
    size_t mem_usage = fragments.back().memory_usage(*s.schema());
    while (mem_usage <= max_buffer_size * 2) {
        fragments.emplace_back(s.make_range_tombstone(query::clustering_range::make(s.make_ckey(0), s.make_ckey(i + 1)), tombstone_deletion_time));
        mem_usage += fragments.back().memory_usage(*s.schema());
        ++i;
    }

    fragments.emplace_back(s.make_row(s.make_ckey(0), "v"));

    return fragments;
}

} // anonymous namespace

// Test that the multishard reader will not skip any rows when the position of
// the mutation fragments in the stream is not strictly monotonous.
// See the explanation in `shard_reader::remote_reader::fill_buffer()`.
// To test this we need to craft a mutation such that after the first
// `fill_buffer()` call, as well as after the second one, the last fragment is a
// range tombstone, with the very same position-in-partition.
// This is to check that the reader will not skip the row after the
// range-tombstones and that it can make progress (doesn't assume that an additional
// fill buffer call will bring in a fragment with a higher position).
SEASTAR_THREAD_TEST_CASE(test_multishard_combining_reader_non_strictly_monotonic_positions) {
    const size_t max_buffer_size = 512;
    const int pk = 0;
    const auto tombstone_deletion_time = gc_clock::now();
    simple_schema s;

    // Validate that the generated fragments are fit for the very strict
    // requirement of this test.
    // The test is meaningless if these requirements are not met.
    {
        auto fragments = make_fragments_with_non_monotonic_positions(s, s.make_pkey(pk), max_buffer_size, tombstone_deletion_time);
        auto rd = make_flat_mutation_reader_from_fragments(s.schema(), std::move(fragments));
        rd.set_max_buffer_size(max_buffer_size);

        rd.fill_buffer(db::no_timeout).get();

        auto mf = rd.pop_mutation_fragment();
        BOOST_REQUIRE_EQUAL(mf.mutation_fragment_kind(), mutation_fragment::kind::partition_start);

        mf = rd.pop_mutation_fragment();
        BOOST_REQUIRE_EQUAL(mf.mutation_fragment_kind(), mutation_fragment::kind::range_tombstone);
        const auto ckey = mf.as_range_tombstone().start;

        while (!rd.is_buffer_empty()) {
            mf = rd.pop_mutation_fragment();
            BOOST_REQUIRE_EQUAL(mf.mutation_fragment_kind(), mutation_fragment::kind::range_tombstone);
            BOOST_REQUIRE(mf.as_range_tombstone().start.equal(*s.schema(), ckey));
        }

        rd.fill_buffer(db::no_timeout).get();

        while (!rd.is_buffer_empty()) {
            mf = rd.pop_mutation_fragment();
            BOOST_REQUIRE_EQUAL(mf.mutation_fragment_kind(), mutation_fragment::kind::range_tombstone);
            BOOST_REQUIRE(mf.as_range_tombstone().start.equal(*s.schema(), ckey));
        }

        rd.fill_buffer(db::no_timeout).get();

        BOOST_REQUIRE(!rd.is_buffer_empty());

        mf = rd.pop_mutation_fragment();
        BOOST_REQUIRE_EQUAL(mf.mutation_fragment_kind(), mutation_fragment::kind::clustering_row);
        BOOST_REQUIRE(mf.as_clustering_row().key().equal(*s.schema(), ckey));
    }

    do_with_cql_env([=, s = std::move(s)] (cql_test_env& env) mutable -> future<> {
        auto factory = [=, gs = global_simple_schema(s)] (
                schema_ptr,
                const dht::partition_range& range,
                const query::partition_slice& slice,
                const io_priority_class& pc,
                tracing::trace_state_ptr trace_state,
                mutation_reader::forwarding fwd_mr) {
            auto s = gs.get();
            auto pkey = s.make_pkey(pk);
            if (s.schema()->get_partitioner().shard_of(pkey.token()) != engine().cpu_id()) {
                return make_empty_flat_reader(s.schema());
            }
            auto fragments = make_fragments_with_non_monotonic_positions(s, std::move(pkey), max_buffer_size, tombstone_deletion_time);
            auto rd = make_flat_mutation_reader_from_fragments(s.schema(), std::move(fragments), range, slice);
            rd.set_max_buffer_size(max_buffer_size);
            return rd;
        };

        auto fragments = make_fragments_with_non_monotonic_positions(s, s.make_pkey(pk), max_buffer_size, tombstone_deletion_time);
        auto rd = make_flat_mutation_reader_from_fragments(s.schema(), std::move(fragments));
        auto mut_opt = read_mutation_from_flat_mutation_reader(rd, db::no_timeout).get0();
        BOOST_REQUIRE(mut_opt);

        assert_that(make_multishard_combining_reader(
                    seastar::make_shared<test_reader_lifecycle_policy>(std::move(factory), true),
                    s.schema(),
                    query::full_partition_range,
                    s.schema()->full_slice(),
                    service::get_local_sstable_query_read_priority()))
                .produces_partition(*mut_opt);

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

    do_with_cql_env([] (cql_test_env& env) -> future<> {
        env.execute_cql("CREATE KEYSPACE multishard_streaming_reader_ks WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1};").get();
        env.execute_cql("CREATE TABLE multishard_streaming_reader_ks.test (pk int, v int, PRIMARY KEY(pk));").get();

        const auto insert_id = env.prepare("INSERT INTO multishard_streaming_reader_ks.test (\"pk\", \"v\") VALUES (?, ?);").get0();

        const auto partition_count = 10000;

        for (int pk = 0; pk < partition_count; ++pk) {
            env.execute_prepared(insert_id, {{
                    cql3::raw_value::make_value(serialized(pk)),
                    cql3::raw_value::make_value(serialized(0))}}).get();
        }

        auto schema = env.local_db().find_column_family("multishard_streaming_reader_ks", "test").schema();

        auto token_range = dht::token_range::make_open_ended_both_sides();
        auto partition_range = dht::to_partition_range(token_range);

        auto& local_partitioner = schema->get_partitioner();
        auto remote_partitioner_ptr = create_object<dht::i_partitioner, const unsigned&, const unsigned&>(local_partitioner.name(),
                local_partitioner.shard_count() - 1, local_partitioner.sharding_ignore_msb());
        auto& remote_partitioner = *remote_partitioner_ptr;

        auto tested_reader = make_multishard_streaming_reader(env.db(), schema,
                [sharder = dht::selective_token_range_sharder(remote_partitioner, token_range, 0)] () mutable -> std::optional<dht::partition_range> {
            if (auto next = sharder.next()) {
                return dht::to_partition_range(*next);
            }
            return std::nullopt;
        });

        auto reader_factory = [db = &env.db()] (
                schema_ptr s,
                const dht::partition_range& range,
                const query::partition_slice& slice,
                const io_priority_class& pc,
                tracing::trace_state_ptr trace_state,
                mutation_reader::forwarding fwd_mr) mutable {
            auto& table = db->local().find_column_family(s);
            return table.as_mutation_source().make_reader(std::move(s), no_reader_permit(), range, slice, pc, std::move(trace_state),
                    streamed_mutation::forwarding::no, fwd_mr);
        };
        auto reference_reader = make_filtering_reader(
                make_multishard_combining_reader(seastar::make_shared<test_reader_lifecycle_policy>(std::move(reader_factory)),
                    schema, partition_range, schema->full_slice(), service::get_local_sstable_query_read_priority()),
                [&remote_partitioner] (const dht::decorated_key& pkey) {
                    return remote_partitioner.shard_of(pkey.token()) == 0;
                });

        std::vector<mutation> reference_muts;
        while (auto mut_opt = read_mutation_from_flat_mutation_reader(reference_reader, db::no_timeout).get0()) {
            reference_muts.push_back(std::move(*mut_opt));
        }

        std::vector<mutation> tested_muts;
        while (auto mut_opt = read_mutation_from_flat_mutation_reader(tested_reader, db::no_timeout).get0()) {
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
    auto gen = random_mutation_generator(random_mutation_generator::generate_counters::no);

    const auto expected_muts = gen(20);

    // Simultaneous read and write
    {
        auto read_all = [] (flat_mutation_reader& reader, std::vector<mutation>& muts) {
            return async([&reader, &muts] {
                while (auto mut_opt = read_mutation_from_flat_mutation_reader(reader, db::no_timeout).get0()) {
                    muts.emplace_back(std::move(*mut_opt));
                }
            });
        };

        auto write_all = [] (queue_reader_handle& handle, const std::vector<mutation>& muts) {
            return async([&] {
                auto reader = flat_mutation_reader_from_mutations(muts);
                while (auto mf_opt = reader(db::no_timeout).get0()) {
                    handle.push(std::move(*mf_opt)).get();
                }
                handle.push_end_of_stream();
            });
        };

        auto actual_muts = std::vector<mutation>{};
        actual_muts.reserve(20);

        auto [reader, handle] = make_queue_reader(gen.schema());

        when_all_succeed(read_all(reader, actual_muts), write_all(handle, expected_muts)).get();
        BOOST_REQUIRE_EQUAL(actual_muts.size(), expected_muts.size());
        for (size_t i = 0; i < expected_muts.size(); ++i) {
            BOOST_REQUIRE_EQUAL(actual_muts.at(i), expected_muts.at(i));
        }
    }

    // abort()
    {
        auto [reader, handle] = make_queue_reader(gen.schema());
        auto fill_buffer_fut = reader.fill_buffer(db::no_timeout);

        auto expected_reader = flat_mutation_reader_from_mutations(expected_muts);

        handle.push(std::move(*expected_reader(db::no_timeout).get0())).get();

        BOOST_REQUIRE(!fill_buffer_fut.available());

        handle.abort(std::make_exception_ptr<std::runtime_error>(std::runtime_error("error")));

        BOOST_REQUIRE_THROW(fill_buffer_fut.get(), std::runtime_error);
        BOOST_REQUIRE_THROW(handle.push(partition_end{}).get(), std::runtime_error);
    }

    // Detached handle
    {
        auto [reader, handle] = make_queue_reader(gen.schema());
        auto fill_buffer_fut = reader.fill_buffer(db::no_timeout);

        {
            auto throwaway_reader = std::move(reader);
        }

        BOOST_REQUIRE_THROW(handle.push(partition_end{}).get(), std::runtime_error);
        BOOST_REQUIRE_THROW(handle.push_end_of_stream(), std::runtime_error);
    }

    // Abandoned handle aborts, move-assignment
    {
        auto [reader, handle] = make_queue_reader(gen.schema());
        auto fill_buffer_fut = reader.fill_buffer(db::no_timeout);

        auto expected_reader = flat_mutation_reader_from_mutations(expected_muts);

        handle.push(std::move(*expected_reader(db::no_timeout).get0())).get();

        BOOST_REQUIRE(!fill_buffer_fut.available());

        {
            auto [throwaway_reader, throwaway_handle] = make_queue_reader(gen.schema());
            // Overwrite handle
            handle = std::move(throwaway_handle);
        }

        BOOST_REQUIRE_THROW(fill_buffer_fut.get(), std::runtime_error);
        BOOST_REQUIRE_THROW(handle.push(partition_end{}).get(), std::runtime_error);
    }

    // Abandoned handle aborts, destructor
    {
        auto [reader, handle] = make_queue_reader(gen.schema());
        auto fill_buffer_fut = reader.fill_buffer(db::no_timeout);

        auto expected_reader = flat_mutation_reader_from_mutations(expected_muts);

        handle.push(std::move(*expected_reader(db::no_timeout).get0())).get();

        BOOST_REQUIRE(!fill_buffer_fut.available());

        {
            // Destroy handle
            queue_reader_handle throwaway_handle(std::move(handle));
        }

        BOOST_REQUIRE_THROW(fill_buffer_fut.get(), std::runtime_error);
        BOOST_REQUIRE_THROW(handle.push(partition_end{}).get(), std::runtime_error);
    }

    // Life-cycle, relies on ASAN for error reporting
    {
        auto [reader, handle] = make_queue_reader(gen.schema());
        {
            auto [throwaway_reader, throwaway_handle] = make_queue_reader(gen.schema());
            // Overwrite handle
            handle = std::move(throwaway_handle);

            auto [another_throwaway_reader, another_throwaway_handle] = make_queue_reader(gen.schema());
            // Overwrite with moved-from handle (move assignment operator)
            another_throwaway_handle = std::move(throwaway_handle);

            // Overwrite with moved-from handle (move constructor)
            queue_reader_handle yet_another_throwaway_handle(std::move(throwaway_handle));
        }
    }

    // push_end_of_stream() detaches handle from reader, relies on ASAN for error reporting
    {
        auto [reader, handle] = make_queue_reader(gen.schema());
        {
            auto throwaway_handle = std::move(handle);
            throwaway_handle.push_end_of_stream();
        }
    }
}

SEASTAR_THREAD_TEST_CASE(test_compacting_reader_as_mutation_source) {
    auto make_populate = [] (bool single_fragment_buffer) {
        return [single_fragment_buffer] (schema_ptr s, const std::vector<mutation>& mutations, gc_clock::time_point query_time) mutable {
            auto mt = make_lw_shared<memtable>(s);
            for (auto& mut : mutations) {
                mt->apply(mut);
            }
            return mutation_source([=] (
                    schema_ptr s,
                    reader_permit,
                    const dht::partition_range& range,
                    const query::partition_slice& slice,
                    const io_priority_class& pc,
                    tracing::trace_state_ptr trace_state,
                    streamed_mutation::forwarding fwd_sm,
                    mutation_reader::forwarding fwd_mr) mutable {
                auto source = mt->make_flat_reader(s, range, slice, pc, std::move(trace_state), streamed_mutation::forwarding::no, fwd_mr);
                auto mr = make_compacting_reader(std::move(source), query_time, [] (const dht::decorated_key&) { return api::min_timestamp; });
                if (single_fragment_buffer) {
                    mr.set_max_buffer_size(1);
                }
                if (fwd_sm == streamed_mutation::forwarding::yes) {
                    return make_forwardable(std::move(mr));
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
    std::deque<mutation_fragment> expected;

    auto mr = [&] () {
        const size_t buffer_size = 1024;
        std::deque<mutation_fragment> mfs;
        auto dk0 = ss.make_pkey(0);
        auto dk1 = ss.make_pkey(1);

        mfs.emplace_back(partition_start(dk0, tombstone{}));

        auto i = 0;
        size_t mfs_size = 0;
        while (mfs_size <= buffer_size) {
            mfs.emplace_back(ss.make_row(ss.make_ckey(i++), "v"));
            mfs_size += mfs.back().memory_usage(schema);
        }
        mfs.emplace_back(partition_end{});

        mfs.emplace_back(partition_start(dk1, tombstone{}));
        mfs.emplace_back(ss.make_row(ss.make_ckey(0), "v"));
        mfs.emplace_back(partition_end{});

        for (const auto& mf : mfs) {
            expected.emplace_back(*ss.schema(), mf);
        }

        auto mr = make_compacting_reader(make_flat_mutation_reader_from_fragments(ss.schema(), std::move(mfs)),
                gc_clock::now(), [] (const dht::decorated_key&) { return api::min_timestamp; });
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
