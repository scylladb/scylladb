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

#include <boost/test/unit_test.hpp>
#include <boost/range/irange.hpp>
#include <boost/range/adaptor/uniqued.hpp>

#include "core/sleep.hh"
#include "core/do_with.hh"
#include "core/thread.hh"

#include "tests/test-utils.hh"
#include "tests/mutation_assertions.hh"
#include "tests/flat_mutation_reader_assertions.hh"
#include "tests/tmpdir.hh"
#include "tests/sstable_utils.hh"
#include "tests/simple_schema.hh"
#include "tests/test_services.hh"
#include "tests/mutation_source_test.hh"
#include "tests/cql_test_env.hh"
#include "tests/make_random_string.hh"

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

static mutation make_mutation_with_key(schema_ptr s, dht::decorated_key dk) {
    mutation m(s, std::move(dk));
    m.set_clustered_cell(clustering_key::make_empty(), "v", data_value(bytes("v1")), 1);
    return m;
}

static mutation make_mutation_with_key(schema_ptr s, const char* key) {
    return make_mutation_with_key(s, dht::global_partitioner().decorate_key(*s, partition_key::from_single_value(*s, bytes(key))));
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
            return dht::global_partitioner().decorate_key(*s, std::move(pk));
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

SEASTAR_TEST_CASE(test_sm_fast_forwarding_combining_reader) {
    return seastar::async([] {
        storage_service_for_tests ssft;
        simple_schema s;

        const auto pkeys = s.make_pkeys(4);
        const auto ckeys = s.make_ckeys(4);

        auto make_mutation = [&] (uint32_t n) {
            mutation m(s.schema(), pkeys[n]);

            int i{0};
            s.add_row(m, ckeys[i], sprint("val_%i", i));
            ++i;
            s.add_row(m, ckeys[i], sprint("val_%i", i));
            ++i;
            s.add_row(m, ckeys[i], sprint("val_%i", i));
            ++i;
            s.add_row(m, ckeys[i], sprint("val_%i", i));

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

struct sst_factory {
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
        auto sst = sstables::make_sstable(s, path, gen, sstables::sstable::version_types::la, sstables::sstable::format_types::big);
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
            s.add_row(mut, ckeys[ckey_index], sprint("%s_%i_val", value_prefix, ckey_index));

            if (static_row) {
                s.add_static_row(mut, sprint("%s_static_val", value_prefix));
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

    auto tmp = make_lw_shared<tmpdir>();

    unsigned gen{0};
    std::vector<sstables::shared_sstable> sstable_list = {
            make_sstable_containing(sst_factory(s.schema(), tmp->path, ++gen, 0), std::move(sstable_level_0_0_mutations)),
            make_sstable_containing(sst_factory(s.schema(), tmp->path, ++gen, 1), std::move(sstable_level_1_0_mutations)),
            make_sstable_containing(sst_factory(s.schema(), tmp->path, ++gen, 1), std::move(sstable_level_1_1_mutations)),
            make_sstable_containing(sst_factory(s.schema(), tmp->path, ++gen, 2), std::move(sstable_level_2_0_mutations)),
            make_sstable_containing(sst_factory(s.schema(), tmp->path, ++gen, 2), std::move(sstable_level_2_1_mutations)),
    };

    auto cs = sstables::make_compaction_strategy(sstables::compaction_strategy_type::leveled, {});
    auto sstable_set = make_lw_shared<sstables::sstable_set>(cs.make_sstable_set(s.schema()));

    std::vector<flat_mutation_reader> sstable_mutation_readers;

    for (auto sst : sstable_list) {
        sstable_set->insert(sst);

        sstable_mutation_readers.emplace_back(
            sst->as_mutation_source().make_reader(
                s.schema(),
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
            sstable_set,
            query::full_partition_range,
            s.schema()->full_slice(),
            seastar::default_priority_class(),
            no_resource_tracking(),
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
    s.add_row(m, s.make_ckey(++i), sprint("val_%i", i));
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

sstables::shared_sstable create_sstable(simple_schema& sschema, const sstring& path) {
    std::vector<mutation> mutations;
    mutations.reserve(1 << 14);

    for (std::size_t p = 0; p < (1 << 10); ++p) {
        mutation m(sschema.schema(), sschema.make_pkey(p));
        sschema.add_static_row(m, sprint("%i_static_val", p));

        for (std::size_t c = 0; c < (1 << 4); ++c) {
            sschema.add_row(m, sschema.make_ckey(c), sprint("val_%i", c));
        }

        mutations.emplace_back(std::move(m));
        thread::yield();
    }

    return make_sstable_containing([&] {
            return make_lw_shared<sstables::sstable>(sschema.schema(), path, 0, sstables::sstable::version_types::la, sstables::sstable::format_types::big);
        }
        , mutations);
}

static
sstables::shared_sstable create_sstable(schema_ptr s, std::vector<mutation> mutations) {
    static thread_local auto tmp = make_lw_shared<tmpdir>();
    static int gen = 0;
    return make_sstable_containing([&] {
        return make_lw_shared<sstables::sstable>(s, tmp->path, gen++, sstables::sstable::version_types::la, sstables::sstable::format_types::big);
    }, mutations);
}

class tracking_reader : public flat_mutation_reader::impl {
    flat_mutation_reader _reader;
    std::size_t _call_count{0};
    std::size_t _ff_count{0};
public:
    tracking_reader(schema_ptr schema, lw_shared_ptr<sstables::sstable> sst, reader_resource_tracker tracker)
        : impl(schema)
        , _reader(sst->read_range_rows_flat(
                        schema,
                        query::full_partition_range,
                        schema->full_slice(),
                        default_priority_class(),
                        tracker,
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
                    const dht::partition_range&,
                    const query::partition_slice&,
                    const io_priority_class&,
                    tracing::trace_state_ptr,
                    streamed_mutation::forwarding,
                    mutation_reader::forwarding,
                    reader_resource_tracker res_tracker) {
            auto tracker_ptr = std::make_unique<tracking_reader>(std::move(schema), std::move(sst), res_tracker);
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
        reader_concurrency_semaphore semaphore(100, 4 * 1024);
        // Testing the tracker here, no need to have a base cost.
        auto permit = semaphore.wait_admission(0).get0();

        {
            reader_resource_tracker resource_tracker(permit);

            auto tracked_file = resource_tracker.track(
                    file(shared_ptr<file_impl>(make_shared<dummy_file_impl>())));

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
    return async([&] {
        storage_service_for_tests ssft;
        reader_concurrency_semaphore semaphore(2, new_reader_base_cost);

        {
            simple_schema s;
            auto tmp = make_lw_shared<tmpdir>();
            auto sst = create_sstable(s, tmp->path);

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
    return async([&] {
        storage_service_for_tests ssft;
        reader_concurrency_semaphore semaphore(2, new_reader_base_cost);

        {
            simple_schema s;
            auto tmp = make_lw_shared<tmpdir>();
            auto sst = create_sstable(s, tmp->path);

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
    return async([&] {
        storage_service_for_tests ssft;

        struct queue_overloaded_exception {};

        reader_concurrency_semaphore semaphore(2, new_reader_base_cost, 2, [] { return std::make_exception_ptr(queue_overloaded_exception()); });

        {
            simple_schema s;
            auto tmp = make_lw_shared<tmpdir>();
            auto sst = create_sstable(s, tmp->path);

            auto reader1_ptr = std::make_unique<reader_wrapper>(semaphore, s.schema(), sst);
            (*reader1_ptr)().get();

            auto reader2_ptr = std::make_unique<reader_wrapper>(semaphore, s.schema(), sst);
            auto read2_fut = (*reader2_ptr)();

            auto reader3_ptr = std::make_unique<reader_wrapper>(semaphore, s.schema(), sst);
            auto read3_fut = (*reader3_ptr)();

            auto reader4 = reader_wrapper(semaphore, s.schema(), sst);

            BOOST_REQUIRE_EQUAL(semaphore.waiters(), 2);

            // The queue should now be full.
            BOOST_REQUIRE_THROW(reader4().get(), queue_overloaded_exception);

            reader1_ptr.reset();
            read2_fut.get();
            reader2_ptr.reset();
            read3_fut.get();
        }

        REQUIRE_EVENTUALLY_EQUAL(new_reader_base_cost, semaphore.available_resources().memory);
    });
}

SEASTAR_TEST_CASE(restricted_reader_create_reader) {
    return async([&] {
        storage_service_for_tests ssft;
        reader_concurrency_semaphore semaphore(100, new_reader_base_cost);

        {
            simple_schema s;
            auto tmp = make_lw_shared<tmpdir>();
            auto sst = create_sstable(s, tmp->path);

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

static mutation compacted(const mutation& m) {
    auto result = m;
    result.partition().compact_for_compaction(*result.schema(), always_gc, gc_clock::now());
    return result;
}

SEASTAR_TEST_CASE(test_fast_forwarding_combined_reader_is_consistent_with_slicing) {
    return async([&] {
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
            mutation_source ds = create_sstable(s, muts)->as_mutation_source();
            readers.push_back(ds.make_reader(s,
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
                    BOOST_FAIL(sprint("Received clustering fragment: %s", mf));
                }
                result.partition().apply(*s, std::move(mf));
                return stop_iteration::no;
            }, db::no_timeout).get();

            for (auto&& range : ranges) {
                auto prange = position_range(range);
                rd.fast_forward_to(prange, db::no_timeout).get();
                rd.consume_pausable([&](mutation_fragment&& mf) {
                    if (!mf.relevant_for_range(*s, prange.start())) {
                        BOOST_FAIL(sprint("Received fragment which is not relevant for range: %s, range: %s", mf, prange));
                    }
                    position_in_partition::less_compare less(*s);
                    if (!less(mf.position(), prange.end())) {
                        BOOST_FAIL(sprint("Received fragment is out of range: %s, range: %s", mf, prange));
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
    return async([&] {
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

        mutation_source ds1 = create_sstable(s, {m1})->as_mutation_source();
        mutation_source ds2 = create_sstable(s, {m2})->as_mutation_source();

        // upper bound ends before the row in m2, so that the raw is fetched after next fast forward.
        auto range = ss.make_ckey_range(0, 3);

        {
            auto slice = partition_slice_builder(*s).with_range(range).build();
            readers.push_back(ds1.make_reader(s, query::full_partition_range, slice));
            readers.push_back(ds2.make_reader(s, query::full_partition_range, slice));

            auto rd = make_combined_reader(s, std::move(readers),
                streamed_mutation::forwarding::no, mutation_reader::forwarding::no);

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

        // Check fast_forward_to()
        {

            readers.push_back(ds1.make_reader(s, query::full_partition_range, s->full_slice(), default_priority_class(),
                nullptr, streamed_mutation::forwarding::yes));
            readers.push_back(ds2.make_reader(s, query::full_partition_range, s->full_slice(), default_priority_class(),
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

// Shards tokens such that tokens are owned by shards in a round-robin manner.
class dummy_partitioner : public dht::i_partitioner {
    dht::i_partitioner& _partitioner;
    std::vector<dht::token> _tokens;

public:
    // We need a container input that enforces token order by design.
    // In addition client code will often map tokens to something, e.g. mutation
    // they originate from or shards, etc. So, for convenience we allow any
    // ordered associative container (std::map) that has dht::token as keys.
    // Values will be ignored.
    template <typename T>
    dummy_partitioner(dht::i_partitioner& partitioner, const std::map<dht::token, T>& something_by_token)
        : i_partitioner(smp::count)
        , _partitioner(partitioner)
        , _tokens(boost::copy_range<std::vector<dht::token>>(something_by_token | boost::adaptors::map_keys)) {
    }

    virtual dht::token midpoint(const dht::token& left, const dht::token& right) const override { return _partitioner.midpoint(left, right); }
    virtual dht::token get_token(const schema& s, partition_key_view key) override { return _partitioner.get_token(s, key); }
    virtual dht::token get_token(const sstables::key_view& key) override { return _partitioner.get_token(key); }
    virtual sstring to_sstring(const dht::token& t) const override { return _partitioner.to_sstring(t); }
    virtual dht::token from_sstring(const sstring& t) const override { return _partitioner.from_sstring(t); }
    virtual dht::token from_bytes(bytes_view bytes) const override { return _partitioner.from_bytes(bytes); }
    virtual dht::token get_random_token() override { return _partitioner.get_random_token(); }
    virtual bool preserves_order() override { return _partitioner.preserves_order(); }
    virtual std::map<dht::token, float> describe_ownership(const std::vector<dht::token>& sorted_tokens) override { return _partitioner.describe_ownership(sorted_tokens); }
    virtual data_type get_token_validator() override { return _partitioner.get_token_validator(); }
    virtual const sstring name() const override { return _partitioner.name(); }
    virtual unsigned shard_of(const dht::token& t) const override;
    virtual dht::token token_for_next_shard(const dht::token& t, shard_id shard, unsigned spans = 1) const override;
    virtual int tri_compare(dht::token_view t1, dht::token_view t2) const override { return _partitioner.tri_compare(t1, t2); }
};

unsigned dummy_partitioner::shard_of(const dht::token& t) const {
    auto it = boost::find(_tokens, t);
    // Unknown tokens are assigned to shard 0
    return it == _tokens.end() ? 0 : std::distance(_tokens.begin(), it) % _partitioner.shard_count();
}

dht::token dummy_partitioner::token_for_next_shard(const dht::token& t, shard_id shard, unsigned spans) const {
    // Find the first token that belongs to `shard` and is larger than `t`
    auto it = std::find_if(_tokens.begin(), _tokens.end(), [this, &t, shard] (const dht::token& shard_token) {
        return shard_token > t && shard_of(shard_token) == shard;
    });

    if (it == _tokens.end()) {
        return dht::maximum_token();
    }

    --spans;

    while (spans) {
        if (std::distance(it, _tokens.end()) <= _partitioner.shard_count()) {
            return dht::maximum_token();
        }
        it += _partitioner.shard_count();
        --spans;
    }

    return *it;
}

class test_reader_lifecycle_policy
        : public reader_lifecycle_policy
        , public enable_shared_from_this<test_reader_lifecycle_policy> {
public:
    enum class operation {
        none,
        create,
        pause,
        try_resume,
    };

    using delay_function = std::function<future<>()>;

    static future<> no_delay() {
        return make_ready_future<>();
    }

private:
    using factory_function = std::function<future<foreign_ptr<std::unique_ptr<flat_mutation_reader>>>(
            shard_id,
            schema_ptr,
            const dht::partition_range&,
            const query::partition_slice&,
            const io_priority_class&,
            tracing::trace_state_ptr,
            mutation_reader::forwarding)>;

    struct reader_params {
        const dht::partition_range range;
        const query::partition_slice slice;
    };
    struct reader_context {
        std::unique_ptr<reader_params> params;
        foreign_ptr<std::unique_ptr<flat_mutation_reader>> reader;
        operation operation_in_progress = operation::none;
    };

    factory_function _factory_function;
    delay_function _delay;
    std::vector<reader_context> _contexts;
    bool _evict_paused_readers = false;

private:
    static sstring to_string(operation op) {
        switch (op) {
            case operation::none:
                return "none";
            case operation::create:
                return "create";
            case operation::pause:
                return "pause";
            case operation::try_resume:
                return "try_resume";
        }
        return "unknown";
    }

    void set_current_operation(shard_id shard, operation new_operation) {
        BOOST_CHECK_MESSAGE(_contexts[shard].operation_in_progress == operation::none,
                sprint("%s(): concurrent operation detected: `%s` initiated but `%s` is still pending", __FUNCTION__,
                        to_string(_contexts[shard].operation_in_progress), to_string(new_operation)));
        _contexts[shard].operation_in_progress = new_operation;
    }

public:
    explicit test_reader_lifecycle_policy(factory_function f, delay_function delay_func = no_delay, bool evict_paused_readers = false)
        : _factory_function(std::move(f))
        , _delay(std::move(delay_func))
        , _contexts(smp::count)
        , _evict_paused_readers(evict_paused_readers) {
    }
    virtual future<foreign_ptr<std::unique_ptr<flat_mutation_reader>>> create_reader(
            shard_id shard,
            schema_ptr schema,
            const dht::partition_range& range,
            const query::partition_slice& slice,
            const io_priority_class& pc,
            tracing::trace_state_ptr trace_state,
            mutation_reader::forwarding fwd_mr) override {
        set_current_operation(shard, operation::create);

        _contexts[shard].params = std::make_unique<reader_params>(reader_params{range, slice});
        return _factory_function(shard, std::move(schema), _contexts[shard].params->range, _contexts[shard].params->slice, pc,
                std::move(trace_state), fwd_mr).finally([this, zis = shared_from_this(), shard] {
            _contexts[shard].operation_in_progress = operation::none;
        });
    }
    virtual void destroy_reader(shard_id shard, future<paused_or_stopped_reader> reader) noexcept override {
        reader.then([shard, this] (paused_or_stopped_reader&& reader) {
            return smp::submit_to(shard, [reader = std::move(reader.remote_reader), ctx = std::move(_contexts[shard])] () mutable {
                reader.release();
            });
        }).finally([zis = shared_from_this()] {});
    }
    virtual future<> pause(foreign_ptr<std::unique_ptr<flat_mutation_reader>> reader) override {
        const auto shard = reader.get_owner_shard();

        set_current_operation(shard, operation::pause);

        return _delay().then([this, shard, reader = std::move(reader)] () mutable {
            _contexts[shard].reader = std::move(reader);
        }).finally([this, zis = shared_from_this(), shard] {
            _contexts[shard].operation_in_progress = operation::none;
        });
    }
    virtual future<foreign_ptr<std::unique_ptr<flat_mutation_reader>>> try_resume(shard_id shard) override {
        set_current_operation(shard, operation::try_resume);

        return _delay().then([this, shard] {
            if (_evict_paused_readers) {
                _contexts[shard].reader.reset();
            }
            return std::move(_contexts[shard].reader);
        }).finally([this, zis = shared_from_this(), shard] {
            _contexts[shard].operation_in_progress = operation::none;
        });
    }
};

// Best run with SMP >= 2
SEASTAR_THREAD_TEST_CASE(test_multishard_combining_reader_as_mutation_source) {
    if (smp::count < 2) {
        std::cerr << "Cannot run test " << get_name() << " with smp::count < 2" << std::endl;
        return;
    }

    do_with_cql_env([] (cql_test_env& env) -> future<> {
        auto make_populate = [] (test_reader_lifecycle_policy::delay_function delay, bool evict_paused_readers) {
            return [delay = std::move(delay), evict_paused_readers] (schema_ptr s, const std::vector<mutation>& mutations) mutable {
                // We need to group mutations that have the same token so they land on the same shard.
                std::map<dht::token, std::vector<mutation>> mutations_by_token;

                for (const auto& mut : mutations) {
                    mutations_by_token[mut.token()].push_back(mut);
                }

                auto partitioner = make_lw_shared<dummy_partitioner>(dht::global_partitioner(), mutations_by_token);

                auto merged_mutations = boost::copy_range<std::vector<std::vector<mutation>>>(mutations_by_token | boost::adaptors::map_values);

                auto remote_memtables = make_lw_shared<std::vector<foreign_ptr<lw_shared_ptr<memtable>>>>();
                for (unsigned shard = 0; shard < partitioner->shard_count(); ++shard) {
                    auto remote_mt = smp::submit_to(shard, [shard, s = global_schema_ptr(s), &merged_mutations, partitioner = *partitioner] {
                        auto mt = make_lw_shared<memtable>(s.get());

                        for (unsigned i = shard; i < merged_mutations.size(); i += partitioner.shard_count()) {
                            for (auto& mut : merged_mutations[i]) {
                                mt->apply(mut);
                            }
                        }

                        return make_foreign(mt);
                    }).get0();
                    remote_memtables->emplace_back(std::move(remote_mt));
                }

                return mutation_source([&delay, partitioner, remote_memtables, evict_paused_readers] (schema_ptr s,
                        const dht::partition_range& range,
                        const query::partition_slice& slice,
                        const io_priority_class& pc,
                        tracing::trace_state_ptr trace_state,
                        streamed_mutation::forwarding fwd_sm,
                        mutation_reader::forwarding fwd_mr) mutable {
                    auto factory = [remote_memtables] (unsigned shard,
                            schema_ptr s,
                            const dht::partition_range& range,
                            const query::partition_slice& slice,
                            const io_priority_class& pc,
                            tracing::trace_state_ptr trace_state,
                            mutation_reader::forwarding fwd_mr) {
                        return smp::submit_to(shard, [mt = &*remote_memtables->at(shard), s = global_schema_ptr(s), &range, &slice, &pc,
                                trace_state = tracing::global_trace_state_ptr(trace_state), fwd_mr] () mutable {
                            return make_foreign(std::make_unique<flat_mutation_reader>(mt->make_flat_reader(s.get(),
                                    range,
                                    slice,
                                    pc,
                                    trace_state.get(),
                                    streamed_mutation::forwarding::no,
                                    fwd_mr)));
                        });
                    };

                    auto lifecycle_policy = seastar::make_shared<test_reader_lifecycle_policy>(std::move(factory), delay, evict_paused_readers);
                    return make_multishard_combining_reader(std::move(lifecycle_policy), *partitioner, s, range, slice, pc, trace_state, fwd_mr);
                });
            };
        };

        auto make_random_delay = [] (int from, int to) {
            return [gen = std::default_random_engine(std::random_device()()),
                    dist = std::uniform_int_distribution(from, to)] () mutable {
                return seastar::sleep(std::chrono::milliseconds(dist(gen)));
            };
        };

        BOOST_TEST_MESSAGE("run_mutation_source_tests(delay=no_delay, evict_readers=false)");
        run_mutation_source_tests(make_populate(test_reader_lifecycle_policy::no_delay, false), streamed_mutation::forwarding::no);

        BOOST_TEST_MESSAGE("run_mutation_source_tests(delay=random, evict_readers=false)");
        run_mutation_source_tests(make_populate(make_random_delay(1, 10), false), streamed_mutation::forwarding::no);

        BOOST_TEST_MESSAGE("run_mutation_source_tests(delay=random, evict_readers=true)");
        run_mutation_source_tests(make_populate(make_random_delay(1, 10), true), streamed_mutation::forwarding::no);

        return make_ready_future<>();
    }).get();
}

// Best run with SMP >= 3
SEASTAR_THREAD_TEST_CASE(test_multishard_combining_reader_reading_empty_table) {
    if (smp::count < 3) {
        std::cerr << "Cannot run test " << get_name() << " with smp::count < 2" << std::endl;
        return;
    }

    do_with_cql_env([] (cql_test_env& env) -> future<> {
        std::vector<bool> shards_touched(smp::count, false);
        simple_schema s;
        auto factory = [&shards_touched] (unsigned shard,
                schema_ptr s,
                const dht::partition_range& range,
                const query::partition_slice& slice,
                const io_priority_class& pc,
                tracing::trace_state_ptr trace_state,
                mutation_reader::forwarding fwd_mr) {
            shards_touched[shard] = true;
            return smp::submit_to(shard, [gs = global_schema_ptr(s)] () mutable {
                return make_foreign(std::make_unique<flat_mutation_reader>(make_empty_flat_reader(gs.get())));
            });
        };

        assert_that(make_multishard_combining_reader(
                    seastar::make_shared<test_reader_lifecycle_policy>(std::move(factory)),
                    dht::global_partitioner(),
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

// A reader that can supply a quasi infinite amount of fragments.
//
// On each fill_buffer() call:
// * It will generate a new partition.
// * It will generate rows until the buffer is full.
class infinite_reader : public flat_mutation_reader::impl {
    simple_schema _s;
    uint32_t _pk = 0;

public:
    infinite_reader(simple_schema s)
        : impl(s.schema())
        , _s(std::move(s)) {
    }

    virtual future<> fill_buffer(db::timeout_clock::time_point) override {
        push_mutation_fragment(partition_start(_s.make_pkey(_pk++), {}));

        auto ck = uint32_t(0);
        while (!is_buffer_full()) {
            push_mutation_fragment(_s.make_row(_s.make_ckey(ck++), make_random_string(2 << 5)));
        }

        push_mutation_fragment(partition_end());

        return make_ready_future<>();
    }

    virtual void next_partition() override { }
    virtual future<> fast_forward_to(const dht::partition_range&, db::timeout_clock::time_point) override { throw std::bad_function_call(); }
    virtual future<> fast_forward_to(position_range, db::timeout_clock::time_point) override { throw std::bad_function_call(); }
};

// Test a background pending reader creation outliving the reader.
//
// The multishard reader will issue read-aheads according to its internal
// concurrency. This concurrency starts from 1 and is increased every time
// a remote reader blocks (buffer is empty) within the same fill_buffer() call.
// When launching a read-ahead it is possible that the shard reader is not
// created yet. In this case the shard reader will be created first and then the
// read-ahead will be executed. The shard reader will be created in the
// background and the fiber will not be synchronized with until the multishard
// reader reaches the shard in question with the normal reading. If the
// multishard reader is destroyed before the synchronization happens the fiber
// is left orphaned. Test that the fiber is prepared for this possibility and
// doesn't attempt to read any members of any destoyed objects causing memory
// errors.
//
// Theory of operation:
// 1) [shard 1] empty remote reader -> move to next shard;
// 2) [shard 2] infinite remote reader -> increase concurrency to 2 because we
//    traversed to another shard in the same fill_buffer() call;
// 3) [shard 3] pending reader -> will be created in the background as part of
//    the read ahead launched due to the increased concurrency;
// 4) Infinite reader on shard 2 fills the buffer, reader creation is still
//    pending in the background;
// 4) Reader is destroyed;
// 5) Set the reader creation promise's value -> the now orphan read-ahead
//    fiber executes;
//
// Has to be run with smp >= 3
SEASTAR_THREAD_TEST_CASE(test_multishard_combining_reader_destroyed_with_pending_create_reader) {
    if (smp::count < 3) {
        std::cerr << "Cannot run test " << get_name() << " with smp::count < 3" << std::endl;
        return;
    }

    do_with_cql_env([] (cql_test_env& env) -> future<> {
        class promise_wrapper {
            promise<foreign_ptr<std::unique_ptr<flat_mutation_reader>>> _pr;
            bool _pending = false;

        public:
            future<foreign_ptr<std::unique_ptr<flat_mutation_reader>>> get_future() {
                _pending = true;
                return _pr.get_future();
            }

            void trigger(flat_mutation_reader r) {
                _pr.set_value(make_foreign(std::make_unique<flat_mutation_reader>(std::move(r))));
            }

            bool is_pending() const {
                return _pending;
            }
        };

        const auto shard_of_interest = 2u;
        auto remote_control = smp::submit_to(shard_of_interest, [] {
            return make_foreign(std::make_unique<promise_wrapper>());
        }).get0();

        auto s = simple_schema();

        auto factory = [&s, shard_of_interest, remote_control = remote_control.get()] (unsigned shard,
                schema_ptr,
                const dht::partition_range& range,
                const query::partition_slice& slice,
                const io_priority_class& pc,
                tracing::trace_state_ptr trace_state,
                mutation_reader::forwarding fwd_mr) {
            return smp::submit_to(shard, [shard_of_interest, gs = global_simple_schema(s), remote_control] () mutable {
                if (engine().cpu_id() == shard_of_interest) {
                    return remote_control->get_future();
                } else {
                    auto reader = engine().cpu_id() == shard_of_interest - 1 ?
                            make_flat_mutation_reader<infinite_reader>(gs.get()) :
                            make_empty_flat_reader(gs.get().schema());

                    using foreign_reader_ptr = foreign_ptr<std::unique_ptr<flat_mutation_reader>>;
                    return make_ready_future<foreign_reader_ptr>(make_foreign(std::make_unique<flat_mutation_reader>(std::move(reader))));
                }
            });
        };

        {
            const auto mutations_by_token = std::map<dht::token, std::vector<mutation>>();
            auto partitioner = dummy_partitioner(dht::global_partitioner(), mutations_by_token);
            auto reader = make_multishard_combining_reader(seastar::make_shared<test_reader_lifecycle_policy>(std::move(factory)), partitioner,
                    s.schema(), query::full_partition_range, s.schema()->full_slice(), service::get_local_sstable_query_read_priority());

            reader.fill_buffer(db::no_timeout).get();

            BOOST_REQUIRE(reader.is_buffer_full());
            BOOST_REQUIRE(smp::submit_to(shard_of_interest, [remote_control = remote_control.get()] {
                return remote_control->is_pending();
            }).get0());
        }

        smp::submit_to(shard_of_interest, [gs = global_schema_ptr(s.schema()), remote_control = remote_control.get()] {
            remote_control->trigger(make_empty_flat_reader(gs.get()));
        }).get();

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
        if (is_end_of_stream()) {
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

            return make_ready_future<control_type, reader_type>(std::move(control), std::move(reader));
        }).get();

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

        auto partitioner = dummy_partitioner(dht::global_partitioner(), std::move(pkeys_by_tokens));

        auto factory = [&s, &remote_controls, &shard_pkeys] (unsigned shard,
                schema_ptr,
                const dht::partition_range& range,
                const query::partition_slice& slice,
                const io_priority_class& pc,
                tracing::trace_state_ptr trace_state,
                mutation_reader::forwarding) {
            return smp::submit_to(shard, [shard, gs = global_simple_schema(s), remote_control = remote_controls.at(shard).get(),
                    pkeys = shard_pkeys.at(shard)] () mutable {
                auto action = shard == 0 ? puppet_reader::fill_buffer_action::fill : puppet_reader::fill_buffer_action::block;
                return make_foreign(std::make_unique<flat_mutation_reader>(
                            make_flat_mutation_reader<puppet_reader>(gs.get(), *remote_control, action, std::move(pkeys))));
            });
        };

        {
            auto reader = make_multishard_combining_reader(seastar::make_shared<test_reader_lifecycle_policy>(std::move(factory)), partitioner,
                    s.schema(), query::full_partition_range, s.schema()->full_slice(), service::get_local_sstable_query_read_priority());
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
