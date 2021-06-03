/*
 * Copyright (C) 2017-present ScyllaDB
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

#include "counters.hh"

#include <random>

#include <seastar/core/thread.hh>

#include <boost/range/algorithm/sort.hpp>
#include <boost/range/algorithm/random_shuffle.hpp>

#include <seastar/testing/test_case.hh>
#include "test/lib/random_utils.hh"
#include "schema_builder.hh"
#include "keys.hh"
#include "mutation.hh"
#include "frozen_mutation.hh"

void verify_shard_order(counter_cell_view ccv) {
    if (ccv.shards().begin() == ccv.shards().end()) {
        return;
    }

    auto it = ccv.shards().begin();
    auto prev = it;
    ++it;

    while (it != ccv.shards().end()) {
        BOOST_REQUIRE_GT(it->id(), prev->id());
        prev = it;
        ++it;
    }
}

std::vector<counter_id> generate_ids(unsigned count) {
    std::vector<counter_id> id;
    std::generate_n(std::back_inserter(id), count, counter_id::generate_random);
    boost::range::sort(id);
    return id;
}

SEASTAR_TEST_CASE(test_counter_cell) {
    return seastar::async([] {
        auto cdef = column_definition("name", counter_type, column_kind::regular_column);

        auto id = generate_ids(3);

        counter_cell_builder b1;
        b1.add_shard(counter_shard(id[0], 5, 1));
        b1.add_shard(counter_shard(id[1], -4, 1));
        auto c1 = atomic_cell_or_collection(b1.build(0));
        
        atomic_cell_or_collection c2;
      {
        counter_cell_view cv(c1.as_atomic_cell(cdef));
        BOOST_REQUIRE_EQUAL(cv.total_value(), 1);
        verify_shard_order(cv);

        counter_cell_builder b2;
        b2.add_shard(counter_shard(*cv.get_shard(id[0])).update(2, 1));
        b2.add_shard(counter_shard(id[2], 1, 1));
        c2 = atomic_cell_or_collection(b2.build(0));
      }

      {
        counter_cell_view cv(c2.as_atomic_cell(cdef));
        BOOST_REQUIRE_EQUAL(cv.total_value(), 8);
        verify_shard_order(cv);
      }

        counter_cell_view::apply(cdef, c1, c2);
      {
        counter_cell_view cv(c1.as_atomic_cell(cdef));
        BOOST_REQUIRE_EQUAL(cv.total_value(), 4);
        verify_shard_order(cv);
      }
    });
}

SEASTAR_TEST_CASE(test_apply) {
    return seastar::async([] {
        auto cdef = column_definition("name", counter_type, column_kind::regular_column);

        auto verify_apply = [&] (const atomic_cell_or_collection& a, const atomic_cell_or_collection& b, int64_t value) {
            auto dst = a.copy(*cdef.type);
            auto src = b.copy(*cdef.type);
            counter_cell_view::apply(cdef, dst, src);

          {
            counter_cell_view cv(dst.as_atomic_cell(cdef));
            BOOST_REQUIRE_EQUAL(cv.total_value(), value);
            BOOST_REQUIRE_EQUAL(cv.timestamp(), std::max(dst.as_atomic_cell(cdef).timestamp(), src.as_atomic_cell(cdef).timestamp()));
          }
        };
        auto id = generate_ids(5);

        counter_cell_builder b1;
        b1.add_shard(counter_shard(id[0], 3, 1));
        b1.add_shard(counter_shard(id[2], 2, 2));
        b1.add_shard(counter_shard(id[4], 1, 3));
        auto c1 = atomic_cell_or_collection(b1.build(1));

        auto c2 = atomic_cell_or_collection(
            counter_cell_builder::from_single_shard(2, counter_shard(id[2], 8, 3))
        );

        verify_apply(c1, c2, 12);
        verify_apply(c2, c1, 12);

        counter_cell_builder b2;
        b2.add_shard(counter_shard(id[1], 4, 5));
        b2.add_shard(counter_shard(id[3], 5, 4));
        auto c3 = atomic_cell_or_collection(b2.build(2));

        verify_apply(c1, c3, 15);
        verify_apply(c3, c1, 15);

        auto c4 = atomic_cell_or_collection(
            counter_cell_builder::from_single_shard(0, counter_shard(id[2], 8, 1))
        );

        verify_apply(c1, c4, 6);
        verify_apply(c4, c1, 6);

        counter_cell_builder b3;
        b3.add_shard(counter_shard(id[0], 9, 0));
        b3.add_shard(counter_shard(id[2], 12, 3));
        b3.add_shard(counter_shard(id[3], 5, 4));
        auto c5 = atomic_cell_or_collection(b3.build(2));

        verify_apply(c1, c5, 21);
        verify_apply(c5, c1, 21);

        auto c6 = atomic_cell_or_collection(
            counter_cell_builder::from_single_shard(3, counter_shard(id[2], 8, 1))
        );

        verify_apply(c1, c6, 6);
        verify_apply(c6, c1, 6);
    });
}

schema_ptr get_schema() {
    return schema_builder("ks", "cf")
            .with_column("pk", int32_type, column_kind::partition_key)
            .with_column("ck", int32_type, column_kind::clustering_key)
            .with_column("s1", counter_type, column_kind::static_column)
            .with_column("c1", counter_type)
            .build();
}

atomic_cell_view get_counter_cell(mutation& m) {
    auto& mp = m.partition();
    BOOST_REQUIRE_EQUAL(mp.clustered_rows().calculate_size(), 1);
    const auto& cells = mp.clustered_rows().begin()->row().cells();
    BOOST_REQUIRE_EQUAL(cells.size(), 1);
    std::optional<atomic_cell_view> acv;
    cells.for_each_cell([&] (column_id id, const atomic_cell_or_collection& ac_o_c) {
        acv = ac_o_c.as_atomic_cell(m.schema()->regular_column_at(id));
    });
    BOOST_REQUIRE(bool(acv));
    return *acv;
};

atomic_cell_view get_static_counter_cell(mutation& m) {
    auto& mp = m.partition();
    const auto& cells = mp.static_row();
    BOOST_REQUIRE_EQUAL(cells.size(), 1);
    std::optional<atomic_cell_view> acv;
    cells.for_each_cell([&] (column_id id, const atomic_cell_or_collection& ac_o_c) {
        acv = ac_o_c.as_atomic_cell(m.schema()->static_column_at(id));
    });
    BOOST_REQUIRE(bool(acv));
    return *acv;
};

SEASTAR_TEST_CASE(test_counter_mutations) {
    return seastar::async([] {
        auto s = get_schema();

        auto id = generate_ids(4);

        auto pk = partition_key::from_single_value(*s, int32_type->decompose(0));
        auto ck = clustering_key::from_single_value(*s, int32_type->decompose(0));
        auto& col = *s->get_column_definition(utf8_type->decompose(sstring("c1")));
        auto& scol = *s->get_column_definition(utf8_type->decompose(sstring("s1")));

        mutation m1(s, pk);
        counter_cell_builder b1;
        b1.add_shard(counter_shard(id[0], 1, 1));
        b1.add_shard(counter_shard(id[1], 2, 1));
        b1.add_shard(counter_shard(id[2], 3, 1));
        m1.set_clustered_cell(ck, col, b1.build(api::new_timestamp()));

        counter_cell_builder b1s;
        b1s.add_shard(counter_shard(id[1], 4, 3));
        b1s.add_shard(counter_shard(id[2], 5, 1));
        b1s.add_shard(counter_shard(id[3], 6, 2));
        m1.set_static_cell(scol, b1s.build(api::new_timestamp()));

        mutation m2(s, pk);
        counter_cell_builder b2;
        b2.add_shard(counter_shard(id[0], 1, 1));
        b2.add_shard(counter_shard(id[2], -5, 4));
        b2.add_shard(counter_shard(id[3], -100, 1));
        m2.set_clustered_cell(ck, col, b2.build(api::new_timestamp()));

        counter_cell_builder b2s;
        b2s.add_shard(counter_shard(id[0], 8, 8));
        b2s.add_shard(counter_shard(id[1], 1, 4));
        b2s.add_shard(counter_shard(id[3], 9, 1));
        m2.set_static_cell(scol, b2s.build(api::new_timestamp()));

        mutation m3(s, pk);
        m3.set_clustered_cell(ck, col, atomic_cell::make_dead(1, gc_clock::now()));
        m3.set_static_cell(scol, atomic_cell::make_dead(1, gc_clock::now()));

        mutation m4(s, pk);
        m4.partition().apply(tombstone(0, gc_clock::now()));

        // Apply

        auto m = m1;
        m.apply(m2);
        auto ac = get_counter_cell(m);
        BOOST_REQUIRE(ac.is_live());
      {
        counter_cell_view ccv(ac);
        BOOST_REQUIRE_EQUAL(ccv.total_value(), -102);
        verify_shard_order(ccv);
      }

        ac = get_static_counter_cell(m);
        BOOST_REQUIRE(ac.is_live());
      {
        counter_cell_view ccv(ac);
        BOOST_REQUIRE_EQUAL(ccv.total_value(), 20);
        verify_shard_order(ccv);
      }

        m.apply(m3);
        ac = get_counter_cell(m);
        BOOST_REQUIRE(!ac.is_live());
        ac = get_static_counter_cell(m);
        BOOST_REQUIRE(!ac.is_live());

        m = m1;
        m.apply(m4);
        m.partition().compact_for_query(*s, gc_clock::now(), { query::clustering_range::make_singular(ck) },
                                        false, false, query::max_rows);
        BOOST_REQUIRE_EQUAL(m.partition().clustered_rows().calculate_size(), 0);
        BOOST_REQUIRE(m.partition().static_row().empty());

        // Difference

        m = mutation(s, m1.decorated_key(), m1.partition().difference(s, m2.partition()));
        ac = get_counter_cell(m);
        BOOST_REQUIRE(ac.is_live());
      {
        counter_cell_view ccv(ac);
        BOOST_REQUIRE_EQUAL(ccv.total_value(), 2);
        verify_shard_order(ccv);
      }

        ac = get_static_counter_cell(m);
        BOOST_REQUIRE(ac.is_live());
      {
        counter_cell_view ccv(ac);
        BOOST_REQUIRE_EQUAL(ccv.total_value(), 11);
        verify_shard_order(ccv);
      }

        m = mutation(s, m1.decorated_key(), m2.partition().difference(s, m1.partition()));
        ac = get_counter_cell(m);
        BOOST_REQUIRE(ac.is_live());
      {
        counter_cell_view ccv(ac);
        BOOST_REQUIRE_EQUAL(ccv.total_value(), -105);
        verify_shard_order(ccv);
      }

        ac = get_static_counter_cell(m);
        BOOST_REQUIRE(ac.is_live());
      {
        counter_cell_view ccv(ac);
        BOOST_REQUIRE_EQUAL(ccv.total_value(), 9);
        verify_shard_order(ccv);
      }

        m = mutation(s, m1.decorated_key(), m1.partition().difference(s, m3.partition()));
        BOOST_REQUIRE_EQUAL(m.partition().clustered_rows().calculate_size(), 0);
        BOOST_REQUIRE(m.partition().static_row().empty());

        m = mutation(s, m1.decorated_key(), m3.partition().difference(s, m1.partition()));
        ac = get_counter_cell(m);
        BOOST_REQUIRE(!ac.is_live());

        ac = get_static_counter_cell(m);
        BOOST_REQUIRE(!ac.is_live());

        // Freeze

        auto fm1 = freeze(m1);
        auto fm2 = freeze(m2);
        auto fm3 = freeze(m3);
        BOOST_REQUIRE_EQUAL(fm1.unfreeze(s), m1);
        BOOST_REQUIRE_EQUAL(fm2.unfreeze(s), m2);
        BOOST_REQUIRE_EQUAL(fm3.unfreeze(s), m3);

        mutation_application_stats app_stats;

        auto m0 = m1;
        m0.partition().apply(*s, fm2.partition(), *s, app_stats);
        m = m1;
        m.apply(m2);
        BOOST_REQUIRE_EQUAL(m, m0);

        m0 = m2;
        m0.partition().apply(*s, fm1.partition(), *s, app_stats);
        m = m2;
        m.apply(m1);
        BOOST_REQUIRE_EQUAL(m, m0);

        m0 = m1;
        m0.partition().apply(*s, fm3.partition(), *s, app_stats);
        m = m1;
        m.apply(m3);
        BOOST_REQUIRE_EQUAL(m, m0);

        m0 = m3;
        m0.partition().apply(*s, fm1.partition(), *s, app_stats);
        m = m3;
        m.apply(m1);
        BOOST_REQUIRE_EQUAL(m, m0);
    });
}

SEASTAR_TEST_CASE(test_counter_update_mutations) {
    return seastar::async([] {
        auto s = get_schema();

        auto pk = partition_key::from_single_value(*s, int32_type->decompose(0));
        auto ck = clustering_key::from_single_value(*s, int32_type->decompose(0));
        auto& col = *s->get_column_definition(utf8_type->decompose(sstring("c1")));
        auto& scol = *s->get_column_definition(utf8_type->decompose(sstring("s1")));

        auto c1 = atomic_cell::make_live_counter_update(api::new_timestamp(), 5);
        auto s1 = atomic_cell::make_live_counter_update(api::new_timestamp(), 4);
        mutation m1(s, pk);
        m1.set_clustered_cell(ck, col, std::move(c1));
        m1.set_static_cell(scol, std::move(s1));

        auto c2 = atomic_cell::make_live_counter_update(api::new_timestamp(), 9);
        auto s2 = atomic_cell::make_live_counter_update(api::new_timestamp(), 8);
        mutation m2(s, pk);
        m2.set_clustered_cell(ck, col, std::move(c2));
        m2.set_static_cell(scol, std::move(s2));

        auto c3 = atomic_cell::make_dead(api::new_timestamp() / 2, gc_clock::now());
        mutation m3(s, pk);
        m3.set_clustered_cell(ck, col, atomic_cell(*counter_type, c3));
        m3.set_static_cell(scol, std::move(c3));

        auto m12 = m1;
        m12.apply(m2);
        auto ac = get_counter_cell(m12);
        BOOST_REQUIRE(ac.is_live());
        BOOST_REQUIRE(ac.is_counter_update());
        BOOST_REQUIRE_EQUAL(ac.counter_update_value(), 14);

        ac = get_static_counter_cell(m12);
        BOOST_REQUIRE(ac.is_live());
        BOOST_REQUIRE(ac.is_counter_update());
        BOOST_REQUIRE_EQUAL(ac.counter_update_value(), 12);

        auto m123 = m12;
        m123.apply(m3);
        ac = get_counter_cell(m123);
        BOOST_REQUIRE(!ac.is_live());

        ac = get_static_counter_cell(m123);
        BOOST_REQUIRE(!ac.is_live());
    });
}

SEASTAR_TEST_CASE(test_transfer_updates_to_shards) {
    return seastar::async([] {
        auto s = get_schema();

        auto pk = partition_key::from_single_value(*s, int32_type->decompose(0));
        auto ck = clustering_key::from_single_value(*s, int32_type->decompose(0));
        auto& col = *s->get_column_definition(utf8_type->decompose(sstring("c1")));
        auto& scol = *s->get_column_definition(utf8_type->decompose(sstring("s1")));

        auto c1 = atomic_cell::make_live_counter_update(api::new_timestamp(), 5);
        auto s1 = atomic_cell::make_live_counter_update(api::new_timestamp(), 4);
        mutation m1(s, pk);
        m1.set_clustered_cell(ck, col, std::move(c1));
        m1.set_static_cell(scol, std::move(s1));

        auto c2 = atomic_cell::make_live_counter_update(api::new_timestamp(), 9);
        auto s2 = atomic_cell::make_live_counter_update(api::new_timestamp(), 8);
        mutation m2(s, pk);
        m2.set_clustered_cell(ck, col, std::move(c2));
        m2.set_static_cell(scol, std::move(s2));

        auto c3 = atomic_cell::make_dead(api::new_timestamp() / 2, gc_clock::now());
        mutation m3(s, pk);
        m3.set_clustered_cell(ck, col, atomic_cell(*counter_type, c3));
        m3.set_static_cell(scol, std::move(c3));

        auto m0 = m1;
        transform_counter_updates_to_shards(m0, nullptr, 0, utils::UUID{});

        auto empty = mutation(s, pk);
        auto m = m1;
        transform_counter_updates_to_shards(m, &empty, 0, utils::UUID{});
        BOOST_REQUIRE_EQUAL(m, m0);

        auto ac = get_counter_cell(m);
        BOOST_REQUIRE(ac.is_live());
      {
        counter_cell_view ccv(ac);
        BOOST_REQUIRE_EQUAL(ccv.total_value(), 5);
        verify_shard_order(ccv);
      }

        ac = get_static_counter_cell(m);
        BOOST_REQUIRE(ac.is_live());
      {
        counter_cell_view ccv(ac);
        BOOST_REQUIRE_EQUAL(ccv.total_value(), 4);
        verify_shard_order(ccv);
      }

        m = m2;
        transform_counter_updates_to_shards(m, &m0, 0, utils::UUID{});

        ac = get_counter_cell(m);
        BOOST_REQUIRE(ac.is_live());
      {
        counter_cell_view ccv(ac);
        BOOST_REQUIRE_EQUAL(ccv.total_value(), 14);
        verify_shard_order(ccv);
      }

        ac = get_static_counter_cell(m);
        BOOST_REQUIRE(ac.is_live());
      {
        counter_cell_view ccv(ac);
        BOOST_REQUIRE_EQUAL(ccv.total_value(), 12);
        verify_shard_order(ccv);
      }

        m = m3;
        transform_counter_updates_to_shards(m, &m0, 0, utils::UUID{});
        ac = get_counter_cell(m);
        BOOST_REQUIRE(!ac.is_live());
        ac = get_static_counter_cell(m);
        BOOST_REQUIRE(!ac.is_live());
    });
}

SEASTAR_TEST_CASE(test_sanitize_corrupted_cells) {
    return seastar::async([] {
        auto& gen = seastar::testing::local_random_engine;

        std::uniform_int_distribution<unsigned> shard_count_dist(2, 64);
        std::uniform_int_distribution<int64_t> logical_clock_dist(1, 1024 * 1024);
        std::uniform_int_distribution<int64_t> value_dist(-1024 * 1024, 1024 * 1024);

        for (auto i = 0; i < 100; i++) {
            auto cdef = column_definition("name", counter_type, column_kind::regular_column);

            auto shard_count = shard_count_dist(gen);
            auto ids = generate_ids(shard_count);

            // Create a valid counter cell
            std::vector<counter_shard> shards;
            for (auto id : ids) {
                shards.emplace_back(id, value_dist(gen), logical_clock_dist(gen));
            }

            counter_cell_builder b1;
            for (auto&& cs : shards) {
                b1.add_shard(cs);
            }
            auto c1 = atomic_cell_or_collection(b1.build(0));

            // Corrupt it by changing shard order and adding duplicates
            boost::range::random_shuffle(shards);

            std::uniform_int_distribution<unsigned> duplicate_count_dist(1, shard_count / 2);
            auto duplicate_count = duplicate_count_dist(gen);
            for (auto i = 0u; i < duplicate_count; i++) {
                auto cs = shards[i];
                shards.emplace_back(cs);
            }

            boost::range::random_shuffle(shards);

            // Sanitize
            counter_cell_builder b2;
            for (auto&& cs : shards) {
                b2.add_maybe_unsorted_shard(cs);
            }
            b2.sort_and_remove_duplicates();
            auto c2 = atomic_cell_or_collection(b2.build(0));

            // Compare
          {
            counter_cell_view cv1(c1.as_atomic_cell(cdef));
            counter_cell_view cv2(c2.as_atomic_cell(cdef));
            BOOST_REQUIRE_EQUAL(cv1, cv2);
            BOOST_REQUIRE_EQUAL(cv1.total_value(), cv2.total_value());
            verify_shard_order(cv1);
            verify_shard_order(cv2);
          }
        }
    });
}

SEASTAR_TEST_CASE(test_counter_id_ordering) {
    return seastar::async([] {
        const char* ids[] = {
                "00000000-0000-0000-0000-000000000000",
                "00000000-0000-0000-0000-000000000001",
                "0290003c-977e-397c-ac3e-fdfdc01d626b",
                "0290003c-987e-397c-ac3e-fdfdc01d626b",
                "0eeeddcc-aa99-8877-6655-443322110000",
                "0feeddcc-aa99-8877-6655-443322110000",
                "0feeddcc-aa99-8877-8655-443322110000",
                "3bf296f0-6e46-4481-87dc-ca53e61a8f08",
                "e41baa44-b178-48fc-ab75-11e9664409be",
                "f2ad405d-1658-484f-9418-6314ae2cedcf",
                "ffeeddcc-aa99-8877-6655-443322110000",
                "ffeeddcc-aa99-8877-6655-443322110001",
                "ffeeddcc-aa99-8878-6655-443322110000",
        };

        auto counter_ids = boost::copy_range<std::vector<counter_id>>(
            ids | boost::adaptors::transformed([] (auto id) {
                return counter_id(utils::UUID(id));
            })
        );

        for (auto it = counter_ids.begin(); it != counter_ids.end(); ++it) {
            BOOST_REQUIRE_EQUAL(*it, *it);
            BOOST_REQUIRE(!(*it < *it));
            BOOST_REQUIRE(!(*it > *it));
            for (auto it2 = counter_ids.begin(); it2 != it; ++it2) {
                BOOST_REQUIRE(*it2 < *it);
                BOOST_REQUIRE(*it2 != *it);
                BOOST_REQUIRE(!(*it2 > *it));
                BOOST_REQUIRE(!(*it2 == *it));
            }
            for (auto it2 = std::next(it); it2 != counter_ids.end(); ++it2) {
                BOOST_REQUIRE(*it2 > *it);
                BOOST_REQUIRE(*it2 != *it);
                BOOST_REQUIRE(!(*it2 < *it));
                BOOST_REQUIRE(!(*it2 == *it));
            }
        }
    });
}
