/*
 * Copyright (C) 2017 ScyllaDB
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

#include <seastar/core/thread.hh>

#include <boost/range/algorithm/sort.hpp>

#include "tests/test-utils.hh"
#include "tests/test_services.hh"
#include "disk-error-handler.hh"
#include "schema_builder.hh"
#include "keys.hh"
#include "mutation.hh"
#include "frozen_mutation.hh"

thread_local disk_error_signal_type commit_error;
thread_local disk_error_signal_type general_disk_error;

std::vector<counter_id> generate_ids(unsigned count) {
    std::vector<counter_id> id;
    std::generate_n(std::back_inserter(id), count, counter_id::generate_random);
    boost::range::sort(id);
    return id;
}

SEASTAR_TEST_CASE(test_counter_cell) {
    return seastar::async([] {
        auto id = generate_ids(3);

        counter_cell_builder b1;
        b1.add_shard(counter_shard(id[0], 5, 1));
        b1.add_shard(counter_shard(id[1], -4, 1));
        auto c1 = atomic_cell_or_collection(b1.build(0));

        auto cv = counter_cell_view(c1.as_atomic_cell());
        BOOST_REQUIRE_EQUAL(cv.total_value(), 1);

        counter_cell_builder b2;
        b2.add_shard(counter_shard(*cv.get_shard(id[0])).update(2, 1));
        b2.add_shard(counter_shard(id[2], 1, 1));
        auto c2 = atomic_cell_or_collection(b2.build(0));

        cv = counter_cell_view(c2.as_atomic_cell());
        BOOST_REQUIRE_EQUAL(cv.total_value(), 8);

        counter_cell_view::apply_reversibly(c1, c2);
        cv = counter_cell_view(c1.as_atomic_cell());
        BOOST_REQUIRE_EQUAL(cv.total_value(), 4);
    });
}

SEASTAR_TEST_CASE(test_reversability_of_apply) {
    return seastar::async([] {
        auto verify_applies_reversibly = [] (atomic_cell_or_collection dst, atomic_cell_or_collection src, int64_t value) {
            auto original_dst = dst;

            auto applied = counter_cell_view::apply_reversibly(dst, src);
            auto applied_dst = dst;

            auto cv = counter_cell_view(dst.as_atomic_cell());
            BOOST_REQUIRE_EQUAL(cv.total_value(), value);
            BOOST_REQUIRE_EQUAL(cv.timestamp(), std::max(dst.as_atomic_cell().timestamp(), src.as_atomic_cell().timestamp()));

            if (applied) {
                counter_cell_view::revert_apply(dst, src);
            }
            BOOST_REQUIRE_EQUAL(counter_cell_view(dst.as_atomic_cell()),
                                counter_cell_view(original_dst.as_atomic_cell()));

            applied = counter_cell_view::apply_reversibly(dst, src);
            BOOST_REQUIRE_EQUAL(counter_cell_view(dst.as_atomic_cell()),
                                counter_cell_view(applied_dst.as_atomic_cell()));

            if (applied) {
                counter_cell_view::revert_apply(dst, src);
            }
            BOOST_REQUIRE_EQUAL(counter_cell_view(dst.as_atomic_cell()),
                                counter_cell_view(original_dst.as_atomic_cell()));
        };
        auto id = generate_ids(5);

        counter_cell_builder b1;
        b1.add_shard(counter_shard(id[0], 3, 1));
        b1.add_shard(counter_shard(id[2], 2, 2));
        b1.add_shard(counter_shard(id[4], 1, 3));
        auto c1 = atomic_cell_or_collection(b1.build(1));

        auto c2 = counter_cell_builder::from_single_shard(2, counter_shard(id[2], 8, 3));

        verify_applies_reversibly(c1, c2, 12);
        verify_applies_reversibly(c2, c1, 12);

        counter_cell_builder b2;
        b2.add_shard(counter_shard(id[1], 4, 5));
        b2.add_shard(counter_shard(id[3], 5, 4));
        auto c3 = atomic_cell_or_collection(b2.build(2));

        verify_applies_reversibly(c1, c3, 15);
        verify_applies_reversibly(c3, c1, 15);

        auto c4 = counter_cell_builder::from_single_shard(0, counter_shard(id[2], 8, 1));

        verify_applies_reversibly(c1, c4, 6);
        verify_applies_reversibly(c4, c1, 6);

        counter_cell_builder b3;
        b3.add_shard(counter_shard(id[0], 9, 0));
        b3.add_shard(counter_shard(id[2], 12, 3));
        b3.add_shard(counter_shard(id[3], 5, 4));
        auto c5 = atomic_cell_or_collection(b3.build(2));

        verify_applies_reversibly(c1, c5, 21);
        verify_applies_reversibly(c5, c1, 21);

        auto c6 = counter_cell_builder::from_single_shard(3, counter_shard(id[2], 8, 1));

        verify_applies_reversibly(c1, c6, 6);
        verify_applies_reversibly(c6, c1, 6);
    });
}

schema_ptr get_schema() {
    return schema_builder("ks", "cf")
            .with_column("pk", int32_type, column_kind::partition_key)
            .with_column("ck", int32_type, column_kind::clustering_key)
            .with_column("c1", counter_type)
            .build();
}

atomic_cell_view get_counter_cell(mutation& m) {
    auto& mp = m.partition();
    BOOST_REQUIRE_EQUAL(mp.clustered_rows().calculate_size(), 1);
    const auto& cells = mp.clustered_rows().begin()->row().cells();
    BOOST_REQUIRE_EQUAL(cells.size(), 1);
    stdx::optional<atomic_cell_view> acv;
    cells.for_each_cell([&] (column_id, const atomic_cell_or_collection& ac_o_c) {
        acv = ac_o_c.as_atomic_cell();
    });
    BOOST_REQUIRE(bool(acv));
    return *acv;
};

SEASTAR_TEST_CASE(test_counter_mutations) {
    return seastar::async([] {
        storage_service_for_tests ssft;

        auto s = get_schema();

        auto id = generate_ids(4);

        auto pk = partition_key::from_single_value(*s, int32_type->decompose(0));
        auto ck = clustering_key::from_single_value(*s, int32_type->decompose(0));
        auto& col = *s->get_column_definition(utf8_type->decompose(sstring("c1")));

        mutation m1(pk, s);
        counter_cell_builder b1;
        b1.add_shard(counter_shard(id[0], 1, 1));
        b1.add_shard(counter_shard(id[1], 2, 1));
        b1.add_shard(counter_shard(id[2], 3, 1));
        m1.set_clustered_cell(ck, col, b1.build(api::new_timestamp()));

        mutation m2(pk, s);
        counter_cell_builder b2;
        b1.add_shard(counter_shard(id[0], 1, 1));
        b2.add_shard(counter_shard(id[2], -5, 4));
        b2.add_shard(counter_shard(id[3], -100, 1));
        m2.set_clustered_cell(ck, col, b2.build(api::new_timestamp()));

        mutation m3(pk, s);
        m3.set_clustered_cell(ck, col, atomic_cell::make_dead(1, gc_clock::now()));

        mutation m4(pk, s);
        m4.partition().apply(tombstone(0, gc_clock::now()));

        // Apply

        auto m = m1;
        m.apply(m2);
        auto ac = get_counter_cell(m);
        BOOST_REQUIRE(ac.is_live());
        counter_cell_view ccv { ac };
        BOOST_REQUIRE_EQUAL(ccv.total_value(), -102);

        m.apply(m3);
        ac = get_counter_cell(m);
        BOOST_REQUIRE(!ac.is_live());

        m = m1;
        m.apply(m4);
        m.partition().compact_for_query(*s, gc_clock::now(), { query::clustering_range::make_singular(ck) },
                                        false, query::max_rows);
        BOOST_REQUIRE_EQUAL(m.partition().clustered_rows().calculate_size(), 0);

        // Difference

        m = mutation(s, m1.decorated_key(), m1.partition().difference(s, m2.partition()));
        ac = get_counter_cell(m);
        BOOST_REQUIRE(ac.is_live());
        ccv = counter_cell_view(ac);
        BOOST_REQUIRE_EQUAL(ccv.total_value(), 3);

        m = mutation(s, m1.decorated_key(), m2.partition().difference(s, m1.partition()));
        ac = get_counter_cell(m);
        BOOST_REQUIRE(ac.is_live());
        ccv = counter_cell_view(ac);
        BOOST_REQUIRE_EQUAL(ccv.total_value(), -105);

        m = mutation(s, m1.decorated_key(), m1.partition().difference(s, m3.partition()));
        BOOST_REQUIRE_EQUAL(m.partition().clustered_rows().calculate_size(), 0);

        m = mutation(s, m1.decorated_key(), m3.partition().difference(s, m1.partition()));
        ac = get_counter_cell(m);
        BOOST_REQUIRE(!ac.is_live());

        // Freeze

        auto fm1 = freeze(m1);
        auto fm2 = freeze(m2);
        auto fm3 = freeze(m3);
        BOOST_REQUIRE_EQUAL(fm1.unfreeze(s), m1);
        BOOST_REQUIRE_EQUAL(fm2.unfreeze(s), m2);
        BOOST_REQUIRE_EQUAL(fm3.unfreeze(s), m3);

        auto m0 = m1;
        m0.partition().apply(*s, fm2.partition(), *s);
        m = m1;
        m.apply(m2);
        BOOST_REQUIRE_EQUAL(m, m0);

        m0 = m2;
        m0.partition().apply(*s, fm1.partition(), *s);
        m = m2;
        m.apply(m1);
        BOOST_REQUIRE_EQUAL(m, m0);

        m0 = m1;
        m0.partition().apply(*s, fm3.partition(), *s);
        m = m1;
        m.apply(m3);
        BOOST_REQUIRE_EQUAL(m, m0);

        m0 = m3;
        m0.partition().apply(*s, fm1.partition(), *s);
        m = m3;
        m.apply(m1);
        BOOST_REQUIRE_EQUAL(m, m0);
    });
}

SEASTAR_TEST_CASE(test_counter_update_mutations) {
    return seastar::async([] {
        storage_service_for_tests ssft;

        auto s = get_schema();

        auto pk = partition_key::from_single_value(*s, int32_type->decompose(0));
        auto ck = clustering_key::from_single_value(*s, int32_type->decompose(0));
        auto& col = *s->get_column_definition(utf8_type->decompose(sstring("c1")));

        auto c1 = atomic_cell::make_live_counter_update(api::new_timestamp(), 5);
        mutation m1(pk, s);
        m1.set_clustered_cell(ck, col, c1);

        auto c2 = atomic_cell::make_live_counter_update(api::new_timestamp(), 9);
        mutation m2(pk, s);
        m2.set_clustered_cell(ck, col, c2);

        auto c3 = atomic_cell::make_dead(api::new_timestamp() / 2, gc_clock::now());
        mutation m3(pk, s);
        m3.set_clustered_cell(ck, col, c3);

        auto m12 = m1;
        m12.apply(m2);
        auto ac = get_counter_cell(m12);
        BOOST_REQUIRE(ac.is_live());
        BOOST_REQUIRE(ac.is_counter_update());
        BOOST_REQUIRE_EQUAL(ac.counter_update_value(), 14);

        auto m123 = m12;
        m123.apply(m3);
        ac = get_counter_cell(m123);
        BOOST_REQUIRE(!ac.is_live());
    });
}
