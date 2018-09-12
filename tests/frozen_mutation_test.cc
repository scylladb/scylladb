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


#include <boost/test/unit_test.hpp>

#include "tests/test_services.hh"
#include "tests/test-utils.hh"

#include "frozen_mutation.hh"
#include "schema_builder.hh"
#include "tests/mutation_assertions.hh"
#include "tests/mutation_source_test.hh"

#include <seastar/core/thread.hh>

static schema_builder new_table() {
    return { "some_keyspace", "some_table" };
}

static api::timestamp_type new_timestamp() {
    static api::timestamp_type t = 0;
    return t++;
};

static tombstone new_tombstone() {
    return { new_timestamp(), gc_clock::now() };
};

SEASTAR_TEST_CASE(test_writing_and_reading) {
    return seastar::async([] {
        storage_service_for_tests ssft;
        for_each_mutation([](const mutation &m) {
            auto frozen = freeze(m);
            BOOST_REQUIRE_EQUAL(frozen.schema_version(), m.schema()->version());
            assert_that(frozen.unfreeze(m.schema())).is_equal_to(m);
            BOOST_REQUIRE(frozen.decorated_key(*m.schema()).equal(*m.schema(), m.decorated_key()));
        });
    });
}

SEASTAR_TEST_CASE(test_application_of_partition_view_has_the_same_effect_as_applying_regular_mutation) {
    return seastar::async([] {
        storage_service_for_tests ssft;
        schema_ptr s = new_table()
                .with_column("pk_col", bytes_type, column_kind::partition_key)
                .with_column("ck_1", bytes_type, column_kind::clustering_key)
                .with_column("reg_1", bytes_type)
                .with_column("reg_2", bytes_type)
                .with_column("static_1", bytes_type, column_kind::static_column)
                .build();

        partition_key key = partition_key::from_single_value(*s, bytes("key"));
        clustering_key ck = clustering_key::from_deeply_exploded(*s, {data_value(bytes("ck"))});

        mutation m1(s, key);
        m1.partition().apply(new_tombstone());
        m1.set_clustered_cell(ck, "reg_1", data_value(bytes("val1")), new_timestamp());
        m1.set_clustered_cell(ck, "reg_2", data_value(bytes("val2")), new_timestamp());
        m1.partition().apply_insert(*s, ck, new_timestamp());
        m1.set_static_cell("static_1", data_value(bytes("val3")), new_timestamp());

        mutation m2(s, key);
        m2.set_clustered_cell(ck, "reg_1", data_value(bytes("val4")), new_timestamp());
        m2.partition().apply_insert(*s, ck, new_timestamp());
        m2.set_static_cell("static_1", data_value(bytes("val5")), new_timestamp());

        mutation m_frozen(s, key);
        m_frozen.partition().apply(*s, freeze(m1).partition(), *s);
        m_frozen.partition().apply(*s, freeze(m2).partition(), *s);

        mutation m_unfrozen(s, key);
        m_unfrozen.partition().apply(*s, m1.partition(), *s);
        m_unfrozen.partition().apply(*s, m2.partition(), *s);

        mutation m_refrozen(s, key);
        m_refrozen.partition().apply(*s, freeze(m1).unfreeze(s).partition(), *s);
        m_refrozen.partition().apply(*s, freeze(m2).unfreeze(s).partition(), *s);

        assert_that(m_unfrozen).is_equal_to(m_refrozen);
        assert_that(m_unfrozen).is_equal_to(m_frozen);
    });
}

SEASTAR_THREAD_TEST_CASE(test_frozen_mutation_fragment) {
    storage_service_for_tests ssft;
    for_each_mutation([] (const mutation& m) {
        auto& s = *m.schema();
        std::vector<mutation_fragment> mfs;
        auto rd = flat_mutation_reader_from_mutations({ m });
        rd.consume_pausable([&] (mutation_fragment mf) {
            mfs.emplace_back(std::move(mf));
            return stop_iteration::no;
        }, db::no_timeout).get();

        for (auto&& mf : mfs) {
            auto refrozen_mf = freeze(s, mf).unfreeze(s);
            if (!mf.equal(s, refrozen_mf)) {
                BOOST_FAIL("Expected " << mf << " got " << refrozen_mf);
            }
        }
    });
}
