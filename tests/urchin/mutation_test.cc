/*
 * Copyright 2015 Cloudius Systems
 */

#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE core

#include <boost/test/unit_test.hpp>
#include "core/sstring.hh"
#include "database.hh"
#include "utils/UUID_gen.hh"
#include <random>

static sstring some_keyspace("ks");
static sstring some_column_family("cf");

static atomic_cell make_atomic_cell(bytes value) {
    return atomic_cell::make_live(0, std::move(value));
};

BOOST_AUTO_TEST_CASE(test_mutation_is_applied) {
    auto s = make_lw_shared(schema({}, some_keyspace, some_column_family,
        {{"p1", utf8_type}}, {{"c1", int32_type}}, {{"r1", int32_type}}, {}, utf8_type));

    column_family cf(s);

    const column_definition& r1_col = *s->get_column_definition("r1");
    auto key = partition_key::from_exploded(*s, {to_bytes("key1")});
    auto c_key = clustering_key::from_exploded(*s, {int32_type->decompose(2)});

    mutation m(key, s);
    m.set_clustered_cell(c_key, r1_col, make_atomic_cell(int32_type->decompose(3)));
    cf.apply(std::move(m));

    row& r = cf.find_or_create_row_slow(key, c_key);
    auto i = r.find_cell(r1_col.id);
    BOOST_REQUIRE(i);
    auto cell = i->as_atomic_cell();
    BOOST_REQUIRE(cell.is_live());
    BOOST_REQUIRE(int32_type->equal(cell.value(), int32_type->decompose(3)));
}

BOOST_AUTO_TEST_CASE(test_multi_level_row_tombstones) {
    auto s = make_lw_shared(schema({}, some_keyspace, some_column_family,
        {{"p1", utf8_type}},
        {{"c1", int32_type}, {"c2", int32_type}, {"c3", int32_type}},
        {{"r1", int32_type}}, {}, utf8_type));

    auto ttl = gc_clock::now() + std::chrono::seconds(1);

    mutation m(partition_key::from_exploded(*s, {to_bytes("key1")}), s);

    auto make_prefix = [s] (const std::vector<boost::any>& v) {
        return clustering_key_prefix::from_deeply_exploded(*s, v);
    };
    auto make_key = [s] (const std::vector<boost::any>& v) {
        return clustering_key::from_deeply_exploded(*s, v);
    };

    m.partition().apply_row_tombstone(*s, make_prefix({1, 2}), tombstone(9, ttl));
    BOOST_REQUIRE_EQUAL(m.partition().tombstone_for_row(*s, make_key({1, 2, 3})), tombstone(9, ttl));

    m.partition().apply_row_tombstone(*s, make_prefix({1, 3}), tombstone(8, ttl));
    BOOST_REQUIRE_EQUAL(m.partition().tombstone_for_row(*s, make_key({1, 2, 0})), tombstone(9, ttl));
    BOOST_REQUIRE_EQUAL(m.partition().tombstone_for_row(*s, make_key({1, 3, 0})), tombstone(8, ttl));

    m.partition().apply_row_tombstone(*s, make_prefix({1}), tombstone(11, ttl));
    BOOST_REQUIRE_EQUAL(m.partition().tombstone_for_row(*s, make_key({1, 2, 0})), tombstone(11, ttl));
    BOOST_REQUIRE_EQUAL(m.partition().tombstone_for_row(*s, make_key({1, 3, 0})), tombstone(11, ttl));

    m.partition().apply_row_tombstone(*s, make_prefix({1, 4}), tombstone(6, ttl));
    BOOST_REQUIRE_EQUAL(m.partition().tombstone_for_row(*s, make_key({1, 2, 0})), tombstone(11, ttl));
    BOOST_REQUIRE_EQUAL(m.partition().tombstone_for_row(*s, make_key({1, 3, 0})), tombstone(11, ttl));
    BOOST_REQUIRE_EQUAL(m.partition().tombstone_for_row(*s, make_key({1, 4, 0})), tombstone(11, ttl));
}

BOOST_AUTO_TEST_CASE(test_row_tombstone_updates) {
    auto s = make_lw_shared(schema({}, some_keyspace, some_column_family,
        {{"p1", utf8_type}}, {{"c1", int32_type}, {"c2", int32_type}}, {{"r1", int32_type}}, {}, utf8_type));

    column_family cf(s);

    auto key = partition_key::from_exploded(*s, {to_bytes("key1")});
    auto c_key1 = clustering_key::from_deeply_exploded(*s, {1, 0});
    auto c_key1_prefix = clustering_key_prefix::from_deeply_exploded(*s, {1});
    auto c_key2 = clustering_key::from_deeply_exploded(*s, {2, 0});
    auto c_key2_prefix = clustering_key_prefix::from_deeply_exploded(*s, {2});

    auto ttl = gc_clock::now() + std::chrono::seconds(1);

    mutation m(key, s);
    m.partition().apply_row_tombstone(*s, c_key1_prefix, tombstone(1, ttl));
    m.partition().apply_row_tombstone(*s, c_key2_prefix, tombstone(0, ttl));

    BOOST_REQUIRE_EQUAL(m.partition().tombstone_for_row(*s, c_key1), tombstone(1, ttl));
    BOOST_REQUIRE_EQUAL(m.partition().tombstone_for_row(*s, c_key2), tombstone(0, ttl));

    m.partition().apply_row_tombstone(*s, c_key2_prefix, tombstone(1, ttl));
    BOOST_REQUIRE_EQUAL(m.partition().tombstone_for_row(*s, c_key2), tombstone(1, ttl));
}

BOOST_AUTO_TEST_CASE(test_map_mutations) {
    auto my_map_type = map_type_impl::get_instance(int32_type, utf8_type, true);
    auto s = make_lw_shared(schema({}, some_keyspace, some_column_family,
        {{"p1", utf8_type}}, {{"c1", int32_type}}, {}, {{"s1", my_map_type}}, utf8_type));
    column_family cf(s);
    auto key = partition_key::from_exploded(*s, {to_bytes("key1")});
    auto& column = *s->get_column_definition("s1");
    map_type_impl::mutation mmut1{{}, {{int32_type->decompose(101), make_atomic_cell(utf8_type->decompose(sstring("101")))}}};
    mutation m1(key, s);
    m1.set_static_cell(column, my_map_type->serialize_mutation_form(mmut1));
    cf.apply(m1);
    map_type_impl::mutation mmut2{{}, {{int32_type->decompose(102), make_atomic_cell(utf8_type->decompose(sstring("102")))}}};
    mutation m2(key, s);
    m2.set_static_cell(column, my_map_type->serialize_mutation_form(mmut2));
    cf.apply(m2);
    map_type_impl::mutation mmut3{{}, {{int32_type->decompose(103), make_atomic_cell(utf8_type->decompose(sstring("103")))}}};
    mutation m3(key, s);
    m3.set_static_cell(column, my_map_type->serialize_mutation_form(mmut3));
    cf.apply(m3);
    map_type_impl::mutation mmut2o{{}, {{int32_type->decompose(102), make_atomic_cell(utf8_type->decompose(sstring("102 override")))}}};
    mutation m2o(key, s);
    m2o.set_static_cell(column, my_map_type->serialize_mutation_form(mmut2o));
    cf.apply(m2o);

    row& r = cf.find_or_create_partition_slow(key).static_row();
    auto i = r.find_cell(column.id);
    BOOST_REQUIRE(i);
    auto cell = i->as_collection_mutation();
    auto muts = my_map_type->deserialize_mutation_form(cell);
    BOOST_REQUIRE(muts.cells.size() == 3);
    // FIXME: more strict tests
}

BOOST_AUTO_TEST_CASE(test_set_mutations) {
    auto my_set_type = set_type_impl::get_instance(int32_type, true);
    auto s = make_lw_shared(schema({}, some_keyspace, some_column_family,
        {{"p1", utf8_type}}, {{"c1", int32_type}}, {}, {{"s1", my_set_type}}, utf8_type));
    column_family cf(s);
    auto key = partition_key::from_exploded(*s, {to_bytes("key1")});
    auto& column = *s->get_column_definition("s1");
    map_type_impl::mutation mmut1{{}, {{int32_type->decompose(101), make_atomic_cell({})}}};
    mutation m1(key, s);
    m1.set_static_cell(column, my_set_type->serialize_mutation_form(mmut1));
    cf.apply(m1);
    map_type_impl::mutation mmut2{{}, {{int32_type->decompose(102), make_atomic_cell({})}}};
    mutation m2(key, s);
    m2.set_static_cell(column, my_set_type->serialize_mutation_form(mmut2));
    cf.apply(m2);
    map_type_impl::mutation mmut3{{}, {{int32_type->decompose(103), make_atomic_cell({})}}};
    mutation m3(key, s);
    m3.set_static_cell(column, my_set_type->serialize_mutation_form(mmut3));
    cf.apply(m3);
    map_type_impl::mutation mmut2o{{}, {{int32_type->decompose(102), make_atomic_cell({})}}};
    mutation m2o(key, s);
    m2o.set_static_cell(column, my_set_type->serialize_mutation_form(mmut2o));
    cf.apply(m2o);

    row& r = cf.find_or_create_partition_slow(key).static_row();
    auto i = r.find_cell(column.id);
    BOOST_REQUIRE(i);
    auto cell = i->as_collection_mutation();
    auto muts = my_set_type->deserialize_mutation_form(cell);
    BOOST_REQUIRE(muts.cells.size() == 3);
    // FIXME: more strict tests
}

BOOST_AUTO_TEST_CASE(test_list_mutations) {
    auto my_list_type = list_type_impl::get_instance(int32_type, true);
    auto s = make_lw_shared(schema({}, some_keyspace, some_column_family,
        {{"p1", utf8_type}}, {{"c1", int32_type}}, {}, {{"s1", my_list_type}}, utf8_type));
    column_family cf(s);
    auto key = partition_key::from_exploded(*s, {to_bytes("key1")});
    auto& column = *s->get_column_definition("s1");
    auto make_key = [] { return timeuuid_type->decompose(utils::UUID_gen::get_time_UUID()); };
    collection_type_impl::mutation mmut1{{}, {{make_key(), make_atomic_cell(int32_type->decompose(101))}}};
    mutation m1(key, s);
    m1.set_static_cell(column, my_list_type->serialize_mutation_form(mmut1));
    cf.apply(m1);
    collection_type_impl::mutation mmut2{{}, {{make_key(), make_atomic_cell(int32_type->decompose(102))}}};
    mutation m2(key, s);
    m2.set_static_cell(column, my_list_type->serialize_mutation_form(mmut2));
    cf.apply(m2);
    collection_type_impl::mutation mmut3{{}, {{make_key(), make_atomic_cell(int32_type->decompose(103))}}};
    mutation m3(key, s);
    m3.set_static_cell(column, my_list_type->serialize_mutation_form(mmut3));
    cf.apply(m3);
    collection_type_impl::mutation mmut2o{{}, {{make_key(), make_atomic_cell(int32_type->decompose(102))}}};
    mutation m2o(key, s);
    m2o.set_static_cell(column, my_list_type->serialize_mutation_form(mmut2o));
    cf.apply(m2o);

    row& r = cf.find_or_create_partition_slow(key).static_row();
    auto i = r.find_cell(column.id);
    BOOST_REQUIRE(i);
    auto cell = i->as_collection_mutation();
    auto muts = my_list_type->deserialize_mutation_form(cell);
    BOOST_REQUIRE(muts.cells.size() == 4);
    // FIXME: more strict tests
}

BOOST_AUTO_TEST_CASE(test_multiple_memtables_one_partition) {
    auto s = make_lw_shared(schema({}, some_keyspace, some_column_family,
        {{"p1", utf8_type}}, {{"c1", int32_type}}, {{"r1", int32_type}}, {}, utf8_type));

    column_family cf(s);

    const column_definition& r1_col = *s->get_column_definition("r1");
    auto key = partition_key::from_exploded(*s, {to_bytes("key1")});

    auto insert_row = [&] (int32_t c1, int32_t r1) {
        auto c_key = clustering_key::from_exploded(*s, {int32_type->decompose(c1)});
        mutation m(key, s);
        m.set_clustered_cell(c_key, r1_col, make_atomic_cell(int32_type->decompose(r1)));
        cf.apply(std::move(m));
        cf.seal_active_memtable();
    };
    insert_row(1001, 2001);
    insert_row(1002, 2002);
    insert_row(1003, 2003);

    auto verify_row = [&] (int32_t c1, int32_t r1) {
        auto c_key = clustering_key::from_exploded(*s, {int32_type->decompose(c1)});
        auto r = cf.find_row(dht::global_partitioner().decorate_key(*s, key), c_key);
        BOOST_REQUIRE(r);
        auto i = r->find_cell(r1_col.id);
        BOOST_REQUIRE(i);
        auto cell = i->as_atomic_cell();
        BOOST_REQUIRE(cell.is_live());
        BOOST_REQUIRE(int32_type->equal(cell.value(), int32_type->decompose(r1)));
    };
    verify_row(1001, 2001);
    verify_row(1002, 2002);
    verify_row(1003, 2003);
}

BOOST_AUTO_TEST_CASE(test_multiple_memtables_multiple_partitions) {
    auto s = make_lw_shared(schema({}, some_keyspace, some_column_family,
        {{"p1", int32_type}}, {{"c1", int32_type}}, {{"r1", int32_type}}, {}, utf8_type));

    column_family cf(s);
    std::map<int32_t, std::map<int32_t, int32_t>> shadow, result;

    const column_definition& r1_col = *s->get_column_definition("r1");

    api::timestamp_type ts = 0;
    auto insert_row = [&] (int32_t p1, int32_t c1, int32_t r1) {
        auto key = partition_key::from_exploded(*s, {int32_type->decompose(p1)});
        auto c_key = clustering_key::from_exploded(*s, {int32_type->decompose(c1)});
        mutation m(key, s);
        m.set_clustered_cell(c_key, r1_col, atomic_cell::make_live(ts++, int32_type->decompose(r1)));
        cf.apply(std::move(m));
        shadow[p1][c1] = r1;
    };
    std::minstd_rand random_engine;
    std::normal_distribution<> pk_distribution(0, 10);
    std::normal_distribution<> ck_distribution(0, 5);
    std::normal_distribution<> r_distribution(0, 100);
    for (unsigned i = 0; i < 10; ++i) {
        for (unsigned j = 0; j < 100; ++j) {
            insert_row(pk_distribution(random_engine), ck_distribution(random_engine), r_distribution(random_engine));
        }
        cf.seal_active_memtable();
    }

    cf.for_all_partitions_slow([&] (const dht::decorated_key& pk, const mutation_partition& mp) {
        auto p1 = boost::any_cast<int32_t>(int32_type->deserialize(pk._key.explode(*s)[0]));
        for (const rows_entry& re : mp.range(*s, query::range<clustering_key_prefix>())) {
            auto c1 = boost::any_cast<int32_t>(int32_type->deserialize(re.key().explode(*s)[0]));
            auto cell = re.row().cells().find_cell(r1_col.id);
            if (cell) {
                result[p1][c1] = boost::any_cast<int32_t>(int32_type->deserialize(cell->as_atomic_cell().value()));
            }
        }
        return true;
    });
    BOOST_REQUIRE(shadow == result);
}

BOOST_AUTO_TEST_CASE(test_cell_ordering) {
    auto now = gc_clock::now();
    auto ttl_1 = gc_clock::duration(1);
    auto ttl_2 = gc_clock::duration(2);
    auto expiry_1 = now + ttl_1;
    auto expiry_2 = now + ttl_2;

    auto assert_order = [] (atomic_cell_view first, atomic_cell_view second) {
        if (compare_atomic_cell_for_merge(first, second) >= 0) {
            BOOST_FAIL(sprint("Expected %s < %s", first, second));
        }
        if (compare_atomic_cell_for_merge(second, first) <= 0) {
            BOOST_FAIL(sprint("Expected %s < %s", second, first));
        }
    };

    auto assert_equal = [] (atomic_cell_view c1, atomic_cell_view c2) {
        BOOST_REQUIRE(compare_atomic_cell_for_merge(c1, c2) == 0);
        BOOST_REQUIRE(compare_atomic_cell_for_merge(c2, c1) == 0);
    };

    assert_equal(
        atomic_cell::make_live(0, bytes("value")),
        atomic_cell::make_live(0, bytes("value")));

    assert_equal(
        atomic_cell::make_live(1, bytes("value"), expiry_1, ttl_1),
        atomic_cell::make_live(1, bytes("value")));

    assert_equal(
        atomic_cell::make_dead(1, expiry_1),
        atomic_cell::make_dead(1, expiry_1));

    // If one cell doesn't have an expiry, Origin considers them equal.
    assert_equal(
        atomic_cell::make_live(1, bytes(), expiry_2, ttl_2),
        atomic_cell::make_live(1, bytes()));

    // Origin doesn't compare ttl (is it wise?)
    assert_equal(
        atomic_cell::make_live(1, bytes("value"), expiry_1, ttl_1),
        atomic_cell::make_live(1, bytes("value"), expiry_1, ttl_2));

    assert_order(
        atomic_cell::make_live(0, bytes("value1")),
        atomic_cell::make_live(0, bytes("value2")));

    assert_order(
        atomic_cell::make_live(0, bytes("value12")),
        atomic_cell::make_live(0, bytes("value2")));

    // Live cells are ordered first by timestamp...
    assert_order(
        atomic_cell::make_live(0, bytes("value2")),
        atomic_cell::make_live(1, bytes("value1")));

    // ..then by value
    assert_order(
        atomic_cell::make_live(1, bytes("value1"), expiry_2, ttl_2),
        atomic_cell::make_live(1, bytes("value2"), expiry_1, ttl_1));

    // ..then by expiry
    assert_order(
        atomic_cell::make_live(1, bytes(), expiry_1, ttl_1),
        atomic_cell::make_live(1, bytes(), expiry_2, ttl_1));

    // Dead wins
    assert_order(
        atomic_cell::make_live(1, bytes("value")),
        atomic_cell::make_dead(1, expiry_1));

    // Dead wins with expiring cell
    assert_order(
        atomic_cell::make_live(1, bytes("value"), expiry_2, ttl_2),
        atomic_cell::make_dead(1, expiry_1));

    // Deleted cells are ordered first by timestamp
    assert_order(
        atomic_cell::make_dead(1, expiry_2),
        atomic_cell::make_dead(2, expiry_1));

    // ...then by expiry
    assert_order(
        atomic_cell::make_dead(1, expiry_1),
        atomic_cell::make_dead(1, expiry_2));
}
