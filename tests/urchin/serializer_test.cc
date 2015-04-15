/*
 * Copyright 2015 Cloudius Systems
 */

#define BOOST_TEST_DYN_LINK

#include <boost/test/unit_test.hpp>
#include <stdlib.h>
#include <iostream>
#include <unordered_map>

#include "tests/test-utils.hh"
#include "core/future-util.hh"
#include "utils/UUID_gen.hh"
#include "db/serializer.hh"

using namespace db;

template<typename T>
static T serialize_deserialize(database& db, const T& t) {
    db::serializer<T> sz(db, t);
    bytes tmp(bytes::initialized_later(), sz.size());
    data_output out(tmp);
    sz(out);
    data_input in(tmp);
    T n = db::serializer<T>::read(db, in);
    if (in.avail() > 0) {
        throw std::runtime_error("Did not consume all bytes");
    }
    return std::move(n);
}

SEASTAR_TEST_CASE(test_sstring){
    database db;

    std::initializer_list<sstring> values = {
            "kow", "abcdefghIJKL78&%\"\r", "\xff\xff"
    };
    for (auto& v : values) {
        auto nv = serialize_deserialize(db, v);
        BOOST_CHECK_EQUAL(nv, v);
    }
    return make_ready_future<>();
}

namespace utils {
inline std::ostream& operator<<(std::ostream& os, const utils::UUID& uuid) {
    return os << uuid.to_bytes();
}
}

SEASTAR_TEST_CASE(test_uuid){
    database db;

    std::initializer_list<utils::UUID> values = {
            utils::UUID_gen::get_time_UUID(), utils::make_random_uuid()
    };
    for (auto& v : values) {
        auto nv = serialize_deserialize(db, v);
        BOOST_CHECK_EQUAL(nv, v);
    }
    return make_ready_future<>();
}

SEASTAR_TEST_CASE(test_tombstone){
    database db;

    std::initializer_list<tombstone> values = {
            //tombstone(),
            tombstone(12, gc_clock::now())
    };
    for (auto& v : values) {
        auto nv = serialize_deserialize(db, v);
        BOOST_CHECK_EQUAL(nv, v);
    }
    return make_ready_future<>();
}

inline bool operator==(const atomic_cell_or_collection& c1, const atomic_cell_or_collection& c2) {
    return c1.as_collection_mutation().data == c2.as_collection_mutation().data;
}

inline std::ostream& operator<<(std::ostream& os, const atomic_cell_or_collection& c) {
    return os << c.as_collection_mutation().data;
}

SEASTAR_TEST_CASE(test_atomic_cell_or_collection){
    database db;

    std::initializer_list<atomic_cell_or_collection> values = {
            atomic_cell_or_collection(atomic_cell::from_bytes("nufflo"))
            , atomic_cell_or_collection(atomic_cell::from_bytes("abcdefghIJKL78&%\"\r"))
    };
    for (auto& v : values) {
        auto nv = serialize_deserialize(db, v);
        BOOST_CHECK_EQUAL(nv, v);
    }
    return make_ready_future<>();
}

template<typename... Args>
inline std::ostream& operator<<(std::ostream& os, const std::map<Args...>& v) {
    os << "{ ";
    int n = 0;
    for (auto& p : v) {
        if (n > 0) {
            os << ", ";
        }
        os << "{ " << p.first << ", " << p.second << " }";
        ++n;
    }
    return os << " }";
}

SEASTAR_TEST_CASE(test_row){
    database db;

    std::initializer_list<row> values = {
            {
                    { 1, atomic_cell_or_collection(atomic_cell::from_bytes("nufflo")) },
                    { 2, atomic_cell_or_collection(atomic_cell::from_bytes("abcdefghIJKL78&%\"\r")) },
            },
            {
                    { 1, atomic_cell_or_collection(atomic_cell::from_bytes("123453646737373")) },
                    { 2, atomic_cell_or_collection(atomic_cell::from_bytes("_._j,;...8&%\"\r")) },
                    { 3, atomic_cell_or_collection(atomic_cell::from_bytes("<>><sdfsfasa")) },
            },
    };
    for (auto& v : values) {
        auto nv = serialize_deserialize(db, v);
        BOOST_CHECK_EQUAL(nv, v);
    }
    return make_ready_future<>();
}

// stolen from mutation_test

static sstring some_keyspace("ks");
static sstring some_column_family("cf");

static atomic_cell make_atomic_cell(bytes value) {
    return atomic_cell::make_live(0, ttl_opt{}, std::move(value));
}

inline bool operator==(const deletable_row& r1, const deletable_row& r2) {
    return r1.t == r2.t && r1.cells == r2.cells;
}

inline bool operator==(const partition_key& m1, const partition_key& m2) {
    const bytes_view& b1 = m1;
    const bytes_view& b2 = m2;
    return b1 == b2;
}

inline bool operator==(const rows_entry& r1, const rows_entry& r2) {
    return r1.row() == r2.row();
}

inline bool operator!=(const rows_entry& r1, const rows_entry& r2) {
    return !(r1 == r2);
}

// TODO: not complete... meh...
inline bool operator==(const mutation_partition& cp1, const mutation_partition& cp2) {
    static schema dummy("", "", {}, {}, {}, {}, utf8_type);
    auto& p1 = const_cast<mutation_partition&>(cp1);
    auto& p2 = const_cast<mutation_partition&>(cp2);
    return p1.static_row() == p2.static_row()
            && p1.range(dummy, query::range<clustering_key_prefix>::make_open_ended_both_sides())
            == p2.range(dummy, query::range<clustering_key_prefix>::make_open_ended_both_sides())
            ;
}

inline bool operator==(const mutation& m1, const mutation& m2) {
    return m1.schema.get() == m2.schema.get()
            && m1.key == m2.key
            && m1.p == m2.p
            ;
}

SEASTAR_TEST_CASE(test_mutation){
    auto s = make_lw_shared(schema(some_keyspace, some_column_family,
        {{"p1", utf8_type}}, {{"c1", int32_type}}, {{"r1", int32_type}}, {}, utf8_type));

    database db;
    db.add_keyspace(some_keyspace, keyspace());
    db.add_column_family(column_family(s));

    auto& r1_col = *s->get_column_definition("r1");
    auto key = partition_key::from_exploded(*s, {to_bytes("key1")});
    auto c_key = clustering_key::from_exploded(*s, {int32_type->decompose(2)});

    mutation m(key, s);
    m.set_clustered_cell(c_key, r1_col, make_atomic_cell(int32_type->decompose(3)));

    auto nv = serialize_deserialize(db, m);
    BOOST_CHECK_EQUAL(nv, m);

    return make_ready_future<>();
}
