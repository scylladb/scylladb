/*
 * Copyright 2015 Cloudius Systems
 */

#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE core

#include <boost/test/unit_test.hpp>
#include "compound.hh"
#include "tests/urchin/range_assert.hh"

static std::vector<bytes> to_bytes_vec(std::vector<sstring> values) {
    std::vector<bytes> result;
    for (auto&& v : values) {
        result.emplace_back(to_bytes(v));
    }
    return result;
}

template <typename Compound>
static
range_assert<typename Compound::iterator>
assert_that_components(Compound& t, bytes packed) {
    return assert_that_range(t.begin(packed), t.end(packed));
}

template <typename Compound>
static void test_sequence(Compound& t, std::vector<sstring> strings) {
    auto packed = t.serialize_value(to_bytes_vec(strings));
    assert_that_components(t, packed).equals(to_bytes_vec(strings));
};

BOOST_AUTO_TEST_CASE(test_iteration_over_non_prefixable_tuple) {
    compound_type<allow_prefixes::no> t({bytes_type, bytes_type, bytes_type});

    test_sequence(t, {"el1", "el2", "el3"});
    test_sequence(t, {"el1", "el2", ""});
    test_sequence(t, {"",    "el2", "el3"});
    test_sequence(t, {"el1", "",    ""});
    test_sequence(t, {"",    "",    "el3"});
    test_sequence(t, {"el1", "",    "el3"});
    test_sequence(t, {"",    "",    ""});
}

BOOST_AUTO_TEST_CASE(test_iteration_over_prefixable_tuple) {
    compound_type<allow_prefixes::yes> t({bytes_type, bytes_type, bytes_type});

    test_sequence(t, {"el1", "el2", "el3"});
    test_sequence(t, {"el1", "el2", ""});
    test_sequence(t, {"",    "el2", "el3"});
    test_sequence(t, {"el1", "",    ""});
    test_sequence(t, {"",    "",    "el3"});
    test_sequence(t, {"el1", "",    "el3"});
    test_sequence(t, {"",    "",    ""});

    test_sequence(t, {"el1", "el2", ""});
    test_sequence(t, {"el1", "el2"});
    test_sequence(t, {"el1", ""});
    test_sequence(t, {"el1"});
    test_sequence(t, {""});
    test_sequence(t, {});
}

BOOST_AUTO_TEST_CASE(test_iteration_over_non_prefixable_singular_tuple) {
    compound_type<allow_prefixes::no> t({bytes_type});

    test_sequence(t, {"el1"});
    test_sequence(t, {""});
}

BOOST_AUTO_TEST_CASE(test_iteration_over_prefixable_singular_tuple) {
    compound_type<allow_prefixes::yes> t({bytes_type});

    test_sequence(t, {"elem1"});
    test_sequence(t, {""});
    test_sequence(t, {});
}

template <allow_prefixes AllowPrefixes>
void do_test_conversion_methods_for_singular_compound() {
    compound_type<AllowPrefixes> t({bytes_type});

    {
        assert_that_components(t, t.serialize_value(to_bytes_vec({"asd"}))) // r-value version
            .equals(to_bytes_vec({"asd"}));
    }

    {
        auto vec = to_bytes_vec({"asd"});
        assert_that_components(t, t.serialize_value(vec)) // l-value version
            .equals(to_bytes_vec({"asd"}));
    }

    {
        std::vector<bytes_opt> vec = { bytes_opt("asd") };
        assert_that_components(t, t.serialize_optionals(vec))
            .equals(to_bytes_vec({"asd"}));
    }

    {
        std::vector<bytes_opt> vec = { bytes_opt("asd") };
        assert_that_components(t, t.serialize_optionals(std::move(vec))) // r-value
            .equals(to_bytes_vec({"asd"}));
    }

    {
        assert_that_components(t, t.serialize_single(bytes("asd")))
            .equals(to_bytes_vec({"asd"}));
    }
}

BOOST_AUTO_TEST_CASE(test_conversion_methods_for_singular_compound) {
    do_test_conversion_methods_for_singular_compound<allow_prefixes::yes>();
    do_test_conversion_methods_for_singular_compound<allow_prefixes::no>();
}

template <allow_prefixes AllowPrefixes>
void do_test_conversion_methods_for_non_singular_compound() {
    compound_type<AllowPrefixes> t({bytes_type, bytes_type, bytes_type});

    {
        assert_that_components(t, t.serialize_value(to_bytes_vec({"el1", "el2", "el2"}))) // r-value version
            .equals(to_bytes_vec({"el1", "el2", "el2"}));
    }

    {
        auto vec = to_bytes_vec({"el1", "el2", "el3"});
        assert_that_components(t, t.serialize_value(vec)) // l-value version
            .equals(to_bytes_vec({"el1", "el2", "el3"}));
    }

    {
        std::vector<bytes_opt> vec = { bytes_opt("el1"), bytes_opt("el2"), bytes_opt("el3") };
        assert_that_components(t, t.serialize_optionals(vec))
            .equals(to_bytes_vec({"el1", "el2", "el3"}));
    }

    {
        std::vector<bytes_opt> vec = { bytes_opt("el1"), bytes_opt("el2"), bytes_opt("el3") };
        assert_that_components(t, t.serialize_optionals(std::move(vec))) // r-value
            .equals(to_bytes_vec({"el1", "el2", "el3"}));
    }
}

BOOST_AUTO_TEST_CASE(test_conversion_methods_for_non_singular_compound) {
    do_test_conversion_methods_for_non_singular_compound<allow_prefixes::yes>();
    do_test_conversion_methods_for_non_singular_compound<allow_prefixes::no>();
}
