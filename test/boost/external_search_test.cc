/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// Unit tests for what the ANN() and BM25() searches share, in cql3/statements/external_search.
//
// The queries themselves are covered by test/cqlpy/test_invalid_ann_queries.py,
// test/cqlpy/test_fulltext_index.py and the mock suites beside them, but they cannot cover all of
// unevaluated_equality(): `never` shows up as a rejection at prepare and `unknown` as one at
// execution, while `always` shows up as nothing at all - deferring the comparison instead reaches
// the same verdict, so no query can tell the two apart. It is also the answer that has to be
// right: `never` and `unknown` at worst report an error at the wrong time, but an `always` that
// should not have been given accepts a query whose selected term is not the one searched with.

#include <boost/test/unit_test.hpp>

#include "cql3/column_identifier.hh"
#include "cql3/column_specification.hh"
#include "cql3/expr/expression.hh"
#include "cql3/functions/native_scalar_function.hh"
#include "cql3/statements/external_search/external_function.hh"
#include "test/lib/expr_test_utils.hh"
#include "types/types.hh"
#include "types/vector.hh"

#include <seastar/core/shared_ptr.hh>

using namespace cql3;
using namespace cql3::expr;
using namespace cql3::expr::test_utils;

using cql3::statements::external_search::equality;
using cql3::statements::external_search::unevaluated_equality;

BOOST_AUTO_TEST_SUITE(external_search_test)

namespace {

/// A bind marker standing for a query value. Each call synthesises its own receiver, the way
/// preparing each occurrence of one marker does - which is what makes two of them for one variable
/// compare unequal structurally, while the value they stand for is the same.
expression marker(int32_t bind_index, data_type type) {
    return bind_variable {
        .bind_index = bind_index,
        .receiver = make_lw_shared<column_specification>("ks", "tab",
                ::make_shared<column_identifier>("?", true), std::move(type)),
    };
}

/// A query vector of literals, as prepare folds one: into the value itself.
expression folded_vector(std::vector<constant> elements) {
    return make_vector_const(elements, float_type);
}

} // anonymous namespace

BOOST_AUTO_TEST_CASE(unevaluated_equality_literals) {
    // A folded query vector arrives as the value itself, so a pair of them is decided either way.
    BOOST_REQUIRE(unevaluated_equality(folded_vector({make_float_const(0.1f), make_float_const(0.2f)}),
                                       folded_vector({make_float_const(0.1f), make_float_const(0.2f)})) == equality::always);
    BOOST_REQUIRE(unevaluated_equality(folded_vector({make_float_const(0.1f), make_float_const(0.2f)}),
                                       folded_vector({make_float_const(0.1f), make_float_const(0.3f)})) == equality::never);
    BOOST_REQUIRE(unevaluated_equality(folded_vector({make_float_const(0.1f)}),
                                       folded_vector({make_float_const(0.1f), make_float_const(0.2f)})) == equality::never);

    // A BM25() search term is one value rather than a vector of them, and is decided the same way.
    BOOST_REQUIRE(unevaluated_equality(make_text_const("hello"), make_text_const("hello")) == equality::always);
    BOOST_REQUIRE(unevaluated_equality(make_text_const("hello"), make_text_const("world")) == equality::never);
}

BOOST_AUTO_TEST_CASE(unevaluated_equality_bind_markers) {
    // One variable written twice is two nodes with two receivers and one value. This is what
    // expression::operator== gets wrong, because it compares the receivers.
    BOOST_REQUIRE(unevaluated_equality(marker(0, utf8_type), marker(0, utf8_type)) == equality::always);
    // Two variables may still be given one value, and a marker may be given what a literal holds.
    BOOST_REQUIRE(unevaluated_equality(marker(0, utf8_type), marker(1, utf8_type)) == equality::unknown);
    BOOST_REQUIRE(unevaluated_equality(marker(0, utf8_type), make_text_const("hello")) == equality::unknown);
    BOOST_REQUIRE(unevaluated_equality(make_text_const("hello"), marker(0, utf8_type)) == equality::unknown);

    // A whole query vector bound at once is the same case.
    const auto vector_type = data_type(vector_type_impl::get_instance(float_type, 2));
    BOOST_REQUIRE(unevaluated_equality(marker(0, vector_type), marker(0, vector_type)) == equality::always);
    BOOST_REQUIRE(unevaluated_equality(marker(0, vector_type),
                                       folded_vector({make_float_const(0.1f), make_float_const(0.2f)})) == equality::unknown);
}

BOOST_AUTO_TEST_CASE(unevaluated_equality_leaves_the_rest_to_execution) {
    // A marker among the elements of a query vector leaves it unfolded, and the two are then
    // compared once they have been evaluated rather than reasoned about here.
    auto partly_bound = [] () {
        return expression(make_vector_constructor({make_float_const(0.1f), marker(1, float_type)},
                                                  float_type, 2));
    };
    BOOST_REQUIRE(unevaluated_equality(partly_bound(), partly_bound()) == equality::unknown);
    BOOST_REQUIRE(unevaluated_equality(partly_bound(),
                                       folded_vector({make_float_const(0.1f), make_float_const(0.2f)})) == equality::unknown);

    // So is a search term written as a function call or as a cast: two of those may well compute
    // one value, and proving when they do is not worth the code.
    auto fn = functions::make_native_scalar_function<true>(
            "external_search_test_fn", utf8_type, std::vector<data_type>{utf8_type},
            [] (std::span<const bytes_opt> args) -> bytes_opt { return args[0]; });
    auto call = [&] () {
        return expression(function_call{.func = fn, .args = {marker(0, utf8_type)}});
    };
    BOOST_REQUIRE(unevaluated_equality(call(), call()) == equality::unknown);

    auto to_text = [] () {
        return expression(cast{.style = cast::cast_style::c, .arg = marker(0, utf8_type), .type = utf8_type});
    };
    BOOST_REQUIRE(unevaluated_equality(to_text(), to_text()) == equality::unknown);
}

BOOST_AUTO_TEST_SUITE_END()
