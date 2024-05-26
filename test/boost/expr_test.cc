// Copyright (C) 2023-present ScyllaDB
// SPDX-License-Identifier: AGPL-3.0-or-later

#include "cql3/column_identifier.hh"
#include "cql3/util.hh"
#include <seastar/core/shared_ptr.hh>
#include "types/types.hh"
#include "types/list.hh"
#include "types/map.hh"
#include <boost/test/tools/old/interface.hpp>
#define BOOST_TEST_MODULE core

#include <boost/test/unit_test.hpp>
#include <utility>
#include <fmt/ranges.h>
#include "cql3/expr/expression.hh"
#include "utils/overloaded_functor.hh"
#include "utils/to_string.hh"
#include <cassert>
#include "cql3/query_options.hh"
#include "types/set.hh"
#include "types/user.hh"
#include "test/lib/expr_test_utils.hh"
#include "test/lib/test_utils.hh"
#include "cql3/expr/evaluate.hh"
#include "cql3/expr/expr-utils.hh"

using namespace cql3;
using namespace cql3::expr;
using namespace cql3::expr::test_utils;

bind_variable new_bind_variable(int bind_index, data_type type = int32_type) {
    return bind_variable {
        .bind_index = bind_index,
        .receiver = make_lw_shared<column_specification>("ks", "tab", make_shared<column_identifier>("?", true), std::move(type)),
    };
}

BOOST_AUTO_TEST_CASE(expr_visit_get_int) {
    expression e = new_bind_variable(1245);

    int read_value = visit(overloaded_functor {
        [](const bind_variable& bv) -> int { return bv.bind_index; },
        [](const auto&) -> int { throw std::runtime_error("Unreachable"); }
    }, e);

    BOOST_REQUIRE_EQUAL(read_value, 1245);
}

BOOST_AUTO_TEST_CASE(expr_visit_void_return) {
    expression e = new_bind_variable(1245);

    visit(overloaded_functor {
        [](const bind_variable& bv) { BOOST_REQUIRE_EQUAL(bv.bind_index, 1245); },
        [](const auto&) { throw std::runtime_error("Unreachable"); }
    }, e);
}

BOOST_AUTO_TEST_CASE(expr_visit_const_ref) {
    const expression e = new_bind_variable(123);

    const bind_variable& ref = visit(overloaded_functor {
        [](const bind_variable& bv) -> const bind_variable& { return bv; },
        [](const auto&) -> const bind_variable& { throw std::runtime_error("Unreachable"); }
    }, e);

    BOOST_REQUIRE_EQUAL(ref.bind_index, 123);
}

BOOST_AUTO_TEST_CASE(expr_visit_ref) {
    expression e = new_bind_variable(456);

    bind_variable& ref = visit(overloaded_functor {
        [](bind_variable& bv) -> bind_variable& { return bv; },
        [](auto&) -> bind_variable& { throw std::runtime_error("Unreachable"); }
    }, e);

    BOOST_REQUIRE_EQUAL(ref.bind_index, 456);

    ref.bind_index = 135;

    bind_variable& ref2 = visit(overloaded_functor {
        [](bind_variable& bv) -> bind_variable& { return bv; },
        [](auto&) -> bind_variable& { throw std::runtime_error("Unreachable"); }
    }, e);

    BOOST_REQUIRE_EQUAL(ref2.bind_index, 135);
}


struct rvalue_visitor {
    rvalue_visitor(){};
    rvalue_visitor(const rvalue_visitor&) = delete;

    bind_variable& operator()(bind_variable& bv) && { return bv; }
    bind_variable& operator()(auto&) && { throw std::runtime_error("Unreachable"); }
} v;

BOOST_AUTO_TEST_CASE(expr_visit_visitor_rvalue) {
    expression e = new_bind_variable(456);

    rvalue_visitor visitor;

    bind_variable& ref2 = visit(std::move(visitor), e);

    BOOST_REQUIRE_EQUAL(ref2.bind_index, 456);
}

static sstring expr_print(const expression& e) {
    return format("{:user}", e);
}

static sstring value_print(const cql3::raw_value& v, const expression& e) {
    return expr_print(constant(v, type_of(e)));
}

static unresolved_identifier make_column(const char* col_name) {
    return unresolved_identifier{::make_shared<column_identifier_raw>(col_name, true)};
}

static function_call make_token(std::vector<expression> args) {
    return function_call {
        .func = functions::function_name::native_function("token"),
        .args = args
    };
}

BOOST_AUTO_TEST_CASE(expr_printer_test) {
    expression col_eq_1234 = binary_operator(
        make_column("col"),
        oper_t::EQ,
        make_int_const(1234)
    );
    BOOST_REQUIRE_EQUAL(expr_print(col_eq_1234), "col = 1234");

    expression token_p1_p2_lt_min_56 = binary_operator(
        make_token({
            unresolved_identifier{::make_shared<column_identifier_raw>("p1", true)},
            unresolved_identifier{::make_shared<column_identifier_raw>("p2", true)},
        }),
        oper_t::LT,
        make_int_const(-56)
    );
    BOOST_REQUIRE_EQUAL(expr_print(token_p1_p2_lt_min_56), "token(p1, p2) < -56");
}

BOOST_AUTO_TEST_CASE(expr_printer_string_test) {
    constant utf8_val(raw_value::make_value(utf8_type->decompose("abcdef")), utf8_type);
    BOOST_REQUIRE_EQUAL(expr_print(utf8_val), "'abcdef'");

    constant ascii_val(raw_value::make_value(ascii_type->decompose("abcdef")), utf8_type);
    BOOST_REQUIRE_EQUAL(expr_print(ascii_val), "'abcdef'");
}

BOOST_AUTO_TEST_CASE(expr_printer_inet_test) {
    constant inet_const(
        raw_value::make_value(inet_addr_type->from_string("1.2.3.4")),
        inet_addr_type
    );
    BOOST_REQUIRE_EQUAL(expr_print(inet_const), "'1.2.3.4'");
}

BOOST_AUTO_TEST_CASE(expr_printer_timestamp_test) {
    constant timestamp_const (
        raw_value::make_value(timestamp_type->from_string("2011-03-02T03:05:00+0000")),
        timestamp_type
    );
    BOOST_REQUIRE_EQUAL(expr_print(timestamp_const), "'2011-03-02T03:05:00.000Z'");
}

BOOST_AUTO_TEST_CASE(expr_printer_time_test) {
    constant time_const (
        raw_value::make_value(time_type->from_string("08:12:54")),
        time_type
    );
    BOOST_REQUIRE_EQUAL(expr_print(time_const), "'08:12:54.000000000'");
}

BOOST_AUTO_TEST_CASE(expr_printer_date_test) {
    constant date_const {
        raw_value::make_value(date_type->from_string("2011-02-03+0000")),
        date_type
    };
    BOOST_REQUIRE_EQUAL(expr_print(date_const), "'2011-02-03T00:00:00.000Z'");
}

BOOST_AUTO_TEST_CASE(expr_printer_duration_test) {
    constant duration_const {
        raw_value::make_value(duration_type->from_string("89h4m48s")),
        duration_type
    };
    BOOST_REQUIRE_EQUAL(expr_print(duration_const), "89h4m48s");
}

BOOST_AUTO_TEST_CASE(expr_printer_list_test) {
    collection_constructor int_list {
        .style = collection_constructor::style_type::list,
        .elements = {make_int_const(13), make_int_const(45), make_int_const(90)},
        .type = list_type_impl::get_instance(int32_type, true)
    };
    BOOST_REQUIRE_EQUAL(expr_print(int_list), "[13, 45, 90]");

    collection_constructor frozen_int_list {
        .style = collection_constructor::style_type::list,
        .elements = {make_int_const(13), make_int_const(45), make_int_const(90)},
        .type = list_type_impl::get_instance(int32_type, false)
    };
    BOOST_REQUIRE_EQUAL(expr_print(frozen_int_list), "[13, 45, 90]");

    cql3::raw_value int_list_constant = evaluate(int_list, query_options::DEFAULT);
    BOOST_REQUIRE_EQUAL(value_print(int_list_constant, int_list), "[13, 45, 90]");

    cql3::raw_value frozen_int_list_constant = evaluate(frozen_int_list, query_options::DEFAULT);
    BOOST_REQUIRE_EQUAL(value_print(frozen_int_list_constant, frozen_int_list), "[13, 45, 90]");
}

BOOST_AUTO_TEST_CASE(expr_printer_set_test) {
    collection_constructor int_set {
        .style = collection_constructor::style_type::set,
        .elements = {make_int_const(13), make_int_const(45), make_int_const(90)},
        .type = set_type_impl::get_instance(int32_type, true)
    };
    BOOST_REQUIRE_EQUAL(expr_print(int_set), "{13, 45, 90}");

    collection_constructor frozen_int_set {
        .style = collection_constructor::style_type::set,
        .elements = {make_int_const(13), make_int_const(45), make_int_const(90)},
        .type = set_type_impl::get_instance(int32_type, true)
    };
    BOOST_REQUIRE_EQUAL(expr_print(frozen_int_set), "{13, 45, 90}");

    cql3::raw_value int_set_constant = evaluate(int_set, query_options::DEFAULT);
    BOOST_REQUIRE_EQUAL(value_print(int_set_constant, int_set), "{13, 45, 90}");

    cql3::raw_value frozen_int_set_constant = evaluate(frozen_int_set, query_options::DEFAULT);
    BOOST_REQUIRE_EQUAL(value_print(frozen_int_set_constant, frozen_int_set), "{13, 45, 90}");
}

BOOST_AUTO_TEST_CASE(expr_printer_map_test) {
    collection_constructor int_int_map {
        .style = collection_constructor::style_type::map,
        .elements = {
            tuple_constructor {
                .elements = {make_int_const(12), make_int_const(34)}
            },
            tuple_constructor {
                .elements = {make_int_const(56), make_int_const(78)}
            }
        },
        .type = map_type_impl::get_instance(int32_type, int32_type, true)
    };
    BOOST_REQUIRE_EQUAL(expr_print(int_int_map), "{12:34, 56:78}");

    collection_constructor frozen_int_int_map {
        .style = collection_constructor::style_type::map,
        .elements = int_int_map.elements,
        .type = map_type_impl::get_instance(int32_type, int32_type, false)
    };
    BOOST_REQUIRE_EQUAL(expr_print(frozen_int_int_map), "{12:34, 56:78}");

    cql3::raw_value int_int_map_const = evaluate(int_int_map, query_options::DEFAULT);
    BOOST_REQUIRE_EQUAL(value_print(int_int_map_const, int_int_map), "{12:34, 56:78}");

    cql3::raw_value frozen_int_int_map_const = evaluate(frozen_int_int_map, query_options::DEFAULT);
    BOOST_REQUIRE_EQUAL(value_print(frozen_int_int_map_const, frozen_int_int_map), "{12:34, 56:78}");
}

BOOST_AUTO_TEST_CASE(expr_printer_tuple_test) {
    tuple_constructor int_int_tuple {
        .elements = {make_int_const(456), make_int_const(789)},
        .type = tuple_type_impl::get_instance({int32_type, int32_type})
    };
    BOOST_REQUIRE_EQUAL(expr_print(int_int_tuple), "(456, 789)");

    cql3::raw_value int_int_tuple_const = evaluate(int_int_tuple, query_options::DEFAULT);
    BOOST_REQUIRE_EQUAL(value_print(int_int_tuple_const, int_int_tuple), "(456, 789)");
}

BOOST_AUTO_TEST_CASE(expr_printer_usertype_test) {
    column_identifier field_a("a", true);
    column_identifier field_b("b", true);
    usertype_constructor::elements_map_type user_type_elements;
    user_type_elements.emplace(field_a, make_int_const(333));
    user_type_elements.emplace(field_b, make_int_const(666));
    usertype_constructor user_typ {
        .elements = user_type_elements,
        .type = user_type_impl::get_instance("ks", "expr_test_type", {field_a.name(), field_b.name()}, {int32_type, int32_type}, true)
    };
    BOOST_REQUIRE_EQUAL(expr_print(user_typ), "{b:666, a:333}");

    cql3::raw_value user_typ_const = evaluate(user_typ, query_options::DEFAULT);
    BOOST_REQUIRE_EQUAL(value_print(user_typ_const, user_typ), "{a:333, b:666}");
}

// When a list is printed as RHS of an IN binary_operator it should be printed as a tuple.
BOOST_AUTO_TEST_CASE(expr_printer_in_test) {
    collection_constructor int_list {
        .style = collection_constructor::style_type::list,
        .elements = {make_int_const(13), make_int_const(45), make_int_const(90)},
        .type = list_type_impl::get_instance(int32_type, true)
    };

    binary_operator a_in_int_list {
        make_column("a"),
        oper_t::IN,
        int_list
    };
    BOOST_REQUIRE_EQUAL(expr_print(a_in_int_list), "a IN (13, 45, 90)");

    cql3::raw_value int_list_const = evaluate(int_list, query_options::DEFAULT);

    binary_operator a_in_int_list_const {
        make_column("a"),
        oper_t::IN,
        constant(int_list_const, type_of(int_list))
    };
    BOOST_REQUIRE_EQUAL(expr_print(a_in_int_list_const), "a IN (13, 45, 90)");
}


// To easily test how many expressions work with expression::printer
// We can use a function that parses a string to expression and then
// print it using another function that uses printer
BOOST_AUTO_TEST_CASE(expr_printer_parse_and_print_test) {
    auto tests = {
        "col = 1234",
        "col != 1234",
        "col < 1234",
        "col <= 1234",
        "col >= 1234",
        "col > 1234",
        "col CONTAINS 1234",
        "col CONTAINS KEY 1234",
        "col IS NOT null",
        "col LIKE 'abc'",
        "token(p1, p2) > -3434",
        "col2 = (1, 2)",
        "col2 = {1, 2}",
        "col2 IN (1, 2, 3)",
        "col2 IN ((1, 2), (3, 4))",
        "(col1, col2) < (1, 2)",
        "(c1, c2) IN ((1, 2), (3, 4))",
        "col > ?",
        "col IN (1, 2, 3, ?, 4, null)"
    };

    for(const char* test : tests) {
        expression parsed_where = cql3::util::where_clause_to_relations(test);
        sstring printed_where = cql3::util::relations_to_where_clause(parsed_where);

        BOOST_REQUIRE_EQUAL(sstring(test), printed_where);
    }
}

BOOST_AUTO_TEST_CASE(boolean_factors_test) {
    BOOST_REQUIRE_EQUAL(boolean_factors(make_bool_const(true)), std::vector<expression>({make_bool_const(true)}));

    BOOST_REQUIRE_EQUAL(boolean_factors(constant::make_null(boolean_type)), std::vector<expression>({constant::make_null(boolean_type)}));

    bind_variable bv1{0};
    bind_variable bv2{1};
    bind_variable bv3{2};
    bind_variable bv4{3};

    BOOST_REQUIRE_EQUAL(
        boolean_factors(
            conjunction{std::vector<expression>({bv1, bv2, bv3, bv4})}
        ),
        std::vector<expression>(
            {bv1, bv2, bv3, bv4}
        )
    );

    BOOST_REQUIRE_EQUAL(
        boolean_factors(
            conjunction{
                std::vector<expression>({
                    make_conjunction(bv1, bv2),
                    make_conjunction(bv3, bv4)
                })
            }
        ),
        std::vector<expression>(
            {bv1, bv2, bv3, bv4}
        )
    );
}

BOOST_AUTO_TEST_CASE(evaluate_constant_null) {
    expression constant_null = constant::make_null();
    BOOST_REQUIRE_EQUAL(evaluate(constant_null, evaluation_inputs{}), raw_value::make_null());

    expression constant_null_with_type = constant::make_null(int32_type);
    BOOST_REQUIRE_EQUAL(evaluate(constant_null_with_type, evaluation_inputs{}), raw_value::make_null());
}

BOOST_AUTO_TEST_CASE(evaluate_constant_empty) {
    expression constant_empty_bool = constant(raw_value::make_value(bytes()), boolean_type);
    BOOST_REQUIRE(evaluate(constant_empty_bool, evaluation_inputs{}).is_empty_value());

    expression constant_empty_int = constant(raw_value::make_value(bytes()), int32_type);
    BOOST_REQUIRE(evaluate(constant_empty_int, evaluation_inputs{}).is_empty_value());

    expression constant_empty_text = constant(raw_value::make_value(bytes()), utf8_type);
    BOOST_REQUIRE_EQUAL(evaluate(constant_empty_text, evaluation_inputs{}), make_text_raw(""));
}

BOOST_AUTO_TEST_CASE(evaluate_constant_int) {
    expression const_int = make_int_const(723);
    BOOST_REQUIRE_EQUAL(evaluate(const_int, evaluation_inputs{}), make_int_raw(723));
}

// Creates a schema_ptr that can be used for testing
// The schema corresponds to a table created by:
// CREATE TABLE test_ks.test_cf (pk int, ck int, r int, s int static, primary key (pk, ck));
static schema_ptr make_simple_test_schema() {
    return schema_builder("test_ks", "test_cf")
        .with_column("pk", int32_type, column_kind::partition_key)
        .with_column("ck", int32_type, column_kind::clustering_key)
        .with_column("r", int32_type, column_kind::regular_column)
        .with_column("s", int32_type, column_kind::static_column)
        .build();
}

// Creates a schema_ptr with three partition key columns can be used for testing
// The schema corresponds to a table created by:
// CREATE TABLE test_ks.test_cf (pk1 int, pk2 int, pk3 int, ck int, r int, s int static, primary key (pk, ck));
static schema_ptr make_three_pk_schema() {
    return schema_builder("test_ks", "test_cf")
        .with_column("pk1", int32_type, column_kind::partition_key)
        .with_column("pk2", int32_type, column_kind::partition_key)
        .with_column("pk3", int32_type, column_kind::partition_key)
        .with_column("ck", int32_type, column_kind::clustering_key)
        .with_column("r", int32_type, column_kind::regular_column)
        .with_column("s", int32_type, column_kind::static_column)
        .build();
}

BOOST_AUTO_TEST_CASE(evaluate_partition_key_column) {
    schema_ptr test_schema = make_simple_test_schema();
    auto [inputs, inputs_data] = make_evaluation_inputs(test_schema, {
                                                                         {"pk", make_int_raw(1)},
                                                                         {"ck", make_int_raw(2)},
                                                                         {"r", make_int_raw(3)},
                                                                         {"s", make_int_raw(4)},
                                                                     });
    expression pk_val = column_value(test_schema->get_column_definition("pk"));
    raw_value val = evaluate(pk_val, inputs);
    BOOST_REQUIRE_EQUAL(val, make_int_raw(1));
}

BOOST_AUTO_TEST_CASE(evaluate_clustering_key_column) {
    schema_ptr test_schema = make_simple_test_schema();
    auto [inputs, inputs_data] = make_evaluation_inputs(test_schema, {
                                                                         {"pk", make_int_raw(1)},
                                                                         {"ck", make_int_raw(2)},
                                                                         {"r", make_int_raw(3)},
                                                                         {"s", make_int_raw(4)},
                                                                     });
    expression ck_val = column_value(test_schema->get_column_definition("ck"));
    raw_value val = evaluate(ck_val, inputs);
    BOOST_REQUIRE_EQUAL(val, make_int_raw(2));
}

BOOST_AUTO_TEST_CASE(evaluate_regular_column) {
    schema_ptr test_schema = make_simple_test_schema();
    auto [inputs, inputs_data] = make_evaluation_inputs(test_schema, {
                                                                         {"pk", make_int_raw(1)},
                                                                         {"ck", make_int_raw(2)},
                                                                         {"r", make_int_raw(3)},
                                                                         {"s", make_int_raw(4)},
                                                                     });
    expression r_val = column_value(test_schema->get_column_definition("r"));
    raw_value val = evaluate(r_val, inputs);
    BOOST_REQUIRE_EQUAL(val, make_int_raw(3));
}

BOOST_AUTO_TEST_CASE(evaluate_static_column) {
    schema_ptr test_schema = make_simple_test_schema();
    auto [inputs, inputs_data] = make_evaluation_inputs(test_schema, {
                                                                         {"pk", make_int_raw(1)},
                                                                         {"ck", make_int_raw(2)},
                                                                         {"r", make_int_raw(3)},
                                                                         {"s", make_int_raw(4)},
                                                                     });
    expression s_val = column_value(test_schema->get_column_definition("s"));
    raw_value val = evaluate(s_val, inputs);
    BOOST_REQUIRE_EQUAL(val, make_int_raw(4));
}

BOOST_AUTO_TEST_CASE(evaluate_column_value_does_not_perfrom_validation) {
    schema_ptr test_schema =
        schema_builder("test_ks", "test_cf").with_column("pk", int32_type, column_kind::partition_key).build();

    raw_value invalid_int_value = make_bool_raw(true);

    auto [inputs, inputs_data] = make_evaluation_inputs(test_schema, {{"pk", invalid_int_value}});

    expression pk_column = column_value(test_schema->get_column_definition("pk"));
    raw_value val = evaluate(pk_column, inputs);
    BOOST_REQUIRE_EQUAL(val, invalid_int_value);
}

BOOST_AUTO_TEST_CASE(evaluate_bind_variable) {
    schema_ptr test_schema = make_simple_test_schema();
    auto [inputs, inputs_data] = make_evaluation_inputs(test_schema,
                                                        {
                                                            {"pk", make_int_raw(1)},
                                                            {"ck", make_int_raw(2)},
                                                            {"r", make_int_raw(3)},
                                                            {"s", make_int_raw(4)},
                                                        },
                                                        {make_int_raw(123)});

    expression bind_var = bind_variable{.bind_index = 0, .receiver = make_receiver(int32_type, "bind_var_0")};

    raw_value val = evaluate(bind_var, inputs);
    BOOST_REQUIRE_EQUAL(val, make_int_raw(123));
}

BOOST_AUTO_TEST_CASE(evaluate_two_bind_variables) {
    schema_ptr test_schema = make_simple_test_schema();
    auto [inputs, inputs_data] = make_evaluation_inputs(test_schema,
                                                        {
                                                            {"pk", make_int_raw(1)},
                                                            {"ck", make_int_raw(2)},
                                                            {"r", make_int_raw(3)},
                                                            {"s", make_int_raw(4)},
                                                        },
                                                        {make_int_raw(123), make_int_raw(456)});

    expression bind_variable0 = bind_variable{.bind_index = 0, .receiver = make_receiver(int32_type, "bind_var_0")};

    expression bind_variable1 = bind_variable{.bind_index = 1, .receiver = make_receiver(int32_type, "bind_var_1")};

    raw_value val0 = evaluate(bind_variable0, inputs);
    BOOST_REQUIRE_EQUAL(val0, make_int_raw(123));

    raw_value val1 = evaluate(bind_variable1, inputs);
    BOOST_REQUIRE_EQUAL(val1, make_int_raw(456));
}

BOOST_AUTO_TEST_CASE(evaluate_bind_variable_performs_validation) {
    schema_ptr test_schema =
        schema_builder("test_ks", "test_cf").with_column("pk", int32_type, column_kind::partition_key).build();

    raw_value invalid_int_value = make_bool_raw(true);

    expression bind_var = bind_variable{.bind_index = 0, .receiver = make_receiver(int32_type, "bind_var")};

    auto [inputs, inputs_data] = make_evaluation_inputs(test_schema, {{"pk", make_int_raw(123)}}, {invalid_int_value});
    BOOST_REQUIRE_THROW(evaluate(bind_var, inputs), exceptions::invalid_request_exception);
}

BOOST_AUTO_TEST_CASE(evaluate_bind_variable_vs_unset) {
    auto qo = query_options(cql3::raw_value_vector_with_unset({raw_value::make_null()}, {true}));
    BOOST_REQUIRE_THROW(evaluate(new_bind_variable(0), evaluation_inputs{.options = &qo}), exceptions::invalid_request_exception);
}

BOOST_AUTO_TEST_CASE(evaluate_list_collection_constructor_empty) {
    // TODO: Empty multi-cell collections are trated as NULL in the database,
    // should the conversion happen in evaluate?
    expression empty_list = make_list_constructor({}, int32_type);
    BOOST_REQUIRE_EQUAL(evaluate(empty_list, evaluation_inputs{}), make_int_list_raw({}));
}

BOOST_AUTO_TEST_CASE(evaluate_list_collection_constructor) {
    expression int_list = make_list_constructor({make_int_const(1), make_int_const(2), make_int_const(3)}, int32_type);
    BOOST_REQUIRE_EQUAL(evaluate(int_list, evaluation_inputs{}), make_int_list_raw({1, 2, 3}));
}

BOOST_AUTO_TEST_CASE(evaluate_list_collection_constructor_does_not_sort) {
    expression int_list =
        make_list_constructor({make_int_const(3), make_int_const(1), make_int_const(3), make_int_const(1)}, int32_type);
    BOOST_REQUIRE_EQUAL(evaluate(int_list, evaluation_inputs{}), make_int_list_raw({3, 1, 3, 1}));
}

BOOST_AUTO_TEST_CASE(evaluate_list_collection_constructor_with_null) {
    expression list_with_null =
        make_list_constructor({make_int_const(1), constant::make_null(int32_type), make_int_const(3)}, int32_type);
    BOOST_REQUIRE_EQUAL(evaluate(list_with_null, evaluation_inputs{}), make_int_list_raw({1, std::nullopt, 3}));
}

BOOST_AUTO_TEST_CASE(evaluate_list_collection_constructor_with_empty_value) {
    expression list_with_empty =
        make_list_constructor({make_int_const(1), make_empty_const(int32_type), make_int_const(3)}, int32_type);
    BOOST_REQUIRE_EQUAL(evaluate(list_with_empty, evaluation_inputs{}),
                        make_list_raw({make_int_raw(1), make_empty_raw(), make_int_raw(3)}));
}

BOOST_AUTO_TEST_CASE(evaluate_set_collection_constructor_empty) {
    // TODO: Empty multi-cell collections are trated as NULL in the database,
    // should the conversion happen in evaluate?
    expression empty_set = make_set_constructor({}, int32_type);
    BOOST_REQUIRE_EQUAL(evaluate(empty_set, evaluation_inputs{}), make_int_set_raw({}));
}

BOOST_AUTO_TEST_CASE(evaluate_set_collection_constructor_sorted) {
    expression sorted_set = make_set_constructor({make_int_const(1), make_int_const(2), make_int_const(3)}, int32_type);
    BOOST_REQUIRE_EQUAL(evaluate(sorted_set, evaluation_inputs{}), make_int_set_raw({1, 2, 3}));
}

BOOST_AUTO_TEST_CASE(evaluate_set_collection_constructor_unsorted) {
    expression unsorted_set =
        make_set_constructor({make_int_const(1), make_int_const(3), make_int_const(2)}, int32_type);
    BOOST_REQUIRE_EQUAL(evaluate(unsorted_set, evaluation_inputs{}), make_int_set_raw({1, 2, 3}));
}

BOOST_AUTO_TEST_CASE(evaluate_set_collection_constructor_with_null) {
    expression set_with_null =
        make_set_constructor({make_int_const(1), constant::make_null(int32_type), make_int_const(3)}, int32_type);
    BOOST_REQUIRE_THROW(evaluate(set_with_null, evaluation_inputs{}), exceptions::invalid_request_exception);
}

BOOST_AUTO_TEST_CASE(evaluate_set_collection_constructor_with_empty) {
    expression set_with_only_one_empty = make_set_constructor({make_empty_const(int32_type)}, int32_type);
    BOOST_REQUIRE_EQUAL(evaluate(set_with_only_one_empty, evaluation_inputs{}), make_set_raw({make_empty_raw()}));

    expression set_two_with_empty =
        make_set_constructor({make_int_const(-1), make_empty_const(int32_type), make_int_const(0),
                              make_empty_const(int32_type), make_int_const(1)},
                             int32_type);
    BOOST_REQUIRE_EQUAL(evaluate(set_two_with_empty, evaluation_inputs{}),
                        make_set_raw({make_empty_raw(), make_int_raw(-1), make_int_raw(0), make_int_raw(1)}));
}

BOOST_AUTO_TEST_CASE(evaluate_map_collection_constructor_with_empty) {
    // TODO: Empty multi-cell collections are trated as NULL in the database,
    // should the conversion happen in evaluate?
    expression empty_map = make_map_constructor(std::vector<expression>(), int32_type, int32_type);
    BOOST_REQUIRE_EQUAL(evaluate(empty_map, evaluation_inputs{}), make_int_int_map_raw({}));
}

BOOST_AUTO_TEST_CASE(evaluate_map_collection_constructor_sorted) {
    expression map = make_map_constructor(
        {
            {make_int_const(1), make_int_const(2)},
            {make_int_const(3), make_int_const(4)},
            {make_int_const(5), make_int_const(6)},
        },
        int32_type, int32_type);
    BOOST_REQUIRE_EQUAL(evaluate(map, evaluation_inputs{}), make_int_int_map_raw({{1, 2}, {3, 4}, {5, 6}}));
}

BOOST_AUTO_TEST_CASE(evaluate_map_collection_constructor_unsorted) {
    expression map = make_map_constructor(
        {
            {make_int_const(3), make_int_const(4)},
            {make_int_const(5), make_int_const(6)},
            {make_int_const(1), make_int_const(2)},
        },
        int32_type, int32_type);
    BOOST_REQUIRE_EQUAL(evaluate(map, evaluation_inputs{}), make_int_int_map_raw({{1, 2}, {3, 4}, {5, 6}}));
}

BOOST_AUTO_TEST_CASE(evaluate_map_collection_constructor_with_null_key) {
    expression map_with_null_key = make_map_constructor(
        {
            {make_int_const(1), make_int_const(2)},
            {constant::make_null(int32_type), make_int_const(4)},
            {make_int_const(5), make_int_const(6)},
        },
        int32_type, int32_type);
    BOOST_REQUIRE_THROW(evaluate(map_with_null_key, evaluation_inputs{}), exceptions::invalid_request_exception);
}

BOOST_AUTO_TEST_CASE(evaluate_map_collection_constructor_with_null_value) {
    expression map_with_null_value = make_map_constructor(
        {
            {make_int_const(1), make_int_const(2)},
            {make_int_const(3), constant::make_null(int32_type)},
            {make_int_const(5), make_int_const(6)},
        },
        int32_type, int32_type);
    BOOST_REQUIRE_THROW(evaluate(map_with_null_value, evaluation_inputs{}), exceptions::invalid_request_exception);
}

BOOST_AUTO_TEST_CASE(evaluate_map_collection_constructor_with_empty_key) {
    expression map_with_empty_key = make_map_constructor(
        {
            {make_int_const(1), make_int_const(2)},
            {make_empty_const(int32_type), make_int_const(4)},
            {make_int_const(5), make_int_const(6)},
        },
        int32_type, int32_type);
    raw_value expected = make_map_raw(
        {{make_empty_raw(), make_int_raw(4)}, {make_int_raw(1), make_int_raw(2)}, {make_int_raw(5), make_int_raw(6)}});
    BOOST_REQUIRE_EQUAL(evaluate(map_with_empty_key, evaluation_inputs{}), expected);
}

BOOST_AUTO_TEST_CASE(evaluate_map_collection_constructor_with_empty_value) {
    expression map_with_empty_key = make_map_constructor(
        {
            {make_int_const(1), make_int_const(2)},
            {make_int_const(3), make_empty_const(int32_type)},
            {make_int_const(5), make_int_const(6)},
        },
        int32_type, int32_type);
    raw_value expected = make_map_raw(
        {{make_int_raw(1), make_int_raw(2)}, {make_int_raw(3), make_empty_raw()}, {make_int_raw(5), make_int_raw(6)}});
    BOOST_REQUIRE_EQUAL(evaluate(map_with_empty_key, evaluation_inputs{}), expected);
}

BOOST_AUTO_TEST_CASE(evaluate_tuple_constructor_empty) {
    expression empty_tuple = make_tuple_constructor({}, {});
    BOOST_REQUIRE_EQUAL(evaluate(empty_tuple, evaluation_inputs{}), make_tuple_raw({}));
}

BOOST_AUTO_TEST_CASE(evaluate_tuple_constructor_empty_prefix) {
    // It's legal for tuples to not have all fields present.
    expression empty_int_text_tuple = make_tuple_constructor({}, {int32_type, utf8_type});
    BOOST_REQUIRE_EQUAL(evaluate(empty_int_text_tuple, evaluation_inputs{}), make_tuple_raw({}));
}

BOOST_AUTO_TEST_CASE(evaluate_tuple_constructor_int_text) {
    expression int_text_tuple =
        make_tuple_constructor({make_int_const(123), make_text_const("tupled")}, {int32_type, utf8_type});
    BOOST_REQUIRE_EQUAL(evaluate(int_text_tuple, evaluation_inputs{}),
                        make_tuple_raw({make_int_raw(123), make_text_raw("tupled")}));
}

BOOST_AUTO_TEST_CASE(evaluate_tuple_constructor_with_null) {
    expression tuple_with_null =
        make_tuple_constructor({make_int_const(12), constant::make_null(int32_type), make_int_const(34)},
                               {int32_type, int32_type, int32_type});
    BOOST_REQUIRE_EQUAL(evaluate(tuple_with_null, evaluation_inputs{}),
                        make_tuple_raw({make_int_raw(12), raw_value::make_null(), make_int_raw(34)}));
}

BOOST_AUTO_TEST_CASE(evaluate_tuple_constructor_with_empty) {
    expression tuple_with_empty = make_tuple_constructor(
        {make_int_const(12), make_empty_const(int32_type), make_int_const(34)}, {int32_type, utf8_type, int32_type});
    BOOST_REQUIRE_EQUAL(evaluate(tuple_with_empty, evaluation_inputs{}),
                        make_tuple_raw({make_int_raw(12), make_empty_raw(), make_int_raw(34)}));
}

BOOST_AUTO_TEST_CASE(evaluate_tuple_constructor_with_prefix_fields) {
    // Tests evaluating a value of type tuple<int, text, int, double>, but only two fields are present
    expression tuple = make_tuple_constructor({make_int_const(1), make_text_const("12")},
                                              {int32_type, utf8_type, int32_type, double_type});
    BOOST_REQUIRE_EQUAL(evaluate(tuple, evaluation_inputs{}), make_tuple_raw({make_int_raw(1), make_text_raw("12")}));
}

BOOST_AUTO_TEST_CASE(evaluate_usertype_constructor_empty) {
    expression empty_usertype = make_usertype_constructor({});
    BOOST_REQUIRE_EQUAL(evaluate(empty_usertype, evaluation_inputs{}), make_tuple_raw({}));
}

BOOST_AUTO_TEST_CASE(evaluate_usertype_constructor) {
    expression usertype = make_usertype_constructor(
        {{"field1", make_int_const(123)}, {"field2", make_text_const("field2val")}, {"field3", make_bool_const(true)}});
    BOOST_REQUIRE_EQUAL(evaluate(usertype, evaluation_inputs{}),
                        make_tuple_raw({make_int_raw(123), make_text_raw("field2val"), make_bool_raw(true)}));
}

BOOST_AUTO_TEST_CASE(evaluate_usertype_constructor_with_null) {
    expression usertype_with_null = make_usertype_constructor({{"field1", make_int_const(123)},
                                                               {"field2", constant::make_null(utf8_type)},
                                                               {"field3", make_bool_const(true)}});
    BOOST_REQUIRE_EQUAL(evaluate(usertype_with_null, evaluation_inputs{}),
                        make_tuple_raw({make_int_raw(123), raw_value::make_null(), make_bool_raw(true)}));
}

BOOST_AUTO_TEST_CASE(evaluate_usertype_constructor_with_empty) {
    expression usertype_with_null = make_usertype_constructor(
        {{"field1", make_int_const(123)}, {"field2", make_empty_const(utf8_type)}, {"field3", make_bool_const(true)}});
    BOOST_REQUIRE_EQUAL(evaluate(usertype_with_null, evaluation_inputs{}),
                        make_tuple_raw({make_int_raw(123), make_empty_raw(), make_bool_raw(true)}));
}

// Evaluates value[subscript_value]
static raw_value evaluate_subscripted(constant value, constant subscript_value) {
    // For now it's only possible to subscript columns, not values, so this is tested.
    schema_ptr table_schema = schema_builder("test_ks", "test_cf")
                                  .with_column("pk", int32_type, column_kind::partition_key)
                                  .with_column("v", value.type, column_kind::regular_column)
                                  .build();

    const column_definition* value_col = table_schema->get_column_definition("v");

    expression sub = subscript{.val = column_value(value_col), .sub = subscript_value};

    auto [inputs, inputs_data] = make_evaluation_inputs(table_schema, {{"pk", make_int_raw(0)}, {"v", value.value}});
    return evaluate(sub, inputs);
}

BOOST_AUTO_TEST_CASE(evalaute_subscripted_empty_list) {
    constant list = make_list_const(std::vector<constant>{}, int32_type);
    BOOST_REQUIRE_EQUAL(evaluate_subscripted(list, make_int_const(0)), raw_value::make_null());
    BOOST_REQUIRE_EQUAL(evaluate_subscripted(list, make_int_const(1)), raw_value::make_null());
    BOOST_REQUIRE_EQUAL(evaluate_subscripted(list, make_int_const(4)), raw_value::make_null());
    BOOST_REQUIRE_EQUAL(evaluate_subscripted(list, make_int_const(std::numeric_limits<int32_t>::max())),
                        raw_value::make_null());
    BOOST_REQUIRE_EQUAL(evaluate_subscripted(list, make_int_const(-1)), raw_value::make_null());
    BOOST_REQUIRE_EQUAL(evaluate_subscripted(list, make_int_const(-4)), raw_value::make_null());
    BOOST_REQUIRE_EQUAL(evaluate_subscripted(list, make_int_const(std::numeric_limits<int32_t>::min())),
                        raw_value::make_null());
    BOOST_REQUIRE_EQUAL(evaluate_subscripted(list, constant::make_null(int32_type)), raw_value::make_null());
    
    // TODO: Should empty value list indexes cause an error? Why not return NULL?
    BOOST_REQUIRE_THROW(evaluate_subscripted(list, make_empty_const(int32_type)), empty_value_exception);
}

BOOST_AUTO_TEST_CASE(evaluate_subscripted_list_empty) {
    // Empty list values seem to not be allowed.
    constant list = make_empty_const(list_type_impl::get_instance(int32_type, true));
    BOOST_REQUIRE_THROW(evaluate_subscripted(list, make_int_const(0)), marshal_exception);
}

constant make_subscript_test_list() {
    return make_list_const({make_int_const(357), make_int_const(468), make_empty_const(int32_type), make_int_const(123)},
                     int32_type);
}

BOOST_AUTO_TEST_CASE(evaluate_subscripted_list_basic) {
    constant list = make_subscript_test_list();
    BOOST_REQUIRE_EQUAL(evaluate_subscripted(list, make_int_const(0)), make_int_raw(357));
    BOOST_REQUIRE_EQUAL(evaluate_subscripted(list, make_int_const(1)), make_int_raw(468));
    BOOST_REQUIRE_EQUAL(evaluate_subscripted(list, make_int_const(3)), make_int_raw(123));
}

BOOST_AUTO_TEST_CASE(evaluate_subscripted_list_empty_value) {
    constant list = make_subscript_test_list();
    BOOST_REQUIRE_EQUAL(evaluate_subscripted(list, make_int_const(2)), make_empty_raw());
}

BOOST_AUTO_TEST_CASE(evaluate_subscripted_list_negative_index) {
    constant list = make_subscript_test_list();
    BOOST_REQUIRE_EQUAL(evaluate_subscripted(list, make_int_const(-1)), raw_value::make_null());
    BOOST_REQUIRE_EQUAL(evaluate_subscripted(list, make_int_const(-2)), raw_value::make_null());
    BOOST_REQUIRE_EQUAL(evaluate_subscripted(list, make_int_const(-3)), raw_value::make_null());
    BOOST_REQUIRE_EQUAL(evaluate_subscripted(list, make_int_const(-1000)), raw_value::make_null());
    BOOST_REQUIRE_EQUAL(evaluate_subscripted(list, make_int_const(std::numeric_limits<int32_t>::min())),
                        raw_value::make_null());
}

BOOST_AUTO_TEST_CASE(evaluate_subscripted_list_too_big_index) {
    constant list = make_subscript_test_list();
    BOOST_REQUIRE_EQUAL(evaluate_subscripted(list, make_int_const(5)), raw_value::make_null());
    BOOST_REQUIRE_EQUAL(evaluate_subscripted(list, make_int_const(6)), raw_value::make_null());
    BOOST_REQUIRE_EQUAL(evaluate_subscripted(list, make_int_const(7)), raw_value::make_null());
    BOOST_REQUIRE_EQUAL(evaluate_subscripted(list, make_int_const(1000)), raw_value::make_null());
    BOOST_REQUIRE_EQUAL(evaluate_subscripted(list, make_int_const(std::numeric_limits<int32_t>::max())),
                        raw_value::make_null());
}

BOOST_AUTO_TEST_CASE(evaluate_subscripted_list_null_index) {
    constant list = make_subscript_test_list();
    BOOST_REQUIRE_EQUAL(evaluate_subscripted(list, constant::make_null(int32_type)), raw_value::make_null());
}

BOOST_AUTO_TEST_CASE(evaluate_subscripted_list_empty_index) {
    constant list = make_subscript_test_list();
    // TODO: Should empty value list indexes cause an error? Why not return NULL?
    BOOST_REQUIRE_THROW(evaluate_subscripted(list, make_empty_const(int32_type)), empty_value_exception);
}

BOOST_AUTO_TEST_CASE(evaluate_subscripted_list_null_list) {
    constant list = constant::make_null(list_type_impl::get_instance(int32_type, true));
    BOOST_REQUIRE_EQUAL(evaluate_subscripted(list, make_int_const(0)), raw_value::make_null());
    BOOST_REQUIRE_EQUAL(evaluate_subscripted(list, make_empty_const(int32_type)), raw_value::make_null());
    BOOST_REQUIRE_EQUAL(evaluate_subscripted(list, constant::make_null(int32_type)), raw_value::make_null());
}

BOOST_AUTO_TEST_CASE(evaluate_subscripted_empty_map) {
    constant map = make_map_const(std::vector<std::pair<constant, constant>>(), int32_type, int32_type);
    BOOST_REQUIRE_EQUAL(evaluate_subscripted(map, make_int_const(0)), raw_value::make_null());
    BOOST_REQUIRE_EQUAL(evaluate_subscripted(map, make_int_const(1)), raw_value::make_null());
    BOOST_REQUIRE_EQUAL(evaluate_subscripted(map, make_int_const(-1)), raw_value::make_null());
    BOOST_REQUIRE_EQUAL(evaluate_subscripted(map, make_int_const(-4)), raw_value::make_null());
    BOOST_REQUIRE_EQUAL(evaluate_subscripted(map, make_int_const(4)), raw_value::make_null());
    BOOST_REQUIRE_EQUAL(evaluate_subscripted(map, make_int_const(std::numeric_limits<int32_t>::min())),
                        raw_value::make_null());
    BOOST_REQUIRE_EQUAL(evaluate_subscripted(map, make_int_const(std::numeric_limits<int32_t>::max())),
                        raw_value::make_null());
    BOOST_REQUIRE_EQUAL(evaluate_subscripted(map, constant::make_null(int32_type)), raw_value::make_null());
    BOOST_REQUIRE_EQUAL(evaluate_subscripted(map, make_empty_const(int32_type)), raw_value::make_null());
}

static constant make_subscript_test_map() {
    return make_map_const({{make_empty_const(int32_type), make_int_const(1)},
                     {make_int_const(2), make_int_const(3)},
                     {make_int_const(4), make_empty_const(int32_type)},
                     {make_int_const(6), make_int_const(7)}},
                    int32_type, int32_type);
}

BOOST_AUTO_TEST_CASE(evaluate_subscripted_map_basic) {
    constant map = make_subscript_test_map();
    BOOST_REQUIRE_EQUAL(evaluate_subscripted(map, make_int_const(2)), make_int_raw(3));
    BOOST_REQUIRE_EQUAL(evaluate_subscripted(map, make_int_const(6)), make_int_raw(7));
}

BOOST_AUTO_TEST_CASE(evaluate_subscripted_map_nonexistant_key) {
    constant map = make_subscript_test_map();
    BOOST_REQUIRE_EQUAL(evaluate_subscripted(map, make_int_const(3)), raw_value::make_null());
    BOOST_REQUIRE_EQUAL(evaluate_subscripted(map, make_int_const(5)), raw_value::make_null());
    BOOST_REQUIRE_EQUAL(evaluate_subscripted(map, make_int_const(-1)), raw_value::make_null());
    BOOST_REQUIRE_EQUAL(evaluate_subscripted(map, make_int_const(-1000)), raw_value::make_null());
    BOOST_REQUIRE_EQUAL(evaluate_subscripted(map, make_int_const(1000)), raw_value::make_null());
    BOOST_REQUIRE_EQUAL(evaluate_subscripted(map, make_int_const(std::numeric_limits<int32_t>::min())),
                        raw_value::make_null());
    BOOST_REQUIRE_EQUAL(evaluate_subscripted(map, make_int_const(std::numeric_limits<int32_t>::max())),
                        raw_value::make_null());
}

BOOST_AUTO_TEST_CASE(evalute_subscripted_map_empty_key) {
    constant map = make_subscript_test_map();
    BOOST_REQUIRE_EQUAL(evaluate_subscripted(map, make_int_const(4)), make_empty_raw());
}

BOOST_AUTO_TEST_CASE(evaluate_subscripted_map_empty_value) {
    constant map = make_subscript_test_map();
    BOOST_REQUIRE_EQUAL(evaluate_subscripted(map, make_empty_const(int32_type)), make_int_raw(1));
}

BOOST_AUTO_TEST_CASE(evaluate_subscripted_map_null_index) {
    constant map = make_subscript_test_map();
    BOOST_REQUIRE_EQUAL(evaluate_subscripted(map, constant::make_null(int32_type)), raw_value::make_null());
}

BOOST_AUTO_TEST_CASE(evaluate_subscripted_map_empty) {
    // Empty list values seem to not be allowed.
    constant map = make_empty_const(map_type_impl::get_instance(int32_type, int32_type, true));
    BOOST_REQUIRE_THROW(evaluate_subscripted(map, make_int_const(0)), marshal_exception);
}

BOOST_AUTO_TEST_CASE(evaluate_subscripted_map_null_map) {
    constant map = constant::make_null(map_type_impl::get_instance(int32_type, int32_type, true));
    BOOST_REQUIRE_EQUAL(evaluate_subscripted(map, make_int_const(0)), raw_value::make_null());
    BOOST_REQUIRE_EQUAL(evaluate_subscripted(map, make_empty_const(int32_type)), raw_value::make_null());
    BOOST_REQUIRE_EQUAL(evaluate_subscripted(map, constant::make_null(int32_type)), raw_value::make_null());
}

enum expected_invalid_or_valid { expected_valid, expected_invalid };

// Checks that trying to evaluate a bind variable with this value succeeds or fails.
// This is used to test bind variable validation.
static void check_bind_variable_evaluate(constant check_value, expected_invalid_or_valid expected_validity) {
    schema_ptr test_schema = schema_builder("test_ks", "test_cf")
                                 .with_column("pk", int32_type, column_kind::partition_key)
                                 .with_column("r", check_value.type, column_kind::regular_column)
                                 .build();

    expression bind_var = bind_variable{.bind_index = 0, .receiver = make_receiver(check_value.type, "bind_var")};

    auto [inputs, inputs_data] = make_evaluation_inputs(
        test_schema, {{"pk", make_int_raw(0)}, {"r", cql3::raw_value::make_null()}}, {check_value.value});

    switch (expected_validity) {
        case expected_valid:
            BOOST_REQUIRE_EQUAL(evaluate(bind_var, inputs), check_value.value);
            return;
        case expected_invalid:
            BOOST_REQUIRE_THROW(evaluate(bind_var, inputs), exceptions::invalid_request_exception);
            return;
    }
}

BOOST_AUTO_TEST_CASE(evaluate_bind_variable_validates_null_in_list) {
    constant list_with_null =
        make_list_const({make_int_const(1), constant::make_null(int32_type), make_int_const(2)}, int32_type);
    check_bind_variable_evaluate(list_with_null, expected_valid);
}

BOOST_AUTO_TEST_CASE(evaluate_bind_variable_validates_empty_in_list) {
    constant lis_with_empty =
        make_list_const({make_int_const(1), make_empty_const(int32_type), make_int_const(2)}, int32_type);
    check_bind_variable_evaluate(lis_with_empty, expected_valid);
}

BOOST_AUTO_TEST_CASE(evaluate_bind_variable_validates_no_null_in_set) {
    constant set_with_null =
        make_set_const({make_int_const(1), constant::make_null(int32_type), make_int_const(2)}, int32_type);
    check_bind_variable_evaluate(set_with_null, expected_invalid);
}

// TODO: This fails, but I feel like this is a bug.
// BOOST_AUTO_TEST_CASE(evaluate_bind_variable_validates_empty_in_set) {
//     constant set_with_empty =
//         make_set_const({make_empty_const(int32_type), make_int_const(1), make_int_const(2)}, int32_type);
//     check_bind_variable_evaluate(set_with_empty, expected_valid);
// }

BOOST_AUTO_TEST_CASE(evaluate_bind_variable_validates_no_null_key_in_map) {
    constant map_with_null_key = make_map_const({{make_int_const(1), make_int_const(2)},
                                                 {constant::make_null(int32_type), make_int_const(4)},
                                                 {make_int_const(5), make_int_const(6)}},
                                                int32_type, int32_type);
    check_bind_variable_evaluate(map_with_null_key, expected_invalid);
}

BOOST_AUTO_TEST_CASE(evaluate_bind_variable_validates_empty_key_in_map) {
    constant map_with_empty_key = make_map_const({{make_empty_const(int32_type), make_int_const(4)},
                                                  {make_int_const(1), make_int_const(2)},
                                                  {make_int_const(5), make_int_const(6)}},
                                                 int32_type, int32_type);
    check_bind_variable_evaluate(map_with_empty_key, expected_valid);
}

BOOST_AUTO_TEST_CASE(evaluate_bind_variable_validates_no_null_value_in_map) {
    constant map_with_null_value = make_map_const({{make_int_const(1), make_int_const(2)},
                                                   {make_int_const(3), constant::make_null(int32_type)},
                                                   {make_int_const(5), make_int_const(6)}},
                                                  int32_type, int32_type);
    check_bind_variable_evaluate(map_with_null_value, expected_invalid);
}

BOOST_AUTO_TEST_CASE(evaluate_bind_variable_validates_empty_value_in_map) {
    constant map_with_empty_value = make_map_const({{make_int_const(1), make_int_const(2)},
                                                    {make_int_const(3), make_empty_const(int32_type)},
                                                    {make_int_const(5), make_int_const(6)}},
                                                   int32_type, int32_type);
    check_bind_variable_evaluate(map_with_empty_value, expected_valid);
}

// Creates a list value of the following form:
// [
//  [[9, 10, 11], [8, 9, 10], [7, 8, 9]],
//  [[8, 9, 10], [7, value_in_list, 9], [6, 7, 8]],
//  [[7, 8, 9], [6, 7, 8], [5, 6, 7]]
// ]
// Used to check that validation recurses into the list
static constant create_nested_list_with_value(constant value_in_list) {
    data_type int_list_type = list_type_impl::get_instance(int32_type, true);
    constant first_list = make_list_const(
        {make_int_list_const({9, 10, 11}), make_int_list_const({8, 9, 10}), make_int_list_const({7, 8, 9})},
        int_list_type);
    constant list_with_value = make_list_const({make_int_const(7), value_in_list, make_int_const(9)}, int32_type);
    constant second_list = make_list_const(
        {make_int_list_const({8, 9, 10}), list_with_value, make_int_list_const({6, 7, 8})}, int_list_type);
    constant third_list = make_list_const(
        {make_int_list_const({7, 8, 9}), make_int_list_const({6, 7, 8}), make_int_list_const({5, 6, 7})},
        int_list_type);

    return make_list_const({first_list, second_list, third_list}, first_list.type);
}

BOOST_AUTO_TEST_CASE(evaluate_bind_variable_validates_null_in_lists_recursively) {
    constant list_with_null = create_nested_list_with_value(constant::make_null(int32_type));
    check_bind_variable_evaluate(list_with_null, expected_valid);
}

// TODO: This fails, but I feel like this is a bug.
// BOOST_AUTO_TEST_CASE(evaluate_bind_variable_validates_empty_in_lists_recursively) {
//     constant list_with_empty = create_nested_list_or_set_with_value(make_empty_const(int32_type));
//     check_bind_variable_evaluate(list_with_empty, expected_valid);
// }

// Creates a set value of the following form:
// {
//  {{value_in_set, 2, 3}, {2, 3, 4}, {3, 4, 5}}
//  {{10, 20, 30}, {20, 30, 40}, {30, 40, 50}},
//  {{100, 200, 300}, {200, 300, 400}, {300, 400, 500}}
// }
// Used to check that validation recurses into the set
static constant create_nested_set_with_value(constant value_in_set) {
    data_type set_of_ints_type = set_type_impl::get_instance(int32_type, true);
    constant set_with_value = make_set_const({value_in_set, make_int_const(2), make_int_const(3)}, int32_type);
    constant first_set = make_set_const({set_with_value, make_int_set_const({2, 3, 4}), make_int_set_const({3, 4, 5})},
                                        set_of_ints_type);
    constant second_set = make_set_const(
        {make_int_set_const({10, 20, 30}), make_int_set_const({20, 30, 40}), make_int_set_const({30, 40, 50})},
        set_of_ints_type);
    constant third_set = make_set_const(
        {make_int_set_const({100, 200, 300}), make_int_set_const({200, 300, 400}), make_int_set_const({300, 400, 500})},
        set_of_ints_type);
    return make_set_const({first_set, second_set, third_set}, first_set.type);
}

BOOST_AUTO_TEST_CASE(evaluate_bind_variable_validates_null_in_sets_recursively) {
    constant set_with_null = create_nested_set_with_value(constant::make_null(int32_type));
    check_bind_variable_evaluate(set_with_null, expected_invalid);
}

BOOST_AUTO_TEST_CASE(evaluate_bind_variable_validates_empty_in_sets_recursively) {
    constant set_with_empty = create_nested_set_with_value(make_empty_const(int32_type));
    check_bind_variable_evaluate(set_with_empty, expected_valid);
}

// Creates a map value of the following form:
// {
//   {{key1: 2, 2: 3}: {2: 3, 3: 4}, {5: 6, 7:8}: {9:10, 11:12}}
//   :
//   {{key2: 14, 15: 16}: {17: 18, 19: 20}, {21: 22, 23: 24}: {25: 26, 27: 28}}},
//   {{29: 30, 31: 32}: {33: 34, 35: 36}, {37: 38, 39: 40}: {41: 42, 43: 44}}
//   :
//   {{45: 46, 47: 48}: {49: 50, 51: 52}, {53: 54, 55: 56}: {57: 58, 59: 60}}
// }
// Used to check that validation recurses into the map
constant create_nested_map_with_key(constant key1, constant key2) {
    auto i = [](int32_t i) -> constant { return make_int_const(i); };

    constant key1_map = make_map_const({{key1, i(2)}, {i(2), i(3)}}, int32_type, int32_type);

    constant map1 =
        make_map_const({{key1_map, make_int_int_map_const({{2, 3}, {3, 4}})},
                        {make_int_int_map_const({{5, 6}, {7, 8}}), make_int_int_map_const({{9, 10}, {11, 12}})}},
                       key1_map.type, key1_map.type);

    constant key2_map = make_map_const({{key2, i(14)}, {i(15), i(16)}}, int32_type, int32_type);
    constant map2 =
        make_map_const({{key2_map, make_int_int_map_const({{17, 18}, {19, 20}})},
                        {make_int_int_map_const({{21, 22}, {23, 24}}), make_int_int_map_const({{25, 26}, {27, 28}})}},
                       key1_map.type, key1_map.type);

    constant map3 =
        make_map_const({{make_int_int_map_const({{29, 30}, {31, 32}}), make_int_int_map_const({{33, 34}, {35, 36}})},
                        {make_int_int_map_const({{37, 38}, {39, 40}}), make_int_int_map_const({{41, 42}, {43, 44}})}},
                       key1_map.type, key1_map.type);

    constant map4 =
        make_map_const({{make_int_int_map_const({{45, 46}, {47, 48}}), make_int_int_map_const({{49, 50}, {51, 52}})},
                        {make_int_int_map_const({{53, 54}, {55, 56}}), make_int_int_map_const({{57, 58}, {59, 60}})}},
                       key1_map.type, key1_map.type);

    return make_map_const({{map1, map2}, {map3, map4}}, map1.type, map1.type);
}

BOOST_AUTO_TEST_CASE(evaluate_bind_variable_validates_null_key_in_maps_recursively) {
    constant map_with_null_key1 = create_nested_map_with_key(constant::make_null(int32_type), make_int_const(13));
    check_bind_variable_evaluate(map_with_null_key1, expected_invalid);

    constant map_with_null_key2 = create_nested_map_with_key(make_int_const(1), constant::make_null(int32_type));
    check_bind_variable_evaluate(map_with_null_key2, expected_invalid);
}

BOOST_AUTO_TEST_CASE(evaluate_bind_variable_validates_empty_key_in_maps_recursively) {
    constant map_with_empty_key1 = create_nested_map_with_key(make_empty_const(int32_type), make_int_const(13));
    check_bind_variable_evaluate(map_with_empty_key1, expected_valid);

    constant map_with_empty_key2 = create_nested_map_with_key(make_int_const(1), make_empty_const(int32_type));
    check_bind_variable_evaluate(map_with_empty_key2, expected_valid);
}

// Creates a map value of the following form:
// {
//   {{1: val1, 2: 3}: {2: 3, 3: 4}, {5: 6, 7:8}: {9:10, 11:12}}
//   :
//   {{13: val2, 15: 16}: {17: 18, 19: 20}, {21: 22, 23: 24}: {25: 26, 27: 28}}},
//   {{29: 30, 31: 32}: {33: 34, 35: 36}, {37: 38, 39: 40}: {41: 42, 43: 44}}
//   :
//   {{45: 46, 47: 48}: {49: 50, 51: 52}, {53: 54, 55: 56}: {57: 58, 59: 60}}
// }
// Used to check that validation recurses into the map
constant create_nested_map_with_value(constant val1, constant val2) {
    auto i = [](int32_t i) -> constant { return make_int_const(i); };

    constant val1_map = make_map_const({{i(1), val1}, {i(2), i(3)}}, int32_type, int32_type);

    constant map1 =
        make_map_const({{val1_map, make_int_int_map_const({{2, 3}, {3, 4}})},
                        {make_int_int_map_const({{5, 6}, {7, 8}}), make_int_int_map_const({{9, 10}, {11, 12}})}},
                       val1_map.type, val1_map.type);

    constant val2_map = make_map_const({{i(13), val2}, {i(15), i(16)}}, int32_type, int32_type);
    constant map2 =
        make_map_const({{val2_map, make_int_int_map_const({{17, 18}, {19, 20}})},
                        {make_int_int_map_const({{21, 22}, {23, 24}}), make_int_int_map_const({{25, 26}, {27, 28}})}},
                       val1_map.type, val1_map.type);

    constant map3 =
        make_map_const({{make_int_int_map_const({{29, 30}, {31, 32}}), make_int_int_map_const({{33, 34}, {35, 36}})},
                        {make_int_int_map_const({{37, 38}, {39, 40}}), make_int_int_map_const({{41, 42}, {43, 44}})}},
                       val1_map.type, val1_map.type);

    constant map4 =
        make_map_const({{make_int_int_map_const({{45, 46}, {47, 48}}), make_int_int_map_const({{49, 50}, {51, 52}})},
                        {make_int_int_map_const({{53, 54}, {55, 56}}), make_int_int_map_const({{57, 58}, {59, 60}})}},
                       val1_map.type, val1_map.type);

    return make_map_const({{map1, map2}, {map3, map4}}, map1.type, map1.type);
}

BOOST_AUTO_TEST_CASE(evaluate_bind_variable_validates_null_value_in_maps_recursively) {
    constant map_with_null_value1 = create_nested_map_with_value(constant::make_null(int32_type), make_int_const(13));
    check_bind_variable_evaluate(map_with_null_value1, expected_invalid);

    constant map_with_null_value2 = create_nested_map_with_value(make_int_const(1), constant::make_null(int32_type));
    check_bind_variable_evaluate(map_with_null_value2, expected_invalid);
}

BOOST_AUTO_TEST_CASE(evaluate_bind_variable_validates_empty_value_in_maps_recursively) {
    constant map_with_empty_value1 = create_nested_map_with_value(make_empty_const(int32_type), make_int_const(13));
    check_bind_variable_evaluate(map_with_empty_value1, expected_valid);

    constant map_with_empty_value2 = create_nested_map_with_value(make_int_const(1), make_empty_const(int32_type));
    check_bind_variable_evaluate(map_with_empty_value2, expected_valid);
}

BOOST_AUTO_TEST_CASE(prepare_partition_column_unresolved_identifier) {
    schema_ptr table_schema = make_simple_test_schema();
    auto [db, db_data] = make_data_dictionary_database(table_schema);

    expression pk_unresolved_identifier =
        unresolved_identifier{.ident = ::make_shared<column_identifier_raw>("pk", true)};
    expression prepared = prepare_expression(pk_unresolved_identifier, db, "test_ks", table_schema.get(), nullptr);

    expression expected = column_value(table_schema->get_column_definition("pk"));

    BOOST_REQUIRE_EQUAL(prepared, expected);
}

BOOST_AUTO_TEST_CASE(prepare_clustering_column_unresolved_identifier) {
    schema_ptr table_schema = make_simple_test_schema();
    auto [db, db_data] = make_data_dictionary_database(table_schema);

    expression ck_unresolved_identifier =
        unresolved_identifier{.ident = ::make_shared<column_identifier_raw>("ck", true)};
    expression prepared = prepare_expression(ck_unresolved_identifier, db, "test_ks", table_schema.get(), nullptr);

    expression expected = column_value(table_schema->get_column_definition("ck"));

    BOOST_REQUIRE_EQUAL(prepared, expected);
}

BOOST_AUTO_TEST_CASE(prepare_regular_column_unresolved_identifier) {
    schema_ptr table_schema = make_simple_test_schema();
    auto [db, db_data] = make_data_dictionary_database(table_schema);

    expression r_unresolved_identifier =
        unresolved_identifier{.ident = ::make_shared<column_identifier_raw>("r", true)};
    expression prepared = prepare_expression(r_unresolved_identifier, db, "test_ks", table_schema.get(), nullptr);

    expression expected = column_value(table_schema->get_column_definition("r"));

    BOOST_REQUIRE_EQUAL(prepared, expected);
}

BOOST_AUTO_TEST_CASE(prepare_static_column_unresolved_identifier) {
    schema_ptr table_schema = make_simple_test_schema();
    auto [db, db_data] = make_data_dictionary_database(table_schema);

    expression s_unresolved_identifier =
        unresolved_identifier{.ident = ::make_shared<column_identifier_raw>("s", true)};
    expression prepared = prepare_expression(s_unresolved_identifier, db, "test_ks", table_schema.get(), nullptr);

    expression expected = column_value(table_schema->get_column_definition("s"));

    BOOST_REQUIRE_EQUAL(prepared, expected);
}

// prepare_expression for a column_value should do nothing
BOOST_AUTO_TEST_CASE(prepare_column_value) {
    schema_ptr table_schema = make_simple_test_schema();
    auto [db, db_data] = make_data_dictionary_database(table_schema);

    expression cval = column_value(table_schema->get_column_definition("pk"));
    expression prepared = prepare_expression(cval, db, "test_ks", table_schema.get(), nullptr);
    BOOST_REQUIRE_EQUAL(cval, prepared);
}

BOOST_AUTO_TEST_CASE(prepare_subscript_list) {
    schema_ptr table_schema =
        schema_builder("test_ks", "test_cf")
            .with_column("pk", int32_type, column_kind::partition_key)
            .with_column("r", list_type_impl::get_instance(boolean_type, true), column_kind::regular_column)
            .build();
    auto [db, db_data] = make_data_dictionary_database(table_schema);

    expression sub =
        subscript{.val = unresolved_identifier{.ident = ::make_shared<column_identifier_raw>("r", true)},
                  .sub = make_int_untyped("123")};

    expression prepared = prepare_expression(sub, db, "test_ks", table_schema.get(), nullptr);

    expression expected = subscript{.val = column_value(table_schema->get_column_definition("r")),
                                    .sub = make_int_const(123),
                                    .type = boolean_type};

    BOOST_REQUIRE_EQUAL(prepared, expected);
}

BOOST_AUTO_TEST_CASE(prepare_subscript_map) {
    schema_ptr table_schema =
        schema_builder("test_ks", "test_cf")
            .with_column("pk", int32_type, column_kind::partition_key)
            .with_column("r", map_type_impl::get_instance(boolean_type, utf8_type, true), column_kind::regular_column)
            .build();
    auto [db, db_data] = make_data_dictionary_database(table_schema);

    expression sub =
        subscript{.val = unresolved_identifier{.ident = ::make_shared<column_identifier_raw>("r", true)},
                  .sub = make_bool_untyped("true")};

    expression prepared = prepare_expression(sub, db, "test_ks", table_schema.get(), nullptr);

    expression expected = subscript{
        .val = column_value(table_schema->get_column_definition("r")), .sub = make_bool_const(true), .type = utf8_type};

    BOOST_REQUIRE_EQUAL(prepared, expected);
}

BOOST_AUTO_TEST_CASE(prepare_subscript_set) {
    schema_ptr table_schema =
        schema_builder("test_ks", "test_cf")
            .with_column("pk", int32_type, column_kind::partition_key)
            .with_column("r", set_type_impl::get_instance(boolean_type, true), column_kind::regular_column)
            .build();
    auto [db, db_data] = make_data_dictionary_database(table_schema);

    expression sub =
        subscript{.val = unresolved_identifier{.ident = ::make_shared<column_identifier_raw>("r", true)},
                  .sub = make_int_untyped("123")};

    BOOST_REQUIRE_THROW(prepare_expression(sub, db, "test_ks", table_schema.get(), nullptr),
                        exceptions::invalid_request_exception);
}

BOOST_AUTO_TEST_CASE(prepare_subscript_list_checks_type) {
    schema_ptr table_schema =
        schema_builder("test_ks", "test_cf")
            .with_column("pk", int32_type, column_kind::partition_key)
            .with_column("r", list_type_impl::get_instance(boolean_type, true), column_kind::regular_column)
            .build();
    auto [db, db_data] = make_data_dictionary_database(table_schema);

    expression sub =
        subscript{.val = unresolved_identifier{.ident = ::make_shared<column_identifier_raw>("r", true)},
                  .sub = make_bool_untyped("true")};

    BOOST_REQUIRE_THROW(prepare_expression(sub, db, "test_ks", table_schema.get(), nullptr),
                        exceptions::invalid_request_exception);
}

BOOST_AUTO_TEST_CASE(prepare_subscript_map_checks_type) {
    schema_ptr table_schema =
        schema_builder("test_ks", "test_cf")
            .with_column("pk", int32_type, column_kind::partition_key)
            .with_column("r", map_type_impl::get_instance(boolean_type, utf8_type, true), column_kind::regular_column)
            .build();
    auto [db, db_data] = make_data_dictionary_database(table_schema);

    expression sub =
        subscript{.val = unresolved_identifier{.ident = ::make_shared<column_identifier_raw>("r", true)},
                  .sub = make_int_untyped("123")};

    BOOST_REQUIRE_THROW(prepare_expression(sub, db, "test_ks", table_schema.get(), nullptr),
                        exceptions::invalid_request_exception);
}

BOOST_AUTO_TEST_CASE(prepare_token) {
    schema_ptr table_schema = schema_builder("test_ks", "test_cf")
                                  .with_column("p1", int32_type, column_kind::partition_key)
                                  .with_column("p2", int32_type, column_kind::partition_key)
                                  .with_column("p3", int32_type, column_kind::partition_key)
                                  .build();
    auto [db, db_data] = make_data_dictionary_database(table_schema);

    expression tok = make_token({unresolved_identifier{::make_shared<column_identifier_raw>("p1", true)},
                            unresolved_identifier{::make_shared<column_identifier_raw>("p2", true)},
                            unresolved_identifier{::make_shared<column_identifier_raw>("p3", true)}});

    expression prepared = prepare_expression(tok, db, "test_ks", table_schema.get(), nullptr);

    std::vector<expression> expected_args = {column_value(table_schema->get_column_definition("p1")),
                                 column_value(table_schema->get_column_definition("p2")),
                                 column_value(table_schema->get_column_definition("p3"))};

    const function_call* token_fun_call = as_if<function_call>(&prepared);
    BOOST_REQUIRE(token_fun_call != nullptr);
    BOOST_REQUIRE(is_token_function(*token_fun_call));
    BOOST_REQUIRE(std::holds_alternative<shared_ptr<db::functions::function>>(token_fun_call->func));
    BOOST_REQUIRE_EQUAL(token_fun_call->args, expected_args);
}

BOOST_AUTO_TEST_CASE(prepare_token_no_args) {
    schema_ptr table_schema = schema_builder("test_ks", "test_cf")
                                  .with_column("p1", int32_type, column_kind::partition_key)
                                  .with_column("p2", int32_type, column_kind::partition_key)
                                  .with_column("p3", int32_type, column_kind::partition_key)
                                  .build();
    auto [db, db_data] = make_data_dictionary_database(table_schema);

    expression tok = make_token(std::vector<expression>());

    BOOST_REQUIRE_THROW(prepare_expression(tok, db, "test_ks", table_schema.get(), nullptr),
                        exceptions::invalid_request_exception);
}

BOOST_AUTO_TEST_CASE(prepare_cast_int_int) {
    schema_ptr table_schema = make_simple_test_schema();
    auto [db, db_data] = make_data_dictionary_database(table_schema);

    expression cast_expr =
        cast{.arg = make_int_untyped("123"),
             .type = cql3_type::raw::from(int32_type)};

    ::lw_shared_ptr<column_specification> receiver = make_receiver(int32_type);

    expression prepared = prepare_expression(cast_expr, db, "test_ks", table_schema.get(), receiver);

    expression expected = cast{.arg = make_int_const(123), .type = int32_type};
    BOOST_REQUIRE_EQUAL(prepared, expected);
}

BOOST_AUTO_TEST_CASE(prepare_cast_int_short) {
    schema_ptr table_schema = make_simple_test_schema();
    auto [db, db_data] = make_data_dictionary_database(table_schema);

    expression cast_expr =
        cast{.arg = make_int_untyped("123"),
             .type = cql3_type::raw::from(short_type)};

    ::lw_shared_ptr<column_specification> receiver = make_receiver(short_type);

    expression prepared = prepare_expression(cast_expr, db, "test_ks", table_schema.get(), receiver);

    expression expected = cast{.arg = make_smallint_const(123), .type = short_type};
    BOOST_REQUIRE_EQUAL(prepared, expected);
}

BOOST_AUTO_TEST_CASE(prepare_cast_text_int) {
    schema_ptr table_schema = make_simple_test_schema();
    auto [db, db_data] = make_data_dictionary_database(table_schema);

    expression cast_expr =
        cast{.arg = make_string_untyped("123"),
             .type = cql3_type::raw::from(short_type)};

    ::lw_shared_ptr<column_specification> receiver = make_receiver(short_type);

    BOOST_REQUIRE_THROW(prepare_expression(cast_expr, db, "test_ks", table_schema.get(), receiver),
                        exceptions::invalid_request_exception);
}

// Test that preparing `(text)?` without a receiver works.
// Here (text) serves as a type hint that specifies the type of the bind variable.
// This syntax is useful for passing bind variables in places where it's impossible
// to infer the type from context, for example as an argument for a function
// with multiple overloads.
BOOST_AUTO_TEST_CASE(prepare_cast_bind_var_no_receiver) {
    schema_ptr table_schema = make_simple_test_schema();
    auto [db, db_data] = make_data_dictionary_database(table_schema);

    expression cast_expr =
        cast{.arg = bind_variable{.bind_index = 0, .receiver = nullptr}, .type = cql3_type::raw::from(utf8_type)};

    expression prepared = prepare_expression(cast_expr, db, "test_ks", table_schema.get(), nullptr);

    // Can't do a direct comparison because we don't have prepared.arg.receiver
    BOOST_REQUIRE(is<cast>(prepared) && is<bind_variable>(as<cast>(prepared).arg));
    ::lw_shared_ptr<column_specification> bind_var_receiver = as<bind_variable>(as<cast>(prepared).arg).receiver;
    BOOST_REQUIRE(bind_var_receiver->type == utf8_type);

    expression expected = cast{.arg = bind_variable{.bind_index = 0, .receiver = bind_var_receiver}, .type = utf8_type};
    BOOST_REQUIRE_EQUAL(prepared, expected);

    BOOST_REQUIRE(expr::type_of(prepared) == utf8_type);
}

// Test preparing (text)? with a known text receiver.
// Corresponds to `text_col = (text)?`
BOOST_AUTO_TEST_CASE(prepare_cast_bind_var_text_type_hint_and_text_receiver) {
    schema_ptr table_schema = make_simple_test_schema();
    auto [db, db_data] = make_data_dictionary_database(table_schema);

    expression cast_expr =
        cast{.arg = bind_variable{.bind_index = 0, .receiver = nullptr}, .type = cql3_type::raw::from(utf8_type)};

    expression prepared = prepare_expression(cast_expr, db, "test_ks", table_schema.get(), make_receiver(utf8_type));

    // Can't do a direct comparison because we don't have prepared.arg.receiver
    BOOST_REQUIRE(is<cast>(prepared) && is<bind_variable>(as<cast>(prepared).arg));
    ::lw_shared_ptr<column_specification> bind_var_receiver = as<bind_variable>(as<cast>(prepared).arg).receiver;
    BOOST_REQUIRE(bind_var_receiver->type == utf8_type);

    expression expected = cast{.arg = bind_variable{.bind_index = 0, .receiver = bind_var_receiver}, .type = utf8_type};
    BOOST_REQUIRE_EQUAL(prepared, expected);

    BOOST_REQUIRE(expr::type_of(prepared) == utf8_type);
}

// Test preparing (int)? with a known int receiver.
// Corresponds to `int_col = (int)?`
BOOST_AUTO_TEST_CASE(prepare_cast_bind_var_int_type_hint_and_int_receiver) {
    schema_ptr table_schema = make_simple_test_schema();
    auto [db, db_data] = make_data_dictionary_database(table_schema);

    expression cast_expr =
        cast{.arg = bind_variable{.bind_index = 0, .receiver = nullptr}, .type = cql3_type::raw::from(int32_type)};

    expression prepared = prepare_expression(cast_expr, db, "test_ks", table_schema.get(), make_receiver(int32_type));

    // Can't do a direct comparison because we don't have prepared.arg.receiver
    BOOST_REQUIRE(is<cast>(prepared) && is<bind_variable>(as<cast>(prepared).arg));
    ::lw_shared_ptr<column_specification> bind_var_receiver = as<bind_variable>(as<cast>(prepared).arg).receiver;
    BOOST_REQUIRE(bind_var_receiver->type == int32_type);

    expression expected =
        cast{.arg = bind_variable{.bind_index = 0, .receiver = bind_var_receiver}, .type = int32_type};
    BOOST_REQUIRE_EQUAL(prepared, expected);

    BOOST_REQUIRE(expr::type_of(prepared) == int32_type);
}

// Test preparing (text)? with a known int receiver.
// Corresponds to `int_col = (text)?`
BOOST_AUTO_TEST_CASE(prepare_cast_bind_var_text_type_hint_and_int_receiver) {
    schema_ptr table_schema = make_simple_test_schema();
    auto [db, db_data] = make_data_dictionary_database(table_schema);

    expression cast_expr =
        cast{.arg = bind_variable{.bind_index = 0, .receiver = nullptr}, .type = cql3_type::raw::from(utf8_type)};

    BOOST_REQUIRE_THROW(prepare_expression(cast_expr, db, "test_ks", table_schema.get(), make_receiver(int32_type)),
                        exceptions::invalid_request_exception);
}

// Test preparing (int)? with a known text receiver.
// Corresponds to `text_col = (int)?`
BOOST_AUTO_TEST_CASE(prepare_cast_bind_var_int_type_hint_and_text_receiver) {
    schema_ptr table_schema = make_simple_test_schema();
    auto [db, db_data] = make_data_dictionary_database(table_schema);

    expression cast_expr =
        cast{.arg = bind_variable{.bind_index = 0, .receiver = nullptr}, .type = cql3_type::raw::from(int32_type)};

    BOOST_REQUIRE_THROW(prepare_expression(cast_expr, db, "test_ks", table_schema.get(), make_receiver(utf8_type)),
                        exceptions::invalid_request_exception);
}

BOOST_AUTO_TEST_CASE(prepare_null) {
    schema_ptr table_schema = make_simple_test_schema();
    auto [db, db_data] = make_data_dictionary_database(table_schema);

    expression null_expr = make_untyped_null();

    expression prepared = prepare_expression(null_expr, db, "test_ks", table_schema.get(), make_receiver(int32_type));
    expression expected = constant::make_null(int32_type);
    BOOST_REQUIRE_EQUAL(prepared, expected);
}

// null can't be prepared without a receiver because we are unable to infer the type.
BOOST_AUTO_TEST_CASE(prepare_null_no_type_fails) {
    schema_ptr table_schema = make_simple_test_schema();
    auto [db, db_data] = make_data_dictionary_database(table_schema);

    expression null_expr = make_untyped_null();
    BOOST_REQUIRE_THROW(prepare_expression(null_expr, db, "test_ks", table_schema.get(), nullptr),
                        exceptions::invalid_request_exception);
}

BOOST_AUTO_TEST_CASE(prepare_bind_variable) {
    schema_ptr table_schema = make_simple_test_schema();
    auto [db, db_data] = make_data_dictionary_database(table_schema);

    expression bind_var = bind_variable{.bind_index = 1, .receiver = nullptr};

    ::lw_shared_ptr<column_specification> receiver = make_receiver(int32_type);

    expression prepared = prepare_expression(bind_var, db, "test_ks", table_schema.get(), receiver);

    expression expected = bind_variable{
        .bind_index = 1,
        .receiver = receiver,
    };

    BOOST_REQUIRE_EQUAL(prepared, expected);
}

BOOST_AUTO_TEST_CASE(prepare_bind_variable_no_receiver) {
    schema_ptr table_schema = make_simple_test_schema();
    auto [db, db_data] = make_data_dictionary_database(table_schema);

    expression bind_var = bind_variable{.bind_index = 1, .receiver = nullptr};

    BOOST_REQUIRE_THROW(prepare_expression(bind_var, db, "test_ks", table_schema.get(), nullptr),
                        exceptions::invalid_request_exception);
}

BOOST_AUTO_TEST_CASE(prepare_untyped_constant_no_receiver) {
    schema_ptr table_schema = make_simple_test_schema();
    auto [db, db_data] = make_data_dictionary_database(table_schema);

    expression untyped = make_int_untyped("1337");

    // Can't infer type
    BOOST_REQUIRE_THROW(prepare_expression(untyped, db, "test_ks", table_schema.get(), nullptr),
                        exceptions::invalid_request_exception);
}

BOOST_AUTO_TEST_CASE(prepare_untyped_constant_bool) {
    schema_ptr table_schema = make_simple_test_schema();
    auto [db, db_data] = make_data_dictionary_database(table_schema);

    expression untyped = make_bool_untyped("true");

    expression prepared = prepare_expression(untyped, db, "test_ks", table_schema.get(), make_receiver(boolean_type));
    expression expected = make_bool_const(true);

    BOOST_REQUIRE_EQUAL(prepared, expected);
}

BOOST_AUTO_TEST_CASE(prepare_untyped_constant_int8) {
    schema_ptr table_schema = make_simple_test_schema();
    auto [db, db_data] = make_data_dictionary_database(table_schema);

    expression untyped = make_int_untyped("13");

    expression prepared = prepare_expression(untyped, db, "test_ks", table_schema.get(), make_receiver(byte_type));
    expression expected = make_tinyint_const(13);

    BOOST_REQUIRE_EQUAL(prepared, expected);
}

BOOST_AUTO_TEST_CASE(prepare_untyped_constant_int16) {
    schema_ptr table_schema = make_simple_test_schema();
    auto [db, db_data] = make_data_dictionary_database(table_schema);

    expression untyped = make_int_untyped("1337");

    expression prepared = prepare_expression(untyped, db, "test_ks", table_schema.get(), make_receiver(short_type));
    expression expected = make_smallint_const(1337);

    BOOST_REQUIRE_EQUAL(prepared, expected);
}

BOOST_AUTO_TEST_CASE(prepare_untyped_constant_int32) {
    schema_ptr table_schema = make_simple_test_schema();
    auto [db, db_data] = make_data_dictionary_database(table_schema);

    expression untyped =
        make_int_untyped("13377331");

    expression prepared = prepare_expression(untyped, db, "test_ks", table_schema.get(), make_receiver(int32_type));
    expression expected = make_int_const(13377331);

    BOOST_REQUIRE_EQUAL(prepared, expected);
}

BOOST_AUTO_TEST_CASE(prepare_untyped_constant_int64) {
    schema_ptr table_schema = make_simple_test_schema();
    auto [db, db_data] = make_data_dictionary_database(table_schema);

    expression untyped =
        make_int_untyped("1337733113377331");

    expression prepared = prepare_expression(untyped, db, "test_ks", table_schema.get(), make_receiver(long_type));
    expression expected = make_bigint_const(1337733113377331);

    BOOST_REQUIRE_EQUAL(prepared, expected);
}

BOOST_AUTO_TEST_CASE(prepare_untyped_constant_text) {
    schema_ptr table_schema = make_simple_test_schema();
    auto [db, db_data] = make_data_dictionary_database(table_schema);

    expression untyped =
        make_string_untyped("scylla_is_the_best");

    expression prepared = prepare_expression(untyped, db, "test_ks", table_schema.get(), make_receiver(utf8_type));
    expression expected = make_text_const("scylla_is_the_best");

    BOOST_REQUIRE_EQUAL(prepared, expected);
}

BOOST_AUTO_TEST_CASE(prepare_untyped_constant_bad_int) {
    schema_ptr table_schema = make_simple_test_schema();
    auto [db, db_data] = make_data_dictionary_database(table_schema);

    expression untyped =
        make_int_untyped("not_integer_text");

    BOOST_REQUIRE_THROW(prepare_expression(untyped, db, "test_ks", table_schema.get(), make_receiver(int32_type)),
                        exceptions::invalid_request_exception);
}

BOOST_AUTO_TEST_CASE(prepare_tuple_constructor_no_receiver_fails) {
    schema_ptr table_schema = make_simple_test_schema();
    auto [db, db_data] = make_data_dictionary_database(table_schema);

    expression tup = tuple_constructor{
        .elements =
            {
                make_int_untyped("123"),
                make_int_untyped("456"),
                make_string_untyped("some text"),
            },
        .type = nullptr};

    BOOST_REQUIRE_THROW(prepare_expression(tup, db, "test_ks", table_schema.get(), nullptr),
                        exceptions::invalid_request_exception);
}

BOOST_AUTO_TEST_CASE(prepare_tuple_constructor) {
    schema_ptr table_schema = make_simple_test_schema();
    auto [db, db_data] = make_data_dictionary_database(table_schema);

    expression tup = tuple_constructor{
        .elements =
            {
                make_int_untyped("123"),
                make_int_untyped("456"),
                make_string_untyped("some text"),
            },
        .type = nullptr};

    data_type tup_type = tuple_type_impl::get_instance({int32_type, short_type, utf8_type});
    ::lw_shared_ptr<column_specification> receiver = make_receiver(tup_type);

    expression prepared = prepare_expression(tup, db, "test_ks", table_schema.get(), receiver);
    expression expected =
        make_tuple_const({make_int_const(123), make_smallint_const(456), make_text_const("some text")},
                         {int32_type, short_type, utf8_type});

    BOOST_REQUIRE_EQUAL(prepared, expected);
}

BOOST_AUTO_TEST_CASE(prepare_tuple_constructor_of_columns) {
    schema_ptr table_schema = schema_builder("test_ks", "test_cf")
                                  .with_column("pk", int32_type, column_kind::partition_key)
                                  .with_column("c1", int32_type, column_kind::clustering_key)
                                  .with_column("c2", utf8_type, column_kind::clustering_key)
                                  .with_column("c3", byte_type, column_kind::clustering_key)
                                  .build();
    auto [db, db_data] = make_data_dictionary_database(table_schema);

    expression tup = tuple_constructor{
        .elements = {unresolved_identifier{.ident = ::make_shared<column_identifier_raw>("c1", true)},
                     unresolved_identifier{.ident = ::make_shared<column_identifier_raw>("c2", true)},
                     unresolved_identifier{.ident = ::make_shared<column_identifier_raw>("c3", true)}},
        .type = nullptr};

    data_type tup_type = tuple_type_impl::get_instance({int32_type, utf8_type, byte_type});

    expression prepared = prepare_expression(tup, db, "test_ks", table_schema.get(), nullptr);
    expression expected = tuple_constructor{.elements =
                                                {
                                                    column_value(table_schema->get_column_definition("c1")),
                                                    column_value(table_schema->get_column_definition("c2")),
                                                    column_value(table_schema->get_column_definition("c3")),
                                                },
                                            .type = tup_type};

    BOOST_REQUIRE_EQUAL(prepared, expected);
}

BOOST_AUTO_TEST_CASE(prepare_list_collection_constructor) {
    schema_ptr table_schema = make_simple_test_schema();
    auto [db, db_data] = make_data_dictionary_database(table_schema);

    expression constructor = collection_constructor{
        .style = collection_constructor::style_type::list,
        .elements =
            {
                make_int_untyped("123"),
                make_int_untyped("456"),
                make_int_untyped("789"),
            },
        .type = nullptr};

    data_type list_type = list_type_impl::get_instance(long_type, true);

    expression prepared = prepare_expression(constructor, db, "test_ks", table_schema.get(), make_receiver(list_type));
    expression expected =
        make_list_const({make_bigint_const(123), make_bigint_const(456), make_bigint_const(789)}, long_type);

    BOOST_REQUIRE_EQUAL(prepared, expected);
}

// preparing empty nonfrozen collections results in null
BOOST_AUTO_TEST_CASE(prepare_list_collection_constructor_empty_nonfrozen) {
    schema_ptr table_schema = make_simple_test_schema();
    auto [db, db_data] = make_data_dictionary_database(table_schema);

    expression constructor =
        collection_constructor{.style = collection_constructor::style_type::list, .elements = {}, .type = nullptr};

    data_type list_type = list_type_impl::get_instance(long_type, true);

    expression prepared = prepare_expression(constructor, db, "test_ks", table_schema.get(), make_receiver(list_type));
    expression expected = constant::make_null(list_type);

    BOOST_REQUIRE_EQUAL(prepared, expected);
}

BOOST_AUTO_TEST_CASE(prepare_list_collection_constructor_empty_frozen) {
    schema_ptr table_schema = make_simple_test_schema();
    auto [db, db_data] = make_data_dictionary_database(table_schema);

    expression constructor =
        collection_constructor{.style = collection_constructor::style_type::list, .elements = {}, .type = nullptr};

    data_type list_type = list_type_impl::get_instance(long_type, false);

    expression prepared = prepare_expression(constructor, db, "test_ks", table_schema.get(), make_receiver(list_type));
    expression expected = constant(make_list_raw({}), list_type);

    BOOST_REQUIRE_EQUAL(prepared, expected);
}

BOOST_AUTO_TEST_CASE(prepare_list_collection_constructor_no_receiver) {
    schema_ptr table_schema = make_simple_test_schema();
    auto [db, db_data] = make_data_dictionary_database(table_schema);

    expression constructor = collection_constructor{
        .style = collection_constructor::style_type::list,
        .elements =
            {
                make_int_untyped("123"),
                make_int_untyped("456"),
                make_int_untyped("789"),
            },
        .type = nullptr};

    data_type list_type = list_type_impl::get_instance(long_type, true);

    BOOST_REQUIRE_THROW(prepare_expression(constructor, db, "test_ks", table_schema.get(), nullptr),
                        exceptions::invalid_request_exception);
}

BOOST_AUTO_TEST_CASE(prepare_list_collection_constructor_with_bind_var) {
    schema_ptr table_schema = make_simple_test_schema();
    auto [db, db_data] = make_data_dictionary_database(table_schema);

    expression constructor = collection_constructor{
        .style = collection_constructor::style_type::list,
        .elements =
            {
                make_int_untyped("123"),
                bind_variable{.bind_index = 1, .receiver = nullptr},
                make_int_untyped("789"),
            },
        .type = nullptr};

    data_type list_type = list_type_impl::get_instance(long_type, true);

    expression prepared = prepare_expression(constructor, db, "test_ks", table_schema.get(), make_receiver(list_type));

    // prepared bind_variable contains a receiver which we need to extract
    // in order to prepare an equal expected value.
    collection_constructor* prepared_constructor = as_if<collection_constructor>(&prepared);
    BOOST_REQUIRE(prepared_constructor != nullptr);
    BOOST_REQUIRE_EQUAL(prepared_constructor->elements.size(), 3);

    bind_variable* prepared_bind_var = as_if<bind_variable>(&prepared_constructor->elements[1]);
    BOOST_REQUIRE(prepared_bind_var != nullptr);

    ::lw_shared_ptr<column_specification> bind_var_receiver = prepared_bind_var->receiver;
    BOOST_REQUIRE(bind_var_receiver.get() != nullptr);
    BOOST_REQUIRE(bind_var_receiver->type == long_type);

    expression expected = collection_constructor{
        .style = collection_constructor::style_type::list,
        .elements = {make_bigint_const(123), bind_variable{.bind_index = 1, .receiver = bind_var_receiver},
                     make_bigint_const(789)},
        .type = list_type};

    BOOST_REQUIRE_EQUAL(prepared, expected);
}

BOOST_AUTO_TEST_CASE(prepare_list_collection_constructor_with_null) {
    schema_ptr table_schema = make_simple_test_schema();
    auto [db, db_data] = make_data_dictionary_database(table_schema);

    expression constructor = collection_constructor{
        .style = collection_constructor::style_type::list,
        .elements = {make_int_untyped("123"),
                     make_int_untyped("456"),
                     make_untyped_null()},
        .type = nullptr};

    data_type list_type = list_type_impl::get_instance(int32_type, true);

    BOOST_REQUIRE_EQUAL(prepare_expression(constructor, db, "test_ks", table_schema.get(), make_receiver(list_type)),
                        make_int_list_const({123, 456, std::nullopt}));
}

BOOST_AUTO_TEST_CASE(prepare_set_collection_constructor) {
    schema_ptr table_schema = make_simple_test_schema();
    auto [db, db_data] = make_data_dictionary_database(table_schema);

    expression constructor = collection_constructor{
        .style = collection_constructor::style_type::set,
        .elements =
            {
                make_int_untyped("789"),
                make_int_untyped("123"),
                make_int_untyped("456"),
            },
        .type = nullptr};

    data_type set_type = set_type_impl::get_instance(short_type, true);

    expression prepared = prepare_expression(constructor, db, "test_ks", table_schema.get(), make_receiver(set_type));
    expression expected =
        make_set_const({make_smallint_const(123), make_smallint_const(456), make_smallint_const(789)}, short_type);

    BOOST_REQUIRE_EQUAL(prepared, expected);
}

// preparing empty nonfrozen collections results in null
BOOST_AUTO_TEST_CASE(prepare_set_collection_constructor_empty_nonfrozen) {
    schema_ptr table_schema = make_simple_test_schema();
    auto [db, db_data] = make_data_dictionary_database(table_schema);

    expression constructor =
        collection_constructor{.style = collection_constructor::style_type::set, .elements = {}, .type = nullptr};

    data_type set_type = set_type_impl::get_instance(short_type, true);

    expression prepared = prepare_expression(constructor, db, "test_ks", table_schema.get(), make_receiver(set_type));
    expression expected = constant::make_null(set_type);

    BOOST_REQUIRE_EQUAL(prepared, expected);
}

BOOST_AUTO_TEST_CASE(prepare_set_collection_constructor_empty_frozen) {
    schema_ptr table_schema = make_simple_test_schema();
    auto [db, db_data] = make_data_dictionary_database(table_schema);

    expression constructor =
        collection_constructor{.style = collection_constructor::style_type::set, .elements = {}, .type = nullptr};

    data_type set_type = set_type_impl::get_instance(short_type, false);

    expression prepared = prepare_expression(constructor, db, "test_ks", table_schema.get(), make_receiver(set_type));
    expression expected = constant(make_set_raw({}), set_type);

    BOOST_REQUIRE_EQUAL(prepared, expected);
}

BOOST_AUTO_TEST_CASE(prepare_set_collection_constructor_no_receiver) {
    schema_ptr table_schema = make_simple_test_schema();
    auto [db, db_data] = make_data_dictionary_database(table_schema);

    expression constructor = collection_constructor{
        .style = collection_constructor::style_type::set,
        .elements =
            {
                make_int_untyped("789"),
                make_int_untyped("123"),
                make_int_untyped("456"),
            },
        .type = nullptr};

    BOOST_REQUIRE_THROW(prepare_expression(constructor, db, "test_ks", table_schema.get(), nullptr),
                        exceptions::invalid_request_exception);
}

BOOST_AUTO_TEST_CASE(prepare_set_collection_constructor_with_bind_var) {
    schema_ptr table_schema = make_simple_test_schema();
    auto [db, db_data] = make_data_dictionary_database(table_schema);

    expression constructor = collection_constructor{
        .style = collection_constructor::style_type::set,
        .elements =
            {
                make_int_untyped("789"),
                bind_variable{.bind_index = 1, .receiver = nullptr},
                make_int_untyped("123"),
            },
        .type = nullptr};

    data_type set_type = set_type_impl::get_instance(long_type, true);

    expression prepared = prepare_expression(constructor, db, "test_ks", table_schema.get(), make_receiver(set_type));

    // Can't directly compare because the bind variable receiver is created inside prepare_expression
    // prepared bind_variable contains a receiver which we need to extract
    // in order to prepare an equal expected value.
    collection_constructor* prepared_constructor = as_if<collection_constructor>(&prepared);
    BOOST_REQUIRE(prepared_constructor != nullptr);
    BOOST_REQUIRE_EQUAL(prepared_constructor->elements.size(), 3);

    bind_variable* prepared_bind_var = as_if<bind_variable>(&prepared_constructor->elements[1]);
    BOOST_REQUIRE(prepared_bind_var != nullptr);

    ::lw_shared_ptr<column_specification> bind_var_receiver = prepared_bind_var->receiver;
    BOOST_REQUIRE(bind_var_receiver.get() != nullptr);
    BOOST_REQUIRE(bind_var_receiver->type == long_type);

    expression expected = collection_constructor{
        .style = collection_constructor::style_type::set,
        .elements = {make_bigint_const(789), bind_variable{.bind_index = 1, .receiver = bind_var_receiver},
                     make_bigint_const(123)},
        .type = set_type};
}

BOOST_AUTO_TEST_CASE(prepare_set_collection_constructor_with_null) {
    schema_ptr table_schema = make_simple_test_schema();
    auto [db, db_data] = make_data_dictionary_database(table_schema);

    expression constructor = collection_constructor{
        .style = collection_constructor::style_type::set,
        .elements =
            {
                make_int_untyped("789"),
                make_untyped_null(),
                make_int_untyped("456"),
            },
        .type = nullptr};

    data_type set_type = set_type_impl::get_instance(short_type, true);

    BOOST_REQUIRE_THROW(prepare_expression(constructor, db, "test_ks", table_schema.get(), make_receiver(set_type)),
                        exceptions::invalid_request_exception);
}

BOOST_AUTO_TEST_CASE(prepare_map_collection_constructor) {
    schema_ptr table_schema = make_simple_test_schema();
    auto [db, db_data] = make_data_dictionary_database(table_schema);

    expression constructor =
        collection_constructor{
            .style = collection_constructor::style_type::map,
            .elements =
                {
                    tuple_constructor{
                        .elements =
                            {make_int_untyped("3"),
                             make_int_untyped("30")},
                        .type = nullptr},
                    tuple_constructor{
                        .elements = {make_int_untyped("2"),
                                     make_int_untyped("-20")},
                        .type = nullptr},
                    tuple_constructor{
                        .elements = {make_int_untyped("1"),
                                     make_int_untyped("10")},
                        .type = nullptr},
                },
            .type = nullptr};

    data_type map_type = map_type_impl::get_instance(short_type, long_type, true);

    expression prepared = prepare_expression(constructor, db, "test_ks", table_schema.get(), make_receiver(map_type));

    expression expected = make_map_const({{make_smallint_const(1), make_bigint_const(10)},
                                          {make_smallint_const(2), make_bigint_const(-20)},
                                          {make_smallint_const(3), make_bigint_const(30)}},
                                         short_type, long_type);

    BOOST_REQUIRE_EQUAL(prepared, expected);
}

// preparing empty nonfrozen collections results in null
BOOST_AUTO_TEST_CASE(prepare_map_collection_constructor_empty_nonfrozen) {
    schema_ptr table_schema = make_simple_test_schema();
    auto [db, db_data] = make_data_dictionary_database(table_schema);

    expression constructor =
        collection_constructor{.style = collection_constructor::style_type::map, .elements = {}, .type = nullptr};

    data_type map_type = map_type_impl::get_instance(short_type, long_type, true);

    expression prepared = prepare_expression(constructor, db, "test_ks", table_schema.get(), make_receiver(map_type));
    expression expected = constant::make_null(map_type);

    BOOST_REQUIRE_EQUAL(prepared, expected);
}

BOOST_AUTO_TEST_CASE(prepare_map_collection_constructor_empty_frozen) {
    schema_ptr table_schema = make_simple_test_schema();
    auto [db, db_data] = make_data_dictionary_database(table_schema);

    expression constructor =
        collection_constructor{.style = collection_constructor::style_type::map, .elements = {}, .type = nullptr};

    data_type map_type = map_type_impl::get_instance(short_type, long_type, false);

    expression prepared = prepare_expression(constructor, db, "test_ks", table_schema.get(), make_receiver(map_type));
    expression expected = constant(make_map_raw({}), map_type);

    BOOST_REQUIRE_EQUAL(prepared, expected);
}

BOOST_AUTO_TEST_CASE(prepare_map_collection_constructor_no_receiver) {
    schema_ptr table_schema = make_simple_test_schema();
    auto [db, db_data] = make_data_dictionary_database(table_schema);

    expression constructor =
        collection_constructor{
            .style = collection_constructor::style_type::map,
            .elements =
                {
                    tuple_constructor{
                        .elements =
                            {make_int_untyped("3"),
                             make_int_untyped("30")},
                        .type = nullptr},
                    tuple_constructor{
                        .elements = {make_int_untyped("2"),
                                     make_int_untyped("-20")},
                        .type = nullptr},
                    tuple_constructor{
                        .elements = {make_int_untyped("1"),
                                     make_int_untyped("10")},
                        .type = nullptr},
                },
            .type = nullptr};

    BOOST_REQUIRE_THROW(prepare_expression(constructor, db, "test_ks", table_schema.get(), nullptr),
                        exceptions::invalid_request_exception);
}

BOOST_AUTO_TEST_CASE(prepare_map_collection_constructor_with_bind_var_key) {
    schema_ptr table_schema = make_simple_test_schema();
    auto [db, db_data] = make_data_dictionary_database(table_schema);

    expression constructor =
        collection_constructor{
            .style = collection_constructor::style_type::map,
            .elements =
                {
                    tuple_constructor{
                        .elements =
                            {make_int_untyped("3"),
                             make_int_untyped("30")},
                        .type = nullptr},
                    tuple_constructor{
                        .elements = {bind_variable{.bind_index = 1, .receiver = nullptr},
                                     make_int_untyped("-20")},
                        .type = nullptr},
                    tuple_constructor{
                        .elements = {make_int_untyped("1"),
                                     make_int_untyped("10")},
                        .type = nullptr},
                },
            .type = nullptr};

    data_type map_type = map_type_impl::get_instance(short_type, long_type, true);

    expression prepared = prepare_expression(constructor, db, "test_ks", table_schema.get(), make_receiver(map_type));

    // prepared bind_variable contains a receiver which we need to extract
    // in order to prepare an equal expected value.
    collection_constructor* prepared_constructor = as_if<collection_constructor>(&prepared);
    BOOST_REQUIRE(prepared_constructor != nullptr);
    BOOST_REQUIRE_EQUAL(prepared_constructor->elements.size(), 3);

    tuple_constructor* prepared_tup = as_if<tuple_constructor>(&prepared_constructor->elements[1]);
    BOOST_REQUIRE(prepared_tup != nullptr);
    BOOST_REQUIRE_EQUAL(prepared_tup->elements.size(), 2);

    bind_variable* prepared_bind_var = as_if<bind_variable>(&prepared_tup->elements[0]);
    BOOST_REQUIRE(prepared_bind_var != nullptr);

    ::lw_shared_ptr<column_specification> bind_var_receiver = prepared_bind_var->receiver;
    BOOST_REQUIRE(bind_var_receiver->type == short_type);

    expression expected = collection_constructor{
        .style = collection_constructor::style_type::map,
        .elements =
            {
                tuple_constructor{.elements = {make_smallint_const(3), make_bigint_const(30)},
                                  .type = tuple_type_impl::get_instance({short_type, long_type})},
                tuple_constructor{
                    .elements = {bind_variable{.bind_index = 1, .receiver = bind_var_receiver}, make_bigint_const(-20)},
                    .type = tuple_type_impl::get_instance({short_type, long_type})},
                tuple_constructor{.elements = {make_smallint_const(1), make_bigint_const(10)},
                                  .type = tuple_type_impl::get_instance({short_type, long_type})},
            },
        .type = map_type};

    BOOST_REQUIRE_EQUAL(prepared, expected);
}

BOOST_AUTO_TEST_CASE(prepare_map_collection_constructor_with_bind_var_value) {
    schema_ptr table_schema = make_simple_test_schema();
    auto [db, db_data] = make_data_dictionary_database(table_schema);

    expression constructor = collection_constructor{
        .style = collection_constructor::style_type::map,
        .elements =
            {
                tuple_constructor{.elements = {make_int_untyped("3"),
                                               make_int_untyped("30")},
                                  .type = nullptr},
                tuple_constructor{.elements = {make_int_untyped("2"),
                                               bind_variable{.bind_index = 1, .receiver = nullptr}},
                                  .type = nullptr},
                tuple_constructor{.elements = {make_int_untyped("1"),
                                               make_int_untyped("10")},
                                  .type = nullptr},
            },
        .type = nullptr};

    data_type map_type = map_type_impl::get_instance(short_type, long_type, true);

    expression prepared = prepare_expression(constructor, db, "test_ks", table_schema.get(), make_receiver(map_type));

    // prepared bind_variable contains a receiver which we need to extract
    // in order to prepare an equal expected value.
    collection_constructor* prepared_constructor = as_if<collection_constructor>(&prepared);
    BOOST_REQUIRE(prepared_constructor != nullptr);
    BOOST_REQUIRE_EQUAL(prepared_constructor->elements.size(), 3);

    tuple_constructor* prepared_tup = as_if<tuple_constructor>(&prepared_constructor->elements[1]);
    BOOST_REQUIRE(prepared_tup != nullptr);
    BOOST_REQUIRE_EQUAL(prepared_tup->elements.size(), 2);

    bind_variable* prepared_bind_var = as_if<bind_variable>(&prepared_tup->elements[1]);
    BOOST_REQUIRE(prepared_bind_var != nullptr);

    ::lw_shared_ptr<column_specification> bind_var_receiver = prepared_bind_var->receiver;
    BOOST_REQUIRE(bind_var_receiver->type == long_type);

    expression expected = collection_constructor{
        .style = collection_constructor::style_type::map,
        .elements =
            {
                tuple_constructor{.elements = {make_smallint_const(3), make_bigint_const(30)},
                                  .type = tuple_type_impl::get_instance({short_type, long_type})},
                tuple_constructor{
                    .elements = {make_smallint_const(2), bind_variable{.bind_index = 1, .receiver = bind_var_receiver}},
                    .type = tuple_type_impl::get_instance({short_type, long_type})},
                tuple_constructor{.elements = {make_smallint_const(1), make_bigint_const(10)},
                                  .type = tuple_type_impl::get_instance({short_type, long_type})},
            },
        .type = map_type};

    BOOST_REQUIRE_EQUAL(prepared, expected);
}

BOOST_AUTO_TEST_CASE(prepare_map_collection_constructor_null_key) {
    schema_ptr table_schema = make_simple_test_schema();
    auto [db, db_data] = make_data_dictionary_database(table_schema);

    expression constructor =
        collection_constructor{
            .style = collection_constructor::style_type::map,
            .elements =
                {
                    tuple_constructor{
                        .elements =
                            {make_int_untyped("3"),
                             make_int_untyped("30")},
                        .type = nullptr},
                    tuple_constructor{
                        .elements = {make_untyped_null(),
                                     make_int_untyped("-20")},
                        .type = nullptr},
                    tuple_constructor{
                        .elements = {make_int_untyped("1"),
                                     make_int_untyped("10")},
                        .type = nullptr},
                },
            .type = nullptr};

    data_type map_type = map_type_impl::get_instance(short_type, long_type, true);

    BOOST_REQUIRE_THROW(prepare_expression(constructor, db, "test_ks", table_schema.get(), make_receiver(map_type)),
                        exceptions::invalid_request_exception);
}

BOOST_AUTO_TEST_CASE(prepare_map_collection_constructor_null_value) {
    schema_ptr table_schema = make_simple_test_schema();
    auto [db, db_data] = make_data_dictionary_database(table_schema);

    expression constructor = collection_constructor{
        .style = collection_constructor::style_type::map,
        .elements =
            {
                tuple_constructor{.elements = {make_int_untyped("3"),
                                               make_int_untyped("30")},
                                  .type = nullptr},
                tuple_constructor{.elements = {make_int_untyped("2"),
                                               make_untyped_null()},
                                  .type = nullptr},
                tuple_constructor{.elements = {make_int_untyped("1"),
                                               make_int_untyped("10")},
                                  .type = nullptr},
            },
        .type = nullptr};

    data_type map_type = map_type_impl::get_instance(short_type, long_type, true);

    BOOST_REQUIRE_THROW(prepare_expression(constructor, db, "test_ks", table_schema.get(), make_receiver(map_type)),
                        exceptions::invalid_request_exception);
}

// preparing the collection constructor should check that the type of constructor
// matches the type of the receiver. style_type::set shouldn't be assignable to a list.
BOOST_AUTO_TEST_CASE(prepare_collection_constructor_checks_style_type) {
    schema_ptr table_schema = make_simple_test_schema();
    auto [db, db_data] = make_data_dictionary_database(table_schema);

    expression set_constructor = collection_constructor{
        .style = collection_constructor::style_type::set,
        .elements = {make_int_untyped("123")},
        .type = nullptr};

    data_type set_type = set_type_impl::get_instance(int32_type, true);
    expression prepared =
        prepare_expression(set_constructor, db, "test_ks", table_schema.get(), make_receiver(set_type));
    expression expected = make_set_const({make_int_const(123)}, int32_type);
    BOOST_REQUIRE_EQUAL(prepared, expected);

    data_type list_type = list_type_impl::get_instance(int32_type, true);
    BOOST_REQUIRE_THROW(
        prepare_expression(set_constructor, db, "test_ks", table_schema.get(), make_receiver(list_type)),
        exceptions::invalid_request_exception);
}

BOOST_AUTO_TEST_CASE(prepare_usertype_constructor) {
    schema_ptr table_schema = make_simple_test_schema();
    auto [db, db_data] = make_data_dictionary_database(table_schema);

    usertype_constructor::elements_map_type constructor_elements;
    constructor_elements.emplace(
        column_identifier("field1", true),
        make_int_untyped("152"));
    constructor_elements.emplace(
        column_identifier("field2", true),
        make_int_untyped("987"));
    constructor_elements.emplace(
        column_identifier("field3", true),
        make_string_untyped("ututu"));

    expression constructor = usertype_constructor{.elements = constructor_elements, .type = nullptr};

    data_type user_type = user_type_impl::get_instance("test_ks", "test_ut", {"field1", "field2", "field3"},
                                                       {short_type, long_type, utf8_type}, true);

    expression prepared = prepare_expression(constructor, db, "test_ks", table_schema.get(), make_receiver(user_type));

    raw_value expected_raw = make_tuple_raw({make_smallint_raw(152), make_bigint_raw(987), make_text_raw("ututu")});
    expression expected = constant(expected_raw, user_type);

    BOOST_REQUIRE_EQUAL(prepared, expected);
}

BOOST_AUTO_TEST_CASE(prepare_usertype_constructor_with_null) {
    schema_ptr table_schema = make_simple_test_schema();
    auto [db, db_data] = make_data_dictionary_database(table_schema);

    usertype_constructor::elements_map_type constructor_elements;
    constructor_elements.emplace(
        column_identifier("field1", true),
        make_int_untyped("152"));
    constructor_elements.emplace(column_identifier("field2", true), make_untyped_null());
    constructor_elements.emplace(
        column_identifier("field3", true),
        make_string_untyped("ututu"));

    expression constructor = usertype_constructor{.elements = constructor_elements, .type = nullptr};

    data_type user_type = user_type_impl::get_instance("test_ks", "test_ut", {"field1", "field2", "field3"},
                                                       {short_type, long_type, utf8_type}, true);

    expression prepared = prepare_expression(constructor, db, "test_ks", table_schema.get(), make_receiver(user_type));

    raw_value expected_raw = make_tuple_raw({make_smallint_raw(152), raw_value::make_null(), make_text_raw("ututu")});
    expression expected = constant(expected_raw, user_type);

    BOOST_REQUIRE_EQUAL(prepared, expected);
}

// prepare_expression will treat all missing as if they were specified with value null.
BOOST_AUTO_TEST_CASE(prepare_usertype_constructor_missing_field) {
    schema_ptr table_schema = make_simple_test_schema();
    auto [db, db_data] = make_data_dictionary_database(table_schema);

    usertype_constructor::elements_map_type constructor_elements;
    constructor_elements.emplace(
        column_identifier("field1", true),
        make_int_untyped("152"));
    constructor_elements.emplace(
        column_identifier("field3", true),
        make_string_untyped("ututu"));

    expression constructor = usertype_constructor{.elements = constructor_elements, .type = nullptr};

    data_type user_type = user_type_impl::get_instance("test_ks", "test_ut", {"field1", "field2", "field3"},
                                                       {short_type, long_type, utf8_type}, true);

    expression prepared = prepare_expression(constructor, db, "test_ks", table_schema.get(), make_receiver(user_type));

    raw_value expected_raw = make_tuple_raw({make_smallint_raw(152), raw_value::make_null(), make_text_raw("ututu")});
    expression expected = constant(expected_raw, user_type);

    BOOST_REQUIRE_EQUAL(prepared, expected);
}

BOOST_AUTO_TEST_CASE(prepare_usertype_constructor_no_receiver) {
    schema_ptr table_schema = make_simple_test_schema();
    auto [db, db_data] = make_data_dictionary_database(table_schema);

    usertype_constructor::elements_map_type constructor_elements;
    constructor_elements.emplace(
        column_identifier("field1", true),
        make_int_untyped("152"));
    constructor_elements.emplace(
        column_identifier("field2", true),
        make_int_untyped("987"));
    constructor_elements.emplace(
        column_identifier("field3", true),
        make_string_untyped("ututu"));

    expression constructor = usertype_constructor{.elements = constructor_elements, .type = nullptr};

    BOOST_REQUIRE_THROW(prepare_expression(constructor, db, "test_ks", table_schema.get(), nullptr),
                        exceptions::invalid_request_exception);
}

BOOST_AUTO_TEST_CASE(prepare_usertype_constructor_with_bind_variable) {
    schema_ptr table_schema = make_simple_test_schema();
    auto [db, db_data] = make_data_dictionary_database(table_schema);

    usertype_constructor::elements_map_type constructor_elements;
    constructor_elements.emplace(
        column_identifier("field1", true),
        make_int_untyped("152"));
    constructor_elements.emplace(column_identifier("field2", true),
                                 bind_variable{.bind_index = 2, .receiver = nullptr});
    constructor_elements.emplace(
        column_identifier("field3", true),
        make_string_untyped("ututu"));

    expression constructor = usertype_constructor{.elements = constructor_elements, .type = nullptr};

    data_type user_type = user_type_impl::get_instance("test_ks", "test_ut", {"field1", "field2", "field3"},
                                                       {short_type, long_type, utf8_type}, true);

    expression prepared = prepare_expression(constructor, db, "test_ks", table_schema.get(), make_receiver(user_type));

    // prepared bind_variable contains a receiver which we need to extract
    // in order to prepare an equal expected value.
    usertype_constructor* prepared_constructor = as_if<usertype_constructor>(&prepared);
    BOOST_REQUIRE(prepared_constructor != nullptr);
    BOOST_REQUIRE(prepared_constructor->elements.contains(column_identifier("field2", true)));

    bind_variable* prepared_bind_var =
        as_if<bind_variable>(&prepared_constructor->elements.at(column_identifier("field2", true)));
    BOOST_REQUIRE(prepared_bind_var != nullptr);

    ::lw_shared_ptr<column_specification> bind_var_receiver = prepared_bind_var->receiver;
    BOOST_REQUIRE(bind_var_receiver.get() != nullptr);
    BOOST_REQUIRE(bind_var_receiver->type == long_type);

    usertype_constructor::elements_map_type expected_constructor_elements;
    expected_constructor_elements.emplace(column_identifier("field1", true), make_smallint_const(152));
    expected_constructor_elements.emplace(column_identifier("field2", true),
                                          bind_variable{.bind_index = 2, .receiver = bind_var_receiver});
    expected_constructor_elements.emplace(column_identifier("field3", true), make_text_const("ututu"));

    expression expected = usertype_constructor{.elements = expected_constructor_elements, .type = user_type};

    BOOST_REQUIRE_EQUAL(prepared, expected);
}

// A combination of a bind variable and a missing field.
// prepare_expression should properly fill in the missing field in this case as well.
BOOST_AUTO_TEST_CASE(prepare_usertype_constructor_with_bind_variable_and_missing_field) {
    schema_ptr table_schema = make_simple_test_schema();
    auto [db, db_data] = make_data_dictionary_database(table_schema);

    usertype_constructor::elements_map_type constructor_elements;
    constructor_elements.emplace(column_identifier("field2", true),
                                 bind_variable{.bind_index = 2, .receiver = nullptr});
    constructor_elements.emplace(
        column_identifier("field3", true),
        make_string_untyped("ututu"));

    expression constructor = usertype_constructor{.elements = constructor_elements, .type = nullptr};

    data_type user_type = user_type_impl::get_instance("test_ks", "test_ut", {"field1", "field2", "field3"},
                                                       {short_type, long_type, utf8_type}, true);

    expression prepared = prepare_expression(constructor, db, "test_ks", table_schema.get(), make_receiver(user_type));

    // prepared bind_variable contains a receiver which we need to extract
    // in order to prepare an equal expected value.
    usertype_constructor* prepared_constructor = as_if<usertype_constructor>(&prepared);
    BOOST_REQUIRE(prepared_constructor != nullptr);
    BOOST_REQUIRE(prepared_constructor->elements.contains(column_identifier("field2", true)));

    bind_variable* prepared_bind_var =
        as_if<bind_variable>(&prepared_constructor->elements.at(column_identifier("field2", true)));
    BOOST_REQUIRE(prepared_bind_var != nullptr);

    ::lw_shared_ptr<column_specification> bind_var_receiver = prepared_bind_var->receiver;
    BOOST_REQUIRE(bind_var_receiver.get() != nullptr);
    BOOST_REQUIRE(bind_var_receiver->type == long_type);

    usertype_constructor::elements_map_type expected_constructor_elements;
    expected_constructor_elements.emplace(column_identifier("field1", true), constant::make_null(short_type));
    expected_constructor_elements.emplace(column_identifier("field2", true),
                                          bind_variable{.bind_index = 2, .receiver = bind_var_receiver});
    expected_constructor_elements.emplace(column_identifier("field3", true), make_text_const("ututu"));

    expression expected = usertype_constructor{.elements = expected_constructor_elements, .type = user_type};

    BOOST_REQUIRE_EQUAL(prepared, expected);
}

BOOST_AUTO_TEST_CASE(prepare_constant_no_receiver) {
    schema_ptr table_schema = make_simple_test_schema();
    auto [db, db_data] = make_data_dictionary_database(table_schema);

    expression int_const = make_int_const(1234);
    expression prepared_int_const = prepare_expression(int_const, db, "test_ks", table_schema.get(), nullptr);

    BOOST_REQUIRE_EQUAL(int_const, prepared_int_const);

    expression text_const = make_text_const("helo");
    expression prepared_text_const = prepare_expression(text_const, db, "test_ks", table_schema.get(), nullptr);

    BOOST_REQUIRE_EQUAL(text_const, prepared_text_const);
}

BOOST_AUTO_TEST_CASE(prepare_constant_with_receiver) {
    schema_ptr table_schema = make_simple_test_schema();
    auto [db, db_data] = make_data_dictionary_database(table_schema);

    expression int_const = make_int_const(1234);
    expression prepared_int_const =
        prepare_expression(int_const, db, "test_ks", table_schema.get(), make_receiver(int32_type));

    BOOST_REQUIRE_EQUAL(int_const, prepared_int_const);

    // Preparing an int32 with int64 receiver fails
    BOOST_REQUIRE_THROW(prepare_expression(int_const, db, "test_ks", table_schema.get(), make_receiver(long_type)),
                        exceptions::invalid_request_exception);

    // Preparing an int32 with text receiver fails
    BOOST_REQUIRE_THROW(prepare_expression(int_const, db, "test_ks", table_schema.get(), make_receiver(utf8_type)),
                        exceptions::invalid_request_exception);

    // Preparing an int32 with blob receiver works - ints can be implicitly converted to blobs
    expression prepared_int_blob =
        prepare_expression(int_const, db, "test_ks", table_schema.get(), make_receiver(bytes_type));
    expression expected_blob = constant(make_int_const(1234).value, bytes_type);

    BOOST_REQUIRE_EQUAL(prepared_int_blob, expected_blob);
}

BOOST_AUTO_TEST_CASE(prepare_writetime_ttl) {
    schema_ptr table_schema = make_simple_test_schema();
    auto [db, db_data] = make_data_dictionary_database(table_schema);

    auto prep = [&, db = db] (const expression& e, std::optional<data_type> receiver_type = std::nullopt) {
        auto receiver = receiver_type
                ? make_lw_shared<column_specification>("foo", "bar", make_shared<column_identifier>("ci", false), *receiver_type)
                : nullptr;
        return prepare_expression(e, db, "test_ks", table_schema.get(), receiver);
    };

    for (auto kind : {column_mutation_attribute::attribute_kind::ttl, column_mutation_attribute::attribute_kind::writetime}) {
        auto expected_type = kind == column_mutation_attribute::attribute_kind::ttl ? int32_type : long_type;

        auto ok1 = prep(column_mutation_attribute{kind, make_column("r")});
        BOOST_REQUIRE_EQUAL(ok1, (column_mutation_attribute{kind, column_value{&table_schema->regular_column_at(0)}}));
        BOOST_REQUIRE(type_of(ok1) == expected_type);

        // now try with a receiver
        auto ok2 = prep(column_mutation_attribute{kind, make_column("r")}, expected_type);
        BOOST_REQUIRE_EQUAL(ok1, (column_mutation_attribute{kind, column_value{&table_schema->regular_column_at(0)}}));
        BOOST_REQUIRE(type_of(ok1) == expected_type);

        // now try with a receiver of the wrong type
        BOOST_REQUIRE_THROW(prep(column_mutation_attribute{kind, make_column("r")}, utf8_type), exceptions::invalid_request_exception);

        // Try a partition key component
        BOOST_REQUIRE_THROW(prep(column_mutation_attribute{kind, make_column("p")}), exceptions::invalid_request_exception);
        BOOST_REQUIRE_THROW(prep(column_mutation_attribute{kind, make_column("c")}), exceptions::invalid_request_exception);

        // Try something that isn't a column_value
        BOOST_REQUIRE_THROW(prep(column_mutation_attribute{kind, bind_variable{}}), exceptions::invalid_request_exception);
    }
}


// Test how evaluating a given binary operator behaves when null is present.
// A binary with null on either side should evaluate to null.
static void test_evaluate_binop_null(oper_t op, expression valid_lhs, expression valid_rhs) {
    constant lhs_null_val = constant::make_null(type_of(valid_lhs));
    constant rhs_null_val = constant::make_null(type_of(valid_rhs));

    expression valid_binop = binary_operator(valid_lhs, op, valid_rhs);
    BOOST_REQUIRE(evaluate(valid_binop, evaluation_inputs{}).is_value());

    expression binop_lhs_null = binary_operator(lhs_null_val, op, valid_rhs);
    BOOST_REQUIRE_EQUAL(evaluate(binop_lhs_null, evaluation_inputs{}), raw_value::make_null());

    expression binop_rhs_null = binary_operator(valid_lhs, op, rhs_null_val);
    BOOST_REQUIRE_EQUAL(evaluate(binop_rhs_null, evaluation_inputs{}), raw_value::make_null());

    expression binop_both_null = binary_operator(lhs_null_val, op, rhs_null_val);
    BOOST_REQUIRE_EQUAL(evaluate(binop_both_null, evaluation_inputs{}), raw_value::make_null());
}

BOOST_AUTO_TEST_CASE(evaluate_binary_operator_eq) {
    expression true_eq_binop = binary_operator(make_int_const(1), oper_t::EQ, make_int_const(1));
    BOOST_REQUIRE_EQUAL(evaluate(true_eq_binop, evaluation_inputs{}), make_bool_raw(true));

    expression false_eq_binop = binary_operator(make_int_const(1), oper_t::EQ, make_int_const(2));
    BOOST_REQUIRE_EQUAL(evaluate(false_eq_binop, evaluation_inputs{}), make_bool_raw(false));

    expression empty_eq = binary_operator(make_empty_const(int32_type), oper_t::EQ, make_empty_const(int32_type));
    BOOST_REQUIRE_EQUAL(evaluate(empty_eq, evaluation_inputs{}), make_bool_raw(true));

    expression empty_neq = binary_operator(make_int_const(0), oper_t::EQ, make_empty_const(int32_type));
    BOOST_REQUIRE_EQUAL(evaluate(empty_neq, evaluation_inputs{}), make_bool_raw(false));

    test_evaluate_binop_null(oper_t::EQ, make_int_const(123), make_int_const(456));
}

BOOST_AUTO_TEST_CASE(evaluate_binary_operator_neq) {
    expression true_neq_binop = binary_operator(make_int_const(1), oper_t::NEQ, make_int_const(1000));
    BOOST_REQUIRE_EQUAL(evaluate(true_neq_binop, evaluation_inputs{}), make_bool_raw(true));

    expression false_neq_binop = binary_operator(make_int_const(2), oper_t::NEQ, make_int_const(2));
    BOOST_REQUIRE_EQUAL(evaluate(false_neq_binop, evaluation_inputs{}), make_bool_raw(false));

    expression empty_neq_empty =
        binary_operator(make_empty_const(int32_type), oper_t::NEQ, make_empty_const(int32_type));
    BOOST_REQUIRE_EQUAL(evaluate(empty_neq_empty, evaluation_inputs{}), make_bool_raw(false));

    expression empty_neq_0 = binary_operator(make_empty_const(int32_type), oper_t::NEQ, make_int_const(0));
    BOOST_REQUIRE_EQUAL(evaluate(empty_neq_0, evaluation_inputs{}), make_bool_raw(true));

    test_evaluate_binop_null(oper_t::NEQ, make_int_const(123), make_int_const(456));
}

static
binary_operator
lwt_binary_operator(expression lhs, oper_t op, expression rhs) {
    auto ret = binary_operator(std::move(lhs), op, std::move(rhs));
    ret.null_handling = null_handling_style::lwt_nulls;
    return ret;
}

BOOST_AUTO_TEST_CASE(evaluate_binary_operator_eq_neq_lwt_nulls) {
    auto one = make_int_const(1);
    auto two = make_int_const(2);
    auto null = constant::make_null(int32_type);
    auto t = make_bool_raw(true);
    auto f = make_bool_raw(false);
    auto eval = [] (const expression& e) { return evaluate(e, evaluation_inputs{}); };

    BOOST_REQUIRE_EQUAL(eval(lwt_binary_operator(one, oper_t::EQ, two)), f);
    BOOST_REQUIRE_EQUAL(eval(lwt_binary_operator(one, oper_t::EQ, one)), t);
    BOOST_REQUIRE_EQUAL(eval(lwt_binary_operator(one, oper_t::EQ, null)), f);
    BOOST_REQUIRE_EQUAL(eval(lwt_binary_operator(null, oper_t::EQ, two)), f);
    BOOST_REQUIRE_EQUAL(eval(lwt_binary_operator(null, oper_t::EQ, null)), t);

    BOOST_REQUIRE_EQUAL(eval(lwt_binary_operator(one, oper_t::NEQ, two)), t);
    BOOST_REQUIRE_EQUAL(eval(lwt_binary_operator(one, oper_t::NEQ, one)), f);
    BOOST_REQUIRE_EQUAL(eval(lwt_binary_operator(one, oper_t::NEQ, null)), t);
    BOOST_REQUIRE_EQUAL(eval(lwt_binary_operator(null, oper_t::NEQ, two)), t);
    BOOST_REQUIRE_EQUAL(eval(lwt_binary_operator(null, oper_t::NEQ, null)), f);
}

BOOST_AUTO_TEST_CASE(evaluate_binary_operator_in_lwt_nulls) {
    auto int32_list_type = list_type_impl::get_instance(int32_type, true); 
    auto one = make_int_const(1);
    auto two = make_int_const(2);
    auto null = constant::make_null(int32_type);
    auto list_with_null = collection_constructor{
        .style = collection_constructor::style_type::list,
        .elements = {one, null},
        .type = int32_list_type,
    };
    auto list_without_null = collection_constructor{
        .style = collection_constructor::style_type::list,
        .elements = {one},
        .type = int32_list_type,
    };
    auto null_list = constant::make_null(int32_list_type);
    auto t = make_bool_raw(true);
    auto f = make_bool_raw(false);
    auto eval = [] (const expression& e) { return evaluate(e, evaluation_inputs{}); };

    BOOST_REQUIRE_EQUAL(eval(lwt_binary_operator(one, oper_t::IN, list_with_null)), t);
    BOOST_REQUIRE_EQUAL(eval(lwt_binary_operator(two, oper_t::IN, list_with_null)), f);
    BOOST_REQUIRE_EQUAL(eval(lwt_binary_operator(null, oper_t::IN, list_with_null)), t);

    BOOST_REQUIRE_EQUAL(eval(lwt_binary_operator(one, oper_t::IN, list_without_null)), t);
    BOOST_REQUIRE_EQUAL(eval(lwt_binary_operator(two, oper_t::IN, list_without_null)), f);
    BOOST_REQUIRE_EQUAL(eval(lwt_binary_operator(null, oper_t::IN, list_without_null)), f);

    BOOST_REQUIRE_EQUAL(eval(lwt_binary_operator(one, oper_t::IN, null_list)), f);
    BOOST_REQUIRE_EQUAL(eval(lwt_binary_operator(two, oper_t::IN, null_list)), f);
    BOOST_REQUIRE_EQUAL(eval(lwt_binary_operator(null, oper_t::IN, null_list)), f);
}

BOOST_AUTO_TEST_CASE(evaluate_binary_operator_lt) {
    expression true_lt_binop = binary_operator(make_int_const(1), oper_t::LT, make_int_const(2));
    BOOST_REQUIRE_EQUAL(evaluate(true_lt_binop, evaluation_inputs{}), make_bool_raw(true));

    expression false_lt_binop = binary_operator(make_int_const(10), oper_t::LT, make_int_const(2));
    BOOST_REQUIRE_EQUAL(evaluate(false_lt_binop, evaluation_inputs{}), make_bool_raw(false));

    expression false_lt_binop2 = binary_operator(make_int_const(2), oper_t::LT, make_int_const(2));
    BOOST_REQUIRE_EQUAL(evaluate(false_lt_binop2, evaluation_inputs{}), make_bool_raw(false));

    expression empty_lt_empty = binary_operator(make_empty_const(int32_type), oper_t::LT, make_empty_const(int32_type));
    BOOST_REQUIRE_EQUAL(evaluate(empty_lt_empty, evaluation_inputs{}), make_bool_raw(false));

    expression empty_lt_int_min =
        binary_operator(make_empty_const(int32_type), oper_t::LT, make_int_const(std::numeric_limits<int32_t>::min()));
    BOOST_REQUIRE_EQUAL(evaluate(empty_lt_int_min, evaluation_inputs{}), make_bool_raw(true));

    test_evaluate_binop_null(oper_t::LT, make_int_const(123), make_int_const(456));
}

BOOST_AUTO_TEST_CASE(evaluate_binary_operator_lte) {
    expression true_lte_binop = binary_operator(make_int_const(1), oper_t::LTE, make_int_const(2));
    BOOST_REQUIRE_EQUAL(evaluate(true_lte_binop, evaluation_inputs{}), make_bool_raw(true));

    expression true_lte_binop2 = binary_operator(make_int_const(12), oper_t::LTE, make_int_const(12));
    BOOST_REQUIRE_EQUAL(evaluate(true_lte_binop2, evaluation_inputs{}), make_bool_raw(true));

    expression false_lte_binop = binary_operator(make_int_const(123), oper_t::LTE, make_int_const(2));
    BOOST_REQUIRE_EQUAL(evaluate(false_lte_binop, evaluation_inputs{}), make_bool_raw(false));

    expression empty_lte_empty =
        binary_operator(make_empty_const(int32_type), oper_t::LTE, make_empty_const(int32_type));
    BOOST_REQUIRE_EQUAL(evaluate(empty_lte_empty, evaluation_inputs{}), make_bool_raw(true));

    expression empty_lte_int_min =
        binary_operator(make_empty_const(int32_type), oper_t::LT, make_int_const(std::numeric_limits<int32_t>::min()));
    BOOST_REQUIRE_EQUAL(evaluate(empty_lte_int_min, evaluation_inputs{}), make_bool_raw(true));

    test_evaluate_binop_null(oper_t::LTE, make_int_const(123), make_int_const(456));
}

BOOST_AUTO_TEST_CASE(evaluate_binary_operator_gt) {
    expression true_gt_binop = binary_operator(make_int_const(2), oper_t::GT, make_int_const(1));
    BOOST_REQUIRE_EQUAL(evaluate(true_gt_binop, evaluation_inputs{}), make_bool_raw(true));

    expression false_gt_binop = binary_operator(make_int_const(1), oper_t::GT, make_int_const(2));
    BOOST_REQUIRE_EQUAL(evaluate(false_gt_binop, evaluation_inputs{}), make_bool_raw(false));

    expression false_gt_binop2 = binary_operator(make_int_const(2), oper_t::GT, make_int_const(2));
    BOOST_REQUIRE_EQUAL(evaluate(false_gt_binop2, evaluation_inputs{}), make_bool_raw(false));

    expression empty_gt_empty = binary_operator(make_empty_const(int32_type), oper_t::GT, make_empty_const(int32_type));
    BOOST_REQUIRE_EQUAL(evaluate(empty_gt_empty, evaluation_inputs{}), make_bool_raw(false));

    expression int_min_gt_empty =
        binary_operator(make_int_const(std::numeric_limits<int32_t>::min()), oper_t::GT, make_empty_const(int32_type));
    BOOST_REQUIRE_EQUAL(evaluate(int_min_gt_empty, evaluation_inputs{}), make_bool_raw(true));

    test_evaluate_binop_null(oper_t::GT, make_int_const(234), make_int_const(-3434));
}

BOOST_AUTO_TEST_CASE(evaluate_binary_operator_gte) {
    expression true_gte_binop = binary_operator(make_int_const(20), oper_t::GTE, make_int_const(10));
    BOOST_REQUIRE_EQUAL(evaluate(true_gte_binop, evaluation_inputs{}), make_bool_raw(true));

    expression true_gte_binop2 = binary_operator(make_int_const(10), oper_t::GTE, make_int_const(10));
    BOOST_REQUIRE_EQUAL(evaluate(true_gte_binop2, evaluation_inputs{}), make_bool_raw(true));

    expression false_gte_binop = binary_operator(make_int_const(-10), oper_t::GTE, make_int_const(10));
    BOOST_REQUIRE_EQUAL(evaluate(false_gte_binop, evaluation_inputs{}), make_bool_raw(false));

    expression empty_gte_empty =
        binary_operator(make_empty_const(int32_type), oper_t::GTE, make_empty_const(int32_type));
    BOOST_REQUIRE_EQUAL(evaluate(empty_gte_empty, evaluation_inputs{}), make_bool_raw(true));

    expression int_min_gte_empty =
        binary_operator(make_int_const(std::numeric_limits<int32_t>::min()), oper_t::GTE, make_empty_const(int32_type));
    BOOST_REQUIRE_EQUAL(evaluate(int_min_gte_empty, evaluation_inputs{}), make_bool_raw(true));

    test_evaluate_binop_null(oper_t::GTE, make_int_const(234), make_int_const(-3434));
}

// helper to evaluate expressions that we expect to throw sometimes. std::nullopt -> threw
static
std::optional<raw_value>
evaluate_maybe_exception(const expression& e, const evaluation_inputs& inputs) {
    try {
        return evaluate(e, evaluation_inputs{});
    } catch (exceptions::invalid_request_exception&) {
        return std::nullopt;
    }
}

// LWT inequality has different results if the second operand is NULL
BOOST_AUTO_TEST_CASE(evalute_lwt_inequality) {
    const auto operand_pool = std::vector({make_int_const(2), make_int_const(3), constant::make_null(int32_type)});
    for (auto op : { oper_t::LT, oper_t::LTE, oper_t::GT, oper_t::GTE}) {
        for (auto& lhs : operand_pool) {
            for (auto& rhs: operand_pool) {
                auto binop = lwt_binary_operator(lhs, op, rhs);
                auto result = evaluate_maybe_exception(binop, evaluation_inputs{});
                if (rhs.is_null()) {
                    BOOST_REQUIRE(!result); // NULL rhs is illegal for lwt
                } else {
                    // Otherwise, results should be the same as for non-LWT, except FALSE is returned
                    // instead of NULL
                    binop.null_handling = null_handling_style::sql;
                    auto sql_result = evaluate_maybe_exception(binop, evaluation_inputs{});
                    if (sql_result && sql_result->is_null()) {
                        sql_result = cql3::raw_value::make_value(data_value(false).serialize_nonnull());
                    }
                    BOOST_REQUIRE_EQUAL(result, sql_result);
                }
            }
        }
    }
}

BOOST_AUTO_TEST_CASE(evaluate_binary_operator_in) {
    // IN expects a list as its rhs, sets are not allowed
    expression in_list = make_int_list_const({1, 3, 5});

    expression true_in_binop = binary_operator(make_int_const(3), oper_t::IN, in_list);
    BOOST_REQUIRE_EQUAL(evaluate(true_in_binop, evaluation_inputs{}), make_bool_raw(true));

    expression false_in_binop = binary_operator(make_int_const(2), oper_t::IN, in_list);
    BOOST_REQUIRE_EQUAL(evaluate(false_in_binop, evaluation_inputs{}), make_bool_raw(false));

    expression empty_in_list = binary_operator(make_empty_const(int32_type), oper_t::IN, in_list);
    BOOST_REQUIRE_EQUAL(evaluate(empty_in_list, evaluation_inputs{}), make_bool_raw(false));

    expression list_with_empty =
        make_list_const({make_int_const(1), make_empty_const(int32_type), make_int_const(3)}, int32_type);
    expression empty_in_list_with_empty = binary_operator(make_empty_const(int32_type), oper_t::IN, list_with_empty);
    BOOST_REQUIRE_EQUAL(evaluate(empty_in_list_with_empty, evaluation_inputs{}), make_bool_raw(true));

    expression existing_int_in_list_with_empty = binary_operator(make_int_const(3), oper_t::IN, list_with_empty);
    BOOST_REQUIRE_EQUAL(evaluate(existing_int_in_list_with_empty, evaluation_inputs{}), make_bool_raw(true));

    expression nonexisting_int_in_list_with_empty = binary_operator(make_int_const(321), oper_t::IN, list_with_empty);
    BOOST_REQUIRE_EQUAL(evaluate(nonexisting_int_in_list_with_empty, evaluation_inputs{}), make_bool_raw(false));

    test_evaluate_binop_null(oper_t::IN, make_int_const(5), in_list);
}

// Tests `<int_value> IN (123, ?, 789)` where the bind variable has value 456
BOOST_AUTO_TEST_CASE(evaluate_binary_operator_in_list_with_bind_variable) {
    schema_ptr table_schema =
        schema_builder("test_ks", "test_cf").with_column("pk", int32_type, column_kind::partition_key).build();

    expression in_list = collection_constructor{
        .style = collection_constructor::style_type::list,
        .elements = {make_int_const(123), bind_variable{.bind_index = 0, .receiver = make_receiver(int32_type)},
                     make_int_const(789)},
        .type = list_type_impl::get_instance(int32_type, true)};

    auto [inputs, inputs_data] = make_evaluation_inputs(table_schema, {{"pk", make_int_raw(111)}}, {make_int_raw(456)});

    expression true_in_binop = binary_operator(make_int_const(456), oper_t::IN, in_list);
    BOOST_REQUIRE_EQUAL(evaluate(true_in_binop, inputs), make_bool_raw(true));

    expression false_in_binop = binary_operator(make_int_const(-100), oper_t::IN, in_list);
    BOOST_REQUIRE_EQUAL(evaluate(false_in_binop, inputs), make_bool_raw(false));

    expression empty_in_list = binary_operator(make_empty_const(int32_type), oper_t::IN, in_list);
    BOOST_REQUIRE_EQUAL(evaluate(empty_in_list, inputs), make_bool_raw(false));
}

BOOST_AUTO_TEST_CASE(evaluate_binary_operator_list_contains) {
    expression list_val = make_int_list_const({1, 3, 5});

    expression list_contains_true = binary_operator(list_val, oper_t::CONTAINS, make_int_const(3));
    BOOST_REQUIRE_EQUAL(evaluate(list_contains_true, evaluation_inputs{}), make_bool_raw(true));

    expression list_contains_false = binary_operator(list_val, oper_t::CONTAINS, make_int_const(2));
    BOOST_REQUIRE_EQUAL(evaluate(list_contains_false, evaluation_inputs{}), make_bool_raw(false));

    expression list_contains_empty = binary_operator(list_val, oper_t::CONTAINS, make_empty_const(int32_type));
    BOOST_REQUIRE_EQUAL(evaluate(list_contains_empty, evaluation_inputs{}), make_bool_raw(false));

    expression list_with_empty =
        make_list_const({make_int_const(1), make_empty_const(int32_type), make_int_const(3)}, int32_type);
    expression list_with_empty_contains_empty =
        binary_operator(list_with_empty, oper_t::CONTAINS, make_empty_const(int32_type));
    BOOST_REQUIRE_EQUAL(evaluate(list_with_empty_contains_empty, evaluation_inputs{}), make_bool_raw(true));

    expression list_with_empty_contains_existing_int =
        binary_operator(list_with_empty, oper_t::CONTAINS, make_int_const(3));
    BOOST_REQUIRE_EQUAL(evaluate(list_with_empty_contains_existing_int, evaluation_inputs{}), make_bool_raw(true));

    expression list_with_empty_contains_nonexisting_int =
        binary_operator(list_with_empty, oper_t::CONTAINS, make_int_const(321));
    BOOST_REQUIRE_EQUAL(evaluate(list_with_empty_contains_nonexisting_int, evaluation_inputs{}), make_bool_raw(false));

    test_evaluate_binop_null(oper_t::CONTAINS, list_val, make_int_const(5));
}

BOOST_AUTO_TEST_CASE(evaluate_binary_operator_set_contains) {
    expression set_val = make_int_set_const({1, 3, 5});

    expression set_contains_true = binary_operator(set_val, oper_t::CONTAINS, make_int_const(3));
    BOOST_REQUIRE_EQUAL(evaluate(set_contains_true, evaluation_inputs{}), make_bool_raw(true));

    expression set_contains_false = binary_operator(set_val, oper_t::CONTAINS, make_int_const(2));
    BOOST_REQUIRE_EQUAL(evaluate(set_contains_false, evaluation_inputs{}), make_bool_raw(false));

    expression set_contains_empty = binary_operator(set_val, oper_t::CONTAINS, make_empty_const(int32_type));
    BOOST_REQUIRE_EQUAL(evaluate(set_contains_empty, evaluation_inputs{}), make_bool_raw(false));

    expression set_with_empty =
        make_set_const({make_empty_const(int32_type), make_int_const(2), make_int_const(3)}, int32_type);
    expression set_with_empty_contains_empty =
        binary_operator(set_with_empty, oper_t::CONTAINS, make_empty_const(int32_type));
    BOOST_REQUIRE_EQUAL(evaluate(set_with_empty_contains_empty, evaluation_inputs{}), make_bool_raw(true));

    expression set_with_empty_contains_existing_int =
        binary_operator(set_with_empty, oper_t::CONTAINS, make_int_const(3));
    BOOST_REQUIRE_EQUAL(evaluate(set_with_empty_contains_existing_int, evaluation_inputs{}), make_bool_raw(true));

    expression set_with_empty_contains_nonexisting_int =
        binary_operator(set_with_empty, oper_t::CONTAINS, make_int_const(321));
    BOOST_REQUIRE_EQUAL(evaluate(set_with_empty_contains_nonexisting_int, evaluation_inputs{}), make_bool_raw(false));

    test_evaluate_binop_null(oper_t::CONTAINS, set_val, make_int_const(5));
}

BOOST_AUTO_TEST_CASE(evaluate_binary_operator_map_contains) {
    expression map_val = make_int_int_map_const({{1, 2}, {3, 4}, {5, 6}});

    expression map_contains_true = binary_operator(map_val, oper_t::CONTAINS, make_int_const(4));
    BOOST_REQUIRE_EQUAL(evaluate(map_contains_true, evaluation_inputs{}), make_bool_raw(true));

    expression map_contains_false = binary_operator(map_val, oper_t::CONTAINS, make_int_const(3));
    BOOST_REQUIRE_EQUAL(evaluate(map_contains_false, evaluation_inputs{}), make_bool_raw(false));

    expression map_contains_empty = binary_operator(map_val, oper_t::CONTAINS, make_empty_const(int32_type));
    BOOST_REQUIRE_EQUAL(evaluate(map_contains_empty, evaluation_inputs{}), make_bool_raw(false));

    expression map_with_empty =
        make_map_const({{make_int_const(1), make_empty_const(int32_type)}, {make_int_const(3), make_int_const(4)}},
                       int32_type, int32_type);
    expression map_with_empty_contains_empty =
        binary_operator(map_with_empty, oper_t::CONTAINS, make_empty_const(int32_type));
    BOOST_REQUIRE_EQUAL(evaluate(map_with_empty_contains_empty, evaluation_inputs{}), make_bool_raw(true));

    expression map_with_empty_contains_existing_int =
        binary_operator(map_with_empty, oper_t::CONTAINS, make_int_const(4));
    BOOST_REQUIRE_EQUAL(evaluate(map_with_empty_contains_existing_int, evaluation_inputs{}), make_bool_raw(true));

    expression map_with_empty_contains_nonexisting_int =
        binary_operator(map_with_empty, oper_t::CONTAINS, make_int_const(3));
    BOOST_REQUIRE_EQUAL(evaluate(map_with_empty_contains_nonexisting_int, evaluation_inputs{}), make_bool_raw(false));

    test_evaluate_binop_null(oper_t::CONTAINS, map_val, make_int_const(5));
}

BOOST_AUTO_TEST_CASE(evaluate_binary_operator_map_contains_key) {
    expression map_val = make_int_int_map_const({{1, 2}, {3, 4}, {5, 6}});

    expression true_contains_key_binop = binary_operator(map_val, oper_t::CONTAINS_KEY, make_int_const(5));
    BOOST_REQUIRE_EQUAL(evaluate(true_contains_key_binop, evaluation_inputs{}), make_bool_raw(true));

    expression false_contains_key_binop = binary_operator(map_val, oper_t::CONTAINS_KEY, make_int_const(6));
    BOOST_REQUIRE_EQUAL(evaluate(false_contains_key_binop, evaluation_inputs{}), make_bool_raw(false));

    expression map_contains_key_empty = binary_operator(map_val, oper_t::CONTAINS_KEY, make_empty_const(int32_type));
    BOOST_REQUIRE_EQUAL(evaluate(map_contains_key_empty, evaluation_inputs{}), make_bool_raw(false));

    expression map_with_empty =
        make_map_const({{make_empty_const(int32_type), make_int_const(2)}, {make_int_const(3), make_int_const(4)}},
                       int32_type, int32_type);
    expression map_with_empty_contains_key_empty =
        binary_operator(map_with_empty, oper_t::CONTAINS_KEY, make_empty_const(int32_type));
    BOOST_REQUIRE_EQUAL(evaluate(map_with_empty_contains_key_empty, evaluation_inputs{}), make_bool_raw(true));

    expression map_with_empty_contains_key_existing_int =
        binary_operator(map_with_empty, oper_t::CONTAINS_KEY, make_int_const(3));
    BOOST_REQUIRE_EQUAL(evaluate(map_with_empty_contains_key_existing_int, evaluation_inputs{}), make_bool_raw(true));

    expression map_with_empty_contains_key_nonexisting_int =
        binary_operator(map_with_empty, oper_t::CONTAINS_KEY, make_int_const(4));
    BOOST_REQUIRE_EQUAL(evaluate(map_with_empty_contains_key_nonexisting_int, evaluation_inputs{}),
                        make_bool_raw(false));

    test_evaluate_binop_null(oper_t::CONTAINS_KEY, map_val, make_int_const(5));
}

BOOST_AUTO_TEST_CASE(evaluate_binary_operator_is_not) {
    expression true_is_not_binop = binary_operator(make_int_const(1), oper_t::IS_NOT, constant::make_null(int32_type));
    BOOST_REQUIRE_EQUAL(evaluate(true_is_not_binop, evaluation_inputs{}), make_bool_raw(true));

    expression false_is_not_binop =
        binary_operator(constant::make_null(int32_type), oper_t::IS_NOT, constant::make_null(int32_type));
    BOOST_REQUIRE_EQUAL(evaluate(false_is_not_binop, evaluation_inputs{}), make_bool_raw(false));

    expression forbidden_is_not_binop = binary_operator(make_int_const(1), oper_t::IS_NOT, make_int_const(2));
    BOOST_REQUIRE_THROW(evaluate(forbidden_is_not_binop, evaluation_inputs{}), exceptions::invalid_request_exception);

    expression empty_is_not_null =
        binary_operator(make_empty_const(int32_type), oper_t::IS_NOT, constant::make_null(int32_type));
    BOOST_REQUIRE_EQUAL(evaluate(empty_is_not_null, evaluation_inputs{}), make_bool_raw(true));
}

BOOST_AUTO_TEST_CASE(evaluate_binary_operator_like) {
    expression true_like_binop = binary_operator(make_text_const("some_text"), oper_t::LIKE, make_text_const("some_%"));
    BOOST_REQUIRE_EQUAL(evaluate(true_like_binop, evaluation_inputs{}), make_bool_raw(true));

    expression false_like_binop =
        binary_operator(make_text_const("some_text"), oper_t::LIKE, make_text_const("some_other_%"));
    BOOST_REQUIRE_EQUAL(evaluate(false_like_binop, evaluation_inputs{}), make_bool_raw(false));

    // Binary representation of an empty value is the same as empty string
    BOOST_REQUIRE_EQUAL(make_text_raw(""), make_empty_raw());

    expression empty_like_text = binary_operator(make_empty_const(utf8_type), oper_t::LIKE, make_text_const("%"));
    BOOST_REQUIRE_EQUAL(evaluate(empty_like_text, evaluation_inputs{}), make_bool_raw(true));

    expression text_like_empty = binary_operator(make_text_const(""), oper_t::LIKE, make_empty_const(utf8_type));
    BOOST_REQUIRE_EQUAL(evaluate(text_like_empty, evaluation_inputs{}), make_bool_raw(true));

    expression empty_like_empty =
        binary_operator(make_empty_const(utf8_type), oper_t::LIKE, make_empty_const(utf8_type));
    BOOST_REQUIRE_EQUAL(evaluate(empty_like_empty, evaluation_inputs{}), make_bool_raw(true));

    test_evaluate_binop_null(oper_t::LIKE, make_text_const("some_text"), make_text_const("some_%"));
}

// An empty conjunction should evaluate to true
BOOST_AUTO_TEST_CASE(evaluate_conjunction_empty) {
    expression empty_conj = conjunction{.children = {}};

    BOOST_REQUIRE_EQUAL(evaluate(empty_conj, evaluation_inputs{}), make_bool_raw(true));
}

// A conjunction with one false value should evaluate to false
BOOST_AUTO_TEST_CASE(evaluate_conjunction_one_false) {
    expression conj_one_false = conjunction{.children = {make_bool_const(false)}};

    BOOST_REQUIRE_EQUAL(evaluate(conj_one_false, evaluation_inputs{}), make_bool_raw(false));
}

// A conjunction with one true value should evaluate to true
BOOST_AUTO_TEST_CASE(evaluate_conjunction_one_true) {
    expression conj_one_true = conjunction{.children = {make_bool_const(true)}};

    BOOST_REQUIRE_EQUAL(evaluate(conj_one_true, evaluation_inputs{}), make_bool_raw(true));
}

// Helper function that evaluates a conjunction with given elements
raw_value eval_conj(std::vector<raw_value> elements) {
    std::vector<expression> conj_children;
    for (const raw_value& element : elements) {
        conj_children.push_back(constant(element, boolean_type));
    }

    return evaluate(conjunction{.children = conj_children}, evaluation_inputs{});
};

// Evaluate all possible two-element conjunctions containing true, false and null.
BOOST_AUTO_TEST_CASE(evaluate_conjunction_two_elements) {
    raw_value true_val = make_bool_raw(true);
    raw_value false_val = make_bool_raw(false);
    raw_value null_val = raw_value::make_null();

    BOOST_REQUIRE_EQUAL(eval_conj({true_val, true_val}), true_val);
    BOOST_REQUIRE_EQUAL(eval_conj({true_val, false_val}), false_val);
    BOOST_REQUIRE_EQUAL(eval_conj({true_val, null_val}), null_val);

    BOOST_REQUIRE_EQUAL(eval_conj({false_val, true_val}), false_val);
    BOOST_REQUIRE_EQUAL(eval_conj({false_val, false_val}), false_val);
    BOOST_REQUIRE_EQUAL(eval_conj({false_val, null_val}), false_val);

    BOOST_REQUIRE_EQUAL(eval_conj({null_val, true_val}), null_val);
    BOOST_REQUIRE_EQUAL(eval_conj({null_val, false_val}), false_val);
    BOOST_REQUIRE_EQUAL(eval_conj({null_val, null_val}), null_val);
}

// Evaluate all possible three-element conjunctions containing true, false and null.
BOOST_AUTO_TEST_CASE(evaluate_conjunction_three_elements) {
    raw_value true_val = make_bool_raw(true);
    raw_value false_val = make_bool_raw(false);
    raw_value null_val = raw_value::make_null();

    BOOST_REQUIRE_EQUAL(eval_conj({true_val, true_val, true_val}), true_val);
    BOOST_REQUIRE_EQUAL(eval_conj({true_val, true_val, false_val}), false_val);
    BOOST_REQUIRE_EQUAL(eval_conj({true_val, true_val, null_val}), null_val);
    BOOST_REQUIRE_EQUAL(eval_conj({true_val, false_val, true_val}), false_val);
    BOOST_REQUIRE_EQUAL(eval_conj({true_val, false_val, false_val}), false_val);
    BOOST_REQUIRE_EQUAL(eval_conj({true_val, false_val, null_val}), false_val);
    BOOST_REQUIRE_EQUAL(eval_conj({true_val, null_val, true_val}), null_val);
    BOOST_REQUIRE_EQUAL(eval_conj({true_val, null_val, false_val}), false_val);
    BOOST_REQUIRE_EQUAL(eval_conj({true_val, null_val, null_val}), null_val);

    BOOST_REQUIRE_EQUAL(eval_conj({false_val, true_val, true_val}), false_val);
    BOOST_REQUIRE_EQUAL(eval_conj({false_val, true_val, false_val}), false_val);
    BOOST_REQUIRE_EQUAL(eval_conj({false_val, true_val, null_val}), false_val);
    BOOST_REQUIRE_EQUAL(eval_conj({false_val, false_val, true_val}), false_val);
    BOOST_REQUIRE_EQUAL(eval_conj({false_val, false_val, false_val}), false_val);
    BOOST_REQUIRE_EQUAL(eval_conj({false_val, false_val, null_val}), false_val);
    BOOST_REQUIRE_EQUAL(eval_conj({false_val, null_val, true_val}), false_val);
    BOOST_REQUIRE_EQUAL(eval_conj({false_val, null_val, false_val}), false_val);
    BOOST_REQUIRE_EQUAL(eval_conj({false_val, null_val, null_val}), false_val);

    BOOST_REQUIRE_EQUAL(eval_conj({null_val, true_val, true_val}), null_val);
    BOOST_REQUIRE_EQUAL(eval_conj({null_val, true_val, false_val}), false_val);
    BOOST_REQUIRE_EQUAL(eval_conj({null_val, true_val, null_val}), null_val);
    BOOST_REQUIRE_EQUAL(eval_conj({null_val, false_val, true_val}), false_val);
    BOOST_REQUIRE_EQUAL(eval_conj({null_val, false_val, false_val}), false_val);
    BOOST_REQUIRE_EQUAL(eval_conj({null_val, false_val, null_val}), false_val);
    BOOST_REQUIRE_EQUAL(eval_conj({null_val, null_val, true_val}), null_val);
    BOOST_REQUIRE_EQUAL(eval_conj({null_val, null_val, false_val}), false_val);
    BOOST_REQUIRE_EQUAL(eval_conj({null_val, null_val, null_val}), null_val);
}

// `true AND true AND true AND ...' should evaluate to true
BOOST_AUTO_TEST_CASE(evaluate_conjunction_long_true) {
    expression conj_long_true = conjunction{
        .children = {make_bool_const(true), make_bool_const(true), make_bool_const(true), make_bool_const(true),
                     make_bool_const(true), make_bool_const(true), make_bool_const(true), make_bool_const(true),
                     make_bool_const(true), make_bool_const(true), make_bool_const(true), make_bool_const(true),
                     make_bool_const(true), make_bool_const(true), make_bool_const(true)}};

    BOOST_REQUIRE_EQUAL(evaluate(conj_long_true, evaluation_inputs{}), make_bool_raw(true));
}

// `true AND true AND false AND ...' should evaluate to false
BOOST_AUTO_TEST_CASE(evaluate_conjunction_long_mixed) {
    expression conj_long_mixed = conjunction{
        .children = {make_bool_const(true), make_bool_const(true), make_bool_const(false), make_bool_const(true),
                     make_bool_const(true), make_bool_const(true), make_bool_const(true), make_bool_const(true),
                     make_bool_const(false), make_bool_const(false), make_bool_const(true), make_bool_const(false),
                     make_bool_const(true), make_bool_const(true), make_bool_const(true)}};

    BOOST_REQUIRE_EQUAL(evaluate(conj_long_mixed, evaluation_inputs{}), make_bool_raw(false));
}

// A conjunction with one null value should evaluate to null
BOOST_AUTO_TEST_CASE(evaluate_conjunction_one_null) {
    expression conj_one_null = conjunction{.children = {constant::make_null(boolean_type)}};

    BOOST_REQUIRE_EQUAL(evaluate(conj_one_null, evaluation_inputs{}), raw_value::make_null());
}

// 'true AND true AND true AND null AND true AND ...' should evaluate to null
BOOST_AUTO_TEST_CASE(evaluate_conjunction_with_null) {
    expression conj_with_null =
        conjunction{.children = {make_bool_const(true), make_bool_const(true), make_bool_const(true),
                                 constant::make_null(boolean_type), make_bool_const(true), make_bool_const(true)}};

    BOOST_REQUIRE_EQUAL(evaluate(conj_with_null, evaluation_inputs{}), raw_value::make_null());
}

// Evaluating a conjunction that contains a single empty value should throw an error
BOOST_AUTO_TEST_CASE(evaluate_conjunction_one_empty) {
    expression conj_one_empty = conjunction{.children = {make_empty_const(boolean_type)}};

    BOOST_REQUIRE_THROW(evaluate(conj_one_empty, evaluation_inputs{}), exceptions::invalid_request_exception);
}

// Evaluating 'true AND true AND true AND EMPTY AND ...' should throw an error
BOOST_AUTO_TEST_CASE(evaluate_conjunction_with_empty) {
    expression conj_with_empty =
        conjunction{.children = {make_bool_const(true), make_bool_const(true), make_bool_const(true),
                                 make_empty_const(boolean_type), make_bool_const(false), make_bool_const(true)}};

    BOOST_REQUIRE_THROW(evaluate(conj_with_empty, evaluation_inputs{}), exceptions::invalid_request_exception);
}

static cql3::query_options query_options_with_unset_bind_variable() {
    return cql3::query_options(cql3::raw_value_vector_with_unset({cql3::raw_value::make_null()}, {true}));
}

// Short circuiting on false ignores all further values, even though they could make the expression invalid
BOOST_AUTO_TEST_CASE(evaluate_conjunction_short_circuit_on_false_does_not_detect_invalid_values) {
    auto qo = query_options_with_unset_bind_variable();
    auto inputs = evaluation_inputs{.options = &qo};

    // An expression which would throw an error when evaluated
    expression invalid_to_evaluate = conjunction{.children = {new_bind_variable(0)}};

    BOOST_REQUIRE_THROW(evaluate(invalid_to_evaluate, inputs), exceptions::invalid_request_exception);

    expression conj_with_false_then_invalid =
        conjunction{.children = {make_bool_const(true), make_bool_const(false), make_empty_const(boolean_type),
                                 new_bind_variable(0), invalid_to_evaluate, make_bool_const(true)}};

    BOOST_REQUIRE_EQUAL(evaluate(conj_with_false_then_invalid, inputs), make_bool_raw(false));
}

// Null doesn't short-circuit
BOOST_AUTO_TEST_CASE(evaluate_conjunction_doesnt_short_circuit_on_null) {
    auto qo = query_options_with_unset_bind_variable();
    auto inputs = evaluation_inputs{.options = &qo};

    // An expression which would throw an error when evaluated
    expression invalid_to_evaluate = conjunction{.children = {new_bind_variable(0)}};

    BOOST_REQUIRE_THROW(evaluate(invalid_to_evaluate, inputs), exceptions::invalid_request_exception);

    expression conj_with_null_then_invalid = conjunction{
        .children = {make_bool_const(true), constant::make_null(boolean_type), make_empty_const(boolean_type),
                     new_bind_variable(0), invalid_to_evaluate, make_bool_const(true)}};

    BOOST_REQUIRE_THROW(evaluate(conj_with_null_then_invalid, inputs),
                        exceptions::invalid_request_exception);
}

// '() AND (true AND true) AND (true AND true) AND (true)' evaluates to true.
// This test also ensures that evaluate(conjunction) calls evaluate
// for each of the conjunction's children.
BOOST_AUTO_TEST_CASE(evaluate_conjunction_of_conjunctions_to_true) {
    expression conj1 = conjunction{.children = {}};

    expression conj2 = conjunction{.children = {make_bool_const(true), make_bool_const(true)}};

    expression conj3 = conjunction{.children = {make_bool_const(true), make_bool_const(true)}};

    expression conj4 = conjunction{.children = {make_bool_const(true)}};

    expression conj_of_conjs = conjunction{.children = {conj1, conj2, conj3, conj4}};

    BOOST_REQUIRE_EQUAL(evaluate(conj_of_conjs, evaluation_inputs{}), make_bool_raw(true));
}

// '() AND (true AND true) AND (true AND false) AND (true)' evaluates to false
BOOST_AUTO_TEST_CASE(evaluate_conjunction_of_conjunctions_to_false) {
    expression conj1 = conjunction{.children = {}};

    expression conj2 = conjunction{.children = {make_bool_const(true), make_bool_const(true)}};

    expression conj3 = conjunction{.children = {make_bool_const(true), make_bool_const(false)}};

    expression conj4 = conjunction{.children = {make_bool_const(true)}};

    expression conj_of_conjs = conjunction{.children = {conj1, conj2, conj3, conj4}};

    BOOST_REQUIRE_EQUAL(evaluate(conj_of_conjs, evaluation_inputs{}), make_bool_raw(false));
}

// '() AND (true AND true) AND (true AND null) AND (false)' evaluates to false
BOOST_AUTO_TEST_CASE(evaluate_conjunction_of_conjunctions_to_null) {
    expression conj1 = conjunction{.children = {}};

    expression conj2 = conjunction{.children = {make_bool_const(true), make_bool_const(true)}};

    expression conj3 = conjunction{.children = {make_bool_const(true), constant::make_null(boolean_type)}};

    expression conj4 = conjunction{.children = {make_bool_const(false)}};

    expression conj_of_conjs = conjunction{.children = {conj1, conj2, conj3, conj4}};

    BOOST_REQUIRE_EQUAL(evaluate(conj_of_conjs, evaluation_inputs{}), make_bool_raw(false));
}

// Evaluating '() AND (true AND true) AND (true AND UNSET_VALUE) AND (false)' throws an error
BOOST_AUTO_TEST_CASE(evaluate_conjunction_of_conjunctions_with_invalid) {
    auto qo = query_options_with_unset_bind_variable();
    auto inputs = evaluation_inputs{.options = &qo};

    expression conj1 = conjunction{.children = {}};

    expression conj2 = conjunction{.children = {make_bool_const(true), new_bind_variable(0)}};

    expression conj3 = conjunction{.children = {make_bool_const(true), make_bool_const(true)}};

    expression conj4 = conjunction{.children = {make_bool_const(true)}};

    expression conj_of_conjs = conjunction{.children = {conj1, conj2, conj3, conj4}};

    BOOST_REQUIRE_THROW(evaluate(conj_of_conjs, inputs), exceptions::invalid_request_exception);
}

BOOST_AUTO_TEST_CASE(evaluate_field_selection) {
    auto schema = make_simple_test_schema();
    auto [db, db_data] = make_data_dictionary_database(schema);

    // The user defined type has 5 fields:
    // CREATE TYPE test_ks.my_type (
    //   int_field int,
    //   float_field float,
    //   text_field text,
    //   bigint_field bigint,
    //   int_field2 int,
    // )
    shared_ptr<const user_type_impl> udt_type = user_type_impl::get_instance(
        "test_ks", "my_type", {"int_field", "float_field", "text_field", "bigint_field", "int_field2"},
        {int32_type, float_type, utf8_type, long_type, int32_type}, true);

    // Create a UDT value:
    // {int_field: 123, float_field: null, text_field: 'abcdef', bigint_field: blobasbigint(0x), int_field2: 1337}
    usertype_constructor::elements_map_type udt_value_elements;
    udt_value_elements.emplace(column_identifier("int_field", false), make_int_const(123));
    udt_value_elements.emplace(column_identifier("float_field", false), constant::make_null(float_type));
    udt_value_elements.emplace(column_identifier("text_field", false), make_text_const("abcdef"));
    udt_value_elements.emplace(column_identifier("bigint_field", false), make_empty_const(long_type));
    udt_value_elements.emplace(column_identifier("int_field2", false), make_int_const(1337));
    expression udt_value =
        constant(evaluate(usertype_constructor{.elements = std::move(udt_value_elements), .type = udt_type},
                          evaluation_inputs{}),
                 udt_type);

    auto make_field_selection = [&](expression value, const char* selected_field, data_type field_type) -> expression {
        return field_selection{
            .structure = value, .field = make_shared<column_identifier_raw>(selected_field, true), .type = field_type};
    };


    auto prepare_and_evaluate = [&,db = db] (const expression& e, const evaluation_inputs& inputs) {
        auto prepared = prepare_expression(e, db, "", schema.get(), nullptr);
        return evaluate(prepared, inputs);
    };

    // Evaluate the fields, check that field values are correct
    BOOST_REQUIRE_EQUAL(prepare_and_evaluate(make_field_selection(udt_value, "int_field", int32_type), evaluation_inputs{}), make_int_raw(123));
    BOOST_REQUIRE_EQUAL(prepare_and_evaluate(make_field_selection(udt_value, "float_field", float_type), evaluation_inputs{}),
                        cql3::raw_value::make_null());
    BOOST_REQUIRE_EQUAL(prepare_and_evaluate(make_field_selection(udt_value, "text_field", utf8_type), evaluation_inputs{}),
                        make_text_raw("abcdef"));
    BOOST_REQUIRE_EQUAL(prepare_and_evaluate(make_field_selection(udt_value, "bigint_field", long_type), evaluation_inputs{}),
                        make_empty_raw());
    BOOST_REQUIRE_EQUAL(prepare_and_evaluate(make_field_selection(udt_value, "int_field2", int32_type), evaluation_inputs{}),
                        make_int_raw(1337));

    // Evaluate a nonexistent field, should throw an exception
    BOOST_REQUIRE_THROW(prepare_and_evaluate(make_field_selection(udt_value, "field_testing", int32_type), evaluation_inputs{}),
                        exceptions::invalid_request_exception);

    // Create a UDT value with values for the first 3 fields.
    // There's no value for the 4th and 5th field, so they should be NULL.
    // This is normal behavior, some fields might not have a value if the UDT value was serialized before
    // adding new fields to the type.
    // {int_field: 123, float_field: null, text_field: ''}
    expression short_udt_value =
        constant(make_tuple_raw({make_int_raw(123), cql3::raw_value::make_null(), make_text_raw("")}), udt_type);

    // Evaluate the first 3 fields, check that the value is correct
    BOOST_REQUIRE_EQUAL(prepare_and_evaluate(make_field_selection(short_udt_value, "int_field", int32_type), evaluation_inputs{}),
                        make_int_raw(123));
    BOOST_REQUIRE_EQUAL(prepare_and_evaluate(make_field_selection(short_udt_value, "float_field", float_type), evaluation_inputs{}),
                        cql3::raw_value::make_null());
    BOOST_REQUIRE_EQUAL(prepare_and_evaluate(make_field_selection(short_udt_value, "text_field", utf8_type), evaluation_inputs{}),
                        make_text_raw(""));

    // The serialized value doesn't contain any data for the 4th or 5th field, so they should be NULL.
    BOOST_REQUIRE_EQUAL(prepare_and_evaluate(make_field_selection(short_udt_value, "bigint_field", long_type), evaluation_inputs{}),
                        cql3::raw_value::make_null());
    BOOST_REQUIRE_EQUAL(prepare_and_evaluate(make_field_selection(short_udt_value, "int_field2", int32_type), evaluation_inputs{}),
                        cql3::raw_value::make_null());

    // Evaluate a nonexistent field, should throw an exception
    BOOST_REQUIRE_THROW(prepare_and_evaluate(make_field_selection(short_udt_value, "field_testing", int32_type), evaluation_inputs{}),
                        exceptions::invalid_request_exception);
}

BOOST_AUTO_TEST_CASE(evaluate_column_mutation_attribute) {
    auto s = make_simple_test_schema();
    auto ttls = std::array{ int32_t(1), int32_t(2) };
    auto timestamps = std::array{ int64_t(12345), int64_t(23456) };
    auto ttl_of_s = column_mutation_attribute{
        .kind = column_mutation_attribute::attribute_kind::ttl,
        .column = column_value(&s->static_column_at(0)),
    };
    auto writetime_of_s = column_mutation_attribute{
        .kind = column_mutation_attribute::attribute_kind::writetime,
        .column = column_value(&s->static_column_at(0)),
    };
    auto ttl_of_r = column_mutation_attribute{
        .kind = column_mutation_attribute::attribute_kind::ttl,
        .column = column_value(&s->regular_column_at(0)),
    };
    auto writetime_of_r = column_mutation_attribute{
        .kind = column_mutation_attribute::attribute_kind::writetime,
        .column = column_value(&s->regular_column_at(0)),
    };

    auto null = cql3::raw_value::make_null();

    auto [inputs1, inputs_data1] = make_evaluation_inputs(s, {
        {"pk", mutation_column_value{make_int_raw(3)}},
        {"ck", mutation_column_value{make_int_raw(4)}},
        {"s", mutation_column_value{make_int_raw(3), 12345, 7}},
        {"r", mutation_column_value{make_int_raw(3), 23456, 8}}});
    BOOST_REQUIRE_EQUAL(evaluate(ttl_of_s, inputs1), make_int_raw(7));
    BOOST_REQUIRE_EQUAL(evaluate(writetime_of_s, inputs1), make_bigint_raw(12345));
    BOOST_REQUIRE_EQUAL(evaluate(ttl_of_r, inputs1), make_int_raw(8));
    BOOST_REQUIRE_EQUAL(evaluate(writetime_of_r, inputs1), make_bigint_raw(23456));

    auto [inputs2, inputs_data2] = make_evaluation_inputs(s, {
        {"pk", make_int_raw(3)},
        {"ck", make_int_raw(4)},
        {"s", null},
        {"r", null}});

    BOOST_REQUIRE_EQUAL(evaluate(ttl_of_s, inputs2), null);
    BOOST_REQUIRE_EQUAL(evaluate(writetime_of_s, inputs2), null);
    BOOST_REQUIRE_EQUAL(evaluate(ttl_of_r, inputs2), null);
    BOOST_REQUIRE_EQUAL(evaluate(writetime_of_r, inputs2), null);

}

// It should be possible to prepare an empty conjunction
BOOST_AUTO_TEST_CASE(prepare_conjunction_empty) {
    schema_ptr table_schema = make_simple_test_schema();
    auto [db, db_data] = make_data_dictionary_database(table_schema);

    expression conj_empty = conjunction{
        .children = {},
    };

    expression expected = make_bool_const(true);

    BOOST_REQUIRE_EQUAL(prepare_expression(conj_empty, db, "test_ks", table_schema.get(), nullptr), expected);
    BOOST_REQUIRE_EQUAL(prepare_expression(conj_empty, db, "test_ks", table_schema.get(), make_receiver(boolean_type)),
                        expected);
}

// Preparing an empty conjunction should check that the type of receiver is correct
BOOST_AUTO_TEST_CASE(prepare_conjunction_empty_with_int_receiver) {
    schema_ptr table_schema = make_simple_test_schema();
    auto [db, db_data] = make_data_dictionary_database(table_schema);

    expression conj_empty = conjunction{
        .children = {},
    };

    ::lw_shared_ptr<column_specification> int_receiver = make_receiver(int32_type);

    BOOST_REQUIRE_THROW(prepare_expression(conj_empty, db, "test_ks", table_schema.get(), int_receiver),
                        exceptions::invalid_request_exception);
}

// Prepare a conjunction whose only element is the constant `false`
BOOST_AUTO_TEST_CASE(prepare_conjunction_one_untyped_const_false) {
    schema_ptr table_schema = make_simple_test_schema();
    auto [db, db_data] = make_data_dictionary_database(table_schema);

    expression conj_one = conjunction{
        .children = {make_bool_untyped("false")}};

    expression expected = make_bool_const(false);

    BOOST_REQUIRE_EQUAL(prepare_expression(conj_one, db, "test_ks", table_schema.get(), nullptr), expected);
    BOOST_REQUIRE_EQUAL(prepare_expression(conj_one, db, "test_ks", table_schema.get(), make_receiver(boolean_type)),
                        expected);
}

// Prepare a conjunction whose only element is the constant `true`
BOOST_AUTO_TEST_CASE(prepare_conjunction_one_untyped_const_true) {
    schema_ptr table_schema = make_simple_test_schema();
    auto [db, db_data] = make_data_dictionary_database(table_schema);

    expression conj_one = conjunction{
        .children = {make_bool_untyped("true")}};

    expression expected = make_bool_const(true);

    BOOST_REQUIRE_EQUAL(prepare_expression(conj_one, db, "test_ks", table_schema.get(), nullptr), expected);
    BOOST_REQUIRE_EQUAL(prepare_expression(conj_one, db, "test_ks", table_schema.get(), make_receiver(boolean_type)),
                        expected);
}

// Prepare a conjunction whose only element is the constant `null`
BOOST_AUTO_TEST_CASE(prepare_conjunction_one_untyped_const_null) {
    schema_ptr table_schema = make_simple_test_schema();
    auto [db, db_data] = make_data_dictionary_database(table_schema);

    expression conj_one = conjunction{
        .children = {make_null_untyped()}};

    expression expected = constant::make_null(boolean_type);

    BOOST_REQUIRE_EQUAL(prepare_expression(conj_one, db, "test_ks", table_schema.get(), nullptr), expected);
    BOOST_REQUIRE_EQUAL(prepare_expression(conj_one, db, "test_ks", table_schema.get(), make_receiver(boolean_type)),
                        expected);
}

// Try preparing a conjunction which contains an integer `0`.
// It shouldn't be possible, conjunctions don't accept integers and 0 can't be cast to bool.
BOOST_AUTO_TEST_CASE(prepare_conjunction_one_int_untyped_const_0) {
    schema_ptr table_schema = make_simple_test_schema();
    auto [db, db_data] = make_data_dictionary_database(table_schema);

    expression conj_one = conjunction{
        .children = {make_int_untyped("0")}};

    BOOST_REQUIRE_THROW(prepare_expression(conj_one, db, "test_ks", table_schema.get(), nullptr),
                        exceptions::invalid_request_exception);
    BOOST_REQUIRE_THROW(prepare_expression(conj_one, db, "test_ks", table_schema.get(), make_receiver(boolean_type)),
                        exceptions::invalid_request_exception);
}

// Preparing 'true AND true AND 1 AND true` should throw an error
BOOST_AUTO_TEST_CASE(prepare_conjunction_bools_and_one_int_untyped_const_0) {
    schema_ptr table_schema = make_simple_test_schema();
    auto [db, db_data] = make_data_dictionary_database(table_schema);

    expression conj = conjunction{
        .children = {make_bool_untyped("true"),
                     make_bool_untyped("true"),
                     make_int_untyped("1"),
                     make_bool_untyped("true")}};

    BOOST_REQUIRE_THROW(prepare_expression(conj, db, "test_ks", table_schema.get(), nullptr),
                        exceptions::invalid_request_exception);
    BOOST_REQUIRE_THROW(prepare_expression(conj, db, "test_ks", table_schema.get(), make_receiver(boolean_type)),
                        exceptions::invalid_request_exception);
}

// AND can only be used to AND booleans, not the binary AND on integers
BOOST_AUTO_TEST_CASE(prepare_conjunction_and_of_ints_is_invalid) {
    schema_ptr table_schema = make_simple_test_schema();
    auto [db, db_data] = make_data_dictionary_database(table_schema);

    expression conj_one = conjunction{
        .children = {make_int_untyped("0"),
                     make_int_untyped("1")}};

    BOOST_REQUIRE_THROW(prepare_expression(conj_one, db, "test_ks", table_schema.get(), nullptr),
                        exceptions::invalid_request_exception);
    BOOST_REQUIRE_THROW(prepare_expression(conj_one, db, "test_ks", table_schema.get(), make_receiver(int32_type)),
                        exceptions::invalid_request_exception);
}

// Prepare a conjunction with many elements:
// 'true AND true AND b1 AND (false AND b2)'
BOOST_AUTO_TEST_CASE(prepare_conjunction_many_elements) {
    schema_ptr table_schema = schema_builder("test_ks", "test_cf")
                                  .with_column("pk", int32_type, column_kind::partition_key)
                                  .with_column("b1", boolean_type, column_kind::regular_column)
                                  .with_column("b2", boolean_type, column_kind::regular_column)
                                  .build();

    auto [db, db_data] = make_data_dictionary_database(table_schema);

    expression sub_conj = conjunction{
        .children = {make_bool_untyped("false"),
                     unresolved_identifier{::make_shared<column_identifier_raw>("b2", false)}}};

    expression conj_many = conjunction{
        .children = {make_bool_untyped("true"),
                     make_bool_untyped("false"),
                     unresolved_identifier{
                         unresolved_identifier{::make_shared<column_identifier_raw>("b1", false)},
                     },
                     sub_conj}};

    expression expected =
        conjunction{.children = {make_bool_const(true), make_bool_const(false),
                                 column_value(table_schema->get_column_definition("b1")),
                                 conjunction{.children = {make_bool_const(false),
                                                          column_value(table_schema->get_column_definition("b2"))}}}};

    BOOST_REQUIRE_EQUAL(prepare_expression(conj_many, db, "test_ks", table_schema.get(), nullptr), expected);
    BOOST_REQUIRE_EQUAL(prepare_expression(conj_many, db, "test_ks", table_schema.get(), make_receiver(boolean_type)),
                        expected);

    // Integer receiver is rejected
    BOOST_REQUIRE_THROW(prepare_expression(conj_many, db, "test_ks", table_schema.get(), make_receiver(int32_type)),
                        exceptions::invalid_request_exception);
}

// Test that preparing the given binary operator works and produces the expected result.
void test_prepare_good_binary_operator(expression good_binop_unprepared,
                                       expression expected_prepared,
                                       data_dictionary::database db,
                                       const schema_ptr& table_schema) {
    // Preparing without a receiver works as expected
    BOOST_REQUIRE_EQUAL(prepare_expression(good_binop_unprepared, db, "test_ks", table_schema.get(), nullptr),
                        expected_prepared);

    // Preparing with boolean receiver works as expected
    BOOST_REQUIRE_EQUAL(
        prepare_expression(good_binop_unprepared, db, "test_ks", table_schema.get(), make_receiver(boolean_type)),
        expected_prepared);

    // reversed boolean type is also accepted
    BOOST_REQUIRE_EQUAL(prepare_expression(good_binop_unprepared, db, "test_ks", table_schema.get(),
                                           make_receiver(reversed_type_impl::get_instance(boolean_type))),
                        expected_prepared);

    // Receivers with non-bool type are rejected.
    // Potentially we could allow receivers that are tuple<bool, ...>.
    // Both Scylla and Cassandra allow to insert a single value in place of a tuple without adding the parenthesis.
    // So something like:
    // INSERT INTO tab (pk_int, float_bool_tuple) VALUES (123, 321.456)'
    // Would insert a tuple value (321.456, null)
    // There is no need to write VALUES (123, (321.456,))
    // In my opinion this kind of hidden type conversion is harmful and bug inducing.
    // We should allow it only in places where it's needed to stay compatible with Cassandra.
    std::vector<data_type> invalid_receiver_types = {
        byte_type,
        short_type,
        int32_type,
        long_type,
        ascii_type,
        bytes_type,
        utf8_type,
        date_type,
        timeuuid_type,
        timestamp_type,
        simple_date_type,
        time_type,
        uuid_type,
        inet_addr_type,
        float_type,
        double_type,
        varint_type,
        decimal_type,
        counter_type,
        duration_type,
        empty_type,
        list_type_impl::get_instance(boolean_type, false),
        list_type_impl::get_instance(boolean_type, true),
        set_type_impl::get_instance(boolean_type, false),
        set_type_impl::get_instance(boolean_type, true),
        map_type_impl::get_instance(boolean_type, boolean_type, false),
        map_type_impl::get_instance(boolean_type, boolean_type, true),
        tuple_type_impl::get_instance({boolean_type}),
        tuple_type_impl::get_instance({boolean_type, float_type}),
        tuple_type_impl::get_instance({utf8_type, float_type}),
        user_type_impl::get_instance("test_ks", "test_ut", {"field1", "field2"}, {boolean_type, float_type}, false),
        user_type_impl::get_instance("test_ks", "test_ut", {"field1", "field2"}, {boolean_type, float_type}, true)};

    for (const data_type& invalid_receiver_type : invalid_receiver_types) {
        BOOST_REQUIRE_THROW(prepare_expression(good_binop_unprepared, db, "test_ks", table_schema.get(),
                                               make_receiver(invalid_receiver_type)),
                            exceptions::invalid_request_exception);
        BOOST_REQUIRE_THROW(prepare_expression(good_binop_unprepared, db, "test_ks", table_schema.get(),
                                               make_receiver(reversed_type_impl::get_instance(invalid_receiver_type))),
                            exceptions::invalid_request_exception);
    }
}

// Expected valid type for RHS values.
// We must know which values are valid to generate the invalid ones.
enum struct expected_rhs_type {
    // float
    float_type,
    // text/ascii
    string_type,
    // tuple<float, int, text, double>
    multi_column_tuple,
    // list<float>
    float_in_list,
    // list<tuple<float, int, text, double>
    multi_column_tuple_in_list,
    // IS_NOT allows only NULL as the RHS, everything else is invalid
    is_not_null_rhs
};

// Generates invalid RHS values for use in prepare_binary_operator tests.
// The argument specifies what type of RHS values are right, so we can avoid them when generating the wrong ones.
std::vector<expression> get_invalid_rhs_values(expected_rhs_type expected_rhs) {
    // Start by adding values that are wrong in all prepare_binary_operator tests
    std::vector<expression> invalid_rhs_vals = {
        make_bool_untyped("true"), make_duration_untyped("365d"), make_hex_untyped("0xdeadbeef"),
        // A tuple where the third element has a type that doesn't match the one expected by multi_column_tuple
        tuple_constructor{.elements =
                              {
                                  make_float_untyped("123.45"),
                                  make_int_untyped("234"),
                                  make_bool_untyped("true"),
                                  make_float_untyped("45.67"),
                              }},
        // A tuple with too many elements for multi_column_tuple.
        // A tuple with too little elements doesn't cause an error - CQL accepts it, the missing fields are assumed to
        // be null.
        tuple_constructor{.elements =
                              {
                                  make_float_untyped("123.45"),
                                  make_int_untyped("234"),
                                  make_string_untyped("hello"),
                                  make_float_untyped("45.67"),
                                  make_float_untyped("123.45"),
                              }},
        collection_constructor{.style = collection_constructor::style_type::set, .elements = {}},
        collection_constructor{.style = collection_constructor::style_type::set,
                               .elements = {make_float_untyped("12.3"), make_float_untyped("5.6")}},
        collection_constructor{.style = collection_constructor::style_type::map, .elements = {}},
        collection_constructor{
            .style = collection_constructor::style_type::map,
            .elements = {tuple_constructor{.elements = {make_float_untyped("1"), make_float_untyped("2")}},
                         tuple_constructor{.elements = {make_float_untyped("3"), make_float_untyped("4")}}}},
        usertype_constructor{.elements = {}},
        usertype_constructor{.elements = {{column_identifier("field1", false), make_float_untyped("1")},
                                          {column_identifier("field2", false), make_float_untyped("2")}}}};

    // `float_int_tuple = 1.23` is a valid expression, so we have to avoid adding int/float values for multi_column
    // tests. This is allowed in both Cassandra and Scylla, and is equivalent to writing `float_int_tuple = (1.23,
    // null)`
    if (expected_rhs != expected_rhs_type::float_type && expected_rhs != expected_rhs_type::multi_column_tuple) {
        invalid_rhs_vals.push_back(make_int_untyped("123"));
        invalid_rhs_vals.push_back(make_float_untyped("56.78"));
    }

    if (expected_rhs != expected_rhs_type::string_type) {
        invalid_rhs_vals.push_back(make_string_untyped("good_day"));
    }

    if (expected_rhs != expected_rhs_type::multi_column_tuple) {
        invalid_rhs_vals.push_back(tuple_constructor{.elements = {}});
        invalid_rhs_vals.push_back(tuple_constructor{.elements = {
                                                         make_float_untyped("123.45"),
                                                         make_int_untyped("234"),
                                                         make_string_untyped("hi"),
                                                         make_float_untyped("45.67"),
                                                     }});
        invalid_rhs_vals.push_back(tuple_constructor{.elements = {
                                                         make_float_untyped("123.45"),
                                                         make_int_untyped("234"),
                                                         make_string_untyped("hi"),
                                                     }});
    }
    if (expected_rhs != expected_rhs_type::float_in_list &&
        expected_rhs != expected_rhs_type::multi_column_tuple_in_list) {
        invalid_rhs_vals.push_back(collection_constructor{
            .style = collection_constructor::style_type::list,
            .elements = {make_float_untyped("123.45"), make_float_untyped("732.2"), make_float_untyped("42.1")}});
        invalid_rhs_vals.push_back(collection_constructor{
            .style = collection_constructor::style_type::list,
            .elements = {make_float_untyped("232"), make_float_untyped("121"), make_float_untyped("937")}});
    }
    if (expected_rhs != expected_rhs_type::multi_column_tuple_in_list) {
        invalid_rhs_vals.push_back(collection_constructor{.style = collection_constructor::style_type::list,
                                                          .elements = {
                                                              tuple_constructor{.elements =
                                                                                    {
                                                                                        make_float_untyped("123.45"),
                                                                                        make_int_untyped("234"),
                                                                                        make_string_untyped("hi"),
                                                                                        make_float_untyped("45.67"),
                                                                                    }},
                                                              tuple_constructor{.elements =
                                                                                    {
                                                                                        make_float_untyped("231.1"),
                                                                                        make_int_untyped("232"),
                                                                                        make_string_untyped("dfdf"),
                                                                                        make_float_untyped("76.54"),
                                                                                    }},
                                                          }});
    }

    if (expected_rhs != expected_rhs_type::float_in_list &&
        expected_rhs != expected_rhs_type::multi_column_tuple_in_list) {
        invalid_rhs_vals.push_back(
            collection_constructor{.style = collection_constructor::style_type::list, .elements = {}});
    }
    return invalid_rhs_vals;
}

// Test preparing the given binary_operator with various invalid RHS values.
// The values are generated using get_invalid_rhs_values().
void test_prepare_binary_operator_invalid_rhs_values(const expression& good_binop,
                                                     expected_rhs_type expected_rhs,
                                                     data_dictionary::database db,
                                                     const schema_ptr& table_schema) {
    std::vector<expression> invalid_rhs_vals = get_invalid_rhs_values(expected_rhs);

    for (const expression& invalid_rhs : invalid_rhs_vals) {
        binary_operator invalid_binop = as<binary_operator>(good_binop);
        invalid_binop.rhs = invalid_rhs;

        BOOST_REQUIRE_THROW(prepare_expression(invalid_binop, db, "test_ks", table_schema.get(), nullptr),
                            exceptions::invalid_request_exception);
        BOOST_REQUIRE_THROW(
            prepare_expression(invalid_binop, db, "test_ks", table_schema.get(), make_receiver(boolean_type)),
            exceptions::invalid_request_exception);
    }
}

// The tests iterate over all possible comparison_orders so a convenience function is convenient.
std::array<comparison_order, 2> get_possible_comparison_orders() {
    return {comparison_order::cql, comparison_order::clustering};
}

// Test preparing a binary_operator with operations: =, !=, <, <=, >, >=
// The test enumerates various possible LHS values and tries all the operators for each of them.
// The LHS values always are of type float to make testing easy.
// This means that multi-column LHS are not tested in here and need to be tested in a separate test.
// The same goes for reversed_type.
BOOST_AUTO_TEST_CASE(prepare_binary_operator_eq_neq_lt_lte_gt_gte) {
    schema_ptr table_schema =
        schema_builder("test_ks", "test_cf")
            .with_column("pk", int32_type, column_kind::partition_key)
            .with_column("float_col", float_type, column_kind::regular_column)
            .with_column("reversed_float_col", reversed_type_impl::get_instance(float_type))
            .with_column("float_list_col", list_type_impl::get_instance(float_type, true), column_kind::regular_column)
            .with_column("frozen_float_list_col", list_type_impl::get_instance(float_type, false),
                         column_kind::regular_column)
            .with_column("double_float_map_col", map_type_impl::get_instance(double_type, float_type, true),
                         column_kind::regular_column)
            .with_column("frozen_double_float_map_col", map_type_impl::get_instance(double_type, float_type, false),
                         column_kind::regular_column)
            .build();
    auto [db, db_data] = make_data_dictionary_database(table_schema);

    // `float_col`
    expression unprepared_float_col =
        unresolved_identifier{.ident = ::make_shared<column_identifier_raw>("float_col", false)};
    expression prepared_float_col = column_value(table_schema->get_column_definition("float_col"));

    // `float_list_col[123]`
    expression unprepared_subscripted_float_list =
        subscript{.val = unresolved_identifier{.ident = ::make_shared<column_identifier_raw>("float_list_col", false)},
                  .sub = make_int_untyped("123")};

    expression prepared_subscripted_float_list =
        subscript{.val = column_value(table_schema->get_column_definition("float_list_col")),
                  .sub = make_int_const(123),
                  .type = float_type};

    // `frozen_float_list_col[123]`
    expression unprepared_subscripted_frozen_float_list = subscript{
        .val = unresolved_identifier{.ident = ::make_shared<column_identifier_raw>("frozen_float_list_col", false)},
        .sub = make_int_untyped("123")};

    expression prepared_subscripted_frozen_float_list =
        subscript{.val = column_value(table_schema->get_column_definition("frozen_float_list_col")),
                  .sub = make_int_const(123),
                  .type = float_type};

    // `double_float_map_col[123.4]`
    expression unprepared_subscripted_double_float_map = subscript{
        .val = unresolved_identifier{.ident = ::make_shared<column_identifier_raw>("double_float_map_col", false)},
        .sub = make_float_untyped("123.4")};

    expression prepared_subscripted_double_float_map =
        subscript{.val = column_value(table_schema->get_column_definition("double_float_map_col")),
                  .sub = make_double_const(123.4),
                  .type = float_type};

    // `frozen_double_float_map_col[123.4]`
    expression unprepared_subscripted_frozen_double_float_map = subscript{
        .val =
            unresolved_identifier{.ident = ::make_shared<column_identifier_raw>("frozen_double_float_map_col", false)},
        .sub = make_float_untyped("123.4")};

    expression prepared_subscripted_frozen_double_float_map =
        subscript{.val = column_value(table_schema->get_column_definition("frozen_double_float_map_col")),
                  .sub = make_double_const(123.4),
                  .type = float_type};

    // `double_float_map_col[123]` <- int index should work where double is expected
    expression unprepared_subscripted_double_float_map_with_int_index = subscript{
        .val = unresolved_identifier{.ident = ::make_shared<column_identifier_raw>("double_float_map_col", false)},
        .sub = make_int_untyped("123")};

    expression prepared_subscripted_double_float_map_with_int_index =
        subscript{.val = column_value(table_schema->get_column_definition("double_float_map_col")),
                  .sub = make_double_const(123),
                  .type = float_type};

    std::vector<std::pair<expression, expression>> possible_lhs_vals = {
        {unprepared_float_col, prepared_float_col},
        {unprepared_subscripted_float_list, prepared_subscripted_float_list},
        {unprepared_subscripted_frozen_float_list, prepared_subscripted_frozen_float_list},
        {unprepared_subscripted_double_float_map, prepared_subscripted_double_float_map},
        {unprepared_subscripted_frozen_double_float_map, prepared_subscripted_frozen_double_float_map},
        {unprepared_subscripted_double_float_map_with_int_index, prepared_subscripted_double_float_map_with_int_index}};

    std::vector<oper_t> possible_operations = {oper_t::EQ,  oper_t::NEQ, oper_t::LT,
                                               oper_t::LTE, oper_t::GT,  oper_t::GTE};

    for (auto [unprepared_lhs, prepared_lhs] : possible_lhs_vals) {
        for (const oper_t& op : possible_operations) {
            for (const comparison_order& comp_order : get_possible_comparison_orders()) {
                expression to_prepare = binary_operator(unprepared_lhs, op, make_float_untyped("123.4"), comp_order);

                expression expected = binary_operator(prepared_lhs, op, make_float_const(123.4), comp_order);

                test_prepare_good_binary_operator(to_prepare, expected, db, table_schema);

                test_prepare_binary_operator_invalid_rhs_values(to_prepare, expected_rhs_type::float_type, db,
                                                                table_schema);
            }
        }
    }
}

// Test operations =, !=, <, <=, >, >= with a LHS column that has reversed type.
// The prepared RHS should also have inherit the reversed type.
BOOST_AUTO_TEST_CASE(prepare_binary_operator_eq_neq_lt_lte_gt_gte_reversed_type) {
    schema_ptr table_schema = schema_builder("test_ks", "test_cf")
                                  .with_column("pk", int32_type, column_kind::partition_key)
                                  .with_column("reversed_float_col", reversed_type_impl::get_instance(float_type),
                                               column_kind::regular_column)
                                  .build();
    auto [db, db_data] = make_data_dictionary_database(table_schema);

    std::vector<oper_t> possible_operations = {oper_t::EQ,  oper_t::NEQ, oper_t::LT,
                                               oper_t::LTE, oper_t::GT,  oper_t::GTE};

    for (const oper_t& op : possible_operations) {
        for (const comparison_order& comp_order : get_possible_comparison_orders()) {
            expression to_prepare = binary_operator(
                unresolved_identifier{.ident = ::make_shared<column_identifier_raw>("reversed_float_col", false)}, op,
                make_float_untyped("123.4"), comp_order);

            constant rhs_const = make_float_const(123.4);
            rhs_const.type = reversed_type_impl::get_instance(float_type);
            expression expected = binary_operator(
                column_value(table_schema->get_column_definition("reversed_float_col")), op, rhs_const, comp_order);

            test_prepare_good_binary_operator(to_prepare, expected, db, table_schema);

            test_prepare_binary_operator_invalid_rhs_values(to_prepare, expected_rhs_type::float_type, db,
                                                            table_schema);
        }
    }
}

// Test operations =, !=, <, <=, >, >= with a multi-column LHS.
BOOST_AUTO_TEST_CASE(prepare_binary_operator_eq_neq_lt_lte_gt_gte_multi_column) {
    schema_ptr table_schema =
        schema_builder("test_ks", "test_cf")
            .with_column("pk", int32_type, column_kind::partition_key)
            .with_column("c1", float_type, column_kind::clustering_key)
            .with_column("c2", int32_type, column_kind::clustering_key)
            .with_column("c3", utf8_type, column_kind::clustering_key)
            .with_column("c4", reversed_type_impl::get_instance(double_type), column_kind::clustering_key)
            .build();
    auto [db, db_data] = make_data_dictionary_database(table_schema);

    expression unprepared_lhs = tuple_constructor{
        .elements = {unresolved_identifier{.ident = ::make_shared<column_identifier_raw>("c1", false)},
                     unresolved_identifier{.ident = ::make_shared<column_identifier_raw>("c2", false)},
                     unresolved_identifier{.ident = ::make_shared<column_identifier_raw>("c3", false)},
                     unresolved_identifier{.ident = ::make_shared<column_identifier_raw>("c4", false)}},
        .type = nullptr};

    expression prepared_lhs =
        tuple_constructor{.elements = {column_value(table_schema->get_column_definition("c1")),
                                       column_value(table_schema->get_column_definition("c2")),
                                       column_value(table_schema->get_column_definition("c3")),
                                       column_value(table_schema->get_column_definition("c4"))},
                          .type = tuple_type_impl::get_instance(
                              {float_type, int32_type, utf8_type, reversed_type_impl::get_instance(double_type)})};

    expression unprepared_rhs =
        tuple_constructor{.elements = {make_float_untyped("123.4"), make_int_untyped("1234"),
                                       make_string_untyped("hello"), make_float_untyped("112233.44")},
                          .type = nullptr};

    expression prepared_rhs = make_tuple_const(
        {make_float_raw(123.4), make_int_raw(1234), make_text_raw("hello"), make_double_raw(112233.44)},
        {float_type, int32_type, utf8_type, double_type});

    std::vector<oper_t> possible_operations = {oper_t::EQ,  oper_t::NEQ, oper_t::LT,
                                               oper_t::LTE, oper_t::GT,  oper_t::GTE};

    for (const oper_t& op : possible_operations) {
        for (const comparison_order& comp_order : get_possible_comparison_orders()) {
            expression to_prepare = binary_operator(unprepared_lhs, op, unprepared_rhs, comp_order);

            expression expected = binary_operator(prepared_lhs, op, prepared_rhs, comp_order);

            test_prepare_good_binary_operator(to_prepare, expected, db, table_schema);

            test_prepare_binary_operator_invalid_rhs_values(to_prepare, expected_rhs_type::multi_column_tuple, db,
                                                            table_schema);
        }
    }
}


// `float_col IN ()`
BOOST_AUTO_TEST_CASE(prepare_binary_operator_float_col_in_empty_list) {
    schema_ptr table_schema = schema_builder("test_ks", "test_cf")
                                  .with_column("pk", int32_type, column_kind::partition_key)
                                  .with_column("float_col", float_type, column_kind::regular_column)
                                  .build();

    auto [db, db_data] = make_data_dictionary_database(table_schema);

    for (const comparison_order& comp_order : get_possible_comparison_orders()) {
        expression to_prepare = binary_operator(
            unresolved_identifier{.ident = ::make_shared<column_identifier_raw>("float_col", false)}, oper_t::IN,
            collection_constructor{.style = collection_constructor::style_type::list, .elements = {}}, comp_order);

        expression expected =
            binary_operator(column_value(table_schema->get_column_definition("float_col")), oper_t::IN,
                            constant(make_list_raw({}), list_type_impl::get_instance(float_type, false)), comp_order);

        test_prepare_good_binary_operator(to_prepare, expected, db, table_schema);

        test_prepare_binary_operator_invalid_rhs_values(to_prepare, expected_rhs_type::float_in_list, db, table_schema);
    }
}

// `float_col IN (1, 2.3)`
BOOST_AUTO_TEST_CASE(prepare_binary_operator_float_col_in_1_2_dot_3) {
    schema_ptr table_schema = schema_builder("test_ks", "test_cf")
                                  .with_column("pk", int32_type, column_kind::partition_key)
                                  .with_column("float_col", float_type, column_kind::regular_column)
                                  .build();

    auto [db, db_data] = make_data_dictionary_database(table_schema);

    for (const comparison_order& comp_order : get_possible_comparison_orders()) {
        expression to_prepare = binary_operator(
            unresolved_identifier{.ident = ::make_shared<column_identifier_raw>("float_col", false)}, oper_t::IN,
            collection_constructor{.style = collection_constructor::style_type::list,
                                   .elements = {make_int_untyped("1"), make_float_untyped("2.3")}},
            comp_order);

        expression expected =
            binary_operator(column_value(table_schema->get_column_definition("float_col")), oper_t::IN,
                            constant(make_list_raw({make_float_raw(1), make_float_raw(2.3)}),
                                     list_type_impl::get_instance(float_type, false)),
                            comp_order);

        test_prepare_good_binary_operator(to_prepare, expected, db, table_schema);

        test_prepare_binary_operator_invalid_rhs_values(to_prepare, expected_rhs_type::float_in_list, db, table_schema);
    }
}

// `float_col IN (1, 2, 3, 4)`
BOOST_AUTO_TEST_CASE(prepare_binary_operator_float_col_in_1_2_3_4) {
    schema_ptr table_schema = schema_builder("test_ks", "test_cf")
                                  .with_column("pk", int32_type, column_kind::partition_key)
                                  .with_column("float_col", float_type, column_kind::regular_column)
                                  .build();

    auto [db, db_data] = make_data_dictionary_database(table_schema);

    for (const comparison_order& comp_order : get_possible_comparison_orders()) {
        expression to_prepare = binary_operator(
            unresolved_identifier{.ident = ::make_shared<column_identifier_raw>("float_col", false)}, oper_t::IN,
            collection_constructor{.style = collection_constructor::style_type::list,
                                   .elements =
                                       {
                                           make_int_untyped("1"),
                                           make_int_untyped("2"),
                                           make_int_untyped("3"),
                                           make_int_untyped("4"),
                                       }},
            comp_order);

        expression expected = binary_operator(
            column_value(table_schema->get_column_definition("float_col")), oper_t::IN,
            constant(make_list_raw({make_float_raw(1), make_float_raw(2), make_float_raw(3), make_float_raw(4)}),
                     list_type_impl::get_instance(float_type, false)),
            comp_order);

        test_prepare_good_binary_operator(to_prepare, expected, db, table_schema);

        test_prepare_binary_operator_invalid_rhs_values(to_prepare, expected_rhs_type::float_in_list, db, table_schema);
    }
}

// reverse_float_col IN ()
BOOST_AUTO_TEST_CASE(prepare_binary_operato_reverse_float_col_in_empty_list) {
    schema_ptr table_schema =
        schema_builder("test_ks", "test_cf")
            .with_column("pk", int32_type, column_kind::partition_key)
            .with_column("reverse_float_col", reversed_type_impl::get_instance(float_type), column_kind::regular_column)
            .build();

    auto [db, db_data] = make_data_dictionary_database(table_schema);

    for (const comparison_order& comp_order : get_possible_comparison_orders()) {
        expression to_prepare = binary_operator(
            unresolved_identifier{.ident = ::make_shared<column_identifier_raw>("reverse_float_col", false)},
            oper_t::IN, collection_constructor{.style = collection_constructor::style_type::list, .elements = {}},
            comp_order);

        expression expected =
            binary_operator(column_value(table_schema->get_column_definition("reverse_float_col")), oper_t::IN,
                            constant(make_list_raw({}), list_type_impl::get_instance(float_type, false)), comp_order);

        test_prepare_good_binary_operator(to_prepare, expected, db, table_schema);

        test_prepare_binary_operator_invalid_rhs_values(to_prepare, expected_rhs_type::float_in_list, db, table_schema);
    }
}

// `reverse_float_col IN (1.2, 2.3)`
BOOST_AUTO_TEST_CASE(prepare_binary_operator_float_col_in_1_dot_2_2_dot_3) {
    schema_ptr table_schema =
        schema_builder("test_ks", "test_cf")
            .with_column("pk", int32_type, column_kind::partition_key)
            .with_column("reverse_float_col", reversed_type_impl::get_instance(float_type), column_kind::regular_column)
            .build();

    auto [db, db_data] = make_data_dictionary_database(table_schema);

    for (const comparison_order& comp_order : get_possible_comparison_orders()) {
        expression to_prepare = binary_operator(
            unresolved_identifier{.ident = ::make_shared<column_identifier_raw>("reverse_float_col", false)},
            oper_t::IN,
            collection_constructor{.style = collection_constructor::style_type::list,
                                   .elements = {make_float_untyped("1.2"), make_float_untyped("2.3")}},
            comp_order);

        expression expected =
            binary_operator(column_value(table_schema->get_column_definition("reverse_float_col")), oper_t::IN,
                            constant(make_list_raw({make_float_raw(1.2), make_float_raw(2.3)}),
                                     list_type_impl::get_instance(float_type, false)),
                            comp_order);

        test_prepare_good_binary_operator(to_prepare, expected, db, table_schema);

        test_prepare_binary_operator_invalid_rhs_values(to_prepare, expected_rhs_type::float_in_list, db, table_schema);
    }
}

// `(float_col, int_col, text_col, reverse_double_col) IN ()`
BOOST_AUTO_TEST_CASE(prepare_binary_operator_multi_col_in_empty_list) {
    schema_ptr table_schema = schema_builder("test_ks", "test_cf")
                                  .with_column("pk", int32_type, column_kind::partition_key)
                                  .with_column("float_col", float_type, column_kind::clustering_key)
                                  .with_column("int_col", int32_type, column_kind::clustering_key)
                                  .with_column("text_col", utf8_type, column_kind::clustering_key)
                                  .with_column("reverse_double_col", reversed_type_impl::get_instance(double_type))
                                  .build();
    auto [db, db_data] = make_data_dictionary_database(table_schema);

    expression unprepared_lhs = tuple_constructor{
        .elements =
            {
                unresolved_identifier{.ident = ::make_shared<column_identifier_raw>("float_col", false)},
                unresolved_identifier{.ident = ::make_shared<column_identifier_raw>("int_col", false)},
                unresolved_identifier{.ident = ::make_shared<column_identifier_raw>("text_col", false)},
                unresolved_identifier{.ident = ::make_shared<column_identifier_raw>("reverse_double_col", false)},
            },
        .type = nullptr};

    data_type tuple_type = tuple_type_impl::get_instance(
        {float_type, int32_type, utf8_type, reversed_type_impl::get_instance(double_type)});

    expression prepared_lhs =
        tuple_constructor{.elements = {column_value(table_schema->get_column_definition("float_col")),
                                       column_value(table_schema->get_column_definition("int_col")),
                                       column_value(table_schema->get_column_definition("text_col")),
                                       column_value(table_schema->get_column_definition("reverse_double_col"))},
                          .type = tuple_type};

    expression unprepared_rhs =
        collection_constructor{.style = collection_constructor::style_type::list, .elements = {}};

    // reversed is removed!
    expression prepared_rhs = constant(
        make_list_raw({}), list_type_impl::get_instance(
                               tuple_type_impl::get_instance({float_type, int32_type, utf8_type, double_type}), false));

    for (const comparison_order& comp_order : get_possible_comparison_orders()) {
        expression to_prepare = binary_operator(unprepared_lhs, oper_t::IN, unprepared_rhs, comp_order);

        expression expected = binary_operator(prepared_lhs, oper_t::IN, prepared_rhs, comp_order);

        test_prepare_good_binary_operator(to_prepare, expected, db, table_schema);

        test_prepare_binary_operator_invalid_rhs_values(to_prepare, expected_rhs_type::multi_column_tuple_in_list, db,
                                                        table_schema);
    }
}

// `(float_col, int_col, text_col, reverse_double_col) IN ((1.2, 3, 'four', 8.9), (5, 6, 'seven', 10.11))`
BOOST_AUTO_TEST_CASE(prepare_binary_operator_multi_col_in_values) {
    schema_ptr table_schema = schema_builder("test_ks", "test_cf")
                                  .with_column("pk", int32_type, column_kind::partition_key)
                                  .with_column("float_col", float_type, column_kind::clustering_key)
                                  .with_column("int_col", int32_type, column_kind::clustering_key)
                                  .with_column("text_col", utf8_type, column_kind::clustering_key)
                                  .with_column("reverse_double_col", reversed_type_impl::get_instance(double_type))
                                  .build();
    auto [db, db_data] = make_data_dictionary_database(table_schema);

    expression unprepared_lhs = tuple_constructor{
        .elements =
            {
                unresolved_identifier{.ident = ::make_shared<column_identifier_raw>("float_col", false)},
                unresolved_identifier{.ident = ::make_shared<column_identifier_raw>("int_col", false)},
                unresolved_identifier{.ident = ::make_shared<column_identifier_raw>("text_col", false)},
                unresolved_identifier{.ident = ::make_shared<column_identifier_raw>("reverse_double_col", false)},
            },
        .type = nullptr};

    data_type tuple_type = tuple_type_impl::get_instance(
        {float_type, int32_type, utf8_type, reversed_type_impl::get_instance(double_type)});

    expression prepared_lhs =
        tuple_constructor{.elements = {column_value(table_schema->get_column_definition("float_col")),
                                       column_value(table_schema->get_column_definition("int_col")),
                                       column_value(table_schema->get_column_definition("text_col")),
                                       column_value(table_schema->get_column_definition("reverse_double_col"))},
                          .type = tuple_type};

    expression unprepared_rhs =
        collection_constructor{.style = collection_constructor::style_type::list,
                               .elements = {tuple_constructor{.elements =
                                                                  {
                                                                      make_float_untyped("1.2"),
                                                                      make_int_untyped("3"),
                                                                      make_string_untyped("four"),
                                                                      make_float_untyped("8.9"),
                                                                  }},
                                            tuple_constructor{.elements = {
                                                                  make_int_untyped("5"),
                                                                  make_int_untyped("6"),
                                                                  make_string_untyped("seven"),
                                                                  make_float_untyped("10.11"),
                                                              }}}};

    raw_value prepared_rhs_raw = make_list_raw(
        {make_tuple_raw({make_float_raw(1.2), make_int_raw(3), make_text_raw("four"), make_double_raw(8.9)}),
         make_tuple_raw({make_float_raw(5), make_int_raw(6), make_text_raw("seven"), make_double_raw(10.11)})});

    // reversed is removed!
    data_type prepared_rhs_type = list_type_impl::get_instance(
        tuple_type_impl::get_instance({float_type, int32_type, utf8_type, double_type}), false);

    expression prepared_rhs = constant(prepared_rhs_raw, prepared_rhs_type);

    for (const comparison_order& comp_order : get_possible_comparison_orders()) {
        expression to_prepare = binary_operator(unprepared_lhs, oper_t::IN, unprepared_rhs, comp_order);

        expression expected = binary_operator(prepared_lhs, oper_t::IN, prepared_rhs, comp_order);

        test_prepare_good_binary_operator(to_prepare, expected, db, table_schema);

        test_prepare_binary_operator_invalid_rhs_values(to_prepare, expected_rhs_type::multi_column_tuple_in_list, db,
                                                        table_schema);
    }
}

BOOST_AUTO_TEST_CASE(prepare_binary_operator_contains) {
    schema_ptr table_schema =
        schema_builder("test_ks", "test_cf")
            .with_column("pk", int32_type, column_kind::partition_key)
            .with_column("float_list", list_type_impl::get_instance(float_type, true), column_kind::regular_column)
            .with_column("frozen_float_list", list_type_impl::get_instance(float_type, false),
                         column_kind::regular_column)
            .with_column("float_set", set_type_impl::get_instance(float_type, true), column_kind::regular_column)
            .with_column("frozen_float_set", set_type_impl::get_instance(float_type, false),
                         column_kind::regular_column)
            .with_column("double_float_map", map_type_impl::get_instance(double_type, float_type, true),
                         column_kind::regular_column)
            .with_column("frozen_double_float_map", map_type_impl::get_instance(double_type, float_type, false),
                         column_kind::regular_column)
            .build();

    auto [db, db_data] = make_data_dictionary_database(table_schema);

    std::vector<const char*> possible_lhs_col_names = {"float_list",       "frozen_float_list",
                                                       "float_set",        "frozen_float_set",
                                                       "double_float_map", "frozen_double_float_map"};

    std::vector<std::pair<untyped_constant, constant>> possible_rhs_vals = {
        {make_int_untyped("123"), make_float_const(123)},
        {
            make_float_untyped("123.45"),
            make_float_const(123.45),
        }};

    for (const char* lhs_col_name : possible_lhs_col_names) {
        for (auto& [unprepared_rhs, prepared_rhs] : possible_rhs_vals) {
            for (const comparison_order& comp_order : get_possible_comparison_orders()) {
                expression to_prepare = binary_operator(
                    unresolved_identifier{.ident = ::make_shared<column_identifier_raw>(lhs_col_name, false)},
                    oper_t::CONTAINS, unprepared_rhs, comp_order);

                expression expected = binary_operator(column_value(table_schema->get_column_definition(lhs_col_name)),
                                                      oper_t::CONTAINS, prepared_rhs, comp_order);

                test_prepare_good_binary_operator(to_prepare, expected, db, table_schema);

                test_prepare_binary_operator_invalid_rhs_values(to_prepare, expected_rhs_type::float_type, db,
                                                                table_schema);
            }
        }
    }
}

BOOST_AUTO_TEST_CASE(prepare_binary_operator_contains_key) {
    schema_ptr table_schema =
        schema_builder("test_ks", "test_cf")
            .with_column("pk", int32_type, column_kind::partition_key)
            .with_column("double_float_map", map_type_impl::get_instance(double_type, float_type, true),
                         column_kind::regular_column)
            .with_column("frozen_double_float_map", map_type_impl::get_instance(double_type, float_type, false),
                         column_kind::regular_column)
            .build();

    auto [db, db_data] = make_data_dictionary_database(table_schema);

    std::vector<const char*> possible_lhs_col_names = {"double_float_map", "frozen_double_float_map"};

    std::vector<std::pair<untyped_constant, constant>> possible_rhs_vals = {
        {make_int_untyped("123"), make_double_const(123)},
        {
            make_float_untyped("123.45"),
            make_double_const(123.45),
        }};

    for (const char* lhs_col_name : possible_lhs_col_names) {
        for (auto& [unprepared_rhs, prepared_rhs] : possible_rhs_vals) {
            for (const comparison_order& comp_order : get_possible_comparison_orders()) {
                expression to_prepare = binary_operator(
                    unresolved_identifier{.ident = ::make_shared<column_identifier_raw>(lhs_col_name, false)},
                    oper_t::CONTAINS_KEY, unprepared_rhs, comp_order);

                expression expected = binary_operator(column_value(table_schema->get_column_definition(lhs_col_name)),
                                                      oper_t::CONTAINS_KEY, prepared_rhs, comp_order);

                test_prepare_good_binary_operator(to_prepare, expected, db, table_schema);

                test_prepare_binary_operator_invalid_rhs_values(to_prepare, expected_rhs_type::float_type, db,
                                                                table_schema);
            }
        }
    }
}

BOOST_AUTO_TEST_CASE(prepare_binary_operator_like) {
    schema_ptr table_schema = schema_builder("test_ks", "test_cf")
                                  .with_column("pk", int32_type, column_kind::partition_key)
                                  .with_column("text_col", utf8_type, column_kind::regular_column)
                                  .with_column("ascii_col", ascii_type, column_kind::regular_column)
                                  .build();

    auto [db, db_data] = make_data_dictionary_database(table_schema);

    std::vector<const char*> possible_lhs_col_names = {"text_col", "ascii_col"};

    for (const char* lhs_col_name : possible_lhs_col_names) {
        for (const comparison_order& comp_order : get_possible_comparison_orders()) {
            expression to_prepare = binary_operator(
                unresolved_identifier{.ident = ::make_shared<column_identifier_raw>(lhs_col_name, false)}, oper_t::LIKE,
                make_string_untyped("some%hing"), comp_order);

            const column_definition* lhs_col_def = table_schema->get_column_definition(lhs_col_name);

            expression expected = binary_operator(column_value(lhs_col_def), oper_t::LIKE,
                                                  constant(make_text_raw("some%hing"), lhs_col_def->type), comp_order);

            test_prepare_good_binary_operator(to_prepare, expected, db, table_schema);

            test_prepare_binary_operator_invalid_rhs_values(to_prepare, expected_rhs_type::string_type, db,
                                                            table_schema);
        }
    }
}

BOOST_AUTO_TEST_CASE(prepare_binary_operator_is_not_null) {
    schema_ptr table_schema = schema_builder("test_ks", "test_cf")
                                  .with_column("pk", int32_type, column_kind::partition_key)
                                  .with_column("float_col", float_type, column_kind::regular_column)
                                  .build();
    auto [db, db_data] = make_data_dictionary_database(table_schema);

    for (const comparison_order& comp_order : get_possible_comparison_orders()) {
        expression to_prepare =
            binary_operator(unresolved_identifier{.ident = ::make_shared<column_identifier_raw>("float_col", false)},
                            oper_t::IS_NOT, make_null_untyped(), comp_order);

        expression expected = binary_operator(column_value(table_schema->get_column_definition("float_col")),
                                              oper_t::IS_NOT, constant::make_null(float_type), comp_order);

        test_prepare_good_binary_operator(to_prepare, expected, db, table_schema);

        test_prepare_binary_operator_invalid_rhs_values(to_prepare, expected_rhs_type::is_not_null_rhs, db,
                                                        table_schema);
    }
}

// `float_col = NULL`, `float_col < NULL`, ...
// The RHS should be prepared as a NULL constant with float type.
BOOST_AUTO_TEST_CASE(prepare_binary_operator_with_null_rhs) {
    schema_ptr table_schema = schema_builder("test_ks", "test_cf")
                                  .with_column("pk", int32_type, column_kind::partition_key)
                                  .with_column("float_col", float_type, column_kind::regular_column)
                                  .build();
    auto [db, db_data] = make_data_dictionary_database(table_schema);

    std::vector<oper_t> possible_operations = {oper_t::EQ,  oper_t::NEQ, oper_t::LT,
                                               oper_t::LTE, oper_t::GT,  oper_t::GTE};

    for (const oper_t& op : possible_operations) {
        for (const comparison_order& comp_order : get_possible_comparison_orders()) {
            expression to_prepare = binary_operator(
                unresolved_identifier{.ident = ::make_shared<column_identifier_raw>("float_col", false)}, op,
                make_null_untyped(), comp_order);

            expression expected = binary_operator(column_value(table_schema->get_column_definition("float_col")), op,
                                                  constant::make_null(float_type), comp_order);

            test_prepare_good_binary_operator(to_prepare, expected, db, table_schema);

            test_prepare_binary_operator_invalid_rhs_values(to_prepare, expected_rhs_type::float_type, db,
                                                            table_schema);
        }
    }
}

BOOST_AUTO_TEST_CASE(optimized_constant_like) {
    auto check = [] (expression e, std::optional<sstring> target, bool expect_optimization, std::optional<sstring> pattern_arg = {}) {
        auto optimized = optimize_like(e);
        bool was_optimized = find_binop(optimized, [] (const binary_operator&) { return true; }) == nullptr;
        if (was_optimized != expect_optimization) {
            return false;
        }
        auto params = std::vector({target ? make_text_raw(*target) : raw_value::make_null()});
        if (pattern_arg) {
            params.push_back(make_text_raw(*pattern_arg));
        }
        return evaluate_with_bind_variables(optimized, params) == evaluate_with_bind_variables(e, params);
    };

    auto target_var = make_bind_variable(0, utf8_type);
    auto pattern_var = make_bind_variable(1, utf8_type);

    BOOST_REQUIRE(check(binary_operator(target_var, oper_t::LIKE, make_text_const("xx%")), "xxyyz", true));
    BOOST_REQUIRE(check(binary_operator(target_var, oper_t::LIKE, make_text_const("xx%")), "qxyyz", true));
    BOOST_REQUIRE(check(binary_operator(target_var, oper_t::LIKE, make_text_const("xx%")), std::nullopt, true));
    BOOST_REQUIRE(check(binary_operator(target_var, oper_t::LIKE, pattern_var), "xxyyz", false, "xx%"));
    BOOST_REQUIRE(check(binary_operator(target_var, oper_t::LIKE, pattern_var), "qxyyz", false, "xx%"));
    BOOST_REQUIRE(check(binary_operator(target_var, oper_t::LIKE, pattern_var), std::nullopt, false, "xx%"));

    // Verify that optimization works for subexpressions, not just top-level expressions
    auto complex = make_conjunction(
            binary_operator(target_var, oper_t::LIKE, make_text_const("xx%")),
            // repeated for simplicity
            binary_operator(target_var, oper_t::LIKE, make_text_const("xx%")));
    BOOST_REQUIRE(check(std::move(complex), "xxyyz", true));
}

BOOST_AUTO_TEST_CASE(prepare_token_func_without_receiver) {
    schema_ptr table_schema = make_three_pk_schema();
    auto [db, db_data] = make_data_dictionary_database(table_schema);

    expression token_fun_call = function_call{
        .func = functions::function_name::native_function("token"),
        .args = {column_value(table_schema->get_column_definition("pk1")), make_column("pk2"), make_int_const(1234)}};

    expression prepared_no_receiver = prepare_expression(token_fun_call, db, "test_ks", table_schema.get(), nullptr);
    expression prepared_with_receiver =
        prepare_expression(token_fun_call, db, "test_ks", table_schema.get(), make_receiver(long_type));

    auto check_prepared = [&](const expression& prepared) {
        const function_call* prepared_token = as_if<function_call>(&prepared);
        BOOST_REQUIRE(prepared_token != nullptr);

        const seastar::shared_ptr<functions::function>* token_fun =
            std::get_if<shared_ptr<functions::function>>(&prepared_token->func);
        BOOST_REQUIRE(token_fun != nullptr);
        BOOST_REQUIRE((*token_fun)->name() == functions::function_name::native_function("token"));

        std::vector<expression> expected_args = {column_value(table_schema->get_column_definition("pk1")),
                                                 column_value(table_schema->get_column_definition("pk2")),
                                                 make_int_const(1234)};
        BOOST_REQUIRE_EQUAL(prepared_token->args, expected_args);
    };

    check_prepared(prepared_no_receiver);
    check_prepared(prepared_with_receiver);
}

BOOST_AUTO_TEST_CASE(is_token_function_valid_call) {
    function_call token_fun_call{.func = functions::function_name::native_function("token"), .args = {}};
    BOOST_REQUIRE_EQUAL(is_token_function(token_fun_call), true);
}

BOOST_AUTO_TEST_CASE(is_token_function_invalid_call) {
    function_call token_fun_call{.func = functions::function_name::native_function("invalid_function"), .args = {}};
    BOOST_REQUIRE_EQUAL(is_token_function(token_fun_call), false);
}

BOOST_AUTO_TEST_CASE(is_token_function_valid_call_with_keyspace) {
    function_call token_fun_call{.func = functions::function_name("system", "token"), .args = {}};
    BOOST_REQUIRE_EQUAL(is_token_function(token_fun_call), true);
}

BOOST_AUTO_TEST_CASE(is_token_function_invalid_call_with_keyspace) {
    function_call token_fun_call{.func = functions::function_name("system", "invalid_token"),
                                                       .args = {}};
    BOOST_REQUIRE_EQUAL(is_token_function(token_fun_call), false);
}

BOOST_AUTO_TEST_CASE(is_token_function_invalid_call_with_invalid_keyspace) {
    function_call token_fun_call{
        .func = functions::function_name("invalid_keyspace", "token"), .args = {}};
    BOOST_REQUIRE_EQUAL(is_token_function(token_fun_call), false);
}

BOOST_AUTO_TEST_CASE(is_token_function_prepared_token) {
    schema_ptr table_schema = make_three_pk_schema();
    auto [db, db_data] = make_data_dictionary_database(table_schema);

    function_call token_fun{
        .func = functions::function_name::native_function("token"),
        .args = {make_int_untyped("123"), make_int_untyped("443"), make_bind_variable(0, int32_type)}};

    expression prepared = prepare_expression(token_fun, db, "test_ks", table_schema.get(), nullptr);

    BOOST_REQUIRE_EQUAL(is_token_function(prepared), true);
}

BOOST_AUTO_TEST_CASE(is_token_function_prepared_nontoken) {
    schema_ptr table_schema = make_three_pk_schema();
    auto [db, db_data] = make_data_dictionary_database(table_schema);

    function_call now_fun{.func = functions::function_name::native_function("now"), .args = {}};

    expression prepared = prepare_expression(now_fun, db, "test_ks", table_schema.get(), nullptr);

    BOOST_REQUIRE_EQUAL(is_token_function(prepared), false);
}

// Test the function expr::is_partition_token_for_schema
BOOST_AUTO_TEST_CASE(is_partition_token_for_schema_test) {
    schema_ptr schema1 = make_simple_test_schema();  // partition key: (pk)
    schema_ptr schema2 = make_three_pk_schema();     // partition key: (pk1, pk2, pk3)
    schema_ptr schema3 = make_three_pk_schema();     // partition key: (pk1, pk2, pk3)

    auto [db1, db1_data] = make_data_dictionary_database(schema1);
    auto [db2, db2_data] = make_data_dictionary_database(schema2);
    auto [db3, db3_data] = make_data_dictionary_database(schema3);

    // Prepare token(pk)
    expression unprepared_token_one_pk =
        function_call{.func = functions::function_name::native_function("token"), .args = {make_column("pk")}};
    expression prepared_token_one_pk =
        prepare_expression(unprepared_token_one_pk, db1, "test_ks", schema1.get(), nullptr);

    BOOST_REQUIRE_EQUAL(is_partition_token_for_schema(prepared_token_one_pk, *schema1), true);
    BOOST_REQUIRE_EQUAL(is_partition_token_for_schema(prepared_token_one_pk, *schema2), false);
    BOOST_REQUIRE_EQUAL(is_partition_token_for_schema(prepared_token_one_pk, *schema3), false);

    // Prepare token(pk1, pk2, pk3)
    expression unprepared_token_three_pk =
        function_call{.func = functions::function_name::native_function("token"),
                      .args = {make_column("pk1"), make_column("pk2"), make_column("pk3")}};
    expression prepared_token_three_pk =
        prepare_expression(unprepared_token_three_pk, db2, "test_ks", schema2.get(), nullptr);

    BOOST_REQUIRE_EQUAL(is_partition_token_for_schema(prepared_token_one_pk, *schema1), true);
    BOOST_REQUIRE_EQUAL(is_partition_token_for_schema(prepared_token_one_pk, *schema2), false);

    // Same columns, but different schema!
    BOOST_REQUIRE_EQUAL(is_partition_token_for_schema(prepared_token_one_pk, *schema3), false);

    // Try preparing token(pk1, pk2), fail
    expression unprepared_token_two_pk = function_call{.func = functions::function_name::native_function("token"),
                                                       .args = {make_column("pk1"), make_column("pk2")}};
    BOOST_REQUIRE_THROW(prepare_expression(unprepared_token_two_pk, db2, "test_ks", schema2.get(), nullptr),
                        exceptions::invalid_request_exception);

    // Prepare token(pk1, pk3, pk2) - wrong order of pk columns
    expression unprepared_token_pk1_pk3_pk2 =
        function_call{.func = functions::function_name::native_function("token"),
                      .args = {make_column("pk1"), make_column("pk3"), make_column("pk2")}};
    expression prepared_token_pk1_pk3_pk2 =
        prepare_expression(unprepared_token_pk1_pk3_pk2, db2, "test_ks", schema2.get(), nullptr);

    BOOST_REQUIRE_EQUAL(is_partition_token_for_schema(prepared_token_pk1_pk3_pk2, *schema1), false);
    BOOST_REQUIRE_EQUAL(is_partition_token_for_schema(prepared_token_pk1_pk3_pk2, *schema2), false);
    BOOST_REQUIRE_EQUAL(is_partition_token_for_schema(prepared_token_pk1_pk3_pk2, *schema3), false);

    // Prepare token(pk1, pk1, pk3) - duplicate column
    expression unprepared_token_pk1_pk1_pk2 =
        function_call{.func = functions::function_name::native_function("token"),
                      .args = {make_column("pk1"), make_column("pk1"), make_column("pk3")}};
    expression prepared_token_pk1_pk1_pk2 =
        prepare_expression(unprepared_token_pk1_pk1_pk2, db2, "test_ks", schema2.get(), nullptr);

    BOOST_REQUIRE_EQUAL(is_partition_token_for_schema(prepared_token_pk1_pk1_pk2, *schema1), false);
    BOOST_REQUIRE_EQUAL(is_partition_token_for_schema(prepared_token_pk1_pk1_pk2, *schema2), false);
    BOOST_REQUIRE_EQUAL(is_partition_token_for_schema(prepared_token_pk1_pk1_pk2, *schema3), false);

    // Prepare token(ck)
    expression unprepared_token_ck =
        function_call{.func = functions::function_name::native_function("token"), .args = {make_column("ck")}};
    expression prepared_token_ck = prepare_expression(unprepared_token_ck, db1, "test_ks", schema1.get(), nullptr);

    BOOST_REQUIRE_EQUAL(is_partition_token_for_schema(prepared_token_ck, *schema1), false);
    BOOST_REQUIRE_EQUAL(is_partition_token_for_schema(prepared_token_ck, *schema2), false);
    BOOST_REQUIRE_EQUAL(is_partition_token_for_schema(prepared_token_ck, *schema3), false);

    // Prepare token(1, 2, ?) - not a partition token
    // The bind variable is there to prevent the prepared expression from turning into an expr::constant
    expression unprepared_token_other =
        function_call{.func = functions::function_name::native_function("token"),
                      .args = {make_int_untyped("1"), make_int_untyped("2"), make_bind_variable(0, int32_type)}};

    expression prepared_token_other =
        prepare_expression(unprepared_token_other, db2, "test_ks", schema2.get(), nullptr);

    BOOST_REQUIRE_EQUAL(is_partition_token_for_schema(prepared_token_other, *schema1), false);
    BOOST_REQUIRE_EQUAL(is_partition_token_for_schema(prepared_token_other, *schema2), false);
    BOOST_REQUIRE_EQUAL(is_partition_token_for_schema(prepared_token_other, *schema3), false);
}

BOOST_AUTO_TEST_CASE(test_aggregation_depth) {
    BOOST_REQUIRE_EQUAL(aggregation_depth(make_bigint_const(7)), 0);
    auto schema = make_simple_test_schema();
    auto [db, db_data] = make_data_dictionary_database(schema);
    auto avg_sum_r = expression(
            function_call{
                    .func = functions::function_name("", "avg"),
                    .args = {
                            function_call{
                                    .func = functions::function_name("", "sum"),
                                    .args = {
                                            unresolved_identifier{
                                                    .ident = ::make_shared<column_identifier_raw>("r", false),
                                            },
                                    },
                            },
                    },
            }
    );
    avg_sum_r = prepare_expression(avg_sum_r, db, "test_ks", schema.get(), nullptr);
    
    BOOST_REQUIRE_EQUAL(aggregation_depth(avg_sum_r), 2);


    // Now test something with an imbalance
    auto avg_r = function_call{
            .func = functions::function_name("", "avg"),
            .args = {
                    unresolved_identifier{
                            .ident = ::make_shared<column_identifier_raw>("r", false),
                    },
            },
    };
    auto compared = expression(binary_operator(avg_r, oper_t::EQ, avg_sum_r));
    compared = prepare_expression(compared, db, "test_ks", schema.get(), nullptr);
 
    BOOST_REQUIRE_EQUAL(aggregation_depth(compared), 2);
}

static
shared_ptr<functions::function>
make_two_arg_aggregate_function() {
    return make_shared<db::functions::aggregate_function>(db::functions::stateless_aggregate_function{
            .name = functions::function_name("foo", "my_agg"),
            .result_type = int32_type,
            .argument_types = { int32_type, int32_type },
            // THe other fields are important, but not for test_levellize_aggregation_depth
    });
}

BOOST_AUTO_TEST_CASE(test_levellize_aggregation_depth) {
    auto schema = make_simple_test_schema();
    auto [db, db_data] = make_data_dictionary_database(schema);
    // my_agg(sum(r), r))
    auto e = expression(
            function_call{
                    .func = make_two_arg_aggregate_function(),
                    .args = {
                            function_call{
                                    .func = functions::function_name::native_function("sum"),
                                    .args = {
                                            column_value(&schema->regular_column_at(0)),
                                    },
                            },
                            column_value(&schema->regular_column_at(0)),
                    },
            }
    );
    e = prepare_expression(e, db, "test_ks", schema.get(), nullptr);
    BOOST_REQUIRE_EQUAL(aggregation_depth(e), 2);
    e = levellize_aggregation_depth(e, 3); // Note: aggregation_depth(e) == 2 before the call
    BOOST_REQUIRE_EQUAL(aggregation_depth(e), 3);
    // Somewhat fragile, but easiest way to test entire structure
    BOOST_REQUIRE_EQUAL(fmt::format("{:debug}", e), "foo.my_agg(system.sum(system.$$first$$(r)), system.$$first$$(system.$$first$$(r)))");

    // Repeat the test, but for writetime(r) rather than r, to make sure we
    // get first(writetime(r)) rather than writetime(first(r)) (#14715).
    // my_agg(sum(r), writetime(r)))
    auto e2 = expression(
            function_call{
                    .func = make_two_arg_aggregate_function(),
                    .args = {
                            function_call{
                                    .func = functions::function_name::native_function("sum"),
                                    .args = {
                                            column_value(&schema->regular_column_at(0)),
                                    },
                            },
                            column_mutation_attribute{
                                .kind = column_mutation_attribute::attribute_kind::ttl, // conveniently returns int32_type like r
                                .column = column_value(&schema->regular_column_at(0)),
                            },
                    },
            }
    );
    e2 = prepare_expression(e2, db, "test_ks", schema.get(), nullptr);
    BOOST_REQUIRE_EQUAL(aggregation_depth(e2), 2);
    e2 = levellize_aggregation_depth(e2, 3); // Note: aggregation_depth(e) == 2 before the call
    BOOST_REQUIRE_EQUAL(aggregation_depth(e2), 3);
    // Somewhat fragile, but easiest way to test entire structure
    BOOST_REQUIRE_EQUAL(fmt::format("{:debug}", e2), "foo.my_agg(system.sum(system.$$first$$(r)), system.$$first$$(system.$$first$$(TTL(r))))");
}
