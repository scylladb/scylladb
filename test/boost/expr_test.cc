#include "cql3/column_identifier.hh"
#include "cql3/util.hh"
#include "seastar/core/shared_ptr.hh"
#include "types.hh"
#include "types/list.hh"
#include "types/map.hh"
#include <boost/test/tools/old/interface.hpp>
#define BOOST_TEST_MODULE core

#include <boost/test/unit_test.hpp>
#include <utility>
#include "cql3/expr/expression.hh"
#include "utils/overloaded_functor.hh"
#include <cassert>
#include "cql3/query_options.hh"
#include "types/set.hh"
#include "types/user.hh"
#include "test/lib/expr_test_utils.hh"

using namespace cql3;
using namespace cql3::expr;
using namespace cql3::expr::test_utils;

bind_variable new_bind_variable(int bind_index) {
    return bind_variable {
        .bind_index = bind_index,
        .receiver = nullptr
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
    expression::printer p {
        .expr_to_print = e,
        .debug_mode = false
    };
    return format("{}", p);
}

static sstring value_print(const cql3::raw_value& v, const expression& e) {
    return expr_print(constant(v, type_of(e)));
}

static unresolved_identifier make_column(const char* col_name) {
    return unresolved_identifier{::make_shared<column_identifier_raw>(col_name, true)};
}

BOOST_AUTO_TEST_CASE(expr_printer_test) {
    expression col_eq_1234 = binary_operator(
        make_column("col"),
        oper_t::EQ,
        make_int_const(1234)
    );
    BOOST_REQUIRE_EQUAL(expr_print(col_eq_1234), "col = 1234");

    expression token_p1_p2_lt_min_56 = binary_operator(
        token({
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
    BOOST_REQUIRE_EQUAL(expr_print(timestamp_const), "'2011-03-02T03:05:00+0000'");
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
    BOOST_REQUIRE_EQUAL(expr_print(date_const), "'2011-02-03T00:00:00+0000'");
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

BOOST_AUTO_TEST_CASE(evaluate_constant_unset) {
    expression constant_unset = constant::make_unset_value();
    BOOST_REQUIRE_EQUAL(evaluate(constant_unset, evaluation_inputs{}), raw_value::make_unset_value());
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
    BOOST_REQUIRE_THROW(evaluate(list_with_null, evaluation_inputs{}), exceptions::invalid_request_exception);
}

BOOST_AUTO_TEST_CASE(evaluate_list_collection_constructor_with_unset) {
    expression list_with_unset = make_list_constructor(
        {make_int_const(1), constant::make_unset_value(int32_type), make_int_const(3)}, int32_type);
    BOOST_REQUIRE_THROW(evaluate(list_with_unset, evaluation_inputs{}), exceptions::invalid_request_exception);
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

BOOST_AUTO_TEST_CASE(evaluate_set_collection_constructor_with_unset) {
    expression set_with_unset = make_set_constructor(
        {make_int_const(1), constant::make_unset_value(int32_type), make_int_const(3)}, int32_type);
    BOOST_REQUIRE_THROW(evaluate(set_with_unset, evaluation_inputs{}), exceptions::invalid_request_exception);
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

BOOST_AUTO_TEST_CASE(evaluate_map_collection_constructor_with_unset_key) {
    expression map_with_unset_key = make_map_constructor(
        {
            {make_int_const(1), make_int_const(2)},
            {constant::make_unset_value(int32_type), make_int_const(4)},
            {make_int_const(5), make_int_const(6)},
        },
        int32_type, int32_type);
    BOOST_REQUIRE_THROW(evaluate(map_with_unset_key, evaluation_inputs{}), exceptions::invalid_request_exception);
}

BOOST_AUTO_TEST_CASE(evaluate_map_collection_constructor_with_unset_value) {
    expression map_with_unset_value = make_map_constructor(
        {
            {make_int_const(1), make_int_const(2)},
            {make_int_const(3), constant::make_unset_value(int32_type)},
            {make_int_const(5), make_int_const(6)},
        },
        int32_type, int32_type);
    BOOST_REQUIRE_THROW(evaluate(map_with_unset_value, evaluation_inputs{}), exceptions::invalid_request_exception);
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

BOOST_AUTO_TEST_CASE(evaluate_tuple_constructor_with_unset) {
    expression tuple_with_unset =
        make_tuple_constructor({make_int_const(12), constant::make_unset_value(int32_type)}, {int32_type, utf8_type});
    BOOST_REQUIRE_THROW(evaluate(tuple_with_unset, evaluation_inputs{}), exceptions::invalid_request_exception);
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

BOOST_AUTO_TEST_CASE(evaluate_usertype_constructor_with_unset) {
    expression usertype_with_unset = make_usertype_constructor({{"field1", make_int_const(123)},
                                                                {"field2", constant::make_unset_value(utf8_type)},
                                                                {"field3", make_bool_const(true)}});
    BOOST_REQUIRE_THROW(evaluate(usertype_with_unset, evaluation_inputs{}), exceptions::invalid_request_exception);
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
    BOOST_REQUIRE_THROW(evaluate_subscripted(list, constant::make_unset_value(int32_type)),
                        exceptions::invalid_request_exception);
    
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

BOOST_AUTO_TEST_CASE(evaluate_subscripted_list_unset_index) {
    constant list = make_subscript_test_list();
    BOOST_REQUIRE_THROW(evaluate_subscripted(list, constant::make_unset_value(int32_type)),
                        exceptions::invalid_request_exception);
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

    // TODO: Shouldn't this throw an error?
    BOOST_REQUIRE_EQUAL(evaluate_subscripted(list, constant::make_unset_value(int32_type)), raw_value::make_null());
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
    BOOST_REQUIRE_THROW(evaluate_subscripted(map, constant::make_unset_value(int32_type)),
                        exceptions::invalid_request_exception);
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

BOOST_AUTO_TEST_CASE(evaluate_subscripted_map_unset_index) {
    constant map = make_subscript_test_map();
    BOOST_REQUIRE_THROW(evaluate_subscripted(map, constant::make_unset_value(int32_type)),
                        exceptions::invalid_request_exception);
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

    // TODO: Shouldn't this throw an error?
    BOOST_REQUIRE_EQUAL(evaluate_subscripted(map, constant::make_unset_value(int32_type)), raw_value::make_null());
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
        test_schema, {{"pk", make_int_raw(0)}, {"r", raw_value::make_null()}}, {check_value.value});

    switch (expected_validity) {
        case expected_valid:
            BOOST_REQUIRE_EQUAL(evaluate(bind_var, inputs), check_value.value);
            return;
        case expected_invalid:
            BOOST_REQUIRE_THROW(evaluate(bind_var, inputs), exceptions::invalid_request_exception);
            return;
    }
}

BOOST_AUTO_TEST_CASE(evaluate_bind_variable_validates_no_null_in_list) {
    constant list_with_null =
        make_list_const({make_int_const(1), constant::make_null(int32_type), make_int_const(2)}, int32_type);
    check_bind_variable_evaluate(list_with_null, expected_invalid);
}

BOOST_AUTO_TEST_CASE(evaluate_bind_variable_validates_no_unset_in_list) {
    constant list_with_unset =
        make_list_const({make_int_const(1), constant::make_unset_value(int32_type), make_int_const(2)}, int32_type);
    check_bind_variable_evaluate(list_with_unset, expected_invalid);
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

BOOST_AUTO_TEST_CASE(evaluate_bind_variable_validates_no_unset_in_set) {
    constant set_with_unset =
        make_set_const({make_int_const(1), constant::make_unset_value(int32_type), make_int_const(2)}, int32_type);
    check_bind_variable_evaluate(set_with_unset, expected_invalid);
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

BOOST_AUTO_TEST_CASE(evaluate_bind_variable_validates_no_unset_key_in_map) {
    constant map_with_unset_key = make_map_const({{make_int_const(1), make_int_const(2)},
                                                  {constant::make_unset_value(int32_type), make_int_const(4)},
                                                  {make_int_const(5), make_int_const(6)}},
                                                 int32_type, int32_type);
    check_bind_variable_evaluate(map_with_unset_key, expected_invalid);
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

BOOST_AUTO_TEST_CASE(evaluate_bind_variable_validates_no_unset_value_in_map) {
    constant map_with_unset_value = make_map_const({{make_int_const(1), make_int_const(2)},
                                                    {make_int_const(3), constant::make_unset_value(int32_type)},
                                                    {make_int_const(5), make_int_const(6)}},
                                                   int32_type, int32_type);
    check_bind_variable_evaluate(map_with_unset_value, expected_invalid);
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
    check_bind_variable_evaluate(list_with_null, expected_invalid);
}

BOOST_AUTO_TEST_CASE(evaluate_bind_variable_validates_unset_in_lists_recursively) {
    constant list_with_unset = create_nested_list_with_value(constant::make_unset_value(int32_type));
    check_bind_variable_evaluate(list_with_unset, expected_invalid);
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

BOOST_AUTO_TEST_CASE(evaluate_bind_variable_validates_unset_in_sets_recursively) {
    constant set_with_unset = create_nested_set_with_value(constant::make_unset_value(int32_type));
    check_bind_variable_evaluate(set_with_unset, expected_invalid);
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

BOOST_AUTO_TEST_CASE(evaluate_bind_variable_validates_unset_key_in_maps_recursively) {
    constant map_with_unset_key1 =
        create_nested_map_with_key(constant::make_unset_value(int32_type), make_int_const(13));
    check_bind_variable_evaluate(map_with_unset_key1, expected_invalid);

    constant map_with_unset_key2 =
        create_nested_map_with_key(make_int_const(1), constant::make_unset_value(int32_type));
    check_bind_variable_evaluate(map_with_unset_key2, expected_invalid);
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

BOOST_AUTO_TEST_CASE(evaluate_bind_variable_validates_unset_value_in_maps_recursively) {
    constant map_with_unset_value1 =
        create_nested_map_with_value(constant::make_unset_value(int32_type), make_int_const(13));
    check_bind_variable_evaluate(map_with_unset_value1, expected_invalid);

    constant map_with_unset_value2 =
        create_nested_map_with_value(make_int_const(1), constant::make_unset_value(int32_type));
    check_bind_variable_evaluate(map_with_unset_value2, expected_invalid);
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
                  .sub = untyped_constant{.partial_type = untyped_constant::type_class::integer, .raw_text = "123"}};

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
                  .sub = untyped_constant{.partial_type = untyped_constant::type_class::boolean, .raw_text = "true"}};

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
                  .sub = untyped_constant{.partial_type = untyped_constant::type_class::integer, .raw_text = "123"}};

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
                  .sub = untyped_constant{.partial_type = untyped_constant::type_class::boolean, .raw_text = "true"}};

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
                  .sub = untyped_constant{.partial_type = untyped_constant::type_class::integer, .raw_text = "123"}};

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

    expression tok =
        token({::make_shared<column_identifier_raw>("p1", true), ::make_shared<column_identifier_raw>("p2", true),
               ::make_shared<column_identifier_raw>("p3", true)});

    expression prepared = prepare_expression(tok, db, "test_ks", table_schema.get(), nullptr);

    expression expected = token({column_value(table_schema->get_column_definition("p1")),
                                 column_value(table_schema->get_column_definition("p2")),
                                 column_value(table_schema->get_column_definition("p3"))});

    BOOST_REQUIRE_EQUAL(prepared, expected);
}

// prepare_expression(token) doesn't validate its arguments,
// validation is done in a different place
BOOST_AUTO_TEST_CASE(prepare_token_no_args) {
    schema_ptr table_schema = schema_builder("test_ks", "test_cf")
                                  .with_column("p1", int32_type, column_kind::partition_key)
                                  .with_column("p2", int32_type, column_kind::partition_key)
                                  .with_column("p3", int32_type, column_kind::partition_key)
                                  .build();
    auto [db, db_data] = make_data_dictionary_database(table_schema);

    expression tok = token(std::vector<expression>());

    expression prepared = prepare_expression(tok, db, "test_ks", table_schema.get(), nullptr);

    BOOST_REQUIRE_EQUAL(tok, prepared);
}

BOOST_AUTO_TEST_CASE(prepare_cast_int_int) {
    schema_ptr table_schema = make_simple_test_schema();
    auto [db, db_data] = make_data_dictionary_database(table_schema);

    expression cast_expr =
        cast{.arg = untyped_constant{.partial_type = untyped_constant::type_class::integer, .raw_text = "123"},
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
        cast{.arg = untyped_constant{.partial_type = untyped_constant::type_class::integer, .raw_text = "123"},
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
        cast{.arg = untyped_constant{.partial_type = untyped_constant::type_class::string, .raw_text = "123"},
             .type = cql3_type::raw::from(short_type)};

    ::lw_shared_ptr<column_specification> receiver = make_receiver(short_type);

    BOOST_REQUIRE_THROW(prepare_expression(cast_expr, db, "test_ks", table_schema.get(), receiver),
                        exceptions::invalid_request_exception);
}

BOOST_AUTO_TEST_CASE(prepare_null) {
    schema_ptr table_schema = make_simple_test_schema();
    auto [db, db_data] = make_data_dictionary_database(table_schema);

    expression null_expr = null{};

    expression prepared = prepare_expression(null_expr, db, "test_ks", table_schema.get(), make_receiver(int32_type));
    expression expected = constant::make_null(int32_type);
    BOOST_REQUIRE_EQUAL(prepared, expected);
}

// null can't be prepared without a receiver because we are unable to infer the type.
BOOST_AUTO_TEST_CASE(prepare_null_no_type_fails) {
    schema_ptr table_schema = make_simple_test_schema();
    auto [db, db_data] = make_data_dictionary_database(table_schema);

    expression null_expr = null{};
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

    expression untyped = untyped_constant{.partial_type = untyped_constant::type_class::integer, .raw_text = "1337"};

    // Can't infer type
    BOOST_REQUIRE_THROW(prepare_expression(untyped, db, "test_ks", table_schema.get(), nullptr),
                        exceptions::invalid_request_exception);
}

BOOST_AUTO_TEST_CASE(prepare_untyped_constant_bool) {
    schema_ptr table_schema = make_simple_test_schema();
    auto [db, db_data] = make_data_dictionary_database(table_schema);

    expression untyped = untyped_constant{.partial_type = untyped_constant::type_class::boolean, .raw_text = "true"};

    expression prepared = prepare_expression(untyped, db, "test_ks", table_schema.get(), make_receiver(boolean_type));
    expression expected = make_bool_const(true);

    BOOST_REQUIRE_EQUAL(prepared, expected);
}

BOOST_AUTO_TEST_CASE(prepare_untyped_constant_int8) {
    schema_ptr table_schema = make_simple_test_schema();
    auto [db, db_data] = make_data_dictionary_database(table_schema);

    expression untyped = untyped_constant{.partial_type = untyped_constant::type_class::integer, .raw_text = "13"};

    expression prepared = prepare_expression(untyped, db, "test_ks", table_schema.get(), make_receiver(byte_type));
    expression expected = make_tinyint_const(13);

    BOOST_REQUIRE_EQUAL(prepared, expected);
}

BOOST_AUTO_TEST_CASE(prepare_untyped_constant_int16) {
    schema_ptr table_schema = make_simple_test_schema();
    auto [db, db_data] = make_data_dictionary_database(table_schema);

    expression untyped = untyped_constant{.partial_type = untyped_constant::type_class::integer, .raw_text = "1337"};

    expression prepared = prepare_expression(untyped, db, "test_ks", table_schema.get(), make_receiver(short_type));
    expression expected = make_smallint_const(1337);

    BOOST_REQUIRE_EQUAL(prepared, expected);
}

BOOST_AUTO_TEST_CASE(prepare_untyped_constant_int32) {
    schema_ptr table_schema = make_simple_test_schema();
    auto [db, db_data] = make_data_dictionary_database(table_schema);

    expression untyped =
        untyped_constant{.partial_type = untyped_constant::type_class::integer, .raw_text = "13377331"};

    expression prepared = prepare_expression(untyped, db, "test_ks", table_schema.get(), make_receiver(int32_type));
    expression expected = make_int_const(13377331);

    BOOST_REQUIRE_EQUAL(prepared, expected);
}

BOOST_AUTO_TEST_CASE(prepare_untyped_constant_int64) {
    schema_ptr table_schema = make_simple_test_schema();
    auto [db, db_data] = make_data_dictionary_database(table_schema);

    expression untyped =
        untyped_constant{.partial_type = untyped_constant::type_class::integer, .raw_text = "1337733113377331"};

    expression prepared = prepare_expression(untyped, db, "test_ks", table_schema.get(), make_receiver(long_type));
    expression expected = make_bigint_const(1337733113377331);

    BOOST_REQUIRE_EQUAL(prepared, expected);
}

BOOST_AUTO_TEST_CASE(prepare_untyped_constant_text) {
    schema_ptr table_schema = make_simple_test_schema();
    auto [db, db_data] = make_data_dictionary_database(table_schema);

    expression untyped =
        untyped_constant{.partial_type = untyped_constant::type_class::string, .raw_text = "scylla_is_the_best"};

    expression prepared = prepare_expression(untyped, db, "test_ks", table_schema.get(), make_receiver(utf8_type));
    expression expected = make_text_const("scylla_is_the_best");

    BOOST_REQUIRE_EQUAL(prepared, expected);
}

BOOST_AUTO_TEST_CASE(prepare_untyped_constant_bad_int) {
    schema_ptr table_schema = make_simple_test_schema();
    auto [db, db_data] = make_data_dictionary_database(table_schema);

    expression untyped =
        untyped_constant{.partial_type = untyped_constant::type_class::integer, .raw_text = "not_integer_text"};

    BOOST_REQUIRE_THROW(prepare_expression(untyped, db, "test_ks", table_schema.get(), make_receiver(int32_type)),
                        exceptions::invalid_request_exception);
}
