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

using namespace cql3;
using namespace cql3::expr;

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

static constant make_int(int value) {
    return constant(raw_value::make_value(int32_type->decompose(value)), int32_type);
}

static unresolved_identifier make_column(const char* col_name) {
    return unresolved_identifier{::make_shared<column_identifier_raw>(col_name, true)};
}

BOOST_AUTO_TEST_CASE(expr_printer_test) {
    expression col_eq_1234 = binary_operator(
        make_column("col"),
        oper_t::EQ,
        make_int(1234)
    );
    BOOST_REQUIRE_EQUAL(expr_print(col_eq_1234), "col = 1234");

    expression token_p1_p2_lt_min_56 = binary_operator(
        token({
            unresolved_identifier{::make_shared<column_identifier_raw>("p1", true)},
            unresolved_identifier{::make_shared<column_identifier_raw>("p2", true)},
        }),
        oper_t::LT,
        make_int(-56)
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
        .elements = {make_int(13), make_int(45), make_int(90)},
        .type = list_type_impl::get_instance(int32_type, true)
    };
    BOOST_REQUIRE_EQUAL(expr_print(int_list), "[13, 45, 90]");

    collection_constructor frozen_int_list {
        .style = collection_constructor::style_type::list,
        .elements = {make_int(13), make_int(45), make_int(90)},
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
        .elements = {make_int(13), make_int(45), make_int(90)},
        .type = set_type_impl::get_instance(int32_type, true)
    };
    BOOST_REQUIRE_EQUAL(expr_print(int_set), "{13, 45, 90}");

    collection_constructor frozen_int_set {
        .style = collection_constructor::style_type::set,
        .elements = {make_int(13), make_int(45), make_int(90)},
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
                .elements = {make_int(12), make_int(34)}
            },
            tuple_constructor {
                .elements = {make_int(56), make_int(78)}
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
        .elements = {make_int(456), make_int(789)},
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
    user_type_elements.emplace(field_a, make_int(333));
    user_type_elements.emplace(field_b, make_int(666));
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
        .elements = {make_int(13), make_int(45), make_int(90)},
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
    BOOST_REQUIRE_EQUAL(boolean_factors(constant::make_bool(true)), std::vector<expression>({constant::make_bool(true)}));

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
