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
#include "cql3/selection/selection.hh"

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

using column_values = std::map<sstring, raw_value>;

static raw_value make_bool_val(bool val) {
    return raw_value::make_value(boolean_type->decompose(val));
}

static raw_value make_int_val(int32_t val) {
    return raw_value::make_value(int32_type->decompose(val));
}

static raw_value make_text_val(const sstring_view& text) {
    return raw_value::make_value(utf8_type->decompose(text));
}

static raw_value make_int_list_val(const std::vector<int32_t>& list_elems) {
    list_type_impl::native_type list;
    for (const int32_t& elem : list_elems) {
        list.push_back(data_value(elem));
    }
    data_type list_type = list_type_impl::get_instance(int32_type, true);
    bytes list_bytes = list_type->decompose(make_list_value(list_type, list));
    return raw_value::make_value(std::move(list_bytes));
}

static raw_value make_int_set_val(std::vector<int32_t> set_elems) {
    std::sort(set_elems.begin(), set_elems.end());

    // Set and list have the same binary representation
    return make_int_list_val(set_elems);
}

static raw_value make_int_int_map_val(const std::vector<std::pair<int32_t, int32_t>>& map_elems) {
    map_type_impl::native_type map;
    for (const std::pair<int32_t, int32_t>& key_value_elem : map_elems) {
        map.push_back(key_value_elem);
    }
    data_type map_type = map_type_impl::get_instance(int32_type, int32_type, true);
    bytes map_bytes = map_type->decompose(make_map_value(map_type, map));
    return raw_value::make_value(std::move(map_bytes));
}

static raw_value make_tuple_val(const std::vector<constant>& tuple_vals) {
    tuple_type_impl::native_type tuple;
    for (const constant& val : tuple_vals) {
        if (val.value.is_null()) {
            tuple.push_back(data_value::make_null(val.type));
            continue;
        }
        if (val.value.is_unset_value()) {
            throw std::runtime_error("make_tuple_val - unset value is not allowed");
        }
        if (val.value.is_empty_value()) {
            throw std::runtime_error("make_tuple_val - empty value is not supported");
        }
        data_value cur_val = val.value.view().with_value(
            [&](const FragmentedView auto& val_bytes) { return val.type->deserialize(val_bytes); });
        tuple.push_back(cur_val);
    }
    data_type tuple_type = tuple_type_impl::get_instance({int32_type, boolean_type});
    bytes tuple_bytes = tuple_type->decompose(make_tuple_value(tuple_type, tuple));
    return raw_value::make_value(std::move(tuple_bytes));
}

static raw_value make_int_bool_usertype_val(int32_t int_val, bool bool_val) {
    data_type usertype_type = user_type_impl::get_instance("test_ks", "test_type", {"int_field", "bool_field"},
                                                           {int32_type, boolean_type}, true);
    bytes usertype_bytes =
        usertype_type->decompose(make_user_value(usertype_type, {data_value(int_val), data_value(bool_val)}));
    return raw_value::make_value(std::move(usertype_bytes));
}

struct evaluation_inputs_data {
    std::vector<bytes> partition_key;
    std::vector<bytes> clustering_key;
    std::vector<managed_bytes_opt> static_and_regular_columns;
    ::shared_ptr<selection::selection> selection;
    query_options options;
};

static std::pair<evaluation_inputs, std::unique_ptr<evaluation_inputs_data>> make_evaluation_inputs(
    const schema_ptr& table_schema,
    const column_values& column_vals,
    const std::vector<raw_value>& bind_marker_values = {}) {
    auto throw_error = [&](const auto&... fmt_args) -> sstring {
        sstring error_msg = format(fmt_args...);
        sstring final_msg = format("make_evaluation_inputs error: {}. (table_schema: {}, column_vals: {})", error_msg,
                                   *table_schema, column_vals);
        throw std::runtime_error(final_msg);
    };

    auto get_col_val = [&](const column_definition& col) -> const raw_value& {
        auto col_value_iter = column_vals.find(col.name_as_text());
        if (col_value_iter == column_vals.end()) {
            throw_error("no value for column {}", col.name_as_text());
        }
        return col_value_iter->second;
    };

    std::vector<bytes> partition_key;
    for (const column_definition& pk_col : table_schema->partition_key_columns()) {
        const raw_value& col_value = get_col_val(pk_col);

        if (col_value.is_null()) {
            throw_error("Passed NULL as value for {}. This is not allowed for partition key columns.",
                        pk_col.name_as_text());
        }
        if (col_value.is_unset_value()) {
            throw_error("Passed UNSET_VALUE as value for {}. This is not allowed for partition key columns.",
                        pk_col.name_as_text());
        }
        partition_key.push_back(raw_value(col_value).to_bytes());
    }

    std::vector<bytes> clustering_key;
    for (const column_definition& ck_col : table_schema->clustering_key_columns()) {
        const raw_value& col_value = get_col_val(ck_col);

        if (col_value.is_null()) {
            throw_error("Passed NULL as value for {}. This is not allowed for clustering key columns.",
                        ck_col.name_as_text());
        }
        if (col_value.is_unset_value()) {
            throw_error("Passed UNSET_VALUE as value for {}. This is not allowed for clustering key columns.",
                        ck_col.name_as_text());
        }
        clustering_key.push_back(raw_value(col_value).to_bytes());
    }

    std::vector<const column_definition*> selection_columns;
    for (const column_definition& cdef : table_schema->regular_columns()) {
        selection_columns.push_back(&cdef);
    }
    for (const column_definition& cdef : table_schema->static_columns()) {
        selection_columns.push_back(&cdef);
    }

    ::shared_ptr<selection::selection> selection =
        cql3::selection::selection::for_columns(table_schema, std::move(selection_columns));
    std::vector<managed_bytes_opt> static_and_regular_columns(table_schema->regular_columns_count() +
                                                              table_schema->static_columns_count());

    for (const column_definition& col : table_schema->regular_columns()) {
        const raw_value& col_value = get_col_val(col);
        if (col_value.is_unset_value()) {
            throw_error("Passed UNSET_VALUE as value for {}. This is not allowed.", col.name_as_text());
        }
        int32_t index = selection->index_of(col);
        static_and_regular_columns[index] = raw_value(col_value).to_managed_bytes_opt();
    }

    for (const column_definition& col : table_schema->static_columns()) {
        const raw_value& col_value = get_col_val(col);
        if (col_value.is_unset_value()) {
            throw_error("Passed UNSET_VALUE as value for {}. This is not allowed.", col.name_as_text());
        }
        int32_t index = selection->index_of(col);
        static_and_regular_columns[index] = raw_value(col_value).to_managed_bytes_opt();
    }

    query_options options(default_cql_config, db::consistency_level::ONE, std::nullopt, bind_marker_values, true,
                          query_options::specific_options::DEFAULT, cql_serialization_format::internal());

    std::unique_ptr<evaluation_inputs_data> data = std::make_unique<evaluation_inputs_data>(
        evaluation_inputs_data{.partition_key = std::move(partition_key),
                               .clustering_key = std::move(clustering_key),
                               .static_and_regular_columns = std::move(static_and_regular_columns),
                               .selection = std::move(selection),
                               .options = std::move(options)});

    evaluation_inputs inputs{.partition_key = &data->partition_key,
                             .clustering_key = &data->clustering_key,
                             .static_and_regular_columns = &data->static_and_regular_columns,
                             .selection = data->selection.get(),
                             .options = &data->options};

    return std::pair(std::move(inputs), std::move(data));
}

// Empty expression defaults to an empty conjunction, which evaluates to true.
BOOST_AUTO_TEST_CASE(evaluate_empty_expression) {
    expression empty;
    raw_value val = evaluate(empty, evaluation_inputs{});
    BOOST_REQUIRE_EQUAL(val, make_bool_val(true));
}

BOOST_AUTO_TEST_CASE(evaluate_constant_bool) {
    expression bool_true = constant::make_bool(true);
    BOOST_REQUIRE_EQUAL(evaluate(bool_true, evaluation_inputs{}), make_bool_val(true));

    expression bool_false = constant::make_bool(false);
    BOOST_REQUIRE_EQUAL(evaluate(bool_false, evaluation_inputs{}), make_bool_val(false));
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
    BOOST_REQUIRE_EQUAL(evaluate(constant_empty_text, evaluation_inputs{}), make_text_val(""));
}

BOOST_AUTO_TEST_CASE(evaluate_partition_key_column) {
    schema_ptr test_schema = make_simple_test_schema();
    auto [inputs, inputs_data] = make_evaluation_inputs(test_schema, {
                                                                         {"pk", make_int_val(1)},
                                                                         {"ck", make_int_val(2)},
                                                                         {"r", make_int_val(3)},
                                                                         {"s", make_int_val(4)},
                                                                     });
    expression pk_val = column_value(test_schema->get_column_definition("pk"));
    raw_value val = evaluate(pk_val, inputs);
    BOOST_REQUIRE_EQUAL(val, make_int_val(1));
}

BOOST_AUTO_TEST_CASE(evaluate_clustering_key_column) {
    schema_ptr test_schema = make_simple_test_schema();
    auto [inputs, inputs_data] = make_evaluation_inputs(test_schema, {
                                                                         {"pk", make_int_val(1)},
                                                                         {"ck", make_int_val(2)},
                                                                         {"r", make_int_val(3)},
                                                                         {"s", make_int_val(4)},
                                                                     });
    expression ck_val = column_value(test_schema->get_column_definition("ck"));
    raw_value val = evaluate(ck_val, inputs);
    BOOST_REQUIRE_EQUAL(val, make_int_val(2));
}

BOOST_AUTO_TEST_CASE(evaluate_regular_column) {
    schema_ptr test_schema = make_simple_test_schema();
    auto [inputs, inputs_data] = make_evaluation_inputs(test_schema, {
                                                                         {"pk", make_int_val(1)},
                                                                         {"ck", make_int_val(2)},
                                                                         {"r", make_int_val(3)},
                                                                         {"s", make_int_val(4)},
                                                                     });
    expression r_val = column_value(test_schema->get_column_definition("r"));
    raw_value val = evaluate(r_val, inputs);
    BOOST_REQUIRE_EQUAL(val, make_int_val(3));
}

BOOST_AUTO_TEST_CASE(evaluate_static_column) {
    schema_ptr test_schema = make_simple_test_schema();
    auto [inputs, inputs_data] = make_evaluation_inputs(test_schema, {
                                                                         {"pk", make_int_val(1)},
                                                                         {"ck", make_int_val(2)},
                                                                         {"r", make_int_val(3)},
                                                                         {"s", make_int_val(4)},
                                                                     });
    BOOST_REQUIRE(test_schema->get_column_definition("s") != nullptr);
    expression s_val = column_value(test_schema->get_column_definition("s"));
    raw_value val = evaluate(s_val, inputs);
    BOOST_REQUIRE_EQUAL(val, make_int_val(4));
}

BOOST_AUTO_TEST_CASE(evaluate_bind_variable) {
    schema_ptr test_schema = make_simple_test_schema();
    auto [inputs, inputs_data] = make_evaluation_inputs(test_schema,
                                                        {
                                                            {"pk", make_int_val(1)},
                                                            {"ck", make_int_val(2)},
                                                            {"r", make_int_val(3)},
                                                            {"s", make_int_val(4)},
                                                        },
                                                        {make_int_val(123)});

    expression bind_variable = cql3::expr::bind_variable{
        .bind_index = 0,
        .receiver = ::make_lw_shared<column_specification>(
            "test_ks", "test_cf", ::make_shared<cql3::column_identifier>("bind_var_0", true), int32_type)};

    raw_value val = evaluate(bind_variable, inputs);
    BOOST_REQUIRE_EQUAL(val, make_int_val(123));
}

BOOST_AUTO_TEST_CASE(evaluate_two_bind_variables) {
    schema_ptr test_schema = make_simple_test_schema();
    auto [inputs, inputs_data] = make_evaluation_inputs(test_schema,
                                                        {
                                                            {"pk", make_int_val(1)},
                                                            {"ck", make_int_val(2)},
                                                            {"r", make_int_val(3)},
                                                            {"s", make_int_val(4)},
                                                        },
                                                        {make_int_val(123), make_int_val(456)});

    expression bind_variable0 = cql3::expr::bind_variable{
        .bind_index = 0,
        .receiver = ::make_lw_shared<column_specification>(
            "test_ks", "test_cf", ::make_shared<cql3::column_identifier>("bind_var_0", true), int32_type)};

    expression bind_variable1 = cql3::expr::bind_variable{
        .bind_index = 1,
        .receiver = ::make_lw_shared<column_specification>(
            "test_ks", "test_cf", ::make_shared<cql3::column_identifier>("bind_var_1", true), int32_type)};

    raw_value val0 = evaluate(bind_variable0, inputs);
    BOOST_REQUIRE_EQUAL(val0, make_int_val(123));

    raw_value val1 = evaluate(bind_variable1, inputs);
    BOOST_REQUIRE_EQUAL(val1, make_int_val(456));
}

BOOST_AUTO_TEST_CASE(evaluate_subscripted_list) {
    schema_ptr table_schema =
        schema_builder("test_ks", "test_cf")
            .with_column("pk", int32_type, column_kind::partition_key)
            .with_column("l", list_type_impl::get_instance(int32_type, true), column_kind::regular_column)
            .build();

    const column_definition* list_col = table_schema->get_column_definition("l");

    raw_value list_val = make_int_list_val({357, 468, 579});
    expression sub = subscript{
        .val = column_value(list_col), .sub = make_int(1), .type = list_type_impl::get_instance(int32_type, true)};

    auto inputs_pair = make_evaluation_inputs(table_schema, {{"pk", make_int_val(0)}, {"l", list_val}});

    auto evaluate_subscripted = [&](const raw_value& sub_val) -> raw_value {
        expression sub = subscript{.val = column_value(list_col),
                                   .sub = constant(sub_val, int32_type),
                                   .type = set_type_impl::get_instance(int32_type, true)};
        return evaluate(sub, inputs_pair.first);
    };

    BOOST_REQUIRE_EQUAL(evaluate_subscripted(make_int_val(0)), make_int_val(357));
    BOOST_REQUIRE_EQUAL(evaluate_subscripted(make_int_val(1)), make_int_val(468));
    BOOST_REQUIRE_EQUAL(evaluate_subscripted(make_int_val(2)), make_int_val(579));
    BOOST_REQUIRE_EQUAL(evaluate_subscripted(make_int_val(-10000000)), raw_value::make_null());
    BOOST_REQUIRE_EQUAL(evaluate_subscripted(make_int_val(4)), raw_value::make_null());
    BOOST_REQUIRE_EQUAL(evaluate_subscripted(make_int_val(10000000)), raw_value::make_null());
    BOOST_REQUIRE_EQUAL(evaluate_subscripted(raw_value::make_null()), raw_value::make_null());
    BOOST_REQUIRE_THROW(evaluate_subscripted(raw_value::make_unset_value()), exceptions::invalid_request_exception);
    BOOST_REQUIRE_THROW(evaluate_subscripted(raw_value::make_value(bytes())), empty_value_exception);
}

BOOST_AUTO_TEST_CASE(evaluate_subscripted_map) {
    schema_ptr table_schema =
        schema_builder("test_ks", "test_cf")
            .with_column("pk", int32_type, column_kind::partition_key)
            .with_column("m", map_type_impl::get_instance(int32_type, int32_type, true), column_kind::regular_column)
            .build();

    const column_definition* map_col = table_schema->get_column_definition("m");

    raw_value map_val = make_int_int_map_val({{1, 2}, {3, 4}, {5, 6}});

    auto inputs_pair = make_evaluation_inputs(table_schema, {{"pk", make_int_val(0)}, {"m", map_val}});

    auto evaluate_subscripted = [&](const raw_value& sub_val) -> raw_value {
        expression sub = subscript{.val = column_value(map_col),
                                   .sub = constant(sub_val, int32_type),
                                   .type = set_type_impl::get_instance(int32_type, true)};
        return evaluate(sub, inputs_pair.first);
    };

    BOOST_REQUIRE_EQUAL(evaluate_subscripted(make_int_val(1)), make_int_val(2));
    BOOST_REQUIRE_EQUAL(evaluate_subscripted(make_int_val(3)), make_int_val(4));
    BOOST_REQUIRE_EQUAL(evaluate_subscripted(make_int_val(5)), make_int_val(6));
    BOOST_REQUIRE_EQUAL(evaluate_subscripted(make_int_val(-10000000)), raw_value::make_null());
    BOOST_REQUIRE_EQUAL(evaluate_subscripted(make_int_val(4)), raw_value::make_null());
    BOOST_REQUIRE_EQUAL(evaluate_subscripted(make_int_val(10000000)), raw_value::make_null());
    BOOST_REQUIRE_EQUAL(evaluate_subscripted(raw_value::make_null()), raw_value::make_null());
    BOOST_REQUIRE_THROW(evaluate_subscripted(raw_value::make_unset_value()), exceptions::invalid_request_exception);
    BOOST_REQUIRE_EQUAL(evaluate_subscripted(raw_value::make_value(bytes())), raw_value::make_null());
}

BOOST_AUTO_TEST_CASE(evaluate_conjunction) {
    BOOST_REQUIRE_EQUAL(evaluate(expression(conjunction{.children = {}}), evaluation_inputs{}), make_bool_val(true));

    BOOST_REQUIRE_EQUAL(
        evaluate(expression(conjunction{.children = {constant::make_bool(false)}}), evaluation_inputs{}),
        make_bool_val(false));

    BOOST_REQUIRE_EQUAL(evaluate(expression(conjunction{.children = {constant::make_bool(true)}}), evaluation_inputs{}),
                        make_bool_val(true));

    BOOST_REQUIRE_EQUAL(
        evaluate(expression(conjunction{.children = {constant::make_bool(false), constant::make_bool(false)}}),
                 evaluation_inputs{}),
        make_bool_val(false));

    BOOST_REQUIRE_EQUAL(
        evaluate(expression(conjunction{.children = {constant::make_bool(false), constant::make_bool(true)}}),
                 evaluation_inputs{}),
        make_bool_val(false));

    BOOST_REQUIRE_EQUAL(
        evaluate(expression(conjunction{.children = {constant::make_bool(true), constant::make_bool(false)}}),
                 evaluation_inputs{}),
        make_bool_val(false));

    BOOST_REQUIRE_EQUAL(
        evaluate(expression(conjunction{.children = {constant::make_bool(true), constant::make_bool(true)}}),
                 evaluation_inputs{}),
        make_bool_val(true));

    BOOST_REQUIRE_EQUAL(
        evaluate(expression(conjunction{
                     .children = {constant::make_bool(true), constant::make_bool(true), constant::make_bool(false)},
                 }),
                 evaluation_inputs{}),
        make_bool_val(false));

    BOOST_REQUIRE_EQUAL(
        evaluate(expression(conjunction{.children = {constant::make_bool(true), constant::make_bool(true),
                                                     constant::make_bool(true), constant::make_bool(true)}}),
                 evaluation_inputs{}),
        make_bool_val(true));

    BOOST_REQUIRE_EQUAL(
        evaluate(expression(conjunction{
                     .children = {constant::make_bool(false), constant::make_bool(true), constant::make_bool(true)}}),
                 evaluation_inputs{}),
        make_bool_val(false));
}

BOOST_AUTO_TEST_CASE(evaluate_list_collection_constructor) {
    data_type list_type = list_type_impl::get_instance(int32_type, true);

    expression empty_list =
        collection_constructor{.style = collection_constructor::style_type::list, .elements = {}, .type = list_type};
    BOOST_REQUIRE_EQUAL(evaluate(empty_list, evaluation_inputs{}), make_int_list_val({}));

    expression list = collection_constructor{.style = collection_constructor::style_type::list,
                                             .elements = {make_int(1), make_int(2), make_int(3)},
                                             .type = list_type};
    BOOST_REQUIRE_EQUAL(evaluate(list, evaluation_inputs{}), make_int_list_val({1, 2, 3}));
}

BOOST_AUTO_TEST_CASE(evaluate_set_collection_constructor) {
    data_type set_type = set_type_impl::get_instance(int32_type, true);

    expression empty_set =
        collection_constructor{.style = collection_constructor::style_type::set, .elements = {}, .type = set_type};
    BOOST_REQUIRE_EQUAL(evaluate(empty_set, evaluation_inputs{}), make_int_set_val({}));

    expression set = collection_constructor{.style = collection_constructor::style_type::set,
                                            .elements = {make_int(1), make_int(2), make_int(3)},
                                            .type = set_type};
    BOOST_REQUIRE_EQUAL(evaluate(set, evaluation_inputs{}), make_int_set_val({1, 2, 3}));
}

BOOST_AUTO_TEST_CASE(evaluate_map_collection_constructor) {
    data_type map_type = map_type_impl::get_instance(int32_type, int32_type, true);

    expression empty_map =
        collection_constructor{.style = collection_constructor::style_type::map, .elements = {}, .type = map_type};
    BOOST_REQUIRE_EQUAL(evaluate(empty_map, evaluation_inputs{}), make_int_int_map_val({}));

    expression map = collection_constructor{.style = collection_constructor::style_type::map,
                                            .elements = {tuple_constructor{.elements = {make_int(1), make_int(2)}},
                                                         tuple_constructor{.elements = {make_int(3), make_int(4)}},
                                                         tuple_constructor{.elements = {make_int(5), make_int(6)}}},
                                            .type = map_type};
    BOOST_REQUIRE_EQUAL(evaluate(map, evaluation_inputs{}), make_int_int_map_val({{1, 2}, {3, 4}, {5, 6}}));
}

BOOST_AUTO_TEST_CASE(evaluate_tuple_constructor) {
    data_type tuple_type = tuple_type_impl::get_instance({int32_type, boolean_type});

    expression empty_tuple = tuple_constructor{
        .elements = {},
        .type = tuple_type,
    };
    BOOST_REQUIRE_EQUAL(evaluate(empty_tuple, evaluation_inputs{}), raw_value::make_value(bytes()));

    // It's possible to have tuples with only some values present
    expression single_elem_tuple = tuple_constructor{.elements = {make_int(123)}, .type = tuple_type};
    BOOST_REQUIRE_EQUAL(evaluate(single_elem_tuple, evaluation_inputs{}), make_tuple_val({make_int(123)}));

    // Tuples can contain nulls
    expression null_tuple =
        tuple_constructor{.elements = {constant::make_null(int32_type), constant::make_bool(12)}, .type = tuple_type};
    BOOST_REQUIRE_EQUAL(evaluate(null_tuple, evaluation_inputs{}),
                        make_tuple_val({constant::make_null(int32_type), constant::make_bool(true)}));

    expression tuple = tuple_constructor{.elements = {make_int(123), constant::make_bool(true)}, .type = tuple_type};
    BOOST_REQUIRE_EQUAL(evaluate(tuple, evaluation_inputs{}),
                        make_tuple_val({make_int(123), constant::make_bool(true)}));
}

BOOST_AUTO_TEST_CASE(evaluate_usertype_constructor) {
    data_type usertype_type = user_type_impl::get_instance("test_ks", "test_type", {"int_field", "bool_field"},
                                                           {int32_type, boolean_type}, true);

    expression type =
        usertype_constructor{.elements = {{column_identifier("int_field", true), make_int(321)},
                                          {column_identifier("bool_field", true), constant::make_bool(false)}},
                             .type = usertype_type};
    BOOST_REQUIRE_EQUAL(evaluate(type, evaluation_inputs{}),
                        make_tuple_val({make_int(321), constant::make_bool(false)}));

    // user types can contain nulls
    expression null_type =
        usertype_constructor{.elements = {{column_identifier("int_field", true), constant::make_null(int32_type)},
                                          {column_identifier("bool_field", true), constant::make_bool(false)}},
                             .type = usertype_type};
    BOOST_REQUIRE_EQUAL(evaluate(null_type, evaluation_inputs{}),
                        make_tuple_val({constant::make_null(int32_type), constant::make_bool(false)}));

    // evaluate() doesn't support evaluating user type values without some fields present.
    // Missing fields are filled with nulls during prepare(), and then evaluate doesn't need to think about them.
    // Trying to pass a usertype_constructor without some fields present results in on_internal_error.
}

BOOST_AUTO_TEST_CASE(evaluate_binary_operator_eq) {
    expression true_eq_binop = binary_operator(make_int(1), oper_t::EQ, make_int(1));
    BOOST_REQUIRE_EQUAL(evaluate(true_eq_binop, evaluation_inputs{}), make_bool_val(true));

    expression false_eq_binop = binary_operator(make_int(1), oper_t::EQ, make_int(2));
    BOOST_REQUIRE_EQUAL(evaluate(false_eq_binop, evaluation_inputs{}), make_bool_val(false));
}

BOOST_AUTO_TEST_CASE(evaluate_binary_operator_neq) {
    expression true_neq_binop = binary_operator(make_int(1), oper_t::NEQ, make_int(1000));
    BOOST_REQUIRE_EQUAL(evaluate(true_neq_binop, evaluation_inputs{}), make_bool_val(true));

    expression false_neq_binop = binary_operator(make_int(2), oper_t::NEQ, make_int(2));
    BOOST_REQUIRE_EQUAL(evaluate(false_neq_binop, evaluation_inputs{}), make_bool_val(false));
}

BOOST_AUTO_TEST_CASE(evaluate_binary_operator_lt) {
    expression true_lt_binop = binary_operator(make_int(1), oper_t::LT, make_int(2));
    BOOST_REQUIRE_EQUAL(evaluate(true_lt_binop, evaluation_inputs{}), make_bool_val(true));

    expression false_lt_binop = binary_operator(make_int(10), oper_t::LT, make_int(2));
    BOOST_REQUIRE_EQUAL(evaluate(false_lt_binop, evaluation_inputs{}), make_bool_val(false));

    expression false_lt_binop2 = binary_operator(make_int(2), oper_t::LT, make_int(2));
    BOOST_REQUIRE_EQUAL(evaluate(false_lt_binop2, evaluation_inputs{}), make_bool_val(false));
}

BOOST_AUTO_TEST_CASE(evaluate_binary_operator_lte) {
    expression true_lte_binop = binary_operator(make_int(1), oper_t::LTE, make_int(2));
    BOOST_REQUIRE_EQUAL(evaluate(true_lte_binop, evaluation_inputs{}), make_bool_val(true));

    expression true_lte_binop2 = binary_operator(make_int(12), oper_t::LTE, make_int(12));
    BOOST_REQUIRE_EQUAL(evaluate(true_lte_binop2, evaluation_inputs{}), make_bool_val(true));

    expression false_lte_binop = binary_operator(make_int(123), oper_t::LTE, make_int(2));
    BOOST_REQUIRE_EQUAL(evaluate(false_lte_binop, evaluation_inputs{}), make_bool_val(false));
}

BOOST_AUTO_TEST_CASE(evaluate_binary_operator_gt) {
    expression true_gt_binop = binary_operator(make_int(2), oper_t::GT, make_int(1));
    BOOST_REQUIRE_EQUAL(evaluate(true_gt_binop, evaluation_inputs{}), make_bool_val(true));

    expression false_gt_binop = binary_operator(make_int(1), oper_t::GT, make_int(2));
    BOOST_REQUIRE_EQUAL(evaluate(false_gt_binop, evaluation_inputs{}), make_bool_val(false));

    expression false_gt_binop2 = binary_operator(make_int(2), oper_t::GT, make_int(2));
    BOOST_REQUIRE_EQUAL(evaluate(false_gt_binop2, evaluation_inputs{}), make_bool_val(false));
}

BOOST_AUTO_TEST_CASE(evaluate_binary_operator_gte) {
    expression true_gte_binop = binary_operator(make_int(20), oper_t::GTE, make_int(10));
    BOOST_REQUIRE_EQUAL(evaluate(true_gte_binop, evaluation_inputs{}), make_bool_val(true));

    expression true_gte_binop2 = binary_operator(make_int(10), oper_t::GTE, make_int(10));
    BOOST_REQUIRE_EQUAL(evaluate(true_gte_binop2, evaluation_inputs{}), make_bool_val(true));

    expression false_gte_binop = binary_operator(make_int(-10), oper_t::GTE, make_int(10));
    BOOST_REQUIRE_EQUAL(evaluate(false_gte_binop, evaluation_inputs{}), make_bool_val(false));
}

BOOST_AUTO_TEST_CASE(evaluate_binary_operator_in) {
    expression in_list = constant(make_int_list_val({1, 3, 5}), list_type_impl::get_instance(int32_type, true));

    expression true_in_binop = binary_operator(make_int(3), oper_t::IN, in_list);
    BOOST_REQUIRE_EQUAL(evaluate(true_in_binop, evaluation_inputs{}), make_bool_val(true));

    expression false_in_binop = binary_operator(make_int(2), oper_t::IN, in_list);
    BOOST_REQUIRE_EQUAL(evaluate(false_in_binop, evaluation_inputs{}), make_bool_val(false));
}

BOOST_AUTO_TEST_CASE(evaluate_binary_operator_list_contains) {
    expression list_val = constant(make_int_list_val({1, 3, 5}), list_type_impl::get_instance(int32_type, true));

    expression list_contains_true = binary_operator(list_val, oper_t::CONTAINS, make_int(3));
    BOOST_REQUIRE_EQUAL(evaluate(list_contains_true, evaluation_inputs{}), make_bool_val(true));

    expression list_contains_false = binary_operator(list_val, oper_t::CONTAINS, make_int(2));
    BOOST_REQUIRE_EQUAL(evaluate(list_contains_false, evaluation_inputs{}), make_bool_val(false));
}

BOOST_AUTO_TEST_CASE(evaluate_binary_operator_set_contains) {
    expression set_val = constant(make_int_set_val({1, 3, 5}), set_type_impl::get_instance(int32_type, true));

    expression set_contains_true = binary_operator(set_val, oper_t::CONTAINS, make_int(3));
    BOOST_REQUIRE_EQUAL(evaluate(set_contains_true, evaluation_inputs{}), make_bool_val(true));

    expression set_contains_false = binary_operator(set_val, oper_t::CONTAINS, make_int(2));
    BOOST_REQUIRE_EQUAL(evaluate(set_contains_false, evaluation_inputs{}), make_bool_val(false));
}

BOOST_AUTO_TEST_CASE(evaluate_binary_operator_map_contains) {
    expression map_val = constant(make_int_int_map_val({{1, 2}, {3, 4}, {5, 6}}),
                                  map_type_impl::get_instance(int32_type, int32_type, true));

    expression map_contains_true = binary_operator(map_val, oper_t::CONTAINS, make_int(4));
    BOOST_REQUIRE_EQUAL(evaluate(map_contains_true, evaluation_inputs{}), make_bool_val(true));

    expression map_contains_false = binary_operator(map_val, oper_t::CONTAINS, make_int(3));
    BOOST_REQUIRE_EQUAL(evaluate(map_contains_false, evaluation_inputs{}), make_bool_val(false));
}

BOOST_AUTO_TEST_CASE(evaluate_binary_operator_map_contains_key) {
    expression map_val = constant(make_int_int_map_val({{1, 2}, {3, 4}, {5, 6}}),
                                  map_type_impl::get_instance(int32_type, int32_type, true));

    expression true_contains_key_binop = binary_operator(map_val, oper_t::CONTAINS_KEY, make_int(5));
    BOOST_REQUIRE_EQUAL(evaluate(true_contains_key_binop, evaluation_inputs{}), make_bool_val(true));

    expression false_contains_key_binop = binary_operator(map_val, oper_t::CONTAINS_KEY, make_int(6));
    BOOST_REQUIRE_EQUAL(evaluate(false_contains_key_binop, evaluation_inputs{}), make_bool_val(false));
}

BOOST_AUTO_TEST_CASE(evaluate_binary_operator_is_not) {
    expression true_is_not_binop = binary_operator(make_int(1), oper_t::IS_NOT, constant::make_null(int32_type));
    BOOST_REQUIRE_EQUAL(evaluate(true_is_not_binop, evaluation_inputs{}), make_bool_val(true));

    expression false_is_not_binop =
        binary_operator(constant::make_null(int32_type), oper_t::IS_NOT, constant::make_null(int32_type));
    BOOST_REQUIRE_EQUAL(evaluate(false_is_not_binop, evaluation_inputs{}), make_bool_val(false));

    expression forbidden_is_not_binop = binary_operator(make_int(1), oper_t::IS_NOT, make_int(2));
    BOOST_REQUIRE_THROW(evaluate(forbidden_is_not_binop, evaluation_inputs{}), exceptions::invalid_request_exception);
}

BOOST_AUTO_TEST_CASE(evaluate_binary_operator_like) {
    expression true_like_binop = binary_operator(constant(make_text_val("some_text"), utf8_type), oper_t::LIKE,
                                                 constant(make_text_val("some_%"), utf8_type));
    BOOST_REQUIRE_EQUAL(evaluate(true_like_binop, evaluation_inputs{}), make_bool_val(true));

    expression false_like_binop = binary_operator(constant(make_text_val("some_text"), utf8_type), oper_t::LIKE,
                                                  constant(make_text_val("some_other_%"), utf8_type));
    BOOST_REQUIRE_EQUAL(evaluate(false_like_binop, evaluation_inputs{}), make_bool_val(false));
}
