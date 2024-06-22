/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "cql3/expr/expression.hh"
#include "cql3/expr/evaluate.hh"
#include "cql3/query_options.hh"
#include "cql3/selection/selection.hh"
#include "data_dictionary/data_dictionary.hh"
#include "data_dictionary/impl.hh"
#include "data_dictionary/keyspace_metadata.hh"
#include "db/config.hh"
#include "types/list.hh"
#include "types/map.hh"
#include "types/set.hh"

namespace cql3 {
namespace expr {
namespace test_utils {

raw_value make_empty_raw();
raw_value make_bool_raw(bool val);
raw_value make_tinyint_raw(int8_t val);
raw_value make_smallint_raw(int16_t val);
raw_value make_int_raw(int32_t val);
raw_value make_bigint_raw(int64_t val);
raw_value make_text_raw(const sstring_view& text);
raw_value make_float_raw(float val);
raw_value make_double_raw(double val);

constant make_empty_const(data_type type);
constant make_bool_const(bool val);
constant make_tinyint_const(int8_t val);
constant make_smallint_const(int16_t val);
constant make_int_const(int32_t val);
constant make_bigint_const(int64_t val);
constant make_text_const(const sstring_view& text);
constant make_float_const(float val);
constant make_double_const(double val);

untyped_constant make_int_untyped(const char* raw_text);
untyped_constant make_float_untyped(const char* raw_text);
untyped_constant make_string_untyped(const char* raw_text);
untyped_constant make_bool_untyped(const char* raw_text);
untyped_constant make_duration_untyped(const char* raw_text);
untyped_constant make_uuid_untyped(const char* raw_text);
untyped_constant make_hex_untyped(const char* raw_text);
untyped_constant make_null_untyped();

// This function implements custom serialization of collection values.
// Some tests require the collection to contain unset_value or an empty value,
// which is impossible to express using the existing code.
cql3::raw_value make_collection_raw(size_t size_to_write, const std::vector<cql3::raw_value>& elements_to_write);

raw_value make_list_raw(const std::vector<raw_value>& values);
raw_value make_set_raw(const std::vector<raw_value>& values);
raw_value make_map_raw(const std::vector<std::pair<raw_value, raw_value>>& values);

// This function implements custom serialization of tuples.
// Some tests require the tuple to contain unset_value or an empty value,
// which is impossible to express using the existing code.
raw_value make_tuple_raw(const std::vector<raw_value>& values);

constant make_list_const(const std::vector<raw_value>& vals, data_type elements_type);
constant make_list_const(const std::vector<constant>& vals, data_type elements_type);

constant make_set_const(const std::vector<raw_value>& vals, data_type elements_type);
constant make_set_const(const std::vector<constant>& vals, data_type elements_type);

constant make_map_const(const std::vector<std::pair<raw_value, raw_value>>& vals,
                        data_type key_type,
                        data_type value_type);

constant make_map_const(const std::vector<std::pair<constant, constant>>& vals,
                        data_type key_type,
                        data_type value_type);

constant make_tuple_const(const std::vector<raw_value>& vals, const std::vector<data_type>& element_types);
constant make_tuple_const(const std::vector<constant>& vals, const std::vector<data_type>& element_types);

raw_value make_int_list_raw(const std::vector<std::optional<int32_t>>& values);
raw_value make_int_set_raw(const std::vector<int32_t>& values);

raw_value make_int_int_map_raw(const std::vector<std::pair<int32_t, int32_t>>& values);

constant make_int_list_const(const std::vector<std::optional<int32_t>>& values);
constant make_int_set_const(const std::vector<int32_t>& values);
constant make_int_int_map_const(const std::vector<std::pair<int32_t, int32_t>>& values);

collection_constructor make_list_constructor(std::vector<expression> elements, data_type elements_type);
collection_constructor make_set_constructor(std::vector<expression> elements, data_type elements_type);
collection_constructor make_map_constructor(const std::vector<expression> elements,
                                            data_type key_type,
                                            data_type element_type);
collection_constructor make_map_constructor(const std::vector<std::pair<expression, expression>>& elements,
                                            data_type key_type,
                                            data_type element_type);
tuple_constructor make_tuple_constructor(std::vector<expression> elements, std::vector<data_type> element_types);
usertype_constructor make_usertype_constructor(std::vector<std::pair<sstring_view, constant>> field_values);

::lw_shared_ptr<column_specification> make_receiver(data_type receiver_type, sstring name = "receiver_name");

bind_variable make_bind_variable(int32_t index, data_type type);

struct evaluation_inputs_data {
    std::vector<bytes> partition_key;
    std::vector<bytes> clustering_key;
    std::vector<managed_bytes_opt> static_and_regular_columns;
    ::shared_ptr<selection::selection> selection;
    query_options options;
    std::vector<api::timestamp_type> timestamps;
    std::vector<int32_t> ttls;
};

struct mutation_column_value {
    mutation_column_value(cql3::raw_value v, api::timestamp_type ts, int32_t ttl)
        : value(std::move(v)), timestamp(ts), ttl(ttl) {}
    // Convenience constructor when timestamp/ttl are not interesting
    mutation_column_value(cql3::raw_value v)
            : value(std::move(v))
            , timestamp(value.is_null() ? api::missing_timestamp : 12345)
            , ttl(-1) {
    }

    cql3::raw_value value;
    api::timestamp_type timestamp;
    int32_t ttl;
};

using column_values = std::map<sstring, mutation_column_value>;

// Creates evaluation_inputs that can be used to evaluate columns and bind variables using evaluate()
std::pair<evaluation_inputs, std::unique_ptr<evaluation_inputs_data>> make_evaluation_inputs(
    const schema_ptr& table_schema,
    const column_values& column_vals,
    const std::vector<raw_value>& bind_marker_values = {});

// Creates a mock implementation of data_dictionary::database, useful in tests.
std::pair<data_dictionary::database, std::unique_ptr<data_dictionary::impl>> make_data_dictionary_database(
    const schema_ptr& table_schema);

raw_value evaluate_with_bind_variables(const expression& e, std::vector<raw_value> bind_variable_values);


}  // namespace test_utils
}  // namespace expr
}  // namespace cql3

template <> struct fmt::formatter<cql3::expr::test_utils::mutation_column_value> : fmt::formatter<string_view> {
    auto format(const cql3::expr::test_utils::mutation_column_value& mcv, fmt::format_context& ctx) const {
        return fmt::format_to(ctx.out(), "{{{}/ts={}/ttl={}}}", mcv.value, mcv.timestamp, mcv.ttl);

    }
};
