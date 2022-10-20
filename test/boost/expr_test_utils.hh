/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "cql3/expr/expression.hh"
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

template <class T>
raw_value make_raw(T t) {
    data_type val_type = data_type_for<T>();
    data_value data_val(t);
    return raw_value::make_value(val_type->decompose(data_val));
}

inline raw_value make_empty_raw() {
    return raw_value::make_value(managed_bytes());
}

inline raw_value make_bool_raw(bool val) {
    return make_raw(val);
}

inline raw_value make_int_raw(int32_t val) {
    return make_raw(val);
}

inline raw_value make_text_raw(const sstring_view& text) {
    return raw_value::make_value(utf8_type->decompose(text));
}

template <class T>
constant make_const(T t) {
    data_type val_type = data_type_for<T>();
    return constant(make_raw(t), val_type);
}

inline constant make_empty_const(data_type type) {
    return constant(make_empty_raw(), type);
}

inline constant make_bool_const(bool val) {
    return make_const(val);
}

inline constant make_int_const(int32_t val) {
    return make_const(val);
}

inline constant make_text_const(const sstring_view& text) {
    return constant(make_text_raw(text), utf8_type);
}

// This function implements custom serialization of collection values.
// Some tests require the collection to contain unset_value or an empty value,
// which is impossible to express using the existing code.
inline cql3::raw_value make_collection_raw(size_t size_to_write,
                                           const std::vector<cql3::raw_value>& elements_to_write) {
    cql_serialization_format sf = cql_serialization_format::latest();

    size_t serialized_len = 0;
    serialized_len += collection_size_len(sf);
    for (const cql3::raw_value& val : elements_to_write) {
        serialized_len += collection_value_len(sf);
        if (val.is_value()) {
            serialized_len += val.view().with_value([](const FragmentedView auto& view) { return view.size_bytes(); });
        }
    }

    bytes b(bytes::initialized_later(), serialized_len);
    bytes::iterator out = b.begin();

    write_collection_size(out, size_to_write, sf);
    for (const cql3::raw_value& val : elements_to_write) {
        if (val.is_null()) {
            write_int32(out, -1);
        } else if (val.is_unset_value()) {
            write_int32(out, -2);
        } else {
            val.view().with_value(
                [&](const FragmentedView auto& val_view) { write_collection_value(out, sf, linearized(val_view)); });
        }
    }

    return cql3::raw_value::make_value(b);
}

inline raw_value make_list_raw(const std::vector<raw_value>& values) {
    return make_collection_raw(values.size(), values);
}

inline raw_value make_set_raw(const std::vector<raw_value>& values) {
    return make_collection_raw(values.size(), values);
}

inline raw_value make_map_raw(const std::vector<std::pair<raw_value, raw_value>>& values) {
    std::vector<raw_value> flattened_values;
    for (const std::pair<raw_value, raw_value>& pair_val : values) {
        flattened_values.push_back(pair_val.first);
        flattened_values.push_back(pair_val.second);
    }
    return make_collection_raw(values.size(), flattened_values);
}

// This function implements custom serialization of tuples.
// Some tests require the tuple to contain unset_value or an empty value,
// which is impossible to express using the existing code.
inline raw_value make_tuple_raw(const std::vector<raw_value>& values) {
    size_t serialized_len = 0;
    for (const raw_value& val : values) {
        serialized_len += 4;
        if (val.is_value()) {
            serialized_len += val.view().size_bytes();
        }
    }
    managed_bytes ret(managed_bytes::initialized_later(), serialized_len);
    managed_bytes_mutable_view out(ret);
    for (const raw_value& val : values) {
        if (val.is_null()) {
            write<int32_t>(out, -1);
        } else if (val.is_unset_value()) {
            write<int32_t>(out, -2);
        } else {
            val.view().with_value([&](const FragmentedView auto& bytes_view) {
                write<int32_t>(out, bytes_view.size_bytes());
                write_fragmented(out, bytes_view);
            });
        }
    }
    return raw_value::make_value(std::move(ret));
}

template <class T>
raw_value to_raw_value(const T& t) {
    if constexpr (std::same_as<T, raw_value>) {
        return t;
    } else if constexpr (std::same_as<T, constant>) {
        return t.value;
    } else {
        return make_raw<T>(t);
    }
}

// A concept which expresses that it's possible to convert T to raw_value.
// Satisfied for raw_value, constant, int32_t, and various other types.
template <class T>
concept ToRawValue = requires(T t) {
    data_value(t);
    { to_raw_value(t) } -> std::same_as<raw_value>;
}
|| std::same_as<T, constant> || std::same_as<T, raw_value>;

template <ToRawValue T>
inline std::vector<raw_value> to_raw_values(const std::vector<T>& values) {
    std::vector<raw_value> raw_vals;
    for (const T& val : values) {
        raw_vals.push_back(to_raw_value(val));
    }
    return raw_vals;
}

template <ToRawValue T1, ToRawValue T2>
inline std::vector<std::pair<raw_value, raw_value>> to_raw_value_pairs(const std::vector<std::pair<T1, T2>>& values) {
    std::vector<std::pair<raw_value, raw_value>> raw_vals;
    for (const std::pair<T1, T2>& val : values) {
        raw_vals.emplace_back(to_raw_value(val.first), to_raw_value(val.second));
    }
    return raw_vals;
}

inline constant make_list_const(const std::vector<raw_value>& vals, data_type elements_type) {
    raw_value raw_list = make_list_raw(vals);
    data_type list_type = list_type_impl::get_instance(elements_type, true);
    return constant(std::move(raw_list), std::move(list_type));
}

inline constant make_list_const(const std::vector<constant>& vals, data_type elements_type) {
    return make_list_const(to_raw_values(vals), elements_type);
}

inline constant make_set_const(const std::vector<raw_value>& vals, data_type elements_type) {
    raw_value raw_set = make_set_raw(vals);
    data_type set_type = set_type_impl::get_instance(elements_type, true);
    return constant(std::move(raw_set), std::move(set_type));
}

inline constant make_set_const(const std::vector<constant>& vals, data_type elements_type) {
    return make_set_const(to_raw_values(vals), elements_type);
}

inline constant make_map_const(const std::vector<std::pair<raw_value, raw_value>>& vals,
                               data_type key_type,
                               data_type value_type) {
    raw_value raw_map = make_map_raw(vals);
    data_type map_type = map_type_impl::get_instance(key_type, value_type, true);
    return constant(std::move(raw_map), std::move(map_type));
}

inline constant make_map_const(const std::vector<std::pair<constant, constant>>& vals,
                               data_type key_type,
                               data_type value_type) {
    return make_map_const(to_raw_value_pairs(vals), key_type, value_type);
}

inline constant make_tuple_const(const std::vector<raw_value>& vals, const std::vector<data_type>& element_types) {
    raw_value raw_tuple = make_tuple_raw(vals);
    data_type tuple_type = tuple_type_impl::get_instance(element_types);
    return constant(std::move(raw_tuple), std::move(tuple_type));
}

inline constant make_tuple_const(const std::vector<constant>& vals, const std::vector<data_type>& element_types) {
    return test_utils::make_tuple_const(to_raw_values(vals), element_types);
}

inline raw_value make_int_list_raw(const std::vector<int32_t>& values) {
    return make_list_raw(to_raw_values(values));
}

inline raw_value make_int_set_raw(const std::vector<int32_t>& values) {
    return make_set_raw(to_raw_values(values));
}

inline raw_value make_int_int_map_raw(const std::vector<std::pair<int32_t, int32_t>>& values) {
    return make_map_raw(to_raw_value_pairs(values));
}

inline constant make_int_list_const(const std::vector<int32_t>& values) {
    return constant(make_int_list_raw(values), list_type_impl::get_instance(int32_type, true));
}

inline constant make_int_set_const(const std::vector<int32_t>& values) {
    return constant(make_int_set_raw(values), set_type_impl::get_instance(int32_type, true));
}

inline constant make_int_int_map_const(const std::vector<std::pair<int32_t, int32_t>>& values) {
    return constant(make_int_int_map_raw(values), map_type_impl::get_instance(int32_type, int32_type, true));
}

inline collection_constructor make_list_constructor(std::vector<expression> elements, data_type elements_type) {
    return collection_constructor{.style = collection_constructor::style_type::list,
                                  .elements = std::move(elements),
                                  .type = list_type_impl::get_instance(elements_type, true)};
}

inline collection_constructor make_set_constructor(std::vector<expression> elements, data_type elements_type) {
    return collection_constructor{.style = collection_constructor::style_type::set,
                                  .elements = std::move(elements),
                                  .type = set_type_impl::get_instance(elements_type, true)};
}

inline collection_constructor make_map_constructor(const std::vector<expression> elements,
                                                   data_type key_type,
                                                   data_type element_type) {
    return collection_constructor{.style = collection_constructor::style_type::map,
                                  .elements = std::move(elements),
                                  .type = map_type_impl::get_instance(key_type, element_type, true)};
}

inline collection_constructor make_map_constructor(const std::vector<std::pair<expression, expression>>& elements,
                                                   data_type key_type,
                                                   data_type element_type) {
    std::vector<expression> map_element_pairs;
    for (const std::pair<expression, expression>& element : elements) {
        map_element_pairs.push_back(tuple_constructor{.elements = {element.first, element.second},
                                                      .type = tuple_type_impl::get_instance({key_type, element_type})});
    }
    return make_map_constructor(map_element_pairs, key_type, element_type);
}

inline tuple_constructor make_tuple_constructor(std::vector<expression> elements,
                                                std::vector<data_type> element_types) {
    return tuple_constructor{.elements = std::move(elements),
                             .type = tuple_type_impl::get_instance(std::move(element_types))};
}

inline usertype_constructor make_usertype_constructor(std::vector<std::pair<sstring_view, constant>> field_values) {
    usertype_constructor::elements_map_type elements_map;
    std::vector<bytes> field_names;
    std::vector<data_type> field_types;
    for (auto& [field_name, field_value] : field_values) {
        field_names.push_back(field_name.data());
        field_types.push_back(field_value.type);
        elements_map.emplace(column_identifier(sstring(field_name), true), field_value);
    }
    data_type type = user_type_impl::get_instance("test_ks", "test_user_type", field_names, field_types, true);
    return usertype_constructor{.elements = std::move(elements_map), .type = std::move(type)};
}

struct evaluation_inputs_data {
    std::vector<bytes> partition_key;
    std::vector<bytes> clustering_key;
    std::vector<managed_bytes_opt> static_and_regular_columns;
    ::shared_ptr<selection::selection> selection;
    query_options options;
};
using column_values = std::map<sstring, raw_value>;

// Creates evaluation_inputs that can be used to evaluate columns and bind variables using evaluate()
inline std::pair<evaluation_inputs, std::unique_ptr<evaluation_inputs_data>> make_evaluation_inputs(
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

}  // namespace test_utils
}  // namespace expr
}  // namespace cql3