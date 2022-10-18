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