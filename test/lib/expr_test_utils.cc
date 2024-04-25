/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "expr_test_utils.hh"
#include <fmt/ranges.h>

namespace cql3 {
namespace expr {
namespace test_utils {
template <class T>
requires (!requires (T t) { t.has_value(); })
raw_value make_raw(T t) {
    data_type val_type = data_type_for<T>();
    data_value data_val(t);
    return raw_value::make_value(val_type->decompose(data_val));
}

template <class T>
raw_value make_raw(std::optional<T> t) {
    if (t.has_value()) {
        return make_raw(t.value());
    } else {
        return raw_value::make_null();
    }
}

raw_value make_empty_raw() {
    return raw_value::make_value(managed_bytes());
}

raw_value make_bool_raw(bool val) {
    return make_raw(val);
}

raw_value make_tinyint_raw(int8_t val) {
    return make_raw(val);
}

raw_value make_smallint_raw(int16_t val) {
    return make_raw(val);
}

raw_value make_int_raw(int32_t val) {
    return make_raw(val);
}

raw_value make_bigint_raw(int64_t val) {
    return make_raw(val);
}

raw_value make_text_raw(const sstring_view& text) {
    return raw_value::make_value(utf8_type->decompose(text));
}

raw_value make_float_raw(float val) {
    return make_raw(val);
}

raw_value make_double_raw(double val) {
    return make_raw(val);
}

template <class T>
constant make_const(T t) {
    data_type val_type = data_type_for<T>();
    return constant(make_raw(t), val_type);
}

constant make_empty_const(data_type type) {
    return constant(make_empty_raw(), type);
}

constant make_bool_const(bool val) {
    return make_const(val);
}

constant make_tinyint_const(int8_t val) {
    return make_const(val);
}

constant make_smallint_const(int16_t val) {
    return make_const(val);
}

constant make_int_const(int32_t val) {
    return make_const(val);
}

constant make_bigint_const(int64_t val) {
    return make_const(val);
}

constant make_text_const(const sstring_view& text) {
    return constant(make_text_raw(text), utf8_type);
}

constant make_float_const(float val) {
    return make_const(val);
}

constant make_double_const(double val) {
    return make_const(val);
}

untyped_constant make_int_untyped(const char* raw_text) {
    return untyped_constant{.partial_type = untyped_constant::type_class::integer, .raw_text = raw_text};
}

untyped_constant make_float_untyped(const char* raw_text) {
    return untyped_constant{.partial_type = untyped_constant::type_class::floating_point, .raw_text = raw_text};
}

untyped_constant make_string_untyped(const char* raw_text) {
    return untyped_constant{.partial_type = untyped_constant::type_class::string, .raw_text = raw_text};
}

untyped_constant make_bool_untyped(const char* raw_text) {
    return untyped_constant{.partial_type = untyped_constant::type_class::boolean, .raw_text = raw_text};
}

untyped_constant make_duration_untyped(const char* raw_text) {
    return untyped_constant{.partial_type = untyped_constant::type_class::duration, .raw_text = raw_text};
}

untyped_constant make_uuid_untyped(const char* raw_text) {
    return untyped_constant{.partial_type = untyped_constant::type_class::uuid, .raw_text = raw_text};
}

untyped_constant make_hex_untyped(const char* raw_text) {
    return untyped_constant{.partial_type = untyped_constant::type_class::hex, .raw_text = raw_text};
}

untyped_constant make_null_untyped() {
    return untyped_constant{.partial_type = untyped_constant::type_class::null, .raw_text = "null"};
}

// This function implements custom serialization of collection values.
// Some tests require the collection to contain an empty value,
// which is impossible to express using the existing code.
cql3::raw_value make_collection_raw(size_t size_to_write, const std::vector<cql3::raw_value>& elements_to_write) {
    size_t serialized_len = 0;
    serialized_len += collection_size_len();
    for (const cql3::raw_value& val : elements_to_write) {
        serialized_len += collection_value_len();
        if (val.is_value()) {
            serialized_len += val.view().with_value([](const FragmentedView auto& view) { return view.size_bytes(); });
        }
    }

    bytes b(bytes::initialized_later(), serialized_len);
    bytes::iterator out = b.begin();

    write_collection_size(out, size_to_write);
    for (const cql3::raw_value& val : elements_to_write) {
        if (val.is_null()) {
            write_int32(out, -1);
        } else {
            val.view().with_value(
                [&](const FragmentedView auto& val_view) { write_collection_value(out, linearized(val_view)); });
        }
    }

    return cql3::raw_value::make_value(b);
}

raw_value make_list_raw(const std::vector<raw_value>& values) {
    return make_collection_raw(values.size(), values);
}

raw_value make_set_raw(const std::vector<raw_value>& values) {
    return make_collection_raw(values.size(), values);
}

raw_value make_map_raw(const std::vector<std::pair<raw_value, raw_value>>& values) {
    std::vector<raw_value> flattened_values;
    for (const std::pair<raw_value, raw_value>& pair_val : values) {
        flattened_values.push_back(pair_val.first);
        flattened_values.push_back(pair_val.second);
    }
    return make_collection_raw(values.size(), flattened_values);
}

// This function implements custom serialization of tuples.
// Some tests require the tuple to contain an empty value,
// which is impossible to express using the existing code.
raw_value make_tuple_raw(const std::vector<raw_value>& values) {
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
std::vector<raw_value> to_raw_values(const std::vector<T>& values) {
    std::vector<raw_value> raw_vals;
    for (const T& val : values) {
        raw_vals.push_back(to_raw_value(val));
    }
    return raw_vals;
}

template <ToRawValue T1, ToRawValue T2>
std::vector<std::pair<raw_value, raw_value>> to_raw_value_pairs(const std::vector<std::pair<T1, T2>>& values) {
    std::vector<std::pair<raw_value, raw_value>> raw_vals;
    for (const std::pair<T1, T2>& val : values) {
        raw_vals.emplace_back(to_raw_value(val.first), to_raw_value(val.second));
    }
    return raw_vals;
}

constant make_list_const(const std::vector<raw_value>& vals, data_type elements_type) {
    raw_value raw_list = make_list_raw(vals);
    data_type list_type = list_type_impl::get_instance(elements_type, true);
    return constant(std::move(raw_list), std::move(list_type));
}

constant make_list_const(const std::vector<constant>& vals, data_type elements_type) {
    return make_list_const(to_raw_values(vals), elements_type);
}

constant make_set_const(const std::vector<raw_value>& vals, data_type elements_type) {
    raw_value raw_set = make_set_raw(vals);
    data_type set_type = set_type_impl::get_instance(elements_type, true);
    return constant(std::move(raw_set), std::move(set_type));
}

constant make_set_const(const std::vector<constant>& vals, data_type elements_type) {
    return make_set_const(to_raw_values(vals), elements_type);
}

constant make_map_const(const std::vector<std::pair<raw_value, raw_value>>& vals,
                        data_type key_type,
                        data_type value_type) {
    raw_value raw_map = make_map_raw(vals);
    data_type map_type = map_type_impl::get_instance(key_type, value_type, true);
    return constant(std::move(raw_map), std::move(map_type));
}

constant make_map_const(const std::vector<std::pair<constant, constant>>& vals,
                        data_type key_type,
                        data_type value_type) {
    return make_map_const(to_raw_value_pairs(vals), key_type, value_type);
}

constant make_tuple_const(const std::vector<raw_value>& vals, const std::vector<data_type>& element_types) {
    raw_value raw_tuple = make_tuple_raw(vals);
    data_type tuple_type = tuple_type_impl::get_instance(element_types);
    return constant(std::move(raw_tuple), std::move(tuple_type));
}

constant make_tuple_const(const std::vector<constant>& vals, const std::vector<data_type>& element_types) {
    return test_utils::make_tuple_const(to_raw_values(vals), element_types);
}

raw_value make_int_list_raw(const std::vector<std::optional<int32_t>>& values) {
    return make_list_raw(to_raw_values(values));
}

raw_value make_int_set_raw(const std::vector<int32_t>& values) {
    return make_set_raw(to_raw_values(values));
}

raw_value make_int_int_map_raw(const std::vector<std::pair<int32_t, int32_t>>& values) {
    return make_map_raw(to_raw_value_pairs(values));
}

constant make_int_list_const(const std::vector<std::optional<int32_t>>& values) {
    return constant(make_int_list_raw(values), list_type_impl::get_instance(int32_type, true));
}

constant make_int_set_const(const std::vector<int32_t>& values) {
    return constant(make_int_set_raw(values), set_type_impl::get_instance(int32_type, true));
}

constant make_int_int_map_const(const std::vector<std::pair<int32_t, int32_t>>& values) {
    return constant(make_int_int_map_raw(values), map_type_impl::get_instance(int32_type, int32_type, true));
}

collection_constructor make_list_constructor(std::vector<expression> elements, data_type elements_type) {
    return collection_constructor{.style = collection_constructor::style_type::list,
                                  .elements = std::move(elements),
                                  .type = list_type_impl::get_instance(elements_type, true)};
}

collection_constructor make_set_constructor(std::vector<expression> elements, data_type elements_type) {
    return collection_constructor{.style = collection_constructor::style_type::set,
                                  .elements = std::move(elements),
                                  .type = set_type_impl::get_instance(elements_type, true)};
}

collection_constructor make_map_constructor(const std::vector<expression> elements,
                                            data_type key_type,
                                            data_type element_type) {
    return collection_constructor{.style = collection_constructor::style_type::map,
                                  .elements = std::move(elements),
                                  .type = map_type_impl::get_instance(key_type, element_type, true)};
}

collection_constructor make_map_constructor(const std::vector<std::pair<expression, expression>>& elements,
                                            data_type key_type,
                                            data_type element_type) {
    std::vector<expression> map_element_pairs;
    for (const std::pair<expression, expression>& element : elements) {
        map_element_pairs.push_back(tuple_constructor{.elements = {element.first, element.second},
                                                      .type = tuple_type_impl::get_instance({key_type, element_type})});
    }
    return make_map_constructor(map_element_pairs, key_type, element_type);
}

tuple_constructor make_tuple_constructor(std::vector<expression> elements, std::vector<data_type> element_types) {
    return tuple_constructor{.elements = std::move(elements),
                             .type = tuple_type_impl::get_instance(std::move(element_types))};
}

usertype_constructor make_usertype_constructor(std::vector<std::pair<sstring_view, constant>> field_values) {
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

::lw_shared_ptr<column_specification> make_receiver(data_type receiver_type, sstring receiver_name) {
    return ::make_lw_shared<column_specification>(
        "test_ks", "test_cf", ::make_shared<cql3::column_identifier>(receiver_name, true), receiver_type);
}

// Creates evaluation_inputs that can be used to evaluate columns and bind variables using evaluate()
std::pair<evaluation_inputs, std::unique_ptr<evaluation_inputs_data>> make_evaluation_inputs(
    const schema_ptr& table_schema,
    const column_values& column_vals,
    const std::vector<raw_value>& bind_marker_values) {
    auto throw_error = [&](const auto&... fmt_args) -> sstring {
        sstring error_msg = format(fmt_args...);
        sstring final_msg = format("make_evaluation_inputs error: {}. (table_schema: {}, column_vals: {})", error_msg,
                                   *table_schema, column_vals);
        throw std::runtime_error(final_msg);
    };

    auto get_col_val = [&](const column_definition& col) -> const mutation_column_value& {
        auto col_value_iter = column_vals.find(col.name_as_text());
        if (col_value_iter == column_vals.end()) {
            throw_error("no value for column {}", col.name_as_text());
        }
        return col_value_iter->second;
    };

    std::vector<bytes> partition_key;
    for (const column_definition& pk_col : table_schema->partition_key_columns()) {
        const raw_value& col_value = get_col_val(pk_col).value;

        if (col_value.is_null()) {
            throw_error("Passed NULL as value for {}. This is not allowed for partition key columns.",
                        pk_col.name_as_text());
        }
        partition_key.push_back(raw_value(col_value).to_bytes());
    }

    std::vector<bytes> clustering_key;
    for (const column_definition& ck_col : table_schema->clustering_key_columns()) {
        const raw_value& col_value = get_col_val(ck_col).value;

        if (col_value.is_null()) {
            throw_error("Passed NULL as value for {}. This is not allowed for clustering key columns.",
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
    std::vector<api::timestamp_type> static_and_regular_column_timestamps(static_and_regular_columns.size());
    std::vector<int32_t> static_and_regular_column_ttls(static_and_regular_columns.size());

    for (const column_definition& col : table_schema->regular_columns()) {
        auto& mut_value = get_col_val(col);
        const raw_value& col_value = mut_value.value;
        int32_t index = selection->index_of(col);
        static_and_regular_columns[index] = raw_value(col_value).to_managed_bytes_opt();
        static_and_regular_column_timestamps[index] = mut_value.timestamp;
        static_and_regular_column_ttls[index] = mut_value.ttl;
    }

    for (const column_definition& col : table_schema->static_columns()) {
        auto& mut_value = get_col_val(col);
        const raw_value& col_value = mut_value.value;
        int32_t index = selection->index_of(col);
        static_and_regular_columns[index] = raw_value(col_value).to_managed_bytes_opt();
        static_and_regular_column_timestamps[index] = mut_value.timestamp;
        static_and_regular_column_ttls[index] = mut_value.ttl;
    }

    query_options options(default_cql_config, db::consistency_level::ONE, std::nullopt, bind_marker_values, true,
                          query_options::specific_options::DEFAULT);

    std::unique_ptr<evaluation_inputs_data> data = std::make_unique<evaluation_inputs_data>(
        evaluation_inputs_data{.partition_key = std::move(partition_key),
                               .clustering_key = std::move(clustering_key),
                               .static_and_regular_columns = std::move(static_and_regular_columns),
                               .selection = std::move(selection),
                               .options = std::move(options),
                               .timestamps = std::move(static_and_regular_column_timestamps),
                               .ttls = std::move(static_and_regular_column_ttls)});

    evaluation_inputs inputs{.partition_key = data->partition_key,
                             .clustering_key = data->clustering_key,
                             .static_and_regular_columns = data->static_and_regular_columns,
                             .selection = data->selection.get(),
                             .options = &data->options,
                             .static_and_regular_timestamps = data->timestamps,
                             .static_and_regular_ttls = data->ttls,
                             };

    return std::pair(std::move(inputs), std::move(data));
}

bind_variable
make_bind_variable(int32_t index, data_type type) {
    return bind_variable{index, make_receiver(type, "?")};
}

raw_value
evaluate_with_bind_variables(const expression& e, std::vector<raw_value> parameters) {
    query_options options(default_cql_config, db::consistency_level::ONE, std::nullopt, parameters, true,
                          query_options::specific_options::DEFAULT);
    return evaluate(e, evaluation_inputs{.options = &options});
}


// A mock implementation of data_dictionary::database, used in tests
class mock_database_impl : public data_dictionary::impl {
    schema_ptr _table_schema;
    // we cannot set _table_views here, as _table_schema is not necessarily
    // a view. but if a test calls get_table_views(), the test guarantees
    // that the _table_schema is a view, so we set _table_views is the accsseor
    // instead.
    mutable std::vector<view_ptr> _table_views;
    ::lw_shared_ptr<data_dictionary::keyspace_metadata> _keyspace_metadata;

    db::config _config;

public:
    explicit mock_database_impl(schema_ptr table_schema)
        : _table_schema(table_schema),
          _keyspace_metadata(make_lw_shared<data_dictionary::keyspace_metadata>(_table_schema->ks_name(),
                                                                              "MockReplicationStrategy",
                                                                              locator::replication_strategy_config_options{},
                                                                              std::nullopt,
                                                                              false,
                                                                              std::vector<schema_ptr>({_table_schema}))) {}

    static std::pair<data_dictionary::database, std::unique_ptr<mock_database_impl>> make(schema_ptr table_schema) {
        std::unique_ptr<mock_database_impl> mock_db = std::make_unique<mock_database_impl>(table_schema);
        data_dictionary::database db = make_database(mock_db.get(), nullptr);
        return std::pair(std::move(db), std::move(mock_db));
    }

    virtual const table_schema_version& get_version(data_dictionary::database db) const override {
        throw std::bad_function_call();
    }

    virtual std::optional<data_dictionary::keyspace> try_find_keyspace(data_dictionary::database db,
                                                                       std::string_view name) const override {
        if (_table_schema->ks_name() == name) {
            return make_keyspace(this, nullptr);
        }
        return std::nullopt;
    }
    virtual std::vector<data_dictionary::keyspace> get_keyspaces(data_dictionary::database db) const override {
        return {make_keyspace(this, nullptr)};
    }
    virtual std::vector<sstring> get_user_keyspaces(data_dictionary::database db) const override {
        return {_table_schema->ks_name()};
    }
    virtual std::vector<sstring> get_all_keyspaces(data_dictionary::database db) const override {
        return {_table_schema->ks_name()};
    }
    virtual std::vector<data_dictionary::table> get_tables(data_dictionary::database db) const override {
        return {make_table(this, nullptr)};
    }
    virtual std::optional<data_dictionary::table> try_find_table(data_dictionary::database db,
                                                                 std::string_view ks,
                                                                 std::string_view tab) const override {
        if (_table_schema->ks_name() == ks && _table_schema->cf_name() == tab) {
            return make_table(this, nullptr);
        }
        return std::nullopt;
    }
    virtual std::optional<data_dictionary::table> try_find_table(data_dictionary::database db,
                                                                 table_id id) const override {
        if (_table_schema->id() == id) {
            return make_table(this, nullptr);
        }
        return std::nullopt;
    }
    virtual const secondary_index::secondary_index_manager& get_index_manager(data_dictionary::table t) const override {
        throw std::bad_function_call();
    }
    virtual schema_ptr get_table_schema(data_dictionary::table t) const override { return _table_schema; }
    virtual lw_shared_ptr<data_dictionary::keyspace_metadata> get_keyspace_metadata(
        data_dictionary::keyspace ks) const override {
        return _keyspace_metadata;
    }

    virtual bool is_internal(data_dictionary::keyspace ks) const override { return false; }
    virtual const locator::abstract_replication_strategy& get_replication_strategy(
        data_dictionary::keyspace ks) const override {
        throw std::bad_function_call();
    }
    virtual const std::vector<view_ptr>& get_table_views(data_dictionary::table t) const override {
        _table_views = std::vector{view_ptr(_table_schema)};
        return _table_views;
    }
    virtual sstring get_available_index_name(data_dictionary::database db,
                                             std::string_view ks_name,
                                             std::string_view table_name,
                                             std::optional<sstring> index_name_root) const override {
        return {};
    }
    virtual std::set<sstring> existing_index_names(data_dictionary::database db,
                                                   std::string_view ks_name,
                                                   std::string_view cf_to_exclude = {}) const override {
        return {};
    }
    virtual schema_ptr find_indexed_table(data_dictionary::database db,
                                          std::string_view ks_name,
                                          std::string_view index_name) const override {
        return nullptr;
    }
    virtual schema_ptr get_cdc_base_table(data_dictionary::database db, const schema&) const override {
        return nullptr;
    }
    virtual const db::config& get_config(data_dictionary::database db) const override { return _config; }
    virtual const db::extensions& get_extensions(data_dictionary::database db) const override {
        return _config.extensions();
    }
    virtual const gms::feature_service& get_features(data_dictionary::database db) const override {
        throw std::bad_function_call();
    }
    virtual replica::database& real_database(data_dictionary::database db) const override {
        throw std::bad_function_call();
    }
    virtual replica::database* real_database_ptr(data_dictionary::database db) const override {
        return nullptr;
    }

    virtual ~mock_database_impl() = default;
};

std::pair<data_dictionary::database, std::unique_ptr<data_dictionary::impl>> make_data_dictionary_database(
    const schema_ptr& table_schema) {
    return mock_database_impl::make(table_schema);
}
}  // namespace test_utils
}  // namespace expr
}  // namespace cql3
