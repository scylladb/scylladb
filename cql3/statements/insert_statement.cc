/*
 * Copyright (C) 2026-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.1 and Apache-2.0)
 */

#include "utils/assert.hh"
#include "insert_statement.hh"
#include "cql3/expr/expression.hh"
#include "cql3/expr/evaluate.hh"
#include "cql3/expr/expr-utils.hh"
#include "raw/insert_statement.hh"

#include "cql3/operation_impl.hh"
#include "cql3/lists.hh"
#include "cql3/maps.hh"
#include "cql3/sets.hh"
#include "cql3/user_types.hh"
#include "types/json_utils.hh"
#include "types/list.hh"
#include "types/map.hh"
#include "types/set.hh"
#include "types/user.hh"
#include "types/concrete_types.hh"
#include "validation.hh"
#include <seastar/util/defer.hh>
#include "dht/i_partitioner.hh"
#include <optional>

namespace cql3 {

namespace json_helpers {

/*
 * According to CQL3+JSON documentation names wrapped with double quotes
 * should be treated as case-sensitive, while regular strings should be
 * case-insensitive.
 */
static std::unordered_map<sstring, rjson::value> handle_case_sensitivity(rjson::value&& value_map) {
    std::unordered_map<sstring, rjson::value> case_sensitive_map;
    for (auto it = value_map.MemberBegin(); it != value_map.MemberEnd(); ++it) {
        sstring name(rjson::to_string_view(it->name));
        if (name.size() > 1 && *name.begin() == '"' && name.back() == '"') {
            case_sensitive_map.emplace(name.substr(1, name.size() - 2), std::move(it->value));
        } else {
            std::transform(name.begin(), name.end(), name.begin(), ::tolower);
            case_sensitive_map.emplace(std::move(name), std::move(it->value));
        }
    }
    return case_sensitive_map;
}

std::unordered_map<sstring, bytes_opt>
parse(const sstring& json_string, const schema::columns_type& expected_receivers) {
    std::unordered_map<sstring, bytes_opt> json_map;
    auto prepared_map = handle_case_sensitivity(rjson::parse(json_string));
    for (const auto& def : expected_receivers) {
        sstring cql_name = def.name_as_text();
        auto value_it = prepared_map.find(cql_name);
        if (value_it == prepared_map.end()) {
            continue;
        } else if (value_it->second.IsNull()) {
            json_map.emplace(std::move(cql_name), bytes_opt{});
            prepared_map.erase(value_it);
        } else {
            json_map.emplace(std::move(cql_name), from_json_object(*def.type, std::move(value_it->second)));
            prepared_map.erase(value_it);
        }
    }
    if (!prepared_map.empty()) {
        throw exceptions::invalid_request_exception(format("JSON values map contains unrecognized column: {}", prepared_map.begin()->first));
    }
    return json_map;
}

}

namespace statements {

insert_statement::insert_statement(
        audit::audit_info_ptr&& audit_info,
        uint32_t bound_terms,
        schema_ptr s,
        std::unique_ptr<attributes> attrs,
        cql_stats& stats)
    : modification_statement{statement_type::INSERT, bound_terms, std::move(s), std::move(attrs), stats}
{
    set_audit_info(std::move(audit_info));
}

void insert_statement::add_key_value(const column_definition& def, expr::expression value) {
    _key_values.emplace_back(&def, std::move(value));
}

bool insert_statement::names(const column_definition& def) const {
    return std::ranges::any_of(_key_values, [&def] (const auto& kv) { return kv.first == &def; });
}

void insert_statement::validate_addressed_row() {
    classify_exists_condition(std::ranges::any_of(s->clustering_key_columns(),
            [this] (const column_definition& def) { return names(def); }));

    // The clustering columns the statement names must form a prefix of the
    // clustering key, and - unless the statement writes the static row, or the
    // table is COMPACT STORAGE, where a partial prefix names a range of cells
    // (CASSANDRA-7990) - the whole of it.
    const column_definition* unnamed = nullptr;
    bool named_after_unnamed = false;
    for (const column_definition& def : s->clustering_key_columns()) {
        if (!names(def)) {
            if (!unnamed) {
                unnamed = &def;
            }
        } else if (unnamed) {
            named_after_unnamed = true;
        }
    }
    if (unnamed && (named_after_unnamed || (!applies_only_to_static_columns() && !s->is_dense()))) {
        throw exceptions::invalid_request_exception(format("Missing mandatory PRIMARY KEY part {}",
                unnamed->name_as_text()));
    }

    for (const column_definition& def : s->partition_key_columns()) {
        if (!names(def)) {
            throw exceptions::invalid_request_exception(format("Missing mandatory PRIMARY KEY part {}",
                    def.name_as_text()));
        }
    }
}

const expr::expression* insert_statement::value_for(const column_definition& def) const {
    auto it = std::ranges::find_if(_key_values, [&def] (const auto& kv) { return kv.first == &def; });
    return it == _key_values.end() ? nullptr : &it->second;
}

dht::partition_range_vector
insert_statement::build_partition_keys(const query_options& options, const json_cache_opt& json_cache) const {
    std::vector<bytes_opt> exploded;
    exploded.reserve(s->partition_key_size());
    for (const column_definition& def : s->partition_key_columns()) {
        // Every partition key column is named; validate_addressed_row() saw to it.
        auto value = expr::evaluate(*value_for(def), options).to_bytes_opt();
        if (!value) {
            // A null key names no partition.  validate_primary_key() rejects it
            // before this on the request path; this mirrors what a null value in
            // an EQ restriction used to yield.
            return {};
        }
        exploded.emplace_back(std::move(value));
    }
    auto pkey = partition_key::from_optional_exploded(*s, std::move(exploded));
    validation::validate_cql_key(*s, pkey);
    dht::partition_range_vector ranges;
    ranges.emplace_back(query::range<query::ring_position>::make_singular(dht::decorate_key(*s, std::move(pkey))));
    return ranges;
}

query::clustering_row_ranges
insert_statement::create_clustering_ranges(const query_options& options, const json_cache_opt& json_cache) const {
    std::vector<bytes_opt> exploded;
    exploded.reserve(s->clustering_key_size());
    for (const column_definition& def : s->clustering_key_columns()) {
        // The named columns form a prefix, so the first unnamed one ends it.
        const expr::expression* value = value_for(def);
        if (!value) {
            break;
        }
        auto bytes = expr::evaluate(*value, options).to_bytes_opt();
        if (!bytes) {
            return {};  // as above: a null key names no row
        }
        exploded.emplace_back(std::move(bytes));
    }
    if (exploded.empty()) {
        // The statement names no clustering column, so it writes the static row
        // or, on a COMPACT STORAGE table, the whole partition - which an
        // unbounded range addresses, as an empty WHERE clause used to.
        return {query::clustering_range::make_open_ended_both_sides()};
    }
    return {query::clustering_range::make_singular(clustering_key_prefix::from_optional_exploded(*s, std::move(exploded)))};
}

void insert_statement::validate_primary_key(const query_options& options) const {
    for (const auto& [def, value] : _key_values) {
        if (expr::evaluate(value, options).is_null()) {
            throw exceptions::invalid_request_exception(format("Invalid null value in condition for column {}",
                    def->name_as_text()));
        }
    }
}

void insert_statement::execute_operations_for_key(mutation& m, const clustering_key_prefix& prefix, const update_parameters& params, const json_cache_opt& json_cache) const {
    apply_column_operations(_column_operations, m, prefix, params);
}

utils::chunked_vector<mutation> insert_statement::apply_updates(
        const std::vector<dht::partition_range>& keys,
        const std::vector<query::clustering_range>& ranges,
        const update_parameters& params,
        const json_cache_opt& json_cache) const {
    auto mutations = make_mutations(keys);
    for (auto& m : mutations) {
        for (auto&& range : ranges) {
            auto prefix = row_key(range);
            open_row(*s, type, !_column_operations.empty(), m, prefix, params);
            execute_operations_for_key(m, prefix, params, json_cache);
        }
    }

    warn(unimplemented::cause::INDEXES);

    return mutations;
}

modification_statement::json_cache_opt insert_prepared_json_statement::maybe_prepare_json_cache(const query_options& options) const {
    cql3::raw_value c = expr::evaluate(_value, options);
    sstring json_string = utf8_type->to_string(to_bytes(c.view()));
    return json_helpers::parse(std::move(json_string), s->all_columns());
}

void
insert_prepared_json_statement::execute_set_value(mutation& m, const clustering_key_prefix& prefix,
    const update_parameters& params, const column_definition& column, const bytes_opt& value) const {

    if (!value) {
        visit(*column.type, make_visitor(
        [&] (const list_type_impl&) {
            lists::setter::execute(m, prefix, params, column, cql3::raw_value::make_null());
        },
        [&] (const set_type_impl&) {
            sets::setter::execute(m, prefix, params, column, cql3::raw_value::make_null());
        },
        [&] (const map_type_impl&) {
            maps::setter::execute(m, prefix, params, column, cql3::raw_value::make_null());
        },
        [&] (const user_type_impl&) {
            user_types::setter::execute(m, prefix, params, column, cql3::raw_value::make_null());
        },
        [&] (const abstract_type& type) {
            if (type.is_collection()) {
                throw std::runtime_error(format("insert_prepared_json_statement::execute_set_value: unhandled collection type {}", type.name()));
            }
            m.set_cell(prefix, column, params.make_dead_cell());
        }
        ));
        return;
    }


    auto val = raw_value::make_value(*value);
    visit(*column.type, make_visitor(
    [&] (const list_type_impl& ltype) {
        lists::setter::execute(m, prefix, params, column, val);
    },
    [&] (const set_type_impl& stype) {
        sets::setter::execute(m, prefix, params, column, val);
    },
    [&] (const map_type_impl& mtype) {
        maps::setter::execute(m, prefix, params, column, val);
    },
    [&] (const user_type_impl& utype) {
        user_types::setter::execute(m, prefix, params, column, val);
    },
    [&] (const abstract_type& type) {
        if (type.is_collection()) {
            throw std::runtime_error(format("insert_prepared_json_statement::execute_set_value: unhandled collection type {}", type.name()));
        }
        constants::setter::execute(m, prefix, params, column, val.view());
    }
    ));
}

dht::partition_range_vector
insert_prepared_json_statement::build_partition_keys(const query_options& options, const json_cache_opt& json_cache) const {
    dht::partition_range_vector ranges;
    std::vector<bytes_opt> exploded;
    for (const auto& def : s->partition_key_columns()) {
        auto json_value = json_cache->find(def.name_as_text());
        if (json_value == json_cache->end() || !json_value->second) {
            throw exceptions::invalid_request_exception(format("Missing mandatory PRIMARY KEY part {}", def.name_as_text()));
        }
        exploded.emplace_back(json_value->second);
    }
    auto pkey = partition_key::from_optional_exploded(*s, std::move(exploded));
    validation::validate_cql_key(*s, pkey);
    auto k = query::range<query::ring_position>::make_singular(dht::decorate_key(*s, std::move(pkey)));
    ranges.emplace_back(std::move(k));
    return ranges;
}

query::clustering_row_ranges insert_prepared_json_statement::create_clustering_ranges(const query_options& options, const json_cache_opt& json_cache) const {
    query::clustering_row_ranges ranges;
    std::vector<bytes_opt> exploded;
    for (const auto& def : s->clustering_key_columns()) {
        auto json_value = json_cache->find(def.name_as_text());
        if (json_value == json_cache->end() || !json_value->second) {
            throw exceptions::invalid_request_exception(format("Missing mandatory PRIMARY KEY part {}", def.name_as_text()));
        }
        exploded.emplace_back(json_value->second);
    }
    auto k = query::range<clustering_key_prefix>::make_singular(clustering_key_prefix::from_optional_exploded(*s, std::move(exploded)));
    ranges.emplace_back(query::clustering_range(std::move(k)));
    return ranges;
}

void insert_prepared_json_statement::execute_operations_for_key(mutation& m, const clustering_key_prefix& prefix, const update_parameters& params, const json_cache_opt& json_cache) const {
    for (const auto& def : s->regular_columns()) {
        if (def.type->is_counter()) {
            throw exceptions::invalid_request_exception(format("Cannot set the value of counter column {} in JSON", def.name_as_text()));
        }

        auto it = json_cache->find(def.name_as_text());
        if (it != json_cache->end()) {
            execute_set_value(m, prefix, params, def, it->second);
        } else if (!_default_unset) {
            execute_set_value(m, prefix, params, def, bytes_opt{});
        }
    }
}

namespace raw {

insert_statement::insert_statement(cf_name name,
                                   std::unique_ptr<attributes::raw> attrs,
                                   std::vector<::shared_ptr<column_identifier::raw>> column_names,
                                   std::vector<expr::expression> column_values,
                                   bool if_not_exists)
    : raw::modification_statement{std::move(name), std::move(attrs), std::nullopt /* condition */, if_not_exists, false}
    , _column_names{std::move(column_names)}
    , _column_values{std::move(column_values)}
{ }

::shared_ptr<cql3::statements::modification_statement>
insert_statement::prepare_internal(data_dictionary::database db, schema_ptr schema,
    prepare_context& ctx, std::unique_ptr<attributes> attrs, cql_stats& stats) const
{
    auto stmt = ::make_shared<cql3::statements::insert_statement>(audit_info(), ctx.bound_variables_size(), schema, std::move(attrs), stats);

    // Created from an INSERT
    if (stmt->is_counter()) {
        throw exceptions::invalid_request_exception("INSERT statement are not allowed on counter tables, use UPDATE instead");
    }

    if (_column_names.size() != _column_values.size()) {
        throw exceptions::invalid_request_exception("Unmatched column names/values");
    }

    if (_column_names.empty()) {
        throw exceptions::invalid_request_exception("No columns provided to INSERT");
    }

    std::unordered_set<bytes> column_ids;
    for (size_t i = 0; i < _column_names.size(); i++) {
        auto&& col = _column_names[i];
        auto id = col->prepare_column_identifier(*schema);
        auto def = get_column_definition(*schema, *id);
        if (!def) {
            throw exceptions::invalid_request_exception(format("Unknown identifier {}", *id));
        }
        if (column_ids.contains(id->name())) {
            throw exceptions::invalid_request_exception(format("Multiple definitions found for column {}", *id));
        }
        column_ids.emplace(id->name());

        auto&& value = _column_values[i];

        if (def->is_primary_key()) {
            // Prepare the value against the column it is written to, exactly as
            // the WHERE clause this used to synthesize did: that is what puts
            // the bind marker in the prepare context under this column's name,
            // which is what token-aware routing reads back.
            auto prepared = expr::prepare_expression(value, db, schema->ks_name(), schema.get(),
                    def->column_specification);
            // As operation::set_value::prepare() does for the other columns:
            // evaluate() only knows how to call scalar functions.
            expr::verify_no_aggregate_functions(prepared, "VALUES clause");
            auto reset_processing_pk_column = defer([&ctx] () noexcept { ctx.set_processing_pk_restrictions(false); });
            if (def->is_partition_key()) {
                // So that a non-pure function under a partition key column is
                // registered for caching, and an LWT insert evaluates it once.
                ctx.set_processing_pk_restrictions(true);
            }
            expr::fill_prepare_context(prepared, ctx);
            stmt->add_key_value(*def, std::move(prepared));
        } else {
            auto operation = operation::set_value(value).prepare(db, keyspace(), *def);
            operation->fill_prepare_context(ctx);
            stmt->add_operation(std::move(operation));
        };
    }
    prepare_conditions(db, *schema, ctx, *stmt);
    stmt->validate_addressed_row();
    return stmt;
}

insert_json_statement::insert_json_statement(cf_name name,
                                             std::unique_ptr<attributes::raw> attrs,
                                             expr::expression json_value,
                                             bool if_not_exists,
                                             bool default_unset)
    : raw::modification_statement{name, std::move(attrs), std::nullopt /* condition */, if_not_exists, false}
    , _name(name)
    , _json_value(std::move(json_value))
    , _if_not_exists(if_not_exists)
    , _default_unset(default_unset) { }

::shared_ptr<cql3::statements::modification_statement>
insert_json_statement::prepare_internal(data_dictionary::database db, schema_ptr schema,
    prepare_context& ctx, std::unique_ptr<attributes> attrs, cql_stats& stats) const
{
    // FIXME: handle _if_not_exists. For now, mark it used to quiet the compiler. #8682
    (void)_if_not_exists;
    throwing_assert(expr::is<cql3::expr::untyped_constant>(_json_value) || expr::is<cql3::expr::bind_variable>(_json_value));
    auto json_column_placeholder = ::make_shared<column_identifier>("", true);
    auto prepared_json_value = prepare_expression(_json_value, db, "", nullptr, make_lw_shared<column_specification>("", "", json_column_placeholder, utf8_type));
    expr::verify_no_aggregate_functions(prepared_json_value, "JSON clause");
    expr::fill_prepare_context(prepared_json_value, ctx);
    auto stmt = ::make_shared<cql3::statements::insert_prepared_json_statement>(audit_info(), ctx.bound_variables_size(), schema, std::move(attrs), stats, std::move(prepared_json_value), _default_unset);
    prepare_conditions(db, *schema, ctx, *stmt);
    return stmt;
}

}

}

}
