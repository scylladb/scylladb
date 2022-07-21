/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include "update_statement.hh"
#include "raw/update_statement.hh"

#include "raw/insert_statement.hh"
#include "unimplemented.hh"

#include "cql3/operation_impl.hh"
#include "cql3/type_json.hh"
#include "types/map.hh"
#include "types/set.hh"
#include "types/list.hh"
#include "types/user.hh"
#include "concrete_types.hh"
#include "validation.hh"

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
parse(const sstring& json_string, const std::vector<column_definition>& expected_receivers, cql_serialization_format sf) {
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
            json_map.emplace(std::move(cql_name), from_json_object(*def.type, std::move(value_it->second), sf));
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

update_statement::update_statement(
        statement_type type,
        uint32_t bound_terms,
        schema_ptr s,
        std::unique_ptr<attributes> attrs,
        cql_stats& stats)
    : modification_statement{type, bound_terms, std::move(s), std::move(attrs), stats}
{ }

bool update_statement::require_full_clustering_key() const {
    return true;
}

bool update_statement::allow_clustering_key_slices() const {
    return false;
}

void update_statement::execute_operations_for_key(mutation& m, const clustering_key_prefix& prefix, const update_parameters& params, const json_cache_opt& json_cache) const {
    for (auto&& update : _column_operations) {
        update->execute(m, prefix, params);
    }
}

void update_statement::add_update_for_key(mutation& m, const query::clustering_range& range, const update_parameters& params, const json_cache_opt& json_cache) const {
    auto prefix = range.start() ? std::move(range.start()->value()) : clustering_key_prefix::make_empty();
    if (s->is_dense()) {
        if (prefix.is_empty(*s) || prefix.components().front().empty()) {
            throw exceptions::invalid_request_exception(format("Missing PRIMARY KEY part {}", s->clustering_key_columns().begin()->name_as_text()));
        }
        // An empty name for the value is what we use to recognize the case where there is not column
        // outside the PK, see CreateStatement.
        // Since v3 schema we use empty_type instead, see schema.cc.
        auto rb = s->regular_begin();
        if (rb->name().empty() || rb->type == empty_type) {
            // There is no column outside the PK. So no operation could have passed through validation
            assert(_column_operations.empty());
            constants::setter(*s->regular_begin(), expr::constant(cql3::raw_value::make_value(bytes()), empty_type)).execute(m, prefix, params);
        } else {
            // dense means we don't have a row marker, so don't accept to set only the PK. See CASSANDRA-5648.
            if (_column_operations.empty()) {
                throw exceptions::invalid_request_exception(format("Column {} is mandatory for this COMPACT STORAGE table", s->regular_begin()->name_as_text()));
            }
        }
    } else {
        // If there are static columns, there also must be clustering columns, in which
        // case empty prefix can only refer to the static row.
        bool is_static_prefix = s->has_static_columns() && prefix.is_empty(*s);
        if (type.is_insert() && !is_static_prefix && s->is_cql3_table()) {
            auto& row = m.partition().clustered_row(*s, prefix);
            row.apply(row_marker(params.timestamp(), params.ttl(), params.expiry()));
        }
    }

    execute_operations_for_key(m, prefix, params, json_cache);

    warn(unimplemented::cause::INDEXES);
#if 0
        SecondaryIndexManager indexManager = Keyspace.open(cfm.ksName).getColumnFamilyStore(cfm.cfId).indexManager;
        if (indexManager.hasIndexes())
        {
            for (Cell cell : cf)
            {
                // Indexed values must be validated by any applicable index. See CASSANDRA-3057/4240/8081 for more details
                if (!indexManager.validate(cell))
                    throw new InvalidRequestException(String.format("Can't index column value of size %d for index %s on %s.%s",
                                                                    cell.value().remaining(),
                                                                    cfm.getColumnDefinition(cell.name()).getIndexName(),
                                                                    cfm.ksName,
                                                                    cfm.cfName));
            }
        }
    }
#endif
}

modification_statement::json_cache_opt insert_prepared_json_statement::maybe_prepare_json_cache(const query_options& options) const {
    cql3::raw_value c = expr::evaluate(_value, options);
    sstring json_string = utf8_type->to_string(to_bytes(c.view()));
    return json_helpers::parse(std::move(json_string), s->all_columns(), cql_serialization_format::internal());
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
    : raw::modification_statement{std::move(name), std::move(attrs), conditions_vector{}, if_not_exists, false}
    , _column_names{std::move(column_names)}
    , _column_values{std::move(column_values)}
{ }

::shared_ptr<cql3::statements::modification_statement>
insert_statement::prepare_internal(data_dictionary::database db, schema_ptr schema,
    prepare_context& ctx, std::unique_ptr<attributes> attrs, cql_stats& stats) const
{
    auto stmt = ::make_shared<cql3::statements::update_statement>(statement_type::INSERT, ctx.bound_variables_size(), schema, std::move(attrs), stats);

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

    std::vector<expr::expression> relations;
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
            relations.push_back(expr::binary_operator(expr::unresolved_identifier{col}, expr::oper_t::EQ, value));
        } else {
            auto operation = operation::set_value(value).prepare(db, keyspace(), *def);
            operation->fill_prepare_context(ctx);
            stmt->add_operation(std::move(operation));
        };
    }
    prepare_conditions(db, *schema, ctx, *stmt);
    stmt->process_where_clause(db, expr::conjunction{relations}, ctx);
    return stmt;
}

insert_json_statement::insert_json_statement(cf_name name,
                                             std::unique_ptr<attributes::raw> attrs,
                                             expr::expression json_value,
                                             bool if_not_exists,
                                             bool default_unset)
    : raw::modification_statement{name, std::move(attrs), conditions_vector{}, if_not_exists, false}
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
    assert(expr::is<cql3::expr::untyped_constant>(_json_value) || expr::is<cql3::expr::bind_variable>(_json_value));
    auto json_column_placeholder = ::make_shared<column_identifier>("", true);
    auto prepared_json_value = prepare_expression(_json_value, db, "", nullptr, make_lw_shared<column_specification>("", "", json_column_placeholder, utf8_type));
    expr::fill_prepare_context(prepared_json_value, ctx);
    auto stmt = ::make_shared<cql3::statements::insert_prepared_json_statement>(ctx.bound_variables_size(), schema, std::move(attrs), stats, std::move(prepared_json_value), _default_unset);
    prepare_conditions(db, *schema, ctx, *stmt);
    return stmt;
}

update_statement::update_statement(cf_name name,
                                   std::unique_ptr<attributes::raw> attrs,
                                   std::vector<std::pair<::shared_ptr<column_identifier::raw>, std::unique_ptr<operation::raw_update>>> updates,
                                   expr::expression where_clause,
                                   conditions_vector conditions, bool if_exists)
    : raw::modification_statement(std::move(name), std::move(attrs), std::move(conditions), false, if_exists)
    , _updates(std::move(updates))
    , _where_clause(std::move(where_clause))
{ }

::shared_ptr<cql3::statements::modification_statement>
update_statement::prepare_internal(data_dictionary::database db, schema_ptr schema,
    prepare_context& ctx, std::unique_ptr<attributes> attrs, cql_stats& stats) const
{
    auto stmt = ::make_shared<cql3::statements::update_statement>(statement_type::UPDATE, ctx.bound_variables_size(), schema, std::move(attrs), stats);

    // FIXME: quadratic
    for (size_t i = 0; i < _updates.size(); ++i) {
        auto& ui = _updates[i];
        for (size_t j = i + 1; j < _updates.size(); ++j) {
            auto& uj = _updates[j];
            if (*ui.first == *uj.first && !uj.second->is_compatible_with(ui.second)) {
                throw exceptions::invalid_request_exception(format("Multiple incompatible setting of column {}", *ui.first));
            }
        }
    }

    for (auto&& entry : _updates) {
        auto id = entry.first->prepare_column_identifier(*schema);
        auto def = get_column_definition(*schema, *id);
        if (!def) {
            throw exceptions::invalid_request_exception(format("Unknown identifier {}", *entry.first));
        }

        auto operation = entry.second->prepare(db, keyspace(), *def);
        operation->fill_prepare_context(ctx);

        if (def->is_primary_key()) {
            throw exceptions::invalid_request_exception(format("PRIMARY KEY part {} found in SET part", *entry.first));
        }
        stmt->add_operation(std::move(operation));
    }
    prepare_conditions(db, *schema, ctx, *stmt);
    stmt->process_where_clause(db, _where_clause, ctx);
    return stmt;
}

}

}

}
