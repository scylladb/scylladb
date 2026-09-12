/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.1 and Apache-2.0)
 */

#include "utils/assert.hh"
#include "update_statement.hh"
#include "cql3/expr/expression.hh"
#include "cql3/expr/evaluate.hh"
#include "cql3/expr/expr-utils.hh"
#include "raw/update_statement.hh"

#include "unimplemented.hh"

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
#include "dht/i_partitioner.hh"
#include <optional>

namespace cql3 {


namespace statements {

update_statement::update_statement(
        audit::audit_info_ptr&& audit_info,
        statement_type type,
        uint32_t bound_terms,
        schema_ptr s,
        std::unique_ptr<attributes> attrs,
        cql_stats& stats)
    : modification_statement{type, bound_terms, std::move(s), std::move(attrs), stats}
{
    set_audit_info(std::move(audit_info));
}

clustering_key_prefix row_key(const query::clustering_range& range) {
    return range.start() ? std::move(range.start()->value()) : clustering_key_prefix::make_empty();
}

void open_row(const schema& s, statement_type type, bool has_column_operations,
        mutation& m, const clustering_key_prefix& prefix, const update_parameters& params) {
    if (s.is_dense()) {
        if (prefix.is_empty(s) || prefix.components().front().empty()) {
            throw exceptions::invalid_request_exception(format("Missing PRIMARY KEY part {}", s.clustering_key_columns().begin()->name_as_text()));
        }
        // An empty name for the value is what we use to recognize the case where there is not column
        // outside the PK, see CreateStatement.
        // Since v3 schema we use empty_type instead, see schema.cc.
        auto rb = s.regular_begin();
        if (rb->name().empty() || rb->type == empty_type) {
            // There is no column outside the PK. So no operation could have passed through validation
            throwing_assert(!has_column_operations);
            constants::setter(*s.regular_begin(), expr::constant(cql3::raw_value::make_value(bytes()), empty_type)).execute(m, prefix, params);
        } else {
            // dense means we don't have a row marker, so don't accept to set only the PK. See CASSANDRA-5648.
            if (!has_column_operations) {
                throw exceptions::invalid_request_exception(format("Column {} is mandatory for this COMPACT STORAGE table", s.regular_begin()->name_as_text()));
            }
        }
    } else {
        // If there are static columns, there also must be clustering columns, in which
        // case empty prefix can only refer to the static row.
        bool is_static_prefix = s.has_static_columns() && prefix.is_empty(s);
        if (type.is_insert() && !is_static_prefix && s.is_cql3_table()) {
            auto& row = m.partition().clustered_row(s, prefix);
            row.apply(row_marker(params.timestamp(), params.ttl(), params.expiry()));
        }
    }
}

void apply_column_operations(const std::vector<std::unique_ptr<operation>>& ops,
        mutation& m, const clustering_key_prefix& prefix, const update_parameters& params) {
    for (auto&& update : ops) {
        if (update->should_skip_operation(params._options)) {
            continue;
        }
        update->execute(m, prefix, params);
    }
}

void update_statement::execute_operations_for_key(mutation& m, const clustering_key_prefix& prefix, const update_parameters& params, const json_cache_opt& json_cache) const {
    apply_column_operations(_column_operations, m, prefix, params);
}

utils::chunked_vector<mutation> update_statement::apply_updates(
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

    return mutations;
}


namespace raw {

update_statement::update_statement(cf_name name,
                                   std::unique_ptr<attributes::raw> attrs,
                                   std::vector<std::pair<::shared_ptr<column_identifier::raw>, std::unique_ptr<operation::raw_update>>> updates,
                                   expr::expression where_clause,
                                   std::optional<expr::expression> conditions, bool if_exists)
    : raw::modification_statement(std::move(name), std::move(attrs), std::move(conditions), false, if_exists)
    , _updates(std::move(updates))
    , _where_clause(std::move(where_clause))
{ }

::shared_ptr<cql3::statements::modification_statement>
update_statement::prepare_internal(data_dictionary::database db, schema_ptr schema,
    prepare_context& ctx, std::unique_ptr<attributes> attrs, cql_stats& stats) const
{
    auto stmt = ::make_shared<cql3::statements::update_statement>(audit_info(), statement_type::UPDATE, ctx.bound_variables_size(), schema, std::move(attrs), stats);

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
    if (stmt->requires_lwt() && !stmt->has_conditions()) {
        throw exceptions::invalid_request_exception(
            "SET with a column expression (e.g. col = col + 1) requires an LWT condition (e.g., IF col != NULL or IF EXISTS) to ensure atomic read-before-write");
    }
    return stmt;
}

}

}

}
