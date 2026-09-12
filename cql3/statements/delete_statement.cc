/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.1 and Apache-2.0)
 */

#include "utils/assert.hh"

#include "data_dictionary/data_dictionary.hh"
#include "delete_statement.hh"
#include "validation.hh"
#include "raw/delete_statement.hh"
#include "mutation/mutation.hh"
#include "cql3/expr/expression.hh"
#include "cql3/expr/expr-utils.hh"

namespace cql3 {

namespace statements {

delete_statement::delete_statement(audit::audit_info_ptr&& audit_info, statement_type type, uint32_t bound_terms, schema_ptr s, std::unique_ptr<attributes> attrs, cql_stats& stats)
        : modification_statement{type, bound_terms, std::move(s), std::move(attrs), stats}
{
    set_audit_info(std::move(audit_info));
}

dht::partition_range_vector
delete_statement::build_partition_keys(const query_options& options, const json_cache_opt& json_cache) const {
    auto keys = _restrictions->get_partition_key_ranges(options);
    for (auto const& k : keys) {
        validation::validate_cql_key(*s, *k.start()->value().key());
    }
    return keys;
}

query::clustering_row_ranges
delete_statement::create_clustering_ranges(const query_options& options, const json_cache_opt& json_cache) const {
    return _restrictions->clustering_ranges(options);
}

void delete_statement::validate_primary_key(const query_options& options) const {
    _restrictions->validate_primary_key(options);
}

void delete_statement::process_where_clause(data_dictionary::database db, expr::expression where_clause, prepare_context& ctx) {
    _restrictions = restrictions::analyze_delete_restrictions(db, s, where_clause, ctx,
            applies_only_to_static_columns());
    classify_exists_condition(_restrictions->has_clustering_columns_restriction());
    // A DELETE may name a range of rows, so it need not name the whole clustering
    // key - but it cannot then delete a particular regular column of them.
    if (auto* missing = _restrictions->clustering_column_required_for_regular_columns(
                applies_only_to_static_columns())) {
        for (auto&& op : _column_operations) {
            if (!op->column.is_static()) {
                throw exceptions::invalid_request_exception(format("Primary key column '{}' must be specified in order to modify column '{}'",
                    missing->name_as_text(), op->column.name_as_text()));
            }
        }
    }
    _restrictions->reject_incomplete_partition_key();
    if (has_conditions()) {
        validate_where_clause_for_conditions();
    }
}

void delete_statement::validate_where_clause_for_conditions() const {
    reject_in_relations_with_conditions(_restrictions->key_is_in_relation(),
            _restrictions->clustering_key_restrictions_has_IN());

    if (_restrictions->addresses_exact_rows()) {
        return;
    }
    bool deletes_regular_columns = _column_operations.empty() ||
        std::any_of(_column_operations.begin(), _column_operations.end(), [] (auto&& op) {
            return !op->column.is_static();
        });
    // For example, primary key is (a, b, c), only a and b are restricted
    if (deletes_regular_columns) {
        throw exceptions::invalid_request_exception(
                "DELETE statements must restrict all PRIMARY KEY columns with equality relations"
                " in order to delete non static columns");
    }

    // All primary key parts must be specified, unless this statement has only static column conditions
    if (has_regular_column_conditions()) {
        throw exceptions::invalid_request_exception(
                "DELETE statements must restrict all PRIMARY KEY columns with equality relations"
                " in order to use IF condition on non static columns");
    }
}

utils::chunked_vector<mutation> delete_statement::apply_updates(
        const std::vector<dht::partition_range>& keys,
        const std::vector<query::clustering_range>& ranges,
        const update_parameters& params,
        const json_cache_opt& json_cache) const {
    auto mutations = make_mutations(keys);
    for (auto& m : mutations) {
        for (auto&& range : ranges) {
            delete_row_range(m, range, params);
        }
    }
    return mutations;
}

void delete_statement::delete_row_range(mutation& m, const query::clustering_range& range, const update_parameters& params) const {
    if (_column_operations.empty()) {
        if (s->clustering_key_size() == 0 || range.is_full()) {
            m.partition().apply(params.make_tombstone());
        } else if (range.is_singular()) {
            m.partition().apply_delete(*s, range.start()->value(), params.make_tombstone());
        } else {
            auto bvs = bound_view::from_range(range);
            m.partition().apply_delete(*s, range_tombstone(bvs.first, bvs.second, params.make_tombstone()));
        }
        return;
    }

    for (auto&& op : _column_operations) {
        if (op->should_skip_operation(params._options)) {
            continue;
        }
        op->execute(m, range.start() ? std::move(range.start()->value()) : clustering_key_prefix::make_empty(), params);
    }
}

namespace raw {

::shared_ptr<cql3::statements::modification_statement>
delete_statement::prepare_internal(data_dictionary::database db, schema_ptr schema, prepare_context& ctx,
        std::unique_ptr<attributes> attrs, cql_stats& stats) const {
    auto stmt = ::make_shared<cql3::statements::delete_statement>(audit_info(), statement_type::DELETE, ctx.bound_variables_size(), schema, std::move(attrs), stats);

    for (auto&& deletion : _deletions) {
        auto&& id = deletion->affected_column().prepare_column_identifier(*schema);
        auto def = get_column_definition(*schema, *id);
        if (!def) {
            throw exceptions::invalid_request_exception(format("Unknown identifier {}", *id));
        }

        // For compact, we only have one value except the key, so the only form of DELETE that make sense is without a column
        // list. However, we support having the value name for coherence with the static/sparse case
        if (def->is_primary_key()) {
            throw exceptions::invalid_request_exception(format("Invalid identifier {} for deletion (should not be a PRIMARY KEY part)", def->name_as_text()));
        }

        auto op = deletion->prepare(db, schema->ks_name(), *def);
        op->fill_prepare_context(ctx);
        stmt->add_operation(std::move(op));
    }
    prepare_conditions(db, *schema, ctx, *stmt);
    stmt->process_where_clause(db, _where_clause, ctx);
    if (stmt->restrictions().deletes_a_range()) {
        if (!schema->is_compound()) {
            throw exceptions::invalid_request_exception("Range deletions on \"compact storage\" schemas are not supported");
        }
        if (!_deletions.empty()) {
            throw exceptions::invalid_request_exception("Range deletions are not supported for specific columns");
        }
    }
    return stmt;
}

delete_statement::delete_statement(cf_name name,
                                 std::unique_ptr<attributes::raw> attrs,
                                 std::vector<std::unique_ptr<operation::raw_deletion>> deletions,
                                 expr::expression where_clause,
                                 std::optional<expr::expression> conditions,
                                 bool if_exists)
    : raw::modification_statement(std::move(name), std::move(attrs), std::move(conditions), false, if_exists)
    , _deletions(std::move(deletions))
    , _where_clause(std::move(where_clause))
{
    throwing_assert(!_attrs->time_to_live.has_value());
}

}

}

}
