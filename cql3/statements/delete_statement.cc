/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include "utils/assert.hh"
#include <boost/algorithm/cxx11/all_of.hpp>
#include <boost/range/adaptors.hpp>

#include "data_dictionary/data_dictionary.hh"
#include "delete_statement.hh"
#include "raw/delete_statement.hh"
#include "mutation/mutation.hh"
#include "cql3/expr/expression.hh"
#include "cql3/expr/expr-utils.hh"

namespace cql3 {

namespace statements {

delete_statement::delete_statement(statement_type type, uint32_t bound_terms, schema_ptr s, std::unique_ptr<attributes> attrs, cql_stats& stats)
        : modification_statement{type, bound_terms, std::move(s), std::move(attrs), stats}
{ }

bool delete_statement::require_full_clustering_key() const {
    return false;
}

bool delete_statement::allow_clustering_key_slices() const {
    return true;
}

void delete_statement::add_update_for_key(mutation& m, const query::clustering_range& range, const update_parameters& params, const json_cache_opt& json_cache) const {
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
    auto stmt = ::make_shared<cql3::statements::delete_statement>(statement_type::DELETE, ctx.bound_variables_size(), schema, std::move(attrs), stats);

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

        auto&& op = deletion->prepare(db, schema->ks_name(), *def);
        op->fill_prepare_context(ctx);
        stmt->add_operation(op);
    }
    prepare_conditions(db, *schema, ctx, *stmt);
    stmt->process_where_clause(db, _where_clause, ctx);
    if (has_slice(stmt->restrictions().get_clustering_columns_restrictions())) {
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
    SCYLLA_ASSERT(!_attrs->time_to_live.has_value());
}

}

}

}
