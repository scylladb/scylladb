/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#include <boost/algorithm/cxx11/all_of.hpp>
#include <boost/range/adaptors.hpp>

#include "cql3/tuples.hh"
#include "database.hh"
#include "delete_statement.hh"
#include "raw/delete_statement.hh"
#include "utils/overloaded_functor.hh"

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
        op->execute(m, range.start() ? std::move(range.start()->value()) : clustering_key_prefix::make_empty(), params);
    }
}

namespace raw {

namespace {

using namespace expr;

/// If oper.lhs is a single column, returns it; otherwise, returns null.
const column_definition* single_column(const binary_operator& oper) {
    if (auto c = std::get_if<column_value>(&oper.lhs)) {
        return c->col;
    }
    return nullptr;
}

/// True iff expr bounds clustering key from both above and below OR it has no clustering-key bounds at all.
/// See #6493.
bool bounds_ck_symmetrically(const expression& expr) {
    /// A visitor to find out if CK boundedness is symmetric.
    class boundedness_tracker {
        using boundedness_bitvector = int; // Combined using binary OR.
        const boundedness_bitvector UPPER=1, LOWER=2;
        /// Individual bounds collected from the visiting expression.  May have a nullptr entry, which
        /// represents multi-column bounds encountered.
        std::unordered_map<const column_definition*, boundedness_bitvector> _found_bounds;

        bool _shortcircuit = false; ///< When true, cease all further visiting and declare boundedness symmetric.

      public:
        /// True iff the nodes visited so far do bound the CK symmetrically.
        bool result() const {
            return _shortcircuit ||
                    // Since multi-column comparisons can't be mixed with single-column ones, _found_bounds will
                    // either have a single entry with key nullptr or one entry per restricted column.
                    boost::algorithm::all_of_equal(_found_bounds | boost::adaptors::map_values, UPPER | LOWER);
        }

        /// Updates state for a boolean expression.
        void operator()(bool b) {
            // b==true doesn't change the current state; b==false shortcircuits the entire expression to empty set.
            if (!b) {
                _shortcircuit = true;
            }
        }

        /// Updates state for a binary-operator expression.
        void operator()(const binary_operator& oper) {
            if (std::holds_alternative<token>(oper.lhs)) {
                return;
            }
            // The rules of multi-column comparison imply that any multi-column expression sets a bound for the
            // entire clustering key.  Therefore, we represent any such expression with special pointer value
            // nullptr.
            auto col = single_column(oper);
            if (col && !col->is_clustering_key()) {
                return;
            }
            if (oper.op == oper_t::EQ) {
                _found_bounds[col] = UPPER | LOWER;
            } else if (oper.op == oper_t::LT || oper.op == oper_t::LTE) {
                _found_bounds[col] |= UPPER;
            } else if (oper.op == oper_t::GTE || oper.op == oper_t::GT) {
                _found_bounds[col] |= LOWER;
            }
        }

        /// Updates state for a conjunction.
        void operator()(const conjunction& conj) {
            for (const auto& child : conj.children) {
                std::visit(*this, child);
                if (_shortcircuit) {
                    break;
                }
            }
        }
    } tracker;

    std::visit(tracker, expr);
    return tracker.result();
}

} // anonymous namespace

::shared_ptr<cql3::statements::modification_statement>
delete_statement::prepare_internal(database& db, schema_ptr schema, variable_specifications& bound_names,
        std::unique_ptr<attributes> attrs, cql_stats& stats) const {
    auto stmt = ::make_shared<cql3::statements::delete_statement>(statement_type::DELETE, bound_names.size(), schema, std::move(attrs), stats);

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
        op->collect_marker_specification(bound_names);
        stmt->add_operation(op);
    }
    prepare_conditions(db, *schema, bound_names, *stmt);
    stmt->process_where_clause(db, _where_clause, bound_names);
    if (!db.supports_infinite_bound_range_deletions() &&
        !bounds_ck_symmetrically(stmt->restrictions().get_clustering_columns_restrictions()->expression)) {
        throw exceptions::invalid_request_exception(
                "A range deletion operation needs to specify both bounds for clusters without sstable mc format support");
    }
    if (has_slice(stmt->restrictions().get_clustering_columns_restrictions()->expression)) {
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
                                 std::vector<::shared_ptr<relation>> where_clause,
                                 conditions_vector conditions,
                                 bool if_exists)
    : raw::modification_statement(std::move(name), std::move(attrs), std::move(conditions), false, if_exists)
    , _deletions(std::move(deletions))
    , _where_clause(std::move(where_clause))
{ }

}

}

}
