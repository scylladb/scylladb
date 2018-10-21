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
 * Copyright (C) 2015 ScyllaDB
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

#pragma once

#include "cql3/statements/raw/cf_statement.hh"
#include "cql3/statements/prepared_statement.hh"
#include "cql3/cql_statement.hh"
#include "cql3/selection/selection.hh"
#include "cql3/selection/raw_selector.hh"
#include "cql3/restrictions/statement_restrictions.hh"
#include "cql3/result_set.hh"
#include "exceptions/unrecognized_entity_exception.hh"
#include "service/client_state.hh"
#include "core/shared_ptr.hh"
#include "core/distributed.hh"
#include "validation.hh"

namespace cql3 {

namespace statements {

namespace raw {

/**
 * Encapsulates a completely parsed SELECT query, including the target
 * column family, expression, result count, and ordering clause.
 *
 */
class select_statement : public cf_statement
{
public:
    class parameters final {
    public:
        using orderings_type = std::vector<std::pair<shared_ptr<column_identifier::raw>, bool>>;
    private:
        const orderings_type _orderings;
        const bool _is_distinct;
        const bool _allow_filtering;
        const bool _is_json;
    public:
        parameters();
        parameters(orderings_type orderings,
            bool is_distinct,
            bool allow_filtering);
        parameters(orderings_type orderings,
            bool is_distinct,
            bool allow_filtering,
            bool is_json);
        bool is_distinct() const;
        bool allow_filtering() const;
        bool is_json() const;
        orderings_type const& orderings() const;
    };
    template<typename T>
    using compare_fn = std::function<bool(const T&, const T&)>;

    using result_row_type = std::vector<bytes_opt>;
    using ordering_comparator_type = compare_fn<result_row_type>;
private:
    ::shared_ptr<parameters> _parameters;
    std::vector<::shared_ptr<selection::raw_selector>> _select_clause;
    std::vector<::shared_ptr<relation>> _where_clause;
    ::shared_ptr<term::raw> _limit;
public:
    select_statement(::shared_ptr<cf_name> cf_name,
            ::shared_ptr<parameters> parameters,
            std::vector<::shared_ptr<selection::raw_selector>> select_clause,
            std::vector<::shared_ptr<relation>> where_clause,
            ::shared_ptr<term::raw> limit);

    virtual std::unique_ptr<prepared> prepare(database& db, cql_stats& stats) override {
        return prepare(db, stats, false);
    }
    std::unique_ptr<prepared> prepare(database& db, cql_stats& stats, bool for_view);
private:
    void maybe_jsonize_select_clause(database& db, schema_ptr schema);
    ::shared_ptr<restrictions::statement_restrictions> prepare_restrictions(
        database& db,
        schema_ptr schema,
        ::shared_ptr<variable_specifications> bound_names,
        ::shared_ptr<selection::selection> selection,
        bool for_view = false,
        bool allow_filtering = false);

    /** Returns a ::shared_ptr<term> for the limit or null if no limit is set */
    ::shared_ptr<term> prepare_limit(database& db, ::shared_ptr<variable_specifications> bound_names);

    static void verify_ordering_is_allowed(::shared_ptr<restrictions::statement_restrictions> restrictions);

    static void validate_distinct_selection(schema_ptr schema,
        ::shared_ptr<selection::selection> selection,
        ::shared_ptr<restrictions::statement_restrictions> restrictions);

    void handle_unrecognized_ordering_column(::shared_ptr<column_identifier> column);

    select_statement::ordering_comparator_type get_ordering_comparator(schema_ptr schema,
        ::shared_ptr<selection::selection> selection,
        ::shared_ptr<restrictions::statement_restrictions> restrictions);

    bool is_reversed(schema_ptr schema);

    /** If ALLOW FILTERING was not specified, this verifies that it is not needed */
    void check_needs_filtering(::shared_ptr<restrictions::statement_restrictions> restrictions);

    void ensure_filtering_columns_retrieval(database& db,
                                            ::shared_ptr<selection::selection> selection,
                                            ::shared_ptr<restrictions::statement_restrictions> restrictions);

    bool contains_alias(::shared_ptr<column_identifier> name);

    ::shared_ptr<column_specification> limit_receiver();

#if 0
    public:
        virtual sstring to_string() override {
            return sstring("raw_statement(")
                + "name=" + cf_name->to_string()
                + ", selectClause=" + to_string(_select_clause)
                + ", whereClause=" + to_string(_where_clause)
                + ", isDistinct=" + to_string(_parameters->is_distinct())
                + ", isJson=" + to_string(_parameters->is_json())
                + ")";
        }
    };
#endif
};

}

}

}
