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

#pragma once

#include "schema_fwd.hh"
#include "column_identifier.hh"
#include "variable_specifications.hh"
#include "restrictions/restriction.hh"
#include "statements/bound.hh"
#include "term.hh"
#include "expr/expression.hh"

namespace cql3 {

class relation : public enable_shared_from_this<relation> {
protected:
    expr::oper_t _relation_type;
public:
    relation(const expr::oper_t& relation_type)
        : _relation_type(relation_type) {
    }
    virtual ~relation() {}

    virtual const expr::oper_t& get_operator() const {
        return _relation_type;
    }

    /**
     * Checks if this relation apply to multiple columns.
     *
     * @return <code>true</code> if this relation apply to multiple columns, <code>false</code> otherwise.
     */
    virtual bool is_multi_column() const {
        return false;
    }

    /**
     * Checks if this relation is a token relation (e.g. <pre>token(a) = token(1)</pre>).
     *
     * @return <code>true</code> if this relation is a token relation, <code>false</code> otherwise.
     */
    virtual bool on_token() const {
        return false;
    }

    /**
     * Checks if the operator of this relation is a <code>CONTAINS</code>.
     * @return <code>true</code>  if the operator of this relation is a <code>CONTAINS</code>, <code>false</code>
     * otherwise.
     */
    virtual bool is_contains() const final {
        return _relation_type == expr::oper_t::CONTAINS;
    }

    /**
     * Checks if the operator of this relation is a <code>CONTAINS_KEY</code>.
     * @return <code>true</code>  if the operator of this relation is a <code>CONTAINS_KEY</code>, <code>false</code>
     * otherwise.
     */
    virtual bool is_contains_key() const final {
        return _relation_type == expr::oper_t::CONTAINS_KEY;
    }

    /**
     * Checks if the operator of this relation is a <code>IN</code>.
     * @return <code>true</code>  if the operator of this relation is a <code>IN</code>, <code>false</code>
     * otherwise.
     */
    virtual bool is_IN() const final {
        return _relation_type == expr::oper_t::IN;
    }

    /**
     * Checks if the operator of this relation is a <code>EQ</code>.
     * @return <code>true</code>  if the operator of this relation is a <code>EQ</code>, <code>false</code>
     * otherwise.
     */
    virtual bool is_EQ() const final {
        return _relation_type == expr::oper_t::EQ;
    }

    /**
     * Checks if the operator of this relation is a <code>Slice</code> (GT, GTE, LTE, LT).
     *
     * @return <code>true</code> if the operator of this relation is a <code>Slice</code>, <code>false</code> otherwise.
     */
    virtual bool is_slice() const final {
        return _relation_type == expr::oper_t::GT
                || _relation_type == expr::oper_t::GTE
                || _relation_type == expr::oper_t::LTE
                || _relation_type ==
            expr::oper_t::LT;
    }

    /**
     * Converts this <code>Relation</code> into a <code>Restriction</code>.
     *
     * @param cfm the Column Family meta data
     * @param boundNames the variables specification where to collect the bind variables
     * @return the <code>Restriction</code> corresponding to this <code>Relation</code>
     * @throws InvalidRequestException if this <code>Relation</code> is not valid
     */
    virtual ::shared_ptr<restrictions::restriction> to_restriction(database& db, schema_ptr schema, variable_specifications& bound_names) final {
        if (_relation_type == expr::oper_t::EQ) {
            return new_EQ_restriction(db, schema, bound_names);
        } else if (_relation_type == expr::oper_t::LT) {
            return new_slice_restriction(db, schema, bound_names, statements::bound::END, false);
        } else if (_relation_type == expr::oper_t::LTE) {
            return new_slice_restriction(db, schema, bound_names, statements::bound::END, true);
        } else if (_relation_type == expr::oper_t::GTE) {
            return new_slice_restriction(db, schema, bound_names, statements::bound::START, true);
        } else if (_relation_type == expr::oper_t::GT) {
            return new_slice_restriction(db, schema, bound_names, statements::bound::START, false);
        } else if (_relation_type == expr::oper_t::IN) {
            return new_IN_restriction(db, schema, bound_names);
        } else if (_relation_type == expr::oper_t::CONTAINS) {
            return new_contains_restriction(db, schema, bound_names, false);
        } else if (_relation_type == expr::oper_t::CONTAINS_KEY) {
            return new_contains_restriction(db, schema, bound_names, true);
        } else if (_relation_type == expr::oper_t::IS_NOT) {
            // This case is not supposed to happen: statement_restrictions
            // constructor does not call this function for views' IS_NOT.
            throw exceptions::invalid_request_exception(format("Unsupported \"IS NOT\" relation: {}", to_string()));
        } else if (_relation_type == expr::oper_t::LIKE) {
            return new_LIKE_restriction(db, schema, bound_names);
        } else {
            throw exceptions::invalid_request_exception(format("Unsupported \"!=\" relation: {}", to_string()));
        }
    }

    virtual sstring to_string() const = 0;

    friend std::ostream& operator<<(std::ostream& out, const relation& r) {
        return out << r.to_string();
    }

    /**
     * Creates a new EQ restriction instance.
     *
     * @param cfm the Column Family meta data
     * @param boundNames the variables specification where to collect the bind variables
     * @return a new EQ restriction instance.
     * @throws InvalidRequestException if the relation cannot be converted into an EQ restriction.
     */
    virtual ::shared_ptr<restrictions::restriction> new_EQ_restriction(database& db, schema_ptr schema,
        variable_specifications& bound_names) = 0;

    /**
     * Creates a new IN restriction instance.
     *
     * @param cfm the Column Family meta data
     * @param bound_names the variables specification where to collect the bind variables
     * @return a new IN restriction instance
     * @throws InvalidRequestException if the relation cannot be converted into an IN restriction.
     */
    virtual ::shared_ptr<restrictions::restriction> new_IN_restriction(database& db, schema_ptr schema,
        variable_specifications& bound_names) = 0;

    /**
     * Creates a new Slice restriction instance.
     *
     * @param cfm the Column Family meta data
     * @param bound_names the variables specification where to collect the bind variables
     * @param bound the slice bound
     * @param inclusive <code>true</code> if the bound is included.
     * @return a new slice restriction instance
     * @throws InvalidRequestException if the <code>Relation</code> is not valid
     */
    virtual ::shared_ptr<restrictions::restriction> new_slice_restriction(database& db, schema_ptr schema,
        variable_specifications& bound_names,
        statements::bound bound,
        bool inclusive) = 0;

    /**
     * Creates a new Contains restriction instance.
     *
     * @param cfm the Column Family meta data
     * @param bound_names the variables specification where to collect the bind variables
     * @param isKey <code>true</code> if the restriction to create is a CONTAINS KEY
     * @return a new Contains <code>::shared_ptr<restrictions::restriction></code> instance
     * @throws InvalidRequestException if the <code>Relation</code> is not valid
     */
    virtual ::shared_ptr<restrictions::restriction> new_contains_restriction(database& db, schema_ptr schema,
        variable_specifications& bound_names, bool isKey) = 0;

    /**
     * Creates a new LIKE restriction instance.
     */
    virtual ::shared_ptr<restrictions::restriction> new_LIKE_restriction(database& db, schema_ptr schema,
        variable_specifications& bound_names) = 0;

    /**
     * Renames an identifier in this Relation, if applicable.
     * @param from the old identifier
     * @param to the new identifier
     * @return a pointer object, if the old identifier is not in the set of entities that this relation covers;
     *         otherwise a new Relation with "from" replaced by "to" is returned.
     */
    virtual ::shared_ptr<relation> maybe_rename_identifier(const column_identifier::raw& from, column_identifier::raw to) = 0;

protected:

    /**
     * Converts the specified <code>Raw</code> into a <code>Term</code>.
     * @param receivers the columns to which the values must be associated at
     * @param raw the raw term to convert
     * @param keyspace the keyspace name
     * @param boundNames the variables specification where to collect the bind variables
     *
     * @return the <code>Term</code> corresponding to the specified <code>Raw</code>
     * @throws InvalidRequestException if the <code>Raw</code> term is not valid
     */
    virtual ::shared_ptr<term> to_term(const std::vector<lw_shared_ptr<column_specification>>& receivers,
                                       const term::raw& raw,
                                       database& db,
                                       const sstring& keyspace,
                                       variable_specifications& boundNames) const = 0;

    /**
     * Converts the specified <code>Raw</code> terms into a <code>Term</code>s.
     * @param receivers the columns to which the values must be associated at
     * @param raws the raw terms to convert
     * @param keyspace the keyspace name
     * @param boundNames the variables specification where to collect the bind variables
     *
     * @return the <code>Term</code>s corresponding to the specified <code>Raw</code> terms
     * @throws InvalidRequestException if the <code>Raw</code> terms are not valid
     */
    std::vector<::shared_ptr<term>> to_terms(const std::vector<lw_shared_ptr<column_specification>>& receivers,
                                             const std::vector<::shared_ptr<term::raw>>& raws,
                                             database& db,
                                             const sstring& keyspace,
                                             variable_specifications& boundNames) const {
        std::vector<::shared_ptr<term>> terms;
        for (const auto& r : raws) {
            terms.emplace_back(to_term(receivers, *r, db, keyspace, boundNames));
        }
        return terms;
    }

    /**
     * Converts the specified entity into a column definition.
     *
     * @param cfm the column family meta data
     * @param entity the entity to convert
     * @return the column definition corresponding to the specified entity
     * @throws InvalidRequestException if the entity cannot be recognized
     */
    virtual const column_definition& to_column_definition(const schema& schema, const column_identifier::raw& entity) final;
};

using relation_ptr = ::shared_ptr<relation>;

}
