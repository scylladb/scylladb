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
 * Copyright 2015 Cloudius Systems
 *
 * Modified by Cloudius Systems
 */

#pragma once

#include "operator.hh"
#include "database.hh"
#include "column_identifier.hh"
#include "variable_specifications.hh"
#include "restrictions/restriction.hh"
#include "statements/bound.hh"

namespace cql3 {

class relation : public enable_shared_from_this<relation> {
protected:
    const operator_type& _relation_type;
public:
    relation(const operator_type& relation_type)
        : _relation_type(relation_type) {
    }
    virtual ~relation() {}

    virtual const operator_type& get_operator() {
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
        return _relation_type == operator_type::CONTAINS;
    }

    /**
     * Checks if the operator of this relation is a <code>CONTAINS_KEY</code>.
     * @return <code>true</code>  if the operator of this relation is a <code>CONTAINS_KEY</code>, <code>false</code>
     * otherwise.
     */
    virtual bool is_contains_key() const final {
        return _relation_type == operator_type::CONTAINS_KEY;
    }

    /**
     * Checks if the operator of this relation is a <code>IN</code>.
     * @return <code>true</code>  if the operator of this relation is a <code>IN</code>, <code>false</code>
     * otherwise.
     */
    virtual bool is_IN() const final {
        return _relation_type == operator_type::IN;
    }

    /**
     * Checks if the operator of this relation is a <code>EQ</code>.
     * @return <code>true</code>  if the operator of this relation is a <code>EQ</code>, <code>false</code>
     * otherwise.
     */
    virtual bool is_EQ() const final {
        return _relation_type == operator_type::EQ;
    }

    /**
     * Checks if the operator of this relation is a <code>Slice</code> (GT, GTE, LTE, LT).
     *
     * @return <code>true</code> if the operator of this relation is a <code>Slice</code>, <code>false</code> otherwise.
     */
    virtual bool is_slice() const final {
        return _relation_type == operator_type::GT
                || _relation_type == operator_type::GTE
                || _relation_type == operator_type::LTE
                || _relation_type ==
            operator_type::LT;
    }

    /**
     * Converts this <code>Relation</code> into a <code>Restriction</code>.
     *
     * @param cfm the Column Family meta data
     * @param boundNames the variables specification where to collect the bind variables
     * @return the <code>Restriction</code> corresponding to this <code>Relation</code>
     * @throws InvalidRequestException if this <code>Relation</code> is not valid
     */
    virtual ::shared_ptr<restrictions::restriction> to_restriction(schema_ptr schema, ::shared_ptr<variable_specifications> bound_names) final {
        if (_relation_type == operator_type::EQ) {
            return new_EQ_restriction(schema, bound_names);
        } else if (_relation_type == operator_type::LT) {
            return new_slice_restriction(schema, bound_names, statements::bound::END, false);
        } else if (_relation_type == operator_type::LTE) {
            return new_slice_restriction(schema, bound_names, statements::bound::END, true);
        } else if (_relation_type == operator_type::GTE) {
            return new_slice_restriction(schema, bound_names, statements::bound::START, true);
        } else if (_relation_type == operator_type::GT) {
            return new_slice_restriction(schema, bound_names, statements::bound::START, false);
        } else if (_relation_type == operator_type::IN) {
            return new_IN_restriction(schema, bound_names);
        } else if (_relation_type == operator_type::CONTAINS) {
            return new_contains_restriction(schema, bound_names, false);
        } else if (_relation_type == operator_type::CONTAINS_KEY) {
            return new_contains_restriction(schema, bound_names, true);
        } else {
            throw exceptions::invalid_request_exception(sprint("Unsupported \"!=\" relation: %s", to_string()));
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
    virtual ::shared_ptr<restrictions::restriction> new_EQ_restriction(schema_ptr schema,
        ::shared_ptr<variable_specifications> bound_names) = 0;

    /**
     * Creates a new IN restriction instance.
     *
     * @param cfm the Column Family meta data
     * @param bound_names the variables specification where to collect the bind variables
     * @return a new IN restriction instance
     * @throws InvalidRequestException if the relation cannot be converted into an IN restriction.
     */
    virtual ::shared_ptr<restrictions::restriction> new_IN_restriction(schema_ptr schema,
        ::shared_ptr<variable_specifications> bound_names) = 0;

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
    virtual ::shared_ptr<restrictions::restriction> new_slice_restriction(schema_ptr schema,
        ::shared_ptr<variable_specifications> bound_names,
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
    virtual ::shared_ptr<restrictions::restriction> new_contains_restriction(schema_ptr schema,
        ::shared_ptr<variable_specifications> bound_names, bool isKey) = 0;

#if 0

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
    protected abstract Term toTerm(List<? extends ColumnSpecification> receivers,
                                   Term.Raw raw,
                                   String keyspace,
                                   VariableSpecifications boundNames)
                                   throws InvalidRequestException;

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
    protected final List<Term> toTerms(List<? extends ColumnSpecification> receivers,
                                       List<? extends Term.Raw> raws,
                                       String keyspace,
                                       VariableSpecifications boundNames) throws InvalidRequestException
    {
        if (raws == null)
            return null;

        List<Term> terms = new ArrayList<>();
        for (int i = 0, m = raws.size(); i < m; i++)
            terms.add(toTerm(receivers, raws.get(i), keyspace, boundNames));

        return terms;
    }

#endif

protected:
    /**
     * Converts the specified entity into a column definition.
     *
     * @param cfm the column family meta data
     * @param entity the entity to convert
     * @return the column definition corresponding to the specified entity
     * @throws InvalidRequestException if the entity cannot be recognized
     */
    virtual column_definition& to_column_definition(schema_ptr schema, ::shared_ptr<column_identifier::raw> entity) final;
};

using relation_ptr = ::shared_ptr<relation>;

}
