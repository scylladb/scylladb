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

#include <vector>
#include <cql3/restrictions/single_column_restriction.hh>

#include "core/shared_ptr.hh"
#include "to_string.hh"

#include "cql3/relation.hh"
#include "cql3/column_identifier.hh"
#include "cql3/term.hh"

namespace cql3 {

/**
 * Relations encapsulate the relationship between an entity of some kind, and
 * a value (term). For example, <key> > "start" or "colname1" = "somevalue".
 *
 */
class single_column_relation final : public relation {
private:
    ::shared_ptr<column_identifier::raw> _entity;
    ::shared_ptr<term::raw> _map_key;
    ::shared_ptr<term::raw> _value;
    std::vector<::shared_ptr<term::raw>> _in_values;
private:
    single_column_relation(::shared_ptr<column_identifier::raw> entity, ::shared_ptr<term::raw> map_key,
        const operator_type& type, ::shared_ptr<term::raw> value, std::vector<::shared_ptr<term::raw>> in_values)
            : relation(type)
            , _entity(std::move(entity))
            , _map_key(std::move(map_key))
            , _value(std::move(value))
            , _in_values(std::move(in_values))
    { }
public:
    /**
     * Creates a new relation.
     *
     * @param entity the kind of relation this is; what the term is being compared to.
     * @param map_key the key into the entity identifying the value the term is being compared to.
     * @param type the type that describes how this entity relates to the value.
     * @param value the value being compared.
     */
    single_column_relation(::shared_ptr<column_identifier::raw> entity, ::shared_ptr<term::raw> map_key,
        const operator_type& type, ::shared_ptr<term::raw> value)
            : single_column_relation(std::move(entity), std::move(map_key), type, std::move(value), {})
    { }

    /**
     * Creates a new relation.
     *
     * @param entity the kind of relation this is; what the term is being compared to.
     * @param type the type that describes how this entity relates to the value.
     * @param value the value being compared.
     */
    single_column_relation(::shared_ptr<column_identifier::raw> entity, const operator_type& type, ::shared_ptr<term::raw> value)
        : single_column_relation(std::move(entity), {}, type, std::move(value))
    { }

#if 0
    public static SingleColumnRelation createInRelation(::shared_ptr<column_identifier::raw> entity, List<::shared_ptr<term::raw>> in_values)
    {
        return new SingleColumnRelation(entity, null, operator_type::IN, null, in_values);
    }
#endif

public:
    ::shared_ptr<column_identifier::raw> get_entity() {
        return _entity;
    }

    ::shared_ptr<term::raw> get_map_key() {
        return _map_key;
    }
protected:
    ::shared_ptr<term> to_term(std::vector<::shared_ptr<column_specification>> receivers,
                          ::shared_ptr<term::raw> raw, const sstring& keyspace,
                          ::shared_ptr<variable_specifications> bound_names);

#if 0
    public SingleColumnRelation withNonStrictOperator()
    {
        switch (relationType)
        {
            case GT: return new SingleColumnRelation(entity, operator_type.GTE, value);
            case LT: return new SingleColumnRelation(entity, operator_type.LTE, value);
            default: return this;
        }
    }
#endif

    virtual sstring to_string() override {
        auto entity_as_string = _entity->to_string();
        if (_map_key) {
            entity_as_string = sprint("%s[%s]", std::move(entity_as_string), _map_key->to_string());
        }

        if (is_IN()) {
            return sprint("%s IN %s", entity_as_string, ::to_string(_in_values));
        }

        return sprint("%s %s %s", entity_as_string, _relation_type, _value->to_string());
    }

protected:
    virtual ::shared_ptr<restrictions::restriction> new_EQ_restriction(schema_ptr schema,
                                           ::shared_ptr<variable_specifications> bound_names);

    virtual ::shared_ptr<restrictions::restriction> new_IN_restriction(schema_ptr schema,
                                           ::shared_ptr<variable_specifications> bound_names) override {
        throw std::runtime_error("not implemented");
#if 0
        ColumnDefinition columnDef = schema.getColumnDefinition(getEntity().prepare(schema));
        List<? extends ColumnSpecification> receivers = toReceivers(schema, columnDef);
        List<Term> terms = toTerms(receivers, in_values, schema.ksName, bound_names);
        if (terms == null)
        {
            Term term = toTerm(receivers, value, schema.ksName, bound_names);
            return new SingleColumnRestriction.InWithMarker(columnDef, (Lists.Marker) term);
        }
        return new SingleColumnRestriction.InWithValues(columnDef, terms);
#endif
    }

    virtual ::shared_ptr<restrictions::restriction> new_slice_restriction(schema_ptr schema,
            ::shared_ptr<variable_specifications> bound_names,
            statements::bound bound,
            bool inclusive) override {
        auto&& column_def = to_column_definition(schema, _entity);
        auto term = to_term(to_receivers(schema, column_def), _value, schema->ks_name, std::move(bound_names));
        return ::make_shared<restrictions::single_column_restriction::slice>(column_def, bound, inclusive, std::move(term));
    }

    virtual shared_ptr<restrictions::restriction> new_contains_restriction(schema_ptr schema,
                                                 ::shared_ptr<variable_specifications> bound_names,
                                                 bool is_key) override {
        throw std::runtime_error("not implemented");
#if 0
        ColumnDefinition columnDef = toColumnDefinition(schema, entity);
        Term term = toTerm(toReceivers(schema, columnDef), value, schema.ksName, bound_names);
        return new SingleColumnRestriction.Contains(columnDef, term, is_key);
#endif
    }

private:
    /**
     * Returns the receivers for this relation.
     *
     * @param schema the Column Family meta data
     * @param column_def the column definition
     * @return the receivers for the specified relation.
     * @throws exceptions::invalid_request_exception if the relation is invalid
     */
    std::vector<::shared_ptr<column_specification>> to_receivers(schema_ptr schema, column_definition& column_def);

#if 0
    private ColumnSpecification makeCollectionReceiver(ColumnSpecification receiver, bool forKey)
    {
        return ((CollectionType<?>) receiver.type).makeCollectionReceiver(receiver, forKey);
    }

    private bool isLegalRelationForNonFrozenCollection()
    {
        return isContainsKey() || isContains() || isMapEntryEquality();
    }

    private bool isMapEntryEquality()
    {
        return map_key != null && isEQ();
    }
#endif

private:
    bool can_have_only_one_value() {
        return is_EQ() || (is_IN() && _in_values.size() == 1);
    }
};

};
