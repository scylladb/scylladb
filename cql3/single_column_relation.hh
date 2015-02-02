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
        : single_column_relation(std::move(entity), {}, std::move(type), std::move(value))
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

#if 0
    @Override
    protected Term toTerm(List<? extends ColumnSpecification> receivers,
                          Raw raw,
                          String keyspace,
                          ::shared_ptr<variable_specifications> boundNames)
                          throws InvalidRequestException
    {
        assert receivers.size() == 1;

        Term term = raw.prepare(keyspace, receivers.get(0));
        term.collectMarkerSpecification(boundNames);
        return term;
    }

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
                                           ::shared_ptr<variable_specifications> bound_names) override {
        throw std::runtime_error("not implemented");
#if 0
        ColumnDefinition columnDef = toColumnDefinition(schema, entity);
        if (map_key == null)
        {
            Term term = toTerm(toReceivers(schema, columnDef), value, schema.ksName, bound_names);
            return new SingleColumnRestriction.EQ(columnDef, term);
        }
        List<? extends ColumnSpecification> receivers = toReceivers(schema, columnDef);
        Term entryKey = toTerm(Collections.singletonList(receivers.get(0)), map_key, schema.ksName, bound_names);
        Term entryValue = toTerm(Collections.singletonList(receivers.get(1)), value, schema.ksName, bound_names);
        return new SingleColumnRestriction.Contains(columnDef, entryKey, entryValue);
#endif
    }

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
        throw std::runtime_error("not implemented");
#if 0
        ColumnDefinition columnDef = toColumnDefinition(schema, entity);
        Term term = toTerm(toReceivers(schema, columnDef), value, schema.ksName, bound_names);
        return new SingleColumnRestriction.Slice(columnDef, bound, inclusive, term);
#endif
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

#if 0
    /**
     * Returns the receivers for this relation.
     *
     * @param schema the Column Family meta data
     * @param columnDef the column definition
     * @return the receivers for the specified relation.
     * @throws InvalidRequestException if the relation is invalid
     */
    private List<? extends ColumnSpecification> toReceivers(schema_ptr schema, ColumnDefinition columnDef) throws InvalidRequestException
    {
        ColumnSpecification receiver = columnDef;

        checkFalse(columnDef.isCompactValue(),
                   "Predicates on the non-primary-key column (%s) of a COMPACT table are not yet supported",
                   columnDef.name);

        if (isIN())
        {
            // For partition keys we only support IN for the last name so far
            checkFalse(columnDef.isPartitionKey() && !isLastPartitionKey(schema, columnDef),
                      "Partition KEY part %s cannot be restricted by IN relation (only the last part of the partition key can)",
                      columnDef.name);

            // We only allow IN on the row key and the clustering key so far, never on non-PK columns, and this even if
            // there's an index
            // Note: for backward compatibility reason, we conside a IN of 1 value the same as a EQ, so we let that
            // slide.
            checkFalse(!columnDef.isPrimaryKeyColumn() && !canHaveOnlyOneValue(),
                       "IN predicates on non-primary-key columns (%s) is not yet supported", columnDef.name);
        }
        else if (isSlice())
        {
            // Non EQ relation is not supported without token(), even if we have a 2ndary index (since even those
            // are ordered by partitioner).
            // Note: In theory we could allow it for 2ndary index queries with ALLOW FILTERING, but that would
            // probably require some special casing
            // Note bis: This is also why we don't bother handling the 'tuple' notation of #4851 for keys. If we
            // lift the limitation for 2ndary
            // index with filtering, we'll need to handle it though.
            checkFalse(columnDef.isPartitionKey(), "Only EQ and IN relation are supported on the partition key (unless you use the token() function)");
        }

        checkFalse(isContainsKey() && !(receiver.type instanceof MapType), "Cannot use CONTAINS KEY on non-map column %s", receiver.name);

        if (map_key != null)
        {
            checkFalse(receiver.type instanceof ListType, "Indexes on list entries (%s[index] = value) are not currently supported.", receiver.name);
            checkTrue(receiver.type instanceof MapType, "Column %s cannot be used as a map", receiver.name);
            checkTrue(receiver.type.isMultiCell(), "Map-entry equality predicates on frozen map column %s are not supported", receiver.name);
            checkTrue(isEQ(), "Only EQ relations are supported on map entries");
        }

        if (receiver.type.isCollection())
        {
            // We don't support relations against entire collections (unless they're frozen), like "numbers = {1, 2, 3}"
            checkFalse(receiver.type.isMultiCell() && !isLegalRelationForNonFrozenCollection(),
                       "Collection column '%s' (%s) cannot be restricted by a '%s' relation",
                       receiver.name,
                       receiver.type.asCQL3Type(),
                       get_operator());

            if (isContainsKey() || isContains())
            {
                receiver = makeCollectionReceiver(receiver, isContainsKey());
            }
            else if (receiver.type.isMultiCell() && map_key != null && isEQ())
            {
                List<ColumnSpecification> receivers = new ArrayList<>(2);
                receivers.add(makeCollectionReceiver(receiver, true));
                receivers.add(makeCollectionReceiver(receiver, false));
                return receivers;
            }
        }

        return Collections.singletonList(receiver);
    }

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

    /**
     * Checks if the specified column is the last column of the partition key.
     *
     * @param schema the column family meta data
     * @param columnDef the column to check
     * @return <code>true</code> if the specified column is the last column of the partition key, <code>false</code>
     * otherwise.
     */
    private static bool isLastPartitionKey(schema_ptr schema, ColumnDefinition columnDef)
    {
        return columnDef.position() == schema.partitionKeyColumns().size() - 1;
    }

    private bool canHaveOnlyOneValue()
    {
        return isEQ() || (isIN() && in_values != null && in_values.size() == 1);
    }
#endif
};

};
