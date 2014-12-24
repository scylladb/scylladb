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
package org.apache.cassandra.cql3;

import java.util.Collections;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.Term.Raw;
import org.apache.cassandra.cql3.restrictions.Restriction;
import org.apache.cassandra.cql3.restrictions.SingleColumnRestriction;
import org.apache.cassandra.cql3.statements.Bound;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.exceptions.InvalidRequestException;

import static org.apache.cassandra.cql3.statements.RequestValidations.checkFalse;
import static org.apache.cassandra.cql3.statements.RequestValidations.checkTrue;

/**
 * Relations encapsulate the relationship between an entity of some kind, and
 * a value (term). For example, <key> > "start" or "colname1" = "somevalue".
 *
 */
public final class SingleColumnRelation extends Relation
{
    private final ColumnIdentifier.Raw entity;
    private final Term.Raw mapKey;
    private final Term.Raw value;
    private final List<Term.Raw> inValues;

    private SingleColumnRelation(ColumnIdentifier.Raw entity, Term.Raw mapKey, Operator type, Term.Raw value, List<Term.Raw> inValues)
    {
        this.entity = entity;
        this.mapKey = mapKey;
        this.relationType = type;
        this.value = value;
        this.inValues = inValues;
    }

    /**
     * Creates a new relation.
     *
     * @param entity the kind of relation this is; what the term is being compared to.
     * @param mapKey the key into the entity identifying the value the term is being compared to.
     * @param type the type that describes how this entity relates to the value.
     * @param value the value being compared.
     */
    public SingleColumnRelation(ColumnIdentifier.Raw entity, Term.Raw mapKey, Operator type, Term.Raw value)
    {
        this(entity, mapKey, type, value, null);
    }

    /**
     * Creates a new relation.
     *
     * @param entity the kind of relation this is; what the term is being compared to.
     * @param type the type that describes how this entity relates to the value.
     * @param value the value being compared.
     */
    public SingleColumnRelation(ColumnIdentifier.Raw entity, Operator type, Term.Raw value)
    {
        this(entity, null, type, value);
    }

    public static SingleColumnRelation createInRelation(ColumnIdentifier.Raw entity, List<Term.Raw> inValues)
    {
        return new SingleColumnRelation(entity, null, Operator.IN, null, inValues);
    }

    public ColumnIdentifier.Raw getEntity()
    {
        return entity;
    }

    public Term.Raw getMapKey()
    {
        return mapKey;
    }

    @Override
    protected Term toTerm(List<? extends ColumnSpecification> receivers,
                          Raw raw,
                          String keyspace,
                          VariableSpecifications boundNames)
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
            case GT: return new SingleColumnRelation(entity, Operator.GTE, value);
            case LT: return new SingleColumnRelation(entity, Operator.LTE, value);
            default: return this;
        }
    }

    @Override
    public String toString()
    {
        String entityAsString = entity.toString();
        if (mapKey != null)
            entityAsString = String.format("%s[%s]", entityAsString, mapKey);

        if (isIN())
            return String.format("%s IN %s", entityAsString, inValues);

        return String.format("%s %s %s", entityAsString, relationType, value);
    }

    @Override
    protected Restriction newEQRestriction(CFMetaData cfm,
                                           VariableSpecifications boundNames) throws InvalidRequestException
    {
        ColumnDefinition columnDef = toColumnDefinition(cfm, entity);
        if (mapKey == null)
        {
            Term term = toTerm(toReceivers(cfm, columnDef), value, cfm.ksName, boundNames);
            return new SingleColumnRestriction.EQ(columnDef, term);
        }
        List<? extends ColumnSpecification> receivers = toReceivers(cfm, columnDef);
        Term entryKey = toTerm(Collections.singletonList(receivers.get(0)), mapKey, cfm.ksName, boundNames);
        Term entryValue = toTerm(Collections.singletonList(receivers.get(1)), value, cfm.ksName, boundNames);
        return new SingleColumnRestriction.Contains(columnDef, entryKey, entryValue);
    }

    @Override
    protected Restriction newINRestriction(CFMetaData cfm,
                                           VariableSpecifications boundNames) throws InvalidRequestException
    {
        ColumnDefinition columnDef = cfm.getColumnDefinition(getEntity().prepare(cfm));
        List<? extends ColumnSpecification> receivers = toReceivers(cfm, columnDef);
        List<Term> terms = toTerms(receivers, inValues, cfm.ksName, boundNames);
        if (terms == null)
        {
            Term term = toTerm(receivers, value, cfm.ksName, boundNames);
            return new SingleColumnRestriction.InWithMarker(columnDef, (Lists.Marker) term);
        }
        return new SingleColumnRestriction.InWithValues(columnDef, terms);
    }

    @Override
    protected Restriction newSliceRestriction(CFMetaData cfm,
                                              VariableSpecifications boundNames,
                                              Bound bound,
                                              boolean inclusive) throws InvalidRequestException
    {
        ColumnDefinition columnDef = toColumnDefinition(cfm, entity);
        Term term = toTerm(toReceivers(cfm, columnDef), value, cfm.ksName, boundNames);
        return new SingleColumnRestriction.Slice(columnDef, bound, inclusive, term);
    }

    @Override
    protected Restriction newContainsRestriction(CFMetaData cfm,
                                                 VariableSpecifications boundNames,
                                                 boolean isKey) throws InvalidRequestException
    {
        ColumnDefinition columnDef = toColumnDefinition(cfm, entity);
        Term term = toTerm(toReceivers(cfm, columnDef), value, cfm.ksName, boundNames);
        return new SingleColumnRestriction.Contains(columnDef, term, isKey);
    }

    /**
     * Returns the receivers for this relation.
     *
     * @param cfm the Column Family meta data
     * @param columnDef the column definition
     * @return the receivers for the specified relation.
     * @throws InvalidRequestException if the relation is invalid
     */
    private List<? extends ColumnSpecification> toReceivers(CFMetaData cfm, ColumnDefinition columnDef) throws InvalidRequestException
    {
        ColumnSpecification receiver = columnDef;

        checkFalse(columnDef.isCompactValue(),
                   "Predicates on the non-primary-key column (%s) of a COMPACT table are not yet supported",
                   columnDef.name);

        if (isIN())
        {
            // For partition keys we only support IN for the last name so far
            checkFalse(columnDef.isPartitionKey() && !isLastPartitionKey(cfm, columnDef),
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

        if (mapKey != null)
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
                       operator());

            if (isContainsKey() || isContains())
            {
                receiver = makeCollectionReceiver(receiver, isContainsKey());
            }
            else if (receiver.type.isMultiCell() && mapKey != null && isEQ())
            {
                List<ColumnSpecification> receivers = new ArrayList<>(2);
                receivers.add(makeCollectionReceiver(receiver, true));
                receivers.add(makeCollectionReceiver(receiver, false));
                return receivers;
            }
        }

        return Collections.singletonList(receiver);
    }

    private ColumnSpecification makeCollectionReceiver(ColumnSpecification receiver, boolean forKey)
    {
        return ((CollectionType<?>) receiver.type).makeCollectionReceiver(receiver, forKey);
    }

    private boolean isLegalRelationForNonFrozenCollection()
    {
        return isContainsKey() || isContains() || isMapEntryEquality();
    }

    private boolean isMapEntryEquality()
    {
        return mapKey != null && isEQ();
    }

    /**
     * Checks if the specified column is the last column of the partition key.
     *
     * @param cfm the column family meta data
     * @param columnDef the column to check
     * @return <code>true</code> if the specified column is the last column of the partition key, <code>false</code>
     * otherwise.
     */
    private static boolean isLastPartitionKey(CFMetaData cfm, ColumnDefinition columnDef)
    {
        return columnDef.position() == cfm.partitionKeyColumns().size() - 1;
    }

    private boolean canHaveOnlyOneValue()
    {
        return isEQ() || (isIN() && inValues != null && inValues.size() == 1);
    }
}
