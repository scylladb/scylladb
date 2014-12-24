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

import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.Term.MultiColumnRaw;
import org.apache.cassandra.cql3.Term.Raw;
import org.apache.cassandra.cql3.restrictions.MultiColumnRestriction;
import org.apache.cassandra.cql3.restrictions.Restriction;
import org.apache.cassandra.cql3.statements.Bound;
import org.apache.cassandra.exceptions.InvalidRequestException;

import static org.apache.cassandra.cql3.statements.RequestValidations.checkFalse;
import static org.apache.cassandra.cql3.statements.RequestValidations.checkTrue;
import static org.apache.cassandra.cql3.statements.RequestValidations.invalidRequest;

/**
 * A relation using the tuple notation, which typically affects multiple columns.
 * Examples:
 *  - SELECT ... WHERE (a, b, c) > (1, 'a', 10)
 *  - SELECT ... WHERE (a, b, c) IN ((1, 2, 3), (4, 5, 6))
 *  - SELECT ... WHERE (a, b) < ?
 *  - SELECT ... WHERE (a, b) IN ?
 */
public class MultiColumnRelation extends Relation
{
    private final List<ColumnIdentifier.Raw> entities;

    /** A Tuples.Literal or Tuples.Raw marker */
    private final Term.MultiColumnRaw valuesOrMarker;

    /** A list of Tuples.Literal or Tuples.Raw markers */
    private final List<? extends Term.MultiColumnRaw> inValues;

    private final Tuples.INRaw inMarker;

    private MultiColumnRelation(List<ColumnIdentifier.Raw> entities, Operator relationType, Term.MultiColumnRaw valuesOrMarker, List<? extends Term.MultiColumnRaw> inValues, Tuples.INRaw inMarker)
    {
        this.entities = entities;
        this.relationType = relationType;
        this.valuesOrMarker = valuesOrMarker;

        this.inValues = inValues;
        this.inMarker = inMarker;
    }

    /**
     * Creates a multi-column EQ, LT, LTE, GT, or GTE relation.
     * For example: "SELECT ... WHERE (a, b) > (0, 1)"
     * @param entities the columns on the LHS of the relation
     * @param relationType the relation operator
     * @param valuesOrMarker a Tuples.Literal instance or a Tuples.Raw marker
     * @return a new <code>MultiColumnRelation</code> instance
     */
    public static MultiColumnRelation createNonInRelation(List<ColumnIdentifier.Raw> entities, Operator relationType, Term.MultiColumnRaw valuesOrMarker)
    {
        assert relationType != Operator.IN;
        return new MultiColumnRelation(entities, relationType, valuesOrMarker, null, null);
    }

    /**
     * Creates a multi-column IN relation with a list of IN values or markers.
     * For example: "SELECT ... WHERE (a, b) IN ((0, 1), (2, 3))"
     * @param entities the columns on the LHS of the relation
     * @param inValues a list of Tuples.Literal instances or a Tuples.Raw markers
     * @return a new <code>MultiColumnRelation</code> instance
     */
    public static MultiColumnRelation createInRelation(List<ColumnIdentifier.Raw> entities, List<? extends Term.MultiColumnRaw> inValues)
    {
        return new MultiColumnRelation(entities, Operator.IN, null, inValues, null);
    }

    /**
     * Creates a multi-column IN relation with a marker for the IN values.
     * For example: "SELECT ... WHERE (a, b) IN ?"
     * @param entities the columns on the LHS of the relation
     * @param inMarker a single IN marker
     * @return a new <code>MultiColumnRelation</code> instance
     */
    public static MultiColumnRelation createSingleMarkerInRelation(List<ColumnIdentifier.Raw> entities, Tuples.INRaw inMarker)
    {
        return new MultiColumnRelation(entities, Operator.IN, null, null, inMarker);
    }

    public List<ColumnIdentifier.Raw> getEntities()
    {
        return entities;
    }

    /**
     * For non-IN relations, returns the Tuples.Literal or Tuples.Raw marker for a single tuple.
     * @return a Tuples.Literal for non-IN relations or Tuples.Raw marker for a single tuple.
     */
    private Term.MultiColumnRaw getValue()
    {
        return relationType == Operator.IN ? inMarker : valuesOrMarker;
    }

    @Override
    public boolean isMultiColumn()
    {
        return true;
    }

    @Override
    protected Restriction newEQRestriction(CFMetaData cfm,
                                           VariableSpecifications boundNames) throws InvalidRequestException
    {
        List<ColumnDefinition> receivers = receivers(cfm);
        Term term = toTerm(receivers, getValue(), cfm.ksName, boundNames);
        return new MultiColumnRestriction.EQ(cfm.comparator, receivers, term);
    }

    @Override
    protected Restriction newINRestriction(CFMetaData cfm,
                                           VariableSpecifications boundNames) throws InvalidRequestException
    {
        List<ColumnDefinition> receivers = receivers(cfm);
        List<Term> terms = toTerms(receivers, inValues, cfm.ksName, boundNames);
        if (terms == null)
        {
            Term term = toTerm(receivers, getValue(), cfm.ksName, boundNames);
            return new MultiColumnRestriction.InWithMarker(cfm.comparator, receivers, (AbstractMarker) term);
        }
        return new MultiColumnRestriction.InWithValues(cfm.comparator, receivers, terms);
    }

    @Override
    protected Restriction newSliceRestriction(CFMetaData cfm,
                                              VariableSpecifications boundNames,
                                              Bound bound,
                                              boolean inclusive) throws InvalidRequestException
    {
        List<ColumnDefinition> receivers = receivers(cfm);
        Term term = toTerm(receivers(cfm), getValue(), cfm.ksName, boundNames);
        return new MultiColumnRestriction.Slice(cfm.comparator, receivers, bound, inclusive, term);
    }

    @Override
    protected Restriction newContainsRestriction(CFMetaData cfm,
                                                 VariableSpecifications boundNames,
                                                 boolean isKey) throws InvalidRequestException
    {
        throw invalidRequest("%s cannot be used for Multi-column relations", operator());
    }

    @Override
    protected Term toTerm(List<? extends ColumnSpecification> receivers,
                          Raw raw,
                          String keyspace,
                          VariableSpecifications boundNames) throws InvalidRequestException
    {
        Term term = ((MultiColumnRaw) raw).prepare(keyspace, receivers);
        term.collectMarkerSpecification(boundNames);
        return term;
    }

    protected List<ColumnDefinition> receivers(CFMetaData cfm) throws InvalidRequestException
    {
        List<ColumnDefinition> names = new ArrayList<>(getEntities().size());
        int previousPosition = -1;
        for (ColumnIdentifier.Raw raw : getEntities())
        {
            ColumnDefinition def = toColumnDefinition(cfm, raw);
            checkTrue(def.isClusteringColumn(), "Multi-column relations can only be applied to clustering columns but was applied to: %s", def.name);
            checkFalse(names.contains(def), "Column \"%s\" appeared twice in a relation: %s", def.name, this);

            // check that no clustering columns were skipped
            if (def.position() != previousPosition + 1)
            {
                checkFalse(previousPosition == -1, "Clustering columns may not be skipped in multi-column relations. " +
                                                   "They should appear in the PRIMARY KEY order. Got %s", this);
                throw invalidRequest("Clustering columns must appear in the PRIMARY KEY order in multi-column relations: %s", this);
            }
            names.add(def);
            previousPosition = def.position();
        }
        return names;
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder(Tuples.tupleToString(entities));
        if (isIN())
        {
            return builder.append(" IN ")
                          .append(inMarker != null ? '?' : Tuples.tupleToString(inValues))
                          .toString();
        }
        return builder.append(" ")
                      .append(relationType)
                      .append(" ")
                      .append(valuesOrMarker)
                      .toString();
    }
}