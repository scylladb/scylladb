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
package org.apache.cassandra.cql3.restrictions;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.statements.Bound;
import org.apache.cassandra.db.IndexExpression;
import org.apache.cassandra.db.index.SecondaryIndexManager;
import org.apache.cassandra.exceptions.InvalidRequestException;

/**
 * A restriction/clause on a column.
 * The goal of this class being to group all conditions for a column in a SELECT.
 */
public interface Restriction
{
    public boolean isOnToken();
    public boolean isSlice();
    public boolean isEQ();
    public boolean isIN();
    public boolean isContains();
    public boolean isMultiColumn();

    public List<ByteBuffer> values(QueryOptions options) throws InvalidRequestException;

    /**
     * Returns <code>true</code> if one of the restrictions use the specified function.
     *
     * @param ksName the keyspace name
     * @param functionName the function name
     * @return <code>true</code> if one of the restrictions use the specified function, <code>false</code> otherwise.
     */
    public boolean usesFunction(String ksName, String functionName);

    /**
     * Checks if the specified bound is set or not.
     * @param b the bound type
     * @return <code>true</code> if the specified bound is set, <code>false</code> otherwise
     */
    public boolean hasBound(Bound b);

    public List<ByteBuffer> bounds(Bound b, QueryOptions options) throws InvalidRequestException;

    /**
     * Checks if the specified bound is inclusive or not.
     * @param b the bound type
     * @return <code>true</code> if the specified bound is inclusive, <code>false</code> otherwise
     */
    public boolean isInclusive(Bound b);

    /**
     * Merges this restriction with the specified one.
     *
     * @param otherRestriction the restriction to merge into this one
     * @return the restriction resulting of the merge
     * @throws InvalidRequestException if the restrictions cannot be merged
     */
    public Restriction mergeWith(Restriction otherRestriction) throws InvalidRequestException;

    /**
     * Check if the restriction is on indexed columns.
     *
     * @param indexManager the index manager
     * @return <code>true</code> if the restriction is on indexed columns, <code>false</code>
     */
    public boolean hasSupportingIndex(SecondaryIndexManager indexManager);

    /**
     * Adds to the specified list the <code>IndexExpression</code>s corresponding to this <code>Restriction</code>.
     *
     * @param expressions the list to add the <code>IndexExpression</code>s to
     * @param options the query options
     * @throws InvalidRequestException if this <code>Restriction</code> cannot be converted into 
     * <code>IndexExpression</code>s
     */
    public void addIndexExpressionTo(List<IndexExpression> expressions,
                                     QueryOptions options)
                                     throws InvalidRequestException;
}
