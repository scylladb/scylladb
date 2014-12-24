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
package org.apache.cassandra.cql3.selection;

import java.nio.ByteBuffer;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.cql3.AssignmentTestable;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.selection.Selection.ResultSetBuilder;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.exceptions.InvalidRequestException;

/**
 * A <code>Selector</code> is used to convert the data returned by the storage engine into the data requested by the 
 * user. They correspond to the &lt;selector&gt; elements from the select clause.
 * <p>Since the introduction of aggregation, <code>Selector</code>s cannot be called anymore by multiple threads 
 * as they have an internal state.</p>
 */
public abstract class Selector implements AssignmentTestable
{
    /**
     * A factory for <code>Selector</code> instances.
     */
    public static abstract class Factory
    {
        public boolean usesFunction(String ksName, String functionName)
        {
            return false;
        }

        /**
         * Returns the column specification corresponding to the output value of the selector instances created by
         * this factory.
         *
         * @param cfm the column family meta data
         * @return a column specification
         */
        public final ColumnSpecification getColumnSpecification(CFMetaData cfm)
        {
            return new ColumnSpecification(cfm.ksName,
                                           cfm.cfName,
                                           new ColumnIdentifier(getColumnName(), true),
                                           getReturnType());
        }

        /**
         * Creates a new <code>Selector</code> instance.
         *
         * @return a new <code>Selector</code> instance
         */
        public abstract Selector newInstance() throws InvalidRequestException;

        /**
         * Checks if this factory creates selectors instances that creates aggregates.
         *
         * @return <code>true</code> if this factory creates selectors instances that creates aggregates,
         * <code>false</code> otherwise
         */
        public boolean isAggregateSelectorFactory()
        {
            return false;
        }

        /**
         * Checks if this factory creates <code>writetime</code> selectors instances.
         *
         * @return <code>true</code> if this factory creates <code>writetime</code> selectors instances,
         * <code>false</code> otherwise
         */
        public boolean isWritetimeSelectorFactory()
        {
            return false;
        }

        /**
         * Checks if this factory creates <code>TTL</code> selectors instances.
         *
         * @return <code>true</code> if this factory creates <code>TTL</code> selectors instances,
         * <code>false</code> otherwise
         */
        public boolean isTTLSelectorFactory()
        {
            return false;
        }

        /**
         * Returns the name of the column corresponding to the output value of the selector instances created by
         * this factory.
         *
         * @return a column name
         */
        protected abstract String getColumnName();

        /**
         * Returns the type of the values returned by the selector instances created by this factory.
         *
         * @return the selector output type
         */
        protected abstract AbstractType<?> getReturnType();

    }

    /**
     * Add the current value from the specified <code>ResultSetBuilder</code>.
     *
     * @param protocolVersion protocol version used for serialization
     * @param rs the <code>ResultSetBuilder</code>
     * @throws InvalidRequestException if a problem occurs while add the input value
     */
    public abstract void addInput(int protocolVersion, ResultSetBuilder rs) throws InvalidRequestException;

    /**
     * Returns the selector output.
     *
     * @param protocolVersion protocol version used for serialization
     * @return the selector output
     * @throws InvalidRequestException if a problem occurs while computing the output value
     */
    public abstract ByteBuffer getOutput(int protocolVersion) throws InvalidRequestException;

    /**
     * Returns the <code>Selector</code> output type.
     *
     * @return the <code>Selector</code> output type.
     */
    public abstract AbstractType<?> getType();

    /**
     * Checks if this <code>Selector</code> is creating aggregates.
     *
     * @return <code>true</code> if this <code>Selector</code> is creating aggregates <code>false</code>
     * otherwise.
     */
    public boolean isAggregate()
    {
        return false;
    }

    /**
     * Reset the internal state of this <code>Selector</code>.
     */
    public abstract void reset();

    public AssignmentTestable.TestResult testAssignment(String keyspace, ColumnSpecification receiver)
    {
        if (receiver.type.equals(getType()))
            return AssignmentTestable.TestResult.EXACT_MATCH;
        else if (receiver.type.isValueCompatibleWith(getType()))
            return AssignmentTestable.TestResult.WEAKLY_ASSIGNABLE;
        else
            return AssignmentTestable.TestResult.NOT_ASSIGNABLE;
    }
}