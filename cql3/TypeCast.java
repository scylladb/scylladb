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

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.exceptions.InvalidRequestException;

public class TypeCast implements Term.Raw
{
    private final CQL3Type.Raw type;
    private final Term.Raw term;

    public TypeCast(CQL3Type.Raw type, Term.Raw term)
    {
        this.type = type;
        this.term = term;
    }

    public Term prepare(String keyspace, ColumnSpecification receiver) throws InvalidRequestException
    {
        if (!term.testAssignment(keyspace, castedSpecOf(keyspace, receiver)).isAssignable())
            throw new InvalidRequestException(String.format("Cannot cast value %s to type %s", term, type));

        if (!testAssignment(keyspace, receiver).isAssignable())
            throw new InvalidRequestException(String.format("Cannot assign value %s to %s of type %s", this, receiver.name, receiver.type.asCQL3Type()));

        return term.prepare(keyspace, receiver);
    }

    private ColumnSpecification castedSpecOf(String keyspace, ColumnSpecification receiver) throws InvalidRequestException
    {
        return new ColumnSpecification(receiver.ksName, receiver.cfName, new ColumnIdentifier(toString(), true), type.prepare(keyspace).getType());
    }

    public AssignmentTestable.TestResult testAssignment(String keyspace, ColumnSpecification receiver)
    {
        try
        {
            AbstractType<?> castedType = type.prepare(keyspace).getType();
            if (receiver.type.equals(castedType))
                return AssignmentTestable.TestResult.EXACT_MATCH;
            else if (receiver.type.isValueCompatibleWith(castedType))
                return AssignmentTestable.TestResult.WEAKLY_ASSIGNABLE;
            else
                return AssignmentTestable.TestResult.NOT_ASSIGNABLE;
        }
        catch (InvalidRequestException e)
        {
            throw new AssertionError();
        }
    }

    @Override
    public String toString()
    {
        return "(" + type + ")" + term;
    }
}
