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

import java.nio.ByteBuffer;
import java.util.*;

import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.transport.Server;

/**
 * Static helper methods and classes for user types.
 */
public abstract class UserTypes
{
    private UserTypes() {}

    public static ColumnSpecification fieldSpecOf(ColumnSpecification column, int field)
    {
        UserType ut = (UserType)column.type;
        return new ColumnSpecification(column.ksName,
                                       column.cfName,
                                       new ColumnIdentifier(column.name + "." + UTF8Type.instance.compose(ut.fieldName(field)), true),
                                       ut.fieldType(field));
    }

    public static class Literal implements Term.Raw
    {
        public final Map<ColumnIdentifier, Term.Raw> entries;

        public Literal(Map<ColumnIdentifier, Term.Raw> entries)
        {
            this.entries = entries;
        }

        public Term prepare(String keyspace, ColumnSpecification receiver) throws InvalidRequestException
        {
            validateAssignableTo(keyspace, receiver);

            UserType ut = (UserType)receiver.type;
            boolean allTerminal = true;
            List<Term> values = new ArrayList<>(entries.size());
            int foundValues = 0;
            for (int i = 0; i < ut.size(); i++)
            {
                ColumnIdentifier field = new ColumnIdentifier(ut.fieldName(i), UTF8Type.instance);
                Term.Raw raw = entries.get(field);
                if (raw == null)
                    raw = Constants.NULL_LITERAL;
                else
                    ++foundValues;
                Term value = raw.prepare(keyspace, fieldSpecOf(receiver, i));

                if (value instanceof Term.NonTerminal)
                    allTerminal = false;

                values.add(value);
            }
            if (foundValues != entries.size())
            {
                // We had some field that are not part of the type
                for (ColumnIdentifier id : entries.keySet())
                    if (!ut.fieldNames().contains(id.bytes))
                        throw new InvalidRequestException(String.format("Unknown field '%s' in value of user defined type %s", id, ut.getNameAsString()));
            }

            DelayedValue value = new DelayedValue(((UserType)receiver.type), values);
            return allTerminal ? value.bind(QueryOptions.DEFAULT) : value;
        }

        private void validateAssignableTo(String keyspace, ColumnSpecification receiver) throws InvalidRequestException
        {
            if (!(receiver.type instanceof UserType))
                throw new InvalidRequestException(String.format("Invalid user type literal for %s of type %s", receiver, receiver.type.asCQL3Type()));

            UserType ut = (UserType)receiver.type;
            for (int i = 0; i < ut.size(); i++)
            {
                ColumnIdentifier field = new ColumnIdentifier(ut.fieldName(i), UTF8Type.instance);
                Term.Raw value = entries.get(field);
                if (value == null)
                    continue;

                ColumnSpecification fieldSpec = fieldSpecOf(receiver, i);
                if (!value.testAssignment(keyspace, fieldSpec).isAssignable())
                    throw new InvalidRequestException(String.format("Invalid user type literal for %s: field %s is not of type %s", receiver, field, fieldSpec.type.asCQL3Type()));
            }
        }

        public AssignmentTestable.TestResult testAssignment(String keyspace, ColumnSpecification receiver)
        {
            try
            {
                validateAssignableTo(keyspace, receiver);
                return AssignmentTestable.TestResult.WEAKLY_ASSIGNABLE;
            }
            catch (InvalidRequestException e)
            {
                return AssignmentTestable.TestResult.NOT_ASSIGNABLE;
            }
        }

        @Override
        public String toString()
        {
            StringBuilder sb = new StringBuilder();
            sb.append("{");
            Iterator<Map.Entry<ColumnIdentifier, Term.Raw>> iter = entries.entrySet().iterator();
            while (iter.hasNext())
            {
                Map.Entry<ColumnIdentifier, Term.Raw> entry = iter.next();
                sb.append(entry.getKey()).append(":").append(entry.getValue());
                if (iter.hasNext())
                    sb.append(", ");
            }
            sb.append("}");
            return sb.toString();
        }
    }

    // Same purpose than Lists.DelayedValue, except we do handle bind marker in that case
    public static class DelayedValue extends Term.NonTerminal
    {
        private final UserType type;
        private final List<Term> values;

        public DelayedValue(UserType type, List<Term> values)
        {
            this.type = type;
            this.values = values;
        }

        public boolean usesFunction(String ksName, String functionName)
        {
            if (values != null)
                for (Term value : values)
                    if (value != null && value.usesFunction(ksName, functionName))
                        return true;
            return false;
        }

        public boolean containsBindMarker()
        {
            for (Term t : values)
                if (t.containsBindMarker())
                    return true;
            return false;
        }

        public void collectMarkerSpecification(VariableSpecifications boundNames)
        {
            for (int i = 0; i < type.size(); i++)
                values.get(i).collectMarkerSpecification(boundNames);
        }

        private ByteBuffer[] bindInternal(QueryOptions options) throws InvalidRequestException
        {
            int version = options.getProtocolVersion();

            ByteBuffer[] buffers = new ByteBuffer[values.size()];
            for (int i = 0; i < type.size(); i++)
            {
                buffers[i] = values.get(i).bindAndGet(options);
                // Inside UDT values, we must force the serialization of collections to v3 whatever protocol
                // version is in use since we're going to store directly that serialized value.
                if (version < Server.VERSION_3 && type.fieldType(i).isCollection() && buffers[i] != null)
                    buffers[i] = ((CollectionType)type.fieldType(i)).getSerializer().reserializeToV3(buffers[i]);
            }
            return buffers;
        }

        public Constants.Value bind(QueryOptions options) throws InvalidRequestException
        {
            return new Constants.Value(bindAndGet(options));
        }

        @Override
        public ByteBuffer bindAndGet(QueryOptions options) throws InvalidRequestException
        {
            return UserType.buildValue(bindInternal(options));
        }
    }
}
