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
package org.apache.cassandra.cql3.statements;

import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.functions.*;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.thrift.ThriftValidation;
import org.apache.cassandra.transport.Event;

/**
 * A <code>DROP AGGREGATE</code> statement parsed from a CQL query.
 */
public final class DropAggregateStatement extends SchemaAlteringStatement
{
    private FunctionName functionName;
    private final boolean ifExists;
    private final List<CQL3Type.Raw> argRawTypes;
    private final boolean argsPresent;

    public DropAggregateStatement(FunctionName functionName,
                                  List<CQL3Type.Raw> argRawTypes,
                                  boolean argsPresent,
                                  boolean ifExists)
    {
        this.functionName = functionName;
        this.argRawTypes = argRawTypes;
        this.argsPresent = argsPresent;
        this.ifExists = ifExists;
    }

    public void prepareKeyspace(ClientState state) throws InvalidRequestException
    {
        if (!functionName.hasKeyspace() && state.getRawKeyspace() != null)
            functionName = new FunctionName(state.getKeyspace(), functionName.name);

        if (!functionName.hasKeyspace())
            throw new InvalidRequestException("Functions must be fully qualified with a keyspace name if a keyspace is not set for the session");

        ThriftValidation.validateKeyspaceNotSystem(functionName.keyspace);
    }

    public void checkAccess(ClientState state) throws UnauthorizedException, InvalidRequestException
    {
        // TODO CASSANDRA-7557 (function DDL permission)

        state.hasKeyspaceAccess(functionName.keyspace, Permission.DROP);
    }

    public void validate(ClientState state) throws RequestValidationException
    {
    }

    public Event.SchemaChange changeEvent()
    {
        return null;
    }

    public boolean announceMigration(boolean isLocalOnly) throws RequestValidationException
    {
        List<Function> olds = Functions.find(functionName);

        if (!argsPresent && olds != null && olds.size() > 1)
            throw new InvalidRequestException(String.format("'DROP AGGREGATE %s' matches multiple function definitions; " +
                                                            "specify the argument types by issuing a statement like " +
                                                            "'DROP AGGREGATE %s (type, type, ...)'. Hint: use cqlsh " +
                                                            "'DESCRIBE AGGREGATE %s' command to find all overloads",
                                                            functionName, functionName, functionName));

        List<AbstractType<?>> argTypes = new ArrayList<>(argRawTypes.size());
        for (CQL3Type.Raw rawType : argRawTypes)
            argTypes.add(rawType.prepare(functionName.keyspace).getType());

        Function old;
        if (argsPresent)
        {
            old = Functions.find(functionName, argTypes);
            if (old == null || !(old instanceof AggregateFunction))
            {
                if (ifExists)
                    return false;
                // just build a nicer error message
                StringBuilder sb = new StringBuilder();
                for (CQL3Type.Raw rawType : argRawTypes)
                {
                    if (sb.length() > 0)
                        sb.append(", ");
                    sb.append(rawType);
                }
                throw new InvalidRequestException(String.format("Cannot drop non existing aggregate '%s(%s)'",
                                                                functionName, sb));
            }
        }
        else
        {
            if (olds == null || olds.isEmpty() || !(olds.get(0) instanceof AggregateFunction))
            {
                if (ifExists)
                    return false;
                throw new InvalidRequestException(String.format("Cannot drop non existing aggregate '%s'", functionName));
            }
            old = olds.get(0);
        }

        if (old.isNative())
            throw new InvalidRequestException(String.format("Cannot drop aggregate '%s' because it is a " +
                                                            "native (built-in) function", functionName));

        MigrationManager.announceAggregateDrop((UDAggregate)old, isLocalOnly);
        return true;
    }
}
