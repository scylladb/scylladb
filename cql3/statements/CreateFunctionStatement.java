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
import java.util.HashSet;
import java.util.List;

import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.ColumnIdentifier;
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
 * A <code>CREATE FUNCTION</code> statement parsed from a CQL query.
 */
public final class CreateFunctionStatement extends SchemaAlteringStatement
{
    private final boolean orReplace;
    private final boolean ifNotExists;
    private FunctionName functionName;
    private final String language;
    private final String body;
    private final boolean deterministic;

    private final List<ColumnIdentifier> argNames;
    private final List<CQL3Type.Raw> argRawTypes;
    private final CQL3Type.Raw rawReturnType;

    public CreateFunctionStatement(FunctionName functionName,
                                   String language,
                                   String body,
                                   boolean deterministic,
                                   List<ColumnIdentifier> argNames,
                                   List<CQL3Type.Raw> argRawTypes,
                                   CQL3Type.Raw rawReturnType,
                                   boolean orReplace,
                                   boolean ifNotExists)
    {
        this.functionName = functionName;
        this.language = language;
        this.body = body;
        this.deterministic = deterministic;
        this.argNames = argNames;
        this.argRawTypes = argRawTypes;
        this.rawReturnType = rawReturnType;
        this.orReplace = orReplace;
        this.ifNotExists = ifNotExists;
    }

    public void prepareKeyspace(ClientState state) throws InvalidRequestException
    {
        if (!functionName.hasKeyspace() && state.getRawKeyspace() != null)
            functionName = new FunctionName(state.getRawKeyspace(), functionName.name);

        if (!functionName.hasKeyspace())
            throw new InvalidRequestException("Functions must be fully qualified with a keyspace name if a keyspace is not set for the session");

        ThriftValidation.validateKeyspaceNotSystem(functionName.keyspace);
    }

    public void checkAccess(ClientState state) throws UnauthorizedException, InvalidRequestException
    {
        // TODO CASSANDRA-7557 (function DDL permission)

        state.hasKeyspaceAccess(functionName.keyspace, Permission.CREATE);
    }

    public void validate(ClientState state) throws InvalidRequestException
    {
        if (ifNotExists && orReplace)
            throw new InvalidRequestException("Cannot use both 'OR REPLACE' and 'IF NOT EXISTS' directives");

        if (Schema.instance.getKSMetaData(functionName.keyspace) == null)
            throw new InvalidRequestException(String.format("Cannot add function '%s' to non existing keyspace '%s'.", functionName.name, functionName.keyspace));
    }

    public Event.SchemaChange changeEvent()
    {
        return null;
    }

    public boolean announceMigration(boolean isLocalOnly) throws RequestValidationException
    {
        if (new HashSet<>(argNames).size() != argNames.size())
            throw new InvalidRequestException(String.format("duplicate argument names for given function %s with argument names %s",
                                                            functionName, argNames));

        List<AbstractType<?>> argTypes = new ArrayList<>(argRawTypes.size());
        for (CQL3Type.Raw rawType : argRawTypes)
            argTypes.add(rawType.prepare(typeKeyspace(rawType)).getType());

        AbstractType<?> returnType = rawReturnType.prepare(typeKeyspace(rawReturnType)).getType();

        Function old = Functions.find(functionName, argTypes);
        if (old != null)
        {
            if (ifNotExists)
                return false;
            if (!orReplace)
                throw new InvalidRequestException(String.format("Function %s already exists", old));
            if (!(old instanceof ScalarFunction))
                throw new InvalidRequestException(String.format("Function %s can only replace a function", old));

            if (!Functions.typeEquals(old.returnType(), returnType))
                throw new InvalidRequestException(String.format("Cannot replace function %s, the new return type %s is not compatible with the return type %s of existing function",
                                                                functionName, returnType.asCQL3Type(), old.returnType().asCQL3Type()));
        }

        MigrationManager.announceNewFunction(UDFunction.create(functionName, argNames, argTypes, returnType, language, body, deterministic), isLocalOnly);
        return true;
    }

    private String typeKeyspace(CQL3Type.Raw rawType)
    {
        String ks = rawType.keyspace();
        if (ks != null)
            return ks;
        return functionName.keyspace;
    }
}
