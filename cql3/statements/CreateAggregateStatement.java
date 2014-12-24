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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.Term;
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
 * A <code>CREATE AGGREGATE</code> statement parsed from a CQL query.
 */
public final class CreateAggregateStatement extends SchemaAlteringStatement
{
    private final boolean orReplace;
    private final boolean ifNotExists;
    private FunctionName functionName;
    private String stateFunc;
    private String finalFunc;
    private final CQL3Type.Raw stateTypeRaw;

    private final List<CQL3Type.Raw> argRawTypes;
    private final Term.Raw ival;

    public CreateAggregateStatement(FunctionName functionName,
                                    List<CQL3Type.Raw> argRawTypes,
                                    String stateFunc,
                                    CQL3Type.Raw stateType,
                                    String finalFunc,
                                    Term.Raw ival,
                                    boolean orReplace,
                                    boolean ifNotExists)
    {
        this.functionName = functionName;
        this.argRawTypes = argRawTypes;
        this.stateFunc = stateFunc;
        this.finalFunc = finalFunc;
        this.stateTypeRaw = stateType;
        this.ival = ival;
        this.orReplace = orReplace;
        this.ifNotExists = ifNotExists;
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

        state.hasKeyspaceAccess(functionName.keyspace, Permission.CREATE);
    }

    public void validate(ClientState state) throws InvalidRequestException
    {
        if (ifNotExists && orReplace)
            throw new InvalidRequestException("Cannot use both 'OR REPLACE' and 'IF NOT EXISTS' directives");

        if (Schema.instance.getKSMetaData(functionName.keyspace) == null)
            throw new InvalidRequestException(String.format("Cannot add aggregate '%s' to non existing keyspace '%s'.", functionName.name, functionName.keyspace));
    }

    public Event.SchemaChange changeEvent()
    {
        return null;
    }

    public boolean announceMigration(boolean isLocalOnly) throws RequestValidationException
    {
        List<AbstractType<?>> argTypes = new ArrayList<>(argRawTypes.size());
        for (CQL3Type.Raw rawType : argRawTypes)
            argTypes.add(rawType.prepare(functionName.keyspace).getType());

        FunctionName stateFuncName = new FunctionName(functionName.keyspace, stateFunc);
        FunctionName finalFuncName;

        ScalarFunction fFinal = null;
        AbstractType<?> stateType = stateTypeRaw.prepare(functionName.keyspace).getType();
        Function f = Functions.find(stateFuncName, stateArguments(stateType, argTypes));
        if (!(f instanceof ScalarFunction))
            throw new InvalidRequestException("State function " + stateFuncSig(stateFuncName, stateTypeRaw, argRawTypes) + " does not exist or is not a scalar function");
        ScalarFunction fState = (ScalarFunction)f;

        AbstractType<?> returnType;
        if (finalFunc != null)
        {
            finalFuncName = new FunctionName(functionName.keyspace, finalFunc);
            f = Functions.find(finalFuncName, Collections.<AbstractType<?>>singletonList(stateType));
            if (!(f instanceof ScalarFunction))
                throw new InvalidRequestException("Final function " + finalFuncName + "(" + stateTypeRaw + ") does not exist");
            fFinal = (ScalarFunction) f;
            returnType = fFinal.returnType();
        }
        else
        {
            returnType = fState.returnType();
            if (!returnType.equals(stateType))
                throw new InvalidRequestException("State function " + stateFuncSig(stateFuncName, stateTypeRaw, argRawTypes) + " return type must be the same as the first argument type (if no final function is used)");
        }

        Function old = Functions.find(functionName, argTypes);
        if (old != null)
        {
            if (ifNotExists)
                return false;
            if (!orReplace)
                throw new InvalidRequestException(String.format("Function %s already exists", old));
            if (!(old instanceof AggregateFunction))
                throw new InvalidRequestException(String.format("Aggregate %s can only replace an aggregate", old));

            // Means we're replacing the function. We still need to validate that 1) it's not a native function and 2) that the return type
            // matches (or that could break existing code badly)
            if (old.isNative())
                throw new InvalidRequestException(String.format("Cannot replace native aggregate %s", old));
            if (!old.returnType().isValueCompatibleWith(returnType))
                throw new InvalidRequestException(String.format("Cannot replace aggregate %s, the new return type %s is not compatible with the return type %s of existing function",
                                                                functionName, returnType.asCQL3Type(), old.returnType().asCQL3Type()));
        }

        ByteBuffer initcond = null;
        if (ival != null)
        {
            ColumnSpecification receiver = new ColumnSpecification(functionName.keyspace, "--dummy--", new ColumnIdentifier("(aggregate_initcond)", true), stateType);
            initcond = ival.prepare(functionName.keyspace, receiver).bindAndGet(QueryOptions.DEFAULT);
        }

        UDAggregate udAggregate = new UDAggregate(functionName, argTypes, returnType,
                                                  fState,
                                                  fFinal,
                                                  initcond);

        MigrationManager.announceNewAggregate(udAggregate, isLocalOnly);

        return true;
    }

    private String stateFuncSig(FunctionName stateFuncName, CQL3Type.Raw stateTypeRaw, List<CQL3Type.Raw> argRawTypes)
    {
        StringBuilder sb = new StringBuilder();
        sb.append(stateFuncName.toString()).append('(').append(stateTypeRaw);
        for (CQL3Type.Raw argRawType : argRawTypes)
            sb.append(", ").append(argRawType);
        sb.append(')');
        return sb.toString();
    }

    private List<AbstractType<?>> stateArguments(AbstractType<?> stateType, List<AbstractType<?>> argTypes)
    {
        List<AbstractType<?>> r = new ArrayList<>(argTypes.size() + 1);
        r.add(stateType);
        r.addAll(argTypes);
        return r;
    }
}
