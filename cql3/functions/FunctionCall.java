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
package org.apache.cassandra.cql3.functions;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.cql3.*;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.transport.Server;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.serializers.MarshalException;

public class FunctionCall extends Term.NonTerminal
{
    private final ScalarFunction fun;
    private final List<Term> terms;

    private FunctionCall(ScalarFunction fun, List<Term> terms)
    {
        this.fun = fun;
        this.terms = terms;
    }

    public boolean usesFunction(String ksName, String functionName)
    {
        return fun.usesFunction(ksName, functionName);
    }

    public void collectMarkerSpecification(VariableSpecifications boundNames)
    {
        for (Term t : terms)
            t.collectMarkerSpecification(boundNames);
    }

    public Term.Terminal bind(QueryOptions options) throws InvalidRequestException
    {
        return makeTerminal(fun, bindAndGet(options), options.getProtocolVersion());
    }

    public ByteBuffer bindAndGet(QueryOptions options) throws InvalidRequestException
    {
        List<ByteBuffer> buffers = new ArrayList<>(terms.size());
        for (Term t : terms)
        {
            // For now, we don't allow nulls as argument as no existing function needs it and it
            // simplify things.
            ByteBuffer val = t.bindAndGet(options);
            if (val == null)
                throw new InvalidRequestException(String.format("Invalid null value for argument to %s", fun));
            buffers.add(val);
        }
        return executeInternal(options.getProtocolVersion(), fun, buffers);
    }

    private static ByteBuffer executeInternal(int protocolVersion, ScalarFunction fun, List<ByteBuffer> params) throws InvalidRequestException
    {
        ByteBuffer result = fun.execute(protocolVersion, params);
        try
        {
            // Check the method didn't lied on it's declared return type
            if (result != null)
                fun.returnType().validate(result);
            return result;
        }
        catch (MarshalException e)
        {
            throw new RuntimeException(String.format("Return of function %s (%s) is not a valid value for its declared return type %s", 
                                                     fun, ByteBufferUtil.bytesToHex(result), fun.returnType().asCQL3Type()));
        }
    }

    public boolean containsBindMarker()
    {
        for (Term t : terms)
        {
            if (t.containsBindMarker())
                return true;
        }
        return false;
    }

    private static Term.Terminal makeTerminal(Function fun, ByteBuffer result, int version) throws InvalidRequestException
    {
        if (!(fun.returnType() instanceof CollectionType))
            return new Constants.Value(result);

        switch (((CollectionType)fun.returnType()).kind)
        {
            case LIST: return Lists.Value.fromSerialized(result, (ListType)fun.returnType(), version);
            case SET:  return Sets.Value.fromSerialized(result, (SetType)fun.returnType(), version);
            case MAP:  return Maps.Value.fromSerialized(result, (MapType)fun.returnType(), version);
        }
        throw new AssertionError();
    }

    public static class Raw implements Term.Raw
    {
        private FunctionName name;
        private final List<Term.Raw> terms;

        public Raw(FunctionName name, List<Term.Raw> terms)
        {
            this.name = name;
            this.terms = terms;
        }

        public Term prepare(String keyspace, ColumnSpecification receiver) throws InvalidRequestException
        {
            Function fun = Functions.get(keyspace, name, terms, receiver.ksName, receiver.cfName);
            if (fun == null)
                throw new InvalidRequestException(String.format("Unknown function %s called", name));
            if (fun.isAggregate())
                throw new InvalidRequestException("Aggregation function are not supported in the where clause");

            ScalarFunction scalarFun = (ScalarFunction) fun;

            // Functions.get() will complain if no function "name" type check with the provided arguments.
            // We still have to validate that the return type matches however
            if (!receiver.type.isValueCompatibleWith(scalarFun.returnType()))
                throw new InvalidRequestException(String.format("Type error: cannot assign result of function %s (type %s) to %s (type %s)",
                                                                scalarFun.name(), scalarFun.returnType().asCQL3Type(),
                                                                receiver.name, receiver.type.asCQL3Type()));

            if (fun.argTypes().size() != terms.size())
                throw new InvalidRequestException(String.format("Incorrect number of arguments specified for function %s (expected %d, found %d)",
                                                                fun.name(), fun.argTypes().size(), terms.size()));

            List<Term> parameters = new ArrayList<>(terms.size());
            boolean allTerminal = true;
            for (int i = 0; i < terms.size(); i++)
            {
                Term t = terms.get(i).prepare(keyspace, Functions.makeArgSpec(receiver.ksName, receiver.cfName, scalarFun, i));
                if (t instanceof NonTerminal)
                    allTerminal = false;
                parameters.add(t);
            }

            // If all parameters are terminal and the function is pure, we can
            // evaluate it now, otherwise we'd have to wait execution time
            return allTerminal && scalarFun.isPure()
                ? makeTerminal(scalarFun, execute(scalarFun, parameters), QueryOptions.DEFAULT.getProtocolVersion())
                : new FunctionCall(scalarFun, parameters);
        }

        // All parameters must be terminal
        private static ByteBuffer execute(ScalarFunction fun, List<Term> parameters) throws InvalidRequestException
        {
            List<ByteBuffer> buffers = new ArrayList<>(parameters.size());
            for (Term t : parameters)
            {
                assert t instanceof Term.Terminal;
                buffers.add(((Term.Terminal)t).get(QueryOptions.DEFAULT));
            }

            return executeInternal(Server.CURRENT_VERSION, fun, buffers);
        }

        public AssignmentTestable.TestResult testAssignment(String keyspace, ColumnSpecification receiver)
        {
            // Note: Functions.get() will return null if the function doesn't exist, or throw is no function matching
            // the arguments can be found. We may get one of those if an undefined/wrong function is used as argument
            // of another, existing, function. In that case, we return true here because we'll throw a proper exception
            // later with a more helpful error message that if we were to return false here.
            try
            {
                Function fun = Functions.get(keyspace, name, terms, receiver.ksName, receiver.cfName);
                if (fun != null && receiver.type.equals(fun.returnType()))
                    return AssignmentTestable.TestResult.EXACT_MATCH;
                else if (fun == null || receiver.type.isValueCompatibleWith(fun.returnType()))
                    return AssignmentTestable.TestResult.WEAKLY_ASSIGNABLE;
                else
                    return AssignmentTestable.TestResult.NOT_ASSIGNABLE;
            }
            catch (InvalidRequestException e)
            {
                return AssignmentTestable.TestResult.WEAKLY_ASSIGNABLE;
            }
        }

        @Override
        public String toString()
        {
            StringBuilder sb = new StringBuilder();
            sb.append(name).append("(");
            for (int i = 0; i < terms.size(); i++)
            {
                if (i > 0)
                    sb.append(", ");
                sb.append(terms.get(i));
            }
            return sb.append(")").toString();
        }
    }
}
