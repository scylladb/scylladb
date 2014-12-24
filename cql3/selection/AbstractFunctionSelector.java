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
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.text.StrBuilder;

import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.exceptions.InvalidRequestException;

abstract class AbstractFunctionSelector<T extends Function> extends Selector
{
    protected final T fun;

    /**
     * The list used to pass the function arguments is recycled to avoid the cost of instantiating a new list
     * with each function call.
     */
    protected final List<ByteBuffer> args;
    protected final List<Selector> argSelectors;

    public static Factory newFactory(final Function fun, final SelectorFactories factories) throws InvalidRequestException
    {
        if (fun.isAggregate())
        {
            if (factories.doesAggregation())
                throw new InvalidRequestException("aggregate functions cannot be used as arguments of aggregate functions");
        }
        else
        {
            if (factories.doesAggregation() && !factories.containsOnlyAggregateFunctions())
                throw new InvalidRequestException(String.format("the %s function arguments must be either all aggregates or all none aggregates",
                                                                fun.name()));
        }

        return new Factory()
        {
            protected String getColumnName()
            {
                return new StrBuilder(fun.name().toString()).append('(')
                                                            .appendWithSeparators(factories.getColumnNames(), ", ")
                                                            .append(')')
                                                            .toString();
            }

            protected AbstractType<?> getReturnType()
            {
                return fun.returnType();
            }

            public boolean usesFunction(String ksName, String functionName)
            {
                return fun.usesFunction(ksName, functionName);
            }

            public Selector newInstance() throws InvalidRequestException
            {
                return fun.isAggregate() ? new AggregateFunctionSelector(fun, factories.newInstances())
                                         : new ScalarFunctionSelector(fun, factories.newInstances());
            }

            public boolean isWritetimeSelectorFactory()
            {
                return factories.containsWritetimeSelectorFactory();
            }

            public boolean isTTLSelectorFactory()
            {
                return factories.containsTTLSelectorFactory();
            }

            public boolean isAggregateSelectorFactory()
            {
                return fun.isAggregate() || factories.containsOnlyAggregateFunctions();
            }
        };
    }

    protected AbstractFunctionSelector(T fun, List<Selector> argSelectors)
    {
        this.fun = fun;
        this.argSelectors = argSelectors;
        this.args = Arrays.asList(new ByteBuffer[argSelectors.size()]);
    }

    public AbstractType<?> getType()
    {
        return fun.returnType();
    }

    @Override
    public String toString()
    {
        return new StrBuilder().append(fun.name())
                               .append("(")
                               .appendWithSeparators(argSelectors, ", ")
                               .append(")")
                               .toString();
    }
}
