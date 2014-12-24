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

import java.util.List;

import com.google.common.base.Objects;

import org.apache.cassandra.db.marshal.AbstractType;

/**
 * Base class for our native/hardcoded functions.
 */
public abstract class AbstractFunction implements Function
{
    protected final FunctionName name;
    protected final List<AbstractType<?>> argTypes;
    protected final AbstractType<?> returnType;

    protected AbstractFunction(FunctionName name, List<AbstractType<?>> argTypes, AbstractType<?> returnType)
    {
        this.name = name;
        this.argTypes = argTypes;
        this.returnType = returnType;
    }

    public FunctionName name()
    {
        return name;
    }

    public List<AbstractType<?>> argTypes()
    {
        return argTypes;
    }

    public AbstractType<?> returnType()
    {
        return returnType;
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof AbstractFunction))
            return false;

        AbstractFunction that = (AbstractFunction)o;
        return Objects.equal(this.name, that.name)
            && Objects.equal(this.argTypes, that.argTypes)
            && Objects.equal(this.returnType, that.returnType);
    }

    public boolean usesFunction(String ksName, String functionName)
    {
        return name.keyspace.equals(ksName) && name.name.equals(functionName);
    }

    public boolean hasReferenceTo(Function function)
    {
        return false;
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(name, argTypes, returnType);
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append(name).append(" : (");
        for (int i = 0; i < argTypes.size(); i++)
        {
            if (i > 0)
                sb.append(", ");
            sb.append(argTypes.get(i).asCQL3Type());
        }
        sb.append(") -> ").append(returnType.asCQL3Type());
        return sb.toString();
    }
}
