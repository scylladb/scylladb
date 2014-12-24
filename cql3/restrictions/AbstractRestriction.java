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

import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.statements.Bound;
import org.apache.cassandra.exceptions.InvalidRequestException;

import static org.apache.cassandra.cql3.statements.RequestValidations.checkFalse;
import static org.apache.cassandra.cql3.statements.RequestValidations.checkNotNull;

/**
 * Base class for <code>Restriction</code>s
 */
abstract class AbstractRestriction  implements Restriction
{
    @Override
    public  boolean isOnToken()
    {
        return false;
    }

    @Override
    public boolean isMultiColumn()
    {
        return false;
    }

    @Override
    public boolean isSlice()
    {
        return false;
    }

    @Override
    public boolean isEQ()
    {
        return false;
    }

    @Override
    public boolean isIN()
    {
        return false;
    }

    @Override
    public boolean isContains()
    {
        return false;
    }

    @Override
    public boolean hasBound(Bound b)
    {
        return true;
    }

    @Override
    public List<ByteBuffer> bounds(Bound b, QueryOptions options) throws InvalidRequestException
    {
        return values(options);
    }

    @Override
    public boolean isInclusive(Bound b)
    {
        return true;
    }

    protected static ByteBuffer validateIndexedValue(ColumnSpecification columnSpec,
                                                     ByteBuffer value)
                                                     throws InvalidRequestException
    {
        checkNotNull(value, "Unsupported null value for indexed column %s", columnSpec.name);
        checkFalse(value.remaining() > 0xFFFF, "Index expression values may not be larger than 64K");
        return value;
    }

    /**
     * Checks if the specified term is using the specified function.
     *
     * @param term the term to check
     * @param ksName the function keyspace name
     * @param functionName the function name
     * @return <code>true</code> if the specified term is using the specified function, <code>false</code> otherwise.
     */
    protected static final boolean usesFunction(Term term, String ksName, String functionName)
    {
        return term != null && term.usesFunction(ksName, functionName);
    }

    /**
     * Checks if one of the specified term is using the specified function.
     *
     * @param terms the terms to check
     * @param ksName the function keyspace name
     * @param functionName the function name
     * @return <code>true</code> if onee of the specified term is using the specified function, <code>false</code> otherwise.
     */
    protected static final boolean usesFunction(List<Term> terms, String ksName, String functionName)
    {
        if (terms != null)
            for (Term value : terms)
                if (usesFunction(value, ksName, functionName))
                    return true;
        return false;
    }
}