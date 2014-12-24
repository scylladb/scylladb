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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.service.QueryState;

public abstract class BatchQueryOptions
{
    public static BatchQueryOptions DEFAULT = withoutPerStatementVariables(QueryOptions.DEFAULT);

    protected final QueryOptions wrapped;
    private final List<Object> queryOrIdList;

    protected BatchQueryOptions(QueryOptions wrapped, List<Object> queryOrIdList)
    {
        this.wrapped = wrapped;
        this.queryOrIdList = queryOrIdList;
    }

    public static BatchQueryOptions withoutPerStatementVariables(QueryOptions options)
    {
        return new WithoutPerStatementVariables(options, Collections.<Object>emptyList());
    }

    public static BatchQueryOptions withPerStatementVariables(QueryOptions options, List<List<ByteBuffer>> variables, List<Object> queryOrIdList)
    {
        return new WithPerStatementVariables(options, variables, queryOrIdList);
    }

    public abstract QueryOptions forStatement(int i);

    public ConsistencyLevel getConsistency()
    {
        return wrapped.getConsistency();
    }

    public ConsistencyLevel getSerialConsistency()
    {
        return wrapped.getSerialConsistency();
    }

    public List<Object> getQueryOrIdList()
    {
        return queryOrIdList;
    }

    public long getTimestamp(QueryState state)
    {
        return wrapped.getTimestamp(state);
    }

    private static class WithoutPerStatementVariables extends BatchQueryOptions
    {
        private WithoutPerStatementVariables(QueryOptions wrapped, List<Object> queryOrIdList)
        {
            super(wrapped, queryOrIdList);
        }

        public QueryOptions forStatement(int i)
        {
            return wrapped;
        }
    }

    private static class WithPerStatementVariables extends BatchQueryOptions
    {
        private final List<QueryOptions> perStatementOptions;

        private WithPerStatementVariables(QueryOptions wrapped, List<List<ByteBuffer>> variables, List<Object> queryOrIdList)
        {
            super(wrapped, queryOrIdList);
            this.perStatementOptions = new ArrayList<>(variables.size());
            for (final List<ByteBuffer> vars : variables)
            {
                perStatementOptions.add(new QueryOptions.QueryOptionsWrapper(wrapped)
                {
                    public List<ByteBuffer> getValues()
                    {
                        return vars;
                    }
                });
            }
        }

        public QueryOptions forStatement(int i)
        {
            return perStatementOptions.get(i);
        }
    }
}
