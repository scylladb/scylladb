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

import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;

public abstract class AuthenticationStatement extends ParsedStatement implements CQLStatement
{
    @Override
    public Prepared prepare()
    {
        return new Prepared(this);
    }

    public int getBoundTerms()
    {
        return 0;
    }

    public ResultMessage execute(QueryState state, QueryOptions options)
    throws RequestExecutionException, RequestValidationException
    {
        return execute(state.getClientState());
    }

    public abstract ResultMessage execute(ClientState state) throws RequestExecutionException, RequestValidationException;

    public ResultMessage executeInternal(QueryState state, QueryOptions options)
    {
        // executeInternal is for local query only, thus altering users doesn't make sense and is not supported
        throw new UnsupportedOperationException();
    }
}

