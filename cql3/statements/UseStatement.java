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
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;

public class UseStatement extends ParsedStatement implements CQLStatement
{
    private final String keyspace;

    public UseStatement(String keyspace)
    {
        this.keyspace = keyspace;
    }

    public int getBoundTerms()
    {
        return 0;
    }

    public Prepared prepare() throws InvalidRequestException
    {
        return new Prepared(this);
    }

    public void checkAccess(ClientState state) throws UnauthorizedException
    {
        state.validateLogin();
    }

    public void validate(ClientState state) throws InvalidRequestException
    {
    }

    public ResultMessage execute(QueryState state, QueryOptions options) throws InvalidRequestException
    {
        state.getClientState().setKeyspace(keyspace);
        return new ResultMessage.SetKeyspace(keyspace);
    }

    public ResultMessage executeInternal(QueryState state, QueryOptions options)
    {
        // Internal queries are exclusively on the system keyspace and 'use' is thus useless
        throw new UnsupportedOperationException();
    }
}
