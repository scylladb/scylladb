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

import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.exceptions.*;

public interface CQLStatement
{
    /**
     * Returns the number of bound terms in this statement.
     */
    public int getBoundTerms();

    /**
     * Perform any access verification necessary for the statement.
     *
     * @param state the current client state
     */
    public void checkAccess(ClientState state) throws UnauthorizedException, InvalidRequestException;

    /**
     * Perform additional validation required by the statment.
     * To be overriden by subclasses if needed.
     *
     * @param state the current client state
     */
    public void validate(ClientState state) throws RequestValidationException;

    /**
     * Execute the statement and return the resulting result or null if there is no result.
     *
     * @param state the current query state
     * @param options options for this query (consistency, variables, pageSize, ...)
     */
    public ResultMessage execute(QueryState state, QueryOptions options) throws RequestValidationException, RequestExecutionException;

    /**
     * Variante of execute used for internal query against the system tables, and thus only query the local node.
     *
     * @param state the current query state
     */
    public ResultMessage executeInternal(QueryState state, QueryOptions options) throws RequestValidationException, RequestExecutionException;

    boolean usesFunction(String ksName, String functionName);
}
