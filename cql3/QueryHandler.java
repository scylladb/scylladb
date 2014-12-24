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

import org.apache.cassandra.cql3.statements.BatchStatement;
import org.apache.cassandra.cql3.statements.ParsedStatement;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.MD5Digest;

public interface QueryHandler
{
    public ResultMessage process(String query, QueryState state, QueryOptions options) throws RequestExecutionException, RequestValidationException;
    public ResultMessage.Prepared prepare(String query, QueryState state) throws RequestValidationException;
    public ParsedStatement.Prepared getPrepared(MD5Digest id);
    public ParsedStatement.Prepared getPreparedForThrift(Integer id);
    public ResultMessage processPrepared(CQLStatement statement, QueryState state, QueryOptions options) throws RequestExecutionException, RequestValidationException;
    public ResultMessage processBatch(BatchStatement statement, QueryState state, BatchQueryOptions options) throws RequestExecutionException, RequestValidationException;
}
