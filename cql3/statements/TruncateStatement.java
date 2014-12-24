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

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.thrift.ThriftValidation;

public class TruncateStatement extends CFStatement implements CQLStatement
{
    public TruncateStatement(CFName name)
    {
        super(name);
    }

    public int getBoundTerms()
    {
        return 0;
    }

    public Prepared prepare() throws InvalidRequestException
    {
        return new Prepared(this);
    }

    public void checkAccess(ClientState state) throws InvalidRequestException, UnauthorizedException
    {
        state.hasColumnFamilyAccess(keyspace(), columnFamily(), Permission.MODIFY);
    }

    public void validate(ClientState state) throws InvalidRequestException
    {
        ThriftValidation.validateColumnFamily(keyspace(), columnFamily());
    }

    public ResultMessage execute(QueryState state, QueryOptions options) throws InvalidRequestException, TruncateException
    {
        try
        {
            StorageProxy.truncateBlocking(keyspace(), columnFamily());
        }
        catch (UnavailableException e)
        {
            throw new TruncateException(e);
        }
        catch (TimeoutException e)
        {
            throw new TruncateException(e);
        }
        catch (IOException e)
        {
            throw new TruncateException(e);
        }
        return null;
    }

    public ResultMessage executeInternal(QueryState state, QueryOptions options)
    {
        throw new UnsupportedOperationException();
    }
}
