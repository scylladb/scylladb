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

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.thrift.ThriftValidation;
import org.apache.cassandra.transport.Event;

public class DropKeyspaceStatement extends SchemaAlteringStatement
{
    private final String keyspace;
    private final boolean ifExists;

    public DropKeyspaceStatement(String keyspace, boolean ifExists)
    {
        super();
        this.keyspace = keyspace;
        this.ifExists = ifExists;
    }

    public void checkAccess(ClientState state) throws UnauthorizedException, InvalidRequestException
    {
        state.hasKeyspaceAccess(keyspace, Permission.DROP);
    }

    public void validate(ClientState state) throws RequestValidationException
    {
        ThriftValidation.validateKeyspaceNotSystem(keyspace);
    }

    @Override
    public String keyspace()
    {
        return keyspace;
    }

    public boolean announceMigration(boolean isLocalOnly) throws ConfigurationException
    {
        try
        {
            MigrationManager.announceKeyspaceDrop(keyspace, isLocalOnly);
            return true;
        }
        catch(ConfigurationException e)
        {
            if (ifExists)
                return false;
            throw e;
        }
    }

    public Event.SchemaChange changeEvent()
    {
        return new Event.SchemaChange(Event.SchemaChange.Change.DROPPED, keyspace());
    }
}
