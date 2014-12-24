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
import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.exceptions.AlreadyExistsException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.thrift.ThriftValidation;
import org.apache.cassandra.transport.Event;

/** A <code>CREATE KEYSPACE</code> statement parsed from a CQL query. */
public class CreateKeyspaceStatement extends SchemaAlteringStatement
{
    private final String name;
    private final KSPropDefs attrs;
    private final boolean ifNotExists;

    /**
     * Creates a new <code>CreateKeyspaceStatement</code> instance for a given
     * keyspace name and keyword arguments.
     *
     * @param name the name of the keyspace to create
     * @param attrs map of the raw keyword arguments that followed the <code>WITH</code> keyword.
     */
    public CreateKeyspaceStatement(String name, KSPropDefs attrs, boolean ifNotExists)
    {
        super();
        this.name = name;
        this.attrs = attrs;
        this.ifNotExists = ifNotExists;
    }

    @Override
    public String keyspace()
    {
        return name;
    }

    public void checkAccess(ClientState state) throws UnauthorizedException
    {
        state.hasAllKeyspacesAccess(Permission.CREATE);
    }

    /**
     * The <code>CqlParser</code> only goes as far as extracting the keyword arguments
     * from these statements, so this method is responsible for processing and
     * validating.
     *
     * @throws InvalidRequestException if arguments are missing or unacceptable
     */
    public void validate(ClientState state) throws RequestValidationException
    {
        ThriftValidation.validateKeyspaceNotSystem(name);

        // keyspace name
        if (!name.matches("\\w+"))
            throw new InvalidRequestException(String.format("\"%s\" is not a valid keyspace name", name));
        if (name.length() > Schema.NAME_LENGTH)
            throw new InvalidRequestException(String.format("Keyspace names shouldn't be more than %s characters long (got \"%s\")", Schema.NAME_LENGTH, name));

        attrs.validate();

        if (attrs.getReplicationStrategyClass() == null)
            throw new ConfigurationException("Missing mandatory replication strategy class");

        // The strategy is validated through KSMetaData.validate() in announceNewKeyspace below.
        // However, for backward compatibility with thrift, this doesn't validate unexpected options yet,
        // so doing proper validation here.
        AbstractReplicationStrategy.validateReplicationStrategy(name,
                                                                AbstractReplicationStrategy.getClass(attrs.getReplicationStrategyClass()),
                                                                StorageService.instance.getTokenMetadata(),
                                                                DatabaseDescriptor.getEndpointSnitch(),
                                                                attrs.getReplicationOptions());
    }

    public boolean announceMigration(boolean isLocalOnly) throws RequestValidationException
    {
        try
        {
            MigrationManager.announceNewKeyspace(attrs.asKSMetadata(name), isLocalOnly);
            return true;
        }
        catch (AlreadyExistsException e)
        {
            if (ifNotExists)
                return false;
            throw e;
        }
    }

    public Event.SchemaChange changeEvent()
    {
        return new Event.SchemaChange(Event.SchemaChange.Change.CREATED, keyspace());
    }
}
