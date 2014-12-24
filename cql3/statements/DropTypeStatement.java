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

import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.config.*;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.functions.Functions;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.transport.Event;

public class DropTypeStatement extends SchemaAlteringStatement
{
    private final UTName name;
    private final boolean ifExists;

    public DropTypeStatement(UTName name, boolean ifExists)
    {
        super();
        this.name = name;
        this.ifExists = ifExists;
    }

    @Override
    public void prepareKeyspace(ClientState state) throws InvalidRequestException
    {
        if (!name.hasKeyspace())
            name.setKeyspace(state.getKeyspace());
    }

    public void checkAccess(ClientState state) throws UnauthorizedException, InvalidRequestException
    {
        state.hasKeyspaceAccess(keyspace(), Permission.DROP);
    }

    public void validate(ClientState state) throws RequestValidationException
    {
        KSMetaData ksm = Schema.instance.getKSMetaData(name.getKeyspace());
        if (ksm == null)
            throw new InvalidRequestException(String.format("Cannot drop type in unknown keyspace %s", name.getKeyspace()));

        UserType old = ksm.userTypes.getType(name.getUserTypeName());
        if (old == null)
        {
            if (ifExists)
                return;
            else
                throw new InvalidRequestException(String.format("No user type named %s exists.", name));
        }

        // We don't want to drop a type unless it's not used anymore (mainly because
        // if someone drops a type and recreates one with the same name but different
        // definition with the previous name still in use, things can get messy).
        // We have two places to check: 1) other user type that can nest the one
        // we drop and 2) existing tables referencing the type (maybe in a nested
        // way).

        for (Function function : Functions.all())
        {
            if (isUsedBy(function.returnType()))
                throw new InvalidRequestException(String.format("Cannot drop user type %s as it is still used by function %s", name, function));
            for (AbstractType<?> argType : function.argTypes())
                if (isUsedBy(argType))
                    throw new InvalidRequestException(String.format("Cannot drop user type %s as it is still used by function %s", name, function));
        }

        for (KSMetaData ksm2 : Schema.instance.getKeyspaceDefinitions())
        {
            for (UserType ut : ksm2.userTypes.getAllTypes().values())
            {
                if (ut.keyspace.equals(name.getKeyspace()) && ut.name.equals(name.getUserTypeName()))
                    continue;
                if (isUsedBy(ut))
                    throw new InvalidRequestException(String.format("Cannot drop user type %s as it is still used by user type %s", name, ut.asCQL3Type()));
            }

            for (CFMetaData cfm : ksm2.cfMetaData().values())
                for (ColumnDefinition def : cfm.allColumns())
                    if (isUsedBy(def.type))
                        throw new InvalidRequestException(String.format("Cannot drop user type %s as it is still used by table %s.%s", name, cfm.ksName, cfm.cfName));
        }
    }

    private boolean isUsedBy(AbstractType<?> toCheck) throws RequestValidationException
    {
        if (toCheck instanceof UserType)
        {
            UserType ut = (UserType)toCheck;
            if (name.getKeyspace().equals(ut.keyspace) && name.getUserTypeName().equals(ut.name))
                return true;

            for (AbstractType<?> subtype : ut.fieldTypes())
                if (isUsedBy(subtype))
                    return true;
        }
        else if (toCheck instanceof CompositeType)
        {
            CompositeType ct = (CompositeType)toCheck;
            for (AbstractType<?> subtype : ct.types)
                if (isUsedBy(subtype))
                    return true;
        }
        else if (toCheck instanceof ColumnToCollectionType)
        {
            for (CollectionType collection : ((ColumnToCollectionType)toCheck).defined.values())
                if (isUsedBy(collection))
                    return true;
        }
        else if (toCheck instanceof CollectionType)
        {
            if (toCheck instanceof ListType)
                return isUsedBy(((ListType)toCheck).getElementsType());
            else if (toCheck instanceof SetType)
                return isUsedBy(((SetType)toCheck).getElementsType());
            else
                return isUsedBy(((MapType)toCheck).getKeysType()) || isUsedBy(((MapType)toCheck).getKeysType());
        }
        return false;
    }

    public Event.SchemaChange changeEvent()
    {
        return new Event.SchemaChange(Event.SchemaChange.Change.DROPPED, Event.SchemaChange.Target.TYPE, keyspace(), name.getStringTypeName());
    }

    @Override
    public String keyspace()
    {
        return name.getKeyspace();
    }

    public boolean announceMigration(boolean isLocalOnly) throws InvalidRequestException, ConfigurationException
    {
        KSMetaData ksm = Schema.instance.getKSMetaData(name.getKeyspace());
        assert ksm != null;

        UserType toDrop = ksm.userTypes.getType(name.getUserTypeName());
        // Can be null with ifExists
        if (toDrop == null)
            return false;

        MigrationManager.announceTypeDrop(toDrop, isLocalOnly);
        return true;
    }
}
