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

/*
 * Copyright 2015 Cloudius Systems
 *
 * Modified by Cloudius Systems
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "cql3/statements/schema_altering_statement.hh"
#include "cql3/statements/ks_prop_defs.hh"

#include <memory>

namespace cql3 {

namespace statements {

class alter_keyspace_statement : public schema_altering_statement {
    sstring _name;
    std::unique_ptr<ks_prop_defs> _attrs;

public:
    alter_keyspace_statement(sstring name, std::unique_ptr<ks_prop_defs>&& attrs)
        : _name{name}
        , _attrs{std::move(attrs)}
    { }

    virtual const sstring& keyspace() const override {
        return _name;
    }

#if 0
    public void checkAccess(ClientState state) throws UnauthorizedException, InvalidRequestException
    {
        state.hasKeyspaceAccess(name, Permission.ALTER);
    }

    public void validate(ClientState state) throws RequestValidationException
    {
        KSMetaData ksm = Schema.instance.getKSMetaData(name);
        if (ksm == null)
            throw new InvalidRequestException("Unknown keyspace " + name);
        if (ksm.name.equalsIgnoreCase(SystemKeyspace.NAME))
            throw new InvalidRequestException("Cannot alter system keyspace");

        attrs.validate();

        if (attrs.getReplicationStrategyClass() == null && !attrs.getReplicationOptions().isEmpty())
        {
            throw new ConfigurationException("Missing replication strategy class");
        }
        else if (attrs.getReplicationStrategyClass() != null)
        {
            // The strategy is validated through KSMetaData.validate() in announceKeyspaceUpdate below.
            // However, for backward compatibility with thrift, this doesn't validate unexpected options yet,
            // so doing proper validation here.
            AbstractReplicationStrategy.validateReplicationStrategy(name,
                                                                    AbstractReplicationStrategy.getClass(attrs.getReplicationStrategyClass()),
                                                                    StorageService.instance.getTokenMetadata(),
                                                                    DatabaseDescriptor.getEndpointSnitch(),
                                                                    attrs.getReplicationOptions());
        }
    }

    public boolean announceMigration(boolean isLocalOnly) throws RequestValidationException
    {
        KSMetaData ksm = Schema.instance.getKSMetaData(name);
        // In the (very) unlikely case the keyspace was dropped since validate()
        if (ksm == null)
            throw new InvalidRequestException("Unknown keyspace " + name);

        MigrationManager.announceKeyspaceUpdate(attrs.asKSMetadataUpdate(ksm), isLocalOnly);
        return true;
    }

    public Event.SchemaChange changeEvent()
    {
        return new Event.SchemaChange(Event.SchemaChange.Change.UPDATED, keyspace());
    }
#endif
};

}

}
