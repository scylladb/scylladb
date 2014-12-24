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

import java.util.Collections;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.IndexType;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.index.SecondaryIndex;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.thrift.ThriftValidation;
import org.apache.cassandra.transport.Event;

/** A <code>CREATE INDEX</code> statement parsed from a CQL query. */
public class CreateIndexStatement extends SchemaAlteringStatement
{
    private static final Logger logger = LoggerFactory.getLogger(CreateIndexStatement.class);

    private final String indexName;
    private final IndexTarget.Raw rawTarget;
    private final IndexPropDefs properties;
    private final boolean ifNotExists;

    public CreateIndexStatement(CFName name,
                                String indexName,
                                IndexTarget.Raw target,
                                IndexPropDefs properties,
                                boolean ifNotExists)
    {
        super(name);
        this.indexName = indexName;
        this.rawTarget = target;
        this.properties = properties;
        this.ifNotExists = ifNotExists;
    }

    public void checkAccess(ClientState state) throws UnauthorizedException, InvalidRequestException
    {
        state.hasColumnFamilyAccess(keyspace(), columnFamily(), Permission.ALTER);
    }

    public void validate(ClientState state) throws RequestValidationException
    {
        CFMetaData cfm = ThriftValidation.validateColumnFamily(keyspace(), columnFamily());
        if (cfm.isCounter())
            throw new InvalidRequestException("Secondary indexes are not supported on counter tables");

        IndexTarget target = rawTarget.prepare(cfm);
        ColumnDefinition cd = cfm.getColumnDefinition(target.column);

        if (cd == null)
            throw new InvalidRequestException("No column definition found for column " + target.column);

        boolean isMap = cd.type instanceof MapType;
        boolean isFrozenCollection = cd.type.isCollection() && !cd.type.isMultiCell();

        if (isFrozenCollection)
        {
            validateForFrozenCollection(target);
        }
        else
        {
            validateNotFullIndex(target);
            validateIsValuesIndexIfTargetColumnNotCollection(cd, target);
            validateTargetColumnIsMapIfIndexInvolvesKeys(isMap, target);
        }

        if (cd.getIndexType() != null)
        {
            IndexTarget.TargetType prevType = IndexTarget.TargetType.fromColumnDefinition(cd);
            if (isMap && target.type != prevType)
            {
                String msg = "Cannot create index on %s(%s): an index on %s(%s) already exists and indexing " +
                             "a map on more than one dimension at the same time is not currently supported";
                throw new InvalidRequestException(String.format(msg,
                                                                target.type, target.column,
                                                                prevType, target.column));
            }

            if (ifNotExists)
                return;
            else
                throw new InvalidRequestException("Index already exists");
        }

        properties.validate();

        // TODO: we could lift that limitation
        if ((cfm.comparator.isDense() || !cfm.comparator.isCompound()) && cd.kind != ColumnDefinition.Kind.REGULAR)
            throw new InvalidRequestException("Secondary indexes are not supported on PRIMARY KEY columns in COMPACT STORAGE tables");

        // It would be possible to support 2ndary index on static columns (but not without modifications of at least ExtendedFilter and
        // CompositesIndex) and maybe we should, but that means a query like:
        //     SELECT * FROM foo WHERE static_column = 'bar'
        // would pull the full partition every time the static column of partition is 'bar', which sounds like offering a
        // fair potential for foot-shooting, so I prefer leaving that to a follow up ticket once we have identified cases where
        // such indexing is actually useful.
        if (cd.isStatic())
            throw new InvalidRequestException("Secondary indexes are not allowed on static columns");

        if (cd.kind == ColumnDefinition.Kind.PARTITION_KEY && cd.isOnAllComponents())
            throw new InvalidRequestException(String.format("Cannot create secondary index on partition key column %s", target.column));
    }

    private void validateForFrozenCollection(IndexTarget target) throws InvalidRequestException
    {
        if (target.type != IndexTarget.TargetType.FULL)
            throw new InvalidRequestException(String.format("Cannot create index on %s of frozen<map> column %s", target.type, target.column));
    }

    private void validateNotFullIndex(IndexTarget target) throws InvalidRequestException
    {
        if (target.type == IndexTarget.TargetType.FULL)
            throw new InvalidRequestException("full() indexes can only be created on frozen collections");
    }

    private void validateIsValuesIndexIfTargetColumnNotCollection(ColumnDefinition cd, IndexTarget target) throws InvalidRequestException
    {
        if (!cd.type.isCollection() && target.type != IndexTarget.TargetType.VALUES)
            throw new InvalidRequestException(String.format("Cannot create index on %s of column %s; only non-frozen collections support %s indexes",
                                                            target.type, target.column, target.type));
    }

    private void validateTargetColumnIsMapIfIndexInvolvesKeys(boolean isMap, IndexTarget target) throws InvalidRequestException
    {
        if (target.type == IndexTarget.TargetType.KEYS || target.type == IndexTarget.TargetType.KEYS_AND_VALUES)
        {
            if (!isMap)
                throw new InvalidRequestException(String.format("Cannot create index on %s of column %s with non-map type",
                                                                target.type, target.column));
        }
    }

    public boolean announceMigration(boolean isLocalOnly) throws RequestValidationException
    {
        CFMetaData cfm = Schema.instance.getCFMetaData(keyspace(), columnFamily()).copy();
        IndexTarget target = rawTarget.prepare(cfm);
        logger.debug("Updating column {} definition for index {}", target.column, indexName);
        ColumnDefinition cd = cfm.getColumnDefinition(target.column);

        if (cd.getIndexType() != null && ifNotExists)
            return false;

        if (properties.isCustom)
        {
            cd.setIndexType(IndexType.CUSTOM, properties.getOptions());
        }
        else if (cfm.comparator.isCompound())
        {
            Map<String, String> options = Collections.emptyMap();
            // For now, we only allow indexing values for collections, but we could later allow
            // to also index map keys, so we record that this is the values we index to make our
            // lives easier then.
            if (cd.type.isCollection() && cd.type.isMultiCell())
                options = ImmutableMap.of(target.type.indexOption(), "");
            cd.setIndexType(IndexType.COMPOSITES, options);
        }
        else
        {
            cd.setIndexType(IndexType.KEYS, Collections.<String, String>emptyMap());
        }

        cd.setIndexName(indexName);
        cfm.addDefaultIndexNames();
        MigrationManager.announceColumnFamilyUpdate(cfm, false, isLocalOnly);
        return true;
    }

    public Event.SchemaChange changeEvent()
    {
        // Creating an index is akin to updating the CF
        return new Event.SchemaChange(Event.SchemaChange.Change.UPDATED, Event.SchemaChange.Target.TABLE, keyspace(), columnFamily());
    }
}
