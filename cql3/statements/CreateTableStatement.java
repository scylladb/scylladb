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

import java.nio.ByteBuffer;
import java.util.*;

import org.apache.cassandra.exceptions.*;
import org.apache.commons.lang3.StringUtils;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;

import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.db.composites.*;
import org.apache.cassandra.db.ColumnFamilyType;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.exceptions.AlreadyExistsException;
import org.apache.cassandra.io.compress.CompressionParameters;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.transport.Event;
import org.apache.cassandra.utils.ByteBufferUtil;

/** A <code>CREATE TABLE</code> parsed from a CQL query statement. */
public class CreateTableStatement extends SchemaAlteringStatement
{
    public CellNameType comparator;
    private AbstractType<?> defaultValidator;
    private AbstractType<?> keyValidator;

    private final List<ByteBuffer> keyAliases = new ArrayList<ByteBuffer>();
    private final List<ByteBuffer> columnAliases = new ArrayList<ByteBuffer>();
    private ByteBuffer valueAlias;

    private boolean isDense;

    private final Map<ColumnIdentifier, AbstractType> columns = new HashMap<ColumnIdentifier, AbstractType>();
    private final Set<ColumnIdentifier> staticColumns;
    private final CFPropDefs properties;
    private final boolean ifNotExists;

    public CreateTableStatement(CFName name, CFPropDefs properties, boolean ifNotExists, Set<ColumnIdentifier> staticColumns)
    {
        super(name);
        this.properties = properties;
        this.ifNotExists = ifNotExists;
        this.staticColumns = staticColumns;

        try
        {
            if (!this.properties.hasProperty(CFPropDefs.KW_COMPRESSION) && CFMetaData.DEFAULT_COMPRESSOR != null)
                this.properties.addProperty(CFPropDefs.KW_COMPRESSION,
                                            new HashMap<String, String>()
                                            {{
                                                put(CompressionParameters.SSTABLE_COMPRESSION, CFMetaData.DEFAULT_COMPRESSOR);
                                            }});
        }
        catch (SyntaxException e)
        {
            throw new AssertionError(e);
        }
    }

    public void checkAccess(ClientState state) throws UnauthorizedException, InvalidRequestException
    {
        state.hasKeyspaceAccess(keyspace(), Permission.CREATE);
    }

    public void validate(ClientState state)
    {
        // validated in announceMigration()
    }

    // Column definitions
    private List<ColumnDefinition> getColumns(CFMetaData cfm)
    {
        List<ColumnDefinition> columnDefs = new ArrayList<>(columns.size());
        Integer componentIndex = comparator.isCompound() ? comparator.clusteringPrefixSize() : null;
        for (Map.Entry<ColumnIdentifier, AbstractType> col : columns.entrySet())
        {
            ColumnIdentifier id = col.getKey();
            columnDefs.add(staticColumns.contains(id)
                           ? ColumnDefinition.staticDef(cfm, col.getKey().bytes, col.getValue(), componentIndex)
                           : ColumnDefinition.regularDef(cfm, col.getKey().bytes, col.getValue(), componentIndex));
        }

        return columnDefs;
    }

    public boolean announceMigration(boolean isLocalOnly) throws RequestValidationException
    {
        try
        {
            MigrationManager.announceNewColumnFamily(getCFMetaData(), isLocalOnly);
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
        return new Event.SchemaChange(Event.SchemaChange.Change.CREATED, Event.SchemaChange.Target.TABLE, keyspace(), columnFamily());
    }

    /**
     * Returns a CFMetaData instance based on the parameters parsed from this
     * <code>CREATE</code> statement, or defaults where applicable.
     *
     * @return a CFMetaData instance corresponding to the values parsed from this statement
     * @throws InvalidRequestException on failure to validate parsed parameters
     */
    public CFMetaData getCFMetaData() throws RequestValidationException
    {
        CFMetaData newCFMD;
        newCFMD = new CFMetaData(keyspace(),
                                 columnFamily(),
                                 ColumnFamilyType.Standard,
                                 comparator);
        applyPropertiesTo(newCFMD);
        return newCFMD;
    }

    public void applyPropertiesTo(CFMetaData cfmd) throws RequestValidationException
    {
        cfmd.defaultValidator(defaultValidator)
            .keyValidator(keyValidator)
            .addAllColumnDefinitions(getColumns(cfmd))
            .isDense(isDense);

        addColumnMetadataFromAliases(cfmd, keyAliases, keyValidator, ColumnDefinition.Kind.PARTITION_KEY);
        addColumnMetadataFromAliases(cfmd, columnAliases, comparator.asAbstractType(), ColumnDefinition.Kind.CLUSTERING_COLUMN);
        if (valueAlias != null)
            addColumnMetadataFromAliases(cfmd, Collections.singletonList(valueAlias), defaultValidator, ColumnDefinition.Kind.COMPACT_VALUE);

        properties.applyToCFMetadata(cfmd);
    }

    private void addColumnMetadataFromAliases(CFMetaData cfm, List<ByteBuffer> aliases, AbstractType<?> comparator, ColumnDefinition.Kind kind)
    {
        if (comparator instanceof CompositeType)
        {
            CompositeType ct = (CompositeType)comparator;
            for (int i = 0; i < aliases.size(); ++i)
                if (aliases.get(i) != null)
                    cfm.addOrReplaceColumnDefinition(new ColumnDefinition(cfm, aliases.get(i), ct.types.get(i), i, kind));
        }
        else
        {
            assert aliases.size() <= 1;
            if (!aliases.isEmpty() && aliases.get(0) != null)
                cfm.addOrReplaceColumnDefinition(new ColumnDefinition(cfm, aliases.get(0), comparator, null, kind));
        }
    }


    public static class RawStatement extends CFStatement
    {
        private final Map<ColumnIdentifier, CQL3Type.Raw> definitions = new HashMap<>();
        public final CFPropDefs properties = new CFPropDefs();

        private final List<List<ColumnIdentifier>> keyAliases = new ArrayList<List<ColumnIdentifier>>();
        private final List<ColumnIdentifier> columnAliases = new ArrayList<ColumnIdentifier>();
        private final Map<ColumnIdentifier, Boolean> definedOrdering = new LinkedHashMap<ColumnIdentifier, Boolean>(); // Insertion ordering is important
        private final Set<ColumnIdentifier> staticColumns = new HashSet<ColumnIdentifier>();

        private boolean useCompactStorage;
        private final Multiset<ColumnIdentifier> definedNames = HashMultiset.create(1);

        private final boolean ifNotExists;

        public RawStatement(CFName name, boolean ifNotExists)
        {
            super(name);
            this.ifNotExists = ifNotExists;
        }

        /**
         * Transform this raw statement into a CreateTableStatement.
         */
        public ParsedStatement.Prepared prepare() throws RequestValidationException
        {
            // Column family name
            if (!columnFamily().matches("\\w+"))
                throw new InvalidRequestException(String.format("\"%s\" is not a valid table name (must be alphanumeric character only: [0-9A-Za-z]+)", columnFamily()));
            if (columnFamily().length() > Schema.NAME_LENGTH)
                throw new InvalidRequestException(String.format("Table names shouldn't be more than %s characters long (got \"%s\")", Schema.NAME_LENGTH, columnFamily()));

            for (Multiset.Entry<ColumnIdentifier> entry : definedNames.entrySet())
                if (entry.getCount() > 1)
                    throw new InvalidRequestException(String.format("Multiple definition of identifier %s", entry.getElement()));

            properties.validate();

            CreateTableStatement stmt = new CreateTableStatement(cfName, properties, ifNotExists, staticColumns);

            Map<ByteBuffer, CollectionType> definedMultiCellCollections = null;
            for (Map.Entry<ColumnIdentifier, CQL3Type.Raw> entry : definitions.entrySet())
            {
                ColumnIdentifier id = entry.getKey();
                CQL3Type pt = entry.getValue().prepare(keyspace());
                if (pt.isCollection() && ((CollectionType) pt.getType()).isMultiCell())
                {
                    if (definedMultiCellCollections == null)
                        definedMultiCellCollections = new HashMap<>();
                    definedMultiCellCollections.put(id.bytes, (CollectionType) pt.getType());
                }
                stmt.columns.put(id, pt.getType()); // we'll remove what is not a column below
            }

            if (keyAliases.isEmpty())
                throw new InvalidRequestException("No PRIMARY KEY specifed (exactly one required)");
            else if (keyAliases.size() > 1)
                throw new InvalidRequestException("Multiple PRIMARY KEYs specifed (exactly one required)");

            List<ColumnIdentifier> kAliases = keyAliases.get(0);

            List<AbstractType<?>> keyTypes = new ArrayList<AbstractType<?>>(kAliases.size());
            for (ColumnIdentifier alias : kAliases)
            {
                stmt.keyAliases.add(alias.bytes);
                AbstractType<?> t = getTypeAndRemove(stmt.columns, alias);
                if (t instanceof CounterColumnType)
                    throw new InvalidRequestException(String.format("counter type is not supported for PRIMARY KEY part %s", alias));
                if (staticColumns.contains(alias))
                    throw new InvalidRequestException(String.format("Static column %s cannot be part of the PRIMARY KEY", alias));
                keyTypes.add(t);
            }
            stmt.keyValidator = keyTypes.size() == 1 ? keyTypes.get(0) : CompositeType.getInstance(keyTypes);

            // Dense means that no part of the comparator stores a CQL column name. This means
            // COMPACT STORAGE with at least one columnAliases (otherwise it's a thrift "static" CF).
            stmt.isDense = useCompactStorage && !columnAliases.isEmpty();

            // Handle column aliases
            if (columnAliases.isEmpty())
            {
                if (useCompactStorage)
                {
                    // There should remain some column definition since it is a non-composite "static" CF
                    if (stmt.columns.isEmpty())
                        throw new InvalidRequestException("No definition found that is not part of the PRIMARY KEY");

                    if (definedMultiCellCollections != null)
                        throw new InvalidRequestException("Non-frozen collection types are not supported with COMPACT STORAGE");

                    stmt.comparator = new SimpleSparseCellNameType(UTF8Type.instance);
                }
                else
                {
                    stmt.comparator = definedMultiCellCollections == null
                                    ? new CompoundSparseCellNameType(Collections.<AbstractType<?>>emptyList())
                                    : new CompoundSparseCellNameType.WithCollection(Collections.<AbstractType<?>>emptyList(), ColumnToCollectionType.getInstance(definedMultiCellCollections));
                }
            }
            else
            {
                // If we use compact storage and have only one alias, it is a
                // standard "dynamic" CF, otherwise it's a composite
                if (useCompactStorage && columnAliases.size() == 1)
                {
                    if (definedMultiCellCollections != null)
                        throw new InvalidRequestException("Collection types are not supported with COMPACT STORAGE");

                    ColumnIdentifier alias = columnAliases.get(0);
                    if (staticColumns.contains(alias))
                        throw new InvalidRequestException(String.format("Static column %s cannot be part of the PRIMARY KEY", alias));

                    stmt.columnAliases.add(alias.bytes);
                    AbstractType<?> at = getTypeAndRemove(stmt.columns, alias);
                    if (at instanceof CounterColumnType)
                        throw new InvalidRequestException(String.format("counter type is not supported for PRIMARY KEY part %s", stmt.columnAliases.get(0)));
                    stmt.comparator = new SimpleDenseCellNameType(at);
                }
                else
                {
                    List<AbstractType<?>> types = new ArrayList<AbstractType<?>>(columnAliases.size() + 1);
                    for (ColumnIdentifier t : columnAliases)
                    {
                        stmt.columnAliases.add(t.bytes);

                        AbstractType<?> type = getTypeAndRemove(stmt.columns, t);
                        if (type instanceof CounterColumnType)
                            throw new InvalidRequestException(String.format("counter type is not supported for PRIMARY KEY part %s", t));
                        if (staticColumns.contains(t))
                            throw new InvalidRequestException(String.format("Static column %s cannot be part of the PRIMARY KEY", t));
                        types.add(type);
                    }

                    if (useCompactStorage)
                    {
                        if (definedMultiCellCollections != null)
                            throw new InvalidRequestException("Collection types are not supported with COMPACT STORAGE");

                        stmt.comparator = new CompoundDenseCellNameType(types);
                    }
                    else
                    {
                        stmt.comparator = definedMultiCellCollections == null
                                        ? new CompoundSparseCellNameType(types)
                                        : new CompoundSparseCellNameType.WithCollection(types, ColumnToCollectionType.getInstance(definedMultiCellCollections));
                    }
                }
            }

            if (!staticColumns.isEmpty())
            {
                // Only CQL3 tables can have static columns
                if (useCompactStorage)
                    throw new InvalidRequestException("Static columns are not supported in COMPACT STORAGE tables");
                // Static columns only make sense if we have at least one clustering column. Otherwise everything is static anyway
                if (columnAliases.isEmpty())
                    throw new InvalidRequestException("Static columns are only useful (and thus allowed) if the table has at least one clustering column");
            }

            if (useCompactStorage && !stmt.columnAliases.isEmpty())
            {
                if (stmt.columns.isEmpty())
                {
                    // The only value we'll insert will be the empty one, so the default validator don't matter
                    stmt.defaultValidator = BytesType.instance;
                    // We need to distinguish between
                    //   * I'm upgrading from thrift so the valueAlias is null
                    //   * I've defined my table with only a PK (and the column value will be empty)
                    // So, we use an empty valueAlias (rather than null) for the second case
                    stmt.valueAlias = ByteBufferUtil.EMPTY_BYTE_BUFFER;
                }
                else
                {
                    if (stmt.columns.size() > 1)
                        throw new InvalidRequestException(String.format("COMPACT STORAGE with composite PRIMARY KEY allows no more than one column not part of the PRIMARY KEY (got: %s)", StringUtils.join(stmt.columns.keySet(), ", ")));

                    Map.Entry<ColumnIdentifier, AbstractType> lastEntry = stmt.columns.entrySet().iterator().next();
                    stmt.defaultValidator = lastEntry.getValue();
                    stmt.valueAlias = lastEntry.getKey().bytes;
                    stmt.columns.remove(lastEntry.getKey());
                }
            }
            else
            {
                // For compact, we are in the "static" case, so we need at least one column defined. For non-compact however, having
                // just the PK is fine since we have CQL3 row marker.
                if (useCompactStorage && stmt.columns.isEmpty())
                    throw new InvalidRequestException("COMPACT STORAGE with non-composite PRIMARY KEY require one column not part of the PRIMARY KEY, none given");

                // There is no way to insert/access a column that is not defined for non-compact storage, so
                // the actual validator don't matter much (except that we want to recognize counter CF as limitation apply to them).
                stmt.defaultValidator = !stmt.columns.isEmpty() && (stmt.columns.values().iterator().next() instanceof CounterColumnType)
                    ? CounterColumnType.instance
                    : BytesType.instance;
            }


            // If we give a clustering order, we must explicitly do so for all aliases and in the order of the PK
            if (!definedOrdering.isEmpty())
            {
                if (definedOrdering.size() > columnAliases.size())
                    throw new InvalidRequestException("Only clustering key columns can be defined in CLUSTERING ORDER directive");

                int i = 0;
                for (ColumnIdentifier id : definedOrdering.keySet())
                {
                    ColumnIdentifier c = columnAliases.get(i);
                    if (!id.equals(c))
                    {
                        if (definedOrdering.containsKey(c))
                            throw new InvalidRequestException(String.format("The order of columns in the CLUSTERING ORDER directive must be the one of the clustering key (%s must appear before %s)", c, id));
                        else
                            throw new InvalidRequestException(String.format("Missing CLUSTERING ORDER for column %s", c));
                    }
                    ++i;
                }
            }

            return new ParsedStatement.Prepared(stmt);
        }

        private AbstractType<?> getTypeAndRemove(Map<ColumnIdentifier, AbstractType> columns, ColumnIdentifier t) throws InvalidRequestException
        {
            AbstractType type = columns.get(t);
            if (type == null)
                throw new InvalidRequestException(String.format("Unknown definition %s referenced in PRIMARY KEY", t));
            if (type.isCollection() && type.isMultiCell())
                throw new InvalidRequestException(String.format("Invalid collection type for PRIMARY KEY component %s", t));

            columns.remove(t);
            Boolean isReversed = definedOrdering.get(t);
            return isReversed != null && isReversed ? ReversedType.getInstance(type) : type;
        }

        public void addDefinition(ColumnIdentifier def, CQL3Type.Raw type, boolean isStatic)
        {
            definedNames.add(def);
            definitions.put(def, type);
            if (isStatic)
                staticColumns.add(def);
        }

        public void addKeyAliases(List<ColumnIdentifier> aliases)
        {
            keyAliases.add(aliases);
        }

        public void addColumnAlias(ColumnIdentifier alias)
        {
            columnAliases.add(alias);
        }

        public void setOrdering(ColumnIdentifier alias, boolean reversed)
        {
            definedOrdering.put(alias, reversed);
        }

        public void setCompactStorage()
        {
            useCompactStorage = true;
        }
    }
}
