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
package org.apache.cassandra.config;

import java.nio.ByteBuffer;
import java.util.*;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.collect.Lists;

import org.apache.cassandra.cql3.*;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.exceptions.*;

public class ColumnDefinition extends ColumnSpecification
{
    /*
     * The type of CQL3 column this definition represents.
     * There is 3 main type of CQL3 columns: those parts of the partition key,
     * those parts of the clustering key and the other, regular ones.
     * But when COMPACT STORAGE is used, there is by design only one regular
     * column, whose name is not stored in the data contrarily to the column of
     * type REGULAR. Hence the COMPACT_VALUE type to distinguish it below.
     *
     * Note that thrift only knows about definitions of type REGULAR (and
     * the ones whose componentIndex == null).
     */
    public enum Kind
    {
        PARTITION_KEY,
        CLUSTERING_COLUMN,
        REGULAR,
        STATIC,
        COMPACT_VALUE
    }

    public final Kind kind;

    private String indexName;
    private IndexType indexType;
    private Map<String,String> indexOptions;

    /*
     * If the column comparator is a composite type, indicates to which
     * component this definition refers to. If null, the definition refers to
     * the full column name.
     */
    private final Integer componentIndex;

    public static ColumnDefinition partitionKeyDef(CFMetaData cfm, ByteBuffer name, AbstractType<?> validator, Integer componentIndex)
    {
        return new ColumnDefinition(cfm, name, validator, componentIndex, Kind.PARTITION_KEY);
    }

    public static ColumnDefinition partitionKeyDef(String ksName, String cfName, ByteBuffer name, AbstractType<?> validator, Integer componentIndex)
    {
        return new ColumnDefinition(ksName, cfName, new ColumnIdentifier(name, UTF8Type.instance), validator, null, null, null, componentIndex, Kind.PARTITION_KEY);
    }

    public static ColumnDefinition clusteringKeyDef(CFMetaData cfm, ByteBuffer name, AbstractType<?> validator, Integer componentIndex)
    {
        return new ColumnDefinition(cfm, name, validator, componentIndex, Kind.CLUSTERING_COLUMN);
    }

    public static ColumnDefinition regularDef(CFMetaData cfm, ByteBuffer name, AbstractType<?> validator, Integer componentIndex)
    {
        return new ColumnDefinition(cfm, name, validator, componentIndex, Kind.REGULAR);
    }

    public static ColumnDefinition staticDef(CFMetaData cfm, ByteBuffer name, AbstractType<?> validator, Integer componentIndex)
    {
        return new ColumnDefinition(cfm, name, validator, componentIndex, Kind.STATIC);
    }

    public static ColumnDefinition compactValueDef(CFMetaData cfm, ByteBuffer name, AbstractType<?> validator)
    {
        return new ColumnDefinition(cfm, name, validator, null, Kind.COMPACT_VALUE);
    }

    public ColumnDefinition(CFMetaData cfm, ByteBuffer name, AbstractType<?> validator, Integer componentIndex, Kind kind)
    {
        this(cfm.ksName,
             cfm.cfName,
             new ColumnIdentifier(name, cfm.getComponentComparator(componentIndex, kind)),
             validator,
             null,
             null,
             null,
             componentIndex,
             kind);
    }

    @VisibleForTesting
    public ColumnDefinition(String ksName,
                            String cfName,
                            ColumnIdentifier name,
                            AbstractType<?> validator,
                            IndexType indexType,
                            Map<String, String> indexOptions,
                            String indexName,
                            Integer componentIndex,
                            Kind kind)
    {
        super(ksName, cfName, name, validator);
        assert name != null && validator != null;
        this.kind = kind;
        this.indexName = indexName;
        this.componentIndex = componentIndex;
        this.setIndexType(indexType, indexOptions);
    }

    public ColumnDefinition copy()
    {
        return new ColumnDefinition(ksName, cfName, name, type, indexType, indexOptions, indexName, componentIndex, kind);
    }

    public ColumnDefinition withNewName(ColumnIdentifier newName)
    {
        return new ColumnDefinition(ksName, cfName, newName, type, indexType, indexOptions, indexName, componentIndex, kind);
    }

    public ColumnDefinition withNewType(AbstractType<?> newType)
    {
        return new ColumnDefinition(ksName, cfName, name, newType, indexType, indexOptions, indexName, componentIndex, kind);
    }

    public boolean isOnAllComponents()
    {
        return componentIndex == null;
    }

    public boolean isPartitionKey()
    {
        return kind == Kind.PARTITION_KEY;
    }

    public boolean isClusteringColumn()
    {
        return kind == Kind.CLUSTERING_COLUMN;
    }

    public boolean isStatic()
    {
        return kind == Kind.STATIC;
    }

    public boolean isRegular()
    {
        return kind == Kind.REGULAR;
    }

    public boolean isCompactValue()
    {
        return kind == Kind.COMPACT_VALUE;
    }

    // The componentIndex. This never return null however for convenience sake:
    // if componentIndex == null, this return 0. So caller should first check
    // isOnAllComponents() to distinguish if that's a possibility.
    public int position()
    {
        return componentIndex == null ? 0 : componentIndex;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;

        if (!(o instanceof ColumnDefinition))
            return false;

        ColumnDefinition cd = (ColumnDefinition) o;

        return Objects.equal(ksName, cd.ksName)
            && Objects.equal(cfName, cd.cfName)
            && Objects.equal(name, cd.name)
            && Objects.equal(type, cd.type)
            && Objects.equal(kind, cd.kind)
            && Objects.equal(componentIndex, cd.componentIndex)
            && Objects.equal(indexName, cd.indexName)
            && Objects.equal(indexType, cd.indexType)
            && Objects.equal(indexOptions, cd.indexOptions);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(ksName, cfName, name, type, kind, componentIndex, indexName, indexType, indexOptions);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                      .add("name", name)
                      .add("type", type)
                      .add("kind", kind)
                      .add("componentIndex", componentIndex)
                      .add("indexName", indexName)
                      .add("indexType", indexType)
                      .toString();
    }

    public boolean isThriftCompatible()
    {
        return kind == ColumnDefinition.Kind.REGULAR && componentIndex == null;
    }

    public boolean isPrimaryKeyColumn()
    {
        return kind == Kind.PARTITION_KEY || kind == Kind.CLUSTERING_COLUMN;
    }

    /**
     * Whether the name of this definition is serialized in the cell nane, i.e. whether
     * it's not just a non-stored CQL metadata.
     */
    public boolean isPartOfCellName()
    {
        return kind == Kind.REGULAR || kind == Kind.STATIC;
    }

    public ColumnDefinition apply(ColumnDefinition def)  throws ConfigurationException
    {
        assert kind == def.kind && Objects.equal(componentIndex, def.componentIndex);

        if (getIndexType() != null && def.getIndexType() != null)
        {
            // If an index is set (and not drop by this update), the validator shouldn't be change to a non-compatible one
            // (and we want true comparator compatibility, not just value one, since the validator is used by LocalPartitioner to order index rows)
            if (!def.type.isCompatibleWith(type))
                throw new ConfigurationException(String.format("Cannot modify validator to a non-order-compatible one for column %s since an index is set", name));

            assert getIndexName() != null;
            if (!getIndexName().equals(def.getIndexName()))
                throw new ConfigurationException("Cannot modify index name");
        }

        return new ColumnDefinition(ksName,
                                    cfName,
                                    name,
                                    def.type,
                                    def.getIndexType(),
                                    def.getIndexOptions(),
                                    def.getIndexName(),
                                    componentIndex,
                                    kind);
    }

    public String getIndexName()
    {
        return indexName;
    }

    public ColumnDefinition setIndexName(String indexName)
    {
        this.indexName = indexName;
        return this;
    }

    public ColumnDefinition setIndexType(IndexType indexType, Map<String,String> indexOptions)
    {
        this.indexType = indexType;
        this.indexOptions = indexOptions;
        return this;
    }

    public ColumnDefinition setIndex(String indexName, IndexType indexType, Map<String,String> indexOptions)
    {
        return setIndexName(indexName).setIndexType(indexType, indexOptions);
    }

    public boolean isIndexed()
    {
        return indexType != null;
    }

    public IndexType getIndexType()
    {
        return indexType;
    }

    public Map<String,String> getIndexOptions()
    {
        return indexOptions;
    }

    /**
     * Checks if the index option with the specified name has been specified.
     *
     * @param name index option name
     * @return <code>true</code> if the index option with the specified name has been specified, <code>false</code>
     * otherwise.
     */
    public boolean hasIndexOption(String name)
    {
        return indexOptions.containsKey(name);
    }

    /**
     * Converts the specified column definitions into column identifiers.
     *
     * @param definitions the column definitions to convert.
     * @return the column identifiers corresponding to the specified definitions
     */
    public static List<ColumnIdentifier> toIdentifiers(List<ColumnDefinition> definitions)
    {
        return Lists.transform(definitions, new Function<ColumnDefinition, ColumnIdentifier>()
        {
            @Override
            public ColumnIdentifier apply(ColumnDefinition columnDef)
            {
                return columnDef.name;
            }
        });
    }
}
