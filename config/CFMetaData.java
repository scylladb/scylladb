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

import java.io.DataInput;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.base.Strings;
import com.google.common.collect.*;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cache.CachingOptions;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.CFStatement;
import org.apache.cassandra.cql3.statements.CreateTableStatement;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.compaction.*;
import org.apache.cassandra.db.composites.*;
import org.apache.cassandra.db.index.SecondaryIndex;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.io.compress.CompressionParameters;
import org.apache.cassandra.io.compress.LZ4Compressor;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.schema.LegacySchemaTables;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;
import org.github.jamm.Unmetered;

/**
 * This class can be tricky to modify. Please read http://wiki.apache.org/cassandra/ConfigurationNotes for how to do so safely.
 */
@Unmetered
public final class CFMetaData
{
    private static final Logger logger = LoggerFactory.getLogger(CFMetaData.class);

    public final static double DEFAULT_READ_REPAIR_CHANCE = 0.0;
    public final static double DEFAULT_DCLOCAL_READ_REPAIR_CHANCE = 0.1;
    public final static int DEFAULT_GC_GRACE_SECONDS = 864000;
    public final static int DEFAULT_MIN_COMPACTION_THRESHOLD = 4;
    public final static int DEFAULT_MAX_COMPACTION_THRESHOLD = 32;
    public final static Class<? extends AbstractCompactionStrategy> DEFAULT_COMPACTION_STRATEGY_CLASS = SizeTieredCompactionStrategy.class;
    public final static CachingOptions DEFAULT_CACHING_STRATEGY = CachingOptions.KEYS_ONLY;
    public final static int DEFAULT_DEFAULT_TIME_TO_LIVE = 0;
    public final static SpeculativeRetry DEFAULT_SPECULATIVE_RETRY = new SpeculativeRetry(SpeculativeRetry.RetryType.PERCENTILE, 0.99);
    public final static int DEFAULT_MIN_INDEX_INTERVAL = 128;
    public final static int DEFAULT_MAX_INDEX_INTERVAL = 2048;

    // Note that this is the default only for user created tables
    public final static String DEFAULT_COMPRESSOR = LZ4Compressor.class.getCanonicalName();

    // Note that this need to come *before* any CFMetaData is defined so before the compile below.
    private static final Comparator<ColumnDefinition> regularColumnComparator = new Comparator<ColumnDefinition>()
    {
        public int compare(ColumnDefinition def1, ColumnDefinition def2)
        {
            return ByteBufferUtil.compareUnsigned(def1.name.bytes, def2.name.bytes);
        }
    };

    public static class SpeculativeRetry
    {
        public enum RetryType
        {
            NONE, CUSTOM, PERCENTILE, ALWAYS
        }

        public final RetryType type;
        public final double value;

        private SpeculativeRetry(RetryType type, double value)
        {
            this.type = type;
            this.value = value;
        }

        public static SpeculativeRetry fromString(String retry) throws ConfigurationException
        {
            String name = retry.toUpperCase();
            try
            {
                if (name.endsWith(RetryType.PERCENTILE.toString()))
                {
                    double value = Double.parseDouble(name.substring(0, name.length() - 10));
                    if (value > 100 || value < 0)
                        throw new ConfigurationException("PERCENTILE should be between 0 and 100");
                    return new SpeculativeRetry(RetryType.PERCENTILE, (value / 100));
                }
                else if (name.endsWith("MS"))
                {
                    double value = Double.parseDouble(name.substring(0, name.length() - 2));
                    return new SpeculativeRetry(RetryType.CUSTOM, value);
                }
                else
                {
                    return new SpeculativeRetry(RetryType.valueOf(name), 0);
                }
            }
            catch (IllegalArgumentException e)
            {
                // ignore to throw the below exception.
            }
            throw new ConfigurationException("invalid speculative_retry type: " + retry);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (!(obj instanceof SpeculativeRetry))
                return false;
            SpeculativeRetry rhs = (SpeculativeRetry) obj;
            return Objects.equal(type, rhs.type) && Objects.equal(value, rhs.value);
        }

        @Override
        public int hashCode()
        {
            return Objects.hashCode(type, value);
        }

        @Override
        public String toString()
        {
            switch (type)
            {
            case PERCENTILE:
                // TODO switch to BigDecimal so round-tripping isn't lossy
                return (value * 100) + "PERCENTILE";
            case CUSTOM:
                return value + "ms";
            default:
                return type.toString();
            }
        }
    }

    //REQUIRED
    public final UUID cfId;                           // internal id, never exposed to user
    public final String ksName;                       // name of keyspace
    public final String cfName;                       // name of this column family
    public final ColumnFamilyType cfType;             // standard, super
    public volatile CellNameType comparator;          // bytes, long, timeuuid, utf8, etc.

    //OPTIONAL
    private volatile String comment = "";
    private volatile double readRepairChance = DEFAULT_READ_REPAIR_CHANCE;
    private volatile double dcLocalReadRepairChance = DEFAULT_DCLOCAL_READ_REPAIR_CHANCE;
    private volatile int gcGraceSeconds = DEFAULT_GC_GRACE_SECONDS;
    private volatile AbstractType<?> defaultValidator = BytesType.instance;
    private volatile AbstractType<?> keyValidator = BytesType.instance;
    private volatile int minCompactionThreshold = DEFAULT_MIN_COMPACTION_THRESHOLD;
    private volatile int maxCompactionThreshold = DEFAULT_MAX_COMPACTION_THRESHOLD;
    private volatile Double bloomFilterFpChance = null;
    private volatile CachingOptions caching = DEFAULT_CACHING_STRATEGY;
    private volatile int minIndexInterval = DEFAULT_MIN_INDEX_INTERVAL;
    private volatile int maxIndexInterval = DEFAULT_MAX_INDEX_INTERVAL;
    private volatile int memtableFlushPeriod = 0;
    private volatile int defaultTimeToLive = DEFAULT_DEFAULT_TIME_TO_LIVE;
    private volatile SpeculativeRetry speculativeRetry = DEFAULT_SPECULATIVE_RETRY;
    private volatile Map<ColumnIdentifier, Long> droppedColumns = new HashMap<>();
    private volatile Map<String, TriggerDefinition> triggers = new HashMap<>();
    private volatile boolean isPurged = false;
    /*
     * All CQL3 columns definition are stored in the columnMetadata map.
     * On top of that, we keep separated collection of each kind of definition, to
     * 1) allow easy access to each kind and 2) for the partition key and
     * clustering key ones, those list are ordered by the "component index" of the
     * elements.
     */
    public static final String DEFAULT_KEY_ALIAS = "key";
    public static final String DEFAULT_COLUMN_ALIAS = "column";
    public static final String DEFAULT_VALUE_ALIAS = "value";

    // We call dense a CF for which each component of the comparator is a clustering column, i.e. no
    // component is used to store a regular column names. In other words, non-composite static "thrift"
    // and CQL3 CF are *not* dense.
    private volatile Boolean isDense; // null means "we don't know and need to infer from other data"

    private volatile Map<ByteBuffer, ColumnDefinition> columnMetadata = new HashMap<>();
    private volatile List<ColumnDefinition> partitionKeyColumns;  // Always of size keyValidator.componentsCount, null padded if necessary
    private volatile List<ColumnDefinition> clusteringColumns;    // Of size comparator.componentsCount or comparator.componentsCount -1, null padded if necessary
    private volatile SortedSet<ColumnDefinition> regularColumns;  // We use a sorted set so iteration is of predictable order (for SELECT for instance)
    private volatile SortedSet<ColumnDefinition> staticColumns;   // Same as above
    private volatile ColumnDefinition compactValueColumn;

    public volatile Class<? extends AbstractCompactionStrategy> compactionStrategyClass = DEFAULT_COMPACTION_STRATEGY_CLASS;
    public volatile Map<String, String> compactionStrategyOptions = new HashMap<>();

    public volatile CompressionParameters compressionParameters = new CompressionParameters(null);

    // attribute setters that return the modified CFMetaData instance
    public CFMetaData comment(String prop) {comment = Strings.nullToEmpty(prop); return this;}
    public CFMetaData readRepairChance(double prop) {readRepairChance = prop; return this;}
    public CFMetaData dcLocalReadRepairChance(double prop) {dcLocalReadRepairChance = prop; return this;}
    public CFMetaData gcGraceSeconds(int prop) {gcGraceSeconds = prop; return this;}
    public CFMetaData defaultValidator(AbstractType<?> prop) {defaultValidator = prop; return this;}
    public CFMetaData keyValidator(AbstractType<?> prop) {keyValidator = prop; return this;}
    public CFMetaData minCompactionThreshold(int prop) {minCompactionThreshold = prop; return this;}
    public CFMetaData maxCompactionThreshold(int prop) {maxCompactionThreshold = prop; return this;}
    public CFMetaData compactionStrategyClass(Class<? extends AbstractCompactionStrategy> prop) {compactionStrategyClass = prop; return this;}
    public CFMetaData compactionStrategyOptions(Map<String, String> prop) {compactionStrategyOptions = prop; return this;}
    public CFMetaData compressionParameters(CompressionParameters prop) {compressionParameters = prop; return this;}
    public CFMetaData bloomFilterFpChance(double prop) {bloomFilterFpChance = prop; return this;}
    public CFMetaData caching(CachingOptions prop) {caching = prop; return this;}
    public CFMetaData minIndexInterval(int prop) {minIndexInterval = prop; return this;}
    public CFMetaData maxIndexInterval(int prop) {maxIndexInterval = prop; return this;}
    public CFMetaData memtableFlushPeriod(int prop) {memtableFlushPeriod = prop; return this;}
    public CFMetaData defaultTimeToLive(int prop) {defaultTimeToLive = prop; return this;}
    public CFMetaData speculativeRetry(SpeculativeRetry prop) {speculativeRetry = prop; return this;}
    public CFMetaData droppedColumns(Map<ColumnIdentifier, Long> cols) {droppedColumns = cols; return this;}
    public CFMetaData triggers(Map<String, TriggerDefinition> prop) {triggers = prop; return this;}
    public CFMetaData isDense(Boolean prop) {isDense = prop; return this;}

    /**
     * Create new ColumnFamily metadata with generated random ID.
     * When loading from existing schema, use CFMetaData
     *
     * @param keyspace keyspace name
     * @param name column family name
     * @param comp default comparator
     */
    public CFMetaData(String keyspace, String name, ColumnFamilyType type, CellNameType comp)
    {
        this(keyspace, name, type, comp, UUIDGen.getTimeUUID());
    }

    public CFMetaData(String keyspace, String name, ColumnFamilyType type, CellNameType comp, UUID id)
    {
        cfId = id;
        ksName = keyspace;
        cfName = name;
        cfType = type;
        comparator = comp;
    }

    public static CFMetaData denseCFMetaData(String keyspace, String name, AbstractType<?> comp, AbstractType<?> subcc)
    {
        CellNameType cellNameType = CellNames.fromAbstractType(makeRawAbstractType(comp, subcc), true);
        return new CFMetaData(keyspace, name, subcc == null ? ColumnFamilyType.Standard : ColumnFamilyType.Super, cellNameType);
    }

    public static CFMetaData sparseCFMetaData(String keyspace, String name, AbstractType<?> comp)
    {
        CellNameType cellNameType = CellNames.fromAbstractType(comp, false);
        return new CFMetaData(keyspace, name, ColumnFamilyType.Standard, cellNameType);
    }

    public static CFMetaData denseCFMetaData(String keyspace, String name, AbstractType<?> comp)
    {
        return denseCFMetaData(keyspace, name, comp, null);
    }

    public static AbstractType<?> makeRawAbstractType(AbstractType<?> comparator, AbstractType<?> subComparator)
    {
        return subComparator == null ? comparator : CompositeType.getInstance(Arrays.asList(comparator, subComparator));
    }

    public Map<String, TriggerDefinition> getTriggers()
    {
        return triggers;
    }

    public static CFMetaData compile(String cql, String keyspace)
    {
        try
        {
            CFStatement parsed = (CFStatement)QueryProcessor.parseStatement(cql);
            parsed.prepareKeyspace(keyspace);
            CreateTableStatement statement = (CreateTableStatement) parsed.prepare().statement;
            CFMetaData cfm = newSystemMetadata(keyspace, statement.columnFamily(), "", statement.comparator);
            statement.applyPropertiesTo(cfm);
            return cfm.rebuild();
        }
        catch (RequestValidationException e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * Generates deterministic UUID from keyspace/columnfamily name pair.
     * This is used to generate the same UUID for C* version < 2.1
     *
     * Since 2.1, this is only used for system columnfamilies and tests.
     */
    public static UUID generateLegacyCfId(String ksName, String cfName)
    {
        return UUID.nameUUIDFromBytes(ArrayUtils.addAll(ksName.getBytes(), cfName.getBytes()));
    }

    private static CFMetaData newSystemMetadata(String keyspace, String cfName, String comment, CellNameType comparator)
    {
        return new CFMetaData(keyspace, cfName, ColumnFamilyType.Standard, comparator, generateLegacyCfId(keyspace, cfName))
                             .comment(comment)
                             .readRepairChance(0)
                             .dcLocalReadRepairChance(0)
                             .gcGraceSeconds(0)
                             .memtableFlushPeriod(3600 * 1000);
    }

    /**
     * Creates CFMetaData for secondary index CF.
     * Secondary index CF has the same CF ID as parent's.
     *
     * @param parent Parent CF where secondary index is created
     * @param info Column definition containing secondary index definition
     * @param indexComparator Comparator for secondary index
     * @return CFMetaData for secondary index
     */
    public static CFMetaData newIndexMetadata(CFMetaData parent, ColumnDefinition info, CellNameType indexComparator)
    {
        // Depends on parent's cache setting, turn on its index CF's cache.
        // Row caching is never enabled; see CASSANDRA-5732
        CachingOptions indexCaching = parent.getCaching().keyCache.isEnabled()
                                    ? CachingOptions.KEYS_ONLY
                                    : CachingOptions.NONE;

        return new CFMetaData(parent.ksName, parent.indexColumnFamilyName(info), ColumnFamilyType.Standard, indexComparator, parent.cfId)
                             .keyValidator(info.type)
                             .readRepairChance(0.0)
                             .dcLocalReadRepairChance(0.0)
                             .gcGraceSeconds(0)
                             .caching(indexCaching)
                             .speculativeRetry(parent.speculativeRetry)
                             .compactionStrategyClass(parent.compactionStrategyClass)
                             .compactionStrategyOptions(parent.compactionStrategyOptions)
                             .reloadSecondaryIndexMetadata(parent)
                             .rebuild();
    }

    public CFMetaData reloadSecondaryIndexMetadata(CFMetaData parent)
    {
        minCompactionThreshold(parent.minCompactionThreshold);
        maxCompactionThreshold(parent.maxCompactionThreshold);
        compactionStrategyClass(parent.compactionStrategyClass);
        compactionStrategyOptions(parent.compactionStrategyOptions);
        compressionParameters(parent.compressionParameters);
        return this;
    }

    public CFMetaData copy()
    {
        return copyOpts(new CFMetaData(ksName, cfName, cfType, comparator, cfId), this);
    }

    /**
     * Clones the CFMetaData, but sets a different cfId
     *
     * @param newCfId the cfId for the cloned CFMetaData
     * @return the cloned CFMetaData instance with the new cfId
     */
    public CFMetaData copy(UUID newCfId)
    {
        return copyOpts(new CFMetaData(ksName, cfName, cfType, comparator, newCfId), this);
    }

    @VisibleForTesting
    public static CFMetaData copyOpts(CFMetaData newCFMD, CFMetaData oldCFMD)
    {
        List<ColumnDefinition> clonedColumns = new ArrayList<>(oldCFMD.allColumns().size());
        for (ColumnDefinition cd : oldCFMD.allColumns())
            clonedColumns.add(cd.copy());

        return newCFMD.addAllColumnDefinitions(clonedColumns)
                      .comment(oldCFMD.comment)
                      .readRepairChance(oldCFMD.readRepairChance)
                      .dcLocalReadRepairChance(oldCFMD.dcLocalReadRepairChance)
                      .gcGraceSeconds(oldCFMD.gcGraceSeconds)
                      .defaultValidator(oldCFMD.defaultValidator)
                      .keyValidator(oldCFMD.keyValidator)
                      .minCompactionThreshold(oldCFMD.minCompactionThreshold)
                      .maxCompactionThreshold(oldCFMD.maxCompactionThreshold)
                      .compactionStrategyClass(oldCFMD.compactionStrategyClass)
                      .compactionStrategyOptions(new HashMap<>(oldCFMD.compactionStrategyOptions))
                      .compressionParameters(oldCFMD.compressionParameters.copy())
                      .bloomFilterFpChance(oldCFMD.getBloomFilterFpChance())
                      .caching(oldCFMD.caching)
                      .defaultTimeToLive(oldCFMD.defaultTimeToLive)
                      .minIndexInterval(oldCFMD.minIndexInterval)
                      .maxIndexInterval(oldCFMD.maxIndexInterval)
                      .speculativeRetry(oldCFMD.speculativeRetry)
                      .memtableFlushPeriod(oldCFMD.memtableFlushPeriod)
                      .droppedColumns(new HashMap<>(oldCFMD.droppedColumns))
                      .triggers(new HashMap<>(oldCFMD.triggers))
                      .isDense(oldCFMD.isDense)
                      .rebuild();
    }

    /**
     * generate a column family name for an index corresponding to the given column.
     * This is NOT the same as the index's name! This is only used in sstable filenames and is not exposed to users.
     *
     * @param info A definition of the column with index
     *
     * @return name of the index ColumnFamily
     */
    public String indexColumnFamilyName(ColumnDefinition info)
    {
        // TODO simplify this when info.index_name is guaranteed to be set
        return cfName + Directories.SECONDARY_INDEX_NAME_SEPARATOR + (info.getIndexName() == null ? ByteBufferUtil.bytesToHex(info.name.bytes) : info.getIndexName());
    }

    public String getComment()
    {
        return comment;
    }

    public boolean isSuper()
    {
        return cfType == ColumnFamilyType.Super;
    }

    /**
     * The '.' char is the only way to identify if the CFMetadata is for a secondary index
     */
    public boolean isSecondaryIndex()
    {
        return cfName.contains(".");
    }

    public Map<ByteBuffer, ColumnDefinition> getColumnMetadata()
    {
        return columnMetadata;
    }

    /**
     *
     * @return The name of the parent cf if this is a seconday index
     */
    public String getParentColumnFamilyName()
    {
        return isSecondaryIndex() ? cfName.substring(0, cfName.indexOf('.')) : null;
    }

    public double getReadRepairChance()
    {
        return readRepairChance;
    }

    public double getDcLocalReadRepairChance()
    {
        return dcLocalReadRepairChance;
    }

    public ReadRepairDecision newReadRepairDecision()
    {
        double chance = ThreadLocalRandom.current().nextDouble();
        if (getReadRepairChance() > chance)
            return ReadRepairDecision.GLOBAL;

        if (getDcLocalReadRepairChance() > chance)
            return ReadRepairDecision.DC_LOCAL;

        return ReadRepairDecision.NONE;
    }

    public int getGcGraceSeconds()
    {
        return gcGraceSeconds;
    }

    public AbstractType<?> getDefaultValidator()
    {
        return defaultValidator;
    }

    public AbstractType<?> getKeyValidator()
    {
        return keyValidator;
    }

    public Integer getMinCompactionThreshold()
    {
        return minCompactionThreshold;
    }

    public Integer getMaxCompactionThreshold()
    {
        return maxCompactionThreshold;
    }

    public CompressionParameters compressionParameters()
    {
        return compressionParameters;
    }

    public Collection<ColumnDefinition> allColumns()
    {
        return columnMetadata.values();
    }

    // An iterator over all column definitions but that respect the order of a SELECT *.
    public Iterator<ColumnDefinition> allColumnsInSelectOrder()
    {
        return new AbstractIterator<ColumnDefinition>()
        {
            private final Iterator<ColumnDefinition> partitionKeyIter = partitionKeyColumns.iterator();
            private final Iterator<ColumnDefinition> clusteringIter = clusteringColumns.iterator();
            private boolean valueDone;
            private final Iterator<ColumnDefinition> staticIter = staticColumns.iterator();
            private final Iterator<ColumnDefinition> regularIter = regularColumns.iterator();

            protected ColumnDefinition computeNext()
            {
                if (partitionKeyIter.hasNext())
                    return partitionKeyIter.next();

                if (clusteringIter.hasNext())
                    return clusteringIter.next();

                if (staticIter.hasNext())
                    return staticIter.next();

                if (compactValueColumn != null && !valueDone)
                {
                    valueDone = true;
                    // If the compactValueColumn is empty, this means we have a dense table but
                    // with only a PK. As far as selects are concerned, we should ignore the value.
                    if (compactValueColumn.name.bytes.hasRemaining())
                        return compactValueColumn;
                }

                if (regularIter.hasNext())
                    return regularIter.next();

                return endOfData();
            }
        };
    }

    public List<ColumnDefinition> partitionKeyColumns()
    {
        return partitionKeyColumns;
    }

    public List<ColumnDefinition> clusteringColumns()
    {
        return clusteringColumns;
    }

    public Set<ColumnDefinition> regularColumns()
    {
        return regularColumns;
    }

    public Set<ColumnDefinition> staticColumns()
    {
        return staticColumns;
    }

    public Iterable<ColumnDefinition> regularAndStaticColumns()
    {
        return Iterables.concat(staticColumns, regularColumns);
    }

    public ColumnDefinition compactValueColumn()
    {
        return compactValueColumn;
    }

    // TODO: we could use CType for key validation too to make this unnecessary but
    // it's unclear it would be a win overall
    public CType getKeyValidatorAsCType()
    {
        return keyValidator instanceof CompositeType
             ? new CompoundCType(((CompositeType) keyValidator).types)
             : new SimpleCType(keyValidator);
    }

    public double getBloomFilterFpChance()
    {
        // we disallow bFFPC==null starting in 1.2.1 but tolerated it before that
        return (bloomFilterFpChance == null || bloomFilterFpChance == 0)
               ? compactionStrategyClass == LeveledCompactionStrategy.class ? 0.1 : 0.01
               : bloomFilterFpChance;
    }

    public CachingOptions getCaching()
    {
        return caching;
    }

    public int getMinIndexInterval()
    {
        return minIndexInterval;
    }

    public int getMaxIndexInterval()
    {
        return maxIndexInterval;
    }

    public SpeculativeRetry getSpeculativeRetry()
    {
        return speculativeRetry;
    }

    public int getMemtableFlushPeriod()
    {
        return memtableFlushPeriod;
    }

    public int getDefaultTimeToLive()
    {
        return defaultTimeToLive;
    }

    public Map<ColumnIdentifier, Long> getDroppedColumns()
    {
        return droppedColumns;
    }

    public Boolean getIsDense()
    {
        return isDense;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;

        if (!(o instanceof CFMetaData))
            return false;

        CFMetaData other = (CFMetaData) o;

        return Objects.equal(cfId, other.cfId)
            && Objects.equal(ksName, other.ksName)
            && Objects.equal(cfName, other.cfName)
            && Objects.equal(cfType, other.cfType)
            && Objects.equal(comparator, other.comparator)
            && Objects.equal(comment, other.comment)
            && Objects.equal(readRepairChance, other.readRepairChance)
            && Objects.equal(dcLocalReadRepairChance, other.dcLocalReadRepairChance)
            && Objects.equal(gcGraceSeconds, other.gcGraceSeconds)
            && Objects.equal(defaultValidator, other.defaultValidator)
            && Objects.equal(keyValidator, other.keyValidator)
            && Objects.equal(minCompactionThreshold, other.minCompactionThreshold)
            && Objects.equal(maxCompactionThreshold, other.maxCompactionThreshold)
            && Objects.equal(columnMetadata, other.columnMetadata)
            && Objects.equal(compactionStrategyClass, other.compactionStrategyClass)
            && Objects.equal(compactionStrategyOptions, other.compactionStrategyOptions)
            && Objects.equal(compressionParameters, other.compressionParameters)
            && Objects.equal(getBloomFilterFpChance(), other.getBloomFilterFpChance())
            && Objects.equal(memtableFlushPeriod, other.memtableFlushPeriod)
            && Objects.equal(caching, other.caching)
            && Objects.equal(defaultTimeToLive, other.defaultTimeToLive)
            && Objects.equal(minIndexInterval, other.minIndexInterval)
            && Objects.equal(maxIndexInterval, other.maxIndexInterval)
            && Objects.equal(speculativeRetry, other.speculativeRetry)
            && Objects.equal(droppedColumns, other.droppedColumns)
            && Objects.equal(triggers, other.triggers)
            && Objects.equal(isDense, other.isDense);
    }

    @Override
    public int hashCode()
    {
        return new HashCodeBuilder(29, 1597)
            .append(cfId)
            .append(ksName)
            .append(cfName)
            .append(cfType)
            .append(comparator)
            .append(comment)
            .append(readRepairChance)
            .append(dcLocalReadRepairChance)
            .append(gcGraceSeconds)
            .append(defaultValidator)
            .append(keyValidator)
            .append(minCompactionThreshold)
            .append(maxCompactionThreshold)
            .append(columnMetadata)
            .append(compactionStrategyClass)
            .append(compactionStrategyOptions)
            .append(compressionParameters)
            .append(getBloomFilterFpChance())
            .append(memtableFlushPeriod)
            .append(caching)
            .append(defaultTimeToLive)
            .append(minIndexInterval)
            .append(maxIndexInterval)
            .append(speculativeRetry)
            .append(droppedColumns)
            .append(triggers)
            .append(isDense)
            .toHashCode();
    }

    public AbstractType<?> getValueValidator(CellName cellName)
    {
        ColumnDefinition def = getColumnDefinition(cellName);
        return def == null ? defaultValidator : def.type;
    }

    public void reload()
    {
        try
        {
            apply(LegacySchemaTables.createTableFromName(ksName, cfName));
        }
        catch (ConfigurationException e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * Updates CFMetaData in-place to match cfm
     *
     * @throws ConfigurationException if ks/cf names or cf ids didn't match
     */
    @VisibleForTesting
    public void apply(CFMetaData cfm) throws ConfigurationException
    {
        logger.debug("applying {} to {}", cfm, this);

        validateCompatility(cfm);

        // TODO: this method should probably return a new CFMetaData so that
        // 1) we can keep comparator final
        // 2) updates are applied atomically
        comparator = cfm.comparator;

        // compaction thresholds are checked by ThriftValidation. We shouldn't be doing
        // validation on the apply path; it's too late for that.

        comment = Strings.nullToEmpty(cfm.comment);
        readRepairChance = cfm.readRepairChance;
        dcLocalReadRepairChance = cfm.dcLocalReadRepairChance;
        gcGraceSeconds = cfm.gcGraceSeconds;
        defaultValidator = cfm.defaultValidator;
        keyValidator = cfm.keyValidator;
        minCompactionThreshold = cfm.minCompactionThreshold;
        maxCompactionThreshold = cfm.maxCompactionThreshold;

        bloomFilterFpChance = cfm.getBloomFilterFpChance();
        caching = cfm.caching;
        minIndexInterval = cfm.minIndexInterval;
        maxIndexInterval = cfm.maxIndexInterval;
        memtableFlushPeriod = cfm.memtableFlushPeriod;
        defaultTimeToLive = cfm.defaultTimeToLive;
        speculativeRetry = cfm.speculativeRetry;

        if (!cfm.droppedColumns.isEmpty())
            droppedColumns = cfm.droppedColumns;

        MapDifference<ByteBuffer, ColumnDefinition> columnDiff = Maps.difference(columnMetadata, cfm.columnMetadata);
        // columns that are no longer needed
        for (ColumnDefinition cd : columnDiff.entriesOnlyOnLeft().values())
            removeColumnDefinition(cd);
        // newly added columns
        for (ColumnDefinition cd : columnDiff.entriesOnlyOnRight().values())
            addColumnDefinition(cd);
        // old columns with updated attributes
        for (ByteBuffer name : columnDiff.entriesDiffering().keySet())
        {
            ColumnDefinition oldDef = columnMetadata.get(name);
            ColumnDefinition def = cfm.columnMetadata.get(name);
            addOrReplaceColumnDefinition(oldDef.apply(def));
        }

        compactionStrategyClass = cfm.compactionStrategyClass;
        compactionStrategyOptions = cfm.compactionStrategyOptions;

        compressionParameters = cfm.compressionParameters;

        triggers = cfm.triggers;

        isDense(cfm.isDense);

        rebuild();
        logger.debug("application result is {}", this);
    }

    public void validateCompatility(CFMetaData cfm) throws ConfigurationException
    {
        // validate
        if (!cfm.ksName.equals(ksName))
            throw new ConfigurationException(String.format("Keyspace mismatch (found %s; expected %s)",
                                                           cfm.ksName, ksName));
        if (!cfm.cfName.equals(cfName))
            throw new ConfigurationException(String.format("Column family mismatch (found %s; expected %s)",
                                                           cfm.cfName, cfName));
        if (!cfm.cfId.equals(cfId))
            throw new ConfigurationException(String.format("Column family ID mismatch (found %s; expected %s)",
                                                           cfm.cfId, cfId));

        if (cfm.cfType != cfType)
            throw new ConfigurationException("types do not match.");

        if (!cfm.comparator.isCompatibleWith(comparator))
            throw new ConfigurationException("comparators do not match or are not compatible.");
    }

    public static void validateCompactionOptions(Class<? extends AbstractCompactionStrategy> strategyClass, Map<String, String> options) throws ConfigurationException
    {
        try
        {
            if (options == null)
                return;

            Map<?,?> unknownOptions = (Map) strategyClass.getMethod("validateOptions", Map.class).invoke(null, options);
            if (!unknownOptions.isEmpty())
                throw new ConfigurationException(String.format("Properties specified %s are not understood by %s", unknownOptions.keySet(), strategyClass.getSimpleName()));
        }
        catch (NoSuchMethodException e)
        {
            logger.warn("Compaction Strategy {} does not have a static validateOptions method. Validation ignored", strategyClass.getName());
        }
        catch (InvocationTargetException e)
        {
            if (e.getTargetException() instanceof ConfigurationException)
                throw (ConfigurationException) e.getTargetException();
            throw new ConfigurationException("Failed to validate compaction options");
        }
        catch (ConfigurationException e)
        {
            throw e;
        }
        catch (Exception e)
        {
            throw new ConfigurationException("Failed to validate compaction options");
        }
    }

    public static Class<? extends AbstractCompactionStrategy> createCompactionStrategy(String className) throws ConfigurationException
    {
        className = className.contains(".") ? className : "org.apache.cassandra.db.compaction." + className;
        Class<AbstractCompactionStrategy> strategyClass = FBUtilities.classForName(className, "compaction strategy");
        if (className.equals(WrappingCompactionStrategy.class.getName()))
            throw new ConfigurationException("You can't set WrappingCompactionStrategy as the compaction strategy!");
        if (!AbstractCompactionStrategy.class.isAssignableFrom(strategyClass))
            throw new ConfigurationException(String.format("Specified compaction strategy class (%s) is not derived from AbstractReplicationStrategy", className));

        return strategyClass;
    }

    public AbstractCompactionStrategy createCompactionStrategyInstance(ColumnFamilyStore cfs)
    {
        try
        {
            Constructor<? extends AbstractCompactionStrategy> constructor =
                compactionStrategyClass.getConstructor(ColumnFamilyStore.class, Map.class);
            return constructor.newInstance(cfs, compactionStrategyOptions);
        }
        catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException | InstantiationException e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * Returns the ColumnDefinition for {@code name}.
     */
    public ColumnDefinition getColumnDefinition(ColumnIdentifier name)
    {
        return columnMetadata.get(name.bytes);
    }

    // In general it is preferable to work with ColumnIdentifier to make it
    // clear that we are talking about a CQL column, not a cell name, but there
    // is a few cases where all we have is a ByteBuffer (when dealing with IndexExpression
    // for instance) so...
    public ColumnDefinition getColumnDefinition(ByteBuffer name)
    {
        return columnMetadata.get(name);
    }

    /**
     * Returns a ColumnDefinition given a cell name.
     */
    public ColumnDefinition getColumnDefinition(CellName cellName)
    {
        ColumnIdentifier id = cellName.cql3ColumnName(this);
        ColumnDefinition def = id == null
                             ? getColumnDefinition(cellName.toByteBuffer())  // Means a dense layout, try the full column name
                             : getColumnDefinition(id);

        // It's possible that the def is a PRIMARY KEY or COMPACT_VALUE one in case a concrete cell
        // name conflicts with a CQL column name, which can happen in 2 cases:
        // 1) because the user inserted a cell through Thrift that conflicts with a default "alias" used
        //    by CQL for thrift tables (see #6892).
        // 2) for COMPACT STORAGE tables with a single utf8 clustering column, the cell name can be anything,
        //    including a CQL column name (without this being a problem).
        // In any case, this is fine, this just mean that columnDefinition is not the ColumnDefinition we are
        // looking for.
        return def != null && def.isPartOfCellName() ? def : null;
    }

    public ColumnDefinition getColumnDefinitionForIndex(String indexName)
    {
        for (ColumnDefinition def : allColumns())
        {
            if (indexName.equals(def.getIndexName()))
                return def;
        }
        return null;
    }

    /**
     * Convert a null index_name to appropriate default name according to column status
     */
    public void addDefaultIndexNames() throws ConfigurationException
    {
        // if this is ColumnFamily update we need to add previously defined index names to the existing columns first
        UUID cfId = Schema.instance.getId(ksName, cfName);
        if (cfId != null)
        {
            CFMetaData cfm = Schema.instance.getCFMetaData(cfId);

            for (ColumnDefinition newDef : allColumns())
            {
                if (!cfm.columnMetadata.containsKey(newDef.name.bytes) || newDef.getIndexType() == null)
                    continue;

                String oldIndexName = cfm.getColumnDefinition(newDef.name).getIndexName();

                if (oldIndexName == null)
                    continue;

                if (newDef.getIndexName() != null && !oldIndexName.equals(newDef.getIndexName()))
                    throw new ConfigurationException("Can't modify index name: was '" + oldIndexName + "' changed to '" + newDef.getIndexName() + "'.");

                newDef.setIndexName(oldIndexName);
            }
        }

        Set<String> existingNames = existingIndexNames(null);
        for (ColumnDefinition column : allColumns())
        {
            if (column.getIndexType() != null && column.getIndexName() == null)
            {
                String baseName = getDefaultIndexName(cfName, column.name);
                String indexName = baseName;
                int i = 0;
                while (existingNames.contains(indexName))
                    indexName = baseName + '_' + (++i);
                column.setIndexName(indexName);
            }
        }
    }

    public static String getDefaultIndexName(String cfName, ColumnIdentifier columnName)
    {
        return (cfName + "_" + columnName + "_idx").replaceAll("\\W", "");
    }

    public Iterator<OnDiskAtom> getOnDiskIterator(FileDataInput in, Version version)
    {
        return getOnDiskIterator(in, ColumnSerializer.Flag.LOCAL, Integer.MIN_VALUE, version);
    }

    public Iterator<OnDiskAtom> getOnDiskIterator(FileDataInput in, ColumnSerializer.Flag flag, int expireBefore, Version version)
    {
        return version.getSSTableFormat().getOnDiskIterator(in, flag, expireBefore, this, version);
    }

    public AtomDeserializer getOnDiskDeserializer(DataInput in, Version version)
    {
        return new AtomDeserializer(comparator, in, ColumnSerializer.Flag.LOCAL, Integer.MIN_VALUE, version);
    }

    public static boolean isNameValid(String name)
    {
        return name != null && !name.isEmpty() && name.length() <= Schema.NAME_LENGTH && name.matches("\\w+");
    }

    public static boolean isIndexNameValid(String name)
    {
        return name != null && !name.isEmpty() && name.matches("\\w+");
    }

    public CFMetaData validate() throws ConfigurationException
    {
        rebuild();

        if (!isNameValid(ksName))
            throw new ConfigurationException(String.format("Keyspace name must not be empty, more than %s characters long, or contain non-alphanumeric-underscore characters (got \"%s\")", Schema.NAME_LENGTH, ksName));
        if (!isNameValid(cfName))
            throw new ConfigurationException(String.format("ColumnFamily name must not be empty, more than %s characters long, or contain non-alphanumeric-underscore characters (got \"%s\")", Schema.NAME_LENGTH, cfName));

        if (cfType == null)
            throw new ConfigurationException(String.format("Invalid column family type for %s", cfName));

        for (int i = 0; i < comparator.size(); i++)
        {
            if (comparator.subtype(i) instanceof CounterColumnType)
                throw new ConfigurationException("CounterColumnType is not a valid comparator");
        }
        if (keyValidator instanceof CounterColumnType)
            throw new ConfigurationException("CounterColumnType is not a valid key validator");

        // Mixing counter with non counter columns is not supported (#2614)
        if (defaultValidator instanceof CounterColumnType)
        {
            for (ColumnDefinition def : regularAndStaticColumns())
                if (!(def.type instanceof CounterColumnType))
                    throw new ConfigurationException("Cannot add a non counter column (" + def.name + ") in a counter column family");
        }
        else
        {
            for (ColumnDefinition def : allColumns())
                if (def.type instanceof CounterColumnType)
                    throw new ConfigurationException("Cannot add a counter column (" + def.name + ") in a non counter column family");
        }

        // initialize a set of names NOT in the CF under consideration
        Set<String> indexNames = existingIndexNames(cfName);
        for (ColumnDefinition c : allColumns())
        {
            if (c.getIndexType() == null)
            {
                if (c.getIndexName() != null)
                    throw new ConfigurationException("Index name cannot be set without index type");
            }
            else
            {
                if (cfType == ColumnFamilyType.Super)
                    throw new ConfigurationException("Secondary indexes are not supported on super column families");
                if (!isIndexNameValid(c.getIndexName()))
                    throw new ConfigurationException("Illegal index name " + c.getIndexName());
                // check index names against this CF _and_ globally
                if (indexNames.contains(c.getIndexName()))
                    throw new ConfigurationException("Duplicate index name " + c.getIndexName());
                indexNames.add(c.getIndexName());

                if (c.getIndexType() == IndexType.CUSTOM)
                {
                    if (c.getIndexOptions() == null || !c.hasIndexOption(SecondaryIndex.CUSTOM_INDEX_OPTION_NAME))
                        throw new ConfigurationException("Required index option missing: " + SecondaryIndex.CUSTOM_INDEX_OPTION_NAME);
                }

                // This method validates the column metadata but does not intialize the index
                SecondaryIndex.createInstance(null, c);
            }
        }

        validateCompactionThresholds();

        if (bloomFilterFpChance != null && bloomFilterFpChance == 0)
            throw new ConfigurationException("Zero false positives is impossible; bloom filter false positive chance bffpc must be 0 < bffpc <= 1");

        validateIndexIntervalThresholds();

        return this;
    }

    private static Set<String> existingIndexNames(String cfToExclude)
    {
        Set<String> indexNames = new HashSet<>();
        for (ColumnFamilyStore cfs : ColumnFamilyStore.all())
            if (cfToExclude == null || !cfs.name.equals(cfToExclude))
                for (ColumnDefinition cd : cfs.metadata.allColumns())
                    indexNames.add(cd.getIndexName());
        return indexNames;
    }

    private void validateCompactionThresholds() throws ConfigurationException
    {
        if (maxCompactionThreshold == 0)
        {
            logger.warn("Disabling compaction by setting max or min compaction has been deprecated, " +
                    "set the compaction strategy option 'enabled' to 'false' instead");
            return;
        }

        if (minCompactionThreshold <= 1)
            throw new ConfigurationException(String.format("Min compaction threshold cannot be less than 2 (got %d).", minCompactionThreshold));

        if (minCompactionThreshold > maxCompactionThreshold)
            throw new ConfigurationException(String.format("Min compaction threshold (got %d) cannot be greater than max compaction threshold (got %d)",
                                                            minCompactionThreshold, maxCompactionThreshold));
    }

    private void validateIndexIntervalThresholds() throws ConfigurationException
    {
        if (minIndexInterval <= 0)
            throw new ConfigurationException(String.format("Min index interval must be greater than 0 (got %d).", minIndexInterval));
        if (maxIndexInterval < minIndexInterval)
            throw new ConfigurationException(String.format("Max index interval (%d) must be greater than the min index " +
                                                           "interval (%d).", maxIndexInterval, minIndexInterval));
    }

    public boolean isPurged()
    {
        return isPurged;
    }

    void markPurged()
    {
        isPurged = true;
    }

    // The comparator to validate the definition name.

    public AbstractType<?> getColumnDefinitionComparator(ColumnDefinition def)
    {
        return getComponentComparator(def.isOnAllComponents() ? null : def.position(), def.kind);
    }

    public AbstractType<?> getComponentComparator(Integer componentIndex, ColumnDefinition.Kind kind)
    {
        switch (kind)
        {
            case REGULAR:
                if (componentIndex == null)
                    return comparator.asAbstractType();

                AbstractType<?> t = comparator.subtype(componentIndex);
                assert t != null : "Non-sensical component index";
                return t;
            default:
                // CQL3 column names are UTF8
                return UTF8Type.instance;
        }
    }

    public CFMetaData addAllColumnDefinitions(Collection<ColumnDefinition> defs)
    {
        for (ColumnDefinition def : defs)
            addOrReplaceColumnDefinition(def);
        return this;
    }

    public CFMetaData addColumnDefinition(ColumnDefinition def) throws ConfigurationException
    {
        if (columnMetadata.containsKey(def.name.bytes))
            throw new ConfigurationException(String.format("Cannot add column %s, a column with the same name already exists", def.name));

        return addOrReplaceColumnDefinition(def);
    }

    // This method doesn't check if a def of the same name already exist and should only be used when we
    // know this cannot happen.
    public CFMetaData addOrReplaceColumnDefinition(ColumnDefinition def)
    {
        if (def.kind == ColumnDefinition.Kind.REGULAR)
            comparator.addCQL3Column(def.name);
        columnMetadata.put(def.name.bytes, def);
        return this;
    }

    public boolean removeColumnDefinition(ColumnDefinition def)
    {
        if (def.kind == ColumnDefinition.Kind.REGULAR)
            comparator.removeCQL3Column(def.name);
        return columnMetadata.remove(def.name.bytes) != null;
    }

    public void addTriggerDefinition(TriggerDefinition def) throws InvalidRequestException
    {
        if (containsTriggerDefinition(def))
            throw new InvalidRequestException(
                String.format("Cannot create trigger %s, a trigger with the same name already exists", def.name));
        triggers.put(def.name, def);
    }

    public boolean containsTriggerDefinition(TriggerDefinition def)
    {
        return triggers.containsKey(def.name);
    }

    public boolean removeTrigger(String name)
    {
        return triggers.remove(name) != null;
    }

    public void recordColumnDrop(ColumnDefinition def)
    {
        assert !def.isOnAllComponents();
        droppedColumns.put(def.name, FBUtilities.timestampMicros());
    }

    public void renameColumn(ColumnIdentifier from, ColumnIdentifier to) throws InvalidRequestException
    {
        ColumnDefinition def = getColumnDefinition(from);
        if (def == null)
            throw new InvalidRequestException(String.format("Cannot rename unknown column %s in keyspace %s", from, cfName));

        if (getColumnDefinition(to) != null)
            throw new InvalidRequestException(String.format("Cannot rename column %s to %s in keyspace %s; another column of that name already exist", from, to, cfName));

        if (def.isPartOfCellName())
        {
            throw new InvalidRequestException(String.format("Cannot rename non PRIMARY KEY part %s", from));
        }
        else if (def.isIndexed())
        {
            throw new InvalidRequestException(String.format("Cannot rename column %s because it is secondary indexed", from));
        }

        ColumnDefinition newDef = def.withNewName(to);
        // don't call addColumnDefinition/removeColumnDefition because we want to avoid recomputing
        // the CQL3 cfDef between those two operation
        addOrReplaceColumnDefinition(newDef);
        removeColumnDefinition(def);
    }

    public CFMetaData rebuild()
    {
        if (isDense == null)
            isDense(calculateIsDense(comparator.asAbstractType(), allColumns()));

        List<ColumnDefinition> pkCols = nullInitializedList(keyValidator.componentsCount());
        List<ColumnDefinition> ckCols = nullInitializedList(comparator.clusteringPrefixSize());
        // We keep things sorted to get consistent/predictable order in select queries
        SortedSet<ColumnDefinition> regCols = new TreeSet<>(regularColumnComparator);
        SortedSet<ColumnDefinition> statCols = new TreeSet<>(regularColumnComparator);
        ColumnDefinition compactCol = null;

        for (ColumnDefinition def : allColumns())
        {
            switch (def.kind)
            {
                case PARTITION_KEY:
                    assert !(def.isOnAllComponents() && keyValidator instanceof CompositeType);
                    pkCols.set(def.position(), def);
                    break;
                case CLUSTERING_COLUMN:
                    assert !(def.isOnAllComponents() && comparator.isCompound());
                    ckCols.set(def.position(), def);
                    break;
                case REGULAR:
                    regCols.add(def);
                    break;
                case STATIC:
                    statCols.add(def);
                    break;
                case COMPACT_VALUE:
                    assert compactCol == null : "There shouldn't be more than one compact value defined: got " + compactCol + " and " + def;
                    compactCol = def;
                    break;
            }
        }

        // Now actually assign the correct value. This is not atomic, but then again, updating CFMetaData is never atomic anyway.
        partitionKeyColumns = addDefaultKeyAliases(pkCols);
        clusteringColumns = addDefaultColumnAliases(ckCols);
        regularColumns = regCols;
        staticColumns = statCols;
        compactValueColumn = addDefaultValueAlias(compactCol);
        return this;
    }

    private List<ColumnDefinition> addDefaultKeyAliases(List<ColumnDefinition> pkCols)
    {
        for (int i = 0; i < pkCols.size(); i++)
        {
            if (pkCols.get(i) == null)
            {
                Integer idx = null;
                AbstractType<?> type = keyValidator;
                if (keyValidator instanceof CompositeType)
                {
                    idx = i;
                    type = ((CompositeType)keyValidator).types.get(i);
                }
                // For compatibility sake, we call the first alias 'key' rather than 'key1'. This
                // is inconsistent with column alias, but it's probably not worth risking breaking compatibility now.
                ByteBuffer name = ByteBufferUtil.bytes(i == 0 ? DEFAULT_KEY_ALIAS : DEFAULT_KEY_ALIAS + (i + 1));
                ColumnDefinition newDef = ColumnDefinition.partitionKeyDef(this, name, type, idx);
                addOrReplaceColumnDefinition(newDef);
                pkCols.set(i, newDef);
            }
        }
        return pkCols;
    }

    private List<ColumnDefinition> addDefaultColumnAliases(List<ColumnDefinition> ckCols)
    {
        for (int i = 0; i < ckCols.size(); i++)
        {
            if (ckCols.get(i) == null)
            {
                Integer idx;
                AbstractType<?> type;
                if (comparator.isCompound())
                {
                    idx = i;
                    type = comparator.subtype(i);
                }
                else
                {
                    idx = null;
                    type = comparator.asAbstractType();
                }
                ByteBuffer name = ByteBufferUtil.bytes(DEFAULT_COLUMN_ALIAS + (i + 1));
                ColumnDefinition newDef = ColumnDefinition.clusteringKeyDef(this, name, type, idx);
                addOrReplaceColumnDefinition(newDef);
                ckCols.set(i, newDef);
            }
        }
        return ckCols;
    }

    private ColumnDefinition addDefaultValueAlias(ColumnDefinition compactValueDef)
    {
        if (comparator.isDense())
        {
            if (compactValueDef != null)
                return compactValueDef;

            ColumnDefinition newDef = ColumnDefinition.compactValueDef(this, ByteBufferUtil.bytes(DEFAULT_VALUE_ALIAS), defaultValidator);
            addOrReplaceColumnDefinition(newDef);
            return newDef;
        }
        else
        {
            assert compactValueDef == null;
            return null;
        }
    }

    /*
     * We call dense a CF for which each component of the comparator is a clustering column, i.e. no
     * component is used to store a regular column names. In other words, non-composite static "thrift"
     * and CQL3 CF are *not* dense.
     * We save whether the table is dense or not during table creation through CQL, but we don't have this
     * information for table just created through thrift, nor for table prior to CASSANDRA-7744, so this
     * method does its best to infer whether the table is dense or not based on other elements.
     */
    public static boolean calculateIsDense(AbstractType<?> comparator, Collection<ColumnDefinition> defs)
    {
        /*
         * As said above, this method is only here because we need to deal with thrift upgrades.
         * Once a CF has been "upgraded", i.e. we've rebuilt and save its CQL3 metadata at least once,
         * then we'll have saved the "is_dense" value and will be good to go.
         *
         * But non-upgraded thrift CF (and pre-7744 CF) will have no value for "is_dense", so we need
         * to infer that information without relying on it in that case. And for the most part this is
         * easy, a CF that has at least one REGULAR definition is not dense. But the subtlety is that not
         * having a REGULAR definition may not mean dense because of CQL3 definitions that have only the
         * PRIMARY KEY defined.
         *
         * So we need to recognize those special case CQL3 table with only a primary key. If we have some
         * clustering columns, we're fine as said above. So the only problem is that we cannot decide for
         * sure if a CF without REGULAR columns nor CLUSTERING_COLUMN definition is meant to be dense, or if it
         * has been created in CQL3 by say:
         *    CREATE TABLE test (k int PRIMARY KEY)
         * in which case it should not be dense. However, we can limit our margin of error by assuming we are
         * in the latter case only if the comparator is exactly CompositeType(UTF8Type).
         */
        boolean hasRegular = false;
        int maxClusteringIdx = -1;
        for (ColumnDefinition def : defs)
        {
            switch (def.kind)
            {
                case CLUSTERING_COLUMN:
                    maxClusteringIdx = Math.max(maxClusteringIdx, def.position());
                    break;
                case REGULAR:
                    hasRegular = true;
                    break;
            }
        }

        return maxClusteringIdx >= 0
             ? maxClusteringIdx == comparator.componentsCount() - 1
             : !hasRegular && !isCQL3OnlyPKComparator(comparator);
    }

    private static boolean isCQL3OnlyPKComparator(AbstractType<?> comparator)
    {
        if (!(comparator instanceof CompositeType))
            return false;

        CompositeType ct = (CompositeType)comparator;
        return ct.types.size() == 1 && ct.types.get(0) instanceof UTF8Type;
    }

    public boolean isCQL3Table()
    {
        return !isSuper() && !comparator.isDense() && comparator.isCompound();
    }

    private static <T> List<T> nullInitializedList(int size)
    {
        List<T> l = new ArrayList<>(size);
        for (int i = 0; i < size; ++i)
            l.add(null);
        return l;
    }

    /**
     * Returns whether this CFMetaData can be returned to thrift.
     */
    public boolean isThriftCompatible()
    {
        // Super CF are always "thrift compatible". But since they may have defs with a componentIndex != null,
        // we have to special case here.
        if (isSuper())
            return true;

        for (ColumnDefinition def : allColumns())
        {
            // Non-REGULAR ColumnDefinition are not "thrift compatible" per-se, but it's ok because they hold metadata
            // this is only of use to CQL3, so we will just skip them in toThrift.
            if (def.kind == ColumnDefinition.Kind.REGULAR && !def.isThriftCompatible())
                return false;
        }

        // The table might also have no REGULAR columns (be PK-only), but still be "thrift incompatible". See #7832.
        if (isCQL3OnlyPKComparator(comparator.asAbstractType()) && !isDense)
            return false;

        return true;
    }

    public boolean isCounter()
    {
        return defaultValidator.isCounter();
    }

    public boolean hasStaticColumns()
    {
        return !staticColumns.isEmpty();
    }

    @Override
    public String toString()
    {
        return new ToStringBuilder(this)
            .append("cfId", cfId)
            .append("ksName", ksName)
            .append("cfName", cfName)
            .append("cfType", cfType)
            .append("comparator", comparator)
            .append("comment", comment)
            .append("readRepairChance", readRepairChance)
            .append("dcLocalReadRepairChance", dcLocalReadRepairChance)
            .append("gcGraceSeconds", gcGraceSeconds)
            .append("defaultValidator", defaultValidator)
            .append("keyValidator", keyValidator)
            .append("minCompactionThreshold", minCompactionThreshold)
            .append("maxCompactionThreshold", maxCompactionThreshold)
            .append("columnMetadata", columnMetadata.values())
            .append("compactionStrategyClass", compactionStrategyClass)
            .append("compactionStrategyOptions", compactionStrategyOptions)
            .append("compressionParameters", compressionParameters.asThriftOptions())
            .append("bloomFilterFpChance", getBloomFilterFpChance())
            .append("memtableFlushPeriod", memtableFlushPeriod)
            .append("caching", caching)
            .append("defaultTimeToLive", defaultTimeToLive)
            .append("minIndexInterval", minIndexInterval)
            .append("maxIndexInterval", maxIndexInterval)
            .append("speculativeRetry", speculativeRetry)
            .append("droppedColumns", droppedColumns)
            .append("triggers", triggers.values())
            .append("isDense", isDense)
            .toString();
    }
}
