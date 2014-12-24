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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.cache.CachingOptions;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.CFMetaData.SpeculativeRetry;
import org.apache.cassandra.db.compaction.AbstractCompactionStrategy;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.io.compress.CompressionParameters;

public class CFPropDefs extends PropertyDefinitions
{
    public static final String KW_COMMENT = "comment";
    public static final String KW_READREPAIRCHANCE = "read_repair_chance";
    public static final String KW_DCLOCALREADREPAIRCHANCE = "dclocal_read_repair_chance";
    public static final String KW_GCGRACESECONDS = "gc_grace_seconds";
    public static final String KW_MINCOMPACTIONTHRESHOLD = "min_threshold";
    public static final String KW_MAXCOMPACTIONTHRESHOLD = "max_threshold";
    public static final String KW_CACHING = "caching";
    public static final String KW_DEFAULT_TIME_TO_LIVE = "default_time_to_live";
    public static final String KW_MIN_INDEX_INTERVAL = "min_index_interval";
    public static final String KW_MAX_INDEX_INTERVAL = "max_index_interval";
    public static final String KW_SPECULATIVE_RETRY = "speculative_retry";
    public static final String KW_BF_FP_CHANCE = "bloom_filter_fp_chance";
    public static final String KW_MEMTABLE_FLUSH_PERIOD = "memtable_flush_period_in_ms";

    public static final String KW_COMPACTION = "compaction";
    public static final String KW_COMPRESSION = "compression";

    public static final String COMPACTION_STRATEGY_CLASS_KEY = "class";

    public static final Set<String> keywords = new HashSet<>();
    public static final Set<String> obsoleteKeywords = new HashSet<>();

    static
    {
        keywords.add(KW_COMMENT);
        keywords.add(KW_READREPAIRCHANCE);
        keywords.add(KW_DCLOCALREADREPAIRCHANCE);
        keywords.add(KW_GCGRACESECONDS);
        keywords.add(KW_CACHING);
        keywords.add(KW_DEFAULT_TIME_TO_LIVE);
        keywords.add(KW_MIN_INDEX_INTERVAL);
        keywords.add(KW_MAX_INDEX_INTERVAL);
        keywords.add(KW_SPECULATIVE_RETRY);
        keywords.add(KW_BF_FP_CHANCE);
        keywords.add(KW_COMPACTION);
        keywords.add(KW_COMPRESSION);
        keywords.add(KW_MEMTABLE_FLUSH_PERIOD);

        obsoleteKeywords.add("index_interval");
        obsoleteKeywords.add("replicate_on_write");
        obsoleteKeywords.add("populate_io_cache_on_flush");
    }

    private Class<? extends AbstractCompactionStrategy> compactionStrategyClass = null;

    public void validate() throws ConfigurationException, SyntaxException
    {
        // Skip validation if the comapction strategy class is already set as it means we've alreayd
        // prepared (and redoing it would set strategyClass back to null, which we don't want)
        if (compactionStrategyClass != null)
            return;

        validate(keywords, obsoleteKeywords);

        Map<String, String> compactionOptions = getCompactionOptions();
        if (!compactionOptions.isEmpty())
        {
            String strategy = compactionOptions.get(COMPACTION_STRATEGY_CLASS_KEY);
            if (strategy == null)
                throw new ConfigurationException("Missing sub-option '" + COMPACTION_STRATEGY_CLASS_KEY + "' for the '" + KW_COMPACTION + "' option.");

            compactionStrategyClass = CFMetaData.createCompactionStrategy(strategy);
            compactionOptions.remove(COMPACTION_STRATEGY_CLASS_KEY);

            CFMetaData.validateCompactionOptions(compactionStrategyClass, compactionOptions);
        }

        Map<String, String> compressionOptions = getCompressionOptions();
        if (!compressionOptions.isEmpty())
        {
            String sstableCompressionClass = compressionOptions.get(CompressionParameters.SSTABLE_COMPRESSION);
            if (sstableCompressionClass == null)
                throw new ConfigurationException("Missing sub-option '" + CompressionParameters.SSTABLE_COMPRESSION + "' for the '" + KW_COMPRESSION + "' option.");

            Integer chunkLength = CompressionParameters.DEFAULT_CHUNK_LENGTH;
            if (compressionOptions.containsKey(CompressionParameters.CHUNK_LENGTH_KB))
                chunkLength = CompressionParameters.parseChunkLength(compressionOptions.get(CompressionParameters.CHUNK_LENGTH_KB));

            Map<String, String> remainingOptions = new HashMap<>(compressionOptions);
            remainingOptions.remove(CompressionParameters.SSTABLE_COMPRESSION);
            remainingOptions.remove(CompressionParameters.CHUNK_LENGTH_KB);
            CompressionParameters cp = new CompressionParameters(sstableCompressionClass, chunkLength, remainingOptions);
            cp.validate();
        }

        validateMinimumInt(KW_DEFAULT_TIME_TO_LIVE, 0, CFMetaData.DEFAULT_DEFAULT_TIME_TO_LIVE);

        Integer minIndexInterval = getInt(KW_MIN_INDEX_INTERVAL, null);
        Integer maxIndexInterval = getInt(KW_MAX_INDEX_INTERVAL, null);
        if (minIndexInterval != null && minIndexInterval < 1)
            throw new ConfigurationException(KW_MIN_INDEX_INTERVAL + " must be greater than 0");
        if (maxIndexInterval != null && minIndexInterval != null && maxIndexInterval < minIndexInterval)
            throw new ConfigurationException(KW_MAX_INDEX_INTERVAL + " must be greater than " + KW_MIN_INDEX_INTERVAL);

        SpeculativeRetry.fromString(getString(KW_SPECULATIVE_RETRY, SpeculativeRetry.RetryType.NONE.name()));
    }

    public Class<? extends AbstractCompactionStrategy> getCompactionStrategy()
    {
        return compactionStrategyClass;
    }

    public Map<String, String> getCompactionOptions() throws SyntaxException
    {
        Map<String, String> compactionOptions = getMap(KW_COMPACTION);
        if (compactionOptions == null)
            return Collections.emptyMap();
        return compactionOptions;
    }

    public Map<String, String> getCompressionOptions() throws SyntaxException
    {
        Map<String, String> compressionOptions = getMap(KW_COMPRESSION);
        if (compressionOptions == null)
            return Collections.emptyMap();
        return compressionOptions;
    }
    public CachingOptions getCachingOptions() throws SyntaxException, ConfigurationException
    {
        CachingOptions options = null;
        Object val = properties.get(KW_CACHING);
        if (val == null)
            return null;
        else if (val instanceof Map)
            options = CachingOptions.fromMap(getMap(KW_CACHING));
        else if (val instanceof String) // legacy syntax
        {
            options = CachingOptions.fromString(getSimple(KW_CACHING));
            logger.warn("Setting caching options with deprecated syntax.");
        }
        return options;
    }

    public void applyToCFMetadata(CFMetaData cfm) throws ConfigurationException, SyntaxException
    {
        if (hasProperty(KW_COMMENT))
            cfm.comment(getString(KW_COMMENT, ""));

        cfm.readRepairChance(getDouble(KW_READREPAIRCHANCE, cfm.getReadRepairChance()));
        cfm.dcLocalReadRepairChance(getDouble(KW_DCLOCALREADREPAIRCHANCE, cfm.getDcLocalReadRepairChance()));
        cfm.gcGraceSeconds(getInt(KW_GCGRACESECONDS, cfm.getGcGraceSeconds()));
        int minCompactionThreshold = toInt(KW_MINCOMPACTIONTHRESHOLD, getCompactionOptions().get(KW_MINCOMPACTIONTHRESHOLD), cfm.getMinCompactionThreshold());
        int maxCompactionThreshold = toInt(KW_MAXCOMPACTIONTHRESHOLD, getCompactionOptions().get(KW_MAXCOMPACTIONTHRESHOLD), cfm.getMaxCompactionThreshold());
        if (minCompactionThreshold <= 0 || maxCompactionThreshold <= 0)
            throw new ConfigurationException("Disabling compaction by setting compaction thresholds to 0 has been deprecated, set the compaction option 'enabled' to false instead.");
        cfm.minCompactionThreshold(minCompactionThreshold);
        cfm.maxCompactionThreshold(maxCompactionThreshold);
        cfm.defaultTimeToLive(getInt(KW_DEFAULT_TIME_TO_LIVE, cfm.getDefaultTimeToLive()));
        cfm.speculativeRetry(CFMetaData.SpeculativeRetry.fromString(getString(KW_SPECULATIVE_RETRY, cfm.getSpeculativeRetry().toString())));
        cfm.memtableFlushPeriod(getInt(KW_MEMTABLE_FLUSH_PERIOD, cfm.getMemtableFlushPeriod()));
        cfm.minIndexInterval(getInt(KW_MIN_INDEX_INTERVAL, cfm.getMinIndexInterval()));
        cfm.maxIndexInterval(getInt(KW_MAX_INDEX_INTERVAL, cfm.getMaxIndexInterval()));

        if (compactionStrategyClass != null)
        {
            cfm.compactionStrategyClass(compactionStrategyClass);
            cfm.compactionStrategyOptions(new HashMap<>(getCompactionOptions()));
        }

        cfm.bloomFilterFpChance(getDouble(KW_BF_FP_CHANCE, cfm.getBloomFilterFpChance()));

        if (!getCompressionOptions().isEmpty())
            cfm.compressionParameters(CompressionParameters.create(getCompressionOptions()));
        CachingOptions cachingOptions = getCachingOptions();
        if (cachingOptions != null)
            cfm.caching(cachingOptions);
    }

    @Override
    public String toString()
    {
        return String.format("CFPropDefs(%s)", properties.toString());
    }

    private void validateMinimumInt(String field, int minimumValue, int defaultValue) throws SyntaxException, ConfigurationException
    {
        Integer val = getInt(field, null);
        if (val != null && val < minimumValue)
            throw new ConfigurationException(String.format("%s cannot be smaller than %s, (default %s)",
                                                            field, minimumValue, defaultValue));

    }
}
