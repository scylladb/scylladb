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

#pragma once

#include "cql3/statements/property_definitions.hh"
 
#include "schema.hh"
#include "database.hh"
#include "schema_builder.hh"

namespace cql3 {

namespace statements {

class cf_prop_defs : public property_definitions {
public:
    static const sstring KW_COMMENT;
    static const sstring KW_READREPAIRCHANCE;
    static const sstring KW_DCLOCALREADREPAIRCHANCE;
    static const sstring KW_GCGRACESECONDS;
    static const sstring KW_MINCOMPACTIONTHRESHOLD;
    static const sstring KW_MAXCOMPACTIONTHRESHOLD;
    static const sstring KW_CACHING;
    static const sstring KW_DEFAULT_TIME_TO_LIVE;
    static const sstring KW_MIN_INDEX_INTERVAL;
    static const sstring KW_MAX_INDEX_INTERVAL;
    static const sstring KW_SPECULATIVE_RETRY;
    static const sstring KW_BF_FP_CHANCE;
    static const sstring KW_MEMTABLE_FLUSH_PERIOD;

    static const sstring KW_COMPACTION;
    static const sstring KW_COMPRESSION;

    static const sstring COMPACTION_STRATEGY_CLASS_KEY;

    // FIXME: In origin the following consts are in CFMetaData.
    static constexpr int32_t DEFAULT_DEFAULT_TIME_TO_LIVE = 0;
    static constexpr int32_t DEFAULT_MIN_INDEX_INTERVAL = 128;
    static constexpr int32_t DEFAULT_MAX_INDEX_INTERVAL = 2048;
private:
    std::experimental::optional<sstring> _compaction_strategy_class;
public:
    void validate() {
        // Skip validation if the comapction strategy class is already set as it means we've alreayd
        // prepared (and redoing it would set strategyClass back to null, which we don't want)
        if (_compaction_strategy_class) {
            return;
        }

        static std::set<sstring> keywords({
            KW_COMMENT, KW_READREPAIRCHANCE, KW_DCLOCALREADREPAIRCHANCE,
            KW_GCGRACESECONDS, KW_CACHING, KW_DEFAULT_TIME_TO_LIVE,
            KW_MIN_INDEX_INTERVAL, KW_MAX_INDEX_INTERVAL, KW_SPECULATIVE_RETRY,
            KW_BF_FP_CHANCE, KW_MEMTABLE_FLUSH_PERIOD, KW_COMPACTION,
            KW_COMPRESSION,
        });
        static std::set<sstring> obsolete_keywords({
            sstring("index_interval"),
            sstring("replicate_on_write"),
            sstring("populate_io_cache_on_flush"),
        });
        property_definitions::validate(keywords, obsolete_keywords);

        auto compaction_options = get_compaction_options();
        if (!compaction_options.empty()) {
            auto strategy = compaction_options.find(COMPACTION_STRATEGY_CLASS_KEY);
            if (strategy == compaction_options.end()) {
                throw exceptions::configuration_exception(sstring("Missing sub-option '") + COMPACTION_STRATEGY_CLASS_KEY + "' for the '" + KW_COMPACTION + "' option.");
            }
            _compaction_strategy_class = strategy->second;
#if 0
            compactionStrategyClass = CFMetaData.createCompactionStrategy(strategy);
            compactionOptions.remove(COMPACTION_STRATEGY_CLASS_KEY);

            CFMetaData.validateCompactionOptions(compactionStrategyClass, compactionOptions);
#endif
        }

        auto compression_options = get_compression_options();
        if (!compression_options.empty()) {
            auto sstable_compression_class = compression_options.find(sstring(compression_parameters::SSTABLE_COMPRESSION));
            if (sstable_compression_class == compression_options.end()) {
                throw exceptions::configuration_exception(sstring("Missing sub-option '") + compression_parameters::SSTABLE_COMPRESSION + "' for the '" + KW_COMPRESSION + "' option.");
            }
            compression_parameters cp(compression_options);
            cp.validate();
        }

        validate_minimum_int(KW_DEFAULT_TIME_TO_LIVE, 0, DEFAULT_DEFAULT_TIME_TO_LIVE);

        auto min_index_interval = get_int(KW_MIN_INDEX_INTERVAL, DEFAULT_MIN_INDEX_INTERVAL);
        auto max_index_interval = get_int(KW_MAX_INDEX_INTERVAL, DEFAULT_MAX_INDEX_INTERVAL);
        if (min_index_interval < 1) {
            throw exceptions::configuration_exception(KW_MIN_INDEX_INTERVAL + " must be greater than 0");
        }
        if (max_index_interval < min_index_interval) {
            throw exceptions::configuration_exception(KW_MAX_INDEX_INTERVAL + " must be greater than " + KW_MIN_INDEX_INTERVAL);
        }

        speculative_retry::from_sstring(get_string(KW_SPECULATIVE_RETRY, speculative_retry(speculative_retry::type::NONE, 0).to_sstring()));
    }

#if 0
    public Class<? extends AbstractCompactionStrategy> getCompactionStrategy()
    {
        return compactionStrategyClass;
    }
#endif
    std::map<sstring, sstring> get_compaction_options() const {
        auto compaction_options = get_map(KW_COMPACTION);
        if (compaction_options ) {
            return compaction_options.value();
        }
        return std::map<sstring, sstring>{};
    }
    std::map<sstring, sstring> get_compression_options() const {
        auto compression_options = get_map(KW_COMPRESSION);
        if (compression_options) {
            return compression_options.value();
        }
        return std::map<sstring, sstring>{};
    }
#if 0
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
#endif

    void apply_to_builder(schema_builder& builder) {
        if (has_property(KW_COMMENT)) {
            builder.set_comment(get_string(KW_COMMENT, ""));
        }

        if (has_property(KW_READREPAIRCHANCE)) {
            builder.set_read_repair_chance(get_double(KW_READREPAIRCHANCE, builder.get_read_repair_chance()));
        }

        if (has_property(KW_DCLOCALREADREPAIRCHANCE)) {
            builder.set_dc_local_read_repair_chance(get_double(KW_DCLOCALREADREPAIRCHANCE, builder.get_dc_local_read_repair_chance()));
        }

        if (has_property(KW_GCGRACESECONDS)) {
            builder.set_gc_grace_seconds(get_int(KW_GCGRACESECONDS, builder.get_gc_grace_seconds()));
        }

        std::experimental::optional<sstring> tmp_value = {};
        if (has_property(KW_MINCOMPACTIONTHRESHOLD)) {
            if (get_compaction_options().count(KW_MINCOMPACTIONTHRESHOLD)) {
                tmp_value = get_compaction_options().at(KW_MINCOMPACTIONTHRESHOLD);
            }
        }
        int min_compaction_threshold = to_int(KW_MINCOMPACTIONTHRESHOLD, tmp_value, builder.get_min_compaction_threshold());

        tmp_value = {};
        if (has_property(KW_MAXCOMPACTIONTHRESHOLD)) {
            if (get_compaction_options().count(KW_MAXCOMPACTIONTHRESHOLD)) {
                tmp_value = get_compaction_options().at(KW_MAXCOMPACTIONTHRESHOLD);
            }
        }
        int max_compaction_threshold = to_int(KW_MAXCOMPACTIONTHRESHOLD, tmp_value, builder.get_max_compaction_threshold());

        if (min_compaction_threshold <= 0 || max_compaction_threshold <= 0)
            throw exceptions::configuration_exception("Disabling compaction by setting compaction thresholds to 0 has been deprecated, set the compaction option 'enabled' to false instead.");
        builder.set_min_compaction_threshold(min_compaction_threshold);
        builder.set_max_compaction_threshold(max_compaction_threshold);

#if 0
        cfm.defaultTimeToLive(getInt(KW_DEFAULT_TIME_TO_LIVE, cfm.getDefaultTimeToLive()));
#endif
        if (has_property(KW_SPECULATIVE_RETRY)) {
            builder.set_speculative_retry(get_string(KW_SPECULATIVE_RETRY, builder.get_speculative_retry().to_sstring()));
        }

        if (has_property(KW_MEMTABLE_FLUSH_PERIOD)) {
            builder.set_memtable_flush_period(get_int(KW_MEMTABLE_FLUSH_PERIOD, builder.get_memtable_flush_period()));
        }

        if (has_property(KW_MIN_INDEX_INTERVAL)) {
            builder.set_min_index_interval(get_int(KW_MIN_INDEX_INTERVAL, builder.get_min_index_interval()));
        }

        if (has_property(KW_MAX_INDEX_INTERVAL)) {
            builder.set_max_index_interval(get_int(KW_MAX_INDEX_INTERVAL, builder.get_max_index_interval()));
        }

#if 0
        if (compactionStrategyClass != null)
        {
            cfm.compactionStrategyClass(compactionStrategyClass);
            cfm.compactionStrategyOptions(new HashMap<>(getCompactionOptions()));
        }
#endif

        builder.set_bloom_filter_fp_chance(get_double(KW_BF_FP_CHANCE, builder.get_bloom_filter_fp_chance()));
        if (!get_compression_options().empty()) {
            builder.set_compressor_params(compression_parameters(get_compression_options()));
        }
#if 0
        CachingOptions cachingOptions = getCachingOptions();
        if (cachingOptions != null)
            cfm.caching(cachingOptions);
#endif
    }

#if 0
    @Override
    public String toString()
    {
        return String.format("CFPropDefs(%s)", properties.toString());
    }
#endif
    void validate_minimum_int(const sstring& field, int32_t minimum_value, int32_t default_value) const
    {
        auto val = get_int(field, default_value);
        if (val < minimum_value) {
            throw exceptions::configuration_exception(sprint("%s cannot be smaller than %s, (default %s)",
                                                             field, minimum_value, default_value));
        }
    }
};

}

}
