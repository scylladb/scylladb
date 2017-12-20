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
 * Copyright (C) 2015 ScyllaDB
 *
 * Modified by ScyllaDB
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

#include "cql3/statements/cf_prop_defs.hh"
#include "db/extensions.hh"

#include <boost/algorithm/string/predicate.hpp>

namespace cql3 {

namespace statements {

const sstring cf_prop_defs::KW_COMMENT = "comment";
const sstring cf_prop_defs::KW_READREPAIRCHANCE = "read_repair_chance";
const sstring cf_prop_defs::KW_DCLOCALREADREPAIRCHANCE = "dclocal_read_repair_chance";
const sstring cf_prop_defs::KW_GCGRACESECONDS = "gc_grace_seconds";
const sstring cf_prop_defs::KW_MINCOMPACTIONTHRESHOLD = "min_threshold";
const sstring cf_prop_defs::KW_MAXCOMPACTIONTHRESHOLD = "max_threshold";
const sstring cf_prop_defs::KW_CACHING = "caching";
const sstring cf_prop_defs::KW_DEFAULT_TIME_TO_LIVE = "default_time_to_live";
const sstring cf_prop_defs::KW_MIN_INDEX_INTERVAL = "min_index_interval";
const sstring cf_prop_defs::KW_MAX_INDEX_INTERVAL = "max_index_interval";
const sstring cf_prop_defs::KW_SPECULATIVE_RETRY = "speculative_retry";
const sstring cf_prop_defs::KW_BF_FP_CHANCE = "bloom_filter_fp_chance";
const sstring cf_prop_defs::KW_MEMTABLE_FLUSH_PERIOD = "memtable_flush_period_in_ms";

const sstring cf_prop_defs::KW_COMPACTION = "compaction";
const sstring cf_prop_defs::KW_COMPRESSION = "compression";
const sstring cf_prop_defs::KW_CRC_CHECK_CHANCE = "crc_check_chance";

const sstring cf_prop_defs::KW_ID = "id";

const sstring cf_prop_defs::COMPACTION_STRATEGY_CLASS_KEY = "class";

const sstring cf_prop_defs::COMPACTION_ENABLED_KEY = "enabled";

void cf_prop_defs::validate(const db::extensions& exts) {
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
        KW_COMPRESSION, KW_CRC_CHECK_CHANCE, KW_ID
    });
    static std::set<sstring> obsolete_keywords({
        sstring("index_interval"),
        sstring("replicate_on_write"),
        sstring("populate_io_cache_on_flush"),
    });
    property_definitions::validate(keywords, exts.schema_extension_keywords(), obsolete_keywords);

    try {
        get_id();
    } catch(...) {
        std::throw_with_nested(exceptions::configuration_exception("Invalid table id"));
    }

    auto compaction_options = get_compaction_options();
    if (!compaction_options.empty()) {
        auto strategy = compaction_options.find(COMPACTION_STRATEGY_CLASS_KEY);
        if (strategy == compaction_options.end()) {
            throw exceptions::configuration_exception(sstring("Missing sub-option '") + COMPACTION_STRATEGY_CLASS_KEY + "' for the '" + KW_COMPACTION + "' option.");
        }
        _compaction_strategy_class = sstables::compaction_strategy::type(strategy->second);
        remove_from_map_if_exists(KW_COMPACTION, COMPACTION_STRATEGY_CLASS_KEY);

#if 0
       CFMetaData.validateCompactionOptions(compactionStrategyClass, compactionOptions);
#endif
    }

    auto compression_options = get_compression_options();
    if (compression_options && !compression_options->empty()) {
        auto sstable_compression_class = compression_options->find(sstring(compression_parameters::SSTABLE_COMPRESSION));
        if (sstable_compression_class == compression_options->end()) {
            throw exceptions::configuration_exception(sstring("Missing sub-option '") + compression_parameters::SSTABLE_COMPRESSION + "' for the '" + KW_COMPRESSION + "' option.");
        }
        compression_parameters cp(*compression_options);
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

std::map<sstring, sstring> cf_prop_defs::get_compaction_options() const {
    auto compaction_options = get_map(KW_COMPACTION);
    if (compaction_options ) {
        return compaction_options.value();
    }
    return std::map<sstring, sstring>{};
}

stdx::optional<std::map<sstring, sstring>> cf_prop_defs::get_compression_options() const {
    auto compression_options = get_map(KW_COMPRESSION);
    if (compression_options) {
        return { compression_options.value() };
    }
    return { };
}

int32_t cf_prop_defs::get_default_time_to_live() const
{
    return get_int(KW_DEFAULT_TIME_TO_LIVE, 0);
}

int32_t cf_prop_defs::get_gc_grace_seconds() const
{
    return get_int(KW_GCGRACESECONDS, DEFAULT_GC_GRACE_SECONDS);
}

stdx::optional<utils::UUID> cf_prop_defs::get_id() const {
    auto id = get_simple(KW_ID);
    if (id) {
        return utils::UUID(*id);
    }

    return stdx::nullopt;
}

void cf_prop_defs::apply_to_builder(schema_builder& builder, const db::extensions& exts) {
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
    if (has_property(KW_COMPACTION)) {
        if (get_compaction_options().count(KW_MINCOMPACTIONTHRESHOLD)) {
            tmp_value = get_compaction_options().at(KW_MINCOMPACTIONTHRESHOLD);
        }
    }
    int min_compaction_threshold = to_int(KW_MINCOMPACTIONTHRESHOLD, tmp_value, builder.get_min_compaction_threshold());

    tmp_value = {};
    if (has_property(KW_COMPACTION)) {
        if (get_compaction_options().count(KW_MAXCOMPACTIONTHRESHOLD)) {
            tmp_value = get_compaction_options().at(KW_MAXCOMPACTIONTHRESHOLD);
        }
    }
    int max_compaction_threshold = to_int(KW_MAXCOMPACTIONTHRESHOLD, tmp_value, builder.get_max_compaction_threshold());

    if (min_compaction_threshold <= 0 || max_compaction_threshold <= 0)
        throw exceptions::configuration_exception("Disabling compaction by setting compaction thresholds to 0 has been deprecated, set the compaction option 'enabled' to false instead.");
    builder.set_min_compaction_threshold(min_compaction_threshold);
    builder.set_max_compaction_threshold(max_compaction_threshold);

    if (has_property(KW_COMPACTION)) {
        if (get_compaction_options().count(COMPACTION_ENABLED_KEY)) {
            auto enabled = boost::algorithm::iequals(get_compaction_options().at(COMPACTION_ENABLED_KEY), "true");
            builder.set_compaction_enabled(enabled);
        }
    }

    builder.set_default_time_to_live(gc_clock::duration(get_int(KW_DEFAULT_TIME_TO_LIVE, DEFAULT_DEFAULT_TIME_TO_LIVE)));

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

    if (_compaction_strategy_class) {
        builder.set_compaction_strategy(*_compaction_strategy_class);
        builder.set_compaction_strategy_options(get_compaction_options());
    }

    builder.set_bloom_filter_fp_chance(get_double(KW_BF_FP_CHANCE, builder.get_bloom_filter_fp_chance()));
    auto compression_options = get_compression_options();
    if (compression_options) {
        builder.set_compressor_params(compression_parameters(*compression_options));
    }
#if 0
    CachingOptions cachingOptions = getCachingOptions();
    if (cachingOptions != null)
        cfm.caching(cachingOptions);
#endif

    schema::extensions_map er;
    for (auto& p : exts.schema_extensions()) {
        auto i = _properties.find(p.first);
        if (i != _properties.end()) {
            std::visit([&](auto& v) { er.emplace(p.first, p.second(v)); }, i->second);
        }
    }
    builder.set_extensions(std::move(er));
}

void cf_prop_defs::validate_minimum_int(const sstring& field, int32_t minimum_value, int32_t default_value) const
{
    auto val = get_int(field, default_value);
    if (val < minimum_value) {
        throw exceptions::configuration_exception(sprint("%s cannot be smaller than %s, (default %s)",
                                                         field, minimum_value, default_value));
    }
}

}

}
