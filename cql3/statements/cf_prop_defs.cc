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
 * Copyright (C) 2015-present ScyllaDB
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
#include "database.hh"
#include "db/extensions.hh"
#include "cdc/log.hh"
#include "cdc/cdc_extension.hh"
#include "gms/feature.hh"
#include "gms/feature_service.hh"

#include <boost/algorithm/string/predicate.hpp>

namespace cql3 {

namespace statements {

const sstring cf_prop_defs::KW_COMMENT = "comment";
const sstring cf_prop_defs::KW_READREPAIRCHANCE = "read_repair_chance";
const sstring cf_prop_defs::KW_DCLOCALREADREPAIRCHANCE = "dclocal_read_repair_chance";
const sstring cf_prop_defs::KW_GCGRACESECONDS = "gc_grace_seconds";
const sstring cf_prop_defs::KW_PAXOSGRACESECONDS = "paxos_grace_seconds";
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

schema::extensions_map cf_prop_defs::make_schema_extensions(const db::extensions& exts) const {
    schema::extensions_map er;
    for (auto& p : exts.schema_extensions()) {
        auto i = _properties.find(p.first);
        if (i != _properties.end()) {
            std::visit([&](auto& v) {
                auto ep = p.second(v);
                if (ep) {
                    er.emplace(p.first, std::move(ep));
                }
            }, i->second);
        }
    }
    return er;
}

void cf_prop_defs::validate(const database& db, const schema::extensions_map& schema_extensions) const {
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
        KW_COMPRESSION, KW_CRC_CHECK_CHANCE, KW_ID, KW_PAXOSGRACESECONDS
    });
    static std::set<sstring> obsolete_keywords({
        sstring("index_interval"),
        sstring("replicate_on_write"),
        sstring("populate_io_cache_on_flush"),
    });

    const auto& exts = db.extensions();
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

    if (auto caching_options = get_caching_options(); caching_options && !caching_options->enabled() && !db.features().cluster_supports_per_table_caching()) {
        throw exceptions::configuration_exception(KW_CACHING + " can't contain \"'enabled':false\" unless whole cluster supports it");
    }

    auto cdc_options = get_cdc_options(schema_extensions);
    if (cdc_options && cdc_options->enabled() && !db.features().cluster_supports_cdc()) {
        throw exceptions::configuration_exception("CDC not supported by the cluster");
    }

    validate_minimum_int(KW_DEFAULT_TIME_TO_LIVE, 0, DEFAULT_DEFAULT_TIME_TO_LIVE);
    validate_minimum_int(KW_PAXOSGRACESECONDS, 0, DEFAULT_GC_GRACE_SECONDS);

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

std::optional<std::map<sstring, sstring>> cf_prop_defs::get_compression_options() const {
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

int32_t cf_prop_defs::get_paxos_grace_seconds() const {
    return get_int(KW_PAXOSGRACESECONDS, DEFAULT_GC_GRACE_SECONDS);
}

std::optional<utils::UUID> cf_prop_defs::get_id() const {
    auto id = get_simple(KW_ID);
    if (id) {
        return utils::UUID(*id);
    }

    return std::nullopt;
}

std::optional<caching_options> cf_prop_defs::get_caching_options() const {
    auto value = get(KW_CACHING);
    if (!value) {
        return {};
    }
    return std::visit(make_visitor(
        [] (const property_definitions::map_type& map) {
            return map.empty() ? std::nullopt : std::optional<caching_options>(caching_options::from_map(map));
        },
        [] (const sstring& str) {
            return std::optional<caching_options>(caching_options::from_sstring(str));
        }
    ), *value);
}

const cdc::options* cf_prop_defs::get_cdc_options(const schema::extensions_map& schema_exts) const {
    auto it = schema_exts.find(cdc::cdc_extension::NAME);
    if (it == schema_exts.end()) {
        return nullptr;
    }

    auto cdc_ext = dynamic_pointer_cast<cdc::cdc_extension>(it->second);
    return &cdc_ext->get_options();
}

void cf_prop_defs::apply_to_builder(schema_builder& builder, schema::extensions_map schema_extensions) const {
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

    if (has_property(KW_PAXOSGRACESECONDS)) {
        builder.set_paxos_grace_seconds(get_paxos_grace_seconds());
    }

    std::optional<sstring> tmp_value = {};
    if (has_property(KW_COMPACTION)) {
        if (get_compaction_options().contains(KW_MINCOMPACTIONTHRESHOLD)) {
            tmp_value = get_compaction_options().at(KW_MINCOMPACTIONTHRESHOLD);
        }
    }
    int min_compaction_threshold = to_int(KW_MINCOMPACTIONTHRESHOLD, tmp_value, builder.get_min_compaction_threshold());

    tmp_value = {};
    if (has_property(KW_COMPACTION)) {
        if (get_compaction_options().contains(KW_MAXCOMPACTIONTHRESHOLD)) {
            tmp_value = get_compaction_options().at(KW_MAXCOMPACTIONTHRESHOLD);
        }
    }
    int max_compaction_threshold = to_int(KW_MAXCOMPACTIONTHRESHOLD, tmp_value, builder.get_max_compaction_threshold());

    if (min_compaction_threshold <= 0 || max_compaction_threshold <= 0)
        throw exceptions::configuration_exception("Disabling compaction by setting compaction thresholds to 0 has been deprecated, set the compaction option 'enabled' to false instead.");
    builder.set_min_compaction_threshold(min_compaction_threshold);
    builder.set_max_compaction_threshold(max_compaction_threshold);

    if (has_property(KW_COMPACTION)) {
        if (get_compaction_options().contains(COMPACTION_ENABLED_KEY)) {
            auto enabled = boost::algorithm::iequals(get_compaction_options().at(COMPACTION_ENABLED_KEY), "true");
            builder.set_compaction_enabled(enabled);
        }
    }

    if (has_property(KW_DEFAULT_TIME_TO_LIVE)) {
        builder.set_default_time_to_live(gc_clock::duration(get_int(KW_DEFAULT_TIME_TO_LIVE, DEFAULT_DEFAULT_TIME_TO_LIVE)));
    }

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

    auto caching_options = get_caching_options();
    if (caching_options) {
        builder.set_caching_options(std::move(*caching_options));
    }

    // for extensions that are not altered, keep the old ones
    auto& old_exts = builder.get_extensions();
    for (auto& [key, ext] : old_exts) {
        if (!_properties.count(key)) {
            schema_extensions.emplace(key, ext);
        }
    }

    builder.set_extensions(std::move(schema_extensions));
}

void cf_prop_defs::validate_minimum_int(const sstring& field, int32_t minimum_value, int32_t default_value) const
{
    auto val = get_int(field, default_value);
    if (val < minimum_value) {
        throw exceptions::configuration_exception(format("{} cannot be smaller than {}, (default {})",
                                                         field, minimum_value, default_value));
    }
}

}

}
