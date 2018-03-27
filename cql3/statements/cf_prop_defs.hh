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

#pragma once

#include "cql3/statements/property_definitions.hh"
 
#include "schema.hh"
#include "schema_builder.hh"
#include "compaction_strategy.hh"
#include "utils/UUID.hh"

namespace db {
class extensions;
}

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
    static const sstring KW_CRC_CHECK_CHANCE;

    static const sstring KW_ID;

    static const sstring COMPACTION_STRATEGY_CLASS_KEY;
    static const sstring COMPACTION_ENABLED_KEY;

    // FIXME: In origin the following consts are in CFMetaData.
    static constexpr int32_t DEFAULT_DEFAULT_TIME_TO_LIVE = 0;
    static constexpr int32_t DEFAULT_MIN_INDEX_INTERVAL = 128;
    static constexpr int32_t DEFAULT_MAX_INDEX_INTERVAL = 2048;
private:
    std::experimental::optional<sstables::compaction_strategy_type> _compaction_strategy_class;
public:
    void validate(const db::extensions&);
    std::map<sstring, sstring> get_compaction_options() const;
    stdx::optional<std::map<sstring, sstring>> get_compression_options() const;
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
    int32_t get_default_time_to_live() const;
    int32_t get_gc_grace_seconds() const;
    stdx::optional<utils::UUID> get_id() const;

    void apply_to_builder(schema_builder& builder, const db::extensions&);
    void validate_minimum_int(const sstring& field, int32_t minimum_value, int32_t default_value) const;
};

}

}
