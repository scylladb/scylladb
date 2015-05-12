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
#include "config/ks_meta_data.hh"
#include "core/sstring.hh"

#include <experimental/optional>

namespace cql3 {

namespace statements {

class ks_prop_defs : public property_definitions {
public:
    static constexpr auto KW_DURABLE_WRITES = "durable_writes";
    static constexpr auto KW_REPLICATION = "replication";

    static constexpr auto REPLICATION_STRATEGY_CLASS_KEY = "class";

#if 0
    public static final Set<String> keywords = new HashSet<>();
    public static final Set<String> obsoleteKeywords = new HashSet<>();

    static
    {
        keywords.add(KW_DURABLE_WRITES);
        keywords.add(KW_REPLICATION);
    }
#endif
private:
    std::experimental::optional<sstring> _strategy_class;
public:
    void validate() {
        // Skip validation if the strategy class is already set as it means we've alreayd
        // prepared (and redoing it would set strategyClass back to null, which we don't want)
        if (_strategy_class) {
            return;
        }
#if 0
        validate(keywords, obsoleteKeywords);
#endif
        auto replication_options = get_replication_options();
        if (!replication_options.empty()) {
            _strategy_class = replication_options[REPLICATION_STRATEGY_CLASS_KEY];
            // FIXME
            //replication_options.remove(REPLICATION_STRATEGY_CLASS_KEY);
        }
    }

    std::unordered_map<sstring, sstring> get_replication_options() const {
        auto replication_options = get_map(KW_REPLICATION);
        if (replication_options) {
            return replication_options.value();
        }
        return std::unordered_map<sstring, sstring>{};
    }

    std::experimental::optional<sstring> get_replication_strategy_class() const {
        return _strategy_class;
    }

    lw_shared_ptr<config::ks_meta_data> as_ks_metadata(sstring ks_name) {
        return config::ks_meta_data::new_keyspace(ks_name, get_replication_strategy_class().value(), get_replication_options(), get_boolean(KW_DURABLE_WRITES, true));
    }

#if 0
    public KSMetaData asKSMetadataUpdate(KSMetaData old) throws RequestValidationException
    {
        String sClass = strategyClass;
        Map<String, String> sOptions = getReplicationOptions();
        if (sClass == null)
        {
            sClass = old.strategyClass.getName();
            sOptions = old.strategyOptions;
        }
        return KSMetaData.newKeyspace(old.name, sClass, sOptions, getBoolean(KW_DURABLE_WRITES, old.durableWrites));
    }
#endif
};

}

}
