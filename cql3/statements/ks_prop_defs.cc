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

#include "cql3/statements/ks_prop_defs.hh"
#include "database.hh"

namespace cql3 {

namespace statements {

static std::map<sstring, sstring> prepare_options(
        const sstring& strategy_class,
        const locator::token_metadata& tm,
        std::map<sstring, sstring> options,
        const std::map<sstring, sstring>& old_options = {}) {
    options.erase(ks_prop_defs::REPLICATION_STRATEGY_CLASS_KEY);

    if (strategy_class != "NetworkTopologyStrategy") {
        return options;
    }

    // For users' convenience, expand the 'replication_factor' option into a replication factor for each DC.
    // If the user simply switches from another strategy without providing any options,
    // but the other strategy used the 'replication_factor' option, it will also be expanded.
    // See issue CASSANDRA-14303.

    sstring rf;
    auto it = options.find(ks_prop_defs::REPLICATION_FACTOR_KEY);
    if (it != options.end()) {
        // Expand: the user explicitly provided a 'replication_factor'.
        rf = it->second;
        options.erase(it);
    } else if (options.empty()) {
        auto it = old_options.find(ks_prop_defs::REPLICATION_FACTOR_KEY);
        if (it != old_options.end()) {
            // Expand: the user switched from another strategy that specified a 'replication_factor'
            // and didn't provide any additional options.
            rf = it->second;
        }
    }

    if (!rf.empty()) {
        // We keep previously specified DC factors for safety.
        for (const auto& opt : old_options) {
            if (opt.first != ks_prop_defs::REPLICATION_FACTOR_KEY) {
                options.insert(opt);
            }
        }

        for (const auto& dc : tm.get_topology().get_datacenter_endpoints()) {
            options.emplace(dc.first, rf);
        }
    }

    return options;
}

void ks_prop_defs::validate() {
    // Skip validation if the strategy class is already set as it means we've alreayd
    // prepared (and redoing it would set strategyClass back to null, which we don't want)
    if (_strategy_class) {
        return;
    }

    static std::set<sstring> keywords({ sstring(KW_DURABLE_WRITES), sstring(KW_REPLICATION) });
    property_definitions::validate(keywords);

    auto replication_options = get_replication_options();
    if (replication_options.contains(REPLICATION_STRATEGY_CLASS_KEY)) {
        _strategy_class = replication_options[REPLICATION_STRATEGY_CLASS_KEY];
    }
}

std::map<sstring, sstring> ks_prop_defs::get_replication_options() const {
    auto replication_options = get_map(KW_REPLICATION);
    if (replication_options) {
        return replication_options.value();
    }
    return std::map<sstring, sstring>{};
}

std::optional<sstring> ks_prop_defs::get_replication_strategy_class() const {
    return _strategy_class;
}

lw_shared_ptr<keyspace_metadata> ks_prop_defs::as_ks_metadata(sstring ks_name, const locator::token_metadata& tm) {
    auto sc = get_replication_strategy_class().value();
    return keyspace_metadata::new_keyspace(ks_name, sc,
            prepare_options(sc, tm, get_replication_options()), get_boolean(KW_DURABLE_WRITES, true));
}

lw_shared_ptr<keyspace_metadata> ks_prop_defs::as_ks_metadata_update(lw_shared_ptr<keyspace_metadata> old, const locator::token_metadata& tm) {
    std::map<sstring, sstring> options;
    const auto& old_options = old->strategy_options();
    auto sc = get_replication_strategy_class();
    if (sc) {
        options = prepare_options(*sc, tm, get_replication_options(), old_options);
    } else {
        sc = old->strategy_name();
        options = old_options;
    }

    return keyspace_metadata::new_keyspace(old->name(), *sc, options, get_boolean(KW_DURABLE_WRITES, true));
}


}

}
