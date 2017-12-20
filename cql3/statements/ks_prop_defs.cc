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

#include "cql3/statements/ks_prop_defs.hh"

namespace cql3 {

namespace statements {

void ks_prop_defs::validate() {
    // Skip validation if the strategy class is already set as it means we've alreayd
    // prepared (and redoing it would set strategyClass back to null, which we don't want)
    if (_strategy_class) {
        return;
    }

    static std::set<sstring> keywords({ sstring(KW_DURABLE_WRITES), sstring(KW_REPLICATION) });
    property_definitions::validate(keywords);

    auto replication_options = get_replication_options();
    if (replication_options.count(REPLICATION_STRATEGY_CLASS_KEY)) {
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

std::experimental::optional<sstring> ks_prop_defs::get_replication_strategy_class() const {
    return _strategy_class;
}

lw_shared_ptr<keyspace_metadata> ks_prop_defs::as_ks_metadata(sstring ks_name) {
    auto options = get_replication_options();
    options.erase(REPLICATION_STRATEGY_CLASS_KEY);
    return keyspace_metadata::new_keyspace(ks_name, get_replication_strategy_class().value(), options, get_boolean(KW_DURABLE_WRITES, true));
}

lw_shared_ptr<keyspace_metadata> ks_prop_defs::as_ks_metadata_update(lw_shared_ptr<keyspace_metadata> old) {
    auto options = get_replication_options();
    options.erase(REPLICATION_STRATEGY_CLASS_KEY);
    auto sc = get_replication_strategy_class();
    if (!sc) {
        sc = old->strategy_name();
        options = old->strategy_options();
    }
    return keyspace_metadata::new_keyspace(old->name(), *sc, options, get_boolean(KW_DURABLE_WRITES, true));
}


}

}
