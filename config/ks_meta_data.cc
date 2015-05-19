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

#include "database.hh"

ks_meta_data::ks_meta_data(sstring name,
                           sstring strategy_name,
                           std::unordered_map<sstring, sstring> strategy_options,
                           bool durable_writes,
                           std::vector<schema_ptr> cf_defs,
                           shared_ptr<user_types_metadata> user_types)
    : _name{std::move(name)}
    , _strategy_name{strategy_name.empty() ? "NetworkTopologyStrategy" : strategy_name}
    , _strategy_options{std::move(strategy_options)}
    , _durable_writes{durable_writes}
    , _user_types{std::move(user_types)}
{
    for (auto&& s : cf_defs) {
        _cf_meta_data.emplace(s->cf_name(), s);
    }
}

lw_shared_ptr<ks_meta_data>
ks_meta_data::new_keyspace(sstring name,
                           sstring strategy_name,
                           std::unordered_map<sstring, sstring> options,
                           bool durables_writes,
                           std::vector<schema_ptr> cf_defs)
{
    return ::make_lw_shared<ks_meta_data>(name, strategy_name, options, durables_writes, cf_defs);
}
