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

#include "config/ks_meta_data.hh"

namespace config {

ks_meta_data::ks_meta_data(sstring name_,
             sstring strategy_name_,
             std::unordered_map<sstring, sstring> strategy_options_,
             bool durable_writes_)
    : ks_meta_data{std::move(name_),
                   std::move(strategy_name_),
                   std::move(strategy_options_),
                   durable_writes_,
                   {}, ::make_shared<ut_meta_data>()}
{ }

ks_meta_data::ks_meta_data(sstring name_,
             sstring strategy_name_,
             std::unordered_map<sstring, sstring> strategy_options_,
             bool durable_writes_,
             std::vector<schema_ptr> cf_defs)
        : ks_meta_data{std::move(name_),
                       std::move(strategy_name_),
                       std::move(strategy_options_),
                       durable_writes_,
                       std::move(cf_defs),
                       ::make_shared<ut_meta_data>()}
{ }

ks_meta_data::ks_meta_data(sstring name_,
                   sstring strategy_name_,
                   std::unordered_map<sstring, sstring> strategy_options_,
                   bool durable_writes_,
                   std::vector<schema_ptr> cf_defs,
                   shared_ptr<ut_meta_data> user_types_)
    : name{std::move(name_)}
    , strategy_name{strategy_name_.empty() ? "NetworkTopologyStrategy" : strategy_name_}
    , strategy_options{std::move(strategy_options_)}
    , durable_writes{durable_writes_}
    , user_types{std::move(user_types_)}
{
    for (auto&& s : cf_defs) {
        _cf_meta_data.emplace(s->cf_name(), s);
    }
}

// For new user created keyspaces (through CQL)
lw_shared_ptr<ks_meta_data> ks_meta_data::new_keyspace(sstring name, sstring strategy_name, std::unordered_map<sstring, sstring> options, bool durable_writes) {
#if 0
    Class<? extends AbstractReplicationStrategy> cls = AbstractReplicationStrategy.getClass(strategyName);
    if (cls.equals(LocalStrategy.class))
        throw new ConfigurationException("Unable to use given strategy class: LocalStrategy is reserved for internal use.");
#endif
    return new_keyspace(name, strategy_name, options, durable_writes, std::vector<schema_ptr>{});
}

lw_shared_ptr<ks_meta_data> ks_meta_data::new_keyspace(sstring name, sstring strategy_name, std::unordered_map<sstring, sstring> options, bool durables_writes, std::vector<schema_ptr> cf_defs)
{
    return ::make_lw_shared<ks_meta_data>(name, strategy_name, options, durables_writes, cf_defs, ::make_shared<ut_meta_data>());
}

}
