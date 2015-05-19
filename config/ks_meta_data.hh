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

#include "schema.hh"

#include "core/shared_ptr.hh"

#include <unordered_map>

class user_types_metadata;

namespace config {

class ks_meta_data final {
public:
    const sstring name;
    const sstring strategy_name;
    const std::unordered_map<sstring, sstring> strategy_options;
private:
    std::unordered_map<sstring, schema_ptr> _cf_meta_data;
public:
    const bool durable_writes;

    const ::shared_ptr<user_types_metadata> user_types;

    ks_meta_data(sstring name_,
                 sstring strategy_name_,
                 std::unordered_map<sstring, sstring> strategy_options_,
                 bool durable_writes_,
                 std::vector<schema_ptr> cf_defs = std::vector<schema_ptr>{},
                 shared_ptr<user_types_metadata> user_types_ = ::make_shared<user_types_metadata>());

    static lw_shared_ptr<ks_meta_data>
    new_keyspace(sstring name,
                 sstring strategy_name,
                 std::unordered_map<sstring, sstring> options,
                 bool durables_writes,
                 std::vector<schema_ptr> cf_defs = std::vector<schema_ptr>{});

    const std::unordered_map<sstring, schema_ptr>& cf_meta_data() const {
        return _cf_meta_data;
    }
};

}
