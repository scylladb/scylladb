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
 * Modified by Cloudius Systems
 * Copyright 2015 Cloudius Systems
 */

#pragma once

#include "service/storage_proxy.hh"
#include "config/ks_meta_data.hh"
#include "mutation.hh"
#include "schema.hh"

#include <vector>

/** system.schema_* tables used to store keyspace/table/type attributes prior to C* 3.0 */
namespace db {
namespace legacy_schema_tables {

static constexpr auto KEYSPACES = "schema_keyspaces";
static constexpr auto COLUMNFAMILIES = "schema_columnfamilies";
static constexpr auto COLUMNS = "schema_columns";
static constexpr auto TRIGGERS = "schema_triggers";
static constexpr auto USERTYPES = "schema_usertypes";
static constexpr auto FUNCTIONS = "schema_functions";
static constexpr auto AGGREGATES = "schema_aggregates";

extern std::vector<const char*> ALL;

std::vector<schema_ptr> all_tables();

future<> merge_schema(service::storage_proxy& proxy, std::vector<mutation> mutations);

future<> merge_schema(service::storage_proxy& proxy, std::vector<mutation> mutations, bool do_flush);

mutation make_create_keyspace_mutation(lw_shared_ptr<config::ks_meta_data> keyspace, api::timestamp_type timestamp, bool with_tables_and_types_and_functions = true);

void add_table_to_schema_mutation(schema_ptr table, api::timestamp_type timestamp, bool with_columns_and_triggers, mutation& m);

} // namespace legacy_schema_tables
} // namespace db
