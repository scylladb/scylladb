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
#include "mutation.hh"
#include "schema.hh"

#include <vector>
#include <map>

namespace query {
class result_set;
}

/** system.schema_* tables used to store keyspace/table/type attributes prior to C* 3.0 */
namespace db {
namespace legacy_schema_tables {

using schema_result = std::map<sstring, lw_shared_ptr<query::result_set>>;

static constexpr auto KEYSPACES = "schema_keyspaces";
static constexpr auto COLUMNFAMILIES = "schema_columnfamilies";
static constexpr auto COLUMNS = "schema_columns";
static constexpr auto TRIGGERS = "schema_triggers";
static constexpr auto USERTYPES = "schema_usertypes";
static constexpr auto FUNCTIONS = "schema_functions";
static constexpr auto AGGREGATES = "schema_aggregates";

extern std::vector<const char*> ALL;

std::vector<schema_ptr> all_tables();

future<schema_result::value_type>
read_schema_partition_for_keyspace(service::storage_proxy& proxy, const sstring& schema_table_name, const sstring& keyspace_name);

future<> merge_schema(service::storage_proxy& proxy, std::vector<mutation> mutations);

future<> merge_schema(service::storage_proxy& proxy, std::vector<mutation> mutations, bool do_flush);

future<std::set<sstring>> merge_keyspaces(service::storage_proxy& proxy, schema_result&& before, schema_result&& after);

std::vector<mutation> make_create_keyspace_mutations(lw_shared_ptr<keyspace_metadata> keyspace, api::timestamp_type timestamp, bool with_tables_and_types_and_functions = true);

lw_shared_ptr<keyspace_metadata> create_keyspace_from_schema_partition(const schema_result::value_type& partition);

mutation make_create_keyspace_mutation(lw_shared_ptr<keyspace_metadata> keyspace, api::timestamp_type timestamp, bool with_tables_and_types_and_functions = true);

std::vector<mutation> make_create_table_mutations(lw_shared_ptr<keyspace_metadata> keyspace, schema_ptr table, api::timestamp_type timestamp);

void add_table_to_schema_mutation(schema_ptr table, api::timestamp_type timestamp, bool with_columns_and_triggers, const partition_key& pkey, std::vector<mutation>& mutations);

} // namespace legacy_schema_tables
} // namespace db
