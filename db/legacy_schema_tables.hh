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

future<> save_system_keyspace_schema();

future<utils::UUID> calculate_schema_digest(service::storage_proxy& proxy);

future<std::vector<frozen_mutation>> convert_schema_to_mutations(service::storage_proxy& proxy);

future<schema_result::value_type>
read_schema_partition_for_keyspace(service::storage_proxy& proxy, const sstring& schema_table_name, const sstring& keyspace_name);

future<> merge_schema(service::storage_proxy& proxy, std::vector<mutation> mutations);

future<> merge_schema(service::storage_proxy& proxy, std::vector<mutation> mutations, bool do_flush);

future<> do_merge_schema(service::storage_proxy& proxy, std::vector<mutation> mutations, bool do_flush);

future<std::set<sstring>> merge_keyspaces(service::storage_proxy& proxy, schema_result&& before, schema_result&& after);

std::vector<mutation> make_create_keyspace_mutations(lw_shared_ptr<keyspace_metadata> keyspace, api::timestamp_type timestamp, bool with_tables_and_types_and_functions = true);

lw_shared_ptr<keyspace_metadata> create_keyspace_from_schema_partition(const schema_result::value_type& partition);

future<> merge_tables(service::storage_proxy& proxy, schema_result&& before, schema_result&& after);

lw_shared_ptr<keyspace_metadata> create_keyspace_from_schema_partition(const schema_result::value_type& partition);

mutation make_create_keyspace_mutation(lw_shared_ptr<keyspace_metadata> keyspace, api::timestamp_type timestamp, bool with_tables_and_types_and_functions = true);

std::vector<mutation> make_create_table_mutations(lw_shared_ptr<keyspace_metadata> keyspace, schema_ptr table, api::timestamp_type timestamp);

future<std::map<sstring, schema_ptr>> create_tables_from_tables_partition(service::storage_proxy& proxy, const schema_result::mapped_type& result);

void add_table_to_schema_mutation(schema_ptr table, api::timestamp_type timestamp, bool with_columns_and_triggers, const partition_key& pkey, std::vector<mutation>& mutations);
    
future<schema_ptr> create_table_from_name(service::storage_proxy& proxy, const sstring& keyspace, const sstring& table);

future<schema_ptr> create_table_from_table_row(service::storage_proxy& proxy, const query::result_set_row& row);

void create_table_from_table_row_and_column_rows(schema_builder& builder, const query::result_set_row& table_row, const schema_result::mapped_type& serialized_columns);

future<schema_ptr> create_table_from_table_partition(service::storage_proxy& proxy, const lw_shared_ptr<query::result_set>& partition);

std::vector<column_definition> create_columns_from_column_rows(const schema_result::mapped_type& rows,
                                                               const sstring& keyspace,
                                                               const sstring& table,/*,
                                                               AbstractType<?> rawComparator, */
                                                               bool is_super);

column_definition create_column_from_column_row(const query::result_set_row& row,
                                                sstring keyspace,
                                                sstring table, /*,
                                                AbstractType<?> rawComparator, */
                                                bool is_super);


void add_column_to_schema_mutation(schema_ptr table, const column_definition& column, api::timestamp_type timestamp, const partition_key& pkey, std::vector<mutation>& mutations);

sstring serialize_kind(column_kind kind);
column_kind deserialize_kind(sstring kind);
data_type parse_type(sstring str);

} // namespace legacy_schema_tables
} // namespace db
