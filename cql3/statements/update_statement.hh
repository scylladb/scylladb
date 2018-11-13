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

#include "cql3/statements/modification_statement.hh"
#include "cql3/statements/raw/modification_statement.hh"
#include "cql3/column_identifier.hh"
#include "cql3/term.hh"

#include "database_fwd.hh"

#include <vector>
#include "unimplemented.hh"

namespace cql3 {

namespace statements {

/**
 * An <code>UPDATE</code> statement parsed from a CQL query statement.
 *
 */
class update_statement : public modification_statement {
public:
#if 0
    private static final Constants.Value EMPTY = new Constants.Value(ByteBufferUtil.EMPTY_BYTE_BUFFER);
#endif

    update_statement(statement_type type, uint32_t bound_terms, schema_ptr s, std::unique_ptr<attributes> attrs, uint64_t* cql_stats_counter_ptr);
private:
    virtual bool require_full_clustering_key() const override;

    virtual bool allow_clustering_key_slices() const override;

    virtual void add_update_for_key(mutation& m, const query::clustering_range& range, const update_parameters& params, const json_cache_opt& json_cache) override;

    virtual void execute_operations_for_key(mutation& m, const clustering_key_prefix& prefix, const update_parameters& params, const json_cache_opt& json_cache);
};

/*
 * Update statement specification that has specifically one bound name - a JSON string.
 * Overridden add_update_for_key uses this parsed JSON to look up values for columns.
 */
class insert_prepared_json_statement : public update_statement {
    ::shared_ptr<term> _term;
    bool _default_unset;
public:
    insert_prepared_json_statement(uint32_t bound_terms, schema_ptr s, std::unique_ptr<attributes> attrs, uint64_t* cql_stats_counter_ptr, ::shared_ptr<term> t, bool default_unset)
        : update_statement(statement_type::INSERT, bound_terms, s, std::move(attrs), cql_stats_counter_ptr), _term(t), _default_unset(default_unset) {
        _restrictions = ::make_shared<restrictions::statement_restrictions>(s, false);
    }
private:
    virtual void execute_operations_for_key(mutation& m, const clustering_key_prefix& prefix, const update_parameters& params, const json_cache_opt& json_cache) override;

    virtual dht::partition_range_vector build_partition_keys(const query_options& options, const json_cache_opt& json_cache) override;

    virtual query::clustering_row_ranges create_clustering_ranges(const query_options& options, const json_cache_opt& json_cache) override;

    json_cache_opt maybe_prepare_json_cache(const query_options& options) override;

    void execute_set_value(mutation& m, const clustering_key_prefix& prefix, const update_parameters& params, const column_definition& column, const bytes_opt& value);
};

}

}
