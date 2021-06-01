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
 * Copyright (C) 2019 ScyllaDB
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
#include "service/paxos/cas_request.hh"
#include "cql3/statements/modification_statement.hh"

namespace service {

class storage_proxy;

} // namespace service

namespace cql3::statements {

using namespace std::chrono;

/**
 * Due to some operation on lists, we can't generate the update that a given Modification statement does before
 * we get the values read by the initial read of Paxos. A RowUpdate thus just store the relevant information
 * (include the statement itself) to generate those updates. We'll have multiple RowUpdate for a Batch, otherwise
 * we'll have only one.
 */
struct cas_row_update {
    modification_statement const& statement;
    std::vector<query::clustering_range> ranges;
    modification_statement::json_cache_opt json_cache;
    // This statement query options. Different from cas_request::query_options,
    // which may stand for BATCH statement, not individual modification_statement,
    // in case of BATCH
    const query_options& options;
};

/**
 * Processed CAS conditions and update on potentially multiple rows of the same partition.
 */
class cas_request: public service::cas_request {
private:
    std::vector<cas_row_update> _updates;
    schema_ptr _schema;
    // A single partition key. Represented as a vector of partition ranges
    // since this is the conventional format for storage_proxy.
    std::vector<dht::partition_range> _key;
    update_parameters::prefetch_data _rows;

public:
    cas_request(schema_ptr schema_arg, std::vector<dht::partition_range> key_arg)
          : _schema(schema_arg)
          , _key(std::move(key_arg))
          , _rows(schema_arg)
    {
        assert(_key.size() == 1 && query::is_single_partition(_key.front()));
    }

    dht::partition_range_vector key() const {
        return dht::partition_range_vector(_key);
    }

    const update_parameters::prefetch_data& rows() const {
        return _rows;
    }

    lw_shared_ptr<query::read_command> read_command(service::storage_proxy& proxy) const;

    void add_row_update(const modification_statement& stmt_arg, std::vector<query::clustering_range> ranges_arg,
        modification_statement::json_cache_opt json_cache_arg, const query_options& options_arg);

    virtual std::optional<mutation> apply(foreign_ptr<lw_shared_ptr<query::result>> qr,
            const query::partition_slice& slice, api::timestamp_type ts) override;

    /// Build a result set with prefetched rows, but return only
    /// the columns required by CAS.
    ///
    /// Each cas_row_update provides a row in the result set.
    /// Rows are ordered the same way as the individual statements appear
    /// in case of batch statement.
    seastar::shared_ptr<cql_transport::messages::result_message>
    build_cas_result_set(seastar::shared_ptr<cql3::metadata> metadata,
            const column_set& mask, bool is_applied) const;

private:
    bool applies_to() const;
    std::optional<mutation> apply_updates(api::timestamp_type t) const;
    /// Find a row in prefetch_data which matches primary key identifying a given `cas_row_update`
    const update_parameters::prefetch_data::row* find_old_row(const cas_row_update& op) const;
};

} // end of namespace "cql3::statements"
