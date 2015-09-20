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
 * Copyright 2014 Cloudius Systems
 *
 * Modified by Cloudius Systems
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

#include "timestamp.hh"
#include "bytes.hh"
#include "db/consistency_level.hh"
#include "service/query_state.hh"
#include "service/pager/paging_state.hh"
#include "cql3/column_specification.hh"
#include "cql3/column_identifier.hh"
#include "serialization_format.hh"

namespace cql3 {

/**
 * Options for a query.
 */
class query_options {
public:
    // Options that are likely to not be present in most queries
    struct specific_options final {
        static thread_local const specific_options DEFAULT;

        const int32_t page_size;
        const ::shared_ptr<service::pager::paging_state> state;
        const std::experimental::optional<db::consistency_level> serial_consistency;
        const api::timestamp_type timestamp;
    };
private:
    const db::consistency_level _consistency;
    const std::experimental::optional<std::vector<sstring_view>> _names;
    std::vector<bytes_opt> _values;
    std::vector<bytes_view_opt> _value_views;
    mutable std::vector<std::vector<int8_t>> _temporaries;
    const bool _skip_metadata;
    const specific_options _options;
    const int32_t _protocol_version; // transient
    serialization_format _serialization_format;
    std::experimental::optional<std::vector<query_options>> _batch_options;
public:
    query_options(query_options&&) = default;
    query_options(const query_options&) = delete;

    explicit query_options(db::consistency_level consistency,
                           std::experimental::optional<std::vector<sstring_view>> names,
                           std::vector<bytes_opt> values,
                           std::vector<bytes_view_opt> value_views,
                           bool skip_metadata,
                           specific_options options,
                           int32_t protocol_version,
                           serialization_format sf);
    explicit query_options(db::consistency_level consistency,
                           std::experimental::optional<std::vector<sstring_view>> names,
                           std::vector<bytes_view_opt> value_views,
                           bool skip_metadata,
                           specific_options options,
                           int32_t protocol_version,
                           serialization_format sf);

    explicit query_options(db::consistency_level consistency,
                           std::vector<std::vector<bytes_view_opt>> value_views,
                           bool skip_metadata,
                           specific_options options,
                           int32_t protocol_version,
                           serialization_format sf);

    // Batch query_options constructor
    explicit query_options(query_options&&, std::vector<std::vector<bytes_view_opt>> value_views);

    // It can't be const because of prepare()
    static thread_local query_options DEFAULT;

    // forInternalUse
    explicit query_options(std::vector<bytes_opt> values);

    db::consistency_level get_consistency() const;
    bytes_view_opt get_value_at(size_t idx) const;
    bytes_view_opt make_temporary(bytes_opt value) const;
    size_t get_values_count() const;
    bool skip_metadata() const;
    /**  The pageSize for this query. Will be <= 0 if not relevant for the query.  */
    int32_t get_page_size() const;
    /** The paging state for this query, or null if not relevant. */
    ::shared_ptr<service::pager::paging_state> get_paging_state() const;
    /**  Serial consistency for conditional updates. */
    std::experimental::optional<db::consistency_level> get_serial_consistency() const;
    api::timestamp_type get_timestamp(service::query_state& state) const;
    /**
     * The protocol version for the query. Will be 3 if the object don't come from
     * a native protocol request (i.e. it's been allocated locally or by CQL-over-thrift).
     */
    int get_protocol_version() const;
    serialization_format get_serialization_format() const;
    // Mainly for the sake of BatchQueryOptions
    const specific_options& get_specific_options() const;
    const query_options& for_statement(size_t i) const;
    void prepare(const std::vector<::shared_ptr<column_specification>>& specs);
};

}
