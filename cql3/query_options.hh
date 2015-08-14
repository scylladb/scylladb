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
    const std::experimental::optional<std::vector<sstring>> _names;
    std::vector<bytes_opt> _values;
    const bool _skip_metadata;
    const specific_options _options;
    const int32_t _protocol_version; // transient
    serialization_format _serialization_format;
    std::experimental::optional<std::vector<query_options>> _batch_options;
public:
    explicit query_options(db::consistency_level consistency,
            std::experimental::optional<std::vector<sstring>> names, std::vector<bytes_opt> values,
            bool skip_metadata, specific_options options, int32_t protocol_version, serialization_format sf)
            : _consistency(consistency)
            , _names(std::move(names))
            , _values(std::move(values))
            , _skip_metadata(skip_metadata)
            , _options(std::move(options))
            , _protocol_version(protocol_version)
            , _serialization_format(sf)
        { }

    // It can't be const because of prepare()
    static thread_local query_options DEFAULT;

    // forInternalUse
    explicit query_options(std::vector<bytes_opt> values);

    db::consistency_level get_consistency() const {
        return _consistency;
    }
    const bytes_opt& get_value_at(size_t idx) const {
        return _values.at(idx);
    }
    size_t get_values_count() const {
        return _values.size();
    }
    bool skip_metadata() const {
        return _skip_metadata;
    }

    /**  The pageSize for this query. Will be <= 0 if not relevant for the query.  */
    int32_t get_page_size() const { return get_specific_options().page_size; }

    /** The paging state for this query, or null if not relevant. */
    ::shared_ptr<service::pager::paging_state> get_paging_state() const {
        return get_specific_options().state;
    }

    /**  Serial consistency for conditional updates. */
    std::experimental::optional<db::consistency_level> get_serial_consistency() const {
        return get_specific_options().serial_consistency;
    }

    api::timestamp_type get_timestamp(service::query_state& state) const {
        auto tstamp = get_specific_options().timestamp;
        return tstamp != api::missing_timestamp ? tstamp : state.get_timestamp();
    }

    /**
     * The protocol version for the query. Will be 3 if the object don't come from
     * a native protocol request (i.e. it's been allocated locally or by CQL-over-thrift).
     */
    int get_protocol_version() const {
        return _protocol_version;
    }
    serialization_format get_serialization_format() const { return _serialization_format; }

    // Mainly for the sake of BatchQueryOptions
    const specific_options& get_specific_options() const {
        return _options;
    }

    const query_options& for_statement(size_t i) const {
        if (!_batch_options) {
            // No per-statement options supplied, so use the "global" options
            return *this;
        }
        return _batch_options->at(i);
    }

    void prepare(const std::vector<::shared_ptr<column_specification>>& specs) {
        if (!_names) {
            return;
        }

        auto& names = *_names;
        std::vector<bytes_opt> ordered_values;
        ordered_values.reserve(specs.size());
        for (auto&& spec : specs) {
            auto& spec_name = spec->name->text();
            for (size_t j = 0; j < names.size(); j++) {
                if (names[j] == spec_name) {
                    ordered_values.emplace_back(_values[j]);
                    break;
                }
            }
        }
        _values = std::move(ordered_values);
    }
};

}
