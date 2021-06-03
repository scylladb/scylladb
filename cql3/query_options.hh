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
 * Copyright (C) 2014-present ScyllaDB
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

#include <concepts>
#include "timestamp.hh"
#include "bytes.hh"
#include "db/consistency_level_type.hh"
#include "service/query_state.hh"
#include "service/pager/paging_state.hh"
#include "cql3/column_specification.hh"
#include "cql3/column_identifier.hh"
#include "cql3/values.hh"
#include "cql_serialization_format.hh"

namespace cql3 {

class cql_config;
extern const cql_config default_cql_config;

/**
 * Options for a query.
 */
class query_options {
public:
    // Options that are likely to not be present in most queries
    struct specific_options final {
        static thread_local const specific_options DEFAULT;

        const int32_t page_size;
        const lw_shared_ptr<service::pager::paging_state> state;
        const std::optional<db::consistency_level> serial_consistency;
        const api::timestamp_type timestamp;
    };
private:
    const cql_config& _cql_config;
    const db::consistency_level _consistency;
    const std::optional<std::vector<sstring_view>> _names;
    std::vector<cql3::raw_value> _values;
    std::vector<cql3::raw_value_view> _value_views;
    const bool _skip_metadata;
    const specific_options _options;
    cql_serialization_format _cql_serialization_format;
    std::optional<std::vector<query_options>> _batch_options;
    // We must use the same microsecond-precision timestamp for
    // all cells created by an LWT statement or when a statement
    // has a user-provided timestamp. In case the statement or
    // a BATCH appends many values to a list, each value should
    // get a unique and monotonic timeuuid. This sequence is
    // used to make all time-based UUIDs:
    // 1) share the same microsecond,
    // 2) monotonic
    // 3) unique.
    mutable int _list_append_seq = 0;
private:
    /**
     * @brief Batch query_options constructor.
     *
     * Requirements:
     *   - @tparam OneMutationDataRange has a begin() and end() iterators.
     *   - The values of @tparam OneMutationDataRange are of either raw_value_view or raw_value types.
     *
     * @param o Base query_options object. query_options objects for each statement in the batch will derive the values from it.
     * @param values_ranges a vector of values ranges for each statement in the batch.
     */
    template<typename OneMutationDataRange>
    requires requires (OneMutationDataRange range) {
         std::begin(range);
         std::end(range);
    } && ( requires (OneMutationDataRange range) { { *range.begin() } -> std::convertible_to<raw_value_view>; } ||
           requires (OneMutationDataRange range) { { *range.begin() } -> std::convertible_to<raw_value>; } )
    explicit query_options(query_options&& o, std::vector<OneMutationDataRange> values_ranges);

public:
    query_options(query_options&&) = default;
    explicit query_options(const query_options&) = default;

    explicit query_options(const cql_config& cfg,
                           db::consistency_level consistency,
                           std::optional<std::vector<sstring_view>> names,
                           std::vector<cql3::raw_value> values,
                           bool skip_metadata,
                           specific_options options,
                           cql_serialization_format sf);
    explicit query_options(const cql_config& cfg,
                           db::consistency_level consistency,
                           std::optional<std::vector<sstring_view>> names,
                           std::vector<cql3::raw_value> values,
                           std::vector<cql3::raw_value_view> value_views,
                           bool skip_metadata,
                           specific_options options,
                           cql_serialization_format sf);
    explicit query_options(const cql_config& cfg,
                           db::consistency_level consistency,
                           std::optional<std::vector<sstring_view>> names,
                           std::vector<cql3::raw_value_view> value_views,
                           bool skip_metadata,
                           specific_options options,
                           cql_serialization_format sf);

    /**
     * @brief Batch query_options factory.
     *
     * Requirements:
     *   - @tparam OneMutationDataRange has a begin() and end() iterators.
     *   - The values of @tparam OneMutationDataRange are of either raw_value_view or raw_value types.
     *
     * @param o Base query_options object. query_options objects for each statement in the batch will derive the values from it.
     * @param values_ranges a vector of values ranges for each statement in the batch.
     */
    template<typename OneMutationDataRange>
    requires requires (OneMutationDataRange range) {
         std::begin(range);
         std::end(range);
    } && ( requires (OneMutationDataRange range) { { *range.begin() } -> std::convertible_to<raw_value_view>; } ||
           requires (OneMutationDataRange range) { { *range.begin() } -> std::convertible_to<raw_value>; } )
    static query_options make_batch_options(query_options&& o, std::vector<OneMutationDataRange> values_ranges) {
        return query_options(std::move(o), std::move(values_ranges));
    }

    // It can't be const because of prepare()
    static thread_local query_options DEFAULT;

    // forInternalUse
    explicit query_options(std::vector<cql3::raw_value> values);
    explicit query_options(db::consistency_level, std::vector<cql3::raw_value> values, specific_options options = specific_options::DEFAULT);
    explicit query_options(std::unique_ptr<query_options>, lw_shared_ptr<service::pager::paging_state> paging_state);
    explicit query_options(std::unique_ptr<query_options>, lw_shared_ptr<service::pager::paging_state> paging_state, int32_t page_size);

    db::consistency_level get_consistency() const {
        return _consistency;
    }

    cql3::raw_value_view get_value_at(size_t idx) const {
        return _value_views.at(idx);
    }

    size_t get_values_count() const {
        return _value_views.size();
    }

    bool skip_metadata() const {
        return _skip_metadata;
    }

    int32_t get_page_size() const {
        return get_specific_options().page_size;
    }

    /** The paging state for this query, or null if not relevant. */
    lw_shared_ptr<service::pager::paging_state> get_paging_state() const {
        return get_specific_options().state;
    }

    /** Serial consistency for conditional updates. */
    std::optional<db::consistency_level> get_serial_consistency() const {
        return get_specific_options().serial_consistency;
    }

    /**  Return serial consistency for conditional updates. Throws if the consistency is not set. */
    db::consistency_level check_serial_consistency() const;

    api::timestamp_type get_timestamp(service::query_state& state) const {
        auto tstamp = get_specific_options().timestamp;
        return tstamp != api::missing_timestamp ? tstamp : state.get_timestamp();
    }

    /**
     * The protocol version for the query. Will be 3 if the object don't come from
     * a native protocol request (i.e. it's been allocated locally or by CQL-over-thrift).
     */
    int get_protocol_version() const {
        return _cql_serialization_format.protocol_version();
    }

    cql_serialization_format get_cql_serialization_format() const {
        return _cql_serialization_format;
    }

    const query_options::specific_options& get_specific_options() const {
        return _options;
    }

    // Mainly for the sake of BatchQueryOptions
    const query_options& for_statement(size_t i) const {
        if (!_batch_options) {
            // No per-statement options supplied, so use the "global" options
            return *this;
        }
        return _batch_options->at(i);
    }


    const std::optional<std::vector<sstring_view>>& get_names() const noexcept {
        return _names;
    }

    const std::vector<cql3::raw_value_view>& get_values() const noexcept {
        return _value_views;
    }

    const cql_config& get_cql_config() const {
        return _cql_config;
    }

    // Generate a next unique list sequence for list append, e.g.
    // a = a + [val1, val2, ...]
    int next_list_append_seq() const {
        return _list_append_seq++;
    }

    // To preserve prepend monotonicity within a batch, each next
    // value must get a timestamp that's smaller than the previous one:
    // BEGIN BATCH
    //      UPDATE t SET l = [1, 2] + l WHERE pk = 0;
    //      UPDATE t SET l = [3] + l WHERE pk = 0;
    //      UPDATE t SET l = [4] + l WHERE pk = 0;
    // APPLY BATCH
    // SELECT l FROM t WHERE pk = 0;
    //  l
    // ------------
    // [4, 3, 1, 2]
    //
    // This function reserves the given number of prepend entries
    // and returns an id for the first prepended entry (it
    // got to be the smallest one, to preserve the order of
    // a multi-value append).
    //
    // @retval sequence number of the first entry of a multi-value
    // append. To get the next value, add 1.
    int next_list_prepend_seq(int num_entries, int max_entries) const {
        if (_list_append_seq + num_entries < max_entries) {
            _list_append_seq += num_entries;
            return max_entries - _list_append_seq;
        }
        return max_entries;
    }

    void prepare(const std::vector<lw_shared_ptr<column_specification>>& specs);
private:
    void fill_value_views();
};

template<typename OneMutationDataRange>
requires requires (OneMutationDataRange range) {
     std::begin(range);
     std::end(range);
} && ( requires (OneMutationDataRange range) { { *range.begin() } -> std::convertible_to<raw_value_view>; } ||
       requires (OneMutationDataRange range) { { *range.begin() } -> std::convertible_to<raw_value>; } )
query_options::query_options(query_options&& o, std::vector<OneMutationDataRange> values_ranges)
    : query_options(std::move(o))
{
    std::vector<query_options> tmp;
    tmp.reserve(values_ranges.size());
    std::transform(values_ranges.begin(), values_ranges.end(), std::back_inserter(tmp), [this](auto& values_range) {
        return query_options(_cql_config, _consistency, {}, std::move(values_range), _skip_metadata, _options, _cql_serialization_format);
    });
    _batch_options = std::move(tmp);
}

}
