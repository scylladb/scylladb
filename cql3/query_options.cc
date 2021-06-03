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
 * Copyright (C) 2015-present ScyllaDB
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

#include "cql3/cql_config.hh"
#include "query_options.hh"
#include "version.hh"
#include "db/consistency_level_type.hh"

namespace cql3 {

const cql_config default_cql_config;

thread_local const query_options::specific_options query_options::specific_options::DEFAULT{
    -1, {}, db::consistency_level::SERIAL, api::missing_timestamp};

thread_local query_options query_options::DEFAULT{default_cql_config,
    db::consistency_level::ONE, std::nullopt,
    std::vector<cql3::raw_value_view>(), false, query_options::specific_options::DEFAULT, cql_serialization_format::latest()};

query_options::query_options(const cql_config& cfg,
                           db::consistency_level consistency,
                           std::optional<std::vector<sstring_view>> names,
                           std::vector<cql3::raw_value> values,
                           std::vector<cql3::raw_value_view> value_views,
                           bool skip_metadata,
                           specific_options options,
                           cql_serialization_format sf)
   : _cql_config(cfg)
   , _consistency(consistency)
   , _names(std::move(names))
   , _values(std::move(values))
   , _value_views(value_views)
   , _skip_metadata(skip_metadata)
   , _options(std::move(options))
   , _cql_serialization_format(sf)
{
}

query_options::query_options(const cql_config& cfg,
                             db::consistency_level consistency,
                             std::optional<std::vector<sstring_view>> names,
                             std::vector<cql3::raw_value> values,
                             bool skip_metadata,
                             specific_options options,
                             cql_serialization_format sf)
    : _cql_config(cfg)
    , _consistency(consistency)
    , _names(std::move(names))
    , _values(std::move(values))
    , _value_views()
    , _skip_metadata(skip_metadata)
    , _options(std::move(options))
    , _cql_serialization_format(sf)
{
    fill_value_views();
}

query_options::query_options(const cql_config& cfg,
                             db::consistency_level consistency,
                             std::optional<std::vector<sstring_view>> names,
                             std::vector<cql3::raw_value_view> value_views,
                             bool skip_metadata,
                             specific_options options,
                             cql_serialization_format sf)
    : _cql_config(cfg)
    , _consistency(consistency)
    , _names(std::move(names))
    , _values()
    , _value_views(std::move(value_views))
    , _skip_metadata(skip_metadata)
    , _options(std::move(options))
    , _cql_serialization_format(sf)
{
}

query_options::query_options(db::consistency_level cl, std::vector<cql3::raw_value> values,
        specific_options options)
    : query_options(
          default_cql_config,
          cl,
          {},
          std::move(values),
          false,
          std::move(options),
          cql_serialization_format::latest()
      )
{
}

query_options::query_options(std::unique_ptr<query_options> qo, lw_shared_ptr<service::pager::paging_state> paging_state)
        : query_options(qo->_cql_config,
        qo->_consistency,
        std::move(qo->_names),
        std::move(qo->_values),
        std::move(qo->_value_views),
        qo->_skip_metadata,
        std::move(query_options::specific_options{qo->_options.page_size, paging_state, qo->_options.serial_consistency, qo->_options.timestamp}),
        qo->_cql_serialization_format) {

}

query_options::query_options(std::unique_ptr<query_options> qo, lw_shared_ptr<service::pager::paging_state> paging_state, int32_t page_size)
        : query_options(qo->_cql_config,
        qo->_consistency,
        std::move(qo->_names),
        std::move(qo->_values),
        std::move(qo->_value_views),
        qo->_skip_metadata,
        std::move(query_options::specific_options{page_size, paging_state, qo->_options.serial_consistency, qo->_options.timestamp}),
        qo->_cql_serialization_format) {

}

query_options::query_options(std::vector<cql3::raw_value> values)
    : query_options(
          db::consistency_level::ONE, std::move(values))
{}

void query_options::prepare(const std::vector<lw_shared_ptr<column_specification>>& specs)
{
    if (!_names) {
        return;
    }

    auto& names = *_names;
    std::vector<cql3::raw_value_view> ordered_values;
    ordered_values.reserve(specs.size());
    for (auto&& spec : specs) {
        auto& spec_name = spec->name->text();
        for (size_t j = 0; j < names.size(); j++) {
            if (names[j] == spec_name) {
                ordered_values.emplace_back(_value_views[j]);
                break;
            }
        }
    }
    _value_views = std::move(ordered_values);
}

void query_options::fill_value_views()
{
    for (auto&& value : _values) {
        _value_views.emplace_back(value.to_view());
    }
}

db::consistency_level query_options::check_serial_consistency() const {

    if (_options.serial_consistency.has_value()) {
        return *_options.serial_consistency;
    }
    throw exceptions::protocol_exception("Consistency level for LWT is missing for a request with conditions");
}

}
