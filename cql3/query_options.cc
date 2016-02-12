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

#include "query_options.hh"
#include "version.hh"

namespace cql3 {

thread_local const query_options::specific_options query_options::specific_options::DEFAULT{-1, {}, {}, api::missing_timestamp};

thread_local query_options query_options::DEFAULT{db::consistency_level::ONE, std::experimental::nullopt,
    {}, false, query_options::specific_options::DEFAULT, version::native_protocol(), cql_serialization_format::use_32_bit()};

query_options::query_options(db::consistency_level consistency,
                             std::experimental::optional<std::vector<sstring_view>> names,
                             std::vector<bytes_opt> values,
                             std::vector<bytes_view_opt> value_views,
                             bool skip_metadata,
                             specific_options options,
                             int32_t protocol_version,
                             cql_serialization_format sf)
    : _consistency(consistency)
    , _names(std::move(names))
    , _values(std::move(values))
    , _value_views(std::move(value_views))
    , _skip_metadata(skip_metadata)
    , _options(std::move(options))
    , _protocol_version(protocol_version)
    , _cql_serialization_format(sf)
{
}

query_options::query_options(db::consistency_level consistency,
                             std::experimental::optional<std::vector<sstring_view>> names,
                             std::vector<bytes_view_opt> value_views,
                             bool skip_metadata,
                             specific_options options,
                             int32_t protocol_version,
                             cql_serialization_format sf)
    : query_options(
          consistency,
          std::move(names),
          {},
          std::move(value_views),
          skip_metadata,
          std::move(options),
          protocol_version,
          sf
      )
{
}

query_options::query_options(query_options&& o, std::vector<std::vector<bytes_view_opt>> value_views)
    : query_options(std::move(o))
{
    std::vector<query_options> tmp;
    tmp.reserve(value_views.size());
    std::transform(value_views.begin(), value_views.end(), std::back_inserter(tmp), [this](auto& vals) {
        return query_options(_consistency, {}, vals, _skip_metadata, _options, _protocol_version, _cql_serialization_format);
    });
    _batch_options = std::move(tmp);
}

query_options::query_options(db::consistency_level cl, std::vector<bytes_opt> values)
    : query_options(
          cl,
          {},
          std::move(values),
          {},
          false,
          query_options::specific_options::DEFAULT,
          version::native_protocol(),
          cql_serialization_format::use_32_bit()
      )
{
    for (auto&& value : _values) {
        if (value) {
            _value_views.emplace_back(bytes_view{*value});
        } else {
            _value_views.emplace_back(std::experimental::nullopt);
        }
    }
}

query_options::query_options(std::vector<bytes_opt> values)
    : query_options(
          db::consistency_level::ONE, std::move(values))
{}

db::consistency_level query_options::get_consistency() const
{
    return _consistency;
}

bytes_view_opt query_options::get_value_at(size_t idx) const
{
    return _value_views.at(idx);
}

size_t query_options::get_values_count() const
{
    return _value_views.size();
}

bytes_view_opt query_options::make_temporary(bytes_opt value) const
{
    if (value) {
        _temporaries.emplace_back(value->begin(), value->end());
        auto& temporary = _temporaries.back();
        return bytes_view{temporary.data(), temporary.size()};
    }
    return std::experimental::nullopt;
}

bool query_options::skip_metadata() const
{
    return _skip_metadata;
}

int32_t query_options::get_page_size() const
{
    return get_specific_options().page_size;
}

::shared_ptr<service::pager::paging_state> query_options::get_paging_state() const
{
    return get_specific_options().state;
}

std::experimental::optional<db::consistency_level> query_options::get_serial_consistency() const
{
    return get_specific_options().serial_consistency;
}

api::timestamp_type query_options::get_timestamp(service::query_state& state) const
{
    auto tstamp = get_specific_options().timestamp;
    return tstamp != api::missing_timestamp ? tstamp : state.get_timestamp();
}

int query_options::get_protocol_version() const
{
    return _protocol_version;
}

cql_serialization_format query_options::get_cql_serialization_format() const
{
    return _cql_serialization_format;
}

const query_options::specific_options& query_options::get_specific_options() const
{
    return _options;
}

const query_options& query_options::for_statement(size_t i) const
{
    if (!_batch_options) {
        // No per-statement options supplied, so use the "global" options
        return *this;
    }
    return _batch_options->at(i);
}

void query_options::prepare(const std::vector<::shared_ptr<column_specification>>& specs)
{
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

}
