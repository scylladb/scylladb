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

#include "cql3/result_set.hh"

namespace cql3 {

metadata::metadata(std::vector<lw_shared_ptr<column_specification>> names_)
        : _flags(flag_enum_set())
        , _column_info(make_lw_shared<column_info>(std::move(names_)))
{ }

metadata::metadata(flag_enum_set flags, std::vector<lw_shared_ptr<column_specification>> names_, uint32_t column_count,
        lw_shared_ptr<const service::pager::paging_state> paging_state)
    : _flags(flags)
    , _column_info(make_lw_shared<column_info>(std::move(names_), column_count))
    , _paging_state(std::move(paging_state))
{ }

// The maximum number of values that the ResultSet can hold. This can be bigger than columnCount due to CASSANDRA-4911
uint32_t metadata::value_count() const {
    return _flags.contains<flag::NO_METADATA>() ? _column_info->_column_count : _column_info->_names.size();
}

void metadata::add_non_serialized_column(lw_shared_ptr<column_specification> name) {
    // See comment above. Because columnCount doesn't account the newly added name, it
    // won't be serialized.
    _column_info->_names.emplace_back(std::move(name));
}

bool metadata::all_in_same_cf() const {
    if (_flags.contains<flag::NO_METADATA>()) {
        return false;
    }

    return column_specification::all_in_same_table(_column_info->_names);
}

void metadata::set_paging_state(lw_shared_ptr<const service::pager::paging_state> paging_state) {
    _flags.set<flag::HAS_MORE_PAGES>();
    _paging_state = std::move(paging_state);
}

void metadata::maybe_set_paging_state(lw_shared_ptr<const service::pager::paging_state> paging_state) {
    assert(paging_state);
    if (paging_state->get_remaining() > 0) {
        set_paging_state(std::move(paging_state));
    } else {
        _flags.remove<flag::HAS_MORE_PAGES>();
        _paging_state = nullptr;
    }
}

void metadata::set_skip_metadata() {
    _flags.set<flag::NO_METADATA>();
}

metadata::flag_enum_set metadata::flags() const {
    return _flags;
}

lw_shared_ptr<const service::pager::paging_state> metadata::paging_state() const {
    return _paging_state;
}

prepared_metadata::prepared_metadata(const std::vector<lw_shared_ptr<column_specification>>& names,
                                     const std::vector<uint16_t>& partition_key_bind_indices,
                                     bool is_conditional)
    : _names{names}
    , _partition_key_bind_indices{partition_key_bind_indices}
{
    if (!names.empty() && column_specification::all_in_same_table(_names)) {
        _flags.set<flag::GLOBAL_TABLES_SPEC>();
    }

    if (is_conditional) {
        _flags.set<flag::LWT>();
    }
}

prepared_metadata::flag_enum_set prepared_metadata::flags() const {
    return _flags;
}

const std::vector<lw_shared_ptr<column_specification>>& prepared_metadata::names() const {
    return _names;
}

const std::vector<uint16_t>& prepared_metadata::partition_key_bind_indices() const {
    return _partition_key_bind_indices;
}

result_set::result_set(std::vector<lw_shared_ptr<column_specification>> metadata_)
    : _metadata(::make_shared<metadata>(std::move(metadata_)))
{ }

result_set::result_set(::shared_ptr<metadata> metadata)
    : _metadata(std::move(metadata))
{ }

size_t result_set::size() const {
    return _rows.size();
}

bool result_set::empty() const {
    return _rows.empty();
}

void result_set::add_row(std::vector<bytes_opt> row) {
    assert(row.size() == _metadata->value_count());
    _rows.emplace_back(std::move(row));
}

void result_set::add_column_value(bytes_opt value) {
    if (_rows.empty() || _rows.back().size() == _metadata->value_count()) {
        std::vector<bytes_opt> row;
        row.reserve(_metadata->value_count());
        _rows.emplace_back(std::move(row));
    }

    _rows.back().emplace_back(std::move(value));
}

void result_set::reverse() {
    std::reverse(_rows.begin(), _rows.end());
}

void result_set::trim(size_t limit) {
    if (_rows.size() > limit) {
        _rows.resize(limit);
    }
}

metadata& result_set::get_metadata() {
    return *_metadata;
}

const metadata& result_set::get_metadata() const {
    return *_metadata;
}

const utils::chunked_vector<std::vector<bytes_opt>>& result_set::rows() const {
    return _rows;
}

shared_ptr<const cql3::metadata>
make_empty_metadata() {
    static thread_local shared_ptr<const metadata> empty_metadata_cache = [] {
        auto result = ::make_shared<metadata>(std::vector<lw_shared_ptr<cql3::column_specification>>{});
        result->set_skip_metadata();
        return result;
    }();
    return empty_metadata_cache;
}

}
