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

#include <deque>
#include <vector>
#include "enum_set.hh"
#include "service/pager/paging_state.hh"
#include "schema.hh"

namespace cql3 {

class metadata {
public:
    enum class flag : uint8_t {
        GLOBAL_TABLES_SPEC,
        HAS_MORE_PAGES,
        NO_METADATA
    };

    using flag_enum = super_enum<flag,
        flag::GLOBAL_TABLES_SPEC,
        flag::HAS_MORE_PAGES,
        flag::NO_METADATA>;

    using flag_enum_set = enum_set<flag_enum>;

private:
    flag_enum_set _flags;

public:
    // Please note that columnCount can actually be smaller than names, even if names is not null. This is
    // used to include columns in the resultSet that we need to do post-query re-orderings
    // (SelectStatement.orderResults) but that shouldn't be sent to the user as they haven't been requested
    // (CASSANDRA-4911). So the serialization code will exclude any columns in name whose index is >= columnCount.
    std::vector<::shared_ptr<column_specification>> names;

private:
    const uint32_t _column_count;
    ::shared_ptr<const service::pager::paging_state> _paging_state;

public:
    metadata(std::vector<::shared_ptr<column_specification>> names_)
        : metadata(flag_enum_set(), std::move(names_), names_.size(), {})
    { }

    metadata(flag_enum_set flags, std::vector<::shared_ptr<column_specification>> names_, uint32_t column_count,
            ::shared_ptr<const service::pager::paging_state> paging_state)
        : _flags(flags)
        , names(std::move(names_))
        , _column_count(column_count)
        , _paging_state(std::move(paging_state))
    { }

    // The maximum number of values that the ResultSet can hold. This can be bigger than columnCount due to CASSANDRA-4911
    uint32_t value_count() {
        return _flags.contains<flag::NO_METADATA>() ? _column_count : names.size();
    }

    void add_non_serialized_column(::shared_ptr<column_specification> name) {
        // See comment above. Because columnCount doesn't account the newly added name, it
        // won't be serialized.
        names.emplace_back(std::move(name));
    }

private:
    bool all_in_same_cf() const {
        if (_flags.contains<flag::NO_METADATA>()) {
            return false;
        }

        assert(!names.empty());

        auto first = names.front();
        return std::all_of(std::next(names.begin()), names.end(), [first] (auto&& spec) {
            return spec->ks_name == first->ks_name && spec->cf_name == first->cf_name;
        });
    }

public:
    void set_has_more_pages(::shared_ptr<const service::pager::paging_state> paging_state) {
        if (!paging_state) {
            return;
        }

        _flags.set<flag::HAS_MORE_PAGES>();
        _paging_state = std::move(paging_state);
    }

    void set_skip_metadata() {
        _flags.set<flag::NO_METADATA>();
    }

    flag_enum_set flags() const {
        return _flags;
    }

    uint32_t column_count() const {
        return _column_count;
    }

    auto paging_state() const {
        return _paging_state;
    }

    auto const& get_names() const {
        return names;
    }
};

inline ::shared_ptr<cql3::metadata> make_empty_metadata()
{
    auto result = ::make_shared<cql3::metadata>(std::vector<::shared_ptr<cql3::column_specification>>{});
    result->set_skip_metadata();
    return result;
}

class result_set {
public:
    ::shared_ptr<metadata> _metadata;
    std::deque<std::vector<bytes_opt>> _rows;
public:
    result_set(std::vector<::shared_ptr<column_specification>> metadata_)
        : _metadata(::make_shared<metadata>(std::move(metadata_)))
    { }

    result_set(::shared_ptr<metadata> metadata)
        : _metadata(std::move(metadata))
    { }

    size_t size() const {
        return _rows.size();
    }

    bool empty() const {
        return _rows.empty();
    }

    void add_row(std::vector<bytes_opt> row) {
        assert(row.size() == _metadata->value_count());
        _rows.emplace_back(std::move(row));
    }

    void add_column_value(bytes_opt value) {
        if (_rows.empty() || _rows.back().size() == _metadata->value_count()) {
            std::vector<bytes_opt> row;
            row.reserve(_metadata->value_count());
            _rows.emplace_back(std::move(row));
        }

        _rows.back().emplace_back(std::move(value));
    }

    void reverse() {
        std::reverse(_rows.begin(), _rows.end());
    }

    void trim(size_t limit) {
        if (_rows.size() > limit) {
            _rows.resize(limit);
        }
    }

    template<typename RowComparator>
    void sort(RowComparator&& cmp) {
        std::sort(_rows.begin(), _rows.end(), std::forward<RowComparator>(cmp));
    }

    metadata& get_metadata() {
        return *_metadata;
    }

    const metadata& get_metadata() const {
        return *_metadata;
    }

    // Returns a range of rows. A row is a range of bytes_opt.
    auto const& rows() const {
        return _rows;
    }
};

}
