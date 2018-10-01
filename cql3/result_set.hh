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

#include "query-result-reader.hh"

#include "result_generator.hh"

#include <seastar/util/gcc6-concepts.hh>

namespace cql3 {

class metadata {
public:
    enum class flag : uint8_t {
        GLOBAL_TABLES_SPEC = 0,
        HAS_MORE_PAGES = 1,
        NO_METADATA = 2,
    };

    using flag_enum = super_enum<flag,
        flag::GLOBAL_TABLES_SPEC,
        flag::HAS_MORE_PAGES,
        flag::NO_METADATA>;

    using flag_enum_set = enum_set<flag_enum>;

    struct column_info {
    // Please note that columnCount can actually be smaller than names, even if names is not null. This is
    // used to include columns in the resultSet that we need to do post-query re-orderings
    // (SelectStatement.orderResults) but that shouldn't be sent to the user as they haven't been requested
    // (CASSANDRA-4911). So the serialization code will exclude any columns in name whose index is >= columnCount.
        std::vector<::shared_ptr<column_specification>> _names;
        uint32_t _column_count;

        column_info(std::vector<::shared_ptr<column_specification>> names, uint32_t column_count)
            : _names(std::move(names))
            , _column_count(column_count)
        { }

        explicit column_info(std::vector<::shared_ptr<column_specification>> names)
            : _names(std::move(names))
            , _column_count(_names.size())
        { }
    };
private:
    flag_enum_set _flags;

private:
    lw_shared_ptr<column_info> _column_info;
    ::shared_ptr<const service::pager::paging_state> _paging_state;

public:
    metadata(std::vector<::shared_ptr<column_specification>> names_);

    metadata(flag_enum_set flags, std::vector<::shared_ptr<column_specification>> names_, uint32_t column_count,
            ::shared_ptr<const service::pager::paging_state> paging_state);

    // The maximum number of values that the ResultSet can hold. This can be bigger than columnCount due to CASSANDRA-4911
    uint32_t value_count() const;

    void add_non_serialized_column(::shared_ptr<column_specification> name);

private:
    bool all_in_same_cf() const;

public:
    void set_paging_state(::shared_ptr<const service::pager::paging_state> paging_state);
    void maybe_set_paging_state(::shared_ptr<const service::pager::paging_state> paging_state);

    void set_skip_metadata();

    flag_enum_set flags() const;

    uint32_t column_count() const { return _column_info->_column_count; }

    ::shared_ptr<const service::pager::paging_state> paging_state() const;

    const std::vector<::shared_ptr<column_specification>>& get_names() const {
        return _column_info->_names;
    }
};

::shared_ptr<const cql3::metadata> make_empty_metadata();

class prepared_metadata {
public:
    enum class flag : uint8_t {
        GLOBAL_TABLES_SPEC = 0,
    };

    using flag_enum = super_enum<flag,
        flag::GLOBAL_TABLES_SPEC>;

    using flag_enum_set = enum_set<flag_enum>;
private:
    flag_enum_set _flags;
    std::vector<::shared_ptr<column_specification>> _names;
    std::vector<uint16_t> _partition_key_bind_indices;
public:
    prepared_metadata(const std::vector<::shared_ptr<column_specification>>& names,
                      const std::vector<uint16_t>& partition_key_bind_indices);

    flag_enum_set flags() const;
    const std::vector<::shared_ptr<column_specification>>& names() const;
    const std::vector<uint16_t>& partition_key_bind_indices() const;
};

GCC6_CONCEPT(

template<typename Visitor>
concept bool ResultVisitor = requires(Visitor& visitor) {
    visitor.start_row();
    visitor.accept_value(std::optional<query::result_bytes_view>());
    visitor.end_row();
};

)

class result_set {
    ::shared_ptr<metadata> _metadata;
    std::deque<std::vector<bytes_opt>> _rows;

    friend class result;
public:
    result_set(std::vector<::shared_ptr<column_specification>> metadata_);

    result_set(::shared_ptr<metadata> metadata);

    size_t size() const;

    bool empty() const;

    void add_row(std::vector<bytes_opt> row);

    void add_column_value(bytes_opt value);

    void reverse();

    void trim(size_t limit);

    template<typename RowComparator>
    void sort(const RowComparator& cmp) {
        std::sort(_rows.begin(), _rows.end(), std::ref(cmp));
    }

    metadata& get_metadata();

    const metadata& get_metadata() const;

    // Returns a range of rows. A row is a range of bytes_opt.
    const std::deque<std::vector<bytes_opt>>& rows() const;

    template<typename Visitor>
    GCC6_CONCEPT(requires ResultVisitor<Visitor>)
    void visit(Visitor&& visitor) const {
        auto column_count = get_metadata().column_count();
        for (auto& row : _rows) {
            visitor.start_row();
            for (auto i = 0u; i < column_count; i++) {
                auto& cell = row[i];
                visitor.accept_value(cell ? std::optional<query::result_bytes_view>(*cell) : std::optional<query::result_bytes_view>());
            }
            visitor.end_row();
        }
    }

    class builder;
};

class result_set::builder {
    result_set _result;
    std::vector<bytes_opt> _current_row;
public:
    explicit builder(shared_ptr<metadata> mtd)
        : _result(std::move(mtd)) { }

    void start_row() { }
    void accept_value(std::optional<query::result_bytes_view> value) {
        if (!value) {
            _current_row.emplace_back();
            return;
        }
        _current_row.emplace_back(value->linearize());
    }
    void end_row() {
        _result.add_row(std::exchange(_current_row, { }));
    }
    result_set get_result_set() && { return std::move(_result); }
};

class result {
    std::unique_ptr<cql3::result_set> _result_set;
    result_generator _result_generator;
    shared_ptr<const cql3::metadata> _metadata;
public:
    explicit result(std::unique_ptr<cql3::result_set> rs)
        : _result_set(std::move(rs))
        , _metadata(_result_set->_metadata)
    { }

    explicit result(result_generator generator, shared_ptr<const metadata> m)
        : _result_generator(std::move(generator))
        , _metadata(std::move(m))
    { }

    const cql3::metadata& get_metadata() const { return *_metadata; }
    cql3::result_set result_set() const {
        if (_result_set) {
            return *_result_set;
        } else {
            auto builder = result_set::builder(make_shared<cql3::metadata>(*_metadata));
            _result_generator.visit(builder);
            return std::move(builder).get_result_set();
        }
    }
    
    template<typename Visitor>
    GCC6_CONCEPT(requires ResultVisitor<Visitor>)
    void visit(Visitor&& visitor) const {
        if (_result_set) {
            _result_set->visit(std::forward<Visitor>(visitor));
        } else {
            _result_generator.visit(std::forward<Visitor>(visitor));
        }
    }
};

}
