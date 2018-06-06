/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
#include <unordered_map>
#include <experimental/optional>
#include "bytes.hh"
#include "types.hh"
#include "transport/messages/result_message_base.hh"

#pragma once

namespace cql3 {

class untyped_result_set_row {
private:
    const std::vector<::shared_ptr<column_specification>> _columns;
    const std::unordered_map<sstring, bytes_opt> _data;
public:
    untyped_result_set_row(const std::unordered_map<sstring, bytes_opt>&);
    untyped_result_set_row(const std::vector<::shared_ptr<column_specification>>&, std::vector<bytes_opt>);
    untyped_result_set_row(untyped_result_set_row&&) = default;
    untyped_result_set_row(const untyped_result_set_row&) = delete;

    bool has(const sstring&) const;
    bytes get_blob(const sstring& name) const {
        return *_data.at(name);
    }
    template<typename T>
    T get_as(const sstring& name) const {
        return value_cast<T>(data_type_for<T>()->deserialize(get_blob(name)));
    }
    template<typename T>
    std::experimental::optional<T> get_opt(const sstring& name) const {
        return has(name) ? get_as<T>(name) : std::experimental::optional<T>{};
    }
    template<typename T>
    T get_or(const sstring& name, T t) const {
        return has(name) ? get_as<T>(name) : t;
    }
    // this could maybe be done as an overload of get_as (or something), but that just
    // muddles things for no real gain. Let user (us) attempt to know what he is doing instead.
    template<typename K, typename V, typename Iter>
    void get_map_data(const sstring& name, Iter out, data_type keytype =
            data_type_for<K>(), data_type valtype =
            data_type_for<V>()) const {
        auto vec =
                value_cast<map_type_impl::native_type>(
                        map_type_impl::get_instance(keytype, valtype, false)->deserialize(
                                get_blob(name)));
        std::transform(vec.begin(), vec.end(), out,
                [](auto& p) {
                    return std::pair<K, V>(value_cast<K>(p.first), value_cast<V>(p.second));
                });
    }
    template<typename K, typename V, typename ... Rest>
    std::unordered_map<K, V, Rest...> get_map(const sstring& name,
            data_type keytype = data_type_for<K>(), data_type valtype =
                    data_type_for<V>()) const {
        std::unordered_map<K, V, Rest...> res;
        get_map_data<K, V>(name, std::inserter(res, res.end()), keytype, valtype);
        return res;
    }
    template<typename V, typename Iter>
    void get_list_data(const sstring& name, Iter out, data_type valtype = data_type_for<V>()) const {
        auto vec =
                value_cast<list_type_impl::native_type>(
                        list_type_impl::get_instance(valtype, false)->deserialize(
                                get_blob(name)));
        std::transform(vec.begin(), vec.end(), out, [](auto& v) { return value_cast<V>(v); });
    }
    template<typename V, typename ... Rest>
    std::vector<V, Rest...> get_list(const sstring& name, data_type valtype = data_type_for<V>()) const {
        std::vector<V, Rest...> res;
        get_list_data<V>(name, std::back_inserter(res), valtype);
        return res;
    }
    template<typename V, typename Iter>
    void get_set_data(const sstring& name, Iter out, data_type valtype =
                    data_type_for<V>()) const {
        auto vec =
                        value_cast<set_type_impl::native_type>(
                                        set_type_impl::get_instance(valtype,
                                                        false)->deserialize(
                                                        get_blob(name)));
        std::transform(vec.begin(), vec.end(), out, [](auto& p) {
            return value_cast<V>(p);
        });
    }
    template<typename V, typename ... Rest>
    std::unordered_set<V, Rest...> get_set(const sstring& name,
            data_type valtype =
                    data_type_for<V>()) const {
        std::unordered_set<V, Rest...> res;
        get_set_data<V>(name, std::inserter(res, res.end()), valtype);
        return res;
    }
    const std::vector<::shared_ptr<column_specification>>& get_columns() const {
        return _columns;
    }
};

class untyped_result_set {
public:
    using row = untyped_result_set_row;
    typedef std::vector<row> rows_type;
    using const_iterator = rows_type::const_iterator;
    using iterator = rows_type::const_iterator;

    untyped_result_set(::shared_ptr<cql_transport::messages::result_message>);
    untyped_result_set(untyped_result_set&&) = default;

    const_iterator begin() const {
        return _rows.begin();
    }
    const_iterator end() const {
        return _rows.end();
    }
    size_t size() const {
        return _rows.size();
    }
    bool empty() const {
        return _rows.empty();
    }
    const row& one() const;
    const row& at(size_t i) const {
        return _rows.at(i);
    }
    const row& front() const {
        return _rows.front();
    }
    const row& back() const {
        return _rows.back();
    }
private:
    rows_type _rows;
    untyped_result_set() = default;
public:
    static untyped_result_set make_empty() {
        return untyped_result_set();
    }
};

}
