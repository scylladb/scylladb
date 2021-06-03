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

#pragma once

#include <seastar/core/shared_ptr.hh>
#include "cql3/column_identifier.hh"
#include <variant>

namespace cql3 {

namespace statements {

struct index_target {
    static const sstring target_option_name;
    static const sstring custom_index_option_name;

    using single_column =::shared_ptr<column_identifier>;
    using multiple_columns = std::vector<::shared_ptr<column_identifier>>;
    using value_type = std::variant<single_column, multiple_columns>;

    enum class target_type {
        values, keys, keys_and_values, full
    };

    const value_type value;
    const target_type type;

    index_target(::shared_ptr<column_identifier> c, target_type t) : value(c) , type(t) {}
    index_target(std::vector<::shared_ptr<column_identifier>> c, target_type t) : value(std::move(c)), type(t) {}

    sstring as_string() const;

    static sstring index_option(target_type type);
    static target_type from_column_definition(const column_definition& cd);
    static index_target::target_type from_sstring(const sstring& s);

    class raw {
    public:
        using single_column = ::shared_ptr<column_identifier::raw>;
        using multiple_columns = std::vector<::shared_ptr<column_identifier::raw>>;
        using value_type = std::variant<single_column, multiple_columns>;

        const value_type value;
        const target_type type;

        raw(::shared_ptr<column_identifier::raw> c, target_type t) : value(c), type(t) {}
        raw(std::vector<::shared_ptr<column_identifier::raw>> pk_columns, target_type t) : value(pk_columns), type(t) {}

        static ::shared_ptr<raw> values_of(::shared_ptr<column_identifier::raw> c);
        static ::shared_ptr<raw> keys_of(::shared_ptr<column_identifier::raw> c);
        static ::shared_ptr<raw> keys_and_values_of(::shared_ptr<column_identifier::raw> c);
        static ::shared_ptr<raw> full_collection(::shared_ptr<column_identifier::raw> c);
        static ::shared_ptr<raw> columns(std::vector<::shared_ptr<column_identifier::raw>> c);
        ::shared_ptr<index_target> prepare(const schema&) const;
    };
};

sstring to_sstring(index_target::target_type type);

}
}
