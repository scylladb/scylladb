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
 * Copyright (C) 2021-present ScyllaDB
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

#include "cql_value.hh"
#include "utils/overloaded_functor.hh"

namespace cql3 {
ordered_cql_value reverse_if_needed(cql_value&& value, bool should_reverse) {
    if (should_reverse) {
        return ordered_cql_value(reversed_cql_value{std::move(value)});
    } else {
        return ordered_cql_value(std::move(value));
    }
}

// Takes the cql_value out of ordered_cql_value
cql_value into_cql_value(ordered_cql_value&& ordered_cql_val) {
    return std::visit(overloaded_functor{
        [](cql_value&& val) { return std::move(val); },
        [](reversed_cql_value&& val) { return std::move(val.value); }
    }, std::move(ordered_cql_val));
}

cql_value cql_value_from_raw_value(const cql3::raw_value& raw_val,
                                   cql_serialization_format sf,
                                   const abstract_type& val_type) {
    if (raw_val.is_null()) {
        return cql_value(null_value{});
    }

    if (raw_val.is_unset_value()) {
        return cql_value(unset_value{});
    }
    // Now we know that raw_val.is_value()

    return raw_val.to_view().with_value([&](const FragmentedView auto& view) {
        return cql_value_from_serialized(view, sf, val_type);
    });
}

// Decides whether serialized bytes of size 0 mean empty_value or not
bool is_0_bytes_value_empty_value(abstract_type::kind type_kind) {
    switch (type_kind) {
        case abstract_type::kind::utf8:
        case abstract_type::kind::bytes:
        case abstract_type::kind::ascii:
        case abstract_type::kind::tuple:
            return false;

        case abstract_type::kind::boolean:
        case abstract_type::kind::byte:
        case abstract_type::kind::short_kind:
        case abstract_type::kind::int32:
        case abstract_type::kind::long_kind:
        case abstract_type::kind::float_kind:
        case abstract_type::kind::double_kind:
        case abstract_type::kind::counter:
        case abstract_type::kind::inet:
        case abstract_type::kind::uuid:
        case abstract_type::kind::date:
        case abstract_type::kind::simple_date:
        case abstract_type::kind::duration:
        case abstract_type::kind::time:
        case abstract_type::kind::timestamp:
        case abstract_type::kind::timeuuid:
        case abstract_type::kind::empty:
        case abstract_type::kind::list:
        case abstract_type::kind::set:
        case abstract_type::kind::map:
        case abstract_type::kind::user:
        case abstract_type::kind::decimal:
        case abstract_type::kind::varint:
            return true;

        case abstract_type::kind::reversed:
            throw std::runtime_error("is_0_bytes_value_empty_value reversed_type is not allowed");

        default:
            throw std::runtime_error(
                fmt::format("is_0_bytes_value_empty_value - unhandled type kind: {}", type_kind));
    }
}
}
