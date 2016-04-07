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

#include <stdexcept>
#include "index_target.hh"
#include "db/index/secondary_index.hh"

using db::index::secondary_index;

sstring cql3::statements::index_target::index_option(target_type type)  {
    switch (type) {
        case target_type::keys: return secondary_index::index_keys_option_name;
        case target_type::keys_and_values: return secondary_index::index_entries_option_name;
        case target_type::values: return secondary_index::index_values_option_name;
        default: throw std::invalid_argument("should not reach");
    }
}

cql3::statements::index_target::target_type
cql3::statements::index_target::from_column_definition(const column_definition& cd) {
    auto& opts = cd.idx_info.index_options;

    if (!opts) {
        throw std::invalid_argument("No index options");
    }
    if (opts->count(secondary_index::index_keys_option_name)) {
        return target_type::keys;
    } else if (opts->count(secondary_index::index_entries_option_name)) {
        return target_type::keys_and_values;
    } else if (cd.type->is_collection() && !cd.type->is_multi_cell()) {
        return target_type::full;
    } else {
        return target_type::values;
    }
}

::shared_ptr<cql3::statements::index_target::raw>
cql3::statements::index_target::raw::values_of(::shared_ptr<column_identifier::raw> c) {
    return ::make_shared<raw>(c, target_type::values);
}
::shared_ptr<cql3::statements::index_target::raw>
cql3::statements::index_target::raw::keys_of(::shared_ptr<column_identifier::raw> c) {
    return ::make_shared<raw>(c, target_type::keys);
}
::shared_ptr<cql3::statements::index_target::raw>
cql3::statements::index_target::raw::keys_and_values_of(::shared_ptr<column_identifier::raw> c) {
    return ::make_shared<raw>(c, target_type::keys_and_values);
}
::shared_ptr<cql3::statements::index_target::raw>
cql3::statements::index_target::raw::full_collection(::shared_ptr<column_identifier::raw> c) {
    return ::make_shared<raw>(c, target_type::full);
}

::shared_ptr<cql3::statements::index_target>
cql3::statements::index_target::raw::prepare(schema_ptr schema) {
    return ::make_shared<index_target>(column->prepare_column_identifier(schema), type);
}
