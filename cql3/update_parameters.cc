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

#include "cql3/update_parameters.hh"

namespace cql3 {

const update_parameters::prefetch_data::cell_list*
update_parameters::get_prefetched_list(
    partition_key_view pkey,
    clustering_key_view ckey,
    const column_definition& column) const
{
    if (!_prefetched) {
        return {};
    }

    if (column.is_static()) {
        ckey = clustering_key_view::make_empty();
    }
    auto i = _prefetched->rows.find(std::make_pair(std::move(pkey), std::move(ckey)));
    if (i == _prefetched->rows.end()) {
        return {};
    }

    auto&& row = i->second;
    auto j = row.find(column.id);
    if (j == row.end()) {
        return {};
    }
    return &j->second;
}

update_parameters::prefetch_data::prefetch_data(schema_ptr schema)
    : rows(8, key_hashing(*schema), key_equality(*schema))
    , schema(schema)
{ }

}
