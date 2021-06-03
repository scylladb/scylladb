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

#include "cql3/keyspace_element_name.hh"

namespace cql3 {

void keyspace_element_name::set_keyspace(std::string_view ks, bool keep_case)
{
    _ks_name = to_internal_name(ks, keep_case);
}

bool keyspace_element_name::has_keyspace() const
{
    return bool(_ks_name);
}

const sstring& keyspace_element_name::get_keyspace() const
{
    assert(_ks_name);
    return *_ks_name;
}

sstring keyspace_element_name::to_internal_name(std::string_view view, bool keep_case)
{
    sstring name(view);
    if (!keep_case) {
        std::transform(name.begin(), name.end(), name.begin(), ::tolower);
    }
    return name;
}

sstring keyspace_element_name::to_string() const
{
    return has_keyspace() ? (get_keyspace() + ".") : "";    
}

}
