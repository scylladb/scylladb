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

#include "cql3/keyspace_element_name.hh"

namespace cql3 {

void keyspace_element_name::set_keyspace(const sstring& ks, bool keep_case)
{
    _ks_name = to_internal_name(ks, keep_case);
}

bool keyspace_element_name::has_keyspace() const
{
    return bool(_ks_name);
}

const sstring& keyspace_element_name::get_keyspace() const
{
    return *_ks_name;
}

sstring keyspace_element_name::to_internal_name(sstring name, bool keep_case)
{
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
