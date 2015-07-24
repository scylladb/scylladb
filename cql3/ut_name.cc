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

#include "cql3/ut_name.hh"

namespace cql3 {

ut_name::ut_name(shared_ptr<column_identifier> ks_name, ::shared_ptr<column_identifier> ut_name)
    : _ks_name{!ks_name ? std::experimental::nullopt : std::experimental::optional<sstring>{ks_name->to_string()}}
    , _ut_name{ut_name}
{ }

bool ut_name::has_keyspace() const {
    return bool(_ks_name);
}

void ut_name::set_keyspace(sstring keyspace) {
    _ks_name = std::experimental::optional<sstring>{keyspace};
}

sstring ut_name::get_keyspace() const {
    return _ks_name.value();
}

bytes ut_name::get_user_type_name() const {
    return _ut_name->bytes_;
}

sstring ut_name::get_string_type_name() const
{
    return _ut_name->to_string();
}

sstring ut_name::to_string() const {
    return (has_keyspace() ? (_ks_name.value() + ".") : "") + _ut_name->to_string();
}

}
