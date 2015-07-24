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

#pragma once

#include "core/shared_ptr.hh"
#include "column_identifier.hh"

#include <experimental/optional>

namespace cql3 {

class ut_name final {
    std::experimental::optional<sstring> _ks_name;
    ::shared_ptr<column_identifier> _ut_name;
public:
    ut_name(shared_ptr<column_identifier> ks_name, ::shared_ptr<column_identifier> ut_name);

    bool has_keyspace() const;

    void set_keyspace(sstring keyspace);

    sstring get_keyspace() const;

    bytes get_user_type_name() const;

    sstring get_string_type_name() const;

    sstring to_string() const;

    friend std::ostream& operator<<(std::ostream& os, const ut_name& n) {
        return os << n.to_string();
    }
};

}
