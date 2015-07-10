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

#include "cql3/keyspace_element_name.hh"

#include "core/shared_ptr.hh"
#include "cql3/cf_name.hh"

namespace cql3 {

class index_name : public keyspace_element_name {
    sstring _idx_name = "";
public:
    void set_index(const sstring& idx, bool keep_case);

    const sstring& get_idx() const;

    ::shared_ptr<cf_name> get_cf_name() const;

    virtual sstring to_string() const override;
};

}
