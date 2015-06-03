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
#include <experimental/optional>
#include "core/sstring.hh"
#include "cf_name.hh"

namespace cql3 {

class index_name {
    std::experimental::optional<sstring> _ks_name;
    sstring _idx_name;

public:
    void set_keyspace(sstring ks, bool keep_case) {
        if (!keep_case) {
            std::transform(ks.begin(), ks.end(), ks.begin(), ::tolower);
        }
        _ks_name = std::experimental::make_optional(std::move(ks));
    }
    void set_index(sstring idx_name, bool keep_case) {
        if (!keep_case) {
            std::transform(idx_name.begin(), idx_name.end(), idx_name.begin(), ::tolower);
        }
        _idx_name = std::move(idx_name);
    }
    bool has_keyspace() const {
        return bool(_ks_name);
    }
    const sstring& keyspace() const {
        return _ks_name;
    }
    const sstring& idx() const {
        return _idx_name;
    }
    cf_name get_cf_name() const {
        cf_name cfname;
        if (has_keyspace())  {
            cf_name.set_keyspace(_ks_name, true);
        }
        return cfname;
    }
};

}
