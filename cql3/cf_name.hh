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
 * Copyright 2014 Cloudius Systems
 *
 * Modified by Cloudius Systems
 */

#ifndef CQL3_CFNAME_HH
#define CQL3_CFNAME_HH

#include <experimental/optional>

namespace cql3 {

class cf_name {
private:
    std::experimental::optional<sstring> _ks_name;
    sstring _cf_name;

public:
    void set_keyspace(sstring ks, bool keep_case) {
        if (!keep_case) {
            std::transform(ks.begin(), ks.end(), ks.begin(), ::tolower);
        }
        _ks_name = std::experimental::make_optional(ks);
    }

    void set_column_family(sstring cf, bool keep_case) {
        _cf_name = cf;
        if (!keep_case) {
            std::transform(_cf_name.begin(), _cf_name.end(), _cf_name.begin(), ::tolower);
        }
    }
    bool has_keyspace() const {
        return _ks_name ? true : false;
    }

    sstring get_keyspace() const {
        return *_ks_name;
    }

    sstring get_column_family() const {
        return _cf_name;
    }

    friend std::ostream& operator<<(std::ostream& os, const cf_name& n);
};

std::ostream&
operator<<(std::ostream& os, const cf_name& n) {
    if (n.has_keyspace()) {
        os << n.get_keyspace() << ".";
    }
    os << n.get_column_family();
    return os;
}

}

#endif
