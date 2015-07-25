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

#include "types.hh"

namespace cql3 {

class column_specification;
class column_identifier;

class column_specification final {
public:
    const sstring ks_name;
    const sstring cf_name;
    const ::shared_ptr<column_identifier> name;
    const data_type type;

    column_specification(sstring ks_name_, sstring cf_name_, ::shared_ptr<column_identifier> name_, data_type type_)
        : ks_name(std::move(ks_name_))
        , cf_name(std::move(cf_name_))
        , name(name_)
        , type(type_)
    { }

    /**
     * Returns a new <code>ColumnSpecification</code> for the same column but with the specified alias.
     *
     * @param alias the column alias
     * @return a new <code>ColumnSpecification</code> for the same column but with the specified alias.
     */
    ::shared_ptr<column_specification> with_alias(::shared_ptr<column_identifier> alias) {
        return ::make_shared<column_specification>(ks_name, cf_name, alias, type);
    }
    
    bool is_reversed_type() const {
        return ::dynamic_pointer_cast<const reversed_type_impl>(type) != nullptr;
    }
};

}
