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

#include "operation.hh"
#include "constants.hh"
#include "maps.hh"
#include "sets.hh"
#include "lists.hh"

namespace cql3 {

class operation::set_value : public raw_update {
private:
    ::shared_ptr<term::raw> _value;
public:
    set_value(::shared_ptr<term::raw> value) : _value(std::move(value)) {}

    virtual ::shared_ptr <operation> prepare(const sstring& keyspace, const column_definition& receiver) override;

#if 0
        protected String toString(ColumnSpecification column)
        {
            return String.format("%s = %s", column, value);
        }
#endif

    virtual bool is_compatible_with(::shared_ptr <raw_update> other) override;
};

}
