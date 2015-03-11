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

namespace cql3 {

class operation::set_value : public raw_update {
private:
    ::shared_ptr<term::raw> _value;
public:
    set_value(::shared_ptr<term::raw> value) : _value(std::move(value)) {}

    virtual ::shared_ptr <operation> prepare(const sstring& keyspace, column_definition& receiver) override {
        auto v = _value->prepare(keyspace, receiver.column_specification);

        if (receiver.type->is_counter()) {
            throw exceptions::invalid_request_exception(sprint("Cannot set the value of counter column %s (counters can only be incremented/decremented, not set)", receiver.name_as_text()));
        }

        if (!receiver.type->is_collection()) {
            return ::make_shared<constants::setter>(receiver, v);
        }

        auto& k = static_pointer_cast<collection_type_impl>(receiver.type)->_kind;
        if (&k == &collection_type_impl::kind::list) {
            throw std::runtime_error("not implemented"); // FIXME
            // return new Lists.Setter(receiver, v);
        } else if (&k == &collection_type_impl::kind::set) {
            throw std::runtime_error("not implemented"); // FIXME
            // return new Sets.Setter(receiver, v);
        } else if (&k == &collection_type_impl::kind::map) {
            return make_shared<maps::setter>(receiver, v);
        } else {
            abort();
        }
    }

#if 0
        protected String toString(ColumnSpecification column)
        {
            return String.format("%s = %s", column, value);
        }
#endif

    virtual bool is_compatible_with(::shared_ptr <raw_update> other) override {
        // We don't allow setting multiple time the same column, because 1)
        // it's stupid and 2) the result would seem random to the user.
        return false;
    }
};

}
