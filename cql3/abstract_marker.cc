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
 * Copyright (C) 2015 ScyllaDB
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

#include "cql3/abstract_marker.hh"

#include "cql3/constants.hh"
#include "cql3/lists.hh"
#include "cql3/maps.hh"
#include "cql3/sets.hh"

namespace cql3 {

abstract_marker::abstract_marker(int32_t bind_index, ::shared_ptr<column_specification>&& receiver)
    : _bind_index{bind_index}
    , _receiver{std::move(receiver)}
{ }

void abstract_marker::collect_marker_specification(::shared_ptr<variable_specifications> bound_names) {
    bound_names->add(_bind_index, _receiver);
}

bool abstract_marker::contains_bind_marker() const {
    return true;
}

abstract_marker::raw::raw(int32_t bind_index)
    : _bind_index{bind_index}
{ }

::shared_ptr<term> abstract_marker::raw::prepare(database& db, const sstring& keyspace, ::shared_ptr<column_specification> receiver)
{
    auto receiver_type = ::dynamic_pointer_cast<const collection_type_impl>(receiver->type);
    if (receiver_type == nullptr) {
        return ::make_shared<constants::marker>(_bind_index, receiver);
    }
    if (&receiver_type->_kind == &collection_type_impl::kind::list) {
        return ::make_shared<lists::marker>(_bind_index, receiver);
    } else if (&receiver_type->_kind == &collection_type_impl::kind::set) {
        return ::make_shared<sets::marker>(_bind_index, receiver);
    } else if (&receiver_type->_kind == &collection_type_impl::kind::map) {
        return ::make_shared<maps::marker>(_bind_index, receiver);
    }
    assert(0);
    return shared_ptr<term>();
}

assignment_testable::test_result abstract_marker::raw::test_assignment(database& db, const sstring& keyspace, ::shared_ptr<column_specification> receiver) {
    return assignment_testable::test_result::WEAKLY_ASSIGNABLE;
}

sstring abstract_marker::raw::to_string() const {
    return "?";
}

abstract_marker::in_raw::in_raw(int32_t bind_index)
    : raw{bind_index}
{ }

::shared_ptr<column_specification> abstract_marker::in_raw::make_in_receiver(::shared_ptr<column_specification> receiver) {
    auto in_name = ::make_shared<column_identifier>(sstring("in(") + receiver->name->to_string() + sstring(")"), true);
    return ::make_shared<column_specification>(receiver->ks_name, receiver->cf_name, in_name, list_type_impl::get_instance(receiver->type, false));
}

::shared_ptr<term> abstract_marker::in_raw::prepare(database& db, const sstring& keyspace, ::shared_ptr<column_specification> receiver) {
    return ::make_shared<lists::marker>(_bind_index, make_in_receiver(receiver));
}

}
