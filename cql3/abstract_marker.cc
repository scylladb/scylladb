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

#include "cql3/abstract_marker.hh"

#include "cql3/constants.hh"
#include "cql3/lists.hh"
#include "cql3/maps.hh"
#include "cql3/sets.hh"

namespace cql3 {

::shared_ptr<term> abstract_marker::raw::prepare(const sstring& keyspace, ::shared_ptr<column_specification> receiver)
{
    auto receiver_type = ::dynamic_pointer_cast<db::marshal::collection_type>(receiver->type);
    if (receiver_type != nullptr) {
        return ::make_shared<constants::marker>(_bind_index, receiver);
    }
    switch (receiver_type->kind) {
        case db::marshal::collection_type::collection_kind::LIST: return ::make_shared<lists::marker>(_bind_index, receiver);
        case db::marshal::collection_type::collection_kind::SET:  return ::make_shared<sets::marker>(_bind_index, receiver);
        case db::marshal::collection_type::collection_kind::MAP:  return ::make_shared<maps::marker>(_bind_index, receiver);
    }
    assert(0);
}

::shared_ptr<term> abstract_marker::in_raw::prepare(const sstring& keyspace, ::shared_ptr<column_specification> receiver) {
    return ::make_shared<lists::marker>(_bind_index, make_in_receiver(receiver));
}

}
