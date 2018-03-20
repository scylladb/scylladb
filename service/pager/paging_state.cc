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

#include "bytes.hh"
#include "keys.hh"
#include "paging_state.hh"
#include "core/simple-stream.hh"
#include "idl/keys.dist.hh"
#include "idl/uuid.dist.hh"
#include "idl/paging_state.dist.hh"
#include "idl/token.dist.hh"
#include "idl/range.dist.hh"
#include "serializer_impl.hh"
#include "idl/keys.dist.impl.hh"
#include "idl/uuid.dist.impl.hh"
#include "idl/paging_state.dist.impl.hh"
#include "idl/token.dist.impl.hh"
#include "idl/range.dist.impl.hh"
#include "message/messaging_service.hh"

service::pager::paging_state::paging_state(partition_key pk,
        std::experimental::optional<clustering_key> ck,
        uint32_t rem,
        utils::UUID query_uuid,
        replicas_per_token_range last_replicas,
        std::experimental::optional<db::read_repair_decision> query_read_repair_decision)
    : _partition_key(std::move(pk))
    , _clustering_key(std::move(ck))
    , _remaining(rem)
    , _query_uuid(query_uuid)
    , _last_replicas(std::move(last_replicas))
    , _query_read_repair_decision(query_read_repair_decision) {
}

::shared_ptr<service::pager::paging_state> service::pager::paging_state::deserialize(
        bytes_opt data) {
    if (!data) {
        return nullptr;
    }

    if (data.value().size() < sizeof(uint32_t) || le_to_cpu(*unaligned_cast<int32_t*>(data.value().begin())) != netw::messaging_service::current_version) {
        throw exceptions::protocol_exception("Invalid value for the paging state");
    }


    // skip 4 bytes that contain format id
    seastar::simple_input_stream in(reinterpret_cast<char*>(data.value().begin() + sizeof(uint32_t)), data.value().size() - sizeof(uint32_t));

    try {
        return ::make_shared<paging_state>(ser::deserialize(in, boost::type<paging_state>()));
    } catch (...) {
        std::throw_with_nested(
                exceptions::protocol_exception(
                        "Invalid value for the paging state"));
    }
}

bytes_opt service::pager::paging_state::serialize() const {
    bytes b = ser::serialize_to_buffer<bytes>(*this, sizeof(uint32_t));
    // put serialization format id
    *unaligned_cast<int32_t*>(b.begin()) = cpu_to_le(netw::messaging_service::current_version);
    return {std::move(b)};
}
