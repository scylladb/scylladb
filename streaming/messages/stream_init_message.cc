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
 *
 * Modified by Cloudius Systems.
 * Copyright 2015 Cloudius Systems.
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

#include "streaming/messages/stream_init_message.hh"
#include "types.hh"
#include "utils/serialization.hh"

namespace streaming {
namespace messages {

    void stream_init_message::serialize(bytes::iterator& out) const {
       from.serialize(out);
       serialize_int32(out, session_index);
       plan_id.serialize(out);
       serialize_string(out, description);
       serialize_int8(out, is_for_outgoing);
       serialize_int8(out, keep_ss_table_level);
    }

    stream_init_message stream_init_message::deserialize(bytes_view& v) {
        auto from_ = inet_address::deserialize(v);
        auto session_index_ = read_simple<int32_t>(v);
        auto plan_id_ = UUID::deserialize(v);
        auto description_ = read_simple_short_string(v);
        auto is_for_outgoing_ = read_simple<int8_t>(v);
        auto keep_ss_table_level_ = read_simple<int8_t>(v);
        return stream_init_message(from_, session_index_, plan_id_, std::move(description_), is_for_outgoing_, keep_ss_table_level_);
    }

    size_t stream_init_message::serialized_size() const {
        return from.serialized_size() + serialize_int32_size + plan_id.serialized_size() +
               serialize_string_size(description) + serialize_int8_size + serialize_int8_size;
    }

} // namespace messages
} // namespace streaming
