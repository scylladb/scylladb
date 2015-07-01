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

#include "streaming/messages/received_message.hh"
#include "types.hh"
#include "util/serialization.hh"

namespace streaming {
namespace messages {

void received_message::serialize(bytes::iterator& out) const {
    cf_id.serialize(out);
    serialize_int32(out, sequence_number);
}

received_message received_message::deserialize(bytes_view& v) {
    auto cf_id_ = UUID::deserialize(v);
    auto sequence_number_ = read_simple<int32_t>(v);
    return received_message(std::move(cf_id_), sequence_number_);
}

size_t received_message::serialized_size() const {
    return cf_id.serialized_size() + serialize_int32_size;
}

} // namespace messages
} // namespace streaming
