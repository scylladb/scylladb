
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

#include "streaming/messages/prepare_message.hh"
#include "types.hh"
#include "utils/serialization.hh"

namespace streaming {
namespace messages {

void prepare_message::serialize(bytes::iterator& out) const {
    serialize_int32(out, uint32_t(requests.size()));
    for (auto& x : requests) {
        x.serialize(out);
    }

    serialize_int32(out, uint32_t(summaries.size()));
    for (auto& x : summaries) {
        x.serialize(out);
    }
}

prepare_message prepare_message::deserialize(bytes_view& v) {
    auto num = read_simple<int32_t>(v);
    std::vector<stream_request> requests_(num);
    for (int32_t i = 0; i < num; i++) {
        auto r = stream_request::deserialize(v);
        requests_.push_back(std::move(r));
    }

    num = read_simple<int32_t>(v);
    std::vector<stream_summary> summaries_(num);
    for (int32_t i = 0; i < num; i++) {
        auto s = stream_summary::deserialize(v);
        summaries_.push_back(std::move(s));
    }

    return prepare_message(std::move(requests_), std::move(summaries_));
}

size_t prepare_message::serialized_size() const {
    size_t size = serialize_int32_size;
    for (auto& x : requests) {
        size += x.serialized_size();
    }

    size += serialize_int32_size;
    for (auto& x : summaries) {
        size += x.serialized_size();
    }
    return size;
}

} // namespace messages
} // namespace streaming
