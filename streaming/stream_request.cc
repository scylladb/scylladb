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

#include "streaming/stream_request.hh"
#include "query-request.hh"

namespace streaming {

void stream_request::serialize(bytes::iterator& out) const {
    serialize_string(out, keyspace);

    serialize_int32(out, uint32_t(ranges.size()));
    for (auto& x : ranges) {
        x.serialize(out);
    }

    serialize_int32(out, uint32_t(column_families.size()));
    for (auto& x : column_families) {
        serialize_string(out, x);
    }

    serialize_int64(out, repaired_at);
}

stream_request stream_request::deserialize(bytes_view& v) {
    auto keyspace_ = read_simple_short_string(v);

    auto num = read_simple<int32_t>(v);
    std::vector<query::range<token>> ranges_;
    for (int32_t i = 0; i < num; i++) {
        ranges_.push_back(query::range<token>::deserialize(v));
    }

    num = read_simple<int32_t>(v);
    std::vector<sstring> column_families_;
    for (int32_t i = 0; i < num; i++) {
        auto s = read_simple_short_string(v);
        column_families_.push_back(std::move(s));
    }

    auto repaired_at_ = read_simple<int64_t>(v);

    return stream_request(std::move(keyspace_), std::move(ranges_), std::move(column_families_), repaired_at_);
}

size_t stream_request::serialized_size() const {
    size_t size = serialize_string_size(keyspace);

    size += serialize_int32_size;
    for (auto& x : ranges) {
        size += x.serialized_size();
    }

    size += serialize_int32_size;
    for (auto& x : column_families) {
        size += serialize_string_size(x);
    }

    size += serialize_int64_size;

    return size;
}

std::ostream& operator<<(std::ostream& os, const stream_request& sr) {
    os << "[ ks = " << sr.keyspace << " cf =  ";
    for (auto& cf : sr.column_families) {
        os << cf << " ";
    }
    return os << "]";
}

} // namespace streaming;
