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

#include "streaming/stream_summary.hh"
#include "types.hh"
#include "utils/serialization.hh"

namespace streaming {

void stream_summary::serialize(bytes::iterator& out) const {
    cf_id.serialize(out);
    serialize_int32(out, files);
    serialize_int64(out, total_size);
}

stream_summary stream_summary::deserialize(bytes_view& v) {
    auto cf_id_ = UUID::deserialize(v);
    auto files_ = read_simple<int32_t>(v);
    auto total_size_ = read_simple<int64_t>(v);
    return stream_summary(std::move(cf_id_), files_, total_size_);
}

size_t stream_summary::serialized_size() const {
    return cf_id.serialized_size() + serialize_int32_size + serialize_int64_size;
}

std::ostream& operator<<(std::ostream& os, const stream_summary& x) {
    os << "[ cf_id=" << x.cf_id << " ]";
    return os;
}

} // namespace streaming
