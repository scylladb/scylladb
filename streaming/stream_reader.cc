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
#include "streaming/stream_reader.hh"

namespace streaming {

stream_reader::stream_reader(file_message_header header, shared_ptr<stream_session> session_)
        : cf_id(header.cf_id)
        , estimated_keys(header.estimated_keys)
        , sections(header.sections)
        , session(session_)
        // input_version = header.format.info.getVersion(header.version)
        , repaired_at(header.repaired_at)
        , format(header.format)
        , sstable_level(header.sstable_level) {
}

stream_reader::~stream_reader() = default;

int64_t stream_reader::total_size() {
    int64_t size = 0;
    for (auto section : sections)
        size += section.second - section.first;
    return size;
}

} // namespace streaming
