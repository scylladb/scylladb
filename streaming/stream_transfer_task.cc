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

#include "streaming/stream_transfer_task.hh"
#include "streaming/stream_session.hh"

namespace streaming {
void stream_transfer_task::add_transfer_file(sstables::sstable& sstable, int64_t estimated_keys, std::map<int64_t, int64_t> sections, int64_t repaired_at) {
    //assert sstable != null && cfId.equals(sstable.metadata.cfId);
    auto message = messages::outgoing_file_message(sstable, sequence_number++, estimated_keys, std::move(sections), repaired_at, session.keep_ss_table_level());
    auto size = message.header.size();
    auto seq = message.header.sequence_number;
    files.emplace(seq, std::move(message));
    total_size += size;
}

} // namespace streaming
