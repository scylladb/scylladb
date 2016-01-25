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

#pragma once

#include "utils/UUID.hh"
#include "sstables/sstables.hh"
#include "streaming/compress/compression_info.hh"

namespace streaming {
namespace messages {
/**
 * StreamingFileHeader is appended before sending actual data to describe what it's sending.
 */
class file_message_header {
    using UUID = utils::UUID;
    using compression_info = compress::compression_info;
    using format_types = sstables::sstable::format_types;
public:
    UUID cf_id;
    int32_t sequence_number;
    /** SSTable version */
    sstring version;

    /** SSTable format **/
    format_types format;
    int64_t estimated_keys;
    std::map<int64_t, int64_t> sections;
    compression_info comp_info;
    int32_t sstable_level;

    file_message_header() = default;

    file_message_header(UUID cf_id_, int32_t sequence_number_, sstring version_, format_types format_,
                        int64_t estimated_keys_, std::map<int64_t, int64_t> sections_,
                        compression_info comp_info_)
        : cf_id(cf_id_)
        , sequence_number(sequence_number_)
        , version(version_)
        , format(format_)
        , estimated_keys(estimated_keys_)
        , sections(std::move(sections_))
        , comp_info(std::move(comp_info_)) {
    }

    /**
     * @return total file size to transfer in bytes
     */
    int64_t size() {
        int64_t size = 0;
        if (true /* comp_info != null */) {
            // calculate total length of transferring chunks
            // for (CompressionMetadata.Chunk chunk : comp_info.chunks)
            //     size += chunk.length + 4; // 4 bytes for CRC
        } else {
            for (auto section : sections) {
                size += section.second - section.first;
            }
        }
        return size;
    }
#if 0
    @Override
    public String toString()
    {
        final StringBuilder sb = new StringBuilder("Header (");
        sb.append("cf_id: ").append(cf_id);
        sb.append(", #").append(sequence_number);
        sb.append(", version: ").append(version);
        sb.append(", format: ").append(format);
        sb.append(", estimated keys: ").append(estimated_keys);
        sb.append(", transfer size: ").append(size());
        sb.append(", compressed?: ").append(comp_info != null);
        sb.append(", repaired_at: ").append(repaired_at);
        sb.append(", level: ").append(sstable_level);
        sb.append(')');
        return sb.toString();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FileMessageHeader that = (FileMessageHeader) o;
        return sequence_number == that.sequence_number && cf_id.equals(that.cf_id);
    }

    @Override
    public int hashCode()
    {
        int result = cf_id.hashCode();
        result = 31 * result + sequence_number;
        return result;
    }
#endif
};

} // namespace messages
} // namespace streaming
