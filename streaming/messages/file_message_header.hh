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
    int64_t repaired_at;
    int32_t sstable_level;

    file_message_header() = default;

    file_message_header(UUID cf_id_, int32_t sequence_number_, sstring version_, format_types format_,
                        int64_t estimated_keys_, std::map<int64_t, int64_t> sections_,
                        compression_info comp_info_, int64_t repaired_at_, int32_t sstable_level_)
        : cf_id(cf_id_)
        , sequence_number(sequence_number_)
        , version(version_)
        , format(format_)
        , estimated_keys(estimated_keys_)
        , sections(std::move(sections_))
        , comp_info(std::move(comp_info_))
        , repaired_at(repaired_at_)
        , sstable_level(sstable_level_) {
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

    static class FileMessageHeaderSerializer implements IVersionedSerializer<FileMessageHeader>
    {
        public void serialize(FileMessageHeader header, DataOutputPlus out, int version) throws IOException
        {
            UUIDSerializer.serializer.serialize(header.cf_id, out, version);
            out.writeInt(header.sequence_number);
            out.writeUTF(header.version);

            //We can't stream to a node that doesn't understand a new sstable format
            if (version < StreamMessage.VERSION_30 && header.format != SSTableFormat.Type.LEGACY && header.format != SSTableFormat.Type.BIG)
                throw new UnsupportedOperationException("Can't stream non-legacy sstables to nodes < 3.0");

            if (version >= StreamMessage.VERSION_30)
                out.writeUTF(header.format.name);

            out.writeLong(header.estimated_keys);
            out.writeInt(header.sections.size());
            for (Pair<Long, Long> section : header.sections)
            {
                out.writeLong(section.left);
                out.writeLong(section.right);
            }
            CompressionInfo.serializer.serialize(header.comp_info, out, version);
            out.writeLong(header.repaired_at);
            out.writeInt(header.sstable_level);
        }

        public FileMessageHeader deserialize(DataInput in, int version) throws IOException
        {
            UUID cf_id = UUIDSerializer.serializer.deserialize(in, MessagingService.current_version);
            int sequence_number = in.readInt();
            sstring sstableVersion = in.readUTF();

            SSTableFormat.Type format = SSTableFormat.Type.LEGACY;
            if (version >= StreamMessage.VERSION_30)
                format = SSTableFormat.Type.validate(in.readUTF());

            long estimated_keys = in.readLong();
            int count = in.readInt();
            List<Pair<Long, Long>> sections = new ArrayList<>(count);
            for (int k = 0; k < count; k++)
                sections.add(Pair.create(in.readLong(), in.readLong()));
            CompressionInfo comp_info = CompressionInfo.serializer.deserialize(in, MessagingService.current_version);
            long repaired_at = in.readLong();
            int sstable_level = in.readInt();
            return new FileMessageHeader(cf_id, sequence_number, sstableVersion, format, estimated_keys, sections, comp_info, repaired_at, sstable_level);
        }

        public long serializedSize(FileMessageHeader header, int version)
        {
            long size = UUIDSerializer.serializer.serializedSize(header.cf_id, version);
            size += TypeSizes.NATIVE.sizeof(header.sequence_number);
            size += TypeSizes.NATIVE.sizeof(header.version);

            if (version >= StreamMessage.VERSION_30)
                size += TypeSizes.NATIVE.sizeof(header.format.name);

            size += TypeSizes.NATIVE.sizeof(header.estimated_keys);

            size += TypeSizes.NATIVE.sizeof(header.sections.size());
            for (Pair<Long, Long> section : header.sections)
            {
                size += TypeSizes.NATIVE.sizeof(section.left);
                size += TypeSizes.NATIVE.sizeof(section.right);
            }
            size += CompressionInfo.serializer.serializedSize(header.comp_info, version);
            size += TypeSizes.NATIVE.sizeof(header.sstable_level);
            return size;
        }
    }
#endif
public:
    void serialize(bytes::iterator& out) const {
    }
    static file_message_header deserialize(bytes_view& v) {
        return file_message_header();
    }
    size_t serialized_size() const {
        return 0;
    }
};

} // namespace messages
} // namespace streaming
