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

#pragma once

#include "utils/UUID.hh"
#include "streaming/messages/stream_message.hh"
#include "streaming/messages/file_message_header.hh"
#include "streaming/compress/compression_info.hh"
#include "sstables/sstables.hh"

namespace streaming {
namespace messages {

/**
 * OutgoingFileMessage is used to transfer the part(or whole) of a SSTable data file.
 */
class outgoing_file_message : public stream_message {
    using UUID = utils::UUID;
    using compression_info = compress::compression_info;
    using format_types = sstables::sstable::format_types;
#if 0
    public static Serializer<OutgoingFileMessage> serializer = new Serializer<OutgoingFileMessage>()
    {
        public OutgoingFileMessage deserialize(ReadableByteChannel in, int version, StreamSession session) throws IOException
        {
            throw new UnsupportedOperationException("Not allowed to call deserialize on an outgoing file");
        }

        public void serialize(OutgoingFileMessage message, DataOutputStreamAndChannel out, int version, StreamSession session) throws IOException
        {
            FileMessageHeader.serializer.serialize(message.header, out, version);

            final SSTableReader reader = message.sstable;
            StreamWriter writer = message.header.compressionInfo == null ?
                    new StreamWriter(reader, message.header.sections, session) :
                    new CompressedStreamWriter(reader,
                            message.header.sections,
                            message.header.compressionInfo, session);
            writer.write(out.getChannel());
            session.fileSent(message.header);
        }
    };
#endif
public:

    file_message_header header;
    sstables::sstable* sstable;

    outgoing_file_message() = default;
    outgoing_file_message(sstables::sstable& sstable_, int32_t sequence_number, int64_t estimated_keys,
                          std::map<int64_t, int64_t> sections, int64_t repaired_at, bool keep_ss_table_level)
        : sstable(&sstable_) {
#if 0
        super(Type.FILE);
        CompressionInfo compressionInfo = null;
        if (sstable.compression)
        {
            CompressionMetadata meta = sstable.getCompressionMetadata();
            compressionInfo = new CompressionInfo(meta.getChunksForSections(sections), meta.parameters);
        }
#endif
        // FIXME:
        UUID cf_id; // sstable.metadata.cfId
        sstring version; // sstable.descriptor.version.toString()
        format_types format = format_types::big; // sstable.descriptor.formatType
        int32_t level = 0; // keepSSTableLevel ? sstable.getSSTableLevel() : 0
        compression_info comp_info;
        header = file_message_header(cf_id, sequence_number, version, format, estimated_keys,
                                     std::move(sections), comp_info, repaired_at, level);
    }

#if 0
    @Override
    public String toString()
    {
        return "File (" + header + ", file: " + sstable.getFilename() + ")";
    }
#endif
public:
    void serialize(bytes::iterator& out) const {
    }
    static outgoing_file_message deserialize(bytes_view& v) {
        return outgoing_file_message();
    }
    size_t serialized_size() const {
        return 0;
    }
};

} // namespace messages
} // namespace streaming

