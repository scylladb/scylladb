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

#include "streaming/messages/stream_message.hh"

namespace streaming {
namespace messages {

/**
 * OutgoingFileMessage is used to transfer the part(or whole) of a SSTable data file.
 */
class outgoing_file_message : public stream_message {
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

    public FileMessageHeader header;
    public SSTableReader sstable;

    public OutgoingFileMessage(SSTableReader sstable, int sequenceNumber, long estimatedKeys, List<Pair<Long, Long>> sections, long repairedAt, boolean keepSSTableLevel)
    {
        super(Type.FILE);
        this.sstable = sstable;

        CompressionInfo compressionInfo = null;
        if (sstable.compression)
        {
            CompressionMetadata meta = sstable.getCompressionMetadata();
            compressionInfo = new CompressionInfo(meta.getChunksForSections(sections), meta.parameters);
        }
        this.header = new FileMessageHeader(sstable.metadata.cfId,
                                            sequenceNumber,
                                            sstable.descriptor.version.toString(),
                                            sstable.descriptor.formatType,
                                            estimatedKeys,
                                            sections,
                                            compressionInfo,
                                            repairedAt,
                                            keepSSTableLevel ? sstable.getSSTableLevel() : 0);
    }

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

