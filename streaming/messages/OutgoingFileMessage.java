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
 */
package org.apache.cassandra.streaming.messages;

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.util.List;

import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.DataOutputStreamAndChannel;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.streaming.StreamWriter;
import org.apache.cassandra.streaming.compress.CompressedStreamWriter;
import org.apache.cassandra.streaming.compress.CompressionInfo;
import org.apache.cassandra.utils.Pair;

/**
 * OutgoingFileMessage is used to transfer the part(or whole) of a SSTable data file.
 */
public class OutgoingFileMessage extends StreamMessage
{
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
}

