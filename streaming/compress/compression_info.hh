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

#include "bytes.hh"

namespace streaming {
namespace compress {

/**
 * Container that carries compression parameters and chunks to decompress data from stream.
 */
class compression_info {
#if 0
    public static final IVersionedSerializer<CompressionInfo> serializer = new CompressionInfoSerializer();

    public final CompressionMetadata.Chunk[] chunks;
    public final CompressionParameters parameters;

    public CompressionInfo(CompressionMetadata.Chunk[] chunks, CompressionParameters parameters)
    {
        assert chunks != null && parameters != null;
        this.chunks = chunks;
        this.parameters = parameters;
    }

    static class CompressionInfoSerializer implements IVersionedSerializer<CompressionInfo>
    {
        public void serialize(CompressionInfo info, DataOutputPlus out, int version) throws IOException
        {
            if (info == null)
            {
                out.writeInt(-1);
                return;
            }

            int chunkCount = info.chunks.length;
            out.writeInt(chunkCount);
            for (int i = 0; i < chunkCount; i++)
                CompressionMetadata.Chunk.serializer.serialize(info.chunks[i], out, version);
            // compression params
            CompressionParameters.serializer.serialize(info.parameters, out, version);
        }

        public CompressionInfo deserialize(DataInput in, int version) throws IOException
        {
            // chunks
            int chunkCount = in.readInt();
            if (chunkCount < 0)
                return null;

            CompressionMetadata.Chunk[] chunks = new CompressionMetadata.Chunk[chunkCount];
            for (int i = 0; i < chunkCount; i++)
                chunks[i] = CompressionMetadata.Chunk.serializer.deserialize(in, version);

            // compression params
            CompressionParameters parameters = CompressionParameters.serializer.deserialize(in, version);
            return new CompressionInfo(chunks, parameters);
        }

        public long serializedSize(CompressionInfo info, int version)
        {
            if (info == null)
                return TypeSizes.NATIVE.sizeof(-1);

            // chunks
            int chunkCount = info.chunks.length;
            long size = TypeSizes.NATIVE.sizeof(chunkCount);
            for (int i = 0; i < chunkCount; i++)
                size += CompressionMetadata.Chunk.serializer.serializedSize(info.chunks[i], version);
            // compression params
            size += CompressionParameters.serializer.serializedSize(info.parameters, version);
            return size;
        }
    }
#endif
public:
    void serialize(bytes::iterator& out) const {
    }
    static compression_info deserialize(bytes_view& v) {
        return compression_info();
    }
    size_t serialized_size() const {
        return 0;
    }
};

} // namespace compress
} // namespace streaming
