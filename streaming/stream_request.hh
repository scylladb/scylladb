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

#include "core/sstring.hh"
#include "query-request.hh"
#include "dht/i_partitioner.hh"
#include <vector>

namespace streaming {

class stream_request {
public:
    using token = dht::token;
    sstring keyspace;
    std::vector<query::range<token>> ranges;
    std::vector<sstring> column_families;
    long repaired_at;
    stream_request(sstring _keyspace, std::vector<query::range<token>> _ranges, std::vector<sstring> _column_families, long _repaired_at)
        : keyspace(std::move(_keyspace))
        , ranges(std::move(_ranges))
        , column_families(std::move(_column_families))
        , repaired_at(_repaired_at) {
    }

#if 0
    public static class StreamRequestSerializer implements IVersionedSerializer<StreamRequest>
    {
        public void serialize(StreamRequest request, DataOutputPlus out, int version) throws IOException
        {
            out.writeUTF(request.keyspace);
            out.writeLong(request.repairedAt);
            out.writeInt(request.ranges.size());
            for (Range<Token> range : request.ranges)
            {
                Token.serializer.serialize(range.left, out);
                Token.serializer.serialize(range.right, out);
            }
            out.writeInt(request.columnFamilies.size());
            for (sstring cf : request.columnFamilies)
                out.writeUTF(cf);
        }

        public StreamRequest deserialize(DataInput in, int version) throws IOException
        {
            sstring keyspace = in.readUTF();
            long repairedAt = in.readLong();
            int rangeCount = in.readInt();
            List<Range<Token>> ranges = new ArrayList<>(rangeCount);
            for (int i = 0; i < rangeCount; i++)
            {
                Token left = Token.serializer.deserialize(in);
                Token right = Token.serializer.deserialize(in);
                ranges.add(new Range<>(left, right));
            }
            int cfCount = in.readInt();
            List<sstring> columnFamilies = new ArrayList<>(cfCount);
            for (int i = 0; i < cfCount; i++)
                columnFamilies.add(in.readUTF());
            return new StreamRequest(keyspace, ranges, columnFamilies, repairedAt);
        }

        public long serializedSize(StreamRequest request, int version)
        {
            int size = TypeSizes.NATIVE.sizeof(request.keyspace);
            size += TypeSizes.NATIVE.sizeof(request.repairedAt);
            size += TypeSizes.NATIVE.sizeof(request.ranges.size());
            for (Range<Token> range : request.ranges)
            {
                size += Token.serializer.serializedSize(range.left, TypeSizes.NATIVE);
                size += Token.serializer.serializedSize(range.right, TypeSizes.NATIVE);
            }
            size += TypeSizes.NATIVE.sizeof(request.columnFamilies.size());
            for (sstring cf : request.columnFamilies)
                size += TypeSizes.NATIVE.sizeof(cf);
            return size;
        }
    }
#endif
};

} // namespace streaming
