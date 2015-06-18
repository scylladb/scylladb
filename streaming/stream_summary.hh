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

namespace streaming {

/**
 * Summary of streaming.
 */
class stream_summary {
public:
    using UUID = utils::UUID;
    UUID cf_id;

    /**
     * Number of files to transfer. Can be 0 if nothing to transfer for some streaming request.
     */
    int files;
    long total_size;

    stream_summary(UUID _cf_id, int _files, long _total_size)
        : cf_id (_cf_id)
        , files(_files)
        , total_size(_total_size) {
    }

#if 0
    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StreamSummary summary = (StreamSummary) o;
        return files == summary.files && totalSize == summary.totalSize && cfId.equals(summary.cfId);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(cfId, files, totalSize);
    }

    @Override
    public String toString()
    {
        final StringBuilder sb = new StringBuilder("StreamSummary{");
        sb.append("path=").append(cfId);
        sb.append(", files=").append(files);
        sb.append(", totalSize=").append(totalSize);
        sb.append('}');
        return sb.toString();
    }

    public static class StreamSummarySerializer implements IVersionedSerializer<StreamSummary>
    {
        // arbitrary version is fine for UUIDSerializer for now...
        public void serialize(StreamSummary summary, DataOutputPlus out, int version) throws IOException
        {
            UUIDSerializer.serializer.serialize(summary.cfId, out, MessagingService.current_version);
            out.writeInt(summary.files);
            out.writeLong(summary.totalSize);
        }

        public StreamSummary deserialize(DataInput in, int version) throws IOException
        {
            UUID cfId = UUIDSerializer.serializer.deserialize(in, MessagingService.current_version);
            int files = in.readInt();
            long totalSize = in.readLong();
            return new StreamSummary(cfId, files, totalSize);
        }

        public long serializedSize(StreamSummary summary, int version)
        {
            long size = UUIDSerializer.serializer.serializedSize(summary.cfId, MessagingService.current_version);
            size += TypeSizes.NATIVE.sizeof(summary.files);
            size += TypeSizes.NATIVE.sizeof(summary.totalSize);
            return size;
        }
    }
#endif
};

} // namespace streaming
