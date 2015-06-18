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
#include "streaming/stream_request.hh"
#include "streaming/stream_summary.hh"

namespace streaming {
namespace messages {

class prepare_message : public stream_message {
#if 0
    public static Serializer<PrepareMessage> serializer = new Serializer<PrepareMessage>()
    {
        public PrepareMessage deserialize(ReadableByteChannel in, int version, StreamSession session) throws IOException
        {
            DataInput input = new DataInputStream(Channels.newInputStream(in));
            PrepareMessage message = new PrepareMessage();
            // requests
            int numRequests = input.readInt();
            for (int i = 0; i < numRequests; i++)
                message.requests.add(StreamRequest.serializer.deserialize(input, version));
            // summaries
            int numSummaries = input.readInt();
            for (int i = 0; i < numSummaries; i++)
                message.summaries.add(StreamSummary.serializer.deserialize(input, version));
            return message;
        }

        public void serialize(PrepareMessage message, DataOutputStreamAndChannel out, int version, StreamSession session) throws IOException
        {
            // requests
            out.writeInt(message.requests.size());
            for (StreamRequest request : message.requests)
                StreamRequest.serializer.serialize(request, out, version);
            // summaries
            out.writeInt(message.summaries.size());
            for (StreamSummary summary : message.summaries)
                StreamSummary.serializer.serialize(summary, out, version);
        }
    };
#endif
public:
    /**
     * Streaming requests
     */
    std::vector<stream_request> requests;

    /**
     * Summaries of streaming out
     */
    std::vector<stream_summary> summaries;

#if 0
    public PrepareMessage()
    {
        super(Type.PREPARE);
    }

    @Override
    public String toString()
    {
        final StringBuilder sb = new StringBuilder("Prepare (");
        sb.append(requests.size()).append(" requests, ");
        int totalFile = 0;
        for (StreamSummary summary : summaries)
            totalFile += summary.files;
        sb.append(" ").append(totalFile).append(" files");
        sb.append('}');
        return sb.toString();
    }
#endif
public:
    void serialize(bytes::iterator& out) const {
    }
    static prepare_message deserialize(bytes_view& v) {
        return prepare_message();
    }
    size_t serialized_size() const {
        return 0;
    }
};

} // namespace messages
} // namespace streaming
