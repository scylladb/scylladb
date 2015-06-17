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

class retry_message : public stream_message {
#if 0
    public static Serializer<RetryMessage> serializer = new Serializer<RetryMessage>()
    {
        public RetryMessage deserialize(ReadableByteChannel in, int version, StreamSession session) throws IOException
        {
            DataInput input = new DataInputStream(Channels.newInputStream(in));
            return new RetryMessage(UUIDSerializer.serializer.deserialize(input, MessagingService.current_version), input.readInt());
        }

        public void serialize(RetryMessage message, DataOutputStreamAndChannel out, int version, StreamSession session) throws IOException
        {
            UUIDSerializer.serializer.serialize(message.cfId, out, MessagingService.current_version);
            out.writeInt(message.sequenceNumber);
        }
    };

    public final UUID cfId;
    public final int sequenceNumber;

    public RetryMessage(UUID cfId, int sequenceNumber)
    {
        super(Type.RETRY);
        this.cfId = cfId;
        this.sequenceNumber = sequenceNumber;
    }

    @Override
    public String toString()
    {
        final StringBuilder sb = new StringBuilder("Retry (");
        sb.append(cfId).append(", #").append(sequenceNumber).append(')');
        return sb.toString();
    }
#endif
public:
    void serialize(bytes::iterator& out) const {
    }
    static retry_message deserialize(bytes_view& v) {
        return retry_message();
    }
    size_t serialized_size() const {
        return 0;
    }
};

} // namespace messages
} // namespace streaming
