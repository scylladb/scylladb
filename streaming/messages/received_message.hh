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

namespace streaming {
namespace messages {

class received_message : public stream_message {
public:
    using UUID = utils::UUID;
#if 0
    public static Serializer<ReceivedMessage> serializer = new Serializer<ReceivedMessage>()
    {
        public ReceivedMessage deserialize(ReadableByteChannel in, int version, StreamSession session) throws IOException
        {
            DataInput input = new DataInputStream(Channels.newInputStream(in));
            return new ReceivedMessage(UUIDSerializer.serializer.deserialize(input, MessagingService.current_version), input.readInt());
        }

        public void serialize(ReceivedMessage message, DataOutputStreamAndChannel out, int version, StreamSession session) throws IOException
        {
            UUIDSerializer.serializer.serialize(message.cfId, out, MessagingService.current_version);
            out.writeInt(message.sequenceNumber);
        }
    };
#endif
    UUID cf_id;
    int sequence_number;

    received_message() = default;
    received_message(UUID cf_id_, int sequence_number_)
        : stream_message(stream_message::Type::RECEIVED)
        , cf_id (cf_id_)
        , sequence_number(sequence_number_) {
    }
#if 0
    @Override
    public String toString()
    {
        final StringBuilder sb = new StringBuilder("Received (");
        sb.append(cfId).append(", #").append(sequenceNumber).append(')');
        return sb.toString();
    }
#endif
public:
    void serialize(bytes::iterator& out) const {
    }
    static received_message deserialize(bytes_view& v) {
        return received_message();
    }
    size_t serialized_size() const {
        return 0;
    }
};

} // namespace messages
} // namespace streaming
