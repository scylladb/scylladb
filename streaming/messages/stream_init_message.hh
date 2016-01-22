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

#include "core/sstring.hh"
#include "bytes.hh"
#include "gms/inet_address.hh"
#include "utils/UUID.hh"

namespace streaming {
namespace messages {


/**
 * StreamInitMessage is first sent from the node where {@link org.apache.cassandra.streaming.StreamSession} is started,
 * to initiate corresponding {@link org.apache.cassandra.streaming.StreamSession} on the other side.
 */
class stream_init_message {
public:
    using inet_address = gms::inet_address;
    using UUID = utils::UUID;
    UUID plan_id;
    sstring description;

    stream_init_message() = default;

    stream_init_message(UUID _plan_id, sstring _description)
        : plan_id(_plan_id)
        , description(_description) {
    }

#if 0
    /**
     * Create serialized message.
     *
     * @param compress true if message is compressed
     * @param version Streaming protocol version
     * @return serialized message in ByteBuffer format
     */
    public ByteBuffer createMessage(boolean compress, int version)
    {
        int header = 0;
        // set compression bit.
        if (compress)
            header |= 4;
        // set streaming bit
        header |= 8;
        // Setting up the version bit
        header |= (version << 8);

        byte[] bytes;
        try
        {
            int size = (int)StreamInitMessage.serializer.serializedSize(this, version);
            DataOutputBuffer buffer = new DataOutputBuffer(size);
            StreamInitMessage.serializer.serialize(this, buffer, version);
            bytes = buffer.getData();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        assert bytes.length > 0;

        ByteBuffer buffer = ByteBuffer.allocate(4 + 4 + bytes.length);
        buffer.putInt(MessagingService.PROTOCOL_MAGIC);
        buffer.putInt(header);
        buffer.put(bytes);
        buffer.flip();
        return buffer;
    }
#endif
public:
    void serialize(bytes::iterator& out) const;
    static stream_init_message deserialize(bytes_view& v);
    size_t serialized_size() const;
};

} // namespace messages
} // namespace streaming
