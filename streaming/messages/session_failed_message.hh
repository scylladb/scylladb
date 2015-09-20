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

#include "streaming/messages/stream_message.hh"

namespace streaming {
namespace messages {

class session_failed_message : public stream_message {
#if 0
    public static Serializer<SessionFailedMessage> serializer = new Serializer<SessionFailedMessage>()
    {
        public SessionFailedMessage deserialize(ReadableByteChannel in, int version, StreamSession session) throws IOException
        {
            return new SessionFailedMessage();
        }

        public void serialize(SessionFailedMessage message, DataOutputStreamAndChannel out, int version, StreamSession session) throws IOException {}
    };
#endif

    session_failed_message() : stream_message(stream_message::Type::SESSION_FAILED) { }

#if 0
    @Override
    public String toString()
    {
        return "Session Failed";
    }
#endif
public:
    void serialize(bytes::iterator& out) const {
    }
    static session_failed_message deserialize(bytes_view& v) {
        return session_failed_message();
    }
    size_t serialized_size() const {
        return 0;
    }
};

} // namespace messages
} // namespace streaming
