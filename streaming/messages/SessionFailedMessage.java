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

import org.apache.cassandra.io.util.DataOutputStreamAndChannel;
import org.apache.cassandra.streaming.StreamSession;

public class SessionFailedMessage extends StreamMessage
{
    public static Serializer<SessionFailedMessage> serializer = new Serializer<SessionFailedMessage>()
    {
        public SessionFailedMessage deserialize(ReadableByteChannel in, int version, StreamSession session) throws IOException
        {
            return new SessionFailedMessage();
        }

        public void serialize(SessionFailedMessage message, DataOutputStreamAndChannel out, int version, StreamSession session) throws IOException {}
    };

    public SessionFailedMessage()
    {
        super(Type.SESSION_FAILED);
    }

    @Override
    public String toString()
    {
        return "Session Failed";
    }
}
