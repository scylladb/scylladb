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
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

import org.apache.cassandra.io.util.DataOutputStreamAndChannel;
import org.apache.cassandra.streaming.StreamSession;

/**
 * StreamMessage is an abstract base class that every messages in streaming protocol inherit.
 *
 * Every message carries message type({@link Type}) and streaming protocol version byte.
 */
public abstract class StreamMessage
{
    /** Streaming protocol version */
    public static final int VERSION_20 = 2;
    public static final int VERSION_30 = 3;
    public static final int CURRENT_VERSION = VERSION_30;

    public static void serialize(StreamMessage message, DataOutputStreamAndChannel out, int version, StreamSession session) throws IOException
    {
        ByteBuffer buff = ByteBuffer.allocate(1);
        // message type
        buff.put(message.type.type);
        buff.flip();
        out.write(buff);
        message.type.outSerializer.serialize(message, out, version, session);
    }

    public static StreamMessage deserialize(ReadableByteChannel in, int version, StreamSession session) throws IOException
    {
        ByteBuffer buff = ByteBuffer.allocate(1);
        if (in.read(buff) > 0)
        {
            buff.flip();
            Type type = Type.get(buff.get());
            return type.inSerializer.deserialize(in, version, session);
        }
        else
        {
            // when socket gets closed, there is a chance that buff is empty
            // in that case, just return null
            return null;
        }
    }

    /** StreamMessage serializer */
    public static interface Serializer<V extends StreamMessage>
    {
        V deserialize(ReadableByteChannel in, int version, StreamSession session) throws IOException;
        void serialize(V message, DataOutputStreamAndChannel out, int version, StreamSession session) throws IOException;
    }

    /** StreamMessage types */
    public static enum Type
    {
        PREPARE(1, 5, PrepareMessage.serializer),
        FILE(2, 0, IncomingFileMessage.serializer, OutgoingFileMessage.serializer),
        RECEIVED(3, 4, ReceivedMessage.serializer),
        RETRY(4, 4, RetryMessage.serializer),
        COMPLETE(5, 1, CompleteMessage.serializer),
        SESSION_FAILED(6, 5, SessionFailedMessage.serializer);

        public static Type get(byte type)
        {
            for (Type t : Type.values())
            {
                if (t.type == type)
                    return t;
            }
            throw new IllegalArgumentException("Unknown type " + type);
        }

        private final byte type;
        public final int priority;
        public final Serializer<StreamMessage> inSerializer;
        public final Serializer<StreamMessage> outSerializer;

        @SuppressWarnings("unchecked")
        private Type(int type, int priority, Serializer serializer)
        {
            this(type, priority, serializer, serializer);
        }

        @SuppressWarnings("unchecked")
        private Type(int type, int priority, Serializer inSerializer, Serializer outSerializer)
        {
            this.type = (byte) type;
            this.priority = priority;
            this.inSerializer = inSerializer;
            this.outSerializer = outSerializer;
        }
    }

    public final Type type;

    protected StreamMessage(Type type)
    {
        this.type = type;
    }

    /**
     * @return priority of this message. higher value, higher priority.
     */
    public int getPriority()
    {
        return type.priority;
    }
}
