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
package org.apache.cassandra.db;

import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.cassandra.dht.*;
import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;

public interface RowPosition extends RingPosition<RowPosition>
{
    public static enum Kind
    {
        // Only add new values to the end of the enum, the ordinal is used
        // during serialization
        ROW_KEY, MIN_BOUND, MAX_BOUND;

        private static final Kind[] allKinds = Kind.values();

        static Kind fromOrdinal(int ordinal)
        {
            return allKinds[ordinal];
        }
    }

    public static final class ForKey
    {
        public static RowPosition get(ByteBuffer key, IPartitioner p)
        {
            return key == null || key.remaining() == 0 ? p.getMinimumToken().minKeyBound() : p.decorateKey(key);
        }
    }

    public static final RowPositionSerializer serializer = new RowPositionSerializer();

    public Kind kind();
    public boolean isMinimum();

    public static class RowPositionSerializer implements ISerializer<RowPosition>
    {
        /*
         * We need to be able to serialize both Token.KeyBound and
         * DecoratedKey. To make this compact, we first write a byte whose
         * meaning is:
         *   - 0: DecoratedKey
         *   - 1: a 'minimum' Token.KeyBound
         *   - 2: a 'maximum' Token.KeyBound
         * In the case of the DecoratedKey, we then serialize the key (the
         * token is recreated on the other side). In the other cases, we then
         * serialize the token.
         */
        public void serialize(RowPosition pos, DataOutputPlus out) throws IOException
        {
            Kind kind = pos.kind();
            out.writeByte(kind.ordinal());
            if (kind == Kind.ROW_KEY)
                ByteBufferUtil.writeWithShortLength(((DecoratedKey)pos).getKey(), out);
            else
                Token.serializer.serialize(pos.getToken(), out);
        }

        public RowPosition deserialize(DataInput in) throws IOException
        {
            Kind kind = Kind.fromOrdinal(in.readByte());
            if (kind == Kind.ROW_KEY)
            {
                ByteBuffer k = ByteBufferUtil.readWithShortLength(in);
                return StorageService.getPartitioner().decorateKey(k);
            }
            else
            {
                Token t = Token.serializer.deserialize(in);
                return kind == Kind.MIN_BOUND ? t.minKeyBound() : t.maxKeyBound();
            }
        }

        public long serializedSize(RowPosition pos, TypeSizes typeSizes)
        {
            Kind kind = pos.kind();
            int size = 1; // 1 byte for enum
            if (kind == Kind.ROW_KEY)
            {
                int keySize = ((DecoratedKey)pos).getKey().remaining();
                size += typeSizes.sizeof((short) keySize) + keySize;
            }
            else
            {
                size += Token.serializer.serializedSize(pos.getToken(), typeSizes);
            }
            return size;
        }
    }
}
