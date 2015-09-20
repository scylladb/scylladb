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

/*
 * Copyright 2015 Cloudius Systems
 *
 * Modified by Cloudius Systems
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

#include "unimplemented.hh"
#include "bytes.hh"

namespace service {

namespace pager {

class paging_state final {
#if 0
    public final ByteBuffer partitionKey;
    public final ByteBuffer cellName;
    public final int remaining;

    public PagingState(ByteBuffer partitionKey, ByteBuffer cellName, int remaining)
    {
        this.partitionKey = partitionKey == null ? ByteBufferUtil.EMPTY_BYTE_BUFFER : partitionKey;
        this.cellName = cellName == null ? ByteBufferUtil.EMPTY_BYTE_BUFFER : cellName;
        this.remaining = remaining;
    }

    public static PagingState deserialize(ByteBuffer bytes)
    {
        if (bytes == null)
            return null;

        try
        {
            DataInputStream in = new DataInputStream(ByteBufferUtil.inputStream(bytes));
            ByteBuffer pk = ByteBufferUtil.readWithShortLength(in);
            ByteBuffer cn = ByteBufferUtil.readWithShortLength(in);
            int remaining = in.readInt();
            return new PagingState(pk, cn, remaining);
        }
        catch (IOException e)
        {
            throw new ProtocolException("Invalid value for the paging state");
        }
    }
#endif

public:

    bytes_opt serialize() {
        fail(unimplemented::cause::PAGING);
#if 0
        try
        {
            DataOutputBuffer out = new DataOutputBuffer(serializedSize());
            ByteBufferUtil.writeWithShortLength(partitionKey, out);
            ByteBufferUtil.writeWithShortLength(cellName, out);
            out.writeInt(remaining);
            return out.asByteBuffer();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
#endif
    }

#if 0
    private int serializedSize()
    {
        return 2 + partitionKey.remaining()
             + 2 + cellName.remaining()
             + 4;
    }

    @Override
    public String toString()
    {
        return String.format("PagingState(key=%s, cellname=%s, remaining=%d", ByteBufferUtil.bytesToHex(partitionKey), ByteBufferUtil.bytesToHex(cellName), remaining);
    }
#endif
};

}

}
