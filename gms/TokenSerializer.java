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
package org.apache.cassandra.gms;

import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;


public class TokenSerializer
{
    private static final Logger logger = LoggerFactory.getLogger(TokenSerializer.class);

    public static void serialize(IPartitioner partitioner, Collection<Token> tokens, DataOutput out) throws IOException
    {
        for (Token token : tokens)
        {
            byte[] bintoken = partitioner.getTokenFactory().toByteArray(token).array();
            out.writeInt(bintoken.length);
            out.write(bintoken);
        }
        out.writeInt(0);
    }

    public static Collection<Token> deserialize(IPartitioner partitioner, DataInput in) throws IOException
    {
        Collection<Token> tokens = new ArrayList<Token>();
        while (true)
        {
            int size = in.readInt();
            if (size < 1)
                break;
            logger.trace("Reading token of {} bytes", size);
            byte[] bintoken = new byte[size];
            in.readFully(bintoken);
            tokens.add(partitioner.getTokenFactory().fromByteArray(ByteBuffer.wrap(bintoken)));
        }
        return tokens;
    }
}