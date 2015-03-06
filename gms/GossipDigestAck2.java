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

import java.io.*;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.CompactEndpointSerializationHelper;

/**
 * This ack gets sent out as a result of the receipt of a GossipDigestAckMessage. This the
 * last stage of the 3 way messaging of the Gossip protocol.
 */
public class GossipDigestAck2
{
    public static final IVersionedSerializer<GossipDigestAck2> serializer = new GossipDigestAck2Serializer();

    final Map<InetAddress, EndpointState> epStateMap;

    GossipDigestAck2(Map<InetAddress, EndpointState> epStateMap)
    {
        this.epStateMap = epStateMap;
    }

    Map<InetAddress, EndpointState> getEndpointStateMap()
    {
        return epStateMap;
    }
}

class GossipDigestAck2Serializer implements IVersionedSerializer<GossipDigestAck2>
{
    public void serialize(GossipDigestAck2 ack2, DataOutputPlus out, int version) throws IOException
    {
        out.writeInt(ack2.epStateMap.size());
        for (Map.Entry<InetAddress, EndpointState> entry : ack2.epStateMap.entrySet())
        {
            InetAddress ep = entry.getKey();
            CompactEndpointSerializationHelper.serialize(ep, out);
            EndpointState.serializer.serialize(entry.getValue(), out, version);
        }
    }

    public GossipDigestAck2 deserialize(DataInput in, int version) throws IOException
    {
        int size = in.readInt();
        Map<InetAddress, EndpointState> epStateMap = new HashMap<InetAddress, EndpointState>(size);

        for (int i = 0; i < size; ++i)
        {
            InetAddress ep = CompactEndpointSerializationHelper.deserialize(in);
            EndpointState epState = EndpointState.serializer.deserialize(in, version);
            epStateMap.put(ep, epState);
        }
        return new GossipDigestAck2(epStateMap);
    }

    public long serializedSize(GossipDigestAck2 ack2, int version)
    {
        long size = TypeSizes.NATIVE.sizeof(ack2.epStateMap.size());
        for (Map.Entry<InetAddress, EndpointState> entry : ack2.epStateMap.entrySet())
            size += CompactEndpointSerializationHelper.serializedSize(entry.getKey())
                    + EndpointState.serializer.serializedSize(entry.getValue(), version);
        return size;
    }
}

