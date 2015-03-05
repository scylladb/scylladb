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

import java.io.DataInput;
import java.io.IOException;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.CompactEndpointSerializationHelper;

/**
 * This ack gets sent out as a result of the receipt of a GossipDigestSynMessage by an
 * endpoint. This is the 2 stage of the 3 way messaging in the Gossip protocol.
 */
public class GossipDigestAck
{
    public static final IVersionedSerializer<GossipDigestAck> serializer = new GossipDigestAckSerializer();

    final List<GossipDigest> gDigestList;
    final Map<InetAddress, EndpointState> epStateMap;

    GossipDigestAck(List<GossipDigest> gDigestList, Map<InetAddress, EndpointState> epStateMap)
    {
        this.gDigestList = gDigestList;
        this.epStateMap = epStateMap;
    }

    List<GossipDigest> getGossipDigestList()
    {
        return gDigestList;
    }

    Map<InetAddress, EndpointState> getEndpointStateMap()
    {
        return epStateMap;
    }
}

class GossipDigestAckSerializer implements IVersionedSerializer<GossipDigestAck>
{
    public void serialize(GossipDigestAck gDigestAckMessage, DataOutputPlus out, int version) throws IOException
    {
        GossipDigestSerializationHelper.serialize(gDigestAckMessage.gDigestList, out, version);
        out.writeInt(gDigestAckMessage.epStateMap.size());
        for (Map.Entry<InetAddress, EndpointState> entry : gDigestAckMessage.epStateMap.entrySet())
        {
            InetAddress ep = entry.getKey();
            CompactEndpointSerializationHelper.serialize(ep, out);
            EndpointState.serializer.serialize(entry.getValue(), out, version);
        }
    }

    public GossipDigestAck deserialize(DataInput in, int version) throws IOException
    {
        List<GossipDigest> gDigestList = GossipDigestSerializationHelper.deserialize(in, version);
        int size = in.readInt();
        Map<InetAddress, EndpointState> epStateMap = new HashMap<InetAddress, EndpointState>(size);

        for (int i = 0; i < size; ++i)
        {
            InetAddress ep = CompactEndpointSerializationHelper.deserialize(in);
            EndpointState epState = EndpointState.serializer.deserialize(in, version);
            epStateMap.put(ep, epState);
        }
        return new GossipDigestAck(gDigestList, epStateMap);
    }

    public long serializedSize(GossipDigestAck ack, int version)
    {
        int size = GossipDigestSerializationHelper.serializedSize(ack.gDigestList, version);
        size += TypeSizes.NATIVE.sizeof(ack.epStateMap.size());
        for (Map.Entry<InetAddress, EndpointState> entry : ack.epStateMap.entrySet())
            size += CompactEndpointSerializationHelper.serializedSize(entry.getKey())
                    + EndpointState.serializer.serializedSize(entry.getValue(), version);
        return size;
    }
}
