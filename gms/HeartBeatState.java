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

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;

/**
 * HeartBeat State associated with any given endpoint.
 */
class HeartBeatState
{
    public static final IVersionedSerializer<HeartBeatState> serializer = new HeartBeatStateSerializer();

    private int generation;
    private int version;

    HeartBeatState(int gen)
    {
        this(gen, 0);
    }

    HeartBeatState(int gen, int ver)
    {
        generation = gen;
        version = ver;
    }

    int getGeneration()
    {
        return generation;
    }

    void updateHeartBeat()
    {
        version = VersionGenerator.getNextVersion();
    }

    int getHeartBeatVersion()
    {
        return version;
    }

    void forceNewerGenerationUnsafe()
    {
        generation += 1;
    }

    public String toString()
    {
        return String.format("HeartBeat: generation = %d, version = %d", generation, version);
    }
}

class HeartBeatStateSerializer implements IVersionedSerializer<HeartBeatState>
{
    public void serialize(HeartBeatState hbState, DataOutputPlus out, int version) throws IOException
    {
        out.writeInt(hbState.getGeneration());
        out.writeInt(hbState.getHeartBeatVersion());
    }

    public HeartBeatState deserialize(DataInput in, int version) throws IOException
    {
        return new HeartBeatState(in.readInt(), in.readInt());
    }

    public long serializedSize(HeartBeatState state, int version)
    {
        return TypeSizes.NATIVE.sizeof(state.getGeneration()) + TypeSizes.NATIVE.sizeof(state.getHeartBeatVersion());
    }
}
