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
package org.apache.cassandra.cql3;

import java.nio.ByteBuffer;

import org.apache.cassandra.db.ExpiringCell;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.serializers.MarshalException;

/**
 * Utility class for the Parser to gather attributes for modification
 * statements.
 */
public class Attributes
{
    private final Term timestamp;
    private final Term timeToLive;

    public static Attributes none()
    {
        return new Attributes(null, null);
    }

    private Attributes(Term timestamp, Term timeToLive)
    {
        this.timestamp = timestamp;
        this.timeToLive = timeToLive;
    }

    public boolean usesFunction(String ksName, String functionName)
    {
        return (timestamp != null && timestamp.usesFunction(ksName, functionName))
            || (timeToLive != null && timeToLive.usesFunction(ksName, functionName));
    }

    public boolean isTimestampSet()
    {
        return timestamp != null;
    }

    public boolean isTimeToLiveSet()
    {
        return timeToLive != null;
    }

    public long getTimestamp(long now, QueryOptions options) throws InvalidRequestException
    {
        if (timestamp == null)
            return now;

        ByteBuffer tval = timestamp.bindAndGet(options);
        if (tval == null)
            throw new InvalidRequestException("Invalid null value of timestamp");

        try
        {
            LongType.instance.validate(tval);
        }
        catch (MarshalException e)
        {
            throw new InvalidRequestException("Invalid timestamp value");
        }

        return LongType.instance.compose(tval);
    }

    public int getTimeToLive(QueryOptions options) throws InvalidRequestException
    {
        if (timeToLive == null)
            return 0;

        ByteBuffer tval = timeToLive.bindAndGet(options);
        if (tval == null)
            throw new InvalidRequestException("Invalid null value of TTL");

        try
        {
            Int32Type.instance.validate(tval);
        }
        catch (MarshalException e)
        {
            throw new InvalidRequestException("Invalid timestamp value");
        }

        int ttl = Int32Type.instance.compose(tval);
        if (ttl < 0)
            throw new InvalidRequestException("A TTL must be greater or equal to 0");

        if (ttl > ExpiringCell.MAX_TTL)
            throw new InvalidRequestException(String.format("ttl is too large. requested (%d) maximum (%d)", ttl, ExpiringCell.MAX_TTL));

        return ttl;
    }

    public void collectMarkerSpecification(VariableSpecifications boundNames)
    {
        if (timestamp != null)
            timestamp.collectMarkerSpecification(boundNames);
        if (timeToLive != null)
            timeToLive.collectMarkerSpecification(boundNames);
    }

    public static class Raw
    {
        public Term.Raw timestamp;
        public Term.Raw timeToLive;

        public Attributes prepare(String ksName, String cfName) throws InvalidRequestException
        {
            Term ts = timestamp == null ? null : timestamp.prepare(ksName, timestampReceiver(ksName, cfName));
            Term ttl = timeToLive == null ? null : timeToLive.prepare(ksName, timeToLiveReceiver(ksName, cfName));
            return new Attributes(ts, ttl);
        }

        private ColumnSpecification timestampReceiver(String ksName, String cfName)
        {
            return new ColumnSpecification(ksName, cfName, new ColumnIdentifier("[timestamp]", true), LongType.instance);
        }

        private ColumnSpecification timeToLiveReceiver(String ksName, String cfName)
        {
            return new ColumnSpecification(ksName, cfName, new ColumnIdentifier("[ttl]", true), Int32Type.instance);
        }
    }
}
