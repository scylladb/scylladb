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
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;

import io.netty.buffer.ByteBuf;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.pager.PagingState;
import org.apache.cassandra.transport.CBCodec;
import org.apache.cassandra.transport.CBUtil;
import org.apache.cassandra.transport.ProtocolException;
import org.apache.cassandra.utils.Pair;

/**
 * Options for a query.
 */
public abstract class QueryOptions
{
    public static final QueryOptions DEFAULT = new DefaultQueryOptions(ConsistencyLevel.ONE,
                                                                       Collections.<ByteBuffer>emptyList(),
                                                                       false,
                                                                       SpecificOptions.DEFAULT,
                                                                       3);

    public static final CBCodec<QueryOptions> codec = new Codec();

    public static QueryOptions fromProtocolV1(ConsistencyLevel consistency, List<ByteBuffer> values)
    {
        return new DefaultQueryOptions(consistency, values, false, SpecificOptions.DEFAULT, 1);
    }

    public static QueryOptions fromProtocolV2(ConsistencyLevel consistency, List<ByteBuffer> values)
    {
        return new DefaultQueryOptions(consistency, values, false, SpecificOptions.DEFAULT, 2);
    }

    public static QueryOptions forInternalCalls(ConsistencyLevel consistency, List<ByteBuffer> values)
    {
        return new DefaultQueryOptions(consistency, values, false, SpecificOptions.DEFAULT, 3);
    }

    public static QueryOptions forInternalCalls(List<ByteBuffer> values)
    {
        return new DefaultQueryOptions(ConsistencyLevel.ONE, values, false, SpecificOptions.DEFAULT, 3);
    }

    public static QueryOptions fromPreV3Batch(ConsistencyLevel consistency)
    {
        return new DefaultQueryOptions(consistency, Collections.<ByteBuffer>emptyList(), false, SpecificOptions.DEFAULT, 2);
    }

    public static QueryOptions create(ConsistencyLevel consistency, List<ByteBuffer> values, boolean skipMetadata, int pageSize, PagingState pagingState, ConsistencyLevel serialConsistency)
    {
        return new DefaultQueryOptions(consistency, values, skipMetadata, new SpecificOptions(pageSize, pagingState, serialConsistency, -1L), 0);
    }

    public abstract ConsistencyLevel getConsistency();
    public abstract List<ByteBuffer> getValues();
    public abstract boolean skipMetadata();

    /**  The pageSize for this query. Will be <= 0 if not relevant for the query.  */
    public int getPageSize()
    {
        return getSpecificOptions().pageSize;
    }

    /** The paging state for this query, or null if not relevant. */
    public PagingState getPagingState()
    {
        return getSpecificOptions().state;
    }

    /**  Serial consistency for conditional updates. */
    public ConsistencyLevel getSerialConsistency()
    {
        return getSpecificOptions().serialConsistency;
    }

    public long getTimestamp(QueryState state)
    {
        long tstamp = getSpecificOptions().timestamp;
        return tstamp != Long.MIN_VALUE ? tstamp : state.getTimestamp();
    }

    /**
     * The protocol version for the query. Will be 3 if the object don't come from
     * a native protocol request (i.e. it's been allocated locally or by CQL-over-thrift).
     */
    public abstract int getProtocolVersion();

    // Mainly for the sake of BatchQueryOptions
    abstract SpecificOptions getSpecificOptions();

    public QueryOptions prepare(List<ColumnSpecification> specs)
    {
        return this;
    }

    static class DefaultQueryOptions extends QueryOptions
    {
        private final ConsistencyLevel consistency;
        private final List<ByteBuffer> values;
        private final boolean skipMetadata;

        private final SpecificOptions options;

        private final transient int protocolVersion;

        DefaultQueryOptions(ConsistencyLevel consistency, List<ByteBuffer> values, boolean skipMetadata, SpecificOptions options, int protocolVersion)
        {
            this.consistency = consistency;
            this.values = values;
            this.skipMetadata = skipMetadata;
            this.options = options;
            this.protocolVersion = protocolVersion;
        }

        public ConsistencyLevel getConsistency()
        {
            return consistency;
        }

        public List<ByteBuffer> getValues()
        {
            return values;
        }

        public boolean skipMetadata()
        {
            return skipMetadata;
        }

        public int getProtocolVersion()
        {
            return protocolVersion;
        }

        SpecificOptions getSpecificOptions()
        {
            return options;
        }
    }

    static abstract class QueryOptionsWrapper extends QueryOptions
    {
        protected final QueryOptions wrapped;

        QueryOptionsWrapper(QueryOptions wrapped)
        {
            this.wrapped = wrapped;
        }

        public ConsistencyLevel getConsistency()
        {
            return wrapped.getConsistency();
        }

        public boolean skipMetadata()
        {
            return wrapped.skipMetadata();
        }

        public int getProtocolVersion()
        {
            return wrapped.getProtocolVersion();
        }

        SpecificOptions getSpecificOptions()
        {
            return wrapped.getSpecificOptions();
        }

        @Override
        public QueryOptions prepare(List<ColumnSpecification> specs)
        {
            wrapped.prepare(specs);
            return this;
        }
    }

    static class OptionsWithNames extends QueryOptionsWrapper
    {
        private final List<String> names;
        private List<ByteBuffer> orderedValues;

        OptionsWithNames(DefaultQueryOptions wrapped, List<String> names)
        {
            super(wrapped);
            this.names = names;
        }

        @Override
        public QueryOptions prepare(List<ColumnSpecification> specs)
        {
            super.prepare(specs);

            orderedValues = new ArrayList<ByteBuffer>(specs.size());
            for (int i = 0; i < specs.size(); i++)
            {
                String name = specs.get(i).name.toString();
                for (int j = 0; j < names.size(); j++)
                {
                    if (name.equals(names.get(j)))
                    {
                        orderedValues.add(wrapped.getValues().get(j));
                        break;
                    }
                }
            }
            return this;
        }

        public List<ByteBuffer> getValues()
        {
            assert orderedValues != null; // We should have called prepare first!
            return orderedValues;
        }
    }

    // Options that are likely to not be present in most queries
    static class SpecificOptions
    {
        private static final SpecificOptions DEFAULT = new SpecificOptions(-1, null, null, Long.MIN_VALUE);

        private final int pageSize;
        private final PagingState state;
        private final ConsistencyLevel serialConsistency;
        private final long timestamp;

        private SpecificOptions(int pageSize, PagingState state, ConsistencyLevel serialConsistency, long timestamp)
        {
            this.pageSize = pageSize;
            this.state = state;
            this.serialConsistency = serialConsistency == null ? ConsistencyLevel.SERIAL : serialConsistency;
            this.timestamp = timestamp;
        }
    }

    private static class Codec implements CBCodec<QueryOptions>
    {
        private static enum Flag
        {
            // The order of that enum matters!!
            VALUES,
            SKIP_METADATA,
            PAGE_SIZE,
            PAGING_STATE,
            SERIAL_CONSISTENCY,
            TIMESTAMP,
            NAMES_FOR_VALUES;

            private static final Flag[] ALL_VALUES = values();

            public static EnumSet<Flag> deserialize(int flags)
            {
                EnumSet<Flag> set = EnumSet.noneOf(Flag.class);
                for (int n = 0; n < ALL_VALUES.length; n++)
                {
                    if ((flags & (1 << n)) != 0)
                        set.add(ALL_VALUES[n]);
                }
                return set;
            }

            public static int serialize(EnumSet<Flag> flags)
            {
                int i = 0;
                for (Flag flag : flags)
                    i |= 1 << flag.ordinal();
                return i;
            }
        }

        public QueryOptions decode(ByteBuf body, int version)
        {
            assert version >= 2;

            ConsistencyLevel consistency = CBUtil.readConsistencyLevel(body);
            EnumSet<Flag> flags = Flag.deserialize((int)body.readByte());

            List<ByteBuffer> values = Collections.<ByteBuffer>emptyList();
            List<String> names = null;
            if (flags.contains(Flag.VALUES))
            {
                if (flags.contains(Flag.NAMES_FOR_VALUES))
                {
                    Pair<List<String>, List<ByteBuffer>> namesAndValues = CBUtil.readNameAndValueList(body);
                    names = namesAndValues.left;
                    values = namesAndValues.right;
                }
                else
                {
                    values = CBUtil.readValueList(body);
                }
            }

            boolean skipMetadata = flags.contains(Flag.SKIP_METADATA);
            flags.remove(Flag.VALUES);
            flags.remove(Flag.SKIP_METADATA);

            SpecificOptions options = SpecificOptions.DEFAULT;
            if (!flags.isEmpty())
            {
                int pageSize = flags.contains(Flag.PAGE_SIZE) ? body.readInt() : -1;
                PagingState pagingState = flags.contains(Flag.PAGING_STATE) ? PagingState.deserialize(CBUtil.readValue(body)) : null;
                ConsistencyLevel serialConsistency = flags.contains(Flag.SERIAL_CONSISTENCY) ? CBUtil.readConsistencyLevel(body) : ConsistencyLevel.SERIAL;
                long timestamp = Long.MIN_VALUE;
                if (flags.contains(Flag.TIMESTAMP))
                {
                    long ts = body.readLong();
                    if (ts == Long.MIN_VALUE)
                        throw new ProtocolException(String.format("Out of bound timestamp, must be in [%d, %d] (got %d)", Long.MIN_VALUE + 1, Long.MAX_VALUE, ts));
                    timestamp = ts;
                }

                options = new SpecificOptions(pageSize, pagingState, serialConsistency, timestamp);
            }
            DefaultQueryOptions opts = new DefaultQueryOptions(consistency, values, skipMetadata, options, version);
            return names == null ? opts : new OptionsWithNames(opts, names);
        }

        public void encode(QueryOptions options, ByteBuf dest, int version)
        {
            assert version >= 2;

            CBUtil.writeConsistencyLevel(options.getConsistency(), dest);

            EnumSet<Flag> flags = gatherFlags(options);
            dest.writeByte((byte)Flag.serialize(flags));

            if (flags.contains(Flag.VALUES))
                CBUtil.writeValueList(options.getValues(), dest);
            if (flags.contains(Flag.PAGE_SIZE))
                dest.writeInt(options.getPageSize());
            if (flags.contains(Flag.PAGING_STATE))
                CBUtil.writeValue(options.getPagingState().serialize(), dest);
            if (flags.contains(Flag.SERIAL_CONSISTENCY))
                CBUtil.writeConsistencyLevel(options.getSerialConsistency(), dest);
            if (flags.contains(Flag.TIMESTAMP))
                dest.writeLong(options.getSpecificOptions().timestamp);

            // Note that we don't really have to bother with NAMES_FOR_VALUES server side,
            // and in fact we never really encode QueryOptions, only decode them, so we
            // don't bother.
        }

        public int encodedSize(QueryOptions options, int version)
        {
            int size = 0;

            size += CBUtil.sizeOfConsistencyLevel(options.getConsistency());

            EnumSet<Flag> flags = gatherFlags(options);
            size += 1;

            if (flags.contains(Flag.VALUES))
                size += CBUtil.sizeOfValueList(options.getValues());
            if (flags.contains(Flag.PAGE_SIZE))
                size += 4;
            if (flags.contains(Flag.PAGING_STATE))
                size += CBUtil.sizeOfValue(options.getPagingState().serialize());
            if (flags.contains(Flag.SERIAL_CONSISTENCY))
                size += CBUtil.sizeOfConsistencyLevel(options.getSerialConsistency());
            if (flags.contains(Flag.TIMESTAMP))
                size += 8;

            return size;
        }

        private EnumSet<Flag> gatherFlags(QueryOptions options)
        {
            EnumSet<Flag> flags = EnumSet.noneOf(Flag.class);
            if (options.getValues().size() > 0)
                flags.add(Flag.VALUES);
            if (options.skipMetadata())
                flags.add(Flag.SKIP_METADATA);
            if (options.getPageSize() >= 0)
                flags.add(Flag.PAGE_SIZE);
            if (options.getPagingState() != null)
                flags.add(Flag.PAGING_STATE);
            if (options.getSerialConsistency() != ConsistencyLevel.SERIAL)
                flags.add(Flag.SERIAL_CONSISTENCY);
            if (options.getSpecificOptions().timestamp != Long.MIN_VALUE)
                flags.add(Flag.TIMESTAMP);
            return flags;
        }
    }
}
