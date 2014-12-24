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
import java.util.*;

import io.netty.buffer.ByteBuf;

import org.apache.cassandra.transport.*;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.ReversedType;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.CqlMetadata;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.CqlResultType;
import org.apache.cassandra.thrift.CqlRow;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.service.pager.PagingState;

public class ResultSet
{
    public static final Codec codec = new Codec();
    private static final ColumnIdentifier COUNT_COLUMN = new ColumnIdentifier("count", false);

    public final Metadata metadata;
    public final List<List<ByteBuffer>> rows;

    public ResultSet(List<ColumnSpecification> metadata)
    {
        this(new Metadata(metadata), new ArrayList<List<ByteBuffer>>());
    }

    public ResultSet(Metadata metadata, List<List<ByteBuffer>> rows)
    {
        this.metadata = metadata;
        this.rows = rows;
    }

    public int size()
    {
        return rows.size();
    }

    public boolean isEmpty()
    {
        return size() == 0;
    }

    public void addRow(List<ByteBuffer> row)
    {
        assert row.size() == metadata.valueCount();
        rows.add(row);
    }

    public void addColumnValue(ByteBuffer value)
    {
        if (rows.isEmpty() || lastRow().size() == metadata.valueCount())
            rows.add(new ArrayList<ByteBuffer>(metadata.valueCount()));

        lastRow().add(value);
    }

    private List<ByteBuffer> lastRow()
    {
        return rows.get(rows.size() - 1);
    }

    public void reverse()
    {
        Collections.reverse(rows);
    }

    public void trim(int limit)
    {
        int toRemove = rows.size() - limit;
        if (toRemove > 0)
        {
            for (int i = 0; i < toRemove; i++)
                rows.remove(rows.size() - 1);
        }
    }

    public CqlResult toThriftResult()
    {
        assert metadata.names != null;

        String UTF8 = "UTF8Type";
        CqlMetadata schema = new CqlMetadata(new HashMap<ByteBuffer, String>(),
                new HashMap<ByteBuffer, String>(),
                // The 2 following ones shouldn't be needed in CQL3
                UTF8, UTF8);

        for (int i = 0; i < metadata.columnCount; i++)
        {
            ColumnSpecification spec = metadata.names.get(i);
            ByteBuffer colName = ByteBufferUtil.bytes(spec.name.toString());
            schema.name_types.put(colName, UTF8);
            AbstractType<?> normalizedType = spec.type instanceof ReversedType ? ((ReversedType)spec.type).baseType : spec.type;
            schema.value_types.put(colName, normalizedType.toString());

        }

        List<CqlRow> cqlRows = new ArrayList<CqlRow>(rows.size());
        for (List<ByteBuffer> row : rows)
        {
            List<Column> thriftCols = new ArrayList<Column>(metadata.columnCount);
            for (int i = 0; i < metadata.columnCount; i++)
            {
                Column col = new Column(ByteBufferUtil.bytes(metadata.names.get(i).name.toString()));
                col.setValue(row.get(i));
                thriftCols.add(col);
            }
            // The key of CqlRow shoudn't be needed in CQL3
            cqlRows.add(new CqlRow(ByteBufferUtil.EMPTY_BYTE_BUFFER, thriftCols));
        }
        CqlResult res = new CqlResult(CqlResultType.ROWS);
        res.setRows(cqlRows).setSchema(schema);
        return res;
    }

    @Override
    public String toString()
    {
        try
        {
            StringBuilder sb = new StringBuilder();
            sb.append(metadata).append('\n');
            for (List<ByteBuffer> row : rows)
            {
                for (int i = 0; i < row.size(); i++)
                {
                    ByteBuffer v = row.get(i);
                    if (v == null)
                    {
                        sb.append(" | null");
                    }
                    else
                    {
                        sb.append(" | ");
                        if (metadata.flags.contains(Flag.NO_METADATA))
                            sb.append("0x").append(ByteBufferUtil.bytesToHex(v));
                        else
                            sb.append(metadata.names.get(i).type.getString(v));
                    }
                }
                sb.append('\n');
            }
            sb.append("---");
            return sb.toString();
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    public static class Codec implements CBCodec<ResultSet>
    {
        /*
         * Format:
         *   - metadata
         *   - rows count (4 bytes)
         *   - rows
         */
        public ResultSet decode(ByteBuf body, int version)
        {
            Metadata m = Metadata.codec.decode(body, version);
            int rowCount = body.readInt();
            ResultSet rs = new ResultSet(m, new ArrayList<List<ByteBuffer>>(rowCount));

            // rows
            int totalValues = rowCount * m.columnCount;
            for (int i = 0; i < totalValues; i++)
                rs.addColumnValue(CBUtil.readValue(body));

            return rs;
        }

        public void encode(ResultSet rs, ByteBuf dest, int version)
        {
            Metadata.codec.encode(rs.metadata, dest, version);
            dest.writeInt(rs.rows.size());
            for (List<ByteBuffer> row : rs.rows)
            {
                // Note that we do only want to serialize only the first columnCount values, even if the row
                // as more: see comment on Metadata.names field.
                for (int i = 0; i < rs.metadata.columnCount; i++)
                    CBUtil.writeValue(row.get(i), dest);
            }
        }

        public int encodedSize(ResultSet rs, int version)
        {
            int size = Metadata.codec.encodedSize(rs.metadata, version) + 4;
            for (List<ByteBuffer> row : rs.rows)
            {
                for (int i = 0; i < rs.metadata.columnCount; i++)
                    size += CBUtil.sizeOfValue(row.get(i));
            }
            return size;
        }
    }

    public static class Metadata
    {
        public static final CBCodec<Metadata> codec = new Codec();

        public static final Metadata EMPTY = new Metadata(EnumSet.of(Flag.NO_METADATA), null, 0, null);

        private final EnumSet<Flag> flags;
        // Please note that columnCount can actually be smaller than names, even if names is not null. This is
        // used to include columns in the resultSet that we need to do post-query re-orderings
        // (SelectStatement.orderResults) but that shouldn't be sent to the user as they haven't been requested
        // (CASSANDRA-4911). So the serialization code will exclude any columns in name whose index is >= columnCount.
        public final List<ColumnSpecification> names;
        private final int columnCount;
        private PagingState pagingState;

        public Metadata(List<ColumnSpecification> names)
        {
            this(EnumSet.noneOf(Flag.class), names, names.size(), null);
            if (!names.isEmpty() && allInSameCF())
                flags.add(Flag.GLOBAL_TABLES_SPEC);
        }

        private Metadata(EnumSet<Flag> flags, List<ColumnSpecification> names, int columnCount, PagingState pagingState)
        {
            this.flags = flags;
            this.names = names;
            this.columnCount = columnCount;
            this.pagingState = pagingState;
        }

        public Metadata copy()
        {
            return new Metadata(EnumSet.copyOf(flags), names, columnCount, pagingState);
        }

        // The maximum number of values that the ResultSet can hold. This can be bigger than columnCount due to CASSANDRA-4911
        public int valueCount()
        {
            return names == null ? columnCount : names.size();
        }

        public void addNonSerializedColumn(ColumnSpecification name)
        {
            // See comment above. Because columnCount doesn't account the newly added name, it
            // won't be serialized.
            names.add(name);
        }

        private boolean allInSameCF()
        {
            if (names == null)
                return false;

            assert !names.isEmpty();

            Iterator<ColumnSpecification> iter = names.iterator();
            ColumnSpecification first = iter.next();
            while (iter.hasNext())
            {
                ColumnSpecification name = iter.next();
                if (!name.ksName.equals(first.ksName) || !name.cfName.equals(first.cfName))
                    return false;
            }
            return true;
        }

        public void setHasMorePages(PagingState pagingState)
        {
            if (pagingState == null)
                return;

            flags.add(Flag.HAS_MORE_PAGES);
            this.pagingState = pagingState;
        }

        public void setSkipMetadata()
        {
            flags.add(Flag.NO_METADATA);
        }

        @Override
        public String toString()
        {
            StringBuilder sb = new StringBuilder();

            if (names == null)
            {
                sb.append("[").append(columnCount).append(" columns]");
            }
            else
            {
                for (ColumnSpecification name : names)
                {
                    sb.append("[").append(name.name);
                    sb.append("(").append(name.ksName).append(", ").append(name.cfName).append(")");
                    sb.append(", ").append(name.type).append("]");
                }
            }
            if (flags.contains(Flag.HAS_MORE_PAGES))
                sb.append(" (to be continued)");
            return sb.toString();
        }

        private static class Codec implements CBCodec<Metadata>
        {
            public Metadata decode(ByteBuf body, int version)
            {
                // flags & column count
                int iflags = body.readInt();
                int columnCount = body.readInt();

                EnumSet<Flag> flags = Flag.deserialize(iflags);

                PagingState state = null;
                if (flags.contains(Flag.HAS_MORE_PAGES))
                    state = PagingState.deserialize(CBUtil.readValue(body));

                if (flags.contains(Flag.NO_METADATA))
                    return new Metadata(flags, null, columnCount, state);

                boolean globalTablesSpec = flags.contains(Flag.GLOBAL_TABLES_SPEC);

                String globalKsName = null;
                String globalCfName = null;
                if (globalTablesSpec)
                {
                    globalKsName = CBUtil.readString(body);
                    globalCfName = CBUtil.readString(body);
                }

                // metadata (names/types)
                List<ColumnSpecification> names = new ArrayList<ColumnSpecification>(columnCount);
                for (int i = 0; i < columnCount; i++)
                {
                    String ksName = globalTablesSpec ? globalKsName : CBUtil.readString(body);
                    String cfName = globalTablesSpec ? globalCfName : CBUtil.readString(body);
                    ColumnIdentifier colName = new ColumnIdentifier(CBUtil.readString(body), true);
                    AbstractType type = DataType.toType(DataType.codec.decodeOne(body, version));
                    names.add(new ColumnSpecification(ksName, cfName, colName, type));
                }
                return new Metadata(flags, names, names.size(), state);
            }

            public void encode(Metadata m, ByteBuf dest, int version)
            {
                boolean noMetadata = m.flags.contains(Flag.NO_METADATA);
                boolean globalTablesSpec = m.flags.contains(Flag.GLOBAL_TABLES_SPEC);
                boolean hasMorePages = m.flags.contains(Flag.HAS_MORE_PAGES);

                assert version > 1 || (!m.flags.contains(Flag.HAS_MORE_PAGES) && !noMetadata): "version = " + version + ", flags = " + m.flags;

                dest.writeInt(Flag.serialize(m.flags));
                dest.writeInt(m.columnCount);

                if (hasMorePages)
                    CBUtil.writeValue(m.pagingState.serialize(), dest);

                if (!noMetadata)
                {
                    if (globalTablesSpec)
                    {
                        CBUtil.writeString(m.names.get(0).ksName, dest);
                        CBUtil.writeString(m.names.get(0).cfName, dest);
                    }

                    for (int i = 0; i < m.columnCount; i++)
                    {
                        ColumnSpecification name = m.names.get(i);
                        if (!globalTablesSpec)
                        {
                            CBUtil.writeString(name.ksName, dest);
                            CBUtil.writeString(name.cfName, dest);
                        }
                        CBUtil.writeString(name.name.toString(), dest);
                        DataType.codec.writeOne(DataType.fromType(name.type, version), dest, version);
                    }
                }
            }

            public int encodedSize(Metadata m, int version)
            {
                boolean noMetadata = m.flags.contains(Flag.NO_METADATA);
                boolean globalTablesSpec = m.flags.contains(Flag.GLOBAL_TABLES_SPEC);
                boolean hasMorePages = m.flags.contains(Flag.HAS_MORE_PAGES);

                int size = 8;
                if (hasMorePages)
                    size += CBUtil.sizeOfValue(m.pagingState.serialize());

                if (!noMetadata)
                {
                    if (globalTablesSpec)
                    {
                        size += CBUtil.sizeOfString(m.names.get(0).ksName);
                        size += CBUtil.sizeOfString(m.names.get(0).cfName);
                    }

                    for (int i = 0; i < m.columnCount; i++)
                    {
                        ColumnSpecification name = m.names.get(i);
                        if (!globalTablesSpec)
                        {
                            size += CBUtil.sizeOfString(name.ksName);
                            size += CBUtil.sizeOfString(name.cfName);
                        }
                        size += CBUtil.sizeOfString(name.name.toString());
                        size += DataType.codec.oneSerializedSize(DataType.fromType(name.type, version), version);
                    }
                }
                return size;
            }
        }
    }

    public static enum Flag
    {
        // The order of that enum matters!!
        GLOBAL_TABLES_SPEC,
        HAS_MORE_PAGES,
        NO_METADATA;

        public static EnumSet<Flag> deserialize(int flags)
        {
            EnumSet<Flag> set = EnumSet.noneOf(Flag.class);
            Flag[] values = Flag.values();
            for (int n = 0; n < values.length; n++)
            {
                if ((flags & (1 << n)) != 0)
                    set.add(values[n]);
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
}
