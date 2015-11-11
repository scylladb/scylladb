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

#include <deque>
#include <vector>
#include "enum_set.hh"
#include "service/pager/paging_state.hh"
#include "schema.hh"

namespace cql3 {

class metadata {
#if 0
        public static final CBCodec<Metadata> codec = new Codec();
        public static final Metadata EMPTY = new Metadata(EnumSet.of(Flag.NO_METADATA), null, 0, null);
#endif
public:
    enum class flag : uint8_t {
        GLOBAL_TABLES_SPEC,
        HAS_MORE_PAGES,
        NO_METADATA
    };

    using flag_enum = super_enum<flag,
        flag::GLOBAL_TABLES_SPEC,
        flag::HAS_MORE_PAGES,
        flag::NO_METADATA>;

    using flag_enum_set = enum_set<flag_enum>;

private:
    flag_enum_set _flags;

public:
    // Please note that columnCount can actually be smaller than names, even if names is not null. This is
    // used to include columns in the resultSet that we need to do post-query re-orderings
    // (SelectStatement.orderResults) but that shouldn't be sent to the user as they haven't been requested
    // (CASSANDRA-4911). So the serialization code will exclude any columns in name whose index is >= columnCount.
    std::vector<::shared_ptr<column_specification>> names;

private:
    const uint32_t _column_count;
    ::shared_ptr<const service::pager::paging_state> _paging_state;

public:
    metadata(std::vector<::shared_ptr<column_specification>> names_)
        : metadata(flag_enum_set(), std::move(names_), names_.size(), {})
    { }

    metadata(flag_enum_set flags, std::vector<::shared_ptr<column_specification>> names_, uint32_t column_count,
            ::shared_ptr<const service::pager::paging_state> paging_state)
        : _flags(flags)
        , names(std::move(names_))
        , _column_count(column_count)
        , _paging_state(std::move(paging_state))
    { }

    // The maximum number of values that the ResultSet can hold. This can be bigger than columnCount due to CASSANDRA-4911
    uint32_t value_count() {
        return _flags.contains<flag::NO_METADATA>() ? _column_count : names.size();
    }

    void add_non_serialized_column(::shared_ptr<column_specification> name) {
        // See comment above. Because columnCount doesn't account the newly added name, it
        // won't be serialized.
        names.emplace_back(std::move(name));
    }

private:
    bool all_in_same_cf() const {
        if (_flags.contains<flag::NO_METADATA>()) {
            return false;
        }

        assert(!names.empty());

        auto first = names.front();
        return std::all_of(std::next(names.begin()), names.end(), [first] (auto&& spec) {
            return spec->ks_name == first->ks_name && spec->cf_name == first->cf_name;
        });
    }

public:
    void set_has_more_pages(::shared_ptr<const service::pager::paging_state> paging_state) {
        if (!paging_state) {
            return;
        }

        _flags.set<flag::HAS_MORE_PAGES>();
        _paging_state = std::move(paging_state);
    }

    void set_skip_metadata() {
        _flags.set<flag::NO_METADATA>();
    }

    flag_enum_set flags() const {
        return _flags;
    }

    uint32_t column_count() const {
        return _column_count;
    }

    auto paging_state() const {
        return _paging_state;
    }

    auto const& get_names() const {
        return names;
    }

#if 0
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
#endif

};

class result_set {
#if 0
    private static final ColumnIdentifier COUNT_COLUMN = new ColumnIdentifier("count", false);
#endif

public:
    ::shared_ptr<metadata> _metadata;
    std::deque<std::vector<bytes_opt>> _rows;
public:
    result_set(std::vector<::shared_ptr<column_specification>> metadata_)
        : _metadata(::make_shared<metadata>(std::move(metadata_)))
    { }

    result_set(::shared_ptr<metadata> metadata)
        : _metadata(std::move(metadata))
    { }

    size_t size() const {
        return _rows.size();
    }

    bool empty() const {
        return _rows.empty();
    }

    void add_row(std::vector<bytes_opt> row) {
        assert(row.size() == _metadata->value_count());
        _rows.emplace_back(std::move(row));
    }

    void add_column_value(bytes_opt value) {
        if (_rows.empty() || _rows.back().size() == _metadata->value_count()) {
            std::vector<bytes_opt> row;
            row.reserve(_metadata->value_count());
            _rows.emplace_back(std::move(row));
        }

        _rows.back().emplace_back(std::move(value));
    }

    void reverse() {
        std::reverse(_rows.begin(), _rows.end());
    }

    void trim(size_t limit) {
        if (_rows.size() > limit) {
            _rows.resize(limit);
        }
    }

    template<typename RowComparator>
    void sort(RowComparator&& cmp) {
        std::sort(_rows.begin(), _rows.end(), std::forward<RowComparator>(cmp));
    }

    metadata& get_metadata() {
        return *_metadata;
    }

    const metadata& get_metadata() const {
        return *_metadata;
    }

    // Returns a range of rows. A row is a range of bytes_opt.
    auto const& rows() const {
        return _rows;
    }
#if 0
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
#endif
};

}
