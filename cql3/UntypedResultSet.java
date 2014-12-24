/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.cql3;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;

import com.google.common.collect.AbstractIterator;

import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.service.pager.QueryPager;

/** a utility for doing internal cql-based queries */
public abstract class UntypedResultSet implements Iterable<UntypedResultSet.Row>
{
    public static UntypedResultSet create(ResultSet rs)
    {
        return new FromResultSet(rs);
    }

    public static UntypedResultSet create(List<Map<String, ByteBuffer>> results)
    {
        return new FromResultList(results);
    }

    public static UntypedResultSet create(SelectStatement select, QueryPager pager, int pageSize)
    {
        return new FromPager(select, pager, pageSize);
    }

    public boolean isEmpty()
    {
        return size() == 0;
    }

    public abstract int size();
    public abstract Row one();

    // No implemented by all subclasses, but we use it when we know it's there (for tests)
    public abstract List<ColumnSpecification> metadata();

    private static class FromResultSet extends UntypedResultSet
    {
        private final ResultSet cqlRows;

        private FromResultSet(ResultSet cqlRows)
        {
            this.cqlRows = cqlRows;
        }

        public int size()
        {
            return cqlRows.size();
        }

        public Row one()
        {
            if (cqlRows.rows.size() != 1)
                throw new IllegalStateException("One row required, " + cqlRows.rows.size() + " found");
            return new Row(cqlRows.metadata.names, cqlRows.rows.get(0));
        }

        public Iterator<Row> iterator()
        {
            return new AbstractIterator<Row>()
            {
                Iterator<List<ByteBuffer>> iter = cqlRows.rows.iterator();

                protected Row computeNext()
                {
                    if (!iter.hasNext())
                        return endOfData();
                    return new Row(cqlRows.metadata.names, iter.next());
                }
            };
        }

        public List<ColumnSpecification> metadata()
        {
            return cqlRows.metadata.names;
        }
    }

    private static class FromResultList extends UntypedResultSet
    {
        private final List<Map<String, ByteBuffer>> cqlRows;

        private FromResultList(List<Map<String, ByteBuffer>> cqlRows)
        {
            this.cqlRows = cqlRows;
        }

        public int size()
        {
            return cqlRows.size();
        }

        public Row one()
        {
            if (cqlRows.size() != 1)
                throw new IllegalStateException("One row required, " + cqlRows.size() + " found");
            return new Row(cqlRows.get(0));
        }

        public Iterator<Row> iterator()
        {
            return new AbstractIterator<Row>()
            {
                Iterator<Map<String, ByteBuffer>> iter = cqlRows.iterator();

                protected Row computeNext()
                {
                    if (!iter.hasNext())
                        return endOfData();
                    return new Row(iter.next());
                }
            };
        }

        public List<ColumnSpecification> metadata()
        {
            throw new UnsupportedOperationException();
        }
    }

    private static class FromPager extends UntypedResultSet
    {
        private final SelectStatement select;
        private final QueryPager pager;
        private final int pageSize;
        private final List<ColumnSpecification> metadata;

        private FromPager(SelectStatement select, QueryPager pager, int pageSize)
        {
            this.select = select;
            this.pager = pager;
            this.pageSize = pageSize;
            this.metadata = select.getResultMetadata().names;
        }

        public int size()
        {
            throw new UnsupportedOperationException();
        }

        public Row one()
        {
            throw new UnsupportedOperationException();
        }

        public Iterator<Row> iterator()
        {
            return new AbstractIterator<Row>()
            {
                private Iterator<List<ByteBuffer>> currentPage;

                protected Row computeNext()
                {
                    try {
                        while (currentPage == null || !currentPage.hasNext())
                        {
                            if (pager.isExhausted())
                                return endOfData();
                            currentPage = select.process(pager.fetchPage(pageSize)).rows.iterator();
                        }
                        return new Row(metadata, currentPage.next());
                    } catch (RequestValidationException | RequestExecutionException e) {
                        throw new RuntimeException(e);
                    }
                }
            };
        }

        public List<ColumnSpecification> metadata()
        {
            return metadata;
        }
    }

    public static class Row
    {
        private final Map<String, ByteBuffer> data = new HashMap<>();
        private final List<ColumnSpecification> columns = new ArrayList<>();

        public Row(Map<String, ByteBuffer> data)
        {
            this.data.putAll(data);
        }

        public Row(List<ColumnSpecification> names, List<ByteBuffer> columns)
        {
            this.columns.addAll(names);
            for (int i = 0; i < names.size(); i++)
                data.put(names.get(i).name.toString(), columns.get(i));
        }

        public boolean has(String column)
        {
            // Note that containsKey won't work because we may have null values
            return data.get(column) != null;
        }

        public ByteBuffer getBlob(String column)
        {
            return data.get(column);
        }

        public String getString(String column)
        {
            return UTF8Type.instance.compose(data.get(column));
        }

        public boolean getBoolean(String column)
        {
            return BooleanType.instance.compose(data.get(column));
        }

        public int getInt(String column)
        {
            return Int32Type.instance.compose(data.get(column));
        }

        public double getDouble(String column)
        {
            return DoubleType.instance.compose(data.get(column));
        }

        public ByteBuffer getBytes(String column)
        {
            return data.get(column);
        }

        public InetAddress getInetAddress(String column)
        {
            return InetAddressType.instance.compose(data.get(column));
        }

        public UUID getUUID(String column)
        {
            return UUIDType.instance.compose(data.get(column));
        }

        public Date getTimestamp(String column)
        {
            return TimestampType.instance.compose(data.get(column));
        }

        public long getLong(String column)
        {
            return LongType.instance.compose(data.get(column));
        }

        public <T> Set<T> getSet(String column, AbstractType<T> type)
        {
            ByteBuffer raw = data.get(column);
            return raw == null ? null : SetType.getInstance(type, true).compose(raw);
        }

        public <T> List<T> getList(String column, AbstractType<T> type)
        {
            ByteBuffer raw = data.get(column);
            return raw == null ? null : ListType.getInstance(type, true).compose(raw);
        }

        public <K, V> Map<K, V> getMap(String column, AbstractType<K> keyType, AbstractType<V> valueType)
        {
            ByteBuffer raw = data.get(column);
            return raw == null ? null : MapType.getInstance(keyType, valueType, true).compose(raw);
        }

        public List<ColumnSpecification> getColumns()
        {
            return columns;
        }

        @Override
        public String toString()
        {
            return data.toString();
        }
    }
}
