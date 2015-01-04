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
package org.apache.cassandra.db.composites;

import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import com.google.common.collect.AbstractIterator;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.CQL3Row;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.IDiskAtomFilter;
import org.apache.cassandra.db.filter.NamesQueryFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.ColumnToCollectionType;
import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.ByteBufferUtil;

public abstract class AbstractCellNameType extends AbstractCType implements CellNameType
{
    final Comparator<Cell> columnComparator;
    private final Comparator<Cell> columnReverseComparator;
    final Comparator<Object> asymmetricComparator;
    private final Comparator<OnDiskAtom> onDiskAtomComparator;

    private final ISerializer<CellName> cellSerializer;
    private final ColumnSerializer columnSerializer;
    private final OnDiskAtom.Serializer onDiskAtomSerializer;
    private final IVersionedSerializer<NamesQueryFilter> namesQueryFilterSerializer;
    private final IVersionedSerializer<IDiskAtomFilter> diskAtomFilterSerializer;

    protected AbstractCellNameType(boolean isByteOrderComparable)
    {
        super(isByteOrderComparable);
        columnComparator = new Comparator<Cell>()
        {
            public int compare(Cell c1, Cell c2)
            {
                return AbstractCellNameType.this.compare(c1.name(), c2.name());
            }
        };
        asymmetricComparator = new Comparator<Object>()
        {
            public int compare(Object c1, Object c2)
            {
                return AbstractCellNameType.this.compare((Composite) c1, ((Cell) c2).name());
            }
        };
        columnReverseComparator = new Comparator<Cell>()
        {
            public int compare(Cell c1, Cell c2)
            {
                return AbstractCellNameType.this.compare(c2.name(), c1.name());
            }
        };
        onDiskAtomComparator = new Comparator<OnDiskAtom>()
        {
            public int compare(OnDiskAtom c1, OnDiskAtom c2)
            {
                int comp = AbstractCellNameType.this.compare(c1.name(), c2.name());
                if (comp != 0)
                    return comp;

                if (c1 instanceof RangeTombstone)
                {
                    if (c2 instanceof RangeTombstone)
                    {
                        RangeTombstone t1 = (RangeTombstone)c1;
                        RangeTombstone t2 = (RangeTombstone)c2;
                        int comp2 = AbstractCellNameType.this.compare(t1.max, t2.max);
                        return comp2 == 0 ? t1.data.compareTo(t2.data) : comp2;
                    }
                    else
                    {
                        return -1;
                    }
                }
                else
                {
                    return c2 instanceof RangeTombstone ? 1 : 0;
                }
            }
        };

        // A trivial wrapped over the composite serializer
        cellSerializer = new ISerializer<CellName>()
        {
            public void serialize(CellName c, DataOutputPlus out) throws IOException
            {
                serializer().serialize(c, out);
            }

            public CellName deserialize(DataInput in) throws IOException
            {
                Composite ct = serializer().deserialize(in);
                if (ct.isEmpty())
                    throw ColumnSerializer.CorruptColumnException.create(in, ByteBufferUtil.EMPTY_BYTE_BUFFER);

                assert ct instanceof CellName : ct;
                return (CellName)ct;
            }

            public long serializedSize(CellName c, TypeSizes type)
            {
                return serializer().serializedSize(c, type);
            }
        };
        columnSerializer = new ColumnSerializer(this);
        onDiskAtomSerializer = new OnDiskAtom.Serializer(this);
        namesQueryFilterSerializer = new NamesQueryFilter.Serializer(this);
        diskAtomFilterSerializer = new IDiskAtomFilter.Serializer(this);
    }

    public final Comparator<Cell> columnComparator(boolean isRightNative)
    {
        if (!isByteOrderComparable)
            return columnComparator;
        return getByteOrderColumnComparator(isRightNative);
    }

    public final Comparator<Object> asymmetricColumnComparator(boolean isRightNative)
    {
        if (!isByteOrderComparable)
            return asymmetricComparator;
        return getByteOrderAsymmetricColumnComparator(isRightNative);
    }

    public Comparator<Cell> columnReverseComparator()
    {
        return columnReverseComparator;
    }

    public Comparator<OnDiskAtom> onDiskAtomComparator()
    {
        return onDiskAtomComparator;
    }

    public ISerializer<CellName> cellSerializer()
    {
        return cellSerializer;
    }

    public ColumnSerializer columnSerializer()
    {
        return columnSerializer;
    }

    public OnDiskAtom.Serializer onDiskAtomSerializer()
    {
        return onDiskAtomSerializer;
    }

    public IVersionedSerializer<NamesQueryFilter> namesQueryFilterSerializer()
    {
        return namesQueryFilterSerializer;
    }

    public IVersionedSerializer<IDiskAtomFilter> diskAtomFilterSerializer()
    {
        return diskAtomFilterSerializer;
    }

    public CellName cellFromByteBuffer(ByteBuffer bytes)
    {
        // we're not guaranteed to get a CellName back from fromByteBuffer(), so it's on the caller to guarantee this
        return (CellName)fromByteBuffer(bytes);
    }

    public CellName create(Composite prefix, ColumnDefinition column, ByteBuffer collectionElement)
    {
        throw new UnsupportedOperationException();
    }

    public CellName rowMarker(Composite prefix)
    {
        throw new UnsupportedOperationException();
    }

    public Composite staticPrefix()
    {
        throw new UnsupportedOperationException();
    }

    public boolean hasCollections()
    {
        return false;
    }

    public boolean supportCollections()
    {
        return false;
    }

    public ColumnToCollectionType collectionType()
    {
        throw new UnsupportedOperationException();
    }

    public CellNameType addOrUpdateCollection(ColumnIdentifier columnName, CollectionType newCollection)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Composite make(Object... components)
    {
        return components.length == size() ? makeCellName(components) : super.make(components);
    }

    public CellName makeCellName(Object... components)
    {
        ByteBuffer[] rawComponents = new ByteBuffer[components.length];
        for (int i = 0; i < components.length; i++)
        {
            Object c = components[i];
            if (c instanceof ByteBuffer)
            {
                rawComponents[i] = (ByteBuffer)c;
            }
            else
            {
                AbstractType<?> type = subtype(i);
                // If it's a collection type, we need to find the right collection and use the key comparator (since we're building a cell name)
                if (type instanceof ColumnToCollectionType)
                {
                    assert i > 0;
                    type = ((ColumnToCollectionType)type).defined.get(rawComponents[i-1]).nameComparator();
                }
                rawComponents[i] = ((AbstractType)type).decompose(c);
            }
        }
        return makeCellName(rawComponents);
    }

    protected abstract CellName makeCellName(ByteBuffer[] components);

    protected static CQL3Row.Builder makeDenseCQL3RowBuilder(final long now)
    {
        return new CQL3Row.Builder()
        {
            public CQL3Row.RowIterator group(Iterator<Cell> cells)
            {
                return new DenseRowIterator(cells, now);
            }
        };
    }

    private static class DenseRowIterator extends AbstractIterator<CQL3Row> implements CQL3Row.RowIterator
    {
        private final Iterator<Cell> cells;
        private final long now;

        public DenseRowIterator(Iterator<Cell> cells, long now)
        {
            this.cells = cells;
            this.now = now;
        }

        public CQL3Row getStaticRow()
        {
            // There can't be static columns in dense tables
            return null;
        }

        protected CQL3Row computeNext()
        {
            while (cells.hasNext())
            {
                final Cell cell = cells.next();
                if (!cell.isLive(now))
                    continue;

                return new CQL3Row()
                {
                    public ByteBuffer getClusteringColumn(int i)
                    {
                        return cell.name().get(i);
                    }

                    public Cell getColumn(ColumnIdentifier name)
                    {
                        return cell;
                    }

                    public List<Cell> getMultiCellColumn(ColumnIdentifier name)
                    {
                        return null;
                    }
                };
            }
            return endOfData();
        }
    }

    protected static CQL3Row.Builder makeSparseCQL3RowBuilder(final CFMetaData cfMetaData, final CellNameType type, final long now)
    {
        return new CQL3Row.Builder()
        {
            public CQL3Row.RowIterator group(Iterator<Cell> cells)
            {
                return new SparseRowIterator(cfMetaData, type, cells, now);
            }
        };
    }

    private static class SparseRowIterator extends AbstractIterator<CQL3Row> implements CQL3Row.RowIterator
    {
        private final CFMetaData cfMetaData;
        private final CellNameType type;
        private final Iterator<Cell> cells;
        private final long now;
        private final CQL3Row staticRow;

        private Cell nextCell;
        private CellName previous;
        private CQL3RowOfSparse currentRow;

        public SparseRowIterator(CFMetaData cfMetaData, CellNameType type, Iterator<Cell> cells, long now)
        {
            this.cfMetaData = cfMetaData;
            this.type = type;
            this.cells = cells;
            this.now = now;
            this.staticRow = hasNextCell() && nextCell.name().isStatic()
                           ? computeNext()
                           : null;
        }

        public CQL3Row getStaticRow()
        {
            return staticRow;
        }

        private boolean hasNextCell()
        {
            if (nextCell != null)
                return true;

            while (cells.hasNext())
            {
                Cell cell = cells.next();
                if (!cell.isLive(now))
                    continue;

                nextCell = cell;
                return true;
            }
            return false;
        }

        protected CQL3Row computeNext()
        {
            while (hasNextCell())
            {
                CQL3Row toReturn = null;
                CellName current = nextCell.name();
                if (currentRow == null || !current.isSameCQL3RowAs(type, previous))
                {
                    toReturn = currentRow;
                    currentRow = new CQL3RowOfSparse(cfMetaData, current);
                }
                currentRow.add(nextCell);
                nextCell = null;
                previous = current;

                if (toReturn != null)
                    return toReturn;
            }
            if (currentRow != null)
            {
                CQL3Row toReturn = currentRow;
                currentRow = null;
                return toReturn;
            }
            return endOfData();
        }
    }

    private static class CQL3RowOfSparse implements CQL3Row
    {
        private final CFMetaData cfMetaData;
        private final CellName cell;
        private Map<ColumnIdentifier, Cell> columns;
        private Map<ColumnIdentifier, List<Cell>> collections;

        CQL3RowOfSparse(CFMetaData metadata, CellName cell)
        {
            this.cfMetaData = metadata;
            this.cell = cell;
        }

        public ByteBuffer getClusteringColumn(int i)
        {
            return cell.get(i);
        }

        void add(Cell cell)
        {
            CellName cellName = cell.name();
            ColumnIdentifier columnName =  cellName.cql3ColumnName(cfMetaData);
            if (cellName.isCollectionCell())
            {
                if (collections == null)
                    collections = new HashMap<>();

                List<Cell> values = collections.get(columnName);
                if (values == null)
                {
                    values = new ArrayList<Cell>();
                    collections.put(columnName, values);
                }
                values.add(cell);
            }
            else
            {
                if (columns == null)
                    columns = new HashMap<>();
                columns.put(columnName, cell);
            }
        }

        public Cell getColumn(ColumnIdentifier name)
        {
            return columns == null ? null : columns.get(name);
        }

        public List<Cell> getMultiCellColumn(ColumnIdentifier name)
        {
            return collections == null ? null : collections.get(name);
        }
    }
}
