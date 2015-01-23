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
package org.apache.cassandra.thrift;

import java.nio.ByteBuffer;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.*;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.composites.*;
import org.apache.cassandra.db.filter.IDiskAtomFilter;
import org.apache.cassandra.db.filter.NamesQueryFilter;
import org.apache.cassandra.db.filter.SliceQueryFilter;
import org.apache.cassandra.db.index.SecondaryIndexManager;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ColumnToCollectionType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

/**
 * This has a lot of building blocks for CassandraServer to call to make sure it has valid input
 * -- ensuring column names conform to the declared comparator, for instance.
 *
 * The methods here mostly try to do just one part of the validation so they can be combined
 * for different needs -- supercolumns vs regular, range slices vs named, batch vs single-column.
 * (ValidateColumnPath is the main exception in that it includes keyspace and CF validation.)
 */
public class ThriftValidation
{
    private static final Logger logger = LoggerFactory.getLogger(ThriftValidation.class);

    public static void validateKey(CFMetaData metadata, ByteBuffer key) throws org.apache.cassandra.exceptions.InvalidRequestException
    {
        if (key == null || key.remaining() == 0)
        {
            throw new org.apache.cassandra.exceptions.InvalidRequestException("Key may not be empty");
        }

        // check that key can be handled by FBUtilities.writeShortByteArray
        if (key.remaining() > FBUtilities.MAX_UNSIGNED_SHORT)
        {
            throw new org.apache.cassandra.exceptions.InvalidRequestException("Key length of " + key.remaining() +
                                                                              " is longer than maximum of " +
                                                                              FBUtilities.MAX_UNSIGNED_SHORT);
        }

        try
        {
            metadata.getKeyValidator().validate(key);
        }
        catch (MarshalException e)
        {
            throw new org.apache.cassandra.exceptions.InvalidRequestException(e.getMessage());
        }
    }

    public static void validateKeyspace(String keyspaceName) throws KeyspaceNotDefinedException
    {
        if (!Schema.instance.getKeyspaces().contains(keyspaceName))
        {
            throw new KeyspaceNotDefinedException("Keyspace " + keyspaceName + " does not exist");
        }
    }

    public static CFMetaData validateColumnFamily(String keyspaceName, String cfName, boolean isCommutativeOp) throws org.apache.cassandra.exceptions.InvalidRequestException
    {
        CFMetaData metadata = validateColumnFamily(keyspaceName, cfName);

        if (isCommutativeOp)
        {
            if (!metadata.isCounter())
                throw new org.apache.cassandra.exceptions.InvalidRequestException("invalid operation for non commutative table " + cfName);
        }
        else
        {
            if (metadata.isCounter())
                throw new org.apache.cassandra.exceptions.InvalidRequestException("invalid operation for commutative table " + cfName);
        }
        return metadata;
    }

    // To be used when the operation should be authorized whether this is a counter CF or not
    public static CFMetaData validateColumnFamily(String keyspaceName, String cfName) throws org.apache.cassandra.exceptions.InvalidRequestException
    {
        validateKeyspace(keyspaceName);
        if (cfName.isEmpty())
            throw new org.apache.cassandra.exceptions.InvalidRequestException("non-empty table is required");

        CFMetaData metadata = Schema.instance.getCFMetaData(keyspaceName, cfName);
        if (metadata == null)
            throw new org.apache.cassandra.exceptions.InvalidRequestException("unconfigured table " + cfName);

        return metadata;
    }

    /**
     * validates all parts of the path to the column, including the column name
     */
    public static void validateColumnPath(CFMetaData metadata, ColumnPath column_path) throws org.apache.cassandra.exceptions.InvalidRequestException
    {
        if (metadata.cfType == ColumnFamilyType.Standard)
        {
            if (column_path.super_column != null)
            {
                throw new org.apache.cassandra.exceptions.InvalidRequestException("supercolumn parameter is invalid for standard CF " + metadata.cfName);
            }
            if (column_path.column == null)
            {
                throw new org.apache.cassandra.exceptions.InvalidRequestException("column parameter is not optional for standard CF " + metadata.cfName);
            }
        }
        else
        {
            if (column_path.super_column == null)
                throw new org.apache.cassandra.exceptions.InvalidRequestException("supercolumn parameter is not optional for super CF " + metadata.cfName);
        }
        if (column_path.column != null)
        {
            validateColumnNames(metadata, column_path.super_column, Arrays.asList(column_path.column));
        }
        if (column_path.super_column != null)
        {
            validateColumnNames(metadata, (ByteBuffer)null, Arrays.asList(column_path.super_column));
        }
    }

    public static void validateColumnParent(CFMetaData metadata, ColumnParent column_parent) throws org.apache.cassandra.exceptions.InvalidRequestException
    {
        if (metadata.cfType == ColumnFamilyType.Standard)
        {
            if (column_parent.super_column != null)
            {
                throw new org.apache.cassandra.exceptions.InvalidRequestException("table alone is required for standard CF " + metadata.cfName);
            }
        }

        if (column_parent.super_column != null)
        {
            validateColumnNames(metadata, (ByteBuffer)null, Arrays.asList(column_parent.super_column));
        }
    }

    // column_path_or_parent is a ColumnPath for remove, where the "column" is optional even for a standard CF
    static void validateColumnPathOrParent(CFMetaData metadata, ColumnPath column_path_or_parent) throws org.apache.cassandra.exceptions.InvalidRequestException
    {
        if (metadata.cfType == ColumnFamilyType.Standard)
        {
            if (column_path_or_parent.super_column != null)
            {
                throw new org.apache.cassandra.exceptions.InvalidRequestException("supercolumn may not be specified for standard CF " + metadata.cfName);
            }
        }
        if (metadata.cfType == ColumnFamilyType.Super)
        {
            if (column_path_or_parent.super_column == null && column_path_or_parent.column != null)
            {
                throw new org.apache.cassandra.exceptions.InvalidRequestException("A column cannot be specified without specifying a super column for removal on super CF "
                                                                          + metadata.cfName);
            }
        }
        if (column_path_or_parent.column != null)
        {
            validateColumnNames(metadata, column_path_or_parent.super_column, Arrays.asList(column_path_or_parent.column));
        }
        if (column_path_or_parent.super_column != null)
        {
            validateColumnNames(metadata, (ByteBuffer)null, Arrays.asList(column_path_or_parent.super_column));
        }
    }

    /**
     * Validates the column names but not the parent path or data
     */
    private static void validateColumnNames(CFMetaData metadata, ByteBuffer superColumnName, Iterable<ByteBuffer> column_names)
    throws org.apache.cassandra.exceptions.InvalidRequestException
    {
        int maxNameLength = Cell.MAX_NAME_LENGTH;

        if (superColumnName != null)
        {
            if (superColumnName.remaining() > maxNameLength)
                throw new org.apache.cassandra.exceptions.InvalidRequestException("supercolumn name length must not be greater than " + maxNameLength);
            if (superColumnName.remaining() == 0)
                throw new org.apache.cassandra.exceptions.InvalidRequestException("supercolumn name must not be empty");
            if (metadata.cfType == ColumnFamilyType.Standard)
                throw new org.apache.cassandra.exceptions.InvalidRequestException("supercolumn specified to table " + metadata.cfName + " containing normal columns");
        }
        AbstractType<?> comparator = SuperColumns.getComparatorFor(metadata, superColumnName);
        boolean isCQL3Table = !metadata.isThriftCompatible();
        for (ByteBuffer name : column_names)
        {
            if (name.remaining() > maxNameLength)
                throw new org.apache.cassandra.exceptions.InvalidRequestException("column name length must not be greater than " + maxNameLength);
            if (name.remaining() == 0)
                throw new org.apache.cassandra.exceptions.InvalidRequestException("column name must not be empty");
            try
            {
                comparator.validate(name);
            }
            catch (MarshalException e)
            {
                throw new org.apache.cassandra.exceptions.InvalidRequestException(e.getMessage());
            }

            if (isCQL3Table)
            {
                // CQL3 table don't support having only part of their composite column names set
                Composite composite = metadata.comparator.fromByteBuffer(name);

                int minComponents = metadata.comparator.clusteringPrefixSize() + 1;
                if (composite.size() < minComponents)
                    throw new org.apache.cassandra.exceptions.InvalidRequestException(String.format("Not enough components (found %d but %d expected) for column name since %s is a CQL3 table",
                                                                                                    composite.size(), minComponents, metadata.cfName));

                // Furthermore, the column name must be a declared one.
                int columnIndex = metadata.comparator.clusteringPrefixSize();
                ByteBuffer CQL3ColumnName = composite.get(columnIndex);
                if (!CQL3ColumnName.hasRemaining())
                    continue; // Row marker, ok

                ColumnIdentifier columnId = new ColumnIdentifier(CQL3ColumnName, metadata.comparator.subtype(columnIndex));
                if (metadata.getColumnDefinition(columnId) == null)
                    throw new org.apache.cassandra.exceptions.InvalidRequestException(String.format("Invalid cell for CQL3 table %s. The CQL3 column component (%s) does not correspond to a defined CQL3 column",
                                                                                                    metadata.cfName, columnId));

                // On top of that, if we have a collection component, he (CQL3) column must be a collection
                if (metadata.comparator.hasCollections() && composite.size() == metadata.comparator.size())
                {
                    ColumnToCollectionType collectionType = metadata.comparator.collectionType();
                    if (!collectionType.defined.containsKey(CQL3ColumnName))
                        throw new org.apache.cassandra.exceptions.InvalidRequestException(String.format("Invalid collection component, %s is not a collection", UTF8Type.instance.getString(CQL3ColumnName)));
                }
            }
        }
    }

    public static void validateColumnNames(CFMetaData metadata, ColumnParent column_parent, Iterable<ByteBuffer> column_names) throws org.apache.cassandra.exceptions.InvalidRequestException
    {
        validateColumnNames(metadata, column_parent.super_column, column_names);
    }

    public static void validateRange(CFMetaData metadata, ColumnParent column_parent, SliceRange range) throws org.apache.cassandra.exceptions.InvalidRequestException
    {
        if (range.count < 0)
            throw new org.apache.cassandra.exceptions.InvalidRequestException("get_slice requires non-negative count");

        int maxNameLength = Cell.MAX_NAME_LENGTH;
        if (range.start.remaining() > maxNameLength)
            throw new org.apache.cassandra.exceptions.InvalidRequestException("range start length cannot be larger than " + maxNameLength);
        if (range.finish.remaining() > maxNameLength)
            throw new org.apache.cassandra.exceptions.InvalidRequestException("range finish length cannot be larger than " + maxNameLength);

        AbstractType<?> comparator = SuperColumns.getComparatorFor(metadata, column_parent.super_column);
        try
        {
            comparator.validate(range.start);
            comparator.validate(range.finish);
        }
        catch (MarshalException e)
        {
            throw new org.apache.cassandra.exceptions.InvalidRequestException(e.getMessage());
        }

        Comparator<ByteBuffer> orderedComparator = range.isReversed() ? comparator.reverseComparator : comparator;
        if (range.start.remaining() > 0
            && range.finish.remaining() > 0
            && orderedComparator.compare(range.start, range.finish) > 0)
        {
            throw new org.apache.cassandra.exceptions.InvalidRequestException("range finish must come after start in the order of traversal");
        }
    }

    public static void validateColumnOrSuperColumn(CFMetaData metadata, ColumnOrSuperColumn cosc)
            throws org.apache.cassandra.exceptions.InvalidRequestException
    {
        boolean isCommutative = metadata.isCounter();

        int nulls = 0;
        if (cosc.column == null) nulls++;
        if (cosc.super_column == null) nulls++;
        if (cosc.counter_column == null) nulls++;
        if (cosc.counter_super_column == null) nulls++;

        if (nulls != 3)
            throw new org.apache.cassandra.exceptions.InvalidRequestException("ColumnOrSuperColumn must have one (and only one) of column, super_column, counter and counter_super_column");

        if (cosc.column != null)
        {
            if (isCommutative)
                throw new org.apache.cassandra.exceptions.InvalidRequestException("invalid operation for commutative table " + metadata.cfName);

            validateTtl(cosc.column);
            validateColumnPath(metadata, new ColumnPath(metadata.cfName).setSuper_column((ByteBuffer)null).setColumn(cosc.column.name));
            validateColumnData(metadata, null, cosc.column);
        }

        if (cosc.super_column != null)
        {
            if (isCommutative)
                throw new org.apache.cassandra.exceptions.InvalidRequestException("invalid operation for commutative table " + metadata.cfName);

            for (Column c : cosc.super_column.columns)
            {
                validateColumnPath(metadata, new ColumnPath(metadata.cfName).setSuper_column(cosc.super_column.name).setColumn(c.name));
                validateColumnData(metadata, cosc.super_column.name, c);
            }
        }

        if (cosc.counter_column != null)
        {
            if (!isCommutative)
                throw new org.apache.cassandra.exceptions.InvalidRequestException("invalid operation for non commutative table " + metadata.cfName);

            validateColumnPath(metadata, new ColumnPath(metadata.cfName).setSuper_column((ByteBuffer)null).setColumn(cosc.counter_column.name));
        }

        if (cosc.counter_super_column != null)
        {
            if (!isCommutative)
                throw new org.apache.cassandra.exceptions.InvalidRequestException("invalid operation for non commutative table " + metadata.cfName);

            for (CounterColumn c : cosc.counter_super_column.columns)
                validateColumnPath(metadata, new ColumnPath(metadata.cfName).setSuper_column(cosc.counter_super_column.name).setColumn(c.name));
        }
    }

    private static void validateTtl(Column column) throws org.apache.cassandra.exceptions.InvalidRequestException
    {
        if (column.isSetTtl())
        {
            if (column.ttl <= 0)
                throw new org.apache.cassandra.exceptions.InvalidRequestException("ttl must be positive");

            if (column.ttl > ExpiringCell.MAX_TTL)
                throw new org.apache.cassandra.exceptions.InvalidRequestException(String.format("ttl is too large. requested (%d) maximum (%d)", column.ttl, ExpiringCell.MAX_TTL));
        }
        else
        {
            // if it's not set, then it should be zero -- here we are just checking to make sure Thrift doesn't change that contract with us.
            assert column.ttl == 0;
        }
    }

    public static void validateMutation(CFMetaData metadata, Mutation mut)
            throws org.apache.cassandra.exceptions.InvalidRequestException
    {
        ColumnOrSuperColumn cosc = mut.column_or_supercolumn;
        Deletion del = mut.deletion;

        int nulls = 0;
        if (cosc == null) nulls++;
        if (del == null) nulls++;

        if (nulls != 1)
        {
            throw new org.apache.cassandra.exceptions.InvalidRequestException("mutation must have one and only one of column_or_supercolumn or deletion");
        }

        if (cosc != null)
        {
            validateColumnOrSuperColumn(metadata, cosc);
        }
        else
        {
            validateDeletion(metadata, del);
        }
    }

    public static void validateDeletion(CFMetaData metadata, Deletion del) throws org.apache.cassandra.exceptions.InvalidRequestException
    {

        if (del.super_column != null)
            validateColumnNames(metadata, (ByteBuffer)null, Arrays.asList(del.super_column));

        if (del.predicate != null)
            validateSlicePredicate(metadata, del.super_column, del.predicate);

        if (metadata.cfType == ColumnFamilyType.Standard && del.super_column != null)
        {
            String msg = String.format("Deletion of super columns is not possible on a standard table (KeySpace=%s Table=%s Deletion=%s)", metadata.ksName, metadata.cfName, del);
            throw new org.apache.cassandra.exceptions.InvalidRequestException(msg);
        }

        if (metadata.isCounter())
        {
            // forcing server timestamp even if a timestamp was set for coherence with other counter operation
            del.timestamp = System.currentTimeMillis();
        }
        else if (!del.isSetTimestamp())
        {
            throw new org.apache.cassandra.exceptions.InvalidRequestException("Deletion timestamp is not optional for non commutative table " + metadata.cfName);
        }
    }

    public static void validateSlicePredicate(CFMetaData metadata, ByteBuffer scName, SlicePredicate predicate) throws org.apache.cassandra.exceptions.InvalidRequestException
    {
        if (predicate.column_names == null && predicate.slice_range == null)
            throw new org.apache.cassandra.exceptions.InvalidRequestException("A SlicePredicate must be given a list of Columns, a SliceRange, or both");

        if (predicate.slice_range != null)
            validateRange(metadata, new ColumnParent(metadata.cfName).setSuper_column(scName), predicate.slice_range);

        if (predicate.column_names != null)
            validateColumnNames(metadata, scName, predicate.column_names);
    }

    /**
     * Validates the data part of the column (everything in the column object but the name, which is assumed to be valid)
     */
    public static void validateColumnData(CFMetaData metadata, ByteBuffer scName, Column column) throws org.apache.cassandra.exceptions.InvalidRequestException
    {
        validateTtl(column);
        if (!column.isSetValue())
            throw new org.apache.cassandra.exceptions.InvalidRequestException("Column value is required");
        if (!column.isSetTimestamp())
            throw new org.apache.cassandra.exceptions.InvalidRequestException("Column timestamp is required");

        CellName cn = scName == null
                    ? metadata.comparator.cellFromByteBuffer(column.name)
                    : metadata.comparator.makeCellName(scName, column.name);
        try
        {
            AbstractType<?> validator = metadata.getValueValidator(cn);
            if (validator != null)
                validator.validate(column.value);
        }
        catch (MarshalException me)
        {
            if (logger.isDebugEnabled())
                logger.debug("rejecting invalid value {}", ByteBufferUtil.bytesToHex(summarize(column.value)));

            throw new org.apache.cassandra.exceptions.InvalidRequestException(String.format("(%s) [%s][%s][%s] failed validation",
                                                                      me.getMessage(),
                                                                      metadata.ksName,
                                                                      metadata.cfName,
                                                                      (SuperColumns.getComparatorFor(metadata, scName != null)).getString(column.name)));
        }

        // Indexed column values cannot be larger than 64K.  See CASSANDRA-3057/4240 for more details
        if (!Keyspace.open(metadata.ksName).getColumnFamilyStore(metadata.cfName).indexManager.validate(asDBColumn(cn, column)))
                    throw new org.apache.cassandra.exceptions.InvalidRequestException(String.format("Can't index column value of size %d for index %s in CF %s of KS %s",
                                                                              column.value.remaining(),
                                                                              metadata.getColumnDefinition(cn).getIndexName(),
                                                                              metadata.cfName,
                                                                              metadata.ksName));
    }

    private static Cell asDBColumn(CellName name, Column column)
    {
        if (column.ttl <= 0)
            return new BufferCell(name, column.value, column.timestamp);
        else
            return new BufferExpiringCell(name, column.value, column.timestamp, column.ttl);
    }

    /**
     * Return, at most, the first 64K of the buffer. This avoids very large column values being
     * logged in their entirety.
     */
    private static ByteBuffer summarize(ByteBuffer buffer)
    {
        int MAX = Short.MAX_VALUE;
        if (buffer.remaining() <= MAX)
            return buffer;
        return (ByteBuffer) buffer.slice().limit(buffer.position() + MAX);
    }


    public static void validatePredicate(CFMetaData metadata, ColumnParent column_parent, SlicePredicate predicate)
            throws org.apache.cassandra.exceptions.InvalidRequestException
    {
        if (predicate.column_names == null && predicate.slice_range == null)
            throw new org.apache.cassandra.exceptions.InvalidRequestException("predicate column_names and slice_range may not both be null");
        if (predicate.column_names != null && predicate.slice_range != null)
            throw new org.apache.cassandra.exceptions.InvalidRequestException("predicate column_names and slice_range may not both be present");

        if (predicate.getSlice_range() != null)
            validateRange(metadata, column_parent, predicate.slice_range);
        else
            validateColumnNames(metadata, column_parent, predicate.column_names);
    }

    public static void validateKeyRange(CFMetaData metadata, ByteBuffer superColumn, KeyRange range) throws org.apache.cassandra.exceptions.InvalidRequestException
    {
        if ((range.start_key == null) == (range.start_token == null)
            || (range.end_key == null) == (range.end_token == null))
        {
            throw new org.apache.cassandra.exceptions.InvalidRequestException("exactly one each of {start key, start token} and {end key, end token} must be specified");
        }

        // (key, token) is supported (for wide-row CFRR) but not (token, key)
        if (range.start_token != null && range.end_key != null)
            throw new org.apache.cassandra.exceptions.InvalidRequestException("start token + end key is not a supported key range");

        IPartitioner p = StorageService.getPartitioner();

        if (range.start_key != null && range.end_key != null)
        {
            Token startToken = p.getToken(range.start_key);
            Token endToken = p.getToken(range.end_key);
            if (startToken.compareTo(endToken) > 0 && !endToken.isMinimum())
            {
                if (p.preservesOrder())
                    throw new org.apache.cassandra.exceptions.InvalidRequestException("start key must sort before (or equal to) finish key in your partitioner!");
                else
                    throw new org.apache.cassandra.exceptions.InvalidRequestException("start key's token sorts after end key's token.  this is not allowed; you probably should not specify end key at all except with an ordered partitioner");
            }
        }
        else if (range.start_key != null && range.end_token != null)
        {
            // start_token/end_token can wrap, but key/token should not
            RowPosition stop = p.getTokenFactory().fromString(range.end_token).maxKeyBound();
            if (RowPosition.ForKey.get(range.start_key, p).compareTo(stop) > 0 && !stop.isMinimum())
                throw new org.apache.cassandra.exceptions.InvalidRequestException("Start key's token sorts after end token");
        }

        validateFilterClauses(metadata, range.row_filter);

        if (!isEmpty(range.row_filter) && superColumn != null)
        {
            throw new org.apache.cassandra.exceptions.InvalidRequestException("super columns are not supported for indexing");
        }

        if (range.count <= 0)
        {
            throw new org.apache.cassandra.exceptions.InvalidRequestException("maxRows must be positive");
        }
    }

    private static boolean isEmpty(List<IndexExpression> clause)
    {
        return clause == null || clause.isEmpty();
    }

    public static void validateIndexClauses(CFMetaData metadata, IndexClause index_clause)
    throws org.apache.cassandra.exceptions.InvalidRequestException
    {
        if (index_clause.expressions.isEmpty())
            throw new org.apache.cassandra.exceptions.InvalidRequestException("index clause list may not be empty");

        if (!validateFilterClauses(metadata, index_clause.expressions))
            throw new org.apache.cassandra.exceptions.InvalidRequestException("No indexed columns present in index clause with operator EQ");
    }

    // return true if index_clause contains an indexed columns with operator EQ
    public static boolean validateFilterClauses(CFMetaData metadata, List<IndexExpression> index_clause)
    throws org.apache.cassandra.exceptions.InvalidRequestException
    {
        if (isEmpty(index_clause))
            // no filter to apply
            return false;

        SecondaryIndexManager idxManager = Keyspace.open(metadata.ksName).getColumnFamilyStore(metadata.cfName).indexManager;
        AbstractType<?> nameValidator = SuperColumns.getComparatorFor(metadata, null);

        boolean isIndexed = false;
        for (IndexExpression expression : index_clause)
        {
            try
            {
                nameValidator.validate(expression.column_name);
            }
            catch (MarshalException me)
            {
                throw new org.apache.cassandra.exceptions.InvalidRequestException(String.format("[%s]=[%s] failed name validation (%s)",
                                                                                  ByteBufferUtil.bytesToHex(expression.column_name),
                                                                                  ByteBufferUtil.bytesToHex(expression.value),
                                                                                  me.getMessage()));
            }

            if (expression.value.remaining() > 0xFFFF)
                throw new org.apache.cassandra.exceptions.InvalidRequestException("Index expression values may not be larger than 64K");

            CellName name = metadata.comparator.cellFromByteBuffer(expression.column_name);
            AbstractType<?> valueValidator = metadata.getValueValidator(name);
            try
            {
                valueValidator.validate(expression.value);
            }
            catch (MarshalException me)
            {
                throw new org.apache.cassandra.exceptions.InvalidRequestException(String.format("[%s]=[%s] failed value validation (%s)",
                                                                                  ByteBufferUtil.bytesToHex(expression.column_name),
                                                                                  ByteBufferUtil.bytesToHex(expression.value),
                                                                                  me.getMessage()));
            }

            isIndexed |= (expression.op == IndexOperator.EQ) && idxManager.indexes(name);
        }

        return isIndexed;
    }

    public static void validateKeyspaceNotYetExisting(String newKsName) throws org.apache.cassandra.exceptions.InvalidRequestException
    {
        // keyspace names must be unique case-insensitively because the keyspace name becomes the directory
        // where we store CF sstables.  Names that differ only in case would thus cause problems on
        // case-insensitive filesystems (NTFS, most installations of HFS+).
        for (String ksName : Schema.instance.getKeyspaces())
        {
            if (ksName.equalsIgnoreCase(newKsName))
                throw new org.apache.cassandra.exceptions.InvalidRequestException(String.format("Keyspace names must be case-insensitively unique (\"%s\" conflicts with \"%s\")",
                                                                                  newKsName,
                                                                                  ksName));
        }
    }

    public static void validateKeyspaceNotSystem(String modifiedKeyspace) throws org.apache.cassandra.exceptions.InvalidRequestException
    {
        if (modifiedKeyspace.equalsIgnoreCase(SystemKeyspace.NAME))
            throw new org.apache.cassandra.exceptions.InvalidRequestException("system keyspace is not user-modifiable");
    }

    public static IDiskAtomFilter asIFilter(SlicePredicate sp, CFMetaData metadata, ByteBuffer superColumn)
    {
        SliceRange sr = sp.slice_range;
        IDiskAtomFilter filter;

        CellNameType comparator = metadata.isSuper()
                                ? new SimpleDenseCellNameType(metadata.comparator.subtype(superColumn == null ? 0 : 1))
                                : metadata.comparator;
        if (sr == null)
        {

            SortedSet<CellName> ss = new TreeSet<CellName>(comparator);
            for (ByteBuffer bb : sp.column_names)
                ss.add(comparator.cellFromByteBuffer(bb));
            filter = new NamesQueryFilter(ss);
        }
        else
        {
            filter = new SliceQueryFilter(comparator.fromByteBuffer(sr.start),
                                          comparator.fromByteBuffer(sr.finish),
                                          sr.reversed,
                                          sr.count);
        }

        if (metadata.isSuper())
            filter = SuperColumns.fromSCFilter(metadata.comparator, superColumn, filter);
        return filter;
    }
}
