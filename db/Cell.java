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
package org.apache.cassandra.db;

import java.nio.ByteBuffer;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.composites.CellNameType;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.memory.AbstractAllocator;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.memory.MemtableAllocator;

/**
 * Cell is immutable, which prevents all kinds of confusion in a multithreaded environment.
 */
public interface Cell extends OnDiskAtom
{
    public static final int MAX_NAME_LENGTH = FBUtilities.MAX_UNSIGNED_SHORT;

    public Cell withUpdatedName(CellName newName);

    public Cell withUpdatedTimestamp(long newTimestamp);

    @Override
    public CellName name();

    public ByteBuffer value();

    public boolean isLive();

    public boolean isLive(long now);

    public int cellDataSize();

    // returns the size of the Cell and all references on the heap, excluding any costs associated with byte arrays
    // that would be allocated by a localCopy, as these will be accounted for by the allocator
    public long unsharedHeapSizeExcludingData();

    public int serializedSize(CellNameType type, TypeSizes typeSizes);

    public int serializationFlags();

    public Cell diff(Cell cell);

    public Cell reconcile(Cell cell);

    public Cell localCopy(CFMetaData metadata, AbstractAllocator allocator);

    public Cell localCopy(CFMetaData metaData, MemtableAllocator allocator, OpOrder.Group opGroup);

    public String getString(CellNameType comparator);
}
