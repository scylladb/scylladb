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

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.utils.memory.AbstractAllocator;

public class SimpleSparseInternedCellName extends SimpleSparseCellName
{

    // Not meant to be used directly, you should use the CellNameType method instead
    SimpleSparseInternedCellName(ColumnIdentifier columnName)
    {
        super(columnName);
    }

    @Override
    public long unsharedHeapSizeExcludingData()
    {
        return 0;
    }

    @Override
    public long unsharedHeapSize()
    {
        return 0;
    }

    @Override
    public CellName copy(CFMetaData cfm, AbstractAllocator allocator)
    {
        // We're interning those instance in SparceCellNameType so don't need to copy.
        return this;
    }
}
