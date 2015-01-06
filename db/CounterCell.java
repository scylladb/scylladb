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

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.memory.AbstractAllocator;
import org.apache.cassandra.utils.memory.MemtableAllocator;

/**
 * A column that represents a partitioned counter.
 */
public interface CounterCell extends Cell
{
    static final CounterContext contextManager = CounterContext.instance();

    public long timestampOfLastDelete();

    public long total();

    public boolean hasLegacyShards();

    public Cell markLocalToBeCleared();

    CounterCell localCopy(CFMetaData metadata, AbstractAllocator allocator);

    CounterCell localCopy(CFMetaData metaData, MemtableAllocator allocator, OpOrder.Group opGroup);
}
