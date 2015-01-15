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
 * Modified by Cloudius Systems
 * Copyright 2015 Cloudius Systems
 */

#pragma once

#include "core/shared_ptr.hh"
#include "cell.hh"

namespace db {

/**
 * A column that represents a partitioned counter.
 */
class counter_cell : public cell {
    // FIXME: CounterContext
    //static final CounterContext contextManager = CounterContext.instance();

    virtual int64_t timestamp_of_last_delete() = 0;

    virtual int64_t total() = 0;

    virtual bool has_legacy_shards() = 0;

    virtual shared_ptr<cell> mark_local_tobe_cleared() = 0;

    virtual shared_ptr<cell> local_copy(CFMetaData metadata, AbstractAllocator allocator) = 0;

    virtual shared_ptr<cell> local_copy(CFMetaData metaData, MemtableAllocator allocator, OpOrder::Group op_group) = 0;
};

}
