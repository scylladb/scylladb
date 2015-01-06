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

#include "cell.hh"

namespace db {
/**
 * Alternative to Cell that have an expiring time.
 * ExpiringCell is immutable (as Cell is).
 *
 * Note that ExpiringCell does not override Cell.getMarkedForDeleteAt,
 * which means that it's in the somewhat unintuitive position of being deleted (after its expiration)
 * without having a time-at-which-it-became-deleted.  (Because ttl is a server-side measurement,
 * we can't mix it with the timestamp field, which is client-supplied and whose resolution we
 * can't assume anything about.)
 */
class expiring_cell : public cell {
public:
    static const int32_t MAX_TTL = 20 * 365 * 24 * 60 * 60; // 20 years in seconds

    virtual int32_t get_time_to_live() = 0;

    virtual std::shared_ptr<cell> local_copy(CFMetaData metadata, AbstractAllocator allocator) = 0;
    virtual std::shared_ptr<cell> local_copy(CFMetaData metaData, MemtableAllocator allocator, OpOrder::Group op_group) = 0;
};

}
