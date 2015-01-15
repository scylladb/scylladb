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
#include "database.hh"
#include "on_disk_atom.hh"
#include "core/sstring.hh"
#include "composites/cell_name.hh"
#include "composites/cell_name_type.hh"
#include "utils/fb_utilities.hh"

namespace db {

class TypeSizes;
class MemtableAllocator;
class OpOrder {
public:
    class Group {
    };
};

using namespace composites;
using namespace utils;

/**
 * Cell is immutable, which prevents all kinds of confusion in a multithreaded environment.
 */
class cell : public on_disk_atom {
public:
    static const int32_t MAX_NAME_LENGTH = fb_utilities::MAX_UNSIGNED_SHORT;

    virtual shared_ptr<cell> with_updated_name(shared_ptr<cell_name> new_name) = 0;

    virtual shared_ptr<cell> with_updated_timestamp(int64_t new_timestamp) = 0;

    //@Override
    virtual shared_ptr<cell_name> name() = 0;

    virtual bytes value() = 0;

    virtual bool is_live() = 0;

    virtual bool is_live(int64_t now) = 0;

    virtual int32_t cell_data_size() = 0;

    // returns the size of the Cell and all references on the heap, excluding any costs associated with byte arrays
    // that would be allocated by a localCopy, as these will be accounted for by the allocator
    virtual int64_t unshared_heap_size_excluding_data() = 0;

    // FIXME: real TypeSizes
    virtual int32_t serialized_size(shared_ptr<cell_name_type> type, TypeSizes& type_sizes) = 0;

    virtual int32_t serialization_flags() = 0;

    virtual shared_ptr<cell> diff(shared_ptr<cell> cell) = 0;

    virtual shared_ptr<cell> reconcile(shared_ptr<cell> cell) = 0;

    // FIXME: real CFMetaData, AbstractAllocator, etc...
    virtual shared_ptr<cell> local_copy(CFMetaData metadata, AbstractAllocator allocator) = 0;

    virtual shared_ptr<cell> local_copy(CFMetaData metaData, MemtableAllocator allocator, OpOrder::Group op_group) = 0;

    virtual sstring get_string(shared_ptr<cell_name_type> comparator) = 0;
};

}
