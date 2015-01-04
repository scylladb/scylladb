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
 *
 * Copyright 2014 Cloudius Systems
 */

#pragma once

#include "database.hh"

namespace db {
namespace composites {

// FIXME
class CType;
class CFMetaData;
class ColumnSlice;
class AbstractAllocator;


/**
 * A composite value.
 *
 * This can be though as a list of ByteBuffer, except that this also include an
 * 'end-of-component' flag, that allow precise selection of composite ranges.
 *
 * We also make a difference between "true" composites and the "simple" ones. The
 * non-truly composite will have a size() == 1 but differs from true composites with
 * size() == 1 in the way they are stored. Most code shouldn't have to care about the
 * difference.
 */
class composite /* extends IMeasurableMemory */ {
public:
    class EOC {
    public:
        static const EOC START;
        static const EOC NONE;
        static const EOC END;

        // If composite p has this EOC and is a strict prefix of composite c, then this
        // the result of the comparison of p and c. Basically, p sorts before c unless
        // it's EOC is END.
        EOC(int32_t prefix_comparison_result_)
            : prefix_comparison_result(prefix_comparison_result_)
        { }

        static const EOC& from(int32_t eoc)
        {
            return eoc == 0 ? NONE : (eoc < 0 ? START : END);
        }
        int32_t prefix_comparison_result;
    };

    virtual int32_t size() = 0;
    virtual bool is_empty() = 0;
    virtual bytes get(int32_t i) = 0;

    virtual EOC eoc() = 0;
    virtual composite& with_eoc(EOC eoc) = 0;
    virtual composite& start() = 0;
    virtual composite& end() = 0;
    virtual ColumnSlice slice() = 0;

    virtual bool is_static() = 0;

    virtual bool is_prefix_of(CType type, composite& other) = 0;

    virtual bytes to_byte_buffer() = 0;

    virtual int32_t data_size() = 0;
    virtual composite& copy(CFMetaData cfm, AbstractAllocator allocator) = 0;
};

} // composites
} // db
