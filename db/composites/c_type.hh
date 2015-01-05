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

#include "core/sstring.hh"
#include "database.hh"
#include "composite.hh"
#include "c_builder.hh"
#include "io/i_versioned_serializer.hh"
#include "io/i_serializer.hh"
#include "db/deletion_info.hh"
#include "db/range_tombstone.hh"

namespace db {
namespace composites {

using namespace io;
class DataInput;
class ColumnSlice;
class SliceQueryFilter;
class IndexInfo;

/**
 * A type for a Composite.
 *
 * There is essentially 2 types of Composite and such of CType:
 *   1. the "simple" ones, see SimpleCType.
 *   2. the "truly-composite" ones, see CompositeCType.
 *
 * API-wise, a CType is simply a collection of AbstractType with a few utility
 * methods.
 */
class c_type : public comparator<composite> {
    class serializer;
    /**
     * Returns whether this is a "truly-composite" underneath.
     */
    virtual bool is_compound() = 0;

    /**
     * The number of subtypes for this CType.
     */
    virtual int32_t size() = 0;

    virtual int32_t compare(const composite& o1, const composite& o2) = 0;

    /**
     * Gets a subtype of this CType.
     */
    virtual shared_ptr<abstract_type> subtype(int32_t i) = 0;

    /**
     * A builder of Composite.
     */
    virtual std::unique_ptr<c_builder> builder() = 0;

    /**
     * Convenience method to build composites from their component.
     *
     * The arguments can be either ByteBuffer or actual objects of the type
     * corresponding to their position.
     */
    virtual std::unique_ptr<composite> make(std::initializer_list<boost::any> components) = 0;

    /**
     * Validates a composite.
     */
    virtual void validate(composite& name) = 0;

    /**
     * Converts a composite to a user-readable string.
     */
    virtual sstring get_string(composite& c) = 0;

    /**
     * See AbstractType#isCompatibleWith.
     */
    virtual bool is_compatible_with(c_type& previous) = 0;

    /**
     * Returns a new CType that is equivalent to this CType but with
     * one of the subtype replaced by the provided new type.
     */
    virtual std::unique_ptr<c_type> set_subtype(int32_t position, shared_ptr<abstract_type> new_type) = 0;

    /**
     * Deserialize a Composite from a ByteBuffer.
     *
     * This is meant for thrift to convert the fully serialized buffer we
     * get from the clients to composites.
     */
    virtual std::unique_ptr<composite> from_byte_buffer(bytes bb) = 0;

    /**
     * Returns a AbstractType corresponding to this CType for thrift sake.
     *
     * If the CType is a "simple" one, this just return the wrapped type, otherwise
     * it returns the corresponding org.apache.cassandra.db.marshal.CompositeType.
     *
     * This is only meant to be use for backward compatibility (particularly for
     * thrift) but it's not meant to be used internally.
     */
    virtual shared_ptr<abstract_type> as_abstract_type() = 0;

    /**********************************************************/

    /*
     * Follows a number of per-CType instances for the Comparator and Serializer used throughout
     * the code. The reason we need this is that we want the per-CType/per-CellNameType Composite/CellName
     * serializers, which means the following instances have to depend on the type too.
     */

    virtual std::shared_ptr<comparator<composite>> reverse_comparator() = 0;
    virtual std::shared_ptr<comparator<IndexInfo>> index_comparator() = 0;
    virtual std::shared_ptr<comparator<IndexInfo>> index_reverse_comparator() = 0;

    virtual std::unique_ptr<serializer> serializer() = 0;

    virtual std::unique_ptr<i_versioned_serializer<ColumnSlice>> slice_serializer() = 0;
    virtual std::unique_ptr<i_versioned_serializer<SliceQueryFilter>> slice_query_filter_serializer() = 0;
    virtual std::unique_ptr<deletion_info::serializer> deletion_info_serializer() = 0;
    virtual std::unique_ptr<range_tombstone::serializer> range_tombstone_serializer() = 0;

    class serializer : public i_serializer<composite> {
        virtual void skip(DataInput in) = 0;
    };
};

} // composites
} // db
