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
class CellNameType;
class ColumnIdentifier;
class CFMetaData;

/**
 * A CellName is a Composite, but for which, for the sake of CQL3, we
 * distinguish different parts: a CellName has first a number of clustering
 * components, followed by the CQL3 column name, and then possibly followed by
 * a collection element part.
 *
 * The clustering prefix can itself be composed of multiple component. It can
 * also be empty if the table has no clustering keys. In general, the CQL3
 * column name follows. However, some type of COMPACT STORAGE layout do not
 * store the CQL3 column name in the cell name and so this part can be null (we
 * call "dense" the cells whose name don't store the CQL3 column name).
 *
 * Lastly, if the cell is part of a CQL3 collection, we'll have a last
 * component (a UUID for lists, an element for sets and a key for maps).
 */
class cell_name : public composite {
public:
    /**
     * The number of clustering components.
     *
     * It can be 0 if the table has no clustering columns, and it can be
     * equal to size() if the table is dense() (in which case cql3ColumnName()
     * will be null).
     */
    virtual int32_t clustering_size() = 0;

    /**
     * The name of the CQL3 column this cell represents.
     *
     * Will be null for cells of "dense" tables.
     * @param metadata
     */
    virtual ColumnIdentifier cql3_column_name(CFMetaData metadata) = 0;

    /**
     * The value of the collection element, or null if the cell is not part
     * of a collection (i.e. if !isCollectionCell()).
     */
    virtual bytes collection_element() = 0 ;
    virtual bool is_collection_cell() = 0;

    /**
     * Whether this cell is part of the same CQL3 row as the other cell.
     */
    virtual bool is_same_cql3_row_as(CellNameType type, cell_name& other) = 0;

    // If cellnames were sharing some prefix components, this will break it, so
    // we might want to try to do better.
    // @Override
#if 0
    // FIXME
    virtual cell_name copy(CFMetaData cfm, AbstractAllocator allocator) = 0;
#endif

    virtual int64_t unshared_heap_size_excluding_data() = 0;
};

} // composites
} // db
