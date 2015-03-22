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
#include "composite.hh"
#include "c_builder.hh"
#include "c_type.hh"
#include "cql3/column_identifier.hh"
#include "cql3/cql3_row.hh"
#include "io/i_versioned_serializer.hh"
#include "io/i_serializer.hh"
#include "db/on_disk_atom.hh"
#include <boost/any.hpp>

namespace db {
namespace composites {

using namespace cql3;
using namespace io;

class CFMetaData;
class DataInput;
class ColumnSerializer;
class ColumnToCollectionType;
class CollectionType;
class NamesQueryFilter;
class IDiskAtomFilter;
class Cell;

/**
 * The type of CellNames.
 *
 * In the same way that a CellName is a Composite, a CellNameType is a CType, but
 * with a number of method specific to cell names.
 *
 * On top of the dichotomy simple/truly-composite of composites, cell names comes
 * in 2 variants: "dense" and "sparse". The sparse ones are CellName where one of
 * the component (the last or second-to-last for collections) is used to store the
 * CQL3 column name. Dense are those for which it's not the case.
 *
 * In other words, we have 4 types of CellName/CellNameType which correspond to the
 * 4 type of table layout that we need to distinguish:
 *   1. Simple (non-truly-composite) dense: this is the dynamic thrift CFs whose
 *      comparator is not composite.
 *   2. Composite dense: this is the dynamic thrift CFs with a CompositeType comparator.
 *   3. Simple (non-truly-composite) sparse: this is the thrift static CFs (that
 *      don't have a composite comparator).
 *   4. Composite sparse: this is the CQL3 layout (note that this is the only one that
 *      support collections).
 */
class cell_name_type : public c_type {
public:
    class deserializer;

    /**
     * Whether or not the cell names for this type are dense.
     */
    virtual bool is_dense() = 0;

    /**
     * The number of clustering columns for the table this is the type of.
     */
    virtual int32_t exploded_clustering_prefix_size() = 0;

    /**
     * A builder for the clustering prefix.
     */
    virtual std::unique_ptr<c_builder> prefix_builder() = 0;

    /**
     * The prefix to use for static columns.
     *
     * Note that create() methods below for creating CellName automatically handle static columns already
     * for convenience, and so there is not need to pass this prefix for them. There is few other cases
     * where we need the prefix directly however.
     */
    virtual composite& static_prefix() = 0;

    /**
     * Whether or not there is some collections defined in this type.
     */
    virtual bool has_collections() = 0;

    /**
     * Whether or not this type layout support collections.
     */
    virtual bool support_collections() = 0;

    /**
     * The type of the collections (or null if the type does not have any non-frozen collections).
     */
    virtual ColumnToCollectionType& collectionType() = 0;

    /**
     * Return the new type obtained by adding/updating to the new collection type for the provided column name
     * to this type.
     */
    virtual std::unique_ptr<cell_name_type> add_or_update_collection(column_identifier columnName, CollectionType newCollection) = 0;

    /**
     * Returns a new CellNameType that is equivalent to this one but with one
     * of the subtype replaced by the provided new type.
     */
    // @Override
    // The orign code returns cell_name_type instead of c_type
    virtual std::unique_ptr<c_type> set_subtype(int32_t position, shared_ptr<abstract_type> newtype) = 0;

    /**
     * Creates a row marker for the CQL3 having the provided clustering prefix.
     *
     * Note that this is only valid for CQL3 tables (isCompound() and !isDense()) and should
     * only be called for them.
     */
    virtual std::unique_ptr<cell_name> row_marker(composite& prefix) = 0;

    /**
     * Creates a new CellName given a clustering prefix and a CQL3 column.
     *
     * Note that for dense types, the column can be null as a shortcut for designing the only
     * COMPACT_VALUE column of the table.
     */
    virtual std::unique_ptr<cell_name> create(composite& prefix, column_definition column) = 0;

    /**
     * Creates a new collection CellName given a clustering prefix, a CQL3 column and the collection element.
     */
    virtual std::unique_ptr<cell_name> create(composite& prefix, column_definition column, bytes collection_element) = 0;

    /**
     * Convenience method to create cell names given its components.
     *
     * This is equivalent to CType#make() but return a full cell name (and thus
     * require all the components of the name).
     */
    virtual std::unique_ptr<cell_name> make_cell_name(std::initializer_list<boost::any> components) = 0;

    /**
     * Deserialize a Composite from a ByteBuffer.
     *
     * This is equilvalent to CType#fromByteBuffer but assumes the buffer is a full cell
     * name. This is meant for thrift to convert the fully serialized buffer we
     * get from the clients.
     */
    virtual std::unique_ptr<cell_name> cell_from_byte_buffer(bytes bb) = 0;

    /**
     * Creates a new CQL3Row builder for this type. See CQL3Row for details.
     */
     virtual std::unique_ptr<cql3_row::builder> cql3_row_builder(CFMetaData metadata, long now) = 0;

    // The two following methods are used to pass the declared regular column names (in CFMetaData)
    // to the CellNameType. This is only used for optimization sake, see SparseCellNameType.
    virtual void add_cql3_column(column_identifier id) = 0;
    virtual void remove_cql3_column(column_identifier id) = 0;

    /**
     * Creates a new Deserializer. This is used by AtomDeserializer to do incremental and on-demand
     * deserialization of the on disk atoms. See AtomDeserializer for details.
     */
    virtual std::unique_ptr<deserializer> new_deserializer(DataInput in) = 0;

    /*
     * Same as in CType, follows a number of per-CellNameType instances for the Comparator and Serializer used
     * throughout the code (those that require full CellName versus just Composite).
     */
    // Ultimately, those might be split into an IVersionedSerializer and an ISSTableSerializer
    virtual std::unique_ptr<i_serializer<cell_name>> cell_serializer() = 0;

    virtual shared_ptr<comparator<Cell>> column_comparator(bool is_right_native) = 0;
    virtual shared_ptr<comparator<boost::any>> asymmetric_columncomparator(bool is_right_native) = 0;
    virtual shared_ptr<comparator<Cell>> column_reverse_comparator() = 0;
    virtual shared_ptr<comparator<on_disk_atom>> on_disk_atom_comparator() = 0;

    virtual std::unique_ptr<ColumnSerializer> column_Serializer() = 0;
    virtual std::unique_ptr<on_disk_atom::serializer> onDiskAtomSerializer() = 0;
    virtual std::unique_ptr<i_versioned_serializer<NamesQueryFilter>> names_query_filter_serializer() = 0;
    virtual std::unique_ptr<i_versioned_serializer<IDiskAtomFilter>> disk_atomfilter_serializer() = 0;

    class deserializer {
        /**
         * Whether this deserializer is done or not, i.e. whether we're reached the end of row marker.
         */
        virtual bool has_next() = 0;

        /**
         * Whether or not some name has been read but not consumed by readNext.
         */
        virtual bool has_unprocessed() = 0;

        /**
         * Comparare the next name to read to the provided Composite.
         * This does not consume the next name.
         */
        virtual int32_t compare_next_to(const composite& composite_) = 0;

        /**
         * Actually consume the next name and return it.
         */
        virtual composite& read_next() = 0;

        /**
         * Skip the next name (consuming it).
         */
        virtual void skip_next() = 0;
    };
};

} // composites
} // db
