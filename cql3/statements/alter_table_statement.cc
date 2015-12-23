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
 * Copyright 2015 ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "cql3/statements/alter_table_statement.hh"
#include "service/migration_manager.hh"
#include "validation.hh"

namespace cql3 {

namespace statements {

alter_table_statement::alter_table_statement(shared_ptr<cf_name> name,
                                             type t,
                                             shared_ptr<column_identifier::raw> column_name,
                                             shared_ptr<cql3_type::raw> validator,
                                             shared_ptr<cf_prop_defs> properties,
                                             renames_type renames,
                                             bool is_static)
    : schema_altering_statement(std::move(name))
    , _type(t)
    , _raw_column_name(std::move(column_name))
    , _validator(std::move(validator))
    , _properties(std::move(properties))
    , _renames(std::move(renames))
    , _is_static(is_static)
{
}

void alter_table_statement::check_access(const service::client_state& state)
{
    warn(unimplemented::cause::PERMISSIONS);
#if 0
    state.hasColumnFamilyAccess(keyspace(), columnFamily(), Permission.ALTER);
#endif
}

void alter_table_statement::validate(distributed<service::storage_proxy>& proxy, const service::client_state& state)
{
    // validated in announce_migration()
}

future<bool> alter_table_statement::announce_migration(distributed<service::storage_proxy>& proxy, bool is_local_only)
{
    throw std::runtime_error(sprint("%s not implemented", __PRETTY_FUNCTION__));
#if 0
    CFMetaData meta = validateColumnFamily(keyspace(), columnFamily());
    CFMetaData cfm = meta.copy();

    CQL3Type validator = this.validator == null ? null : this.validator.prepare(keyspace());
    ColumnIdentifier columnName = null;
    ColumnDefinition def = null;
    if (rawColumnName != null)
    {
        columnName = rawColumnName.prepare(cfm);
        def = cfm.getColumnDefinition(columnName);
    }

    switch (oType)
    {
        case ADD:
            assert columnName != null;
            if (cfm.comparator.isDense())
                throw new InvalidRequestException("Cannot add new column to a COMPACT STORAGE table");

            if (isStatic)
            {
                if (!cfm.comparator.isCompound())
                    throw new InvalidRequestException("Static columns are not allowed in COMPACT STORAGE tables");
                if (cfm.clusteringColumns().isEmpty())
                    throw new InvalidRequestException("Static columns are only useful (and thus allowed) if the table has at least one clustering column");
            }

            if (def != null)
            {
                switch (def.kind)
                {
                    case PARTITION_KEY:
                    case CLUSTERING_COLUMN:
                        throw new InvalidRequestException(String.format("Invalid column name %s because it conflicts with a PRIMARY KEY part", columnName));
                    default:
                        throw new InvalidRequestException(String.format("Invalid column name %s because it conflicts with an existing column", columnName));
                }
            }

            // Cannot re-add a dropped counter column. See #7831.
            if (meta.isCounter() && meta.getDroppedColumns().containsKey(columnName))
                throw new InvalidRequestException(String.format("Cannot re-add previously dropped counter column %s", columnName));

            AbstractType<?> type = validator.getType();
            if (type.isCollection() && type.isMultiCell())
            {
                if (!cfm.comparator.supportCollections())
                    throw new InvalidRequestException("Cannot use non-frozen collections with a non-composite PRIMARY KEY");
                if (cfm.isSuper())
                    throw new InvalidRequestException("Cannot use non-frozen collections with super column families");

                // If there used to be a collection column with the same name (that has been dropped), it will
                // still be appear in the ColumnToCollectionType because or reasons explained on #6276. The same
                // reason mean that we can't allow adding a new collection with that name (see the ticket for details).
                if (cfm.comparator.hasCollections())
                {
                    CollectionType previous = cfm.comparator.collectionType() == null ? null : cfm.comparator.collectionType().defined.get(columnName.bytes);
                    if (previous != null && !type.isCompatibleWith(previous))
                        throw new InvalidRequestException(String.format("Cannot add a collection with the name %s " +
                                    "because a collection with the same name and a different type has already been used in the past", columnName));
                }

                cfm.comparator = cfm.comparator.addOrUpdateCollection(columnName, (CollectionType)type);
            }

            Integer componentIndex = cfm.comparator.isCompound() ? cfm.comparator.clusteringPrefixSize() : null;
            cfm.addColumnDefinition(isStatic
                                    ? ColumnDefinition.staticDef(cfm, columnName.bytes, type, componentIndex)
                                    : ColumnDefinition.regularDef(cfm, columnName.bytes, type, componentIndex));
            break;

        case ALTER:
            assert columnName != null;
            if (def == null)
                throw new InvalidRequestException(String.format("Column %s was not found in table %s", columnName, columnFamily()));

            AbstractType<?> validatorType = validator.getType();
            switch (def.kind)
            {
                case PARTITION_KEY:
                    if (validatorType instanceof CounterColumnType)
                        throw new InvalidRequestException(String.format("counter type is not supported for PRIMARY KEY part %s", columnName));
                    if (cfm.getKeyValidator() instanceof CompositeType)
                    {
                        List<AbstractType<?>> oldTypes = ((CompositeType) cfm.getKeyValidator()).types;
                        if (!validatorType.isValueCompatibleWith(oldTypes.get(def.position())))
                            throw new ConfigurationException(String.format("Cannot change %s from type %s to type %s: types are incompatible.",
                                                                           columnName,
                                                                           oldTypes.get(def.position()).asCQL3Type(),
                                                                           validator));

                        List<AbstractType<?>> newTypes = new ArrayList<AbstractType<?>>(oldTypes);
                        newTypes.set(def.position(), validatorType);
                        cfm.keyValidator(CompositeType.getInstance(newTypes));
                    }
                    else
                    {
                        if (!validatorType.isValueCompatibleWith(cfm.getKeyValidator()))
                            throw new ConfigurationException(String.format("Cannot change %s from type %s to type %s: types are incompatible.",
                                                                           columnName,
                                                                           cfm.getKeyValidator().asCQL3Type(),
                                                                           validator));
                        cfm.keyValidator(validatorType);
                    }
                    break;
                case CLUSTERING_COLUMN:
                    if (!cfm.isCQL3Table())
                        throw new InvalidRequestException(String.format("Cannot alter clustering column %s in a non-CQL3 table", columnName));

                    AbstractType<?> oldType = cfm.comparator.subtype(def.position());
                    // Note that CFMetaData.validateCompatibility already validate the change we're about to do. However, the error message it
                    // sends is a bit cryptic for a CQL3 user, so validating here for a sake of returning a better error message
                    // Do note that we need isCompatibleWith here, not just isValueCompatibleWith.
                    if (!validatorType.isCompatibleWith(oldType))
                        throw new ConfigurationException(String.format("Cannot change %s from type %s to type %s: types are not order-compatible.",
                                                                       columnName,
                                                                       oldType.asCQL3Type(),
                                                                       validator));

                    cfm.comparator = cfm.comparator.setSubtype(def.position(), validatorType);
                    break;
                case COMPACT_VALUE:
                    // See below
                    if (!validatorType.isValueCompatibleWith(cfm.getDefaultValidator()))
                        throw new ConfigurationException(String.format("Cannot change %s from type %s to type %s: types are incompatible.",
                                                                       columnName,
                                                                       cfm.getDefaultValidator().asCQL3Type(),
                                                                       validator));
                    cfm.defaultValidator(validatorType);
                    break;
                case REGULAR:
                case STATIC:
                    // Thrift allows to change a column validator so CFMetaData.validateCompatibility will let it slide
                    // if we change to an incompatible type (contrarily to the comparator case). But we don't want to
                    // allow it for CQL3 (see #5882) so validating it explicitly here. We only care about value compatibility
                    // though since we won't compare values (except when there is an index, but that is validated by
                    // ColumnDefinition already).
                    if (!validatorType.isValueCompatibleWith(def.type))
                        throw new ConfigurationException(String.format("Cannot change %s from type %s to type %s: types are incompatible.",
                                                                       columnName,
                                                                       def.type.asCQL3Type(),
                                                                       validator));

                    // For collections, if we alter the type, we need to update the comparator too since it includes
                    // the type too (note that isValueCompatibleWith above has validated that the new type doesn't
                    // change the underlying sorting order, but we still don't want to have a discrepancy between the type
                    // in the comparator and the one in the ColumnDefinition as that would be dodgy).
                    if (validatorType.isCollection() && validatorType.isMultiCell())
                        cfm.comparator = cfm.comparator.addOrUpdateCollection(def.name, (CollectionType)validatorType);

                    break;
            }
            // In any case, we update the column definition
            cfm.addOrReplaceColumnDefinition(def.withNewType(validatorType));
            break;

        case DROP:
            assert columnName != null;
            if (!cfm.isCQL3Table())
                throw new InvalidRequestException("Cannot drop columns from a non-CQL3 table");
            if (def == null)
                throw new InvalidRequestException(String.format("Column %s was not found in table %s", columnName, columnFamily()));

            switch (def.kind)
            {
                case PARTITION_KEY:
                case CLUSTERING_COLUMN:
                    throw new InvalidRequestException(String.format("Cannot drop PRIMARY KEY part %s", columnName));
                case REGULAR:
                case STATIC:
                    ColumnDefinition toDelete = null;
                    for (ColumnDefinition columnDef : cfm.regularAndStaticColumns())
                    {
                        if (columnDef.name.equals(columnName))
                            toDelete = columnDef;
                    }
                    assert toDelete != null;
                    cfm.removeColumnDefinition(toDelete);
                    cfm.recordColumnDrop(toDelete);
                    break;
            }
            break;
        case OPTS:
            if (cfProps == null)
                throw new InvalidRequestException(String.format("ALTER COLUMNFAMILY WITH invoked, but no parameters found"));

            cfProps.validate();

            if (meta.isCounter() && cfProps.getDefaultTimeToLive() > 0)
                throw new InvalidRequestException("Cannot set default_time_to_live on a table with counters");

            cfProps.applyToCFMetadata(cfm);
            break;
        case RENAME:
            for (Map.Entry<ColumnIdentifier.Raw, ColumnIdentifier.Raw> entry : renames.entrySet())
            {
                ColumnIdentifier from = entry.getKey().prepare(cfm);
                ColumnIdentifier to = entry.getValue().prepare(cfm);
                cfm.renameColumn(from, to);
            }
            break;
    }

    MigrationManager.announceColumnFamilyUpdate(cfm, false, isLocalOnly);
    return true;
#endif
}

shared_ptr<transport::event::schema_change> alter_table_statement::change_event()
{
    return make_shared<transport::event::schema_change>(transport::event::schema_change::change_type::UPDATED,
        transport::event::schema_change::target_type::TABLE, keyspace(), column_family());
}

}

}