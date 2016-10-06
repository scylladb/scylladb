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
 * Copyright (C) 2016 ScyllaDB
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


#include <inttypes.h>
#include <regex>

#include <boost/range/adaptor/map.hpp>
#include <boost/range/algorithm/adjacent_find.hpp>

#include "cql3/statements/create_view_statement.hh"
#include "cql3/statements/prepared_statement.hh"
#include "schema_builder.hh"
#include "service/storage_proxy.hh"


namespace cql3 {

namespace statements {

create_view_statement::create_view_statement(
        ::shared_ptr<cf_name> view_name,
        ::shared_ptr<cf_name> base_name,
        std::vector<::shared_ptr<selection::raw_selector>> select_clause,
        std::vector<::shared_ptr<relation>> where_clause,
        std::vector<::shared_ptr<cql3::column_identifier::raw>> partition_keys,
        std::vector<::shared_ptr<cql3::column_identifier::raw>> clustering_keys,
        ::shared_ptr<cf_prop_defs> properties,
        bool if_not_exists)
    : schema_altering_statement{view_name}
    , _base_name{base_name}
    , _select_clause{select_clause}
    , _where_clause{where_clause}
    , _partition_keys{partition_keys}
    , _clustering_keys{clustering_keys}
    , _properties{properties}
    , _if_not_exists{if_not_exists}
{
    // If no compression property is specified explicitly for the new table,
    // use the default one:
    if (!properties->has_property(cf_prop_defs::KW_COMPRESSION) && schema::DEFAULT_COMPRESSOR) {
        std::map<sstring, sstring> compression = {
            { sstring(compression_parameters::SSTABLE_COMPRESSION), schema::DEFAULT_COMPRESSOR.value() },
        };
        properties->add_property(cf_prop_defs::KW_COMPRESSION, compression);
    }
    // TODO: probably need to create a "statement_restrictions" like select does
    // based on the select_clause, base_name and where_clause; However need to
    // pass for_view=true.
    throw exceptions::unsupported_operation_exception("Materialized views not yet supported");
}

// FIXME: I copied the following from create_table_statement. I don't know
// what they do or whether they need to change for create view.
future<> create_view_statement::check_access(const service::client_state& state) {
    return state.has_keyspace_access(keyspace(), auth::permission::CREATE);
}

void create_view_statement::validate(distributed<service::storage_proxy>&, const service::client_state& state) {
    // validated in announceMigration()
}

future<bool> create_view_statement::announce_migration(distributed<service::storage_proxy>& proxy, bool is_local_only) {
    // FIXME: this code from create_table_view is probably wrong, the Java CreateViewStatement.announceMigration is much more elaborate
#if 0
    ****** Our implementation in creat_table_statement (simpler code but that was simpler also in Java)
    return make_ready_future<>().then([this, is_local_only] {
        return service::get_local_migration_manager().announce_new_column_family(get_cf_meta_data(), is_local_only);
    }).then_wrapped([this] (auto&& f) {
        try {
            f.get();
            return true;
        } catch (const exceptions::already_exists_exception& e) {
            if (_if_not_exists) {
                return false;
            }
            throw e;
        }
    });
#endif
#if 0
    ***** This if 0 code is from Cassandra CreateViewStatement
    // We need to make sure that:
    //  - primary key includes all columns in base table's primary key
    //  - make sure that the select statement does not have anything other than columns
    //    and their names match the base table's names
    //  - make sure that primary key does not include any collections
    //  - make sure there is no where clause in the select statement
    //  - make sure there is not currently a table or view
    //  - make sure baseTable gcGraceSeconds > 0

    properties.validate();

    if (properties.useCompactStorage)
        throw new InvalidRequestException("Cannot use 'COMPACT STORAGE' when defining a materialized view");
#endif

    // View and base tables must be in the same keyspace, to ensure that RF
    // is the same (because we assign a view replica to each base replica).
    // If a keyspace was not specified for the base table name, it is assumed
    // it is in the same keyspace as the view table being created (which
    // itself might be the current USEd keyspace, or explicitly specified).
    if (_base_name->get_keyspace().empty()) {
        _base_name->set_keyspace(keyspace(), true);
    }
    if (_base_name->get_keyspace() != keyspace()) {
        throw exceptions::invalid_request_exception(sprint(
                "Cannot create a materialized view on a table in a separate keyspace ('%s' != '%s')",
                _base_name->get_keyspace(), keyspace()));
    }

    // Validate that the keyspace and the base table exist, and is not a
    // special table on which we cannot create views:
    // CONTINUE HERE: something like the code below taken from migration_manager instead of the validateColumFamily below
    auto& db = service::get_local_storage_proxy().get_db().local();
    if (!db.has_keyspace(_base_name->get_keyspace())) {
        throw exceptions::invalid_request_exception(sprint(
                "Keyspace '%s' does not exist", _base_name->get_keyspace()));
    }
//    auto& ks = db.find_keyspace(_base_name->get_keyspace());
    if (!db.has_schema(_base_name->get_keyspace(), _base_name->get_column_family())) {
        throw exceptions::invalid_request_exception(sprint(
                "Base table '%s' does not exist",
                _base_name->get_column_family()));
    }
    return make_ready_future<bool>(true);
//    if (db.has_schema(_base_name->get_keyspace(), _base_name->get_column_family())) {
//            throw exceptions::already_exists_exception(cfm->ks_name(), cfm->cf_name());
//        }
#if 0
    CFMetaData cfm = ThriftValidation.validateColumnFamily(baseName.getKeyspace(), baseName.getColumnFamily());

    if (cfm.isCounter())
        throw new InvalidRequestException("Materialized views are not supported on counter tables");
    if (cfm.isView())
        throw new InvalidRequestException("Materialized views cannot be created against other materialized views");

    if (cfm.params.gcGraceSeconds == 0)
    {
        throw new InvalidRequestException(String.format("Cannot create materialized view '%s' for base table " +
                                                        "'%s' with gc_grace_seconds of 0, since this value is " +
                                                        "used to TTL undelivered updates. Setting gc_grace_seconds" +
                                                        " too low might cause undelivered updates to expire " +
                                                        "before being replayed.", cfName.getColumnFamily(),
                                                        baseName.getColumnFamily()));
    }

    Set<ColumnIdentifier> included = new HashSet<>();
    for (RawSelector selector : selectClause)
    {
        Selectable.Raw selectable = selector.selectable;
        if (selectable instanceof Selectable.WithFieldSelection.Raw)
            throw new InvalidRequestException("Cannot select out a part of type when defining a materialized view");
        if (selectable instanceof Selectable.WithFunction.Raw)
            throw new InvalidRequestException("Cannot use function when defining a materialized view");
        if (selectable instanceof Selectable.WritetimeOrTTL.Raw)
            throw new InvalidRequestException("Cannot use function when defining a materialized view");
        ColumnIdentifier identifier = (ColumnIdentifier) selectable.prepare(cfm);
        if (selector.alias != null)
            throw new InvalidRequestException(String.format("Cannot alias column '%s' as '%s' when defining a materialized view", identifier.toString(), selector.alias.toString()));

        ColumnDefinition cdef = cfm.getColumnDefinition(identifier);

        if (cdef == null)
            throw new InvalidRequestException("Unknown column name detected in CREATE MATERIALIZED VIEW statement : "+identifier);

        included.add(identifier);
    }

    Set<ColumnIdentifier.Raw> targetPrimaryKeys = new HashSet<>();
    for (ColumnIdentifier.Raw identifier : Iterables.concat(partitionKeys, clusteringKeys))
    {
        if (!targetPrimaryKeys.add(identifier))
            throw new InvalidRequestException("Duplicate entry found in PRIMARY KEY: "+identifier);

        ColumnDefinition cdef = cfm.getColumnDefinition(identifier.prepare(cfm));

        if (cdef == null)
            throw new InvalidRequestException("Unknown column name detected in CREATE MATERIALIZED VIEW statement : "+identifier);

        if (cfm.getColumnDefinition(identifier.prepare(cfm)).type.isMultiCell())
            throw new InvalidRequestException(String.format("Cannot use MultiCell column '%s' in PRIMARY KEY of materialized view", identifier));

        if (cdef.isStatic())
            throw new InvalidRequestException(String.format("Cannot use Static column '%s' in PRIMARY KEY of materialized view", identifier));
    }

    // build the select statement
    Map<ColumnIdentifier.Raw, Boolean> orderings = Collections.emptyMap();
    SelectStatement.Parameters parameters = new SelectStatement.Parameters(orderings, false, true, false);
    SelectStatement.RawStatement rawSelect = new SelectStatement.RawStatement(baseName, parameters, selectClause, whereClause, null, null);

    ClientState state = ClientState.forInternalCalls();
    state.setKeyspace(keyspace());

    rawSelect.prepareKeyspace(state);
    rawSelect.setBoundVariables(getBoundVariables());

    ParsedStatement.Prepared prepared = rawSelect.prepare(true);
    SelectStatement select = (SelectStatement) prepared.statement;
    StatementRestrictions restrictions = select.getRestrictions();

    if (!prepared.boundNames.isEmpty())
        throw new InvalidRequestException("Cannot use query parameters in CREATE MATERIALIZED VIEW statements");

    if (!restrictions.nonPKRestrictedColumns(false).isEmpty())
    {
        throw new InvalidRequestException(String.format(
                "Non-primary key columns cannot be restricted in the SELECT statement used for materialized view " +
                "creation (got restrictions on: %s)",
                restrictions.nonPKRestrictedColumns(false).stream().map(def -> def.name.toString()).collect(Collectors.joining(", "))));
    }

    String whereClauseText = View.relationsToWhereClause(whereClause.relations);

    Set<ColumnIdentifier> basePrimaryKeyCols = new HashSet<>();
    for (ColumnDefinition definition : Iterables.concat(cfm.partitionKeyColumns(), cfm.clusteringColumns()))
        basePrimaryKeyCols.add(definition.name);

    List<ColumnIdentifier> targetClusteringColumns = new ArrayList<>();
    List<ColumnIdentifier> targetPartitionKeys = new ArrayList<>();

    // This is only used as an intermediate state; this is to catch whether multiple non-PK columns are used
    boolean hasNonPKColumn = false;
    for (ColumnIdentifier.Raw raw : partitionKeys)
        hasNonPKColumn |= getColumnIdentifier(cfm, basePrimaryKeyCols, hasNonPKColumn, raw, targetPartitionKeys, restrictions);

    for (ColumnIdentifier.Raw raw : clusteringKeys)
        hasNonPKColumn |= getColumnIdentifier(cfm, basePrimaryKeyCols, hasNonPKColumn, raw, targetClusteringColumns, restrictions);

    // We need to include all of the primary key columns from the base table in order to make sure that we do not
    // overwrite values in the view. We cannot support "collapsing" the base table into a smaller number of rows in
    // the view because if we need to generate a tombstone, we have no way of knowing which value is currently being
    // used in the view and whether or not to generate a tombstone. In order to not surprise our users, we require
    // that they include all of the columns. We provide them with a list of all of the columns left to include.
    boolean missingClusteringColumns = false;
    StringBuilder columnNames = new StringBuilder();
    List<ColumnIdentifier> includedColumns = new ArrayList<>();
    for (ColumnDefinition def : cfm.allColumns())
    {
        ColumnIdentifier identifier = def.name;
        boolean includeDef = included.isEmpty() || included.contains(identifier);

        if (includeDef && def.isStatic())
        {
            throw new InvalidRequestException(String.format("Unable to include static column '%s' which would be included by Materialized View SELECT * statement", identifier));
        }

        if (includeDef && !targetClusteringColumns.contains(identifier) && !targetPartitionKeys.contains(identifier))
        {
            includedColumns.add(identifier);
        }
        if (!def.isPrimaryKeyColumn()) continue;

        if (!targetClusteringColumns.contains(identifier) && !targetPartitionKeys.contains(identifier))
        {
            if (missingClusteringColumns)
                columnNames.append(',');
            else
                missingClusteringColumns = true;
            columnNames.append(identifier);
        }
    }
    if (missingClusteringColumns)
        throw new InvalidRequestException(String.format("Cannot create Materialized View %s without primary key columns from base %s (%s)",
                                                        columnFamily(), baseName.getColumnFamily(), columnNames.toString()));

    if (targetPartitionKeys.isEmpty())
        throw new InvalidRequestException("Must select at least a column for a Materialized View");

    if (targetClusteringColumns.isEmpty())
        throw new InvalidRequestException("No columns are defined for Materialized View other than primary key");

    CFMetaData.Builder cfmBuilder = CFMetaData.Builder.createView(keyspace(), columnFamily());
    add(cfm, targetPartitionKeys, cfmBuilder::addPartitionKey);
    add(cfm, targetClusteringColumns, cfmBuilder::addClusteringColumn);
    add(cfm, includedColumns, cfmBuilder::addRegularColumn);
    cfmBuilder.withId(properties.properties.getId());
    TableParams params = properties.properties.asNewTableParams();
    CFMetaData viewCfm = cfmBuilder.build().params(params);
    ViewDefinition definition = new ViewDefinition(keyspace(),
                                                   columnFamily(),
                                                   Schema.instance.getId(keyspace(), baseName.getColumnFamily()),
                                                   baseName.getColumnFamily(),
                                                   included.isEmpty(),
                                                   rawSelect,
                                                   whereClauseText,
                                                   viewCfm);

    try
    {
        MigrationManager.announceNewView(definition, isLocalOnly);
        return new Event.SchemaChange(Event.SchemaChange.Change.CREATED, Event.SchemaChange.Target.TABLE, keyspace(), columnFamily());
    }
    catch (AlreadyExistsException e)
    {
        if (ifNotExists)
            return null;
        throw e;
    }
#endif
}

shared_ptr<transport::event::schema_change> create_view_statement::change_event() {
    // FIXME: this is probably wrong, I just copied it from create_table_statement
    return make_shared<transport::event::schema_change>(transport::event::schema_change::change_type::CREATED, transport::event::schema_change::target_type::TABLE, keyspace(), column_family());
}

shared_ptr<cql3::statements::prepared_statement>
create_view_statement::prepare(database& db) {
    return make_shared<prepared_statement>(make_shared<create_view_statement>(*this));
}

}

}
