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
 * Copyright 2015 Cloudius Systems
 *
 * Modified by Cloudius Systems
 */

#pragma once

#include "cql3/statements/schema_altering_statement.hh"
#include "cql3/statements/cf_prop_defs.hh"
#include "cql3/statements/cf_statement.hh"
#include "cql3/cql3_type.hh"

#include "service/migration_manager.hh"
#include "schema.hh"

#include "core/shared_ptr.hh"

#include <unordered_map>
#include <utility>
#include <vector>
#include <set>

namespace cql3 {

namespace statements {

/** A <code>CREATE TABLE</code> parsed from a CQL query statement. */
class create_table_statement : public schema_altering_statement {
#if 0
    public CellNameType comparator;
#endif
private:
#if 0
    private AbstractType<?> defaultValidator;
    private AbstractType<?> keyValidator;

    private final List<ByteBuffer> keyAliases = new ArrayList<ByteBuffer>();
    private final List<ByteBuffer> columnAliases = new ArrayList<ByteBuffer>();
    private ByteBuffer valueAlias;

    private boolean isDense;

    private final Map<ColumnIdentifier, AbstractType> columns = new HashMap<ColumnIdentifier, AbstractType>();
#endif
    const std::set<::shared_ptr<column_identifier>> _static_columns;
    const ::shared_ptr<cf_prop_defs> _properties;
    const bool _if_not_exists;
public:
    create_table_statement(::shared_ptr<cf_name> name,
                           ::shared_ptr<cf_prop_defs> properties,
                           bool if_not_exists,
                           std::set<::shared_ptr<column_identifier>> static_columns);

    virtual void check_access(const service::client_state& state) override;

    virtual void validate(service::storage_proxy&, const service::client_state& state) override;

    virtual future<bool> announce_migration(service::storage_proxy& proxy, bool is_local_only) override;

    virtual shared_ptr<transport::event::schema_change> change_event() override;

    schema_ptr get_cf_meta_data();

    void apply_properties_to(schema* s);

    class raw_statement;
};

class create_table_statement::raw_statement : public cf_statement {
private:
    std::unordered_map<::shared_ptr<column_identifier>, ::shared_ptr<cql3_type::raw>> _definitions;
public:
    const ::shared_ptr<cf_prop_defs> properties = ::make_shared<cf_prop_defs>();
private:
    std::vector<std::vector<::shared_ptr<column_identifier>>> _key_aliases;
    std::vector<::shared_ptr<column_identifier>> _column_aliases;
    std::vector<std::pair<::shared_ptr<column_identifier>, bool>> defined_ordering; // Insertion ordering is important
    std::set<::shared_ptr<column_identifier>> _static_columns;

    bool _use_compact_storage = false;
    std::multiset<::shared_ptr<column_identifier>> _defined_names;
    bool _if_not_exists;
public:
    raw_statement(::shared_ptr<cf_name> name, bool if_not_exists)
        : cf_statement{std::move(name)}
        , _if_not_exists{if_not_exists}
    { }

    virtual ::shared_ptr<prepared> prepare(database& db) override {
#if 0
        // Column family name
        if (!columnFamily().matches("\\w+"))
            throw new InvalidRequestException(String.format("\"%s\" is not a valid table name (must be alphanumeric character only: [0-9A-Za-z]+)", columnFamily()));
        if (columnFamily().length() > Schema.NAME_LENGTH)
            throw new InvalidRequestException(String.format("Table names shouldn't be more than %s characters long (got \"%s\")", Schema.NAME_LENGTH, columnFamily()));

        for (Multiset.Entry<ColumnIdentifier> entry : definedNames.entrySet())
            if (entry.getCount() > 1)
                throw new InvalidRequestException(String.format("Multiple definition of identifier %s", entry.getElement()));
#endif

        properties->validate();

        auto stmt = ::make_shared<create_table_statement>(_cf_name, properties, _if_not_exists, _static_columns);

#if 0
        Map<ByteBuffer, CollectionType> definedMultiCellCollections = null;
        for (Map.Entry<ColumnIdentifier, CQL3Type.Raw> entry : definitions.entrySet())
        {
            ColumnIdentifier id = entry.getKey();
            CQL3Type pt = entry.getValue().prepare(keyspace());
            if (pt.isCollection() && ((CollectionType) pt.getType()).isMultiCell())
            {
                if (definedMultiCellCollections == null)
                    definedMultiCellCollections = new HashMap<>();
                definedMultiCellCollections.put(id.bytes, (CollectionType) pt.getType());
            }
            stmt.columns.put(id, pt.getType()); // we'll remove what is not a column below
        }

        if (keyAliases.isEmpty())
            throw new InvalidRequestException("No PRIMARY KEY specifed (exactly one required)");
        else if (keyAliases.size() > 1)
            throw new InvalidRequestException("Multiple PRIMARY KEYs specifed (exactly one required)");

        List<ColumnIdentifier> kAliases = keyAliases.get(0);

        List<AbstractType<?>> keyTypes = new ArrayList<AbstractType<?>>(kAliases.size());
        for (ColumnIdentifier alias : kAliases)
        {
            stmt.keyAliases.add(alias.bytes);
            AbstractType<?> t = getTypeAndRemove(stmt.columns, alias);
            if (t instanceof CounterColumnType)
                throw new InvalidRequestException(String.format("counter type is not supported for PRIMARY KEY part %s", alias));
            if (staticColumns.contains(alias))
                throw new InvalidRequestException(String.format("Static column %s cannot be part of the PRIMARY KEY", alias));
            keyTypes.add(t);
        }
        stmt.keyValidator = keyTypes.size() == 1 ? keyTypes.get(0) : CompositeType.getInstance(keyTypes);

        // Dense means that no part of the comparator stores a CQL column name. This means
        // COMPACT STORAGE with at least one columnAliases (otherwise it's a thrift "static" CF).
        stmt.isDense = useCompactStorage && !columnAliases.isEmpty();

        // Handle column aliases
        if (columnAliases.isEmpty())
        {
            if (useCompactStorage)
            {
                // There should remain some column definition since it is a non-composite "static" CF
                if (stmt.columns.isEmpty())
                    throw new InvalidRequestException("No definition found that is not part of the PRIMARY KEY");

                if (definedMultiCellCollections != null)
                    throw new InvalidRequestException("Non-frozen collection types are not supported with COMPACT STORAGE");

                stmt.comparator = new SimpleSparseCellNameType(UTF8Type.instance);
            }
            else
            {
                stmt.comparator = definedMultiCellCollections == null
                                ? new CompoundSparseCellNameType(Collections.<AbstractType<?>>emptyList())
                                : new CompoundSparseCellNameType.WithCollection(Collections.<AbstractType<?>>emptyList(), ColumnToCollectionType.getInstance(definedMultiCellCollections));
            }
        }
        else
        {
            // If we use compact storage and have only one alias, it is a
            // standard "dynamic" CF, otherwise it's a composite
            if (useCompactStorage && columnAliases.size() == 1)
            {
                if (definedMultiCellCollections != null)
                    throw new InvalidRequestException("Collection types are not supported with COMPACT STORAGE");

                ColumnIdentifier alias = columnAliases.get(0);
                if (staticColumns.contains(alias))
                    throw new InvalidRequestException(String.format("Static column %s cannot be part of the PRIMARY KEY", alias));

                stmt.columnAliases.add(alias.bytes);
                AbstractType<?> at = getTypeAndRemove(stmt.columns, alias);
                if (at instanceof CounterColumnType)
                    throw new InvalidRequestException(String.format("counter type is not supported for PRIMARY KEY part %s", stmt.columnAliases.get(0)));
                stmt.comparator = new SimpleDenseCellNameType(at);
            }
            else
            {
                List<AbstractType<?>> types = new ArrayList<AbstractType<?>>(columnAliases.size() + 1);
                for (ColumnIdentifier t : columnAliases)
                {
                    stmt.columnAliases.add(t.bytes);

                    AbstractType<?> type = getTypeAndRemove(stmt.columns, t);
                    if (type instanceof CounterColumnType)
                        throw new InvalidRequestException(String.format("counter type is not supported for PRIMARY KEY part %s", t));
                    if (staticColumns.contains(t))
                        throw new InvalidRequestException(String.format("Static column %s cannot be part of the PRIMARY KEY", t));
                    types.add(type);
                }

                if (useCompactStorage)
                {
                    if (definedMultiCellCollections != null)
                        throw new InvalidRequestException("Collection types are not supported with COMPACT STORAGE");

                    stmt.comparator = new CompoundDenseCellNameType(types);
                }
                else
                {
                    stmt.comparator = definedMultiCellCollections == null
                                    ? new CompoundSparseCellNameType(types)
                                    : new CompoundSparseCellNameType.WithCollection(types, ColumnToCollectionType.getInstance(definedMultiCellCollections));
                }
            }
        }

        if (!staticColumns.isEmpty())
        {
            // Only CQL3 tables can have static columns
            if (useCompactStorage)
                throw new InvalidRequestException("Static columns are not supported in COMPACT STORAGE tables");
            // Static columns only make sense if we have at least one clustering column. Otherwise everything is static anyway
            if (columnAliases.isEmpty())
                throw new InvalidRequestException("Static columns are only useful (and thus allowed) if the table has at least one clustering column");
        }

        if (useCompactStorage && !stmt.columnAliases.isEmpty())
        {
            if (stmt.columns.isEmpty())
            {
                // The only value we'll insert will be the empty one, so the default validator don't matter
                stmt.defaultValidator = BytesType.instance;
                // We need to distinguish between
                //   * I'm upgrading from thrift so the valueAlias is null
                //   * I've defined my table with only a PK (and the column value will be empty)
                // So, we use an empty valueAlias (rather than null) for the second case
                stmt.valueAlias = ByteBufferUtil.EMPTY_BYTE_BUFFER;
            }
            else
            {
                if (stmt.columns.size() > 1)
                    throw new InvalidRequestException(String.format("COMPACT STORAGE with composite PRIMARY KEY allows no more than one column not part of the PRIMARY KEY (got: %s)", StringUtils.join(stmt.columns.keySet(), ", ")));

                Map.Entry<ColumnIdentifier, AbstractType> lastEntry = stmt.columns.entrySet().iterator().next();
                stmt.defaultValidator = lastEntry.getValue();
                stmt.valueAlias = lastEntry.getKey().bytes;
                stmt.columns.remove(lastEntry.getKey());
            }
        }
        else
        {
            // For compact, we are in the "static" case, so we need at least one column defined. For non-compact however, having
            // just the PK is fine since we have CQL3 row marker.
            if (useCompactStorage && stmt.columns.isEmpty())
                throw new InvalidRequestException("COMPACT STORAGE with non-composite PRIMARY KEY require one column not part of the PRIMARY KEY, none given");

            // There is no way to insert/access a column that is not defined for non-compact storage, so
            // the actual validator don't matter much (except that we want to recognize counter CF as limitation apply to them).
            stmt.defaultValidator = !stmt.columns.isEmpty() && (stmt.columns.values().iterator().next() instanceof CounterColumnType)
                ? CounterColumnType.instance
                : BytesType.instance;
        }


        // If we give a clustering order, we must explicitly do so for all aliases and in the order of the PK
        if (!definedOrdering.isEmpty())
        {
            if (definedOrdering.size() > columnAliases.size())
                throw new InvalidRequestException("Only clustering key columns can be defined in CLUSTERING ORDER directive");

            int i = 0;
            for (ColumnIdentifier id : definedOrdering.keySet())
            {
                ColumnIdentifier c = columnAliases.get(i);
                if (!id.equals(c))
                {
                    if (definedOrdering.containsKey(c))
                        throw new InvalidRequestException(String.format("The order of columns in the CLUSTERING ORDER directive must be the one of the clustering key (%s must appear before %s)", c, id));
                    else
                        throw new InvalidRequestException(String.format("Missing CLUSTERING ORDER for column %s", c));
                }
                ++i;
            }
        }
#endif

        return ::make_shared<parsed_statement::prepared>(stmt);
    }

#if 0
        private AbstractType<?> getTypeAndRemove(Map<ColumnIdentifier, AbstractType> columns, ColumnIdentifier t) throws InvalidRequestException
        {
            AbstractType type = columns.get(t);
            if (type == null)
                throw new InvalidRequestException(String.format("Unknown definition %s referenced in PRIMARY KEY", t));
            if (type.isCollection() && type.isMultiCell())
                throw new InvalidRequestException(String.format("Invalid collection type for PRIMARY KEY component %s", t));

            columns.remove(t);
            Boolean isReversed = definedOrdering.get(t);
            return isReversed != null && isReversed ? ReversedType.getInstance(type) : type;
        }
#endif

    void add_definition(::shared_ptr<column_identifier> def, ::shared_ptr<cql3_type::raw> type, bool is_static) {
        _defined_names.emplace(def);
        _definitions.emplace(def, type);
        if (is_static) {
            _static_columns.emplace(def);
        }
    }

    void add_key_aliases(const std::vector<::shared_ptr<column_identifier>> aliases) {
        _key_aliases.emplace_back(aliases);
    }

    void add_column_alias(::shared_ptr<column_identifier> alias) {
        _column_aliases.emplace_back(alias);
    }

    void set_ordering(::shared_ptr<column_identifier> alias, bool reversed) {
        defined_ordering.emplace_back(alias, reversed);
    }

    void set_compact_storage() {
        _use_compact_storage = true;
    }
};

}

}
