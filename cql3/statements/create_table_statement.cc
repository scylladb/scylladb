/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */


#include "utils/assert.hh"
#include <inttypes.h>
#include <boost/regex.hpp>

#include <boost/range/adaptor/map.hpp>
#include <boost/range/algorithm/adjacent_find.hpp>
#include <seastar/core/coroutine.hh>

#include "cql3/statements/create_table_statement.hh"
#include "cql3/statements/prepared_statement.hh"
#include "cql3/query_processor.hh"

#include "auth/resource.hh"
#include "auth/service.hh"
#include "schema/schema_builder.hh"
#include "data_dictionary/data_dictionary.hh"
#include "service/raft/raft_group0_client.hh"
#include "types/user.hh"
#include "gms/feature_service.hh"
#include "service/migration_manager.hh"
#include "service/storage_proxy.hh"
#include "db/config.hh"
#include "compaction/time_window_compaction_strategy.hh"

namespace cql3 {

namespace statements {

static logging::logger mylogger("create_table");

create_table_statement::create_table_statement(cf_name name,
                                               ::shared_ptr<cf_prop_defs> properties,
                                               bool if_not_exists,
                                               column_set_type static_columns,
                                               const std::optional<table_id>& id)
    : schema_altering_statement{name}
    , _use_compact_storage(false)
    , _static_columns{static_columns}
    , _properties{properties}
    , _if_not_exists{if_not_exists}
    , _id(id)
{
}

future<> create_table_statement::check_access(query_processor& qp, const service::client_state& state) const {
    return state.has_keyspace_access(keyspace(), auth::permission::CREATE);
}

// Column definitions
std::vector<column_definition> create_table_statement::get_columns() const
{
    std::vector<column_definition> column_defs;
    column_defs.reserve(_columns.size());
    for (auto&& col : _columns) {
        column_kind kind = column_kind::regular_column;
        if (_static_columns.contains(col.first)) {
            kind = column_kind::static_column;
        }
        column_defs.emplace_back(col.first->name(), col.second, kind);
    }
    return column_defs;
}

future<std::tuple<::shared_ptr<cql_transport::event::schema_change>, std::vector<mutation>, cql3::cql_warnings_vec>>
create_table_statement::prepare_schema_mutations(query_processor& qp, const query_options&, api::timestamp_type ts) const {
    std::vector<mutation> m;

    try {
        m = co_await service::prepare_new_column_family_announcement(qp.proxy(), get_cf_meta_data(qp.db()), ts);
    } catch (const exceptions::already_exists_exception& e) {
        if (!_if_not_exists) {
            co_return coroutine::exception(std::current_exception());
        }
    }

    // If an IF NOT EXISTS clause was used and resource was already created
    // we shouldn't emit created event. However it interacts badly with
    // concurrent clients creating resources. The client seeing no create event
    // assumes resource already previously existed and proceeds with its logic
    // which may depend on that resource. But it may send requests to nodes which
    // are not yet aware of new schema or client's metadata may be outdated.
    // To force synchronization always emit the event (see
    // github.com/scylladb/scylladb/issues/16909).
    co_return std::make_tuple(created_event(), std::move(m), std::vector<sstring>());
}

/**
 * Returns a CFMetaData instance based on the parameters parsed from this
 * <code>CREATE</code> statement, or defaults where applicable.
 *
 * @return a CFMetaData instance corresponding to the values parsed from this statement
 * @throws InvalidRequestException on failure to validate parsed parameters
 */
schema_ptr create_table_statement::get_cf_meta_data(const data_dictionary::database db) const {
    schema_builder builder{keyspace(), column_family(), _id};
    apply_properties_to(builder, db);
    return builder.build(_use_compact_storage ? schema_builder::compact_storage::yes : schema_builder::compact_storage::no);
}

void create_table_statement::apply_properties_to(schema_builder& builder, const data_dictionary::database db) const {
    auto&& columns = get_columns();
    for (auto&& column : columns) {
        builder.with_column_ordered(column);
    }
#if 0
    cfmd.defaultValidator(defaultValidator)
        .addAllColumnDefinitions(getColumns(cfmd))
#endif
    add_column_metadata_from_aliases(builder, _key_aliases, _partition_key_types, column_kind::partition_key);
    add_column_metadata_from_aliases(builder, _column_aliases, _clustering_key_types, column_kind::clustering_key);
#if 0
    if (valueAlias != null)
        addColumnMetadataFromAliases(cfmd, Collections.singletonList(valueAlias), defaultValidator, ColumnDefinition.Kind.COMPACT_VALUE);
#endif

    _properties->apply_to_builder(builder, _properties->make_schema_extensions(db.extensions()), db, keyspace());
}

void create_table_statement::add_column_metadata_from_aliases(schema_builder& builder, std::vector<bytes> aliases, const std::vector<data_type>& types, column_kind kind) const
{
    SCYLLA_ASSERT(aliases.size() == types.size());
    for (size_t i = 0; i < aliases.size(); i++) {
        if (!aliases[i].empty()) {
            builder.with_column(aliases[i], types[i], kind);
        }
    }
}

std::unique_ptr<prepared_statement>
create_table_statement::prepare(data_dictionary::database db, cql_stats& stats) {
    // Cannot happen; create_table_statement is never instantiated as a raw statement
    // (instead we instantiate create_table_statement::raw_statement)
    abort();
}

future<> create_table_statement::grant_permissions_to_creator(const service::client_state& cs, service::group0_batch& mc) const {
    auto resource = auth::make_data_resource(keyspace(), column_family());
    try {
        co_await auth::grant_applicable_permissions(
                *cs.get_auth_service(),
                *cs.user(),
                resource,
                mc);
    } catch (const auth::unsupported_authorization_operation&) {
        // Nothing.
    }
}

create_table_statement::raw_statement::raw_statement(cf_name name, bool if_not_exists)
    : cf_statement{std::move(name)}
    , _if_not_exists{if_not_exists}
{ }

std::unique_ptr<prepared_statement> create_table_statement::raw_statement::prepare(data_dictionary::database db, cql_stats& stats) {
    // Column family name
    const sstring& cf_name = _cf_name->get_column_family();
    boost::regex name_regex("\\w+");
    if (!boost::regex_match(std::string(cf_name), name_regex)) {
        throw exceptions::invalid_request_exception(format("\"{}\" is not a valid table name (must be alphanumeric character only: [0-9A-Za-z]+)", cf_name.c_str()));
    }
    if (cf_name.size() > size_t(schema::NAME_LENGTH)) {
        throw exceptions::invalid_request_exception(format("Table names shouldn't be more than {:d} characters long (got \"{}\")", schema::NAME_LENGTH, cf_name.c_str()));
    }

    // Check for duplicate column names
    auto i = boost::range::adjacent_find(_defined_names, [] (auto&& e1, auto&& e2) {
        return e1->text() == e2->text();
    });
    if (i != _defined_names.end()) {
        throw exceptions::invalid_request_exception(format("Multiple definition of identifier {}", (*i)->text()));
    }

    _properties.validate(db, keyspace(), _properties.properties()->make_schema_extensions(db.extensions()));
    if (_properties.properties()->get_synchronous_updates_flag()) {
        throw exceptions::invalid_request_exception(format("The synchronous_updates option is only applicable to materialized views, not to base tables"));
    }
    std::optional<sstring> warning = check_restricted_table_properties(db, std::nullopt, keyspace(), column_family(), *_properties.properties());
    if (warning) {
        mylogger.warn("{}", *warning);
    }
    const bool has_default_ttl = _properties.properties()->get_default_time_to_live() > 0;

    auto stmt = ::make_shared<create_table_statement>(*_cf_name, _properties.properties(), _if_not_exists, _static_columns, _properties.properties()->get_id());

    std::optional<std::map<bytes, data_type>> defined_multi_cell_columns;
    for (auto&& entry : _definitions) {
        ::shared_ptr<column_identifier> id = entry.first;
        cql3_type pt = entry.second->prepare(db, keyspace());

        if (has_default_ttl && pt.is_counter()) {
            throw exceptions::invalid_request_exception("Cannot set default_time_to_live on a table with counters");
        }

        if (db.find_keyspace(keyspace()).get_replication_strategy().uses_tablets() && pt.is_counter()) {
            throw exceptions::invalid_request_exception(format("Cannot use the 'counter' type for table {}.{}: Counters are not yet supported with tablets", keyspace(), cf_name));
        }

        if (pt.get_type()->is_multi_cell()) {
            if (pt.get_type()->is_user_type()) {
                // check for multi-cell types (non-frozen UDTs or collections) inside a non-frozen UDT
                auto type = static_cast<const user_type_impl*>(pt.get_type().get());
                for (auto&& inner: type->all_types()) {
                    if (inner->is_multi_cell()) {
                        // a nested non-frozen UDT should have already been rejected when defining the type
                        SCYLLA_ASSERT(inner->is_collection());
                        throw exceptions::invalid_request_exception("Non-frozen UDTs with nested non-frozen collections are not supported");
                    }
                }
            }

            if (!defined_multi_cell_columns) {
                defined_multi_cell_columns = std::map<bytes, data_type>{};
            }
            defined_multi_cell_columns->emplace(id->name(), pt.get_type());
        }
        stmt->_columns.emplace(id, pt.get_type()); // we'll remove what is not a column below
    }
    if (_key_aliases.empty()) {
        throw exceptions::invalid_request_exception("No PRIMARY KEY specified (exactly one required)");
    } else if (_key_aliases.size() > 1) {
        throw exceptions::invalid_request_exception("Multiple PRIMARY KEYs specified (exactly one required)");
    }

    stmt->_use_compact_storage = _properties.use_compact_storage();

    auto& key_aliases = _key_aliases[0];
    std::vector<data_type> key_types;
    for (auto&& alias : key_aliases) {
        stmt->_key_aliases.emplace_back(alias->name());
        auto t = get_type_and_remove(stmt->_columns, alias);
        if (t->is_counter()) {
            throw exceptions::invalid_request_exception(format("counter type is not supported for PRIMARY KEY part {}", alias->text()));
        }
        if (t->references_duration()) {
            throw exceptions::invalid_request_exception(format("duration type is not supported for PRIMARY KEY part {}", alias->text()));
        }
        if (_static_columns.contains(alias)) {
            throw exceptions::invalid_request_exception(format("Static column {} cannot be part of the PRIMARY KEY", alias->text()));
        }
        key_types.emplace_back(t);
    }
    stmt->_partition_key_types = key_types;

    // Handle column aliases
    if (_column_aliases.empty()) {
        if (_properties.use_compact_storage()) {
            // There should remain some column definition since it is a non-composite "static" CF
            if (stmt->_columns.empty()) {
                throw exceptions::invalid_request_exception("No definition found that is not part of the PRIMARY KEY");
            }
            if (defined_multi_cell_columns) {
                throw exceptions::invalid_request_exception("Non-frozen collections and UDTs are not supported with COMPACT STORAGE");
            }
        }
        stmt->_clustering_key_types = std::vector<data_type>{};
    } else {
        // If we use compact storage and have only one alias, it is a
        // standard "dynamic" CF, otherwise it's a composite
        if (_properties.use_compact_storage() && _column_aliases.size() == 1) {
            if (defined_multi_cell_columns) {
                throw exceptions::invalid_request_exception("Non-frozen collections and UDTs are not supported with COMPACT STORAGE");
            }
            auto alias = _column_aliases[0];
            if (_static_columns.contains(alias)) {
                throw exceptions::invalid_request_exception(format("Static column {} cannot be part of the PRIMARY KEY", alias->text()));
            }
            stmt->_column_aliases.emplace_back(alias->name());
            auto at = get_type_and_remove(stmt->_columns, alias);
            if (at->is_counter()) {
                throw exceptions::invalid_request_exception(format("counter type is not supported for PRIMARY KEY part {}", stmt->_column_aliases[0]));
            }
            if (at->references_duration()) {
                throw exceptions::invalid_request_exception(format("duration type is not supported for PRIMARY KEY part {}", stmt->_column_aliases[0]));
            }
            stmt->_clustering_key_types.emplace_back(at);
        } else {
            std::vector<data_type> types;
            for (auto&& t : _column_aliases) {
                stmt->_column_aliases.emplace_back(t->name());
                auto type = get_type_and_remove(stmt->_columns, t);
                if (type->is_counter()) {
                    throw exceptions::invalid_request_exception(format("counter type is not supported for PRIMARY KEY part {}", t->text()));
                }
                if (type->references_duration()) {
                    throw exceptions::invalid_request_exception(format("duration type is not supported for PRIMARY KEY part {}", t->text()));
                }
                if (_static_columns.contains(t)) {
                    throw exceptions::invalid_request_exception(format("Static column {} cannot be part of the PRIMARY KEY", t->text()));
                }
                types.emplace_back(type);
            }

            if (_properties.use_compact_storage()) {
                if (defined_multi_cell_columns) {
                    throw exceptions::invalid_request_exception("Non-frozen collections and UDTs are not supported with COMPACT STORAGE");
                }
                stmt->_clustering_key_types = types;
            } else {
                stmt->_clustering_key_types = types;
            }
        }
    }

    if (!_static_columns.empty()) {
        // Only CQL3 tables can have static columns
        if (_properties.use_compact_storage()) {
            throw exceptions::invalid_request_exception("Static columns are not supported in COMPACT STORAGE tables");
        }
        // Static columns only make sense if we have at least one clustering column. Otherwise everything is static anyway
        if (_column_aliases.empty()) {
            throw exceptions::invalid_request_exception("Static columns are only useful (and thus allowed) if the table has at least one clustering column");
        }
    }

    if (_properties.use_compact_storage() && !stmt->_column_aliases.empty()) {
        if (stmt->_columns.empty()) {
#if 0
            // The only value we'll insert will be the empty one, so the default validator don't matter
            stmt.defaultValidator = BytesType.instance;
            // We need to distinguish between
            //   * I'm upgrading from thrift so the valueAlias is null
            //   * I've defined my table with only a PK (and the column value will be empty)
            // So, we use an empty valueAlias (rather than null) for the second case
            stmt.valueAlias = ByteBufferUtil.EMPTY_BYTE_BUFFER;
#endif
        } else {
            if (stmt->_columns.size() > 1) {
                throw exceptions::invalid_request_exception(format("COMPACT STORAGE with composite PRIMARY KEY allows no more than one column not part of the PRIMARY KEY (got: {})",
                    fmt::join(stmt->_columns | boost::adaptors::map_keys, ", ")));
            }
#if 0
            Map.Entry<ColumnIdentifier, AbstractType> lastEntry = stmt.columns.entrySet().iterator().next();
            stmt.defaultValidator = lastEntry.getValue();
            stmt.valueAlias = lastEntry.getKey().bytes;
            stmt.columns.remove(lastEntry.getKey());
#endif
        }
    } else {
        // For compact, we are in the "static" case, so we need at least one column defined. For non-compact however, having
        // just the PK is fine since we have CQL3 row marker.
        if (_properties.use_compact_storage() && stmt->_columns.empty()) {
            throw exceptions::invalid_request_exception("COMPACT STORAGE with non-composite PRIMARY KEY require one column not part of the PRIMARY KEY, none given");
        }
#if 0
        // There is no way to insert/access a column that is not defined for non-compact storage, so
        // the actual validator don't matter much (except that we want to recognize counter CF as limitation apply to them).
        stmt.defaultValidator = !stmt.columns.isEmpty() && (stmt.columns.values().iterator().next() instanceof CounterColumnType)
            ? CounterColumnType.instance
            : BytesType.instance;
#endif
    }

    // If we give a clustering order, we must explicitly do so for all aliases and in the order of the PK
    if (!_properties.defined_ordering().empty()) {
        if (_properties.defined_ordering().size() > _column_aliases.size()) {
            throw exceptions::invalid_request_exception("Only clustering key columns can be defined in CLUSTERING ORDER directive");
        }

        int i = 0;
        for (auto& pair: _properties.defined_ordering()){
            auto& id = pair.first;
            auto& c = _column_aliases.at(i);

            if (!(*id == *c)) {
                if (_properties.find_ordering_info(*c)) {
                    throw exceptions::invalid_request_exception(format("The order of columns in the CLUSTERING ORDER directive must be the one of the clustering key ({} must appear before {})", c, id));
                } else {
                    throw exceptions::invalid_request_exception(format("Missing CLUSTERING ORDER for column {}", c));
                }
            }
            ++i;
        }
    }

    return std::make_unique<prepared_statement>(stmt);
}

data_type create_table_statement::raw_statement::get_type_and_remove(column_map_type& columns, ::shared_ptr<column_identifier> t)
{
    auto it = columns.find(t);
    if (it == columns.end()) {
        throw exceptions::invalid_request_exception(format("Unknown definition {} referenced in PRIMARY KEY", t->text()));
    }
    auto type = it->second;
    if (type->is_multi_cell()) {
        if (type->is_collection()) {
            throw exceptions::invalid_request_exception(format("Invalid non-frozen collection type for PRIMARY KEY component {}", t->text()));
        } else {
            throw exceptions::invalid_request_exception(format("Invalid non-frozen user-defined type for PRIMARY KEY component {}", t->text()));
        }
    }
    columns.erase(t);

    return _properties.get_reversable_type(*t, type);
}

void create_table_statement::raw_statement::add_definition(::shared_ptr<column_identifier> def, ::shared_ptr<cql3_type::raw> type, bool is_static) {
    _defined_names.emplace(def);
    _definitions.emplace(def, type);
    if (is_static) {
        _static_columns.emplace(def);
    }
}

void create_table_statement::raw_statement::add_key_aliases(const std::vector<::shared_ptr<column_identifier>> aliases) {
    _key_aliases.emplace_back(aliases);
}

void create_table_statement::raw_statement::add_column_alias(::shared_ptr<column_identifier> alias) {
    _column_aliases.emplace_back(alias);
}

// Check for choices of table properties (e.g., the choice of compaction
// strategy) which are restricted configuration options.
// This check can throw a configuration_exception immediately if an option
// is forbidden by the configuration, or return a warning string if the
// relevant restriction was set to "warn".
// This function is only supposed to check for options which are usually
// legal but restricted by the configuration. Checks for other of errors
// in the table's options are done elsewhere.
std::optional<sstring> check_restricted_table_properties(
    data_dictionary::database db,
    std::optional<schema_ptr> schema,
    const sstring& keyspace, const sstring& table,
    const cf_prop_defs& cfprops)
{
    // Note: In the current implementation, CREATE TABLE calls this function
    // after cfprops.validate() was called, but ALTER TABLE calls this
    // function before cfprops.validate() (there, validate() is only called
    // in prepare_schema_mutations(), in the middle of execute).
    auto strategy = cfprops.get_compaction_strategy_class();
    sstables::compaction_strategy_type current_strategy = sstables::compaction_strategy_type::null;
    gc_clock::duration current_ttl = gc_clock::duration::zero();
    // cfprops doesn't return any of the table attributes unless the attribute
    // has been specified in the CQL statement. If a schema is defined, then
    // this was an ALTER TABLE statement.
    if (schema) {
        current_strategy = (*schema)->compaction_strategy();
        current_ttl = (*schema)->default_time_to_live();
    }

    if (strategy) {
        sstables::compaction_strategy_impl::validate_options_for_strategy_type(cfprops.get_compaction_type_options(), strategy.value());
    }

    // Evaluate whether the strategy to evaluate was explicitly passed
    auto cs = (strategy) ? strategy : current_strategy;

    if (cs == sstables::compaction_strategy_type::time_window) {
        std::map<sstring, sstring> options = (strategy) ? cfprops.get_compaction_type_options() : (*schema)->compaction_strategy_options();
        sstables::time_window_compaction_strategy_options twcs_options(options);
        long ttl = (cfprops.has_property(cf_prop_defs::KW_DEFAULT_TIME_TO_LIVE)) ? cfprops.get_default_time_to_live() : current_ttl.count();
        auto max_windows = db.get_config().twcs_max_window_count();

        // It may happen that an user tries to update an unrelated table property. Allow the request through.
        if (!cfprops.has_property(cf_prop_defs::KW_DEFAULT_TIME_TO_LIVE) && !strategy) {
            return std::nullopt;
        }

        if (ttl > 0) {
            // Ideally we should not need the window_size check below. However, given #2336 it may happen that some incorrectly
            // table created with a window_size=0 may exist, which would cause a division by zero (eg: in an ALTER statement).
            // Given that, an invalid window size is treated as 1 minute, which is the smaller "supported" window size for TWCS.
            auto window_size = twcs_options.get_sstable_window_size() > std::chrono::seconds::zero() ? twcs_options.get_sstable_window_size() : std::chrono::seconds(60);
            auto window_count = std::chrono::seconds(ttl) / window_size;
            if (max_windows > 0 && window_count > max_windows) {
                throw exceptions::configuration_exception(fmt::format("The setting of default_time_to_live={} and compaction window={}(s) "
                                                   "can lead to {} windows, which is larger than the allowed number of windows specified "
                                                   "by the twcs_max_window_count ({}) parameter. Note that default_time_to_live=0 is also "
                                                   "highly discouraged.", ttl, twcs_options.get_sstable_window_size().count(), window_count, max_windows));
            }
        } else {
              switch (db.get_config().restrict_twcs_without_default_ttl()) {
              case db::tri_mode_restriction_t::mode::TRUE:
                  throw exceptions::configuration_exception(
                      "TimeWindowCompactionStrategy tables without a strict default_time_to_live setting "
                      "are forbidden. You may override this restriction by setting restrict_twcs_without_default_ttl "
                      "configuration option to false.");
              case db::tri_mode_restriction_t::mode::WARN:
                  return format("TimeWindowCompactionStrategy tables without a default_time_to_live "
                      "may potentially introduce too many windows. Ensure that insert statements specify a "
                      "TTL (via USING TTL), when inserting data to this table. The restrict_twcs_without_default_ttl "
                      "configuration option can be changed to silence this warning or make it into an error");
              case db::tri_mode_restriction_t::mode::FALSE:
                  break;
              }
        }
   }
    return std::nullopt;
}

::shared_ptr<schema_altering_statement::event_t> create_table_statement::created_event() const {
    return make_shared<event_t>(
            event_t::change_type::CREATED,
            event_t::target_type::TABLE,
            keyspace(),
            column_family());
}

}

}
