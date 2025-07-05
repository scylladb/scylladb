/*
 * Copyright 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.0 and Apache-2.0)
 */

#include "utils/assert.hh"
#include <seastar/core/coroutine.hh>
#include "cql3/query_options.hh"
#include "cql3/statements/alter_table_statement.hh"
#include "cql3/statements/alter_type_statement.hh"
#include "exceptions/exceptions.hh"
#include "index/secondary_index_manager.hh"
#include "prepared_statement.hh"
#include "service/migration_manager.hh"
#include "service/storage_proxy.hh"
#include "timestamp.hh"
#include "validation.hh"
#include "db/extensions.hh"
#include "cql3/util.hh"
#include "view_info.hh"
#include "data_dictionary/data_dictionary.hh"
#include "db/view/view.hh"
#include "cql3/query_processor.hh"
#include "cdc/cdc_extension.hh"

namespace cql3 {

namespace statements {

static logging::logger mylogger("alter_table");

alter_table_statement::alter_table_statement(uint32_t bound_terms,
                                             cf_name name,
                                             type t,
                                             std::vector<column_change> column_changes,
                                             std::optional<cf_prop_defs> properties,
                                             renames_type renames,
                                             std::unique_ptr<attributes> attrs)
    : schema_altering_statement(std::move(name))
    , _bound_terms(bound_terms)
    , _type(t)
    , _column_changes(std::move(column_changes))
    , _properties(std::move(properties))
    , _renames(std::move(renames))
    , _attrs(std::move(attrs))
{
}

uint32_t alter_table_statement::get_bound_terms() const {
    return _bound_terms;
}

future<> alter_table_statement::check_access(query_processor& qp, const service::client_state& state) const {
    using cdt = auth::command_desc::type;
    auto type = cdt::OTHER;
    if (_type == type::opts) {
        // can modify only KW_MEMTABLE_FLUSH_PERIOD property for system tables (see issue #21223)
        if (is_system_keyspace(keyspace()) && _properties->count() == 1 && _properties->has_property(cf_prop_defs::KW_MEMTABLE_FLUSH_PERIOD)) {
            type = cdt::ALTER_SYSTEM_WITH_ALLOWED_OPTS;
        } else {
            type = cdt::ALTER_WITH_OPTS;
        }
    }
    return state.has_column_family_access(keyspace(), column_family(), auth::permission::ALTER, type);
}

static data_type validate_alter(const schema& schema, const column_definition& def, const cql3_type& validator)
{
    auto type = def.type->is_reversed() && !validator.get_type()->is_reversed()
              ? reversed_type_impl::get_instance(validator.get_type())
              : validator.get_type();
    switch (def.kind) {
    case column_kind::partition_key:
        if (type->is_counter()) {
            throw exceptions::invalid_request_exception(
                    format("counter type is not supported for PRIMARY KEY part {}", def.name_as_text()));
        }

        if (!type->is_value_compatible_with(*def.type)) {
            throw exceptions::configuration_exception(
                    format("Cannot change {} from type {} to type {}: types are incompatible.",
                           def.name_as_text(),
                           def.type->as_cql3_type(),
                           validator));
        }
        break;

    case column_kind::clustering_key:
        if (!schema.is_cql3_table()) {
            throw exceptions::invalid_request_exception(
                    format("Cannot alter clustering column {} in a non-CQL3 table", def.name_as_text()));
        }

        // Note that CFMetaData.validateCompatibility already validate the change we're about to do. However, the error message it
        // sends is a bit cryptic for a CQL3 user, so validating here for a sake of returning a better error message
        // Do note that we need isCompatibleWith here, not just isValueCompatibleWith.
        if (!type->is_compatible_with(*def.type)) {
            throw exceptions::configuration_exception(
                    format("Cannot change {} from type {} to type {}: types are not order-compatible.",
                           def.name_as_text(),
                           def.type->as_cql3_type(),
                           validator));
        }
        break;

    case column_kind::regular_column:
    case column_kind::static_column:
        // Thrift allows to change a column validator so CFMetaData.validateCompatibility will let it slide
        // if we change to an incompatible type (contrarily to the comparator case). But we don't want to
        // allow it for CQL3 (see #5882) so validating it explicitly here. We only care about value compatibility
        // though since we won't compare values (except when there is an index, but that is validated by
        // ColumnDefinition already).
        if (!type->is_value_compatible_with(*def.type)) {
            throw exceptions::configuration_exception(
                    format("Cannot change {} from type {} to type {}: types are incompatible.",
                           def.name_as_text(),
                           def.type->as_cql3_type(),
                           validator));
        }
        break;
    }
    return type;
}

static void validate_column_rename(data_dictionary::database db, const schema& schema, const column_identifier& from, const column_identifier& to)
{
    auto def = schema.get_column_definition(from.name());
    if (!def) {
        throw exceptions::invalid_request_exception(format("Cannot rename unknown column {} in table {}", from, schema.cf_name()));
    }

    if (schema.get_column_definition(to.name())) {
        throw exceptions::invalid_request_exception(format("Cannot rename column {} to {} in table {}; another column of that name already exist", from, to, schema.cf_name()));
    }

    if (def->is_part_of_cell_name()) {
        throw exceptions::invalid_request_exception(format("Cannot rename non PRIMARY KEY part {}", from));
    }

    if (!schema.indices().empty()) {
        auto dependent_indices = db.find_column_family(schema.id()).get_index_manager().get_dependent_indices(*def);
        if (!dependent_indices.empty()) {
            throw exceptions::invalid_request_exception(
                    seastar::format("Cannot rename column {} because it has dependent secondary indexes ({})",
                                    from,
                                    fmt::join(dependent_indices | std::views::transform([](const index_metadata& im) {
                                        return im.name();
                                    }), ", ")));
        }
    }
}

void alter_table_statement::add_column(const query_options&, const schema& schema, data_dictionary::table cf, schema_builder& cfm, std::vector<view_ptr>& view_updates, const column_identifier& column_name, const cql3_type validator, const column_definition* def, bool is_static) const {
    if (is_static) {
        if (!schema.is_compound()) {
            throw exceptions::invalid_request_exception("Static columns are not allowed in COMPACT STORAGE tables");
        }
        if (!schema.clustering_key_size()) {
            throw exceptions::invalid_request_exception("Static columns are only useful (and thus allowed) if the table has at least one clustering column");
        }
    }

    if (def) {
        if (def->is_partition_key()) {
            throw exceptions::invalid_request_exception(format("Invalid column name {} because it conflicts with a PRIMARY KEY part", column_name));
        } else {
            throw exceptions::invalid_request_exception(format("Invalid column name {} because it conflicts with an existing column", column_name));
        }
    }

    // Cannot re-add a dropped counter column. See #7831.
    if (schema.is_counter() && schema.dropped_columns().contains(column_name.text())) {
        throw exceptions::invalid_request_exception(format("Cannot re-add previously dropped counter column {}", column_name));
    }

    auto type = validator.get_type();
    if (type->is_collection() && type->is_multi_cell()) {
        if (!schema.is_compound()) {
            throw exceptions::invalid_request_exception("Cannot use non-frozen collections with a non-composite PRIMARY KEY");
        }
        if (schema.is_super()) {
            throw exceptions::invalid_request_exception("Cannot use non-frozen collections with super column families");
        }


        // If there used to be a non-frozen collection column with the same name (that has been dropped),
        // we could still have some data using the old type, and so we can't allow adding a collection
        // with the same name unless the types are compatible (see #6276).
        auto& dropped = schema.dropped_columns();
        auto i = dropped.find(column_name.text());
        if (i != dropped.end() && i->second.type->is_collection() && i->second.type->is_multi_cell()
                && !type->is_compatible_with(*i->second.type)) {
            throw exceptions::invalid_request_exception(fmt::format("Cannot add a collection with the name {} "
                "because a collection with the same name and a different type has already been used in the past", column_name));
        }
    }
    if (type->is_counter() && !schema.is_counter()) {
        throw exceptions::configuration_exception(format("Cannot add a counter column ({}) in a non counter column family", column_name));
    }

    cfm.with_column(column_name.name(), type, is_static ? column_kind::static_column : column_kind::regular_column);

    // Adding a column to a base table always requires updating the view
    // schemas: If the view includes all columns it should include the new
    // column, but if it doesn't, it may need to include the new
    // unselected column as a virtual column. The case when it we
    // shouldn't add a virtual column is when the view has in its PK one
    // of the base's regular columns - but even in this case we need to
    // rebuild the view schema, to update the column ID.
    if (!is_static) {
        for (auto&& view : cf.views()) {
            schema_builder builder(view);
            if (view->view_info()->include_all_columns()) {
                builder.with_column(column_name.name(), type);
            } else if (!view->view_info()->has_base_non_pk_columns_in_view_pk()) {
                db::view::create_virtual_column(builder, column_name.name(), type);
            }
            view_updates.push_back(view_ptr(builder.build()));
        }
    }
}

void alter_table_statement::alter_column(const query_options&, const schema& schema, data_dictionary::table cf, schema_builder& cfm, std::vector<view_ptr>& view_updates, const column_identifier& column_name, const cql3_type validator, const column_definition* def, bool is_static) const {
    if (!def) {
        throw exceptions::invalid_request_exception(format("Column {} was not found in table {}", column_name, column_family()));
    }

    auto type = validate_alter(schema, *def, validator);
    // In any case, we update the column definition
    cfm.alter_column_type(column_name.name(), type);

    // We also have to validate the view types here. If we have a view which includes a column as part of
    // the clustering key, we need to make sure that it is indeed compatible.
    for (auto&& view : cf.views()) {
        auto* view_def = view->get_column_definition(column_name.name());
        if (view_def) {
            schema_builder builder(view);
            auto view_type = validate_alter(*view, *view_def, validator);
            builder.alter_column_type(column_name.name(), std::move(view_type));
            view_updates.push_back(view_ptr(builder.build()));
        }
    }
}

void alter_table_statement::drop_column(const query_options& options, const schema& schema, data_dictionary::table cf, schema_builder& cfm, std::vector<view_ptr>& view_updates, const column_identifier& column_name, const cql3_type validator, const column_definition* def, bool is_static) const {
    if (!def) {
        throw exceptions::invalid_request_exception(format("Column {} was not found in table {}", column_name, column_family()));
    }

    if (def->is_primary_key()) {
        throw exceptions::invalid_request_exception(format("Cannot drop PRIMARY KEY part {}", column_name));
    } else {
        // We refuse to drop a column from a base-table if one of its
        // materialized views needs this column. This includes columns
        // selected by one of the views, and in some cases even unselected
        // columns needed to determine row liveness (in such case, the
        // column exists in a view as a "virtual column").
        for (const auto& view : cf.views()) {
            for (const auto& column_def : view->all_columns()) {
                if (column_def.name() == column_name.name()) {
                    throw exceptions::invalid_request_exception(format("Cannot drop column {} from base table {}.{}: materialized view {} needs this column",
                            column_name, keyspace(), column_family(), view->cf_name()));
                }
            }
        }

        std::optional<api::timestamp_type> drop_timestamp;
        if (_attrs->is_timestamp_set()) {
            auto now = std::chrono::duration_cast<std::chrono::microseconds>(db_clock::now().time_since_epoch()).count();
            drop_timestamp = _attrs->get_timestamp(now, options);
        }

        for (auto&& column_def : schema.static_and_regular_columns()) { // find
            if (column_def.name() == column_name.name()) {
                cfm.remove_column(column_name.name(), drop_timestamp);
                break;
            }
        }
    }
}

std::pair<schema_ptr, std::vector<view_ptr>> alter_table_statement::prepare_schema_update(data_dictionary::database db, const query_options& options) const {
    auto s = validation::validate_column_family(db, keyspace(), column_family());
    if (s->is_view()) {
        throw exceptions::invalid_request_exception("Cannot use ALTER TABLE on Materialized View");
    }

    auto cfm = schema_builder(s);

    if (_properties->get_id()) {
        throw exceptions::configuration_exception("Cannot alter table id.");
    }

    auto cf = db.find_column_family(s);
    std::vector<view_ptr> view_updates;

    using column_change_fn = std::function<void (const alter_table_statement*, const query_options&, const schema&, data_dictionary::table, schema_builder&, std::vector<view_ptr>&, const column_identifier&, const data_type, const column_definition*, bool)>;

    auto invoke_column_change_fn = [&] (column_change_fn fn) {
         for (auto& [raw_name, raw_validator, is_static] : _column_changes) {
             auto column_name = raw_name->prepare_column_identifier(*s);
             auto validator = raw_validator ? raw_validator->prepare(db, keyspace()).get_type() : nullptr;
             auto* def = get_column_definition(*s, *column_name);
             fn(this, options, *s, cf, cfm, view_updates, *column_name, validator, def, is_static);
         }
    };

    switch (_type) {
    case alter_table_statement::type::add:
        SCYLLA_ASSERT(_column_changes.size());
        if (s->is_dense()) {
            throw exceptions::invalid_request_exception("Cannot add new column to a COMPACT STORAGE table");
        }
        invoke_column_change_fn(std::mem_fn(&alter_table_statement::add_column));
        break;

    case alter_table_statement::type::alter:
        SCYLLA_ASSERT(_column_changes.size() == 1);
        invoke_column_change_fn(std::mem_fn(&alter_table_statement::alter_column));
        break;

    case alter_table_statement::type::drop:
        SCYLLA_ASSERT(_column_changes.size());
        if (!s->is_cql3_table()) {
            throw exceptions::invalid_request_exception("Cannot drop columns from a non-CQL3 table");
        }
        invoke_column_change_fn(std::mem_fn(&alter_table_statement::drop_column));
        break;

    case alter_table_statement::type::opts:
        if (!_properties) {
            throw exceptions::invalid_request_exception("ALTER COLUMNFAMILY WITH invoked, but no parameters found");
        }

        {
            auto schema_extensions = _properties->make_schema_extensions(db.extensions());
            _properties->validate(db, keyspace(), schema_extensions);

            if (!cf.views().empty() && _properties->get_gc_grace_seconds() == 0) {
                throw exceptions::invalid_request_exception(
                        "Cannot alter gc_grace_seconds of the base table of a "
                        "materialized view to 0, since this value is used to TTL "
                        "undelivered updates. Setting gc_grace_seconds too low might "
                        "cause undelivered updates to expire "
                        "before being replayed.");
            }

            if (s->is_counter() && _properties->get_default_time_to_live() > 0) {
                throw exceptions::invalid_request_exception("Cannot set default_time_to_live on a table with counters");
            }

            if (auto it = schema_extensions.find(cdc::cdc_extension::NAME); it != schema_extensions.end()) {
                const auto& cdc_opts = dynamic_pointer_cast<cdc::cdc_extension>(it->second)->get_options();
                if (!cdc_opts.is_enabled_set()) {
                    // "enabled" flag not specified
                    throw exceptions::invalid_request_exception("Altering CDC options requires specifying \"enabled\" flag");
                }
            }

            if (_properties->get_synchronous_updates_flag()) {
                throw exceptions::invalid_request_exception(format("The synchronous_updates option is only applicable to materialized views, not to base tables"));
            }

            _properties->apply_to_builder(cfm, std::move(schema_extensions), db, keyspace());
        }
        break;

    case alter_table_statement::type::rename:
        for (auto&& entry : _renames) {
            auto from = entry.first->prepare_column_identifier(*s);
            auto to = entry.second->prepare_column_identifier(*s);

            validate_column_rename(db, *s, *from, *to);
            cfm.rename_column(from->name(), to->name());
        }
        // New view schemas contain the new column names, so we need to base them on the
        // new base schema.
        schema_ptr new_base_schema = cfm.build();
        // If the view includes a renamed column, it must be renamed in
        // the view table and the definition.
        for (auto&& view : cf.views()) {
            schema_builder builder(view);
            std::vector<std::pair<::shared_ptr<column_identifier>, ::shared_ptr<column_identifier>>> view_renames;
            for (auto&& entry : _renames) {
                auto from = entry.first->prepare_column_identifier(*s);
                if (view->get_column_definition(from->name())) {
                    auto view_from = entry.first->prepare_column_identifier(*view);
                    auto view_to = entry.second->prepare_column_identifier(*view);
                    validate_column_rename(db, *view, *view_from, *view_to);
                    builder.rename_column(view_from->name(), view_to->name());
                    view_renames.emplace_back(view_from, view_to);
                }
            }
            if (!view_renames.empty()) {
                auto new_where = util::rename_columns_in_where_clause(
                        view->view_info()->where_clause(),
                        view_renames,
                        cql3::dialect{});
                builder.with_view_info(new_base_schema, view->view_info()->include_all_columns(), std::move(new_where));
                view_updates.push_back(view_ptr(builder.build()));
            }
        }
        return make_pair(std::move(new_base_schema), std::move(view_updates));
    }

    return make_pair(cfm.build(), std::move(view_updates));
}

future<std::tuple<::shared_ptr<cql_transport::event::schema_change>, utils::chunked_vector<mutation>, cql3::cql_warnings_vec>>
alter_table_statement::prepare_schema_mutations(query_processor& qp, const query_options& options, api::timestamp_type ts) const {
  data_dictionary::database db = qp.db();
  auto [s, view_updates] = prepare_schema_update(db, options);
  auto m = co_await service::prepare_column_family_update_announcement(qp.proxy(), std::move(s), std::move(view_updates), ts);

  using namespace cql_transport;
  auto ret = ::make_shared<event::schema_change>(
            event::schema_change::change_type::UPDATED,
            event::schema_change::target_type::TABLE,
            keyspace(),
            column_family());

  co_return std::make_tuple(std::move(ret), std::move(m), std::vector<sstring>());
}

std::unique_ptr<cql3::statements::prepared_statement>
cql3::statements::alter_table_statement::prepare(data_dictionary::database db, cql_stats& stats) {
    // Cannot happen; alter_table_statement is never instantiated as a raw statement
    // (instead we instantiate alter_table_statement::raw_statement)
    utils::on_internal_error("alter_table_statement cannot be prepared. Use alter_table_statement::raw_statement instead");
}

future<::shared_ptr<messages::result_message>>
alter_table_statement::execute(query_processor& qp, service::query_state& state, const query_options& options, std::optional<service::group0_guard> guard) const {
    validation::validate_column_family(qp.db(), keyspace(), column_family());
    return schema_altering_statement::execute(qp, state, options, std::move(guard));
}

alter_table_statement::raw_statement::raw_statement(cf_name name,
                                                    type t,
                                                    std::vector<column_change> column_changes,
                                                    std::optional<cf_prop_defs> properties,
                                                    renames_type renames,
                                                    std::unique_ptr<attributes::raw> attrs)
    : cf_statement(std::move(name))
    , _type(t)
    , _column_changes(std::move(column_changes))
    , _properties(std::move(properties))
    , _renames(std::move(renames))
    , _attrs(std::move(attrs))
    {}

std::unique_ptr<cql3::statements::prepared_statement>
alter_table_statement::raw_statement::prepare(data_dictionary::database db, cql_stats& stats) {
    auto t = db.try_find_table(keyspace(), column_family());
    std::optional<schema_ptr> s = t ? std::make_optional(t->schema()) : std::nullopt;
    std::optional<sstring> warning = check_restricted_table_properties(db, s, keyspace(), column_family(), *_properties);
    if (warning) {
        // FIXME: should this warning be returned to the caller?
        // See https://github.com/scylladb/scylladb/issues/20945
        mylogger.warn("{}", *warning);
    }

    auto ctx = get_prepare_context();
    auto prepared_attrs = _attrs->prepare(db, keyspace(), column_family());
    prepared_attrs->fill_prepare_context(ctx);

    return std::make_unique<prepared_statement>(audit_info(), ::make_shared<alter_table_statement>(
                ctx.bound_variables_size(),
                *_cf_name,
                _type,
                _column_changes,
                _properties,
                _renames,
                std::move(prepared_attrs)
            ),
            ctx,
            // since alter table is `cql_statement_no_metadata` (it doesn't return any metadata when preparing)
            // and bind markers cannot be a part of partition key,
            // we can pass empty vector as partition_key_bind_indices
            std::vector<uint16_t>()); 
}

}

}
