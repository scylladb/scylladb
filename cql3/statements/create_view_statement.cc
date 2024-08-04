/*
 * Copyright (C) 2016-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include "utils/assert.hh"
#include <unordered_set>
#include <vector>

#include <boost/range/iterator_range.hpp>
#include <boost/range/join.hpp>
#include <boost/range/adaptor/map.hpp>
#include <boost/range/adaptor/transformed.hpp>

#include <seastar/core/coroutine.hh>
#include "cql3/column_identifier.hh"
#include "cql3/restrictions/statement_restrictions.hh"
#include "cql3/statements/create_view_statement.hh"
#include "cql3/statements/prepared_statement.hh"
#include "cql3/statements/select_statement.hh"
#include "cql3/statements/raw/select_statement.hh"
#include "cql3/query_processor.hh"
#include "cql3/util.hh"
#include "schema/schema_builder.hh"
#include "service/storage_proxy.hh"
#include "validation.hh"
#include "data_dictionary/data_dictionary.hh"
#include "gms/feature_service.hh"
#include "db/view/view.hh"
#include "service/migration_manager.hh"

namespace cql3 {

namespace statements {

create_view_statement::create_view_statement(
        cf_name view_name,
        cf_name base_name,
        std::vector<::shared_ptr<selection::raw_selector>> select_clause,
        expr::expression where_clause,
        std::vector<::shared_ptr<cql3::column_identifier::raw>> partition_keys,
        std::vector<::shared_ptr<cql3::column_identifier::raw>> clustering_keys,
        bool if_not_exists)
    : schema_altering_statement{view_name}
    , _base_name{base_name}
    , _select_clause{select_clause}
    , _where_clause{where_clause}
    , _partition_keys{partition_keys}
    , _clustering_keys{clustering_keys}
    , _if_not_exists{if_not_exists}
{
}

future<> create_view_statement::check_access(query_processor& qp, const service::client_state& state) const {
    return state.has_column_family_access(keyspace(), _base_name.get_column_family(), auth::permission::ALTER);
}

static const column_definition* get_column_definition(const schema& schema, column_identifier::raw& identifier) {
    auto prepared = identifier.prepare(schema);
    SCYLLA_ASSERT(dynamic_pointer_cast<column_identifier>(prepared));
    auto id = static_pointer_cast<column_identifier>(prepared);
    return schema.get_column_definition(id->name());
}

static bool validate_primary_key(
        const schema& schema,
        const column_definition* def,
        const std::unordered_set<const column_definition*>& base_pk,
        bool has_non_pk_column,
        const restrictions::statement_restrictions& restrictions) {

    if (def->type->is_multi_cell()) {
        throw exceptions::invalid_request_exception(format("Cannot use MultiCell column '{}' in PRIMARY KEY of materialized view", def->name_as_text()));
    }

    if (def->type->references_duration()) {
        throw exceptions::invalid_request_exception(format("Cannot use Duration column '{}' in PRIMARY KEY of materialized view", def->name_as_text()));
    }

    if (def->is_static()) {
        throw exceptions::invalid_request_exception(format("Cannot use Static column '{}' in PRIMARY KEY of materialized view", def->name_as_text()));
    }

    bool new_non_pk_column = false;
    if (!base_pk.contains(def)) {
        if (has_non_pk_column) {
            throw exceptions::invalid_request_exception(format("Cannot include more than one non-primary key column '{}' in materialized view primary key", def->name_as_text()));
        }
        new_non_pk_column = true;
    }

    // We don't need to include the "IS NOT NULL" filter on a non-composite partition key
    // because we will never allow a single partition key to be NULL
    bool is_non_composite_partition_key = def->is_partition_key() &&
            schema.partition_key_columns().size() == 1;
    if (!is_non_composite_partition_key && !restrictions.is_restricted(def)) {
        throw exceptions::invalid_request_exception(format("Primary key column '{}' is required to be filtered by 'IS NOT NULL'", def->name_as_text()));
    }

    return new_non_pk_column;
}

std::pair<view_ptr, cql3::cql_warnings_vec> create_view_statement::prepare_view(data_dictionary::database db) const {
    // We need to make sure that:
    //  - primary key includes all columns in base table's primary key
    //  - make sure that the select statement does not have anything other than columns
    //    and their names match the base table's names
    //  - make sure that primary key does not include any collections
    //  - make sure there is no where clause in the select statement
    //  - make sure there is not currently a table or view
    //  - make sure base_table gc_grace_seconds > 0

    cql3::cql_warnings_vec warnings;

    auto schema_extensions = _properties.properties()->make_schema_extensions(db.extensions());
    _properties.validate(db, keyspace(), schema_extensions);

    if (_properties.use_compact_storage()) {
        throw exceptions::invalid_request_exception(format("Cannot use 'COMPACT STORAGE' when defining a materialized view"));
    }

    if (_properties.properties()->get_cdc_options(schema_extensions)) {
        throw exceptions::invalid_request_exception("Cannot enable CDC for a materialized view");
    }

    // View and base tables must be in the same keyspace, to ensure that RF
    // is the same (because we assign a view replica to each base replica).
    // If a keyspace was not specified for the base table name, it is assumed
    // it is in the same keyspace as the view table being created (which
    // itself might be the current USEd keyspace, or explicitly specified).
    if (!_base_name.has_keyspace()) {
        _base_name.set_keyspace(keyspace(), true);
    }
    if (_base_name.get_keyspace() != keyspace()) {
        throw exceptions::invalid_request_exception(format("Cannot create a materialized view on a table in a separate keyspace ('{}' != '{}')",
                _base_name.get_keyspace(), keyspace()));
    }

    schema_ptr schema = validation::validate_column_family(db, _base_name.get_keyspace(), _base_name.get_column_family());

    if (schema->is_counter()) {
        throw exceptions::invalid_request_exception(format("Materialized views are not supported on counter tables"));
    }

    if (schema->is_view()) {
        throw exceptions::invalid_request_exception(format("Materialized views cannot be created against other materialized views"));
    }

    if (db.get_cdc_base_table(*schema)) {
        throw exceptions::invalid_request_exception(format("Materialized views cannot be created on CDC Log tables"));
    }

    if (schema->gc_grace_seconds().count() == 0) {
        throw exceptions::invalid_request_exception(fmt::format(
                "Cannot create materialized view '{}' for base table "
                "'{}' with gc_grace_seconds of 0, since this value is "
                "used to TTL undelivered updates. Setting gc_grace_seconds "
                "too low might cause undelivered updates to expire "
                "before being replayed.", column_family(), _base_name.get_column_family()));
    }

    // Gather all included columns, as specified by the select clause
    auto included = boost::copy_range<std::unordered_set<const column_definition*>>(_select_clause | boost::adaptors::transformed([&](auto&& selector) {
        if (selector->alias) {
            throw exceptions::invalid_request_exception(format("Cannot use alias when defining a materialized view"));
        }

        auto& selectable = selector->selectable_;
        shared_ptr<column_identifier::raw> identifier;
        expr::visit(overloaded_functor{
            [&] (const expr::unresolved_identifier& ui) { identifier = ui.ident; },
            [] (const auto& default_case) -> void { throw exceptions::invalid_request_exception(format("Cannot use general expressions when defining a materialized view")); },
        }, selectable);

        auto* def = get_column_definition(*schema, *identifier);
        if (!def) {
            throw exceptions::invalid_request_exception(format("Unknown column name detected in CREATE MATERIALIZED VIEW statement: {}", identifier));
        }
        return def;
    }));

    auto parameters = make_lw_shared<raw::select_statement::parameters>(raw::select_statement::parameters::orderings_type(), false, true);
    raw::select_statement raw_select(_base_name, std::move(parameters), _select_clause, _where_clause, std::nullopt, std::nullopt, {}, std::make_unique<cql3::attributes::raw>());
    raw_select.prepare_keyspace(keyspace());
    raw_select.set_bound_variables({});

    cql_stats ignored;
    auto prepared = raw_select.prepare(db, ignored, true);
    auto restrictions = static_pointer_cast<statements::select_statement>(prepared->statement)->get_restrictions();

    auto base_primary_key_cols = boost::copy_range<std::unordered_set<const column_definition*>>(
            boost::range::join(schema->partition_key_columns(), schema->clustering_key_columns())
            | boost::adaptors::transformed([](auto&& def) { return &def; }));

    // Validate the primary key clause, ensuring only one non-PK base column is used in the view's PK.
    bool has_non_pk_column = false;
    std::unordered_set<const column_definition*> target_primary_keys;
    std::vector<const column_definition*> target_partition_keys;
    std::vector<const column_definition*> target_clustering_keys;
    auto validate_pk = [&] (const std::vector<::shared_ptr<cql3::column_identifier::raw>>& keys, std::vector<const column_definition*>& target_keys) mutable {
        for (auto&& identifier : keys) {
            auto* def = get_column_definition(*schema, *identifier);
            if (!def) {
                throw exceptions::invalid_request_exception(format("Unknown column name detected in CREATE MATERIALIZED VIEW statement: {}", identifier));
            }
            if (!target_primary_keys.insert(def).second) {
                throw exceptions::invalid_request_exception(format("Duplicate entry found in PRIMARY KEY: {}", identifier));
            }
            target_keys.push_back(def);
            has_non_pk_column |= validate_primary_key(*schema, def, base_primary_key_cols, has_non_pk_column, *restrictions);
        }
    };
    validate_pk(_partition_keys, target_partition_keys);
    validate_pk(_clustering_keys, target_clustering_keys);

    std::vector<const column_definition*> missing_pk_columns;
    std::vector<const column_definition*> target_non_pk_columns;
    std::vector<const column_definition*> unselected_columns;

    // We need to include all of the primary key columns from the base table in order to make sure that we do not
    // overwrite values in the view. We cannot support "collapsing" the base table into a smaller number of rows in
    // the view because if we need to generate a tombstone, we have no way of knowing which value is currently being
    // used in the view and whether or not to generate a tombstone. In order to not surprise our users, we require
    // that they include all of the columns. We provide them with a list of all of the columns left to include.
    for (auto& def : schema->all_columns()) {
        bool included_def = included.empty() || included.contains(&def);
        if (included_def && def.is_static()) {
            throw exceptions::invalid_request_exception(format("Unable to include static column '{}' which would be included by Materialized View SELECT * statement", def.name_as_text()));
        }

        bool def_in_target_pk = target_primary_keys.contains(&def);
        if (included_def && !def_in_target_pk) {
            target_non_pk_columns.push_back(&def);
        }
        if (!included_def && !def_in_target_pk && !def.is_static()) {
            unselected_columns.push_back(&def);
        }
        if (def.is_primary_key() && !def_in_target_pk) {
            missing_pk_columns.push_back(&def);
        }
    }

    if (!missing_pk_columns.empty()) {
        auto column_names = fmt::join(missing_pk_columns | boost::adaptors::transformed(std::mem_fn(&column_definition::name_as_text)), ", ");
        throw exceptions::invalid_request_exception(format("Cannot create Materialized View {} without primary key columns from base {} ({})",
                        column_family(), _base_name.get_column_family(), column_names));
    }

    if (_partition_keys.empty()) {
        throw exceptions::invalid_request_exception(format("Must select at least a column for a Materialized View"));
    }

    // Cassandra requires that if CLUSTERING ORDER BY is used, it must specify
    // all clustering columns. Scylla relaxes this requirement and allows just
    // a subset of the clustering columns. But it doesn't make sense (and it's
    // forbidden) to list something which is not a clustering key column in
    // the CLUSTERING ORDER BY. Let's verify that:
    for (auto& pair: _properties.defined_ordering()) {
        auto&& name = pair.first->text();
        bool not_clustering = true;
        for (auto& c : _clustering_keys) {
            if (name == c->to_string()) {
                not_clustering = false;
                break;
            }
        }
        if (not_clustering) {
            throw exceptions::invalid_request_exception(format("CLUSTERING ORDER BY lists {} which is not a clustering column in the view", name));
        }
    }

    // The unique feature of a filter by a non-key column is that the
    // value of such column can be updated - and also be expired with TTL
    // and cause the view row to appear and disappear. We don't currently
    // support support this case - see issue #3430, and neither does
    // Cassandra - see see CASSANDRA-13798 and CASSANDRA-13832.
    // Actually, as CASSANDRA-13798 explains, the problem is "the liveness of
    // view row is now depending on multiple base columns (multiple filtered
    // non-pk base column + base column used in view pk)". When the filtered
    // column *is* the base column added to the view pk, we don't have this
    // problem. And this case actually works correctly.
    const expr::single_column_restrictions_map& non_pk_restrictions = restrictions->get_non_pk_restriction();
    if (non_pk_restrictions.size() == 1 && has_non_pk_column &&
            target_primary_keys.contains(non_pk_restrictions.cbegin()->first)) {
        // This case (filter by new PK column of the view) works, as explained above
    } else if (!non_pk_restrictions.empty()) {
        auto column_names = fmt::join(non_pk_restrictions | boost::adaptors::map_keys | boost::adaptors::transformed(std::mem_fn(&column_definition::name_as_text)), ", ");
        throw exceptions::invalid_request_exception(format("Non-primary key columns cannot be restricted in the SELECT statement used for materialized view {} creation (got restrictions on: {})",
                column_family(), column_names));
    }

    // IS NOT NULL restrictions are handled separately from other restrictions.
    // They need a separate check as they won't be included in non_pk_restrictions.
    std::vector<std::string_view> invalid_not_null_column_names;
    for (const column_definition* not_null_cdef : restrictions->get_not_null_columns()) {
        if (!target_primary_keys.contains(not_null_cdef)) {
            invalid_not_null_column_names.push_back(not_null_cdef->name_as_text());
        }
    }

    if (!invalid_not_null_column_names.empty() &&
        db.get_config().strict_is_not_null_in_views() == db::tri_mode_restriction_t::mode::TRUE) {
        throw exceptions::invalid_request_exception(
            fmt::format("The IS NOT NULL restriction is allowed only columns which are part of the view's primary key,"
                        " found columns: {}. The flag strict_is_not_null_in_views can be used to turn this error "
                        "into a warning, or to silence it. (true - error, warn - warning, false - silent)",
                        fmt::join(invalid_not_null_column_names, ", ")));
    }

    if (!invalid_not_null_column_names.empty() &&
        db.get_config().strict_is_not_null_in_views() == db::tri_mode_restriction_t::mode::WARN) {
        sstring warning_text = fmt::format(
            "The IS NOT NULL restriction is allowed only columns which are part of the view's primary key,"
            " found columns: {}. Restrictions on these columns will be silently ignored. "
            "The flag strict_is_not_null_in_views can be used to turn this warning into an error, or to silence it. "
            "(true - error, warn - warning, false - silent)",
            fmt::join(invalid_not_null_column_names, ", "));
        warnings.emplace_back(std::move(warning_text));
    }

    schema_builder builder{keyspace(), column_family()};
    auto add_columns = [this, &builder] (std::vector<const column_definition*>& defs, column_kind kind) mutable {
        for (auto* def : defs) {
            auto&& type = _properties.get_reversable_type(*def->column_specification->name, def->type);
            builder.with_column(def->name(), type, kind);
        }
    };
    add_columns(target_partition_keys, column_kind::partition_key);
    add_columns(target_clustering_keys, column_kind::clustering_key);
    add_columns(target_non_pk_columns, column_kind::regular_column);
    // Add all unselected columns (base-table columns which are not selected
    // in the view) as "virtual columns" - columns which have timestamp and
    // ttl information, but an empty value. These are needed to keep view
    // rows alive when the base row is alive, even if the view row has no
    // data, just a key (see issue #3362). The virtual columns are not needed
    // when the view pk adds a regular base column (i.e., has_non_pk_column)
    // because in that case, the liveness of that base column is what
    // determines the liveness of the view row.
    if (!has_non_pk_column) {
        for (auto* def : unselected_columns) {
            db::view::create_virtual_column(builder, def->name(), def->type);
        }
    }
    _properties.properties()->apply_to_builder(builder, std::move(schema_extensions), db, keyspace());

    if (builder.default_time_to_live().count() > 0) {
        throw exceptions::invalid_request_exception(
                "Cannot set or alter default_time_to_live for a materialized view. "
                "Data in a materialized view always expire at the same time than "
                "the corresponding data in the parent table.");
    }

    auto where_clause_text = util::relations_to_where_clause(_where_clause);
    builder.with_view_info(schema->id(), schema->cf_name(), included.empty(), std::move(where_clause_text));

    return std::make_pair(view_ptr(builder.build()), std::move(warnings));
}

future<std::tuple<::shared_ptr<cql_transport::event::schema_change>, std::vector<mutation>, cql3::cql_warnings_vec>>
create_view_statement::prepare_schema_mutations(query_processor& qp, const query_options&, api::timestamp_type ts) const {
    std::vector<mutation> m;
    auto [definition, warnings] = prepare_view(qp.db());
    try {
        m = co_await service::prepare_new_view_announcement(qp.proxy(), std::move(definition), ts);
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
    co_return std::make_tuple(created_event(), std::move(m), std::move(warnings));
}

std::unique_ptr<cql3::statements::prepared_statement>
create_view_statement::prepare(data_dictionary::database db, cql_stats& stats) {
    if (!_prepare_ctx.get_variable_specifications().empty()) {
        throw exceptions::invalid_request_exception(format("Cannot use query parameters in CREATE MATERIALIZED VIEW statements"));
    }
    return std::make_unique<prepared_statement>(make_shared<create_view_statement>(*this));
}

::shared_ptr<schema_altering_statement::event_t> create_view_statement::created_event() const {
    return make_shared<event_t>(
            event_t::change_type::CREATED,
            event_t::target_type::TABLE,
            keyspace(),
            column_family());
}

}

}
