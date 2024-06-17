/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include <seastar/core/coroutine.hh>
#include "create_index_statement.hh"
#include "exceptions/exceptions.hh"
#include "prepared_statement.hh"
#include "validation.hh"
#include "service/storage_proxy.hh"
#include "service/migration_manager.hh"
#include "schema/schema.hh"
#include "schema/schema_builder.hh"
#include "request_validations.hh"
#include "data_dictionary/data_dictionary.hh"
#include "index/target_parser.hh"
#include "gms/feature_service.hh"
#include "cql3/query_processor.hh"
#include "cql3/index_name.hh"
#include "cql3/statements/index_prop_defs.hh"
#include "index/secondary_index_manager.hh"
#include "mutation/mutation.hh"

#include <boost/range/adaptor/transformed.hpp>
#include <boost/algorithm/string/join.hpp>
#include <stdexcept>

namespace cql3 {

namespace statements {

create_index_statement::create_index_statement(cf_name name,
                                               ::shared_ptr<index_name> index_name,
                                               std::vector<::shared_ptr<index_target::raw>> raw_targets,
                                               ::shared_ptr<index_prop_defs> properties,
                                               bool if_not_exists)
    : schema_altering_statement(name)
    , _index_name(index_name->get_idx())
    , _raw_targets(raw_targets)
    , _properties(properties)
    , _if_not_exists(if_not_exists)
{
}

future<>
create_index_statement::check_access(query_processor& qp, const service::client_state& state) const {
    return state.has_column_family_access(keyspace(), column_family(), auth::permission::ALTER);
}

static sstring target_type_name(index_target::target_type type) {
    switch (type) {
        case index_target::target_type::keys: return "keys()";
        case index_target::target_type::keys_and_values: return "entries()";
        case index_target::target_type::collection_values: return "values()";
        case index_target::target_type::regular_values: return "value";
        default: throw std::invalid_argument("should not reach");
    }
}

void
create_index_statement::validate(query_processor& qp, const service::client_state& state) const
{
    if (_raw_targets.empty() && !_properties->is_custom) {
        throw exceptions::invalid_request_exception("Only CUSTOM indexes can be created without specifying a target column");
    }

    _properties->validate();
}

std::vector<::shared_ptr<index_target>> create_index_statement::validate_while_executing(data_dictionary::database db) const {
    auto schema = validation::validate_column_family(db, keyspace(), column_family());

    if (schema->is_counter()) {
        throw exceptions::invalid_request_exception("Secondary indexes are not supported on counter tables");
    }

    if (schema->is_view()) {
        throw exceptions::invalid_request_exception("Secondary indexes are not supported on materialized views");
    }

    if (schema->is_dense()) {
        throw exceptions::invalid_request_exception(
                "Secondary indexes are not supported on COMPACT STORAGE tables that have clustering columns");
    }

    validate_for_local_index(*schema);

    std::vector<::shared_ptr<index_target>> targets;
    for (auto& raw_target : _raw_targets) {
        targets.emplace_back(raw_target->prepare(*schema));
    }

    if (targets.size() > 1) {
        validate_targets_for_multi_column_index(targets);
    }

    const bool is_local_index = targets.size() > 0 && std::holds_alternative<index_target::multiple_columns>(targets.front()->value);
    for (auto& target : targets) {
        auto* ident = std::get_if<::shared_ptr<column_identifier>>(&target->value);
        if (!ident) {
            continue;
        }
        auto cd = schema->get_column_definition((*ident)->name());

        if (cd == nullptr) {
            throw exceptions::invalid_request_exception(
                    format("No column definition found for column {}", target->column_name()));
        }

        if (!db.features().secondary_indexes_on_static_columns && cd->is_static()) {
            throw exceptions::invalid_request_exception("Cluster does not support secondary indexes on static columns yet,"
                    " upgrade the whole cluster first in order to be able to create them");
        }

        if (cd->type->references_duration()) {
            using request_validations::check_false;
            const auto& ty = *cd->type;

            check_false(ty.is_collection(), "Secondary indexes are not supported on collections containing durations");
            check_false(ty.is_user_type(), "Secondary indexes are not supported on UDTs containing durations");
            check_false(ty.is_tuple(), "Secondary indexes are not supported on tuples containing durations");

            // We're a duration.
            throw exceptions::invalid_request_exception("Secondary indexes are not supported on duration columns");
        }

        // Origin TODO: we could lift that limitation
        if ((schema->is_dense() || !schema->is_compound()) && cd->is_primary_key()) {
            throw exceptions::invalid_request_exception(
                    "Secondary indexes are not supported on PRIMARY KEY columns in COMPACT STORAGE tables");
        }

        if (cd->kind == column_kind::partition_key && cd->is_on_all_components()) {
            throw exceptions::invalid_request_exception(
                    format("Cannot create secondary index on partition key column {}",
                            target->column_name()));
        }

        if (cd->type->is_multi_cell()) {
            if (cd->type->is_collection()) {
                if (!db.features().collection_indexing) {
                    throw exceptions::invalid_request_exception(
                        "Indexing of collection columns not supported by some older nodes in this cluster. Please upgrade them.");
                }
                if (is_local_index) {
                    throw exceptions::invalid_request_exception(
                            format("Local secondary index on collection column {} is not implemented yet.", target->column_name()));
                }
                validate_not_full_index(*target);
                validate_for_collection(*target, *cd);
                rewrite_target_for_collection(*target, *cd);
            } else {
                throw exceptions::invalid_request_exception(format("Cannot create secondary index on UDT column {}", cd->name_as_text()));
            }
        } else if (cd->type->is_collection()) {
            validate_for_frozen_collection(*target);
        } else {
            validate_not_full_index(*target);
            validate_is_values_index_if_target_column_not_collection(cd, *target);
            validate_target_column_is_map_if_index_involves_keys(cd->type->is_map(), *target);
        }
    }

    if (db.existing_index_names(keyspace()).contains(_index_name)) {
        if (!_if_not_exists) {
            throw exceptions::invalid_request_exception("Index already exists");
        }
    }

    return targets;
}

void create_index_statement::validate_for_local_index(const schema& schema) const {
    if (!_raw_targets.empty()) {
            if (const auto* index_pk = std::get_if<std::vector<::shared_ptr<column_identifier::raw>>>(&_raw_targets.front()->value)) {
                auto base_pk_identifiers = *index_pk | boost::adaptors::transformed([&schema] (const ::shared_ptr<column_identifier::raw>& raw_ident) {
                    return raw_ident->prepare_column_identifier(schema);
                });
                auto remaining_base_pk_columns = schema.partition_key_columns();
                auto next_expected_base_column = remaining_base_pk_columns.begin();
                for (const auto& ident : base_pk_identifiers) {
                    auto it = schema.columns_by_name().find(ident->name());
                    if (it == schema.columns_by_name().end() || !it->second->is_partition_key()) {
                        throw exceptions::invalid_request_exception(format("Local index definition must contain full partition key only. Redundant column: {}", ident->to_string()));
                    }
                    if (next_expected_base_column == remaining_base_pk_columns.end()) {
                        throw exceptions::invalid_request_exception(format("Duplicate column definition in local index: {}", it->first));
                    }
                    if (&*next_expected_base_column != it->second) {
                        break;
                    }
                    ++next_expected_base_column;
                }
                if (next_expected_base_column != remaining_base_pk_columns.end()) {
                    throw exceptions::invalid_request_exception(format("Local index definition must contain full partition key only. Missing column: {}", next_expected_base_column->name_as_text()));
                }
                if (_raw_targets.size() == 1) {
                    throw exceptions::invalid_request_exception(format("Local index definition must provide an indexed column, not just partition key"));
                }
            }
        }
        for (unsigned i = 1; i < _raw_targets.size(); ++i) {
            if (std::holds_alternative<index_target::raw::multiple_columns>(_raw_targets[i]->value)) {
                throw exceptions::invalid_request_exception(format("Multi-column index targets are currently only supported for partition key"));
            } else if (auto* raw_ident = std::get_if<index_target::raw::single_column>(&_raw_targets[i]->value)) {
                auto ident = (*raw_ident)->prepare_column_identifier(schema);
                auto it = schema.columns_by_name().find(ident->name());
                if (it != schema.columns_by_name().end() && it->second->is_static()) {
                    throw exceptions::invalid_request_exception("Local indexes containing static columns are not supported.");
                }
            }
        }
}

void create_index_statement::validate_for_frozen_collection(const index_target& target) const
{
    if (target.type != index_target::target_type::full) {
        throw exceptions::invalid_request_exception(
                format("Cannot create index on {} of frozen collection column {}",
                        target_type_name(target.type),
                        target.column_name()));
    }
}

void create_index_statement::validate_not_full_index(const index_target& target) const
{
    if (target.type == index_target::target_type::full) {
        throw exceptions::invalid_request_exception("full() indexes can only be created on frozen collections");
    }
}

void create_index_statement::validate_for_collection(const index_target& target, const column_definition& cd) const
{
    switch (target.type) {
        case index_target::target_type::full:
            throw std::logic_error("invalid target type(full) in validate_for_collection");
        case index_target::target_type::regular_values:
            break;
        case index_target::target_type::collection_values:
            break;
        case index_target::target_type::keys:
            [[fallthrough]];
        case index_target::target_type::keys_and_values:
            if (!cd.type->is_map()) {
                const char* msg_format = "Cannot create secondary index on {} of column {} with non-map type";
                throw exceptions::invalid_request_exception(format(msg_format, to_sstring(target.type), cd.name_as_text()));
            }
            break;
    }
}

void create_index_statement::rewrite_target_for_collection(index_target& target, const column_definition& cd) const
{
    // In Cassandra, `CREATE INDEX ON table(collection)` works the same as `CREATE INDEX ON table(VALUES(collection))`,
    // and index on VALUES(collection) indexes values, if the collection was a map or a list, but it indexes the keys, if it
    // was a set. Rewrite it to clean the mess.
    switch (target.type) {
        case index_target::target_type::full:
            throw std::logic_error("invalid target type(full) in rewrite_target_for_collection");
        case index_target::target_type::keys:
            // If it was keys, then it must have been a map.
            break;
        case index_target::target_type::keys_and_values:
            // If it was entries, then it must have been a map.
            break;
        case index_target::target_type::regular_values:
            // Regular values for collections means the same as collection values.
            [[fallthrough]];
        case index_target::target_type::collection_values:
            if (cd.type->is_map() || cd.type->is_list()) {
                target.type = index_target::target_type::collection_values;
            } else if (cd.type->is_set()) {
                target.type = index_target::target_type::keys;
            } else {
                throw std::logic_error(format("rewrite_target_for_collection: unknown collection type {}", cd.type->cql3_type_name()));
            }
            break;
    }
}


void create_index_statement::validate_is_values_index_if_target_column_not_collection(
        const column_definition* cd, const index_target& target) const
{
    if (!cd->type->is_collection()
            && target.type != index_target::target_type::regular_values) {
        throw exceptions::invalid_request_exception(
                format("Cannot create index on {} of column {}; only non-frozen collections support {} indexes",
                       target_type_name(target.type),
                       target.column_name(),
                       target_type_name(target.type)));
    }
}

void create_index_statement::validate_target_column_is_map_if_index_involves_keys(bool is_map, const index_target& target) const
{
    if (target.type == index_target::target_type::keys
            || target.type == index_target::target_type::keys_and_values) {
        if (!is_map) {
            throw exceptions::invalid_request_exception(
                    format("Cannot create index on {} of column {} with non-map type",
                           target_type_name(target.type), target.column_name()));
        }
    }
}

void create_index_statement::validate_targets_for_multi_column_index(std::vector<::shared_ptr<index_target>> targets) const
{
    if (!_properties->is_custom) {
        if (targets.size() > 2 || (targets.size() == 2 && std::holds_alternative<index_target::single_column>(targets.front()->value))) {
            throw exceptions::invalid_request_exception("Only CUSTOM indexes support multiple columns");
        }
    }
    std::unordered_set<sstring> columns;
    for (auto& target : targets) {
        if (columns.contains(target->column_name())) {
            throw exceptions::invalid_request_exception(format("Duplicate column {} in index target list", target->column_name()));
        }
        columns.emplace(target->column_name());
    }
}

std::optional<create_index_statement::base_schema_with_new_index> create_index_statement::build_index_schema(data_dictionary::database db) const {
    auto targets = validate_while_executing(db);

    auto schema = db.find_schema(keyspace(), column_family());

    sstring accepted_name = _index_name;
    if (accepted_name.empty()) {
        std::optional<sstring> index_name_root;
        if (targets.size() == 1) {
           index_name_root = targets[0]->column_name();
        } else if ((targets.size() == 2 && std::holds_alternative<index_target::multiple_columns>(targets.front()->value))) {
            index_name_root = targets[1]->column_name();
        }
        accepted_name = db.get_available_index_name(keyspace(), column_family(), index_name_root);
    }
    index_metadata_kind kind;
    index_options_map index_options;
    if (_properties->is_custom) {
        kind = index_metadata_kind::custom;
        index_options = _properties->get_options();
    } else {
        kind = schema->is_compound() ? index_metadata_kind::composites : index_metadata_kind::keys;
    }
    auto index = make_index_metadata(targets, accepted_name, kind, index_options);
    auto existing_index = schema->find_index_noname(index);
    if (existing_index) {
        if (_if_not_exists) {
            return {};
        } else {
            throw exceptions::invalid_request_exception(
                    format("Index {} is a duplicate of existing index {}", index.name(), existing_index.value().name()));
        }
    }
    auto index_table_name = secondary_index::index_table_name(accepted_name);
    if (db.has_schema(keyspace(), index_table_name)) {
        // We print this error even if _if_not_exists - in this case the user
        // asked to create a not-previously-existing index, but under an
        // already-taken name. This should be an error, not a silent success.
        throw exceptions::invalid_request_exception(format("Index {} cannot be created, because table {} already exists", accepted_name, index_table_name));
    }
    ++_cql_stats->secondary_index_creates;
    schema_builder builder{schema};
    builder.with_index(index);

    return base_schema_with_new_index{builder.build(), index};
}

future<std::tuple<::shared_ptr<cql_transport::event::schema_change>, std::vector<mutation>, cql3::cql_warnings_vec>>
create_index_statement::prepare_schema_mutations(query_processor& qp, const query_options&, api::timestamp_type ts) const {
    using namespace cql_transport;
    auto res = build_index_schema(qp.db());

    ::shared_ptr<event::schema_change> ret;
    std::vector<mutation> m;

    if (res) {
        m = co_await service::prepare_column_family_update_announcement(qp.proxy(), std::move(res->schema), {}, ts);

        ret = ::make_shared<event::schema_change>(
                event::schema_change::change_type::UPDATED,
                event::schema_change::target_type::TABLE,
                keyspace(),
                column_family());
    }

    co_return std::make_tuple(std::move(ret), std::move(m), std::vector<sstring>());
}

std::unique_ptr<cql3::statements::prepared_statement>
create_index_statement::prepare(data_dictionary::database db, cql_stats& stats) {
    _cql_stats = &stats;
    return std::make_unique<prepared_statement>(make_shared<create_index_statement>(*this));
}

index_metadata create_index_statement::make_index_metadata(const std::vector<::shared_ptr<index_target>>& targets,
                                                           const sstring& name,
                                                           index_metadata_kind kind,
                                                           const index_options_map& options)
{
    index_options_map new_options = options;
    auto target_option = secondary_index::target_parser::serialize_targets(targets);
    new_options.emplace(index_target::target_option_name, target_option);

    const auto& first_target = targets.front()->value;
    return index_metadata{name, new_options, kind, index_metadata::is_local_index(std::holds_alternative<index_target::multiple_columns>(first_target))};
}

}

}
