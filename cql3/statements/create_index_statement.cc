/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.0 and Apache-2.0)
 */

#include <seastar/core/coroutine.hh>
#include "create_index_statement.hh"
#include "db/config.hh"
#include "db/view/view.hh"
#include "exceptions/exceptions.hh"
#include "index/vector_index.hh"
#include "prepared_statement.hh"
#include "replica/database.hh"
#include "types/types.hh"
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
#include "db/schema_tables.hh"
#include "index/secondary_index_manager.hh"
#include "types/concrete_types.hh"
#include "db/tags/extension.hh"
#include "tombstone_gc_extension.hh"

#include <stdexcept>

namespace cql3 {

namespace statements {

static const data_type collection_keys_type(const abstract_type& t) {
    struct visitor {
        const data_type operator()(const abstract_type& t) {
            throw std::logic_error(format("collection_keys_type: only collections (maps, lists and sets) supported, but received {}", t.cql3_type_name()));
        }
        const data_type operator()(const list_type_impl& l) {
            return timeuuid_type;
        }
        const data_type operator()(const map_type_impl& m) {
            return m.get_keys_type();
        }
        const data_type operator()(const set_type_impl& s) {
            return s.get_elements_type();
        }
    };
    return visit(t, visitor{});
}

static const data_type collection_values_type(const abstract_type& t) {
    struct visitor {
        const data_type operator()(const abstract_type& t) {
            throw std::logic_error(format("collection_values_type: only maps and lists supported, but received {}", t.cql3_type_name()));
        }
        const data_type operator()(const map_type_impl& m) {
            return m.get_values_type();
        }
        const data_type operator()(const list_type_impl& l) {
            return l.get_elements_type();
        }
    };
    return visit(t, visitor{});
}

static const data_type collection_entries_type(const abstract_type& t) {
    struct visitor {
        const data_type operator()(const abstract_type& t) {
            throw std::logic_error(format("collection_entries_type: only maps supported, but received {}", t.cql3_type_name()));
        }
        const data_type operator()(const map_type_impl& m) {
            return tuple_type_impl::get_instance({m.get_keys_type(), m.get_values_type()});
        }
    };
    return visit(t, visitor{});
}

static bytes get_available_column_name(const schema& schema, const bytes& root) {
    bytes accepted_name = root;
    int i = 0;
    while (schema.get_column_definition(accepted_name)) {
        accepted_name = root + to_bytes("_") + to_bytes(std::to_string(++i));
    }
    return accepted_name;
}

static bytes get_available_token_column_name(const schema& schema) {
    return get_available_column_name(schema, "idx_token");
}

static bytes get_available_computed_collection_column_name(const schema& schema) {
    return get_available_column_name(schema, "coll_value");
}

static data_type type_for_computed_column(cql3::statements::index_target::target_type target, const abstract_type& collection_type) {
    using namespace cql3::statements;
    switch (target) {
        case index_target::target_type::keys:               return collection_keys_type(collection_type);
        case index_target::target_type::keys_and_values:    return collection_entries_type(collection_type);
        case index_target::target_type::collection_values:  return collection_values_type(collection_type);
        default: throw std::logic_error("reached regular values or full when only collection index target types were expected");
    }
}

view_ptr create_index_statement::create_view_for_index(const schema_ptr schema, const index_metadata& im,
        const data_dictionary::database& db) const
{
    sstring index_target_name = im.options().at(cql3::statements::index_target::target_option_name);
    schema_builder builder{schema->ks_name(), secondary_index::index_table_name(im.name())};
    auto target_info = secondary_index::target_parser::parse(schema, im);
    const auto* index_target = im.local() ? target_info.ck_columns.front() : target_info.pk_columns.front();
    auto target_type = target_info.type;

    // For local indexing, start with base partition key
    if (im.local()) {
        if (index_target->is_partition_key()) {
            throw exceptions::invalid_request_exception("Local indexing based on partition key column is not allowed,"
                    " since whole base partition key must be used in queries anyway. Use global indexing instead.");
        }
        for (auto& col : schema->partition_key_columns()) {
            builder.with_column(col.name(), col.type, column_kind::partition_key);
        }
        builder.with_column(index_target->name(), index_target->type, column_kind::clustering_key);
    } else {
        if (target_type == cql3::statements::index_target::target_type::regular_values) {
            builder.with_column(index_target->name(), index_target->type, column_kind::partition_key);
        } else {
            bytes key_column_name = get_available_computed_collection_column_name(*schema);
            column_computation_ptr collection_column_computation_ptr = [&name = index_target->name(), target_type] {
                switch (target_type) {
                    case cql3::statements::index_target::target_type::keys:
                        return collection_column_computation::for_keys(name);
                    case cql3::statements::index_target::target_type::collection_values:
                        return collection_column_computation::for_values(name);
                    case cql3::statements::index_target::target_type::keys_and_values:
                        return collection_column_computation::for_entries(name);
                    default:
                        throw std::logic_error(format("create_view_for_index: invalid target_type, received {}", to_sstring(target_type)));
                }
            }().clone();

            data_type t = type_for_computed_column(target_type, *index_target->type);
            builder.with_computed_column(key_column_name, t, column_kind::partition_key, std::move(collection_column_computation_ptr));
        }
        // Additional token column is added to ensure token order on secondary index queries
        bytes token_column_name = get_available_token_column_name(*schema);
        builder.with_computed_column(token_column_name, long_type, column_kind::clustering_key, std::make_unique<token_column_computation>());

        for (auto& col : schema->partition_key_columns()) {
            if (col == *index_target) {
                continue;
            }
            builder.with_column(col.name(), col.type, column_kind::clustering_key);
        }
    }

    if (!index_target->is_static()) {
        for (auto& col : schema->clustering_key_columns()) {
            if (col == *index_target) {
                continue;
            }
            builder.with_column(col.name(), col.type, column_kind::clustering_key);
        }
    }

    // This column needs to be after the base clustering key.
    if (!im.local()) {
        // If two cells within the same collection share the same value but not liveness information, then
        // for the index on the values, the rows generated would share the same primary key and thus the
        // liveness information as well. Prevent that by distinguishing them in the clustering key.
        if (target_type == cql3::statements::index_target::target_type::collection_values) {
            data_type t = type_for_computed_column(cql3::statements::index_target::target_type::keys, *index_target->type);
            bytes column_name = get_available_column_name(*schema, "keys_for_values_idx");
            builder.with_computed_column(column_name, t, column_kind::clustering_key, collection_column_computation::for_keys(index_target->name()).clone());
        }
    }

    if (index_target->is_primary_key()) {
        for (auto& def : schema->regular_columns()) {
            db::view::create_virtual_column(builder, def.name(), def.type);
        }
    }
    // "WHERE col IS NOT NULL" is not needed (and doesn't work)
    // when col is a collection.
    const sstring where_clause =
        (target_type == cql3::statements::index_target::target_type::regular_values) ?
        format("{} IS NOT NULL", index_target->name_as_cql_string()) :
        "";
    builder.with_view_info(schema, false, where_clause);

    bool is_colocated = [&] {
        if (!db.find_keyspace(keyspace()).get_replication_strategy().uses_tablets()) {
            return false;
        }
        return im.local();
    }();

    auto tombstone_gc_ext = seastar::make_shared<tombstone_gc_extension>(get_default_tombstone_gc_mode(db, schema->ks_name(), !is_colocated));
    builder.add_extension(tombstone_gc_extension::NAME, std::move(tombstone_gc_ext));

    // A local secondary index should be backed by a *synchronous* view,
    // see #16371. A view is marked synchronous with a tag. Non-local indexes
    // do not need the tags schema extension at all.
    if (im.local()) {
        std::map<sstring, sstring> tags_map = {{db::SYNCHRONOUS_VIEW_UPDATES_TAG_KEY, "true"}};
        builder.add_extension(db::tags_extension::NAME, ::make_shared<db::tags_extension>(tags_map));
    }

    const schema::extensions_map exts = _view_properties.properties()->make_schema_extensions(db.extensions());
    _view_properties.apply_to_builder(view_prop_defs::op_type::create, builder, exts, db, keyspace(), is_colocated);

    return view_ptr{builder.build()};
}

create_index_statement::create_index_statement(cf_name name,
                                               ::shared_ptr<index_name> index_name,
                                               std::vector<::shared_ptr<index_target::raw>> raw_targets,
                                               ::shared_ptr<index_specific_prop_defs> idx_properties,
                                               view_prop_defs view_properties,
                                               bool if_not_exists)
    : schema_altering_statement(name)
    , _index_name(index_name->get_idx())
    , _raw_targets(raw_targets)
    , _idx_properties(std::move(idx_properties))
    , _view_properties(std::move(view_properties))
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
    if (_raw_targets.empty() && !_idx_properties->is_custom) {
        throw exceptions::invalid_request_exception("Only CUSTOM indexes can be created without specifying a target column");
    }

    _idx_properties->validate();

    // FIXME: This is ugly and can be improved.
    const bool is_vector_index = _idx_properties->custom_class && *_idx_properties->custom_class == "vector_index";
    const bool uses_view_properties = _view_properties.properties()->count() > 0
            || _view_properties.use_compact_storage()
            || _view_properties.defined_ordering().size() > 0;

    if (is_vector_index && uses_view_properties) {
        throw exceptions::invalid_request_exception("You cannot use view properties with a vector index");
    }

    const schema::extensions_map exts = _view_properties.properties()->make_schema_extensions(qp.db().extensions());
    _view_properties.validate_raw(view_prop_defs::op_type::create, qp.db(), keyspace(), exts);

    // These keywords are still accepted by other schema entities, but they don't have effect on them.
    // Since indexes are not bound by any backward compatibility contract in this regard, let's forbid these.
    static sstring obsolete_keywords[] = {
        "index_interval",
        "replicate_on_write",
        "populate_io_cache_on_flush",
        "read_repair_chance",
        "dclocal_read_repair_chance",
    };

    for (const sstring& keyword : obsolete_keywords) {
        if (_view_properties.properties()->has_property(keyword)) {
            // We use the same type of exception and the same error message as would be thrown for
            // an invalid property via `_view_properties.validate_raw`.
            throw exceptions::syntax_exception(seastar::format("Unknown property '{}'", keyword));
        }
    }
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

    if (_index_name.size() > size_t(schema::NAME_LENGTH)) {
        throw exceptions::invalid_request_exception(format("index names shouldn't be more than {:d} characters long (got \"{}\")", schema::NAME_LENGTH, _index_name.c_str()));
    }

    // Regular secondary indexes require rf-rack-validity.
    // Custom indexes need to validate this property themselves, if they need it.
    if (!_idx_properties || !_idx_properties->custom_class) {
        try {
            db::view::validate_view_keyspace(db, keyspace());
        } catch (const std::exception& e) {
            // The type of the thrown exception is not specified, so we need to wrap it here.
            throw exceptions::invalid_request_exception(e.what());
        }
    }

    validate_for_local_index(*schema);

    std::vector<::shared_ptr<index_target>> targets;
    for (auto& raw_target : _raw_targets) {
        targets.emplace_back(raw_target->prepare(*schema));
    }

    if (_idx_properties && _idx_properties->custom_class) {
        auto custom_index_factory = secondary_index::secondary_index_manager::get_custom_class_factory(*_idx_properties->custom_class);
        if (!custom_index_factory) {
            throw exceptions::invalid_request_exception(format("Non-supported custom class \'{}\' provided", *_idx_properties->custom_class));
        }
        auto custom_index = (*custom_index_factory)();
        custom_index->validate(*schema, *_idx_properties, targets, db.features(), db);
        _idx_properties->index_version = custom_index->index_version(*schema);
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
                auto base_pk_identifiers = *index_pk | std::views::transform([&schema] (const ::shared_ptr<column_identifier::raw>& raw_ident) {
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
                constexpr const char* msg_format = "Cannot create secondary index on {} of column {} with non-map type";
                throw exceptions::invalid_request_exception(seastar::format(msg_format, to_sstring(target.type), cd.name_as_text()));
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
    if (!_idx_properties->is_custom) {
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
    if (_idx_properties->custom_class) {
        index_options = _idx_properties->get_options();
        kind = index_metadata_kind::custom;
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
    bool existing_vector_index = _idx_properties->custom_class && _idx_properties->custom_class == "vector_index" && secondary_index::vector_index::has_vector_index_on_column(*schema, targets[0]->column_name());
    bool custom_index_with_same_name = _idx_properties->custom_class && db.existing_index_names(keyspace()).contains(_index_name);
    if (existing_vector_index || custom_index_with_same_name) {
        if (_if_not_exists) {
            return {};
        } else {
            throw exceptions::invalid_request_exception("There exists a duplicate custom index");
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

future<std::tuple<::shared_ptr<cql_transport::event::schema_change>, utils::chunked_vector<mutation>, cql3::cql_warnings_vec>>
create_index_statement::prepare_schema_mutations(query_processor& qp, const query_options&, api::timestamp_type ts) const {
    using namespace cql_transport;
    auto res = build_index_schema(qp.db());

    ::shared_ptr<event::schema_change> ret;
    utils::chunked_vector<mutation> muts;

    if (res) {
        const replica::database& db = qp.proxy().local_db();
        const auto& cf = db.find_column_family(keyspace(), column_family());

        // Produce statements to update schema tables with index-specific information.
        muts = co_await service::prepare_column_family_update_announcement(qp.proxy(), std::move(res->schema), {}, ts);

        // Produce the underlying view for the index.
        if (db::schema_tables::view_should_exist(res->index)) {
            view_ptr view = create_view_for_index(cf.schema(), res->index, qp.db());
            utils::chunked_vector<mutation> view_muts = co_await service::prepare_new_view_announcement(qp.proxy(), std::move(view), ts);

            muts.reserve(muts.size() + view_muts.size());
            for (mutation& view_mutation : view_muts) {
                muts.push_back(std::move(view_mutation));
            }
        }

        ret = ::make_shared<event::schema_change>(
                event::schema_change::change_type::UPDATED,
                event::schema_change::target_type::TABLE,
                keyspace(),
                column_family());
    }

    co_return std::make_tuple(std::move(ret), std::move(muts), std::vector<sstring>());
}

std::unique_ptr<cql3::statements::prepared_statement>
create_index_statement::prepare(data_dictionary::database db, cql_stats& stats) {
    _cql_stats = &stats;
    return std::make_unique<prepared_statement>(audit_info(), make_shared<create_index_statement>(*this));
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
