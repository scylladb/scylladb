/*
 * Copyright (C) 2017-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.0 and Apache-2.0)
 */

#include <functional>
#include <optional>
#include <ranges>
#include <seastar/core/shared_ptr.hh>
#include <string_view>
#include <unordered_map>

#include "index/secondary_index_manager.hh"
#include "index/secondary_index.hh"
#include "index/vector_index.hh"

#include "cql3/expr/expression.hh"
#include "index/target_parser.hh"
#include "schema/schema.hh"
#include "schema/schema_builder.hh"
#include "db/view/view.hh"
#include "types/concrete_types.hh"
#include "db/tags/extension.hh"
#include "utils/histogram_metrics_helper.hh"

seastar::metrics::label idx{"idx"};
seastar::metrics::label ks{"ks"};

namespace secondary_index {

index::index(const sstring& target_column, const index_metadata& im)
    : _im{im}
    , _target_type{cql3::statements::index_target::from_target_string(target_column)}
    , _target_column{cql3::statements::index_target::column_name_from_target_string(target_column)}
{}

bool index::depends_on(const column_definition& cdef) const {
    return cdef.name_as_text() == _target_column;
}

index::supports_expression_v index::supports_expression(const column_definition& cdef, const cql3::expr::oper_t op) const {
    using target_type = cql3::statements::index_target::target_type;
    auto collection_yes = supports_expression_v::from_bool_collection(true);
    if (cdef.name_as_text() != _target_column) {
        return supports_expression_v::from_bool(false);
    }

    switch (op) {
        case cql3::expr::oper_t::EQ:
            return supports_expression_v::from_bool(_target_type == target_type::regular_values); 
        case cql3::expr::oper_t::CONTAINS:
            if (cdef.type->is_set() && _target_type == target_type::keys) {
                return collection_yes;
            }
            if (cdef.type->is_list() && _target_type == target_type::collection_values) {
                return collection_yes;
            }
            if (cdef.type->is_map() && _target_type == target_type::collection_values) {
                return collection_yes;
            }
            return supports_expression_v::from_bool(false);
        case cql3::expr::oper_t::CONTAINS_KEY:
            if (cdef.type->is_map() && _target_type == target_type::keys) {
                return collection_yes;
            }
            return supports_expression_v::from_bool(false);
        case cql3::expr::oper_t::IN:
            return supports_expression_v::from_bool(_target_type == target_type::regular_values); 
        default:
            return supports_expression_v::from_bool(false);
    }
}

index::supports_expression_v index::supports_subscript_expression(const column_definition& cdef, const cql3::expr::oper_t op) const {
    using target_type = cql3::statements::index_target::target_type;
    if (cdef.name_as_text() != _target_column) {
        return supports_expression_v::from_bool(false);
    }

    return supports_expression_v::from_bool_collection(op == cql3::expr::oper_t::EQ && _target_type == target_type::keys_and_values);
}

const index_metadata& index::metadata() const {
    return _im;
}

secondary_index_manager::secondary_index_manager(data_dictionary::table cf)
    : _cf{cf}
{}

void secondary_index_manager::reload() {
    const auto& table_indices = _cf.schema()->all_indices();
    auto it = _indices.begin();
    while (it != _indices.end()) {
        auto index_name = it->first;
        if (!table_indices.contains(index_name)) {
            it = _indices.erase(it);
            _metrics.erase(index_name);
        } else {
            ++it;
        }
    }
    for (const auto& index : _cf.schema()->all_indices()) {
        add_index(index.second);
    }
}

void secondary_index_manager::add_index(const index_metadata& im) {
    sstring index_target = im.options().at(cql3::statements::index_target::target_option_name);
    sstring index_target_name = target_parser::get_target_column_name_from_string(index_target);
    _indices.emplace(im.name(), index{index_target_name, im});
    if (!_metrics.contains(im.name())) {
        _metrics.emplace(im.name(), make_lw_shared<stats>(_cf.schema()->ks_name(), im.name()));
    }
}

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


sstring index_table_name(const sstring& index_name) {
    return format("{}_index", index_name);
}

sstring index_name_from_table_name(const sstring& table_name) {
    if (table_name.size() < 7 || !table_name.ends_with("_index")) {
        throw std::runtime_error(format("Table {} does not have _index suffix", table_name));
    }
    return table_name.substr(0, table_name.size() - 6); // remove the _index suffix from an index name;
}

std::set<sstring>
existing_index_names(const std::vector<schema_ptr>& tables, std::string_view cf_to_exclude) {
    std::set<sstring> names;
    for (auto& schema : tables) {
        if (!cf_to_exclude.empty() && schema->cf_name() == cf_to_exclude) {
            continue;
        }
        for (const auto& index_name : schema->index_names()) {
            names.emplace(index_name);
        }
    }
    return names;
}

sstring get_available_index_name(
        std::string_view ks_name,
        std::string_view cf_name,
        std::optional<sstring> index_name_root,
        const std::set<sstring>& existing_names,
        std::function<bool(std::string_view, std::string_view)> has_schema) {
    auto base_name = index_metadata::get_default_index_name(sstring(cf_name), index_name_root);
    sstring accepted_name = base_name;
    int i = 0;
    auto name_accepted = [&] {
        auto index_table_name = secondary_index::index_table_name(accepted_name);
        return !has_schema(ks_name, index_table_name) && !existing_names.contains(accepted_name);
    };
    while (!name_accepted()) {
        accepted_name = base_name + "_" + std::to_string(++i);
    }
    return accepted_name;
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

view_ptr secondary_index_manager::create_view_for_index(const index_metadata& im) const {
    auto schema = _cf.schema();
    sstring index_target_name = im.options().at(cql3::statements::index_target::target_option_name);
    schema_builder builder{schema->ks_name(), index_table_name(im.name())};
    auto target_info = target_parser::parse(schema, im);
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
    // A local secondary index should be backed by a *synchronous* view,
    // see #16371. A view is marked synchronous with a tag. Non-local indexes
    // do not need the tags schema extension at all.
    if (im.local()) {
        std::map<sstring, sstring> tags_map = {{db::SYNCHRONOUS_VIEW_UPDATES_TAG_KEY, "true"}};
        builder.add_extension(db::tags_extension::NAME, ::make_shared<db::tags_extension>(tags_map));
    }
    return view_ptr{builder.build()};
}

std::vector<index_metadata> secondary_index_manager::get_dependent_indices(const column_definition& cdef) const {
    return _indices
           | std::views::values
           | std::views::filter([&] (auto& index) { return index.depends_on(cdef); })
           | std::views::transform([&] (auto& index) { return index.metadata(); })
           | std::ranges::to<std::vector>();
}

std::vector<index> secondary_index_manager::list_indexes() const {
    return _indices | std::views::values | std::ranges::to<std::vector>();
}

bool secondary_index_manager::is_index(view_ptr view) const {
    return is_index(*view);
}

bool secondary_index_manager::is_index(const schema& s) const {
    return std::ranges::any_of(_indices | std::views::values, [&s] (const index& i) {
        return s.cf_name() == index_table_name(i.metadata().name());
    });
}

bool secondary_index_manager::is_global_index(const schema& s) const {
    return std::ranges::any_of(_indices | std::views::values, [&s] (const index& i) {
        return !i.metadata().local() && s.cf_name() == index_table_name(i.metadata().name());
    });
}

std::optional<sstring> secondary_index_manager::custom_index_class(const schema& s) const {

    auto idx = _indices.find(index_name_from_table_name(s.cf_name()));

    if (idx == _indices.end() || !(*idx).second.metadata().options().contains(db::index::secondary_index::custom_class_option_name)) {
        return std::nullopt;
    } else {
        return (*idx).second.metadata().options().at(db::index::secondary_index::custom_class_option_name);
    }
}

// This function returns a factory, as the custom index class should be lightweight, preferably not holding any state.
// We prefer this over a static custom index class instance, as it allows us to avoid any issues with thread safety.
std::optional<std::function<std::unique_ptr<custom_index>()>> secondary_index_manager::get_custom_class_factory(const sstring& class_name) {
    sstring lower_class_name = class_name;
    std::transform(lower_class_name.begin(), lower_class_name.end(), lower_class_name.begin(), ::tolower);

    const static std::unordered_map<std::string_view, std::function<std::unique_ptr<custom_index>()>> classes = {
        {"vector_index", vector_index_factory},
    };

    if (auto class_it = classes.find(lower_class_name); class_it != classes.end()) {
        return class_it->second;
    } else {
        return std::nullopt;
    }
}

std::optional<std::unique_ptr<custom_index>> secondary_index_manager::get_custom_class(const index_metadata& im) {
    auto it = im.options().find(db::index::secondary_index::custom_class_option_name);
    if (it == im.options().end()) {
        return std::nullopt;
    }
    auto custom_class_factory = secondary_index::secondary_index_manager::get_custom_class_factory(it->second);
    if (!custom_class_factory) {
        return std::nullopt;
    }
    return (*custom_class_factory)();
}

stats::stats(const sstring& ks_name, const sstring& index_name) {
    metrics.add_group("index",
            {seastar::metrics::make_histogram("query_latencies", seastar::metrics::description("Index query latencies"), {idx(index_name), ks(ks_name)},
                    [this]() {
                        return to_metrics_histogram(query_latency);
                    })
                            .aggregate({seastar::metrics::shard_label})
                            .set_skip_when_empty()});
}

void stats::add_latency(std::chrono::steady_clock::duration d) {
    query_latency.add(d);
}

}
