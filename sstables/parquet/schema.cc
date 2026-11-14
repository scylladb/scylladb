/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "sstables/parquet/schema.hh"

#include <algorithm>
#include <limits>
#include <stdexcept>

#include <seastar/core/format.hh>
#include <seastar/core/on_internal_error.hh>

#include "utils/log.hh"

namespace sstables::parquet {

static logging::logger schema_logger("parquet_schema");

schema::schema(sstring root_name) {
    _nodes.push_back(node{.name = std::move(root_name)});
}

node_id schema::add_node(node_id parent, sstring name, repetition_type rep) {
    if (name.size() > max_name_size) {
        throw std::invalid_argument(seastar::format("Parquet schema: a name of {} bytes is longer than {}", name.size(), max_name_size));
    }
    if (parent >= _nodes.size() || _nodes[parent].column) {
        on_internal_error(schema_logger, seastar::format("Parquet schema: parent {} of {} is not a group", parent, name));
    }
    for (node_id sibling : _nodes[parent].children) {
        if (_nodes[sibling].name == name) {
            on_internal_error(schema_logger, seastar::format("Parquet schema: duplicate name {}", name));
        }
    }
    const auto& p = _nodes[parent];
    node n{
        .name = std::move(name),
        .repetition = rep,
        .parent = parent,
        .max_def = uint16_t(p.max_def + (rep != repetition_type::required)),
        .max_rep = uint16_t(p.max_rep + (rep == repetition_type::repeated)),
    };
    // Level bit widths and the level types of other readers (int16) limit the depth.
    if (n.max_def > std::numeric_limits<int16_t>::max()) {
        on_internal_error(schema_logger, "Parquet schema: nested too deeply");
    }
    const node_id id = _nodes.size();
    _nodes.push_back(std::move(n));
    _nodes[parent].children.push_back(id);
    return id;
}

node_id schema::add_group(node_id parent, sstring name, repetition_type rep,
        std::optional<converted_type> converted, std::optional<logical_type> logical) {
    const node_id id = add_node(parent, std::move(name), rep);
    _nodes[id].converted = converted;
    _nodes[id].logical = logical;
    return id;
}

column_id schema::add_column(node_id parent, sstring name, repetition_type rep, physical_type type, column_options opts) {
    if ((type == physical_type::fixed_len_byte_array) != opts.type_length.has_value()
            || (opts.type_length && *opts.type_length <= 0)) {
        on_internal_error(schema_logger, seastar::format("Parquet schema: column {} has a bad type length", name));
    }
    const node_id id = add_node(parent, std::move(name), rep);
    auto& n = _nodes[id];
    n.converted = opts.converted;
    n.logical = opts.logical;
    const column_id c = _columns.size();
    n.column = c;
    _columns.push_back(column{
        .node = id,
        .type = type,
        .type_length = opts.type_length,
        .max_def = n.max_def,
        .max_rep = n.max_rep,
        .always_present = opts.always_present,
    });
    return c;
}

column_id schema::add_list(node_id parent, sstring name, physical_type element_type, column_options element_opts,
        repetition_type element_rep, repetition_type list_rep) {
    const node_id outer = add_group(parent, std::move(name), list_rep, converted_type::list, logical_type{.k = logical_type::kind::list});
    const node_id middle = add_group(outer, "list", repetition_type::repeated);
    return add_column(middle, "element", element_rep, element_type, std::move(element_opts));
}

schema::map_columns schema::add_map(node_id parent, sstring name, physical_type key_type, column_options key_opts,
        physical_type value_type, column_options value_opts, repetition_type value_rep, repetition_type map_rep) {
    const node_id outer = add_group(parent, std::move(name), map_rep, converted_type::map, logical_type{.k = logical_type::kind::map});
    const node_id middle = add_group(outer, "key_value", repetition_type::repeated);
    const column_id key = add_column(middle, "key", repetition_type::required, key_type, std::move(key_opts));
    const column_id value = add_column(middle, "value", value_rep, value_type, std::move(value_opts));
    return {key, value};
}

std::vector<sstring> schema::path(column_id c) const {
    std::vector<sstring> r;
    for (node_id n = get_column(c).node; n != root; n = _nodes[n].parent) {
        r.push_back(_nodes[n].name);
    }
    std::ranges::reverse(r);
    return r;
}

utils::chunked_vector<column_id> schema::columns_in_schema_order() const {
    utils::chunked_vector<column_id> r;
    r.reserve(_columns.size());
    std::vector<node_id> stack{root};
    while (!stack.empty()) {
        const auto& n = _nodes[stack.back()];
        stack.pop_back();
        if (n.column) {
            r.push_back(*n.column);
        }
        stack.insert(stack.end(), n.children.rbegin(), n.children.rend());
    }
    return r;
}

utils::chunked_vector<schema_element> schema::to_elements(const std::vector<bool>& included) const {
    // The number of included children of each node. Children are declared
    // after their parent, so a reverse pass sees every child first.
    utils::chunked_vector<int32_t> kept(_nodes.size());
    for (node_id id = _nodes.size(); id-- > 1;) {
        const auto& n = _nodes[id];
        if (n.column ? bool(included.at(*n.column)) : kept[id] > 0) {
            ++kept[n.parent];
        }
    }
    utils::chunked_vector<schema_element> r;
    std::vector<node_id> stack{root};
    while (!stack.empty()) {
        const node_id id = stack.back();
        stack.pop_back();
        const auto& n = _nodes[id];
        schema_element e{
            .repetition = n.repetition,
            .name = n.name,
            .converted = n.converted,
            .logical = n.logical,
        };
        if (n.column) {
            const auto& c = _columns[*n.column];
            e.type = c.type;
            e.type_length = c.type_length;
        } else {
            e.num_children = kept[id];
        }
        r.push_back(std::move(e));
        for (auto it = n.children.rbegin(); it != n.children.rend(); ++it) {
            const auto& child = _nodes[*it];
            if (child.column ? bool(included.at(*child.column)) : kept[*it] > 0) {
                stack.push_back(*it);
            }
        }
    }
    return r;
}

} // namespace sstables::parquet
