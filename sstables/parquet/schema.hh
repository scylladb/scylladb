/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

// The schema tree that the file writer works with.
//
// A caller declares groups and leaf columns. The file's own schema, the
// list of schema elements in the footer, is a subset of this tree: the
// writer leaves out a leaf that is null in the whole file, unless it is
// declared always present, and a group that has no leaf left. See
// schema::to_elements().
//
// Nesting follows the Parquet rules (the Dremel encoding):
//
// - A node that is optional or repeated adds one definition level.
//   A required node adds none. A group that is always there, such as the
//   reserved `_scylla` group or a non-frozen UDT, is a required group.
// - A repeated node adds one repetition level.
// - The definition level of an entry is the number of optional or repeated
//   nodes on the path from the root that are present. A leaf value is
//   present exactly when the level equals the leaf's maximum.
// - The repetition level of an entry is 0 for the first entry of a row.
//   Otherwise it is the level of the repeated node that repeats.
//
// LIST and MAP have the three-level shapes of LogicalTypes.md, which
// add_list() and add_map() build.

#include <cstdint>
#include <optional>
#include <vector>

#include <seastar/core/sstring.hh>

#include "sstables/parquet/metadata.hh"
#include "utils/chunked_vector.hh"
#include "seastarx.hh"

namespace sstables::parquet {

/// A node of the schema tree. The root is node 0.
using node_id = uint32_t;
/// A leaf column, numbered in the order of declaration from 0.
/// It is not the position in the file's schema.
using column_id = uint32_t;

/// The longest name of a node, in bytes: the longest string that the reader
/// accepts in the footer.
inline constexpr size_t max_name_size = thrift::decoder::max_string_size;

/// The levels of one entry of a leaf column.
struct levels {
    uint16_t rep = 0;
    uint16_t def = 0;
};

/// The annotations and properties of a leaf column.
struct column_options {
    /// The length of a FIXED_LEN_BYTE_ARRAY value. Required for that type
    /// and forbidden for the others.
    std::optional<int32_t> type_length;
    std::optional<converted_type> converted;
    std::optional<logical_type> logical;
    /// Keep the column in the file's schema even if it is null in the whole
    /// file. Key columns need this, because the reader matches them by position.
    bool always_present = false;
};

class schema {
public:
    struct node {
        sstring name;
        /// Unset for the root only.
        std::optional<repetition_type> repetition;
        std::optional<converted_type> converted;
        std::optional<logical_type> logical;
        node_id parent = 0;
        /// The children of a group, in declaration order.
        std::vector<node_id> children;
        /// The maximum definition and repetition levels of the node.
        uint16_t max_def = 0;
        uint16_t max_rep = 0;
        /// Set for leaves only.
        std::optional<column_id> column;
    };

    struct column {
        node_id node;
        physical_type type;
        std::optional<int32_t> type_length;
        uint16_t max_def;
        uint16_t max_rep;
        bool always_present;
    };

    /// The column ids of a map's key and value leaves.
    struct map_columns {
        column_id key;
        column_id value;
    };

private:
    utils::chunked_vector<node> _nodes;
    utils::chunked_vector<column> _columns;

    node_id add_node(node_id parent, sstring name, repetition_type rep);
public:
    explicit schema(sstring root_name = "schema");

    static constexpr node_id root = 0;

    /// Adds a group as the last child of `parent`, which must be a group.
    /// Sibling names must differ. A name longer than max_name_size is
    /// rejected with std::invalid_argument, here and in the other add_*()
    /// methods.
    node_id add_group(node_id parent, sstring name, repetition_type rep,
            std::optional<converted_type> converted = std::nullopt, std::optional<logical_type> logical = std::nullopt);

    /// Adds a leaf column as the last child of `parent`.
    column_id add_column(node_id parent, sstring name, repetition_type rep, physical_type type, column_options opts = {});

    /// Adds a LIST of `name`, and returns the element leaf:
    ///
    ///     <list_rep> group <name> (LIST) {
    ///       repeated group list {
    ///         <element_rep> <element_type> element;
    ///       }
    ///     }
    ///
    /// With the defaults (an optional list of optional elements), the
    /// element's levels are: def 0 for a null list, 1 for an empty list,
    /// 2 for a null element, 3 for a value; rep 0 for the first element of
    /// a row, 1 for the others.
    column_id add_list(node_id parent, sstring name, physical_type element_type, column_options element_opts = {},
            repetition_type element_rep = repetition_type::optional, repetition_type list_rep = repetition_type::optional);

    /// Adds a MAP of `name`, and returns its leaves:
    ///
    ///     <map_rep> group <name> (MAP) {
    ///       repeated group key_value {
    ///         required <key_type> key;
    ///         <value_rep> <value_type> value;
    ///       }
    ///     }
    ///
    /// With the defaults, the key's levels are: def 0 for a null map, 1 for
    /// an empty map, 2 for a key. The value's are the same, plus 3 for a
    /// value that is present.
    map_columns add_map(node_id parent, sstring name, physical_type key_type, column_options key_opts,
            physical_type value_type, column_options value_opts = {},
            repetition_type value_rep = repetition_type::optional, repetition_type map_rep = repetition_type::optional);

    const utils::chunked_vector<node>& nodes() const noexcept { return _nodes; }
    const node& get_node(node_id n) const { return _nodes.at(n); }
    size_t columns() const noexcept { return _columns.size(); }
    const column& get_column(column_id c) const { return _columns.at(c); }

    /// The names from the root's child down to the leaf.
    std::vector<sstring> path(column_id c) const;

    /// All columns in schema order: the depth-first order of the tree.
    /// This is the order of column chunks in a row group.
    utils::chunked_vector<column_id> columns_in_schema_order() const;

    /// The schema elements of the file, which hold the columns `c` for which
    /// `included[c]` is true, and their ancestors. A group with no included
    /// leaf is left out. The root is always there.
    utils::chunked_vector<schema_element> to_elements(const std::vector<bool>& included) const;
};

} // namespace sstables::parquet
