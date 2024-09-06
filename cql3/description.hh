/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <seastar/core/sstring.hh>

#include "bytes_fwd.hh"

#include <optional>
#include <vector>

using namespace seastar;

namespace cql3 {

/// Type representing an entity that can be restored by performing
/// a SINGLE CQL query. It can correspond to a tangible object such as
/// a keyspace, a table, or a role, as well as to a more abstract concept
/// like a role grant.
///
/// Instances of this type correspond to the output of `DESCRIBE` statements.
///
/// ! Important note: !
/// -------------------
/// `description` will wrap `keyspace` and `name` in double quotation marks
/// before serializing them if it's necessary. However, it does NOT apply
/// to `type`. That field is assumed not to need it. If the user uses the interface
/// in a context where that may be needed, they should format `name` on their own.
/// -------------------
///
/// See scylladb/scylladb#11106 and scylladb/scylladb#18750 for more context.
struct description {
    /// The name of the keyspace the entity belongs to.
    /// Empty optional if and only if the entity does not belong to any keyspace.
    std::optional<sstring> keyspace;
    /// The name of the type of an entity, e.g. a role may be of type: role
    sstring type;
    /// The name of the entity itself, e.g. a keyspace of name `ks` will be of name: ks
    sstring name;
    /// CQL statement that can be used to restore the entity.
    std::optional<sstring> create_statement;

    /// Serialize the description to represent multiple UTF-8 columns.
    /// The number of columns will be equal to 4 unless `serialize_create_statement`
    /// is `false`, in which case that number will be equal to 3.
    ///
    /// If `keyspace.has_value() == false`, a `null` will be printed in its place.
    ///
    /// Precondition: if `serialize_create_statement` is true, then `create_statement.has_value()`
    ///               is also true.
    std::vector<bytes_opt> serialize(bool serialize_create_statement = true) const;
};

} // namespace cql3
