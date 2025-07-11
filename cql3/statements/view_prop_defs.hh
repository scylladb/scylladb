/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "cql3/statements/cf_properties.hh"

namespace cql3::statements {

/// This type represents the possible properties of the following CQL statements:
///
/// * CREATE MATERIALIZED VIEW,
/// * ALTER MATERIALIZED VIEW.
///
/// Since the sets of the valid properties may differ between those statements, this type
/// is supposed to represent an overset of them.
///
/// This type does NOT guarantee that all of the necessary validation logic will be performed
/// by it. It strives to do that, but you should keep this in mind.
class view_prop_defs : public cf_properties {
public:
    /// The type of a schema operation on a materialized view.
    /// These values will be used to guide the validation logic.
    enum class op_type {
        create,
        alter
    };

public:
    template <typename... Args>
    view_prop_defs(Args&&... args) : cf_properties(std::forward<Args>(args)...) {}

    // Explicitly delete this method. It's declared in the inherited types.
    // The user of this interface should use `validate_raw` instead.
    void validate(const data_dictionary::database, sstring ks_name, const schema::extensions_map&) const = delete;

    /// Validate the properties for the specified schema operation.
    ///
    /// The validation is *raw*, meaning that it boils down to simple checks like the presence
    /// or absence of a given property. Compare it with `apply_to_builder` declared for this type.
    void validate_raw(op_type, const data_dictionary::database, sstring ks_name, const schema::extensions_map&) const;

    /// Apply the properties to the provided schema_builder and validate them.
    ///
    /// NOTE: If the validation fails, this function will throw an exception. What's more important,
    ///       however, is that the provided schema_builder might have already been modified by that
    ///       point. Because of that, in presence of an exception, the schema builder should NOT be
    ///       used anymore.
    void apply_to_builder(op_type, schema_builder&, schema::extensions_map, const data_dictionary::database, sstring ks_name) const;
};

} // namespace cql3::statements
