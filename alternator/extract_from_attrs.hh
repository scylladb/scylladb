/*
 * Copyright 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <string>
#include <string_view>

#include "utils/rjson.hh"
#include "serialization.hh"
#include "column_computation.hh"
#include "db/view/regular_column_transformation.hh"

namespace alternator {

// An implementation of a "column_computation" which extracts a specific
// non-key attribute from the big map (":attrs") of all non-key attributes,
// and deserializes it if it has the desired type. GSI will use this computed
// column as a materialized-view key when the view key attribute isn't a
// full-fledged CQL column but rather stored in ":attrs".
class extract_from_attrs_column_computation : public regular_column_transformation {
    // The name of the CQL column name holding the attribute map. It is a
    // constant defined in executor.cc (as ":attrs"), so doesn't need
    // to be specified when constructing the column computation.
    static const bytes MAP_NAME;
    // The top-level attribute name to extract from the ":attrs" map.
    bytes _attr_name;
    // The type we expect for the value stored in the attribute. If the type
    // matches the expected type, it is decoded from the serialized format
    // we store in the map's values) into the raw CQL type value that we use
    // for keys, and returned by compute_value(). Only the types "S" (string),
    // "B" (bytes) and "N" (number) are allowed as keys in DynamoDB, and
    // therefore in desired_type.
    alternator_type _desired_type;
public:
    virtual column_computation_ptr clone() const override;
    // TYPE_NAME is a unique string that distinguishes this class from other
    // column_computation subclasses. column_computation::deserialize() will
    // construct an object of this subclass if it sees a "type" TYPE_NAME.
    static inline const std::string TYPE_NAME = "alternator_extract_from_attrs";
    // Serialize the *definition* of this column computation into a JSON
    // string with a unique "type" string - TYPE_NAME - which then causes
    // column_computation::deserialize() to create an object from this class.
    virtual bytes serialize() const override;
    // Construct this object based on the previous output of serialize().
    // Calls on_internal_error() if the string doesn't match the output format
    // of serialize(). "type" is not checked column_computation::deserialize()
    // won't call this constructor if "type" doesn't match.
    extract_from_attrs_column_computation(const rjson::value &v);
    extract_from_attrs_column_computation(bytes_view attr_name, alternator_type desired_type)
        : _attr_name(attr_name), _desired_type(desired_type)
        {}
    // Implement regular_column_transformation's compute_value() that
    // accepts the full row:
    result compute_value(const schema& schema, const partition_key& key,
        const db::view::clustering_or_static_row& row) const override;
    // But do not implement column_computation's compute_value() that
    // accepts only a partition key - that's not enough so our implementation
    // of this function does on_internal_error().
    bytes compute_value(const schema& schema, const partition_key& key) const override;
    // This computed column does depend on a non-primary key column, so
    // its result may change in the update and we need to compute it
    // before and after the update.
    virtual bool depends_on_non_primary_key_column() const override {
        return true;
    }
};
} // namespace alternator
