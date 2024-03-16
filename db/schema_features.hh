/*
 * Copyright (C) 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "enum_set.hh"

namespace db {

enum class schema_feature {
    // When set, the schema digest is calculated in a way such that it doesn't change after all
    // tombstones in an empty partition expire.
    // See https://github.com/scylladb/scylla/issues/4485
    DIGEST_INSENSITIVE_TO_EXPIRY,
    COMPUTED_COLUMNS,
    SCYLLA_KEYSPACES,
    SCYLLA_AGGREGATES,

    // When enabled, schema_mutations::digest() will skip empty mutations (with only tombstones),
    // so that the digest remains the same after schema tables are compacted.
    TABLE_DIGEST_INSENSITIVE_TO_EXPIRY,

    // When enabled we'll add a new column to the `system_schema.scylla_tables` table.
    GROUP0_SCHEMA_VERSIONING,
};

using schema_features = enum_set<super_enum<schema_feature,
    schema_feature::DIGEST_INSENSITIVE_TO_EXPIRY,
    schema_feature::COMPUTED_COLUMNS,
    schema_feature::SCYLLA_KEYSPACES,
    schema_feature::SCYLLA_AGGREGATES,
    schema_feature::TABLE_DIGEST_INSENSITIVE_TO_EXPIRY,
    schema_feature::GROUP0_SCHEMA_VERSIONING
    >>;

}
