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
    VIEW_VIRTUAL_COLUMNS,

    // When set, the schema digest is calcualted in a way such that it doesn't change after all
    // tombstones in an empty partition expire.
    // See https://github.com/scylladb/scylla/issues/4485
    DIGEST_INSENSITIVE_TO_EXPIRY,
    COMPUTED_COLUMNS,
    CDC_OPTIONS,
    PER_TABLE_PARTITIONERS,
    SCYLLA_KEYSPACES,
    SCYLLA_AGGREGATES,
};

using schema_features = enum_set<super_enum<schema_feature,
    schema_feature::VIEW_VIRTUAL_COLUMNS,
    schema_feature::DIGEST_INSENSITIVE_TO_EXPIRY,
    schema_feature::COMPUTED_COLUMNS,
    schema_feature::CDC_OPTIONS,
    schema_feature::PER_TABLE_PARTITIONERS,
    schema_feature::SCYLLA_KEYSPACES,
    schema_feature::SCYLLA_AGGREGATES
    >>;

}
