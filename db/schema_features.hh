/*
 * Copyright (C) 2019-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
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
};

using schema_features = enum_set<super_enum<schema_feature,
    schema_feature::VIEW_VIRTUAL_COLUMNS,
    schema_feature::DIGEST_INSENSITIVE_TO_EXPIRY,
    schema_feature::COMPUTED_COLUMNS,
    schema_feature::CDC_OPTIONS,
    schema_feature::PER_TABLE_PARTITIONERS
    >>;

}
