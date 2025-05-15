/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once
#include "schema.hh"

// Declares a set of internal columns that can be injected into a user table
// via the mm.inject_internal_columns method.
struct internal_column_set {
    const bytes prefix;
    const std::vector<schema::column> regular_columns;
    const std::vector<schema::column> static_columns;

    internal_column_set(bytes prefix_arg,
        std::vector<schema::column> regular_columns_arg,
        std::vector<schema::column> static_columns_arg);
};
