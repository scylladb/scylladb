/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <optional>
#include "bytes_fwd.hh"
#include "schema/schema_fwd.hh"

namespace db {

namespace view {

// Part of the view description which depends on the base schema.
struct base_dependent_view_info {
    bool has_computed_column_depending_on_base_non_primary_key;

    // True if the partition key columns of the view are the same as the
    // partition key columns of the base, maybe in a different order.
    bool is_partition_key_permutation_of_base_partition_key;

    // Indicates if the view hase pk columns which are not part of the base
    // pk, it seems that !base_non_pk_columns_in_view_pk.empty() is the same,
    // but actually there are cases where we can compute this boolean without
    // succeeding to reliably build the former.
    bool has_base_non_pk_columns_in_view_pk;


    // A constructor for a base info that can facilitate reads and writes from the materialized view.
    base_dependent_view_info(bool has_computed_column_depending_on_base_non_primary_key,
            bool is_partition_key_permutation_of_base_partition_key,
            bool has_base_non_pk_columns_in_view_pk);
};

// Immutable snapshot of view's base-schema-dependent part.
using base_info_ptr = lw_shared_ptr<const base_dependent_view_info>;

}

}