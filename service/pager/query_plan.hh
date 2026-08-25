/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <variant>

#include "schema/schema_fwd.hh"

namespace service {

namespace pager {

// The plan a paged read scans, recorded in the paging state so that a later
// page keeps reading whatever the saved position belongs to. See #18992.

// The base table, read in primary index order.
struct primary_index_plan {
    bool operator==(const primary_index_plan&) const = default;
};

// A secondary index, named by the view backing it so that an index re-created
// under the same name is not mistaken for the original.
struct index_plan {
    table_id view_id;
    bool operator==(const index_plan&) const = default;
};

// Arm positions are wire positions: arms may only be appended, and a new arm
// must not be written until every coordinator in the cluster can read it - gate
// it on a cluster feature.
//
// A disengaged std::optional<query_plan> is different: it says no plan was
// recorded at all - by a version predating the field, or by a producer with no
// CQL plan to keep - and then nothing is pinned.
using query_plan = std::variant<primary_index_plan, index_plan>;

}

}
