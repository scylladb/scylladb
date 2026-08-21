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

// Arm positions are wire positions: arms may only be appended, and the inert one
// has to stay first. A reader hands back the first arm for an arm index it does
// not know, so a plan kind added by a later version reads back here as a plan
// this version cannot keep, rather than as a scan it would resume. Neither arm
// is final, so both can still gain fields.
//
// Falling back that way leaves the unknown arm's payload unread: an arm carries
// no length of its own, so there is nothing to skip it by. get_query_plan() has
// to stay the last field of paging_state for that to be harmless - it is read
// from a size-delimited substream, which discards whatever is left. A field
// appended after it would be read out of the middle of an arm instead.
//
// std::monostate is therefore never written. It differs from a disengaged
// std::optional<query_plan>, which says no plan was recorded at all - by a
// version predating the field, or by a producer with no CQL plan to keep - and
// then nothing is pinned.
using query_plan = std::variant<std::monostate, primary_index_plan, index_plan>;

}

}
