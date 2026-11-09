/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "mutation/position_in_partition.hh"

#include <optional>

// Assumes that the bounds of `r` are of 'clustered' type
// and that `r` is non-empty (the left bound is smaller than the right bound).
//
// If `r` does not contain any keys, returns nullopt.
std::optional<query::clustering_range> position_range_to_clustering_range(const position_range& r, const schema&);
