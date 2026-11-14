/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "mutation/position_in_partition.hh"
#include "utils/chunked_vector.hh"

#include <optional>

#include <seastar/core/future.hh>

// Assumes that the bounds of `r` are of 'clustered' type
// and that `r` is non-empty (the left bound is smaller than the right bound).
//
// If `r` does not contain any keys, returns nullopt.
std::optional<query::clustering_range> position_range_to_clustering_range(const position_range& r, const schema&);

using position_range_vector = utils::chunked_vector<position_range>;

// Returns the intersection of a and b, or nullopt if it is empty.
std::optional<position_range> position_range_intersection(const schema& s, const position_range& a, const position_range& b);

// Sorts the ranges by start.
// Not preemptible, meant for small inputs. Large inputs should be built in order.
void sort_position_ranges(const schema& s, position_range_vector& ranges);

// Returns the ranges with empty ranges dropped and overlapping or adjacent ranges merged.
// The ranges must be sorted by start (see sort_position_ranges()), unsorted input
// is reported as an internal error. Preemptible.
seastar::future<position_range_vector> deoverlap_position_ranges(const schema& s, position_range_vector ranges);

// Merges two lists of ranges, each of them sorted by start and non-overlapping
// (as returned by deoverlap_position_ranges()), into one such list.
// Preemptible.
seastar::future<position_range_vector> merge_position_ranges(const schema& s, position_range_vector a, position_range_vector b);
