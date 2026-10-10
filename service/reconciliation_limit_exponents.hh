/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <algorithm>
#include <cstdint>
#include <optional>

namespace service {

// The exponents of the limits which the rounds of a reconciliation send to
// the replicas, see frontier_reconciliation.
struct reconciliation_limit_exponents {
    // Of the per-partition row limit.
    uint8_t partition_row_limit = 0;
    // Of the row limit.
    uint8_t row_limit = 0;

    bool operator==(const reconciliation_limit_exponents&) const = default;
};

// The exponents which the reconciliations of a page suggest for the next
// round or page. Each is nullopt if no round told anything about it.
struct suggested_reconciliation_limit_exponents {
    std::optional<uint8_t> partition_row_limit;
    std::optional<uint8_t> row_limit;

    // Adds the suggestions of another read of the same page. The larger
    // suggestion wins, because a limit which is too small costs a round or a
    // page, and one which is too large costs only a part of one. An engaged
    // optional compares greater than nullopt.
    void merge(const suggested_reconciliation_limit_exponents& other) {
        partition_row_limit = std::max(partition_row_limit, other.partition_row_limit);
        row_limit = std::max(row_limit, other.row_limit);
    }

    // `current` with the suggested exponents in place of its own.
    reconciliation_limit_exponents applied_to(reconciliation_limit_exponents current) const {
        return {partition_row_limit.value_or(current.partition_row_limit), row_limit.value_or(current.row_limit)};
    }
};

} // namespace service
