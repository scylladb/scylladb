/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <cstdint>

struct tombstone_purge_stats {
    int64_t attempts { 0 };
    int64_t failures_due_to_overlapping_with_memtable { 0 };
    int64_t failures_due_to_overlapping_with_uncompacting_sstable { 0 };
    int64_t failures_other { 0 };

    tombstone_purge_stats& operator+=(const tombstone_purge_stats& other) {
        attempts += other.attempts;
        failures_due_to_overlapping_with_memtable += other.failures_due_to_overlapping_with_memtable;
        failures_due_to_overlapping_with_uncompacting_sstable += other.failures_due_to_overlapping_with_uncompacting_sstable;

        return *this;
    }
};

inline tombstone_purge_stats operator+(const tombstone_purge_stats& left, const tombstone_purge_stats& right) {
    auto tmp = left;
    tmp += right;
    return tmp;
}
