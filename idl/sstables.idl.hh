/*
 * Copyright 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */


namespace sstables {

enum class sstable_state : uint8_t {
    normal,
    staging,
    quarantine,
    upload,
};

}
