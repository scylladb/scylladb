/*
 * Copyright 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */


#include "sstables/types.hh"
#include "sstables/version.hh"

namespace sstables {

enum class sstable_state : uint8_t {
    normal,
    staging,
    quarantine,
    upload,
};

class sstable_id final {
    utils::UUID uuid();
};

enum class sstable_version_types : int {
    ka,
    la,
    mc,
    md,
    me,
    ms,
    mt,
};

enum class sstable_format_types : int {
    big,
};

}
