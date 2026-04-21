/*
 * Copyright 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "idl/uuid.idl.hh"

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

}
