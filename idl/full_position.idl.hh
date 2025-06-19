/*
 * Copyright 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "idl/position_in_partition.idl.hh"

struct full_position {
    partition_key partition;
    position_in_partition position;
};
