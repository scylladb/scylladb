/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.1 and Apache-2.0)
 */

#pragma once

namespace db {

// Type of a field in the serialized paging state (see idl/paging_state.idl.hh).
// Coordinators write std::nullopt to the field and ignore the received value.
enum class read_repair_decision {
  NONE,
  GLOBAL,
  DC_LOCAL
};

}
