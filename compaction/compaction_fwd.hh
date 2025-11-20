/*
 * Copyright (C) 2023-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <seastar/core/shared_ptr.hh>

namespace compaction {

class compaction_group_view;
class strategy_control;
struct compaction_state;

} // namespace compaction
