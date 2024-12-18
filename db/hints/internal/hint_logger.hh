/*
 * Modified by ScyllaDB
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */
#pragma once

// Scylla includes.
#include "utils/log.hh"

namespace db::hints {
namespace internal {

// TODO: Change this name later to something that suits its usage better.
inline logging::logger manager_logger{"hints_manager"};

} // namespace internal
} // namespace db::hints
