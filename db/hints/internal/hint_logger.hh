/*
 * Modified by ScyllaDB
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
#pragma once

// Scylla includes.
#include "log.hh"

/// This file is supposed to only be included in the source files
/// related to the hint manager logic. After refactoring the code,
/// the manager file was divided into separate smaller translation units.
/// However, we want to preserve the previous logging behavior.
/// This file serves this purpose.
///
/// DO NOT include this file in headers that are going to be exposed
/// for the user of the module. If you don't know if you should use this
/// logger, you most likely should use another one.

namespace db::hints {
namespace internal {

// TODO: Change this name later to something that suits its usage better.
inline logging::logger manager_logger{"hints_manager"};

} // namespace internal
} // namespace db::hints
