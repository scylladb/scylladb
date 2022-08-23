
/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#ifndef STRINGIFY
// We need to levels of indirection
// to make a string out of the macro name.
// The outer level expands the macro
// and the inner level makes a string out of the expanded macro.
#define STRINGIFY_VALUE(x) #x
#define STRINGIFY_MACRO(x) STRINGIFY_VALUE(x)
#endif

#define SCYLLA_BUILD_MODE_STR STRINGIFY_MACRO(SCYLLA_BUILD_MODE)

#if SCYLLA_BUILD_MODE == release
#define SCYLLA_BUILD_MODE_RELEASE
#elif SCYLLA_BUILD_MODE == dev
#define SCYLLA_BUILD_MODE_DEV
#elif SCYLLA_BUILD_MODE == debug
#define SCYLLA_BUILD_MODE_DEBUG
#elif SCYLLA_BUILD_MODE == sanitize
#define SCYLLA_BUILD_MODE_SANITIZE
#endif
