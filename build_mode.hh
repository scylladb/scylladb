
/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#ifndef SCYLLA_BUILD_MODE
#error SCYLLA_BUILD_MODE must be defined
#endif

#ifndef STRINGIFY
// We need two levels of indirection
// to make a string out of the macro name.
// The outer level expands the macro
// and the inner level makes a string out of the expanded macro.
#define STRINGIFY_VALUE(x) #x
#define STRINGIFY_MACRO(x) STRINGIFY_VALUE(x)
#endif

#define SCYLLA_BUILD_MODE_STR STRINGIFY_MACRO(SCYLLA_BUILD_MODE)

// We use plain macro definitions
// so the preprocessor can expand them
// inline in the #if directives below
#define SCYLLA_BUILD_MODE_CODE_debug 0
#define SCYLLA_BUILD_MODE_CODE_release 1
#define SCYLLA_BUILD_MODE_CODE_dev 2
#define SCYLLA_BUILD_MODE_CODE_sanitize 3
#define SCYLLA_BUILD_MODE_CODE_coverage 4

#define _SCYLLA_BUILD_MODE_CODE(sbm) SCYLLA_BUILD_MODE_CODE_ ## sbm
#define SCYLLA_BUILD_MODE_CODE(sbm) _SCYLLA_BUILD_MODE_CODE(sbm)

#if SCYLLA_BUILD_MODE_CODE(SCYLLA_BUILD_MODE) == SCYLLA_BUILD_MODE_CODE_debug
#define SCYLLA_BUILD_MODE_DEBUG
#elif SCYLLA_BUILD_MODE_CODE(SCYLLA_BUILD_MODE) == SCYLLA_BUILD_MODE_CODE_release
#define SCYLLA_BUILD_MODE_RELEASE
#elif SCYLLA_BUILD_MODE_CODE(SCYLLA_BUILD_MODE) == SCYLLA_BUILD_MODE_CODE_dev
#define SCYLLA_BUILD_MODE_DEV
#elif SCYLLA_BUILD_MODE_CODE(SCYLLA_BUILD_MODE) == SCYLLA_BUILD_MODE_CODE_sanitize
#define SCYLLA_BUILD_MODE_SANITIZE
#elif SCYLLA_BUILD_MODE_CODE(SCYLLA_BUILD_MODE) == SCYLLA_BUILD_MODE_CODE_coverage
#define SCYLLA_BUILD_MODE_COVERAGE
#else
#error unrecognized SCYLLA_BUILD_MODE
#endif

#if (defined(SCYLLA_BUILD_MODE_RELEASE) || defined(SCYLLA_BUILD_MODE_DEV)) && defined(SEASTAR_DEBUG)
#error SEASTAR_DEBUG is not expected to be defined when SCYLLA_BUILD_MODE is "release" or "dev"
#endif

#if (defined(SCYLLA_BUILD_MODE_DEBUG) || defined(SCYLLA_BUILD_MODE_SANITIZE)) && !defined(SEASTAR_DEBUG)
#error SEASTAR_DEBUG is expected to be defined when SCYLLA_BUILD_MODE is "debug" or "sanitize"
#endif
