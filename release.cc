/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "version.hh"
#include "build_mode.hh"

#include <seastar/core/print.hh>

static const char scylla_version_str[] = SCYLLA_VERSION;
static const char scylla_release_str[] = SCYLLA_RELEASE;
static const char scylla_build_mode_str[] = SCYLLA_BUILD_MODE_STR;

std::string scylla_version()
{
    return seastar::format("{}-{}", scylla_version_str, scylla_release_str);
}

std::string scylla_build_mode()
{
    return seastar::format("{}", scylla_build_mode_str);
}

// get the version number into writeable memory, so we can grep for it if we get a core dump
std::string version_stamp_for_core
    = "VERSION VERSION VERSION $Id: " + scylla_version() + " $ VERSION VERSION VERSION";
