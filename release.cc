/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#include "version.hh"

#include <seastar/core/print.hh>

static const char scylla_version_str[] = SCYLLA_VERSION;
static const char scylla_release_str[] = SCYLLA_RELEASE;

std::string scylla_version()
{
    return sprint("%s-%s", scylla_version_str, scylla_release_str);
}
