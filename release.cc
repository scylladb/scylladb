/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "utils/assert.hh"
#include "version.hh"
#include "build_mode.hh"

#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>

#include <seastar/core/print.hh>

static const char scylla_product_str[] = SCYLLA_PRODUCT;
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

std::string doc_link(std::string_view url_tail) {
    const std::string_view product = scylla_product_str;
    const std::string_view version = scylla_version_str;

    const auto prefix = product == "scylla-enterprise" ? "enterprise" : "opensource";

    std::string branch = product == "scylla-enterprise" ? "enterprise" : "master";
    if (!version.ends_with("~dev")) {
        std::vector<std::string> components;
        boost::split(components, version, boost::algorithm::is_any_of("."));
        // Version is compiled into the binary, testing will step on this immediately.
        SCYLLA_ASSERT(components.size() >= 2);
        branch = fmt::format("branch-{}.{}", components[0], components[1]);
    }

    return fmt::format("https://{}.docs.scylladb.com/{}/{}", prefix, branch, url_tail);
}

// get the version number into writeable memory, so we can grep for it if we get a core dump
std::string version_stamp_for_core
    = "VERSION VERSION VERSION $Id: " + scylla_version() + " $ VERSION VERSION VERSION";
