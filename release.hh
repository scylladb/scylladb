/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <string>
#include "build_mode.hh"

std::string scylla_version();

std::string scylla_build_mode();

// Generate the documentation link, which is appropriate for the current version
// and product (open-source or enterprise).
//
// Will return a documentation URL like this:
//      https://${product}.docs.scylladb.com/${branch}/${url_tail}
//
std::string doc_link(std::string_view url_tail);
