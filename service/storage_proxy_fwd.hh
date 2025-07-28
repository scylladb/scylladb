/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */
#pragma once

#include <seastar/util/bool_class.hh>
#include "seastarx.hh"

namespace service {
class storage_proxy;

// Per-request flag that restricts query execution to the local node only
// by filtering out all non-local replicas. Standard consistency level rules apply:
// if the local node alone cannot satisfy the requested CL, an exception is thrown.
using node_local_only = bool_class<class node_local_only_tag>;
}
