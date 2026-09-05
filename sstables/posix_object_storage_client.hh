/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <seastar/core/shared_ptr.hh>
#include "seastarx.hh"

namespace db {
class object_storage_endpoint_param;
}

namespace sstables {

class object_storage_client;

// Creates an object_storage_client backed by a locally-mounted POSIX
// filesystem. Objects are stored as regular files at <path>/<bucket>/<object>,
// where <path> is the endpoint's base directory.
shared_ptr<object_storage_client> make_posix_object_storage_client(const db::object_storage_endpoint_param&);

}
