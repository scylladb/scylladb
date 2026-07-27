/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "storage_manager.hh"
#include "api/api.hh"

namespace api {

using namespace seastar::httpd;

void set_storage_manager(http_context& ctx, routes& r, sharded<sstables::storage_manager>& sstm) {
}

void unset_storage_manager(http_context& ctx, routes& r) {
}

}
