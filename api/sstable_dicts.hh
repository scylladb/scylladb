/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <seastar/core/sharded.hh>

namespace seastar::httpd {
class routes;
}

namespace service {
class raft_group0_client;
}

namespace sstables {
class dictionary_service;
}

namespace api {

struct http_context;

void set_sstable_dicts(http_context& ctx, seastar::httpd::routes& r, seastar::sharded<sstables::dictionary_service>& dicts,
        service::raft_group0_client& group0_client);
void unset_sstable_dicts(http_context& ctx, seastar::httpd::routes& r);

}
