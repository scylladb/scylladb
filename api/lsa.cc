/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "api/api-doc/lsa.json.hh"
#include "api/lsa.hh"

#include <seastar/http/exception.hh>
#include "utils/logalloc.hh"
#include "utils/log.hh"

namespace api {
using namespace seastar::httpd;

static logging::logger alogger("lsa-api");

void set_lsa(http_context& ctx, routes& r) {
    httpd::lsa_json::lsa_compact.set(r, [](std::unique_ptr<request> req) {
        alogger.info("Triggering compaction");
        return smp::invoke_on_all([] {
            logalloc::shard_tracker().reclaim(std::numeric_limits<size_t>::max());
        }).then([] {
            return json::json_return_type(json::json_void());
        });
    });
}

}
