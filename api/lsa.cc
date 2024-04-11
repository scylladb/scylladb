/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "api/api-doc/lsa.json.hh"
#include "api/lsa.hh"

#include <seastar/http/exception.hh>
#include "utils/logalloc.hh"
#include "log.hh"

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
