/*
 * Copyright 2015 Cloudius Systems
 */

#include "api/api-doc/lsa.json.hh"
#include "api/lsa.hh"
#include "api/api.hh"

#include "http/exception.hh"
#include "utils/logalloc.hh"
#include "log.hh"

namespace api {

static logging::logger logger("lsa-api");

void set_lsa(http_context& ctx, routes& r) {
    httpd::lsa_json::lsa_compact.set(r, [&ctx](std::unique_ptr<request> req) {
        logger.info("Triggering compaction");
        return ctx.db.invoke_on_all([] (database&) {
            logalloc::shard_tracker().reclaim(std::numeric_limits<size_t>::max());
        }).then([] {
            return json::json_return_type(json::json_void());
        });
    });
}

}
