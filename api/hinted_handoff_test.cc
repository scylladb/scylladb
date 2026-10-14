/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "build_mode.hh"

#ifndef SCYLLA_BUILD_MODE_RELEASE

#include <seastar/core/coroutine.hh>

#include "api/api.hh"
#include "api/api-doc/hinted_handoff_test.json.hh"
#include "hinted_handoff_test.hh"
#include "db/hints/sync_point.hh"
#include "service/storage_proxy.hh"

namespace api {

namespace hht = httpd::hinted_handoff_test_json;
using namespace json;
using namespace seastar::httpd;

void set_hinted_handoff_test(http_context& ctx, routes& r, sharded<service::storage_proxy>& proxy) {
    hht::discard_hint_segments.set(r, [&proxy] (std::unique_ptr<http::request> req) -> future<json::json_return_type> {
        db::hints::sync_point sync_point;
        const sstring encoded = req->get_query_param("id");
        try {
            sync_point = db::hints::sync_point::decode(encoded);
        } catch (std::exception& e) {
            throw httpd::bad_param_exception(format("Failed to parse the sync point description {}: {}", encoded, e.what()));
        }

        co_await proxy.local().discard_hint_segments_up_to(sync_point);
        co_return json_void();
    });

    hht::flush_hints.set(r, [&proxy] (std::unique_ptr<http::request> req) -> future<json::json_return_type> {
        co_await proxy.local().flush_hints();
        co_return json_void();
    });
}

void unset_hinted_handoff_test(http_context& ctx, routes& r) {
    hht::discard_hint_segments.unset(r);
    hht::flush_hints.unset(r);
}

}

#endif
