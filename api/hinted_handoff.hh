/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <seastar/core/sharded.hh>
#include "api/api_init.hh"
#include "gms/gossiper.hh"

namespace service { class storage_proxy; }

namespace api {

void set_hinted_handoff(http_context& ctx, httpd::routes& r, sharded<service::storage_proxy>& p, sharded<gms::gossiper>& g);
void unset_hinted_handoff(http_context& ctx, httpd::routes& r);

}
