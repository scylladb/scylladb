/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "api.hh"

namespace api {

void set_stream_manager(http_context& ctx, httpd::routes& r, sharded<streaming::stream_manager>& sm);
void unset_stream_manager(http_context& ctx, httpd::routes& r);

}
