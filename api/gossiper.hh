/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "api/api_init.hh"

namespace gms {

class gossiper;

}

namespace api {

void set_gossiper(http_context& ctx, httpd::routes& r, gms::gossiper& g);
void unset_gossiper(http_context& ctx, httpd::routes& r);

}
