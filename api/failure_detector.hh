/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "api_init.hh"

namespace gms {

class gossiper;

}

namespace api {

void set_failure_detector(http_context& ctx, httpd::routes& r, gms::gossiper& g);
void unset_failure_detector(http_context& ctx, httpd::routes& r);

}
