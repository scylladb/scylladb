/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

namespace seastar::httpd {
class routes;
}

namespace api {

void set_system(seastar::httpd::routes& r);

}
