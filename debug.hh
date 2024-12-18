/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <seastar/core/sharded.hh>

namespace replica {
class database;
}

namespace debug {

extern seastar::sharded<replica::database>* the_database;


}

