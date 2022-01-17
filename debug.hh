/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <seastar/core/sharded.hh>

namespace replica {
class database;
}

namespace debug {

extern seastar::sharded<replica::database>* the_database;


}

