/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "debug.hh"

namespace debug {

seastar::sharded<replica::database>* the_database = nullptr;

}
