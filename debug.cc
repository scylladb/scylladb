/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "debug.hh"

namespace debug {

seastar::sharded<replica::database>* volatile the_database = nullptr;

}
