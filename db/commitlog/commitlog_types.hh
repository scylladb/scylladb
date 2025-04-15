
/*
 * Copyright 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <seastar/util/bool_class.hh>
#include "seastarx.hh"

namespace db {

using commitlog_force_sync = bool_class<class force_sync_tag>;

}
