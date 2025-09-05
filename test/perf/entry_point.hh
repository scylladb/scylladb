/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <functional>

#include "db/config.hh"

namespace perf {

int scylla_fast_forward_main(int argc, char** argv);
int scylla_row_cache_update_main(int argc, char**argv);
int scylla_simple_query_main(int argc, char** argv);
int scylla_sstable_main(int argc, char** argv);
int scylla_tablets_main(int argc, char**argv);
std::function<int(int, char**)> alternator(std::function<int(int, char**)> scylla_main, std::function<void(lw_shared_ptr<db::config> cfg)>* after_init_func);
int scylla_tablet_load_balancing_main(int argc, char**argv);
// Launches a performance workload that talks to the embedded server over the native CQL
// protocol using handcrafted CQL binary frames (no driver). Similar to perf_alternator
// (runs inside the server process) and perf_simple_query (similar workload types), but
// exercises the full networking + protocol parsing path.
std::function<int(int, char**)> cql_raw(std::function<int(int, char**)> scylla_main, std::function<void(lw_shared_ptr<db::config> cfg)>* after_init_func);

} // namespace tools
