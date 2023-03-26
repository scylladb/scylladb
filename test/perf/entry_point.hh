/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

namespace perf {

int scylla_fast_forward_main(int argc, char** argv);
int scylla_row_cache_update_main(int argc, char**argv);
int scylla_simple_query_main(int argc, char** argv);
int scylla_sstable_main(int argc, char** argv);
int scylla_tablets_main(int argc, char**argv);

} // namespace tools
