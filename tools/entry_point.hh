/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

namespace tools {

int scylla_local_file_key_generator_main(int argc, char** argv);
int scylla_types_main(int argc, char** argv);
int scylla_sstable_main(int argc, char** argv);
int scylla_nodetool_main(int argc, char** argv);

} // namespace tools
