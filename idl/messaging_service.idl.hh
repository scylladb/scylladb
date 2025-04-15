/*
 * Copyright 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

namespace netw {

struct schema_pull_options {
    bool remote_supports_canonical_mutation_retval;
    bool group0_snapshot_transfer [[version 4.7]] = false;
};

} // namespace netw
