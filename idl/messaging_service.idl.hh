/*
 * Copyright 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

namespace netw {

struct schema_pull_options {
    bool remote_supports_canonical_mutation_retval;
    bool group0_snapshot_transfer [[version 4.7]] = false;
};

} // namespace netw
