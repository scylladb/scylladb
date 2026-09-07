/*
 * Copyright 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

class restore_result {
};

verb [[cancellable]] restore_tablet (raft::server_id dst_id, locator::global_tablet_id gid) -> restore_result;
