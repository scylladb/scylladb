/*
 * Copyright 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

class restore_result {
};

verb [[with_client_info]] restore_tablet (locator::global_tablet_id gid, sstring snap_name, sstring endpoint, sstring bucket) -> restore_result;
verb [[with_client_info]] abort_restore_tablet (locator::global_tablet_id gid)
