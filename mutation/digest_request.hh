/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <cstdint>

// Which hashes a read fills in rows it visits, see row::prepare_hash().
enum class digest_request : uint8_t { none, cells, cells_and_row };

// The row hash is valid only for a single MVCC version; merged versions clear it.
inline digest_request for_versions(digest_request dr, bool single_version) noexcept {
    return single_version || dr == digest_request::none ? dr : digest_request::cells;
}
