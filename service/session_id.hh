/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "utils/UUID.hh"

namespace service {

using session_id = utils::tagged_uuid<struct session_id_tag>;

// We want it to be different than a default-constructed session_id to catch mistakes.
constexpr session_id default_session_id = session_id(
    utils::UUID(0x81e7fc5a8d4411ee, 0x8577325096b39f47)); // timeuuid 2023-11-27 16:46:27.182089.0 UTC

} // namespace service
