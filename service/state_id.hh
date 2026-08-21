/*
 * Copyright (C) 2024-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "utils/UUID.hh"

namespace service {

using state_id = utils::tagged_uuid<struct state_id_tag>;

}
