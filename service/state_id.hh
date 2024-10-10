/*
 * Copyright (C) 2024-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "utils/UUID.hh"

namespace service {

using state_id = utils::tagged_uuid<struct state_id_tag>;

}
