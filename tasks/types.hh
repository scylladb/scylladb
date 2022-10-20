/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "utils/UUID.hh"

namespace tasks {

using task_id = utils::tagged_uuid<struct task_id_tag>;

}
