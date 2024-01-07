/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "utils/UUID.hh"

namespace sstables {

using run_id = utils::tagged_uuid<struct run_id_tag>;

} // namespace sstables
