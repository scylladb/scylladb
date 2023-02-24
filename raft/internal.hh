/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
#pragma once

#include <ostream>
#include <functional>
#include "utils/UUID.hh"

namespace raft {
namespace internal {

template<typename Tag>
using tagged_id = utils::tagged_uuid<Tag>;

} // end of namespace internal
} // end of namespace raft
