/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
#pragma once

#include "utils/UUID.hh"
#include "utils/tagged_integer.hh"

namespace raft {
namespace internal {

template<typename Tag>
using tagged_id = utils::tagged_uuid<Tag>;

template<typename Tag>
using tagged_uint64 = utils::tagged_tagged_integer<struct non_final, Tag, uint64_t>;

} // end of namespace internal
} // end of namespace raft
