/*
 * Copyright 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "utils/tagged_integer.hh"

namespace gms {

using generation_type = utils::tagged_integer<struct generation_type_tag, int32_t>;

generation_type get_generation_number();

void validate_gossip_generation(int64_t generation_number);
inline void debug_validate_gossip_generation([[maybe_unused]] int64_t generation_number) {
#ifndef SCYLLA_BUILD_MODE_RELEASE
    validate_gossip_generation(generation_number);
#endif
}

}
