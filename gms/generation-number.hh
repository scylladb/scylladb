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

}
