/*
 * Modified by ScyllaDB
 *
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include "types/types.hh"
#include "native_scalar_function.hh"
#include "utils/UUID.hh"

namespace cql3 {

namespace functions {

inline
shared_ptr<function>
make_uuid_fct() {
    return make_native_scalar_function<false>("uuid", uuid_type, {},
            [] (std::span<const bytes_opt> parameters) -> bytes_opt {
        return {uuid_type->decompose(utils::make_random_uuid())};
    });
}

}
}
