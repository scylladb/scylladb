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

// See the comment on the analogous declaration in locator/host_id.hh: this
// suppresses per-TU re-instantiation of state_id::to_sstring(), which is
// odr-used from gms/versioned_value.hh (included by ~60 translation units).
// Matching explicit instantiation lives in gms/versioned_value.cc. Must be
// named fully-qualified outside namespace service (see locator/host_id.hh).
extern template struct utils::tagged_uuid<service::state_id_tag>;
