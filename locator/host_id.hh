/*
 * Copyright (C) 2015-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "utils/UUID.hh"

#include <seastar/util/noncopyable_function.hh>

#include <exception>
#include <variant>

namespace locator {

using host_id = utils::tagged_uuid<struct host_id_tag>;
using host_id_or_exception = std::variant<host_id, std::exception_ptr>;
using host_id_or_exception_callback = noncopyable_function<void(host_id_or_exception)>;

// host_id::to_sstring() is the only non-trivial (non-constexpr) member of
// tagged_uuid<Tag>; it is odr-used from several widely-included headers
// (notably gms/versioned_value.hh), so suppress its per-TU re-instantiation.
// See the matching `template struct ...;` explicit instantiation in
// gms/versioned_value.cc.
extern template struct utils::tagged_uuid<host_id_tag>;

}

