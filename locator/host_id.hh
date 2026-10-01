/*
 * Copyright (C) 2015-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "utils/UUID.hh"

#include <seastar/core/future.hh>
#include <seastar/util/noncopyable_function.hh>

#include <exception>
#include <variant>

namespace locator {

using host_id = utils::tagged_uuid<struct host_id_tag>;
using host_id_or_exception = std::variant<host_id, std::exception_ptr>;
// Called when a forwarded request finishes, with the host that executed it or
// the error. The forwarding node waits for the returned future before replying.
using host_id_or_exception_callback = noncopyable_function<seastar::future<>(host_id_or_exception)>;

}

