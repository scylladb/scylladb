/*
 * Copyright 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <string>
#include "utils/loading_cache.hh"
#include "auth/service.hh"

namespace service {
class storage_proxy;
}

namespace alternator {

using key_cache = utils::loading_cache<std::string, std::string, 1>;

future<std::string> get_key_from_roles(service::storage_proxy& proxy, auth::service& as, std::string username);

}
