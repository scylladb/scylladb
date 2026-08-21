/*
 * Copyright 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
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
// Caches the result of validating that a role exists and has can_login=true,
// for authentication methods (e.g. mTLS) that don't need the role's password.
using role_cache = utils::loading_cache<std::string, bool, 1>;

// If get_password is true (the default), also verifies that a salted_hash
// (password) exists and returns it. Set get_password=false for authentication
// methods such as mTLS that do not use passwords; the function then only verifies
// role existence and can_login=true, and returns an empty string.
future<std::string> get_key_from_roles(service::storage_proxy& proxy, std::string username, bool get_password = true);

}
