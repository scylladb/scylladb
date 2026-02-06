/*
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "auth/allow_all_authenticator.hh"

#include "service/migration_manager.hh"

namespace auth {

constexpr std::string_view allow_all_authenticator_name("org.apache.cassandra.auth.AllowAllAuthenticator");

}
