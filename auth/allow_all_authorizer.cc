/*
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "auth/allow_all_authorizer.hh"

#include "auth/common.hh"

namespace auth {

constexpr std::string_view allow_all_authorizer_name("org.apache.cassandra.auth.AllowAllAuthorizer");

}
