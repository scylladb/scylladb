/*
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "auth/allow_all_authorizer.hh"

#include "auth/common.hh"
#include "utils/class_registrator.hh"

namespace auth {

constexpr std::string_view allow_all_authorizer_name("org.apache.cassandra.auth.AllowAllAuthorizer");

// To ensure correct initialization order, we unfortunately need to use a string literal.
static const class_registrator<
    authorizer,
    allow_all_authorizer,
    cql3::query_processor&,
    ::service::raft_group0_client&,
    ::service::migration_manager&> registration("org.apache.cassandra.auth.AllowAllAuthorizer");

}
