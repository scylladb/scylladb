/*
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "auth/allow_all_authenticator.hh"

#include "service/migration_manager.hh"
#include "utils/class_registrator.hh"

namespace auth {

constexpr std::string_view allow_all_authenticator_name("org.apache.cassandra.auth.AllowAllAuthenticator");

// To ensure correct initialization order, we unfortunately need to use a string literal.
static const class_registrator<
        authenticator,
        allow_all_authenticator,
        cql3::query_processor&,
        ::service::raft_group0_client&,
        ::service::migration_manager&> registration("org.apache.cassandra.auth.AllowAllAuthenticator");

}
