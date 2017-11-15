/*
 * Copyright (C) 2017 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "auth/allow_all_authorizer.hh"

#include "auth/common.hh"
#include "utils/class_registrator.hh"

namespace auth {

const sstring& allow_all_authorizer_name() {
    static const sstring name = meta::AUTH_PACKAGE_NAME + "AllowAllAuthorizer";
    return name;
}

// To ensure correct initialization order, we unfortunately need to use a string literal.
static const class_registrator<
    authorizer,
    allow_all_authorizer,
    cql3::query_processor&,
    ::service::migration_manager&> registration("org.apache.cassandra.auth.AllowAllAuthorizer");

}
