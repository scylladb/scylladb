/*
 * Copyright (C) 2014 ScyllaDB
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

#ifndef APPS_SEASTAR_THRIFT_HANDLER_HH_
#define APPS_SEASTAR_THRIFT_HANDLER_HH_

#include "Cassandra.h"
#include "auth/service.hh"
#include "database.hh"
#include "core/distributed.hh"
#include "cql3/query_processor.hh"
#include <memory>

struct timeout_config;

std::unique_ptr<::cassandra::CassandraCobSvIfFactory> create_handler_factory(distributed<database>& db, distributed<cql3::query_processor>& qp, auth::service&, timeout_config);

#endif /* APPS_SEASTAR_THRIFT_HANDLER_HH_ */
