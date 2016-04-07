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
#include "database.hh"
#include "core/distributed.hh"
#include <memory>

std::unique_ptr<org::apache::cassandra::CassandraCobSvIfFactory> create_handler_factory(distributed<database>& db);

#endif /* APPS_SEASTAR_THRIFT_HANDLER_HH_ */
