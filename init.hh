/*
 * Copyright (C) 2015 ScyllaDB
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
#pragma once

#include <seastar/core/sstring.hh>
#include <seastar/core/future.hh>
#include <seastar/core/distributed.hh>
#include "db/config.hh"
#include "database.hh"

void init_storage_service(distributed<database>& db);
void init_ms_fd_gossiper(sstring listen_address
                , uint16_t storage_port
                , uint16_t ssl_storage_port
                , sstring ms_encrypt_what
                , sstring ms_trust_store
                , sstring ms_cert
                , sstring ms_key
                , db::seed_provider_type seed_provider
                , sstring cluster_name = "Test Cluster"
                , double phi = 8);
