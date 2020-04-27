/*
 * Copyright (C) 2019 pengjian.uestc @ gmail.com
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
#include "types.hh"
#include <vector>
#include <map>

class service_permit;

namespace service {
class storage_proxy;
}

class mutation;
namespace redis {

class redis_options;

future<> write_strings(service::storage_proxy& proxy, redis::redis_options& options, bytes&& key, bytes&& data, long ttl, service_permit permit);
future<> write_lists(service::storage_proxy& proxy, redis::redis_options& options, bytes&& key, std::vector<bytes>&& data, long ttl, service_permit permit);
future<> write_hashes(service::storage_proxy& proxy, redis::redis_options& options, bytes&& key, std::map<bytes, bytes>&& data, long ttl, service_permit permit);
future<> write_sets(service::storage_proxy& proxy, redis::redis_options& options, bytes&& key, std::vector<bytes>&& data, long ttl, service_permit permit);
future<> write_zsets(service::storage_proxy& proxy, redis::redis_options& options, bytes&& key, std::vector<bytes>&& data, long ttl, service_permit permit);
future<> delete_objects(service::storage_proxy& proxy, redis::redis_options& options, std::vector<bytes>&& keys, service_permit permit);

}
