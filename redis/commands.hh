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

#include "redis/request.hh"
#include "redis/abstract_command.hh"

namespace redis {

namespace commands {

future<redis_message> get(service::storage_proxy&, request&&, redis_options&, service_permit);
future<redis_message> exists(service::storage_proxy& proxy, request&& req, redis::redis_options& options, service_permit permit);
future<redis_message> ttl(service::storage_proxy& proxy, request&& req, redis::redis_options& options, service_permit permit);
future<redis_message> strlen(service::storage_proxy& proxy, request&& req, redis::redis_options& options, service_permit permit);
future<redis_message> hgetall(service::storage_proxy& proxy, request&& req, redis::redis_options& options, service_permit permit);
future<redis_message> hget(service::storage_proxy& proxy, request&& req, redis::redis_options& options, service_permit permit);
future<redis_message> hset(service::storage_proxy& proxy, request&& req, redis::redis_options& options, service_permit permit);
future<redis_message> hdel(service::storage_proxy& proxy, request&& req, redis::redis_options& options, service_permit permit);
future<redis_message> set(service::storage_proxy& proxy, request&& req, redis::redis_options& options, service_permit permit);
future<redis_message> setex(service::storage_proxy& proxy, request&& req, redis::redis_options& options, service_permit permit);
future<redis_message> del(service::storage_proxy& proxy, request&& req, redis::redis_options& options, service_permit permit);
future<redis_message> unknown(service::storage_proxy&, request&&, redis_options&, service_permit);
future<redis_message> select(service::storage_proxy&, request&& req, redis::redis_options& options, service_permit);
future<redis_message> ping(service::storage_proxy&, request&& req, redis::redis_options&, service_permit);
future<redis_message> echo(service::storage_proxy&, request&& req, redis::redis_options&, service_permit);
future<redis_message> lolwut(service::storage_proxy&, request&& req, redis::redis_options& options, service_permit);

}

}
