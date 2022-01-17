/*
 * Copyright (C) 2019 pengjian.uestc @ gmail.com
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "redis/request.hh"
#include "redis/abstract_command.hh"

namespace redis {

namespace commands {

// request& instead of request&& to make sure ownership is managed by the caller
future<redis_message> get(service::storage_proxy&, request&, redis_options&, service_permit);
future<redis_message> exists(service::storage_proxy& proxy, request& req, redis::redis_options& options, service_permit permit);
future<redis_message> ttl(service::storage_proxy& proxy, request& req, redis::redis_options& options, service_permit permit);
future<redis_message> strlen(service::storage_proxy& proxy, request& req, redis::redis_options& options, service_permit permit);
future<redis_message> hgetall(service::storage_proxy& proxy, request& req, redis::redis_options& options, service_permit permit);
future<redis_message> hget(service::storage_proxy& proxy, request& req, redis::redis_options& options, service_permit permit);
future<redis_message> hset(service::storage_proxy& proxy, request& req, redis::redis_options& options, service_permit permit);
future<redis_message> hdel(service::storage_proxy& proxy, request& req, redis::redis_options& options, service_permit permit);
future<redis_message> hexists(service::storage_proxy& proxy, request& req, redis::redis_options& options, service_permit permit);
future<redis_message> set(service::storage_proxy& proxy, request& req, redis::redis_options& options, service_permit permit);
future<redis_message> setex(service::storage_proxy& proxy, request& req, redis::redis_options& options, service_permit permit);
future<redis_message> del(service::storage_proxy& proxy, request& req, redis::redis_options& options, service_permit permit);
future<redis_message> unknown(service::storage_proxy&, request&, redis_options&, service_permit);
future<redis_message> select(service::storage_proxy&, request& req, redis::redis_options& options, service_permit);
future<redis_message> ping(service::storage_proxy&, request& req, redis::redis_options&, service_permit);
future<redis_message> echo(service::storage_proxy&, request& req, redis::redis_options&, service_permit);
future<redis_message> lolwut(service::storage_proxy&, request& req, redis::redis_options& options, service_permit);

}

}
