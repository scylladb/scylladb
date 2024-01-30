/*
 * Copyright (C) 2019 pengjian.uestc @ gmail.com
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once
#include <vector>
#include <seastar/core/future.hh>
#include "bytes.hh"
#include "seastarx.hh"

class service_permit;

namespace service {
class storage_proxy;
}

class mutation;
namespace redis {

class redis_options;

future<> write_hashes(service::storage_proxy& proxy, redis::redis_options& options, bytes&& key, bytes&& field, bytes&& data, long ttl, service_permit permit);
future<> write_strings(service::storage_proxy& proxy, redis::redis_options& options, bytes&& key, bytes&& data, long ttl, service_permit permit);
future<> delete_objects(service::storage_proxy& proxy, redis::redis_options& options, std::vector<bytes>&& keys, service_permit permit);
future<> delete_fields(service::storage_proxy& proxy, redis::redis_options& options, bytes&& key, std::vector<bytes>&& fields, service_permit permit);

}
