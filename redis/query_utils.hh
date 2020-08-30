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

#include "seastar/core/shared_ptr.hh"
#include "seastar/core/future.hh"
#include "bytes.hh"
#include "gc_clock.hh"
#include "query-request.hh"

using namespace seastar;

namespace service {
class storage_proxy;
class client_state;
}

class service_permit;

namespace redis {

class redis_options;

struct strings_result {
    bytes _result;
    bool _has_result;
    ttl_opt _ttl;
    bytes& result() { return _result; }
    bool has_result() const { return _has_result; }
    gc_clock::duration ttl() { return _ttl.value(); }
    bool has_ttl() { return _ttl.has_value(); }
};

future<lw_shared_ptr<strings_result>> read_strings(service::storage_proxy&, const redis_options&, const bytes&, service_permit);
future<lw_shared_ptr<strings_result>> read_strings_from_hash(service::storage_proxy&, const redis_options&, const bytes&, const bytes&, service_permit);
future<lw_shared_ptr<strings_result>> query_strings(service::storage_proxy&, const redis_options&, const bytes&, service_permit, schema_ptr, query::partition_slice);

}
