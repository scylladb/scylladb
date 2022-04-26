/*
 * Copyright (C) 2019 pengjian.uestc @ gmail.com
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <seastar/core/shared_ptr.hh>
#include <seastar/core/future.hh>
#include "bytes.hh"
#include "gc_clock.hh"
#include "query-request.hh"

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

seastar::future<seastar::lw_shared_ptr<strings_result>> read_strings(service::storage_proxy&, const redis_options&, const bytes&, service_permit);
seastar::future<seastar::lw_shared_ptr<strings_result>> query_strings(service::storage_proxy&, const redis_options&, const bytes&, service_permit, schema_ptr, query::partition_slice);

seastar::future<seastar::lw_shared_ptr<std::map<bytes, bytes>>> read_hashes(service::storage_proxy&, const redis_options&, const bytes&, service_permit);
seastar::future<seastar::lw_shared_ptr<std::map<bytes, bytes>>> read_hashes(service::storage_proxy&, const redis_options&, const bytes&, const bytes&, service_permit);
seastar::future<seastar::lw_shared_ptr<std::map<bytes, bytes>>> query_hashes(service::storage_proxy&, const redis_options&, const bytes&, service_permit, schema_ptr, query::partition_slice);

}
