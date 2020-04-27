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
#include <vector>
#include <map>

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
    bytes& result() { return _result; }
    bool has_result() const { return _has_result; }
};

struct lists_result {
    std::vector<bytes> _result;
    bool _has_result;
    std::vector<bytes>& result() { return _result; }
    bool has_result() const { return _has_result; }
};

struct hashes_result {
    std::map<bytes, bytes> _result;
    bool _has_result;
    std::map<bytes, bytes>& result() { return _result; }
    bool has_result() const { return _has_result; }
};

future<lw_shared_ptr<strings_result>> read_strings(service::storage_proxy&, const redis_options&, const bytes&, service_permit);
future<lw_shared_ptr<lists_result>> read_lists(service::storage_proxy&, const redis_options&, const bytes&, service_permit);
future<lw_shared_ptr<hashes_result>> read_hashes(service::storage_proxy&, const redis_options&, const bytes&, service_permit);
future<lw_shared_ptr<lists_result>> read_sets(service::storage_proxy&, const redis_options&, const bytes&, service_permit);
future<lw_shared_ptr<lists_result>> read_zsets(service::storage_proxy&, const redis_options&, const bytes&, service_permit);

}
