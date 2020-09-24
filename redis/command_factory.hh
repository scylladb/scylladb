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

#include "bytes.hh"
#include "seastar/core/shared_ptr.hh"
#include "abstract_command.hh"
#include "redis/options.hh"

namespace service {
    class storage_proxy;
}

namespace redis {
using namespace seastar;
class request;
class command_factory {
public:
    command_factory() {}
    ~command_factory() {}
    static future<redis_message> create_execute(service::storage_proxy&, request&, redis::redis_options&, service_permit);
};
}
