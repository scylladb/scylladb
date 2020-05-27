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
#include "seastar/core/future.hh"
#include "seastar/core/sstring.hh"
#include "redis/request.hh"
#include "redis/reply.hh"
#include "db/consistency_level_type.hh"
#include "db/timeout_clock.hh"
#include "db/system_keyspace.hh"
#include "keys.hh"
#include "timestamp.hh"
#include <unordered_map>

class service_permit;

namespace service {
class storage_proxy;
}

namespace redis {

class redis_options;
class redis_message;

class abstract_command : public enable_shared_from_this<abstract_command> {
protected:
    bytes _name;
public:
    abstract_command(bytes&& name)
        : _name(std::move(name))
    {
    }
    virtual ~abstract_command() {};

    virtual future<redis_message> execute(service::storage_proxy&, redis::redis_options&, service_permit permit) = 0;
    const bytes& name() const { return _name; }
};

} // end of redis namespace
