/*
 * Copyright (C) 2019 pengjian.uestc @ gmail.com
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once
#include "bytes.hh"
#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>
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
