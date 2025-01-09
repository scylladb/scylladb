/*
 * Copyright (C) 2019 pengjian.uestc @ gmail.com
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once
#include "bytes_fwd.hh"
#include <seastar/core/future.hh>
#include "redis/reply.hh"

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
