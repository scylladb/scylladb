/*
 * Copyright (C) 2019 pengjian.uestc @ gmail.com
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "redis/abstract_command.hh"
#include "redis/options.hh"

namespace service {
    class storage_proxy;
}

namespace redis {
class request;
class command_factory {
public:
    command_factory() {}
    ~command_factory() {}
    static seastar::future<redis_message> create_execute(service::storage_proxy&, request&, redis::redis_options&, service_permit);
};
}
