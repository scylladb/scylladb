/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Modified by ScyllaDB
 * Copyright (C) 2021 ScyllaDB
 *
 */

#pragma once

#include "gms/i_endpoint_state_change_subscriber.hh"

namespace db {
class system_distributed_keyspace;
}

namespace gms {
class gossiper;
}

namespace cdc {

class generation_service : public peering_sharded_service<generation_service>
                         , public async_sharded_service<generation_service>
                         , public gms::i_endpoint_state_change_subscriber {

    bool _stopped = false;

    const db::config& _cfg;
    gms::gossiper& _gossiper;
    sharded<db::system_distributed_keyspace>& _sys_dist_ks;
    abort_source& _abort_src;
    const locator::shared_token_metadata& _token_metadata;

public:
    generation_service(const db::config&, gms::gossiper&,
            sharded<db::system_distributed_keyspace>&, abort_source&, const locator::shared_token_metadata&);

    future<> stop();
    ~generation_service();

    future<> after_join();

    virtual void before_change(gms::inet_address, gms::endpoint_state, gms::application_state, const gms::versioned_value&) override {}
    virtual void on_alive(gms::inet_address, gms::endpoint_state) override {}
    virtual void on_dead(gms::inet_address, gms::endpoint_state) override {}
    virtual void on_remove(gms::inet_address) override {}
    virtual void on_restart(gms::inet_address, gms::endpoint_state) override {}

    virtual void on_join(gms::inet_address, gms::endpoint_state) override;
    virtual void on_change(gms::inet_address, gms::application_state, const gms::versioned_value&) override;
};

} // namespace cdc
