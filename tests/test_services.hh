/*
 * This file is open source software, licensed to you under the terms
 * of the Apache License, Version 2.0 (the "License").  See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership.  You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Copyright (C) 2016 Cloudius Systems, Ltd.
 */

#pragma once

#include <seastar/core/distributed.hh>
#include <seastar/core/thread.hh>
#include "auth/service.hh"
#include "service/storage_service.hh"
#include "message/messaging_service.hh"

class storage_service_for_tests {
    distributed<database> _db;
    sharded<auth::service> _auth_service;
public:
    storage_service_for_tests() {
        auto thread = seastar::thread_impl::get();
        assert(thread);
        netw::get_messaging_service().start(gms::inet_address("127.0.0.1")).get();
        service::get_storage_service().start(std::ref(_db), std::ref(_auth_service)).get();
        service::get_storage_service().invoke_on_all([] (auto& ss) {
            ss.enable_all_features();
        }).get();
    }
    ~storage_service_for_tests() {
        service::get_storage_service().stop().get();
        netw::get_messaging_service().stop().get();
        _db.stop().get();
    }
};
