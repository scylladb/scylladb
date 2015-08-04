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
 */

/*
 * Copyright 2015 Cloudius Systems
 *
 * Modified by Cloudius Systems
 */

#include "cql3/statements/drop_keyspace_statement.hh"

#include "service/migration_manager.hh"
#include "transport/event.hh"

namespace cql3 {

namespace statements {

drop_keyspace_statement::drop_keyspace_statement(const sstring& keyspace, bool if_exists)
    : _keyspace{keyspace}
    , _if_exists{if_exists}
{
}

void drop_keyspace_statement::check_access(const service::client_state& state)
{
    warn(unimplemented::cause::AUTH);
#if 0
    state.hasKeyspaceAccess(keyspace, Permission.DROP);
#endif
}

void drop_keyspace_statement::validate(distributed<service::storage_proxy>&, const service::client_state& state)
{
    warn(unimplemented::cause::VALIDATION);
#if 0
    ThriftValidation.validateKeyspaceNotSystem(keyspace);
#endif
}

const sstring& drop_keyspace_statement::keyspace() const
{
    return _keyspace;
}

future<bool> drop_keyspace_statement::announce_migration(distributed<service::storage_proxy>& proxy, bool is_local_only)
{
    return make_ready_future<>().then([&] {
        return service::get_local_migration_manager().announce_keyspace_drop(proxy, _keyspace, is_local_only);
    }).then_wrapped([this] (auto&& f) {
        try {
            f.get();
            return true;
        } catch (const exceptions::configuration_exception& e) {
            if (_if_exists) {
                return false;
            }
            throw e;
        }
    });
}

shared_ptr<transport::event::schema_change> drop_keyspace_statement::change_event()
{
    using namespace transport;

    return make_shared<event::schema_change>(event::schema_change::change_type::DROPPED, keyspace());

}

}

}
