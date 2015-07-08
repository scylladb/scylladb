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

#pragma once

#include "cql3/statements/schema_altering_statement.hh"

namespace cql3 {

namespace statements {

class drop_keyspace_statement : public schema_altering_statement {
    sstring _keyspace;
    bool _if_exists;
public:
    drop_keyspace_statement(const sstring& keyspace, bool if_exists);

    virtual void check_access(const service::client_state& state) override;

    virtual void validate(distributed<service::storage_proxy>&, const service::client_state& state) override;

    virtual const sstring& keyspace() const override;

    virtual future<bool> announce_migration(distributed<service::storage_proxy>& proxy, bool is_local_only) override;

    virtual shared_ptr<transport::event::schema_change> change_event() override;
};

}

}
