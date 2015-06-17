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

#include "db/legacy_schema_tables.hh"

namespace service {

class migration_manager {
#if 0
    private static final Logger logger = LoggerFactory.getLogger(MigrationManager.class);

    public static final MigrationManager instance = new MigrationManager();

    private static final RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();

    public static final int MIGRATION_DELAY_IN_MS = 60000;

    private final List<IMigrationListener> listeners = new CopyOnWriteArrayList<>();

    private MigrationManager() {}
#endif
public:
    static future<> announce_new_keyspace(distributed<service::storage_proxy>& proxy, lw_shared_ptr<keyspace_metadata> ksm, bool announce_locally = false);

    static future<> announce_new_keyspace(distributed<service::storage_proxy>& proxy, lw_shared_ptr<keyspace_metadata> ksm, api::timestamp_type timestamp, bool announce_locally);

    static future<> announce_column_family_update(distributed<service::storage_proxy>& proxy, schema_ptr cfm, bool from_thrift, bool announce_locally = false);

    static future<> announce_new_column_family(distributed<service::storage_proxy>& proxy, schema_ptr cfm, bool announce_locally = false);

    /**
     * actively announce a new version to active hosts via rpc
     * @param schema The schema mutation to be applied
     */
    static future<> announce(distributed<service::storage_proxy>& proxy, mutation schema, bool announce_locally);

    static future<> announce(distributed<service::storage_proxy>& proxy, std::vector<mutation> mutations, bool announce_locally);

    // Returns a future on the local application of the schema
    static future<> announce(distributed<service::storage_proxy>& proxy, std::vector<mutation> schema);
};

}
