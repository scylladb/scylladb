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

#include "database.hh"
#include "query-request.hh"
#include "query-result.hh"
#include "query-result-set.hh"
#include "core/distributed.hh"
#include "db/consistency_level.hh"

namespace service {

class storage_proxy /*implements StorageProxyMBean*/ {
private:
    distributed<database>& _db;
    future<foreign_ptr<lw_shared_ptr<query::result>>> query_singular(lw_shared_ptr<query::read_command> cmd, std::vector<query::partition_range>&& partition_ranges, db::consistency_level cl);
public:
    storage_proxy(distributed<database>& db) : _db(db) {}

    distributed<database>& get_db() {
        return _db;
    }

    future<> mutate_locally(const mutation& m);
    future<> mutate_locally(const frozen_mutation& m);
    future<> mutate_locally(std::vector<mutation> mutations);

    /**
    * Use this method to have these Mutations applied
    * across all replicas. This method will take care
    * of the possibility of a replica being down and hint
    * the data across to some other replica.
    *
    * @param mutations the mutations to be applied across the replicas
    * @param consistency_level the consistency level for the operation
    */
    future<> mutate(std::vector<mutation> mutations, db::consistency_level cl);

    future<> mutate_with_triggers(std::vector<mutation> mutations, db::consistency_level cl,
        bool should_mutate_atomically);

    /**
    * See mutate. Adds additional steps before and after writing a batch.
    * Before writing the batch (but after doing availability check against the FD for the row replicas):
    *      write the entire batch to a batchlog elsewhere in the cluster.
    * After: remove the batchlog entry (after writing hints for the batch rows, if necessary).
    *
    * @param mutations the Mutations to be applied across the replicas
    * @param consistency_level the consistency level for the operation
    */
    future<> mutate_atomically(std::vector<mutation> mutations, db::consistency_level cl);

    future<foreign_ptr<lw_shared_ptr<query::result>>> query(lw_shared_ptr<query::read_command> cmd, std::vector<query::partition_range>&& partition_ranges, db::consistency_level cl);

    future<lw_shared_ptr<query::result_set>> query_local(const sstring& ks_name, const sstring& cf_name, const dht::decorated_key& key);
};

}
