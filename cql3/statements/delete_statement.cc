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

#include "delete_statement.hh"

namespace cql3 {

namespace statements {

void delete_statement::add_update_for_key(api::mutation& m, const api::clustering_prefix& prefix, const update_parameters& params) {
    if (_column_operations.empty()) {
        m.p.apply_delete(prefix, params.make_tombstone());
        return;
    }

    if (prefix.size() < s->clustering_key.size()) {
        // In general, we can't delete specific columns if not all clustering columns have been specified.
        // However, if we delete only static columns, it's fine since we won't really use the prefix anyway.
        for (auto&& deletion : _column_operations) {
            if (!deletion->column.is_static()) {
                throw exceptions::invalid_request_exception(sprint(
                    "Primary key column '%s' must be specified in order to delete column '%s'",
                    get_first_empty_key()->name, deletion->column.name));
            }
        }
    }

    for (auto&& op : _column_operations) {
        op->execute(m, prefix, params);
    }
}

}

}
