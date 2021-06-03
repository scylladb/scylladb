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
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include <vector>
#include <seastar/core/shared_ptr.hh>

#include "schema_fwd.hh"
#include "query-result.hh"
#include "query-request.hh"
#include "service/query_state.hh"
#include "cql3/selection/selection.hh"
#include "cql3/query_options.hh"
#include "query_pager.hh"

namespace service {

namespace pager {

class query_pagers {
public:
    static bool may_need_paging(const schema& s, uint32_t page_size, const query::read_command&,
            const dht::partition_range_vector&);
    static std::unique_ptr<query_pager> pager(schema_ptr,
            shared_ptr<const cql3::selection::selection>,
            service::query_state&,
            const cql3::query_options&,
            lw_shared_ptr<query::read_command>,
            dht::partition_range_vector,
            ::shared_ptr<cql3::restrictions::statement_restrictions> filtering_restrictions = nullptr);
};

}
}
