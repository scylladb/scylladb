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
 * Copyright (C) 2015 ScyllaDB
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
#include "gms/versioned_value.hh"
#include "message/messaging_service.hh"

namespace gms {

constexpr char versioned_value::DELIMITER;
constexpr const char versioned_value::DELIMITER_STR[];
constexpr const char* versioned_value::STATUS_BOOTSTRAPPING;
constexpr const char* versioned_value::STATUS_NORMAL;
constexpr const char* versioned_value::STATUS_LEAVING;
constexpr const char* versioned_value::STATUS_LEFT;
constexpr const char* versioned_value::STATUS_MOVING;
constexpr const char* versioned_value::REMOVING_TOKEN;
constexpr const char* versioned_value::REMOVED_TOKEN;
constexpr const char* versioned_value::HIBERNATE;
constexpr const char* versioned_value::SHUTDOWN;
constexpr const char* versioned_value::REMOVAL_COORDINATOR;

versioned_value versioned_value::factory::network_version() {
    return versioned_value(sprint("%s",netw::messaging_service::current_version));
}

}
