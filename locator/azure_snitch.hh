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

/*
 * Modified by ScyllaDB
 * Copyright (C) 2021-present ScyllaDB
 */
#pragma once

#include "locator/production_snitch_base.hh"

namespace locator {

class azure_snitch : public production_snitch_base {
public:
    static constexpr auto AZURE_SERVER_ADDR = "169.254.169.254";
    static constexpr auto AZURE_QUERY_PATH_TEMPLATE = "/metadata/instance/compute/{}?api-version=2020-09-01&format=text";

    static const std::string REGION_NAME_QUERY_PATH;
    static const std::string ZONE_NAME_QUERY_PATH;

    explicit azure_snitch(const sstring& fname = "", unsigned io_cpu_id = 0);
    virtual future<> start() override;
    virtual sstring get_name() const override {
        return "org.apache.cassandra.locator.AzureSnitch";
    }
protected:
    future<> load_config();
    future<sstring> azure_api_call(sstring path);
    future<sstring> read_property_file();
};

} // namespace locator
