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
 * Copyright (C) 2015 ScyllaDB
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

#include "cql3/statements/property_definitions.hh"

#include "database.hh"

#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sstring.hh>
#include <experimental/optional>

namespace cql3 {

namespace statements {

class ks_prop_defs : public property_definitions {
public:
    static constexpr auto KW_DURABLE_WRITES = "durable_writes";
    static constexpr auto KW_REPLICATION = "replication";

    static constexpr auto REPLICATION_STRATEGY_CLASS_KEY = "class";
private:
    std::experimental::optional<sstring> _strategy_class;
public:
    void validate();
    std::map<sstring, sstring> get_replication_options() const;
    std::experimental::optional<sstring> get_replication_strategy_class() const;
    lw_shared_ptr<keyspace_metadata> as_ks_metadata(sstring ks_name);
    lw_shared_ptr<keyspace_metadata> as_ks_metadata_update(lw_shared_ptr<keyspace_metadata> old);

#if 0
    public KSMetaData asKSMetadataUpdate(KSMetaData old) throws RequestValidationException
    {
        String sClass = strategyClass;
        Map<String, String> sOptions = getReplicationOptions();
        if (sClass == null)
        {
            sClass = old.strategyClass.getName();
            sOptions = old.strategyOptions;
        }
        return KSMetaData.newKeyspace(old.name, sClass, sOptions, getBoolean(KW_DURABLE_WRITES, old.durableWrites));
    }
#endif
};

}

}
