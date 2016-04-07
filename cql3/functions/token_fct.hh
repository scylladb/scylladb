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
 * Modified by ScyllaDB
 *
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

#pragma once

#include "types.hh"
#include "native_scalar_function.hh"
#include "dht/i_partitioner.hh"
#include "utils/UUID.hh"

namespace cql3 {
namespace functions {

class token_fct: public native_scalar_function {
private:
    schema_ptr _schema;

public:
    token_fct(schema_ptr s)
            : native_scalar_function("token",
                    dht::global_partitioner().get_token_validator(),
                    s->partition_key_type()->types())
                    , _schema(s) {
    }

    bytes_opt execute(cql_serialization_format sf, const std::vector<bytes_opt>& parameters) override {
        auto key = partition_key::from_optional_exploded(*_schema, parameters);
        auto tok = dht::global_partitioner().get_token(*_schema, key);
        warn(unimplemented::cause::VALIDATION);
        return dht::global_partitioner().token_to_bytes(tok);
    }
};

}
}
