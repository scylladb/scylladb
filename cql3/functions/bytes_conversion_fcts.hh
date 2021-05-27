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

#include "native_scalar_function.hh"
#include "exceptions/exceptions.hh"
#include <seastar/core/print.hh>
#include "cql3/cql3_type.hh"
#include "cql_serialization_format.hh"

namespace cql3 {

namespace functions {

// Most of the XAsBlob and blobAsX functions are basically no-op since everything is
// bytes internally. They only "trick" the type system.

inline
shared_ptr<function>
make_to_blob_function(data_type from_type) {
    auto name = from_type->as_cql3_type().to_string() + "asblob";
    return make_native_scalar_function<true>(name, bytes_type, { from_type },
            [] (cql_serialization_format sf, const std::vector<bytes_opt>& parameters) {
        return parameters[0];
    });
}

inline
shared_ptr<function>
make_from_blob_function(data_type to_type) {
    sstring name = sstring("blobas") + to_type->as_cql3_type().to_string();
    return make_native_scalar_function<true>(name, to_type, { bytes_type },
            [name, to_type] (cql_serialization_format sf, const std::vector<bytes_opt>& parameters) -> bytes_opt {
        auto&& val = parameters[0];
        if (!val) {
            return val;
        }
        try {
            to_type->validate(*val, sf);
            return val;
        } catch (marshal_exception& e) {
            using namespace exceptions;
            throw invalid_request_exception(format("In call to function {}, value 0x{} is not a valid binary representation for type {}",
                    name, to_hex(val), to_type->as_cql3_type().to_string()));
        }
    });
}

inline
shared_ptr<function>
make_varchar_as_blob_fct() {
    return make_native_scalar_function<true>("varcharasblob", bytes_type, { utf8_type },
            [] (cql_serialization_format sf, const std::vector<bytes_opt>& parameters) -> bytes_opt {
        return parameters[0];
    });
}

inline
shared_ptr<function>
make_blob_as_varchar_fct() {
    return make_native_scalar_function<true>("blobasvarchar", utf8_type, { bytes_type },
            [] (cql_serialization_format sf, const std::vector<bytes_opt>& parameters) -> bytes_opt {
        return parameters[0];
    });
}

}
}
