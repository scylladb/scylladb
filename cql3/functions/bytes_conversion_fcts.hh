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
 * Modified by Cloudius Systems
 *
 * Copyright 2015 Cloudius Systems
 */

#pragma once

#include "native_scalar_function.hh"
#include "exceptions/exceptions.hh"
#include "core/print.hh"

namespace cql3 {

namespace functions {

// Most of the XAsBlob and blobAsX functions are basically no-op since everything is
// bytes internally. They only "trick" the type system.

inline
shared_ptr<function>
make_to_blob_function(data_type from_type) {
    // FIXME: use cql3 type name
    auto name = from_type->name() + "asblob";
    return make_native_scalar_function<true>(name, bytes_type, { from_type },
            [] (serialization_format sf, const std::vector<bytes>& parameters) {
        return parameters[0];
    });
}

inline
shared_ptr<function>
make_from_blob_function(data_type to_type) {
    // FIXME: use cql3 type name
    sstring name = sstring("blobas") + to_type->name();
    return make_native_scalar_function<true>(name, to_type, { bytes_type },
            [name, to_type] (serialization_format sf, const std::vector<bytes>& parameters) {
        auto&& val = parameters[0];
        try {
#if 0
                    if (val != null)
#endif
            to_type->validate(val);
            return val;
        } catch (marshal_exception& e) {
            using namespace exceptions;
            throw invalid_request_exception(sprint(
                    "In call to function %s, value 0x%s is not a valid binary representation for type %s",
                    name, to_hex(val), /* FIXME: toType.asCQL3Type()*/ to_type->name()));
        }
    });
}

inline
shared_ptr<function>
make_varchar_as_blob_fct() {
    return make_native_scalar_function<true>("varcharasblob", bytes_type, { utf8_type },
            [] (serialization_format sf, const std::vector<bytes>& parameters) {
        return parameters[0];
    });
}

inline
shared_ptr<function>
make_blob_as_varchar_fct() {
    return make_native_scalar_function<true>("blobasvarchar", utf8_type, { bytes_type },
            [] (serialization_format sf, const std::vector<bytes>& parameters) {
        return parameters[0];
    });
}

}
}
