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

#include "types.hh"
#include "native_scalar_function.hh"
#include "utils/UUID.hh"

namespace cql3 {

namespace functions {

inline
shared_ptr<function>
make_uuid_fct() {
    return make_native_scalar_function<false>("uuid", uuid_type, {},
            [] (serialization_format sf, const std::vector<bytes_opt>& parameters) -> bytes_opt {
        return {uuid_type->decompose(boost::any(utils::make_random_uuid()))};
    });
}

}
}
