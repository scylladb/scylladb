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
 * Copyright 2014 Cloudius Systems
 */

#ifndef CQL3_FUNCTIONS_NATIVE_SCALAR_FUNCTION_HH_
#define CQL3_FUNCTIONS_NATIVE_SCALAR_FUNCTION_HH_

#include "native_function.hh"
#include "scalar_function.hh"

namespace cql3 {
namespace functions {

/**
 * Base class for the <code>ScalarFunction</code> native classes.
 */
class native_scalar_function : public native_function, public scalar_function {
protected:
    native_scalar_function(sstring name, data_type return_type, std::vector<data_type> args_type)
            : native_function(std::move(name), std::move(return_type), std::move(args_type)) {
    }

public:
    virtual bool is_aggregate() override {
        return false;
    }
};

}
}

#endif /* CQL3_FUNCTIONS_NATIVE_SCALAR_FUNCTION_HH_ */
