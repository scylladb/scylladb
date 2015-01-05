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

#ifndef CQL3_FUNCTIONS_NATIVE_AGGREGATE_FUNCTION_HH
#define CQL3_FUNCTIONS_NATIVE_AGGREGATE_FUNCTION_HH

#include "database.hh"
#include "native_function.hh"

namespace cql3 {
namespace functions {

/**
 * Base class for the <code>AggregateFunction</code> native classes.
 */
class native_aggregate_function : public native_function, public aggregate_function {
protected:
    native_aggregate_function(sstring name, shared_ptr<abstract_type> return_type,
            std::vector<shared_ptr<abstract_type>> arg_types)
        : native_function(std::move(name), std::move(return_type), std::move(arg_types)) {
    }

public:
    virtual bool is_aggregate() override final {
        return true;
    }
};

template <class Aggregate>
class native_aggregate_function_using : public native_aggregate_function {
public:
    native_aggregate_function_using(sstring name, shared_ptr<abstract_type> type)
            : native_aggregate_function(std::move(name), type, {}) {
    }
    virtual std::unique_ptr<aggregate> new_aggregate() override {
        return std::make_unique<Aggregate>();
    }
};

template <class Aggregate>
std::unique_ptr<native_aggregate_function>
make_native_aggregate_function_using(sstring name, shared_ptr<abstract_type> type) {
    return std::make_unique<native_aggregate_function_using<Aggregate>>(name, type);
}


}
}

#endif

