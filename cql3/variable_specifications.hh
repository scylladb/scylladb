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
 * Copyright 2015 Cloudius Systems
 *
 * Modified by Cloudius Systems
 */

#pragma once

#include "cql3/column_specification.hh"
#include "cql3/column_identifier.hh"

#include <experimental/optional>
#include <vector>

namespace cql3 {

class variable_specifications final {
private:
    std::vector<shared_ptr<column_identifier>> _variable_names;
    std::vector<::shared_ptr<column_specification>> _specs;

public:
    variable_specifications(const std::vector<::shared_ptr<column_identifier>>& variable_names)
        : _variable_names{variable_names}
        , _specs{variable_names.size()}
    { }

    /**
     * Returns an empty instance of <code>VariableSpecifications</code>.
     * @return an empty instance of <code>VariableSpecifications</code>
     */
    static ::shared_ptr<variable_specifications> empty() {
        return ::make_shared<variable_specifications>(std::vector<::shared_ptr<column_identifier>>{});
    }

    size_t size() const {
        return _variable_names.size();
    }

    std::vector<::shared_ptr<column_specification>> get_specifications() const & {
        return std::vector<::shared_ptr<column_specification>>(_specs.begin(), _specs.end());
    }

    std::vector<::shared_ptr<column_specification>> get_specifications() && {
        return std::move(_specs);
    }

    void add(int32_t bind_index, ::shared_ptr<column_specification> spec) {
        auto name = _variable_names[bind_index];
        // Use the user name, if there is one
        if (name) {
            spec = ::make_shared<column_specification>(spec->ks_name, spec->cf_name, name, spec->type);
        }
        _specs[bind_index] = spec;
    }

#if 0
    @Override
    public String toString()
    {
        return Arrays.toString(specs);
    }
#endif
};

}
