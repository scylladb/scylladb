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
 * Copyright 2014 Cloudius Systems
 *
 * Modified by Cloudius Systems
 */

#ifndef CQL3_VARIABLE_SPECIFICATIONS_HH
#define CQL3_VARIABLE_SPECIFICATIONS_HH

#include "cql3/column_specification.hh"
#include "cql3/column_identifier.hh"

#include <vector>
#include <list>

namespace cql3 {

class variable_specifications {
private:
    const std::list<column_identifier> _variable_names;
    const std::vector<column_specification> _specs;

public:
    variable_specifications(const std::list<column_identifier>& variable_names)
        : _variable_names(variable_names)
    { }
#if 0
    {
        this.variableNames = variableNames;
        this.specs = new ColumnSpecification[variableNames.size()];
    }
#endif

#if 0
    /**
     * Returns an empty instance of <code>VariableSpecifications</code>.
     * @return an empty instance of <code>VariableSpecifications</code>
     */
    public static VariableSpecifications empty()
    {
        return new VariableSpecifications(Collections.<ColumnIdentifier> emptyList());
    }

    public int size()
    {
        return variableNames.size();
    }
#endif

    std::list<column_specification> get_specifications() const
    {
        return std::list<column_specification>(_specs.begin(), _specs.end());
    }

#if 0
    public void add(int bindIndex, ColumnSpecification spec)
    {
        ColumnIdentifier name = variableNames.get(bindIndex);
        // Use the user name, if there is one
        if (name != null)
            spec = new ColumnSpecification(spec.ksName, spec.cfName, name, spec.type);
        specs[bindIndex] = spec;
    }

    @Override
    public String toString()
    {
        return Arrays.toString(specs);
    }
#endif
};

}

#endif
