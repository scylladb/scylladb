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
package org.apache.cassandra.cql3;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class VariableSpecifications
{
    private final List<ColumnIdentifier> variableNames;
    private final ColumnSpecification[] specs;

    public VariableSpecifications(List<ColumnIdentifier> variableNames)
    {
        this.variableNames = variableNames;
        this.specs = new ColumnSpecification[variableNames.size()];
    }

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

    public List<ColumnSpecification> getSpecifications()
    {
        return Arrays.asList(specs);
    }

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
}
