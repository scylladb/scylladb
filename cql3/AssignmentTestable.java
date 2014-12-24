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

import java.util.Collection;

public interface AssignmentTestable
{
    /**
     * @return whether this object can be assigned to the provided receiver. We distinguish
     * between 3 values: 
     *   - EXACT_MATCH if this object is exactly of the type expected by the receiver
     *   - WEAKLY_ASSIGNABLE if this object is not exactly the expected type but is assignable nonetheless
     *   - NOT_ASSIGNABLE if it's not assignable
     * Most caller should just call the isAssignable() method on the result, though functions have a use for
     * testing "strong" equality to decide the most precise overload to pick when multiple could match.
     */
    public TestResult testAssignment(String keyspace, ColumnSpecification receiver);

    public enum TestResult
    {
        EXACT_MATCH, WEAKLY_ASSIGNABLE, NOT_ASSIGNABLE;

        public boolean isAssignable()
        {
            return this != NOT_ASSIGNABLE;
        }

        public boolean isExactMatch()
        {
            return this == EXACT_MATCH;
        }

        // Test all elements of toTest for assignment. If all are exact match, return exact match. If any is not assignable,
        // return not assignable. Otherwise, return weakly assignable.
        public static TestResult testAll(String keyspace, ColumnSpecification receiver, Collection<? extends AssignmentTestable> toTest)
        {
            TestResult res = EXACT_MATCH;
            for (AssignmentTestable rt : toTest)
            {
                if (rt == null)
                {
                    res = WEAKLY_ASSIGNABLE;
                    continue;
                }

                TestResult t = rt.testAssignment(keyspace, receiver);
                if (t == NOT_ASSIGNABLE)
                    return NOT_ASSIGNABLE;

                if (t == WEAKLY_ASSIGNABLE)
                    res = WEAKLY_ASSIGNABLE;
            }
            return res;
        }
    }
}
