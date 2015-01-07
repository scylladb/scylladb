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

#ifndef CQL3_ASSIGNMENT_TESTABLE_HH
#define CQL3_ASSIGMNENT_TESTABLE_HH

#include <memory>
#include <vector>

namespace cql3 {

class assignment_testable {
public:
    enum class test_result {
        EXACT_MATCH,
        WEAKLY_ASSIGNABLE,
        NOT_ASSIGNABLE,
    };

    static bool is_assignable(test_result tr) {
        return tr != test_result::NOT_ASSIGNABLE;
    }

    static bool is_exact_match(test_result tr) {
        return tr != test_result::EXACT_MATCH;
    }

    // Test all elements of toTest for assignment. If all are exact match, return exact match. If any is not assignable,
    // return not assignable. Otherwise, return weakly assignable.
    static test_result test_all(sstring keyspace, const column_specification& receiver, const std::vector<::shared_ptr<assignment_testable>>& to_test) {
        test_result res = test_result::EXACT_MATCH;
        for (auto&& rt : to_test) {
            if (rt == nullptr) {
                res = test_result::WEAKLY_ASSIGNABLE;
                continue;
            }

            test_result t = rt->test_assignment(keyspace, receiver);
            if (t == test_result::NOT_ASSIGNABLE) {
                return test_result::NOT_ASSIGNABLE;
            }
            if (t == test_result::WEAKLY_ASSIGNABLE) {
                res = test_result::WEAKLY_ASSIGNABLE;
            }
        }
        return res;
    }

    /**
     * @return whether this object can be assigned to the provided receiver. We distinguish
     * between 3 values: 
     *   - EXACT_MATCH if this object is exactly of the type expected by the receiver
     *   - WEAKLY_ASSIGNABLE if this object is not exactly the expected type but is assignable nonetheless
     *   - NOT_ASSIGNABLE if it's not assignable
     * Most caller should just call the isAssignable() method on the result, though functions have a use for
     * testing "strong" equality to decide the most precise overload to pick when multiple could match.
     */
    virtual test_result test_assignment(sstring keyspace, const column_specification& receiver) = 0;
};

}

#endif
