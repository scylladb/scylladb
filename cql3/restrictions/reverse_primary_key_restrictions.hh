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

#include "cql3/restrictions/forwarding_primary_key_restrictions.hh"

namespace cql3 {

namespace restrictions {

/**
 * <code>PrimaryKeyRestrictions</code> decorator that reverse the slices.
 */
template <typename ValueType>
class reversed_primary_key_restrictions : public forwarding_primary_key_restrictions<ValueType> {
private:
    ::shared_ptr<primary_key_restrictions<ValueType>> _restrictions;
    using bounds_range_type = typename primary_key_restrictions<ValueType>::bounds_range_type;
protected:
    virtual ::shared_ptr<primary_key_restrictions<ValueType>> get_delegate() const override {
        return _restrictions;
    }
public:
    reversed_primary_key_restrictions(shared_ptr<primary_key_restrictions<ValueType>> restrictions)
        : _restrictions(std::move(restrictions))
    { }

    virtual std::vector<bounds_range_type> bounds_ranges(const query_options& options) const override {
        auto ranges = _restrictions->bounds_ranges(options);
        for (auto&& range : ranges) {
            range.reverse();
        }
        return ranges;
    }

    virtual bool is_inclusive(statements::bound bound) const override {
        return _restrictions->is_inclusive(reverse(bound));
    }

    sstring to_string() const override {
        return sprint("Reversed(%s)", forwarding_primary_key_restrictions<ValueType>::to_string());
    }
};

}
}
