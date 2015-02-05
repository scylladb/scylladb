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

#include <vector>

#include "core/shared_ptr.hh"
#include "core/sstring.hh"
#include "cql3/restrictions/restriction.hh"
#include "types.hh"

namespace cql3 {

namespace restrictions {

/**
 * Base class for <code>Restriction</code>s
 */
class abstract_restriction : public restriction {
public:
    virtual bool is_on_token() override {
        return false;
    }

    virtual bool is_multi_column() override {
        return false;
    }

    virtual bool is_slice() override {
        return false;
    }

    virtual bool is_EQ() override {
        return false;
    }

    virtual bool is_IN() override {
        return false;
    }

    virtual bool is_contains() override {
        return false;
    }

    virtual bool has_bound(statements::bound b) override {
        return true;
    }

    virtual std::vector<bytes_opt> bounds(statements::bound b, const query_options& options) override {
        return values(options);
    }

    virtual bool is_inclusive(statements::bound b) override {
        return true;
    }

protected:
#if 0
    protected static ByteBuffer validateIndexedValue(ColumnSpecification columnSpec,
                                                     ByteBuffer value)
                                                     throws InvalidRequestException
    {
        checkNotNull(value, "Unsupported null value for indexed column %s", columnSpec.name);
        checkFalse(value.remaining() > 0xFFFF, "Index expression values may not be larger than 64K");
        return value;
    }
#endif
    /**
     * Checks if the specified term is using the specified function.
     *
     * @param term the term to check
     * @param ks_name the function keyspace name
     * @param function_name the function name
     * @return <code>true</code> if the specified term is using the specified function, <code>false</code> otherwise.
     */
    static bool uses_function(::shared_ptr<term> term, const sstring& ks_name, const sstring& function_name) {
        return bool(term) && term->uses_function(ks_name, function_name);
    }

    /**
     * Checks if one of the specified term is using the specified function.
     *
     * @param terms the terms to check
     * @param ks_name the function keyspace name
     * @param function_name the function name
     * @return <code>true</code> if one of the specified term is using the specified function, <code>false</code> otherwise.
     */
    static bool uses_function(std::vector<::shared_ptr<term>> terms, const sstring& ks_name, const sstring& function_name) {
        for (auto&& value : terms) {
            if (uses_function(value, ks_name, function_name)) {
                return true;
            }
        }
        return false;
    }
};

}

}
