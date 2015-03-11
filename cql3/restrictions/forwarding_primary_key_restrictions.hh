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

#include "cql3/restrictions/primary_key_restrictions.hh"

namespace cql3 {

namespace restrictions {

/**
 * A <code>primary_key_restrictions</code> which forwards all its method calls to another 
 * <code>primary_key_restrictions</code>. Subclasses should override one or more methods to modify the behavior 
 * of the backing <code>primary_key_restrictions</code> as desired per the decorator pattern. 
 */
class forwarding_primary_key_restrictions : public primary_key_restrictions {
protected:
    /**
     * Returns the backing delegate instance that methods are forwarded to.
     */
    virtual ::shared_ptr<primary_key_restrictions> get_delegate() = 0;

public:
    virtual bool uses_function(const sstring& ks_name, const sstring& function_name) override {
        return get_delegate()->uses_function(ks_name, function_name);
    }

    virtual std::vector<const column_definition*> get_column_defs() override {
        return get_delegate()->get_column_defs();
    }

    virtual void merge_with(::shared_ptr<restriction> restriction) override {
        get_delegate()->merge_with(restriction);
    }

#if 0
    virtual bool has_supporting_index(::shared_ptr<secondary_index_manager> index_manager) override {
        return get_delegate()->has_supporting_index(index_manager);
    }
#endif

    virtual std::vector<bytes> values_as_serialized_tuples(const query_options& options) override {
        return get_delegate()->values_as_serialized_tuples(options);
    }

    virtual std::vector<query::range> bounds(const query_options& options) override {
        return get_delegate()->bounds(options);
    }

    virtual bool is_on_token() override {
        return get_delegate()->is_on_token();
    }

    virtual bool is_multi_column() override {
        return get_delegate()->is_multi_column();
    }

    virtual bool is_slice() override {
        return get_delegate()->is_slice();
    }

    virtual bool is_contains() override {
        return get_delegate()->is_contains();
    }

    virtual bool is_IN() override {
        return get_delegate()->is_IN();
    }

    virtual bool empty() override {
        return get_delegate()->empty();
    }

    virtual uint32_t size() override {
        return get_delegate()->size();
    }

#if 0
    virtual void addIndexExpressionTo(List<IndexExpression> expressions, QueryOptions options) {
        get_delegate()->addIndexExpressionTo(expressions, options);
    }
#endif    
};

}
}
