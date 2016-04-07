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
 * Modified by ScyllaDB
 * Copyright (C) 2015 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "selector.hh"
#include "selector_factories.hh"
#include "cql3/functions/function.hh"

namespace cql3 {
namespace selection {

class abstract_function_selector : public selector {
protected:
    shared_ptr<functions::function> _fun;

    /**
     * The list used to pass the function arguments is recycled to avoid the cost of instantiating a new list
     * with each function call.
     */
    std::vector<bytes_opt> _args;
    std::vector<shared_ptr<selector>> _arg_selectors;
public:
    static shared_ptr<factory> new_factory(shared_ptr<functions::function> fun, shared_ptr<selector_factories> factories);

    abstract_function_selector(shared_ptr<functions::function> fun, std::vector<shared_ptr<selector>> arg_selectors)
            : _fun(std::move(fun)), _arg_selectors(std::move(arg_selectors)) {
        _args.resize(_arg_selectors.size());
    }

    virtual data_type get_type() override {
        return _fun->return_type();
    }

#if 0
    @Override
    public String toString()
    {
        return new StrBuilder().append(fun.name())
                               .append("(")
                               .appendWithSeparators(argSelectors, ", ")
                               .append(")")
                               .toString();
    }
#endif
};

template <typename T /* extends Function */>
class abstract_function_selector_for : public abstract_function_selector {
    shared_ptr<T> _tfun;  // We can't use static_pointer_cast due to virtual inheritance,
                          // so store it locally to amortize cost of dynamic_pointer_cast
protected:
    shared_ptr<T> fun() { return _tfun; }
public:
    abstract_function_selector_for(shared_ptr<T> fun, std::vector<shared_ptr<selector>> arg_selectors)
            : abstract_function_selector(fun, std::move(arg_selectors))
            , _tfun(dynamic_pointer_cast<T>(fun)) {
    }
};

}
}
