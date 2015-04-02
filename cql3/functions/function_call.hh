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
 * Copyright 2015 Cloudius Systems
 */

#pragma once

#include "function.hh"
#include "functions.hh"
#include "cql3/term.hh"
#include "cql3/constants.hh"
#include "db/marshal/collection_type.hh"
#include "core/print.hh"
#include "exceptions/exceptions.hh"
#include "cql3/lists.hh"
#include "cql3/sets.hh"
#include "cql3/maps.hh"

namespace cql3 {
namespace functions {

class function_call : public non_terminal {
    const shared_ptr<scalar_function> _fun;
    const std::vector<shared_ptr<term>> _terms;
public:
    function_call(shared_ptr<scalar_function> fun, std::vector<shared_ptr<term>> terms)
            : _fun(std::move(fun)), _terms(std::move(terms)) {
    }

    virtual bool uses_function(const sstring& ks_name, const sstring& function_name) const override {
        return _fun->uses_function(ks_name, function_name);
    }

    virtual void collect_marker_specification(shared_ptr<variable_specifications> bound_names) override {
        for (auto&& t : _terms) {
            t->collect_marker_specification(bound_names);
        }
    }

    virtual shared_ptr<terminal> bind(const query_options& options) override {
        return make_terminal(_fun, bind_and_get(options), options.get_serialization_format());
    }

    virtual bytes_opt bind_and_get(const query_options& options) override {
        std::vector<bytes> buffers;
        buffers.reserve(_terms.size());
        for (auto&& t : _terms) {
            // For now, we don't allow nulls as argument as no existing function needs it and it
            // simplify things.
            bytes_opt val = t->bind_and_get(options);
            if (!val) {
                throw exceptions::invalid_request_exception(sprint("Invalid null value for argument to %s", *_fun));
            }
            buffers.push_back(std::move(*val));
        }
        return execute_internal(options.get_serialization_format(), *_fun, std::move(buffers));
    }

private:
    static bytes execute_internal(serialization_format sf, scalar_function& fun, std::vector<bytes> params) {
        bytes result = fun.execute(sf, params);
        try {
            // Check the method didn't lied on it's declared return type
#if 0
            if (result != null)
#endif
            fun.return_type()->validate(result);
            return result;
        } catch (marshal_exception e) {
            throw runtime_exception(sprint("Return of function %s (%s) is not a valid value for its declared return type %s",
                                           fun, to_hex(result),
                                           *fun.return_type()->as_cql3_type()
                                           ));
        }
    }
public:
    virtual bool contains_bind_marker() const override {
        for (auto&& t : _terms) {
            if (t->contains_bind_marker()) {
                return true;
            }
        }
        return false;
    }

private:
    static shared_ptr<terminal> make_terminal(shared_ptr<function> fun, bytes_opt result, serialization_format sf)  {
        if (!dynamic_pointer_cast<shared_ptr<db::marshal::collection_type>>(fun->return_type())) {
            return ::make_shared<constants::value>(std::move(result));
        }

        auto ctype = static_pointer_cast<collection_type_impl>(fun->return_type());
        bytes_view res;
        if (result) {
            res = *result;
        }
        if (&ctype->_kind == &collection_type_impl::kind::list) {
            return make_shared(lists::value::from_serialized(std::move(res), static_pointer_cast<list_type_impl>(ctype), sf));
        } else if (&ctype->_kind == &collection_type_impl::kind::set) {
            return make_shared(sets::value::from_serialized(std::move(res), static_pointer_cast<set_type_impl>(ctype), sf));
        } else if (&ctype->_kind == &collection_type_impl::kind::map) {
            return make_shared(maps::value::from_serialized(std::move(res), static_pointer_cast<map_type_impl>(ctype), sf));
        }
        abort();
    }

public:
    class raw : public term::raw {
        function_name _name;
        std::vector<shared_ptr<term::raw>> _terms;
    public:
        raw(function_name name, std::vector<shared_ptr<term::raw>> terms)
            : _name(std::move(name)), _terms(std::move(terms)) {
        }

        virtual ::shared_ptr<term> prepare(const sstring& keyspace, ::shared_ptr<column_specification> receiver) override {
            std::vector<shared_ptr<assignment_testable>> args;
            args.reserve(_terms.size());
            std::transform(_terms.begin(), _terms.end(), std::back_inserter(args),
                    [] (auto&& x) -> shared_ptr<assignment_testable> {
                return x;
            });
            auto&& fun = functions::functions::get(keyspace, _name, args, receiver->ks_name, receiver->cf_name);
            if (!fun) {
                throw exceptions::invalid_request_exception(sprint("Unknown function %s called", _name));
            }
            if (fun->is_aggregate()) {
                throw exceptions::invalid_request_exception("Aggregation function are not supported in the where clause");
            }

            // Can't use static_pointer_cast<> because function is a virtual base class of scalar_function
            auto&& scalar_fun = dynamic_pointer_cast<scalar_function>(fun);

            // Functions.get() will complain if no function "name" type check with the provided arguments.
            // We still have to validate that the return type matches however
            if (!receiver->type->is_value_compatible_with(*scalar_fun->return_type())) {
                throw exceptions::invalid_request_exception(sprint("Type error: cannot assign result of function %s (type %s) to %s (type %s)",
                                                            fun->name(), fun->return_type()->as_cql3_type(),
                                                            receiver->name, receiver->type->as_cql3_type()));
            }

            if (scalar_fun->arg_types().size() != _terms.size()) {
                throw exceptions::invalid_request_exception(sprint("Incorrect number of arguments specified for function %s (expected %d, found %d)",
                                                            fun->name(), fun->arg_types().size(), _terms.size()));
            }

            std::vector<shared_ptr<term>> parameters;
            parameters.reserve(_terms.size());
            bool all_terminal = true;
            for (size_t i = 0; i < _terms.size(); ++i) {
                auto&& t = _terms[i]->prepare(keyspace, functions::make_arg_spec(receiver->ks_name, receiver->cf_name, *scalar_fun, i));
                if (dynamic_cast<non_terminal*>(t.get())) {
                    all_terminal = false;
                }
                parameters.push_back(t);
            }

            // If all parameters are terminal and the function is pure, we can
            // evaluate it now, otherwise we'd have to wait execution time
            if (all_terminal && scalar_fun->is_pure()) {
                return make_terminal(scalar_fun, execute(*scalar_fun, parameters), query_options::DEFAULT.get_serialization_format());
            } else {
                return ::make_shared<function_call>(scalar_fun, parameters);
            }
        }

    private:
        // All parameters must be terminal
        static bytes_opt execute(scalar_function& fun, std::vector<shared_ptr<term>> parameters) {
            std::vector<bytes> buffers;
            buffers.reserve(parameters.size());
            for (auto&& t : parameters) {
                assert(dynamic_cast<terminal*>(t.get()));
                // FIXME: converting bytes_opt to bytes.  Losing anything?
                auto&& param = static_cast<terminal*>(t.get())->get(query_options::DEFAULT);
                buffers.push_back(param ? std::move(*param) : bytes());
            }

            return execute_internal(serialization_format::internal(), fun, buffers);
        }

    public:
        virtual assignment_testable::test_result test_assignment(const sstring& keyspace, shared_ptr<column_specification> receiver) override {
            // Note: Functions.get() will return null if the function doesn't exist, or throw is no function matching
            // the arguments can be found. We may get one of those if an undefined/wrong function is used as argument
            // of another, existing, function. In that case, we return true here because we'll throw a proper exception
            // later with a more helpful error message that if we were to return false here.
            try {
                auto&& fun = functions::get(keyspace, _name, _terms, receiver->ks_name, receiver->cf_name);
                if (fun && receiver->type->equals(fun->return_type())) {
                    return assignment_testable::test_result::EXACT_MATCH;
                } else if (!fun || receiver->type->is_value_compatible_with(*fun->return_type())) {
                    return assignment_testable::test_result::WEAKLY_ASSIGNABLE;
                } else {
                    return assignment_testable::test_result::NOT_ASSIGNABLE;
                }
            } catch (exceptions::invalid_request_exception& e) {
                return assignment_testable::test_result::WEAKLY_ASSIGNABLE;
            }
        }

        virtual sstring to_string() const override {
            return sprint("%s(%s)", _name, join(", ", _terms));
        }
    };
};

}
}
