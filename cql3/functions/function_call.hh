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

#include "cql3/term.hh"
#include "cql3/constants.hh"
#include "db/marshal/collection_type.hh"
#include "core/print.hh"

namespace cql3 {
namespace functions {

class function_call : public non_terminal {
    const std::unique_ptr<scalar_function> _fun;
    const std::vector<shared_ptr<term>> _terms;
public:
    function_call(std::unique_ptr<scalar_function> fun, std::vector<shared_ptr<term>> terms)
            : _fun(std::move(fun)), _terms(std::move(terms)) {
    }

    virtual bool uses_function(sstring ks_name, sstring function_name) const override {
        return _fun->uses_function(std::move(ks_name), std::move(function_name));
    }

    virtual void collect_marker_specification(shared_ptr<variable_specifications> bound_names) override {
        for (auto&& t : _terms) {
            t->collect_marker_specification(bound_names);
        }
    }

    virtual shared_ptr<terminal> bind(const query_options& options) override {
        return make_terminal(bind_and_get(options), options.get_protocol_version());
    }

    virtual bytes bind_and_get(const query_options& options) override {
        std::vector<bytes> buffers;
        buffers.reserve(_terms.size());
        for (auto&& t : _terms) {
            // For now, we don't allow nulls as argument as no existing function needs it and it
            // simplify things.
            bytes val = t->bind_and_get(options);
#if 0
            if (val == null)
                throw new InvalidRequestException(String.format("Invalid null value for argument to %s", fun));
#endif
            buffers.push_back(std::move(val));
        }
        return execute_internal(options.get_protocol_version(), std::move(buffers));
    }

private:
    bytes execute_internal(int protocol_version, std::vector<bytes> params) {
        bytes result = _fun->execute(protocol_version, params);
        try {
            // Check the method didn't lied on it's declared return type
#if 0
            if (result != null)
#endif
            _fun->return_type()->validate(result);
            return result;
        } catch (marshal_exception e) {
            throw runtime_exception(sprint("Return of function %s (%s) is not a valid value for its declared return type %s",
                                           *_fun, to_hex(result),
#if 0
                                           _fun->return_type()->as_cql3_type()
#else
                                           "FIXME: as_cql3_type"
#endif
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
    shared_ptr<terminal> make_terminal(bytes result, int version)  {
        if (!dynamic_pointer_cast<shared_ptr<db::marshal::collection_type>>(_fun->return_type())) {
#if 0
            return constants.value(result);
#else
            abort();
#endif
        }

#if 0
        switch (((CollectionType)fun.returnType()).kind)
        {
            case LIST: return Lists.Value.fromSerialized(result, (ListType)fun.returnType(), version);
            case SET:  return Sets.Value.fromSerialized(result, (SetType)fun.returnType(), version);
            case MAP:  return Maps.Value.fromSerialized(result, (MapType)fun.returnType(), version);
        }
        throw new AssertionError();
#else
        abort();
#endif
    }

#if 0
    public static class Raw implements Term.Raw
    {
        private FunctionName name;
        private final List<Term.Raw> terms;

        public Raw(FunctionName name, List<Term.Raw> terms)
        {
            this.name = name;
            this.terms = terms;
        }

        public Term prepare(String keyspace, ColumnSpecification receiver) throws InvalidRequestException
        {
            Function fun = Functions.get(keyspace, name, terms, receiver.ksName, receiver.cfName);
            if (fun == null)
                throw new InvalidRequestException(String.format("Unknown function %s called", name));
            if (fun.isAggregate())
                throw new InvalidRequestException("Aggregation function are not supported in the where clause");

            ScalarFunction scalarFun = (ScalarFunction) fun;

            // Functions.get() will complain if no function "name" type check with the provided arguments.
            // We still have to validate that the return type matches however
            if (!receiver.type.isValueCompatibleWith(scalarFun.returnType()))
                throw new InvalidRequestException(String.format("Type error: cannot assign result of function %s (type %s) to %s (type %s)",
                                                                scalarFun.name(), scalarFun.returnType().asCQL3Type(),
                                                                receiver.name, receiver.type.asCQL3Type()));

            if (fun.argTypes().size() != terms.size())
                throw new InvalidRequestException(String.format("Incorrect number of arguments specified for function %s (expected %d, found %d)",
                                                                fun.name(), fun.argTypes().size(), terms.size()));

            List<Term> parameters = new ArrayList<>(terms.size());
            boolean allTerminal = true;
            for (int i = 0; i < terms.size(); i++)
            {
                Term t = terms.get(i).prepare(keyspace, Functions.makeArgSpec(receiver.ksName, receiver.cfName, scalarFun, i));
                if (t instanceof NonTerminal)
                    allTerminal = false;
                parameters.add(t);
            }

            // If all parameters are terminal and the function is pure, we can
            // evaluate it now, otherwise we'd have to wait execution time
            return allTerminal && scalarFun.isPure()
                ? makeTerminal(scalarFun, execute(scalarFun, parameters), QueryOptions.DEFAULT.getProtocolVersion())
                : new FunctionCall(scalarFun, parameters);
        }

        // All parameters must be terminal
        private static ByteBuffer execute(ScalarFunction fun, List<Term> parameters) throws InvalidRequestException
        {
            List<ByteBuffer> buffers = new ArrayList<>(parameters.size());
            for (Term t : parameters)
            {
                assert t instanceof Term.Terminal;
                buffers.add(((Term.Terminal)t).get(QueryOptions.DEFAULT));
            }

            return executeInternal(Server.CURRENT_VERSION, fun, buffers);
        }

        public AssignmentTestable.TestResult testAssignment(String keyspace, ColumnSpecification receiver)
        {
            // Note: Functions.get() will return null if the function doesn't exist, or throw is no function matching
            // the arguments can be found. We may get one of those if an undefined/wrong function is used as argument
            // of another, existing, function. In that case, we return true here because we'll throw a proper exception
            // later with a more helpful error message that if we were to return false here.
            try
            {
                Function fun = Functions.get(keyspace, name, terms, receiver.ksName, receiver.cfName);
                if (fun != null && receiver.type.equals(fun.returnType()))
                    return AssignmentTestable.TestResult.EXACT_MATCH;
                else if (fun == null || receiver.type.isValueCompatibleWith(fun.returnType()))
                    return AssignmentTestable.TestResult.WEAKLY_ASSIGNABLE;
                else
                    return AssignmentTestable.TestResult.NOT_ASSIGNABLE;
            }
            catch (InvalidRequestException e)
            {
                return AssignmentTestable.TestResult.WEAKLY_ASSIGNABLE;
            }
        }

        @Override
        public String toString()
        {
            StringBuilder sb = new StringBuilder();
            sb.append(name).append("(");
            for (int i = 0; i < terms.size(); i++)
            {
                if (i > 0)
                    sb.append(", ");
                sb.append(terms.get(i));
            }
            return sb.append(")").toString();
        }
    }
#endif
};

}
}
