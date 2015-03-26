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
#include "aggregate_fcts.hh"
#include "time_uuid_fcts.hh"
#include "uuid_fcts.hh"
#include "bytes_conversion_fcts.hh"
#include "aggregate_fcts.hh"
#include "bytes_conversion_fcts.hh"
#include "cql3/assignment_testable.hh"
#include "cql3/cql3_type.hh"
#include <unordered_map>

namespace cql3 {

namespace functions {

#if 0
    // We special case the token function because that's the only function whose argument types actually
    // depend on the table on which the function is called. Because it's the sole exception, it's easier
    // to handle it as a special case.
    private static final FunctionName TOKEN_FUNCTION_NAME = FunctionName.nativeFunction("token");
#endif

class functions {
    static thread_local std::unordered_multimap<function_name, shared_ptr<function>> _declared;
private:
    static std::unordered_multimap<function_name, shared_ptr<function>> init() {
        std::unordered_multimap<function_name, shared_ptr<function>> ret;
        auto declare = [&ret] (shared_ptr<function> f) { ret.emplace(f->name(), std::move(f)); };
        declare(aggregate_fcts::make_count_rows_function());
        declare(time_uuid_fcts::make_now_fct());
        declare(time_uuid_fcts::make_min_timeuuid_fct());
        declare(time_uuid_fcts::make_max_timeuuid_fct());
        declare(time_uuid_fcts::make_date_of_fct());
        declare(time_uuid_fcts::make_unix_timestamp_of_fcf());
        declare(make_uuid_fct());

        for (auto&& type : cql3_type::values()) {
            // Note: because text and varchar ends up being synonimous, our automatic makeToBlobFunction doesn't work
            // for varchar, so we special case it below. We also skip blob for obvious reasons.
            if (type == cql3_type::varchar || type == cql3_type::blob) {
                continue;
            }

            declare(make_to_blob_function(type->get_type()));
            declare(make_from_blob_function(type->get_type()));
        }
        declare(aggregate_fcts::make_count_function<int32_t>());
        declare(aggregate_fcts::make_max_function<int32_t>());
        declare(aggregate_fcts::make_min_function<int32_t>());

        declare(aggregate_fcts::make_count_function<int64_t>());
        declare(aggregate_fcts::make_max_function<int64_t>());
        declare(aggregate_fcts::make_min_function<int64_t>());

        //FIXME:
        //declare(aggregate_fcts::make_count_function<bytes>());
        //declare(aggregate_fcts::make_max_function<bytes>());
        //declare(aggregate_fcts::make_min_function<bytes>());

        // FIXME: more count/min/max

        declare(make_varchar_as_blob_fct());
        declare(make_blob_as_varchar_fct());
        declare(aggregate_fcts::make_sum_function<int32_t>());
        declare(aggregate_fcts::make_sum_function<int64_t>());
        declare(aggregate_fcts::make_avg_function<int32_t>());
        declare(aggregate_fcts::make_avg_function<int64_t>());
#if 0
        declare(AggregateFcts.sumFunctionForFloat);
        declare(AggregateFcts.sumFunctionForDouble);
        declare(AggregateFcts.sumFunctionForDecimal);
        declare(AggregateFcts.sumFunctionForVarint);
        declare(AggregateFcts.avgFunctionForFloat);
        declare(AggregateFcts.avgFunctionForDouble);
        declare(AggregateFcts.avgFunctionForVarint);
        declare(AggregateFcts.avgFunctionForDecimal);
#endif

        // also needed for smp:
#if 0
        MigrationManager.instance.register(new FunctionsMigrationListener());
#endif
        return ret;
    }

#if 0
    public static ColumnSpecification makeArgSpec(String receiverKs, String receiverCf, Function fun, int i)
    {
        return new ColumnSpecification(receiverKs,
                                       receiverCf,
                                       new ColumnIdentifier("arg" + i +  "(" + fun.name().toString().toLowerCase() + ")", true),
                                       fun.argTypes().get(i));
    }
#endif
    static int get_overload_count(const function_name& name) {
        return _declared.count(name);
    }

    static shared_ptr<function> get(const sstring& keyspace,
                                    const function_name& name,
                                    const std::vector<shared_ptr<assignment_testable>>& provided_args,
                                    const sstring& receiver_ks,
                                    const sstring& receiver_cf) {
#if 0
        // later
        if (name.has_keyspace()
            ? name.equals(TOKEN_FUNCTION_NAME)
            : name.name.equals(TOKEN_FUNCTION_NAME.name))
            return new TokenFct(Schema.instance.getCFMetaData(receiverKs, receiverCf));

        List<Function> candidates;
        if (!name.hasKeyspace())
        {
            // function name not fully qualified
            candidates = new ArrayList<>();
            // add 'SYSTEM' (native) candidates
            candidates.addAll(declared.get(name.asNativeFunction()));
            // add 'current keyspace' candidates
            candidates.addAll(declared.get(new FunctionName(keyspace, name.name)));
        }
        else
            // function name is fully qualified (keyspace + name)
            candidates = declared.get(name);

        if (candidates.isEmpty())
            return null;

        // Fast path if there is only one choice
        if (candidates.size() == 1)
        {
            Function fun = candidates.get(0);
            validateTypes(keyspace, fun, providedArgs, receiverKs, receiverCf);
            return fun;
        }

        List<Function> compatibles = null;
        for (Function toTest : candidates)
        {
            AssignmentTestable.TestResult r = matchAguments(keyspace, toTest, providedArgs, receiverKs, receiverCf);
            switch (r)
            {
                case EXACT_MATCH:
                    // We always favor exact matches
                    return toTest;
                case WEAKLY_ASSIGNABLE:
                    if (compatibles == null)
                        compatibles = new ArrayList<>();
                    compatibles.add(toTest);
                    break;
            }
        }

        if (compatibles == null || compatibles.isEmpty())
            throw new InvalidRequestException(String.format("Invalid call to function %s, none of its type signatures match (known type signatures: %s)",
                                                            name, toString(candidates)));

        if (compatibles.size() > 1)
            throw new InvalidRequestException(String.format("Ambiguous call to function %s (can be matched by following signatures: %s): use type casts to disambiguate",
                        name, toString(compatibles)));

        return compatibles.get(0);
#endif
        abort();
    }

    static std::vector<shared_ptr<function>> find(const function_name& name) {
        auto range = _declared.equal_range(name);
        std::vector<shared_ptr<function>> ret;
        for (auto i = range.first; i != range.second; ++i) {
            ret.push_back(i->second);
        }
        return ret;
    }

    static shared_ptr<function> find(const function_name& name, const std::vector<data_type>& arg_types) {
        assert(name.has_keyspace()); // : "function name not fully qualified";
        for (auto&& f : find(name)) {
            if (type_equals(f->arg_types(), arg_types)) {
                return f;
            }
        }
        return {};
    }

#if 0
    // This method and matchArguments are somewhat duplicate, but this method allows us to provide more precise errors in the common
    // case where there is no override for a given function. This is thus probably worth the minor code duplication.
    private static void validateTypes(String keyspace,
                                      Function fun,
                                      List<? extends AssignmentTestable> providedArgs,
                                      String receiverKs,
                                      String receiverCf)
    throws InvalidRequestException
    {
        if (providedArgs.size() != fun.argTypes().size())
            throw new InvalidRequestException(String.format("Invalid number of arguments in call to function %s: %d required but %d provided", fun.name(), fun.argTypes().size(), providedArgs.size()));

        for (int i = 0; i < providedArgs.size(); i++)
        {
            AssignmentTestable provided = providedArgs.get(i);

            // If the concrete argument is a bind variables, it can have any type.
            // We'll validate the actually provided value at execution time.
            if (provided == null)
                continue;

            ColumnSpecification expected = makeArgSpec(receiverKs, receiverCf, fun, i);
            if (!provided.testAssignment(keyspace, expected).isAssignable())
                throw new InvalidRequestException(String.format("Type error: %s cannot be passed as argument %d of function %s of type %s", provided, i, fun.name(), expected.type.asCQL3Type()));
        }
    }

    private static AssignmentTestable.TestResult matchAguments(String keyspace,
                                                               Function fun,
                                                               List<? extends AssignmentTestable> providedArgs,
                                                               String receiverKs,
                                                               String receiverCf)
    {
        if (providedArgs.size() != fun.argTypes().size())
            return AssignmentTestable.TestResult.NOT_ASSIGNABLE;

        // It's an exact match if all are exact match, but is not assignable as soon as any is non assignable.
        AssignmentTestable.TestResult res = AssignmentTestable.TestResult.EXACT_MATCH;
        for (int i = 0; i < providedArgs.size(); i++)
        {
            AssignmentTestable provided = providedArgs.get(i);
            if (provided == null)
            {
                res = AssignmentTestable.TestResult.WEAKLY_ASSIGNABLE;
                continue;
            }

            ColumnSpecification expected = makeArgSpec(receiverKs, receiverCf, fun, i);
            AssignmentTestable.TestResult argRes = provided.testAssignment(keyspace, expected);
            if (argRes == AssignmentTestable.TestResult.NOT_ASSIGNABLE)
                return AssignmentTestable.TestResult.NOT_ASSIGNABLE;
            if (argRes == AssignmentTestable.TestResult.WEAKLY_ASSIGNABLE)
                res = AssignmentTestable.TestResult.WEAKLY_ASSIGNABLE;
        }
        return res;
    }

    private static String toString(List<Function> funs)
    {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < funs.size(); i++)
        {
            if (i > 0) sb.append(", ");
            sb.append(funs.get(i));
        }
        return sb.toString();
    }

    // This is *not* thread safe but is only called in SchemaTables that is synchronized.
    public static void addFunction(AbstractFunction fun)
    {
        // We shouldn't get there unless that function don't exist
        assert find(fun.name(), fun.argTypes()) == null;
        declare(fun);
    }

    // Same remarks than for addFunction
    public static void removeFunction(FunctionName name, List<AbstractType<?>> argsTypes)
    {
        Function old = find(name, argsTypes);
        assert old != null && !old.isNative();
        declared.remove(old.name(), old);
    }

    // Same remarks than for addFunction
    public static void replaceFunction(AbstractFunction fun)
    {
        removeFunction(fun.name(), fun.argTypes());
        addFunction(fun);
    }

    public static List<Function> getReferencesTo(Function old)
    {
        List<Function> references = new ArrayList<>();
        for (Function function : declared.values())
            if (function.hasReferenceTo(old))
                references.add(function);
        return references;
    }

    public static Collection<Function> all()
    {
        return declared.values();
    }

    public static boolean typeEquals(AbstractType<?> t1, AbstractType<?> t2)
    {
        return t1.asCQL3Type().toString().equals(t2.asCQL3Type().toString());
    }

#endif

    static bool type_equals(const std::vector<data_type>& t1, const std::vector<data_type>& t2) {
#if 0
        if (t1.size() != t2.size())
            return false;
        for (int i = 0; i < t1.size(); i ++)
            if (!typeEquals(t1.get(i), t2.get(i)))
                return false;
        return true;
#endif
        abort();
    }

#if 0
    private static class FunctionsMigrationListener implements IMigrationListener
    {
        public void onCreateKeyspace(String ksName) { }
        public void onCreateColumnFamily(String ksName, String cfName) { }
        public void onCreateUserType(String ksName, String typeName) { }
        public void onCreateFunction(String ksName, String functionName) { }
        public void onCreateAggregate(String ksName, String aggregateName) { }

        public void onUpdateKeyspace(String ksName) { }
        public void onUpdateColumnFamily(String ksName, String cfName) { }
        public void onUpdateUserType(String ksName, String typeName) {
            for (Function function : all())
                if (function instanceof UDFunction)
                    ((UDFunction)function).userTypeUpdated(ksName, typeName);
        }
        public void onUpdateFunction(String ksName, String functionName) { }
        public void onUpdateAggregate(String ksName, String aggregateName) { }

        public void onDropKeyspace(String ksName) { }
        public void onDropColumnFamily(String ksName, String cfName) { }
        public void onDropUserType(String ksName, String typeName) { }
        public void onDropFunction(String ksName, String functionName) { }
        public void onDropAggregate(String ksName, String aggregateName) { }
    }
#endif
};

}
}
