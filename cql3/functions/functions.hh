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
 *
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

#include "function.hh"
#include "aggregate_fcts.hh"
#include "time_uuid_fcts.hh"
#include "uuid_fcts.hh"
#include "bytes_conversion_fcts.hh"
#include "aggregate_fcts.hh"
#include "bytes_conversion_fcts.hh"
#include "cql3/assignment_testable.hh"
#include "cql3/cql3_type.hh"
#include "cql3/column_identifier.hh"
#include "to_string.hh"
#include <unordered_map>
#include <boost/lexical_cast.hpp>

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
    static std::unordered_multimap<function_name, shared_ptr<function>> init();
public:
    static shared_ptr<column_specification> make_arg_spec(const sstring& receiver_ks, const sstring& receiver_cf,
            const function& fun, size_t i);
    static int get_overload_count(const function_name& name);
public:
    static shared_ptr<function> get(database& db,
                                    const sstring& keyspace,
                                    const function_name& name,
                                    const std::vector<shared_ptr<assignment_testable>>& provided_args,
                                    const sstring& receiver_ks,
                                    const sstring& receiver_cf,
                                    ::shared_ptr<column_specification> receiver = nullptr);
    template <typename AssignmentTestablePtrRange>
    static shared_ptr<function> get(database& db,
                                    const sstring& keyspace,
                                    const function_name& name,
                                    AssignmentTestablePtrRange&& provided_args,
                                    const sstring& receiver_ks,
                                    const sstring& receiver_cf,
                                    ::shared_ptr<column_specification> receiver = nullptr) {
        const std::vector<shared_ptr<assignment_testable>> args(std::begin(provided_args), std::end(provided_args));
        return get(db, keyspace, name, args, receiver_ks, receiver_cf, receiver);
    }
    static std::vector<shared_ptr<function>> find(const function_name& name);
    static shared_ptr<function> find(const function_name& name, const std::vector<data_type>& arg_types);
private:
    // This method and matchArguments are somewhat duplicate, but this method allows us to provide more precise errors in the common
    // case where there is no override for a given function. This is thus probably worth the minor code duplication.
    static void validate_types(database& db,
                              const sstring& keyspace,
                              shared_ptr<function> fun,
                              const std::vector<shared_ptr<assignment_testable>>& provided_args,
                              const sstring& receiver_ks,
                              const sstring& receiver_cf);
    static assignment_testable::test_result match_arguments(database& db, const sstring& keyspace,
            shared_ptr<function> fun,
            const std::vector<shared_ptr<assignment_testable>>& provided_args,
            const sstring& receiver_ks,
            const sstring& receiver_cf);
#if 0
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

    static bool type_equals(const std::vector<data_type>& t1, const std::vector<data_type>& t2);

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
