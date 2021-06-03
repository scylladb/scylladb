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
 * Copyright (C) 2015-present ScyllaDB
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
#include "log.hh"
#include <unordered_map>

namespace cql3 {

namespace functions {
    using declared_t = std::unordered_multimap<function_name, shared_ptr<function>>;
    void add_agg_functions(declared_t& funcs);

class functions {
    using declared_t = cql3::functions::declared_t;
    static thread_local declared_t _declared;
private:
    static std::unordered_multimap<function_name, shared_ptr<function>> init() noexcept;
public:
    static lw_shared_ptr<column_specification> make_arg_spec(const sstring& receiver_ks, const sstring& receiver_cf,
            const function& fun, size_t i);
public:
    static shared_ptr<function> get(database& db,
                                    const sstring& keyspace,
                                    const function_name& name,
                                    const std::vector<shared_ptr<assignment_testable>>& provided_args,
                                    const sstring& receiver_ks,
                                    const sstring& receiver_cf,
                                    const column_specification* receiver = nullptr);
    template <typename AssignmentTestablePtrRange>
    static shared_ptr<function> get(database& db,
                                    const sstring& keyspace,
                                    const function_name& name,
                                    AssignmentTestablePtrRange&& provided_args,
                                    const sstring& receiver_ks,
                                    const sstring& receiver_cf,
                                    const column_specification* receiver = nullptr) {
        const std::vector<shared_ptr<assignment_testable>> args(std::begin(provided_args), std::end(provided_args));
        return get(db, keyspace, name, args, receiver_ks, receiver_cf, receiver);
    }
    static boost::iterator_range<declared_t::iterator> find(const function_name& name);
    static declared_t::iterator find_iter(const function_name& name, const std::vector<data_type>& arg_types);
    static shared_ptr<function> find(const function_name& name, const std::vector<data_type>& arg_types);
    static void clear_functions() noexcept;
    static void add_function(shared_ptr<function>);
    static void replace_function(shared_ptr<function>);
    static void remove_function(const function_name& name, const std::vector<data_type>& arg_types);
private:
    template <typename F>
    static void with_udf_iter(const function_name& name, const std::vector<data_type>& arg_types, F&& f);

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
