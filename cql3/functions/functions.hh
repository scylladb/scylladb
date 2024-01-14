/*
 * Modified by ScyllaDB
 *
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include "function.hh"
#include "cql3/assignment_testable.hh"
#include "cql3/cql3_type.hh"
#include "cql3/functions/function_name.hh"
#include "schema/schema.hh"
#include <unordered_map>

namespace cql3 {

namespace functions {
//forward declarations
    class user_function;
    class user_aggregate;

    using declared_t = std::unordered_multimap<function_name, shared_ptr<function>>;
    void add_agg_functions(declared_t& funcs);

class functions {
    using declared_t = cql3::functions::declared_t;
    static thread_local declared_t _declared;
private:
    static std::unordered_multimap<function_name, shared_ptr<function>> init() noexcept;
public:
    static lw_shared_ptr<column_specification> make_arg_spec(const sstring& receiver_ks, std::optional<const std::string_view> receiver_cf,
            const function& fun, size_t i);
public:
    static shared_ptr<function> get(data_dictionary::database db,
                                    const sstring& keyspace,
                                    const function_name& name,
                                    const std::vector<shared_ptr<assignment_testable>>& provided_args,
                                    const sstring& receiver_ks,
                                    std::optional<const std::string_view> receiver_cf,
                                    const column_specification* receiver = nullptr);
    template <typename AssignmentTestablePtrRange>
    static shared_ptr<function> get(data_dictionary::database db,
                                    const sstring& keyspace,
                                    const function_name& name,
                                    AssignmentTestablePtrRange&& provided_args,
                                    const sstring& receiver_ks,
                                    std::optional<const std::string_view> receiver_cf,
                                    const column_specification* receiver = nullptr) {
        const std::vector<shared_ptr<assignment_testable>> args(std::begin(provided_args), std::end(provided_args));
        return get(db, keyspace, name, args, receiver_ks, receiver_cf, receiver);
    }
    static std::vector<shared_ptr<user_function>> get_user_functions(const sstring& keyspace);
    static std::vector<shared_ptr<user_aggregate>> get_user_aggregates(const sstring& keyspace);
    static boost::iterator_range<declared_t::iterator> find(const function_name& name);
    static declared_t::iterator find_iter(const function_name& name, const std::vector<data_type>& arg_types);
    static shared_ptr<function> find(const function_name& name, const std::vector<data_type>& arg_types);
    static shared_ptr<function> mock_get(const function_name& name, const std::vector<data_type>& arg_types);
    static void clear_functions() noexcept;
    static void add_function(shared_ptr<function>);
    static void replace_function(shared_ptr<function>);
    static void remove_function(const function_name& name, const std::vector<data_type>& arg_types);
    static std::optional<function_name> used_by_user_aggregate(shared_ptr<user_function>);
    static std::optional<function_name> used_by_user_function(const ut_name& user_type);
private:
    template <typename F>
    static void with_udf_iter(const function_name& name, const std::vector<data_type>& arg_types, F&& f);

    template <typename F>
    static std::vector<shared_ptr<F>> get_filtered_transformed(const sstring& keyspace);

    // This method and matchArguments are somewhat duplicate, but this method allows us to provide more precise errors in the common
    // case where there is no override for a given function. This is thus probably worth the minor code duplication.
    static void validate_types(data_dictionary::database db,
                              const sstring& keyspace,
                              const schema* schema_opt,
                              shared_ptr<function> fun,
                              const std::vector<shared_ptr<assignment_testable>>& provided_args,
                              const sstring& receiver_ks,
                              std::optional<const std::string_view> receiver_cf);
    static assignment_testable::test_result match_arguments(data_dictionary::database db, const sstring& keyspace,
            const schema* schema_opt,
            shared_ptr<function> fun,
            const std::vector<shared_ptr<assignment_testable>>& provided_args,
            const sstring& receiver_ks,
            std::optional<const std::string_view> receiver_cf);

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
