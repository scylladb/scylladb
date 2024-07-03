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
    class change_batch;

    using declared_t = std::unordered_multimap<function_name, shared_ptr<function>>;
    void add_agg_functions(declared_t& funcs);

class functions {
    friend class change_batch;
    std::unordered_multimap<function_name, shared_ptr<function>> init() noexcept;
protected:
    using declared_t = cql3::functions::declared_t;
    declared_t _declared;

    struct skip_init {};
    functions(skip_init) {};
public:
    lw_shared_ptr<column_specification> make_arg_spec(const sstring& receiver_ks, std::optional<const std::string_view> receiver_cf,
            const function& fun, size_t i);

    functions() : _declared(init()) {}

    shared_ptr<function> get(data_dictionary::database db,
                                    const sstring& keyspace,
                                    const function_name& name,
                                    const std::vector<shared_ptr<assignment_testable>>& provided_args,
                                    const sstring& receiver_ks,
                                    std::optional<const std::string_view> receiver_cf,
                                    const column_specification* receiver = nullptr);
    template <typename AssignmentTestablePtrRange>
    shared_ptr<function> get(data_dictionary::database db,
                                    const sstring& keyspace,
                                    const function_name& name,
                                    AssignmentTestablePtrRange&& provided_args,
                                    const sstring& receiver_ks,
                                    std::optional<const std::string_view> receiver_cf,
                                    const column_specification* receiver = nullptr) {
        const std::vector<shared_ptr<assignment_testable>> args(std::begin(provided_args), std::end(provided_args));
        return get(db, keyspace, name, args, receiver_ks, receiver_cf, receiver);
    }
    std::vector<shared_ptr<user_function>> get_user_functions(const sstring& keyspace);
    std::vector<shared_ptr<user_aggregate>> get_user_aggregates(const sstring& keyspace);
    boost::iterator_range<declared_t::iterator> find(const function_name& name);
    declared_t::iterator find_iter(const function_name& name, const std::vector<data_type>& arg_types);
    shared_ptr<function> find(const function_name& name, const std::vector<data_type>& arg_types);
    shared_ptr<function> mock_get(const function_name& name, const std::vector<data_type>& arg_types);
    // Used only by unittest.
    void clear_functions() noexcept;
    std::optional<function_name> used_by_user_aggregate(shared_ptr<user_function>);
    std::optional<function_name> used_by_user_function(const ut_name& user_type);
private:
    template <typename F>
    std::vector<shared_ptr<F>> get_filtered_transformed(const sstring& keyspace);

    // This method and matchArguments are somewhat duplicate, but this method allows us to provide more precise errors in the common
    // case where there is no override for a given function. This is thus probably worth the minor code duplication.
    void validate_types(data_dictionary::database db,
                              const sstring& keyspace,
                              const schema* schema_opt,
                              shared_ptr<function> fun,
                              const std::vector<shared_ptr<assignment_testable>>& provided_args,
                              const sstring& receiver_ks,
                              std::optional<const std::string_view> receiver_cf);
    assignment_testable::test_result match_arguments(data_dictionary::database db, const sstring& keyspace,
            const schema* schema_opt,
            shared_ptr<function> fun,
            const std::vector<shared_ptr<assignment_testable>>& provided_args,
            const sstring& receiver_ks,
            std::optional<const std::string_view> receiver_cf);

    bool type_equals(const std::vector<data_type>& t1, const std::vector<data_type>& t2);
};

// Getter for static functions object.
functions& instance();

// This class should be used via change_batch
class functions_changer : public functions {
public:
    void add_function(shared_ptr<function>);
    void replace_function(shared_ptr<function>);
    void remove_function(const function_name& name, const std::vector<data_type>& arg_types);
protected:
    functions_changer(skip_init) : functions(skip_init{}) {}
private:
    template <typename F>
    void with_udf_iter(const function_name& name, const std::vector<data_type>& arg_types, F&& f);
};

class change_batch : public functions_changer {
public:
    // Skip init as we copy data from static instance.
    change_batch() : functions_changer(skip_init{}) {
        _declared = instance()._declared;
    }

    // Commit allows to atomically apply changes to main functions instance.
    void commit();
};

}
}
