/*
 * Modified by ScyllaDB
 *
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.1 and Apache-2.0)
 */

#pragma once

#include "function.hh"
#include "cql3/assignment_testable.hh"
#include "cql3/cql3_type.hh"
#include "cql3/functions/function_name.hh"
#include "schema/schema.hh"
#include <unordered_map>
#include "data_dictionary/user_types_metadata.hh"
#include "utils/result.hh"
#include "exceptions/exceptions.hh"

namespace cql3 {

namespace functions {
//forward declarations
    class user_function;
    class user_aggregate;
    class change_batch;

    using declared_t = std::unordered_multimap<function_name, shared_ptr<function>>;
    void add_agg_functions(declared_t& funcs);

using function_resolution = utils::result_with_exception<shared_ptr<function>, exceptions::invalid_request_exception>;

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
            const function& fun, size_t i) const;

    functions() : _declared(init()) {}

    shared_ptr<function> get(data_dictionary::database db,
                                    const sstring& keyspace,
                                    const function_name& name,
                                    const std::vector<shared_ptr<assignment_testable>>& provided_args,
                                    const sstring& receiver_ks,
                                    std::optional<const std::string_view> receiver_cf,
                                    const column_specification* receiver = nullptr) const;
    // If receiver_schema is given, it is the schema of the receiver_cf table
    // and is used instead of looking that table up in db. This is needed
    // for a schema which isn't in db, like the output schema of
    // SELECT ... FROM MUTATION_FRAGMENTS().
    function_resolution try_get(data_dictionary::database db,
                                    const sstring& keyspace,
                                    const function_name& name,
                                    const std::vector<shared_ptr<assignment_testable>>& provided_args,
                                    const sstring& receiver_ks,
                                    std::optional<const std::string_view> receiver_cf,
                                    const column_specification* receiver = nullptr,
                                    const schema* receiver_schema = nullptr) const;
    template <typename AssignmentTestablePtrRange>
    shared_ptr<function> get(data_dictionary::database db,
                                    const sstring& keyspace,
                                    const function_name& name,
                                    AssignmentTestablePtrRange&& provided_args,
                                    const sstring& receiver_ks,
                                    std::optional<const std::string_view> receiver_cf,
                                    const column_specification* receiver = nullptr) const {
        const std::vector<shared_ptr<assignment_testable>> args(std::begin(provided_args), std::end(provided_args));
        return get(db, keyspace, name, args, receiver_ks, receiver_cf, receiver);
    }
    std::vector<shared_ptr<user_function>> get_user_functions(const sstring& keyspace) const;
    std::vector<shared_ptr<user_aggregate>> get_user_aggregates(const sstring& keyspace) const;
    std::ranges::subrange<declared_t::const_iterator> find(const function_name& name) const;
    declared_t::const_iterator find_iter(const function_name& name, const std::vector<data_type>& arg_types) const;
    shared_ptr<function> find(const function_name& name, const std::vector<data_type>& arg_types) const;
    shared_ptr<function> mock_get(const function_name& name, const std::vector<data_type>& arg_types) const;
    void add_function(shared_ptr<function>);
    void replace_function(shared_ptr<function>);
    void remove_function(const function_name& name, const std::vector<data_type>& arg_types);
    std::optional<function_name> used_by_user_aggregate(shared_ptr<user_function>) const;
    std::optional<function_name> used_by_user_function(const ut_name& user_type) const;
private:
    void with_udf_iter(const function_name& name, const std::vector<data_type>& arg_types, std::function<void(declared_t::iterator)> f);

    template <typename F>
    std::vector<shared_ptr<F>> get_filtered_transformed(const sstring& keyspace) const;

    // This method and matchArguments are somewhat duplicate, but this method allows us to provide more precise errors in the common
    // case where there is no override for a given function. This is thus probably worth the minor code duplication.
    void validate_types(data_dictionary::database db,
                              const sstring& keyspace,
                              const schema* schema_opt,
                              shared_ptr<function> fun,
                              const std::vector<shared_ptr<assignment_testable>>& provided_args,
                              const sstring& receiver_ks,
                              std::optional<const std::string_view> receiver_cf) const;
    std::optional<sstring> check_types(data_dictionary::database db,
                                       const sstring& keyspace,
                                       const schema* schema_opt,
                                       shared_ptr<function> fun,
                                       const std::vector<shared_ptr<assignment_testable>>& provided_args,
                                       const sstring& receiver_ks,
                                       std::optional<const std::string_view> receiver_cf) const;
    assignment_testable::test_result match_arguments(data_dictionary::database db, const sstring& keyspace,
            const schema* schema_opt,
            shared_ptr<function> fun,
            const std::vector<shared_ptr<assignment_testable>>& provided_args,
            const sstring& receiver_ks,
            std::optional<const std::string_view> receiver_cf) const;

    bool type_equals(const std::vector<data_type>& t1, const std::vector<data_type>& t2) const;
};

// Getter for static functions object.
const functions& instance();

// While an object of this class exists, record_used_function() adds to the
// given vector the name of each user-defined function or aggregate resolved
// while preparing a statement - and for an aggregate, also the names of the
// functions it uses. query_processor uses this to know which prepared
// statements to invalidate when a function is replaced or dropped.
// Preparing a statement doesn't yield, so a statement can't record another
// statement's functions.
class used_functions_recorder {
    used_functions_recorder* _previous;
    std::vector<function_name>& _names;
public:
    explicit used_functions_recorder(std::vector<function_name>& names);
    ~used_functions_recorder();
    used_functions_recorder(const used_functions_recorder&) = delete;
    used_functions_recorder& operator=(const used_functions_recorder&) = delete;
    static void record_used_function(const function& func);
};

class change_batch : public functions {
public:
    struct func_name_and_args {
        function_name name;
        std::vector<data_type> arg_types;
        bool aggregate;
    };
    std::vector<func_name_and_args> removed_functions;
    // Functions and aggregates replaced by a new version, recorded by the
    // schema merge so that listeners can be notified.
    std::vector<func_name_and_args> replaced_functions;

    // Skip init as we copy data from static instance.
    change_batch() : functions(skip_init{}) {
        _declared = instance()._declared;
    }

    // Commit allows to atomically apply changes to main functions instance.
    void commit();

    // Used only by unittest.
    void clear_functions() noexcept;

    void remove_function(function_name& name, std::vector<data_type>& arg_types, bool aggregate = false) {
        removed_functions.emplace_back(name, arg_types, aggregate);
        functions::remove_function(name, arg_types);
    };

    void remove_aggregate(function_name& name, std::vector<data_type>& arg_types) {
        remove_function(name, arg_types, true);
    }

    // Replace a function or aggregate whose argument types changed because a
    // user type they reference was altered. Unlike remove_function(), the
    // function isn't recorded as removed, because it wasn't dropped. If no
    // function is registered with the old argument types, this only logs an
    // error, and the new function is still registered.
    void replace_function_with_new_arg_types(const std::vector<data_type>& old_arg_types, shared_ptr<function> func) {
        functions::remove_function(func->name(), old_arg_types);
        if (find(func->name(), func->arg_types())) {
            replace_function(std::move(func));
        } else {
            add_function(std::move(func));
        }
    }
};

}
}
