/*
 * Copyright (C) 2019-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "abstract_function.hh"
#include "scalar_function.hh"
#include "lang/lua.hh"
#include "lang/wasm.hh"
#include "data_dictionary/keyspace_element.hh"

namespace cql3 {
namespace functions {


class user_function final : public abstract_function, public scalar_function, public data_dictionary::keyspace_element {
public:
    struct lua_context {
        sstring bitcode;
        // FIXME: We should not need a copy in each function. It is here
        // because user_function::execute is only passed the
        // the runtime arguments.  We could
        // avoid it by having a runtime->execute(user_function) instead,
        // but that is a large refactoring. We could also store a
        // lua_runtime in a thread_local variable, but that is one extra
        // global.
        lua::runtime_config cfg;
    };

    using context = std::variant<lua_context, wasm::context>;

private:
    std::vector<sstring> _arg_names;
    sstring _body;
    sstring _language;
    bool _called_on_null_input;
    context _ctx;

public:
    user_function(function_name name, std::vector<data_type> arg_types, std::vector<sstring> arg_names, sstring body,
            sstring language, data_type return_type, bool called_on_null_input, context ctx);

    const std::vector<sstring>& arg_names() const { return _arg_names; }

    const sstring& body() const { return _body; }

    const sstring& language() const { return _language; }

    bool called_on_null_input() const { return _called_on_null_input; }

    virtual bool is_pure() const override;
    virtual bool is_native() const override;
    virtual bool is_aggregate() const override;
    virtual bool requires_thread() const override;
    virtual bytes_opt execute(std::span<const bytes_opt> parameters) override;

    virtual sstring keypace_name() const override { return name().keyspace; }
    virtual sstring element_name() const override { return name().name; }
    virtual sstring element_type() const override { return "function"; }
    virtual std::ostream& describe(std::ostream& os) const override;
};

}
}
