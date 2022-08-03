/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "types.hh"
#include <seastar/core/future.hh>
#include "lang/wasm_engine.hh"
#include "db/functions/function_name.hh"

namespace wasm {

class instance_cache;

struct exception : public std::exception {
    std::string _msg;
public:
    explicit exception(std::string_view msg) : _msg(msg) {}
    const char* what() const noexcept {
        return _msg.c_str();
    }
};

struct instance_corrupting_exception : public exception {
    explicit instance_corrupting_exception(std::string_view msg) : exception(msg) {}
};

#ifdef SCYLLA_ENABLE_WASMTIME

struct context {
    wasm::engine* engine_ptr;
    std::optional<wasmtime::Module> module;
    std::string function_name;
    instance_cache* cache;

    context(wasm::engine* engine_ptr, std::string name, instance_cache* cache);
};

void compile(context& ctx, const std::vector<sstring>& arg_names, std::string script);

seastar::future<bytes_opt> run_script(context& ctx, const std::vector<data_type>& arg_types, const std::vector<bytes_opt>& params, data_type return_type, bool allow_null_input);

seastar::future<bytes_opt> run_script(const db::functions::function_name& name, context& ctx, const std::vector<data_type>& arg_types, const std::vector<bytes_opt>& params, data_type return_type, bool allow_null_input);

#else

struct context {
    context(wasm::engine*, std::string, instance_cache*) {
        throw wasm::exception("WASM support was not enabled during compilation!");
    }
};

inline void compile(context&, const std::vector<sstring>&, std::string) {
    throw wasm::exception("WASM support was not enabled during compilation!");
}

inline seastar::future<bytes_opt> run_script(context&, const std::vector<data_type>&, const std::vector<bytes_opt>&, data_type, bool) {
    throw wasm::exception("WASM support was not enabled during compilation!");
}

inline seastar::future<bytes_opt> run_script(const db::functions::function_name& name, context& ctx, const std::vector<data_type>& arg_types, const std::vector<bytes_opt>& params, data_type return_type, bool allow_null_input) {
    throw wasm::exception("WASM support was not enabled during compilation!");
}
#endif

}
