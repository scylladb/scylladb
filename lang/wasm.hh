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

namespace wasm {

struct exception : public std::exception {
    std::string _msg;
public:
    explicit exception(std::string_view msg) : _msg(msg) {}
    const char* what() const noexcept {
        return _msg.c_str();
    }
};

#ifdef SCYLLA_ENABLE_WASMTIME

struct context {
    wasm::engine* engine_ptr;
    std::optional<wasmtime::Module> module;
    std::string function_name;

    context(wasm::engine* engine_ptr, std::string name);
};

void compile(context& ctx, const std::vector<sstring>& arg_names, std::string script);

seastar::future<bytes_opt> run_script(context& ctx, const std::vector<data_type>& arg_types, const std::vector<bytes_opt>& params, data_type return_type, bool allow_null_input);

#else

struct context {
    context(wasm::engine*, std::string) {
        throw wasm::exception("WASM support was not enabled during compilation!");
    }
};

inline void compile(context&, const std::vector<sstring>&, std::string) {
    throw wasm::exception("WASM support was not enabled during compilation!");
}

inline seastar::future<bytes_opt> run_script(context&, const std::vector<data_type>&, const std::vector<bytes_opt>&, data_type, bool) {
    throw wasm::exception("WASM support was not enabled during compilation!");
}

#endif

}
