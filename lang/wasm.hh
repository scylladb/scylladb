/*
 * Copyright (C) 2021-present ScyllaDB
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
