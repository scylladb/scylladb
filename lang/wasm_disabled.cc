/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// Built instead of lang/wasm*.cc when scylla is configured without wasmtime.

#include "lang/wasm.hh"
#include "lang/manager.hh"
#include "exceptions/exceptions.hh"

namespace wasm {

seastar::future<bytes_opt> run_script(
        const db::functions::function_name&, context&, const std::vector<data_type>&, std::span<const bytes_opt>, data_type, bool) {
    // Unreachable: create_wasm() below never produces a context.
    return seastar::make_exception_future<bytes_opt>(exception("scylla was built without wasmtime"));
}

} // namespace wasm

namespace lang {

void manager::init_wasm(const wasm_config&) {
}

future<> manager::start() {
    return make_ready_future<>();
}

future<> manager::stop() {
    return make_ready_future<>();
}

void manager::remove(const db::functions::function_name&, const std::vector<data_type>&) noexcept {
}

future<wasm::context> manager::create_wasm(sstring, const std::vector<sstring>&, std::string) {
    return make_exception_future<wasm::context>(exceptions::invalid_request_exception("WASM UDFs are not supported: scylla was built without wasmtime"));
}

} // namespace lang
