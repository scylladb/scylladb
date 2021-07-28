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

#ifdef SCYLLA_ENABLE_WASMTIME

#include "wasm.hh"
#include "concrete_types.hh"
#include "utils/utf8.hh"
#include "utils/ascii.hh"
#include "utils/date.h"
#include "db/config.hh"
#include <seastar/core/coroutine.hh>
#include <seastar/util/defer.hh>
#include "seastarx.hh"

static logging::logger wasm_logger("wasm");

namespace wasm {

context::context(wasm::engine* engine_ptr, std::string name) : engine_ptr(engine_ptr), function_name(name) {
}

static std::pair<wasmtime::Instance, wasmtime::Func> create_instance_and_func(context& ctx, wasmtime::Store& store) {
    auto instance_res = wasmtime::Instance::create(store, *ctx.module, {});
    if (!instance_res) {
      throw wasm::exception(format("Creating a wasm runtime instance failed: {}", instance_res.err().message()));
    }
    auto instance = instance_res.unwrap();
    auto function_obj = instance.get(store, ctx.function_name);
    if (!function_obj) {
        throw wasm::exception(format("Function {} was not found in given wasm source code", ctx.function_name));
    }
    wasmtime::Func* func = std::get_if<wasmtime::Func>(&*function_obj);
    if (!func) {
        throw wasm::exception(format("Exported object {} is not a function", ctx.function_name));
    }
    return std::make_pair(std::move(instance), std::move(*func));
}

void compile(context& ctx, const std::vector<sstring>& arg_names, std::string script) {
    wasm_logger.debug("Compiling script {}", script);
    auto module = wasmtime::Module::compile(ctx.engine_ptr->get(), script);
    if (!module) {
        throw wasm::exception(format("Compilation failed: {}", module.err().message()));
    }
    ctx.module = module.unwrap();
    // Create the instance and extract function definition for validation purposes only
    wasmtime::Store store(ctx.engine_ptr->get());
    create_instance_and_func(ctx, store);
}

struct init_arg_visitor {
    const bytes_opt& param;
    std::vector<wasmtime::Val>& argv;
    wasmtime::Store& store;
    wasmtime::Instance& instance;

    void operator()(const boolean_type_impl&) {
        auto dv = boolean_type->deserialize(*param);
        auto val = wasmtime::Val(int32_t(value_cast<bool>(dv)));
        argv.push_back(std::move(val));
    }
    void operator()(const byte_type_impl&) {
        auto dv = byte_type->deserialize(*param);
        auto val = wasmtime::Val(int32_t(value_cast<int8_t>(dv)));
        argv.push_back(std::move(val));
    }
    void operator()(const short_type_impl&) {
        auto dv = short_type->deserialize(*param);
        auto val = wasmtime::Val(int32_t(value_cast<int16_t>(dv)));
        argv.push_back(std::move(val));
    }
    void operator()(const double_type_impl&) {
        auto dv = double_type->deserialize(*param);
        auto val = wasmtime::Val(value_cast<double>(dv));
        argv.push_back(std::move(val));
    }
    void operator()(const float_type_impl&) {
        auto dv = float_type->deserialize(*param);
        auto val = wasmtime::Val(value_cast<float>(dv));
        argv.push_back(std::move(val));
    }
    void operator()(const int32_type_impl&) {
        auto dv = int32_type->deserialize(*param);
        auto val = wasmtime::Val(value_cast<int32_t>(dv));
        argv.push_back(std::move(val));
    }
    void operator()(const long_type_impl&) {
        auto dv = long_type->deserialize(*param);
        auto val = wasmtime::Val(value_cast<int64_t>(dv));
        argv.push_back(std::move(val));
    }

    void operator()(const abstract_type& t) {
        // set up exported memory's underlying buffer,
        // `memory` is required to be exported in the WebAssembly module
        auto memory_export = instance.get(store, "memory");
        if (!memory_export) {
            throw wasm::exception("memory export not found - please export `memory` in the wasm module");
        }
        auto memory = std::get<wasmtime::Memory>(*memory_export);
        uint8_t* data = memory.data(store).data();
        size_t mem_size = memory.size(store);
        if (!param) {
            on_internal_error(wasm_logger, "init_arg_visitor does not accept null values");
        }
        int32_t serialized_size = param->size();
        if (serialized_size > std::numeric_limits<int32_t>::max()) {
            throw wasm::exception(format("Serialized parameter is too large: {} > {}", serialized_size, std::numeric_limits<int32_t>::max()));
        }
        auto grown = memory.grow(store, sizeof(int32_t) + serialized_size); // for fitting serialized size + the buffer itself
        if (!grown) {
            throw wasm::exception(format("Failed to grow wasm memory to {}: {}", serialized_size, grown.err().message()));
        }
        // put the size in wasm module's memory
        std::memcpy(data + mem_size, reinterpret_cast<char*>(&serialized_size), sizeof(int32_t));
        // put the argument in wasm module's memory
        std::memcpy(data + mem_size + sizeof(int32_t), param->data(), serialized_size);

        // the place inside wasm memory where the struct is placed
        argv.push_back(int32_t(mem_size));
    }
};

struct init_nullable_arg_visitor {
    const bytes_opt& param;
    std::vector<wasmtime::Val>& argv;
    wasmtime::Store& store;
    wasmtime::Instance& instance;

    void operator()(const abstract_type& t) {
        // set up exported memory's underlying buffer,
        // `memory` is required to be exported in the WebAssembly module
        auto memory_export = instance.get(store, "memory");
        if (!memory_export) {
            throw wasm::exception("memory export not found - please export `memory` in the wasm module");
        }
        auto memory = std::get<wasmtime::Memory>(*memory_export);
        uint8_t* data = memory.data(store).data();
        size_t mem_size = memory.size(store);
        const int32_t serialized_size = param ? param->size() : 0;
        if (serialized_size > std::numeric_limits<int32_t>::max()) {
            throw wasm::exception(format("Serialized parameter is too large: {} > {}", param->size(), std::numeric_limits<int32_t>::max()));
        }
        auto grown = memory.grow(store, sizeof(int32_t) + serialized_size); // for fitting the serialized size + the buffer itself
        if (!grown) {
            throw wasm::exception(format("Failed to grow wasm memory to {}: {}", serialized_size, grown.err().message()));
        }
        if (param) {
            // put the size in wasm module's memory
            std::memcpy(data + mem_size, reinterpret_cast<const char*>(&serialized_size), sizeof(int32_t));
            // put the argument in wasm module's memory
            std::memcpy(data + mem_size + sizeof(int32_t), param->data(), serialized_size);
        } else {
            // size of -1 means that the value is null
            const int32_t is_null = -1;
            std::memcpy(data + mem_size, reinterpret_cast<const char*>(&is_null), sizeof(int32_t));
        }

        // the place inside wasm memory where the struct is placed
        argv.push_back(int32_t(mem_size));
    }
};

struct from_val_visitor {
    const wasmtime::Val& val;
    wasmtime::Store& store;
    wasmtime::Instance& instance;

    bytes_opt operator()(const boolean_type_impl&) {
        expect_kind(wasmtime::ValKind::I32);
        return boolean_type->decompose(bool(val.i32()));
    }
    bytes_opt operator()(const byte_type_impl&) {
      expect_kind(wasmtime::ValKind::I32);
        return byte_type->decompose(int8_t(val.i32()));
    }
    bytes_opt operator()(const short_type_impl&) {
      expect_kind(wasmtime::ValKind::I32);
        return short_type->decompose(int16_t(val.i32()));
    }
    bytes_opt operator()(const double_type_impl&) {
      expect_kind(wasmtime::ValKind::F64);
        return double_type->decompose(val.f64());
    }
    bytes_opt operator()(const float_type_impl&) {
      expect_kind(wasmtime::ValKind::F32);
        return float_type->decompose(val.f32());
    }
    bytes_opt operator()(const int32_type_impl&) {
      expect_kind(wasmtime::ValKind::I32);
        return int32_type->decompose(val.i32());
    }
    bytes_opt operator()(const long_type_impl&) {
      expect_kind(wasmtime::ValKind::I64);
        return long_type->decompose(val.i64());
    }

    bytes_opt operator()(const abstract_type& t) {
        expect_kind(wasmtime::ValKind::I32);
        auto memory_export = instance.get(store, "memory");
        if (!memory_export) {
            throw wasm::exception("memory export not found - please export `memory` in the wasm module");
        }
        auto memory = std::get<wasmtime::Memory>(*memory_export);
        uint8_t* mem_base = memory.data(store).data();
        uint8_t* data = mem_base + val.i32();
        int32_t ret_size;
        std::memcpy(reinterpret_cast<char*>(&ret_size), data, 4);
        if (ret_size == -1) {
            return bytes_opt{};
        }
        data += sizeof(int32_t); // size of the return type was consumed
        return t.decompose(t.deserialize(bytes_view(reinterpret_cast<int8_t*>(data), ret_size)));
    }

    void expect_kind(wasmtime::ValKind expected) {
        // Created to match wasmtime::ValKind order
        static constexpr std::string_view kind_str[] = {
            "i32",
            "i64",
            "f32",
            "f64",
            "v128",
            "externref",
            "funcref",
        };
        if (val.kind() != expected) {
            throw wasm::exception(format("Incorrect wasm value kind returned. Expected {}, got {}", kind_str[size_t(expected)], kind_str[size_t(val.kind())]));
        }
    }
};

seastar::future<bytes_opt> run_script(context& ctx, const std::vector<data_type>& arg_types, const std::vector<bytes_opt>& params, data_type return_type, bool allow_null_input) {
    wasm_logger.debug("Running function {}", ctx.function_name);

    auto store = wasmtime::Store(ctx.engine_ptr->get());
    // Replenish the store with initial amount of fuel
    auto added = store.context().add_fuel(ctx.engine_ptr->initial_fuel_amount());
    if (!added) {
        co_return coroutine::make_exception(wasm::exception(added.err().message()));
    }
    auto [instance, func] = create_instance_and_func(ctx, store);
    std::vector<wasmtime::Val> argv;
    for (size_t i = 0; i < arg_types.size(); ++i) {
        const abstract_type& type = *arg_types[i];
        const bytes_opt& param = params[i];
        // If nulls are allowed, each type will be passed indirectly
        // as a struct {bool is_null; int32_t serialized_size, char[] serialized_buf}
        if (allow_null_input) {
            visit(type, init_nullable_arg_visitor{param, argv, store, instance});
        } else if (param) {
            visit(type, init_arg_visitor{param, argv, store, instance});
        } else {
            co_return coroutine::make_exception(wasm::exception(format("Function {} cannot be called on null values", ctx.function_name)));
        }
    }
    uint64_t fuel_before = *store.context().fuel_consumed();

    auto result = func.call(store, argv);

    uint64_t consumed = *store.context().fuel_consumed() - fuel_before;
    wasm_logger.debug("Consumed {} fuel units", consumed);

    if (!result) {
        co_return coroutine::make_exception(wasm::exception("Calling wasm function failed: " + result.err().message()));
    }
    std::vector<wasmtime::Val> result_vec = std::move(result).unwrap();
    if (result_vec.size() != 1) {
      co_return coroutine::make_exception(wasm::exception(format("Unexpected number of returned values: {} (expected: 1)", result_vec.size())));
    }

    // TODO: ABI for return values is experimental and subject to change in the future.
    // Currently, if a function is marked with `CALLED ON NULL INPUT` it is also capable
    // of returning nulls - which implies that all types are returned in its serialized form.
    // Otherwise, it is expected to return non-null values, which makes it possible to return
    // values of types natively supported by wasm via registers, without prior serialization
    // and avoiding allocations. This is however not ideal, especially that theoretically
    // it's perfectly fine for a function which `RETURNS NULL ON NULL INPUT` to also want to
    // return null on non-null input. The workaround for UDF programmers now is to always use
    // CALLED ON NULL INPUT if they want to be able to return nulls.
    // In order to properly decide on the ABI, an attempt should be made to provide library
    // wrappers for a few languages (C++, C, Rust), and see whether the ABI makes it easy
    // to interact with - we want to avoid poor user experience, and it's hard to judge it
    // before we actually have helper libraries.
    if (allow_null_input) {
        // Force calling the default method for abstract_type, which checks for nulls
        // and expects a serialized input
        co_return from_val_visitor{result_vec[0], store, instance}(static_cast<const abstract_type&>(*return_type));
    } else {
        co_return visit(*return_type, from_val_visitor{result_vec[0], store, instance});
    }
}

}

#endif
