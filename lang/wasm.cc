/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#ifdef SCYLLA_ENABLE_WASMTIME

#include "wasm.hh"
#include "wasm_instance_cache.hh"
#include "concrete_types.hh"
#include "utils/utf8.hh"
#include "utils/ascii.hh"
#include "utils/date.h"
#include "db/config.hh"
#include <seastar/core/byteorder.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/util/defer.hh>
#include "seastarx.hh"

static logging::logger wasm_logger("wasm");

namespace wasm {

context::context(wasm::engine* engine_ptr, std::string name, instance_cache* cache) : engine_ptr(engine_ptr), function_name(name), cache(cache) {
}

static constexpr size_t WASM_PAGE_SIZE = 64 * 1024;

static uint32_t get_abi(wasmtime::Instance& instance, wasmtime::Store& store, uint8_t* data) {
    auto abi_export = instance.get(store, "_scylla_abi");
    if (!abi_export) {
        throw wasm::exception(format("ABI version export not found - please export `_scylla_abi` in the wasm module"));
    }
    wasmtime::Global* abi_global = std::get_if<wasmtime::Global>(&*abi_export);
    if (!abi_global) {
        throw wasm::exception(format("Exported object {} is not a global", "_scylla_abi"));
    }
    wasmtime::Val abi_val = abi_global->get(store);
    return seastar::read_le<uint32_t>((char*)data + abi_val.i32());
}

static wasmtime::Func import_func(wasmtime::Instance& instance, wasmtime::Store& store, std::string_view name) {
    auto func_export = instance.get(store, name);
    if (func_export) {
        wasmtime::Func* func = std::get_if<wasmtime::Func>(&*func_export);
        if (!func) {
            throw wasm::exception(format("Exported object {} is not a function", name));
        }
        return std::move(*func);
    } else {
        throw wasm::exception(format("Function {} was not found in given wasm source code", name));
    }
}

static wasmtime::Val call_func(wasmtime::Store& store, wasmtime::Func func, std::vector<wasmtime::Val> argv) {
    auto result = func.call(store, argv);
    if (!result) {
        throw wasm::exception("Calling wasm function failed: " + result.err().message());
    }
    std::vector<wasmtime::Val> result_vec = std::move(result).unwrap();
    if (result_vec.size() != 1) {
        throw wasm::exception(format("Unexpected number of returned values: {} (expected: 1)", result_vec.size()));
    }
    return std::move(result_vec[0]);
}

static void call_void_func(wasmtime::Store& store, wasmtime::Func func, std::vector<wasmtime::Val> argv) {
    auto result = func.call(store, argv);
    if (!result) {
        throw wasm::exception("Calling wasm function failed: " + result.err().message());
    }
    std::vector<wasmtime::Val> result_vec = std::move(result).unwrap();
    if (result_vec.size() != 0) {
        throw wasm::exception(format("Unexpected number of returned values: {} (expected: 0)", result_vec.size()));
    }
}

static std::pair<wasmtime::Instance, wasmtime::Func> create_instance_and_func(context& ctx, wasmtime::Store& store) {
    auto linker = wasmtime::Linker(ctx.engine_ptr->get());
    auto wasi_def = linker.define_wasi();
    if (!wasi_def) {
        throw wasm::exception(format("Setting up wasi failed: {}", wasi_def.err().message()));
    }
    auto cfg = wasmtime::WasiConfig();
    auto set_cfg = store.context().set_wasi(std::move(cfg));
    if (!set_cfg) {
        throw wasm::exception(format("Setting up wasi failed: {}", set_cfg.err().message()));
    }
    auto instance_res = linker.instantiate(store, *ctx.module);
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

static void init_abstract_arg(const abstract_type& t, const bytes_opt& param, std::vector<wasmtime::Val>& argv, wasmtime::Store& store, wasmtime::Instance& instance) {
        // set up exported memory's underlying buffer,
        // `memory` is required to be exported in the WebAssembly module
        auto memory_export = instance.get(store, "memory");
        if (!memory_export) {
            throw wasm::exception("memory export not found - please export `memory` in the wasm module");
        }
        auto memory = std::get<wasmtime::Memory>(*memory_export);
        uint8_t* data = memory.data(store).data();
        size_t mem_size = memory.size(store) * WASM_PAGE_SIZE;
        int32_t serialized_size = param ? param->size() : 0;
        if (serialized_size > std::numeric_limits<int32_t>::max()) {
            throw wasm::exception(format("Serialized parameter is too large: {} > {}", param->size(), std::numeric_limits<int32_t>::max()));
        }
        switch (uint32_t abi_ver = get_abi(instance, store, data)) {
            case 1: {
                auto grown = memory.grow(store, 1 + (sizeof(int32_t) + serialized_size - 1) / WASM_PAGE_SIZE); // for fitting serialized size + the buffer itself
                if (!grown) {
                    throw wasm::exception(format("Failed to grow wasm memory to {}: {}", serialized_size, grown.err().message()));
                }
                mem_size = std::move(grown).unwrap() * WASM_PAGE_SIZE;
                break;
            }
            case 2: {
                auto malloc_func = import_func(instance, store, "_scylla_malloc");
                import_func(instance, store, "_scylla_free");
                auto size = call_func(store, malloc_func, {int32_t(sizeof(int32_t) + serialized_size)});
                mem_size = size.i32();
                break;
            }
            default:
                throw wasm::exception(format("ABI version {} not recognized", abi_ver));
        }
        if (param) {
            // put the argument in wasm module's memory
            std::memcpy(data + mem_size, param->data(), serialized_size);
        } else {
            // size of -1 means that the value is null
            serialized_size = -1;
        }

        // the size of the struct in top 32 bits and the place inside wasm memory where the struct is placed in the bottom 32 bits
        int64_t arg_combined = ((int64_t)serialized_size << 32) | mem_size;
        argv.push_back(arg_combined);
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
        if (!param) {
            on_internal_error(wasm_logger, "init_arg_visitor does not accept null values");
        }
        init_abstract_arg(t, param, argv, store, instance);
    }
};

struct init_nullable_arg_visitor {
    const bytes_opt& param;
    std::vector<wasmtime::Val>& argv;
    wasmtime::Store& store;
    wasmtime::Instance& instance;

    void operator()(const abstract_type& t) {
        init_abstract_arg(t, param, argv, store, instance);
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
        expect_kind(wasmtime::ValKind::I64);
        auto memory_export = instance.get(store, "memory");
        if (!memory_export) {
            throw wasm::exception("memory export not found - please export `memory` in the wasm module");
        }
        auto memory = std::get<wasmtime::Memory>(*memory_export);
        uint8_t* mem_base = memory.data(store).data();
        uint8_t* data = mem_base + (val.i64() & 0xffffffff);
        int32_t ret_size = val.i64() >> 32;
        if (ret_size == -1) {
            return bytes_opt{};
        }
        bytes_opt ret = t.decompose(t.deserialize(bytes_view(reinterpret_cast<int8_t*>(data), ret_size)));

        if (get_abi(instance, store, mem_base) == 2) {
            auto free_func = import_func(instance, store, "_scylla_free");
            call_void_func(store, free_func, {wasmtime::Val((int32_t)val.i64())});
        }

        return ret;
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

seastar::future<bytes_opt> run_script(context& ctx, wasmtime::Store& store, wasmtime::Instance& instance, wasmtime::Func& func, const std::vector<data_type>& arg_types, const std::vector<bytes_opt>& params, data_type return_type, bool allow_null_input) {
    wasm_logger.debug("Running function {}", ctx.function_name);

    // Replenish the store with initial amount of fuel
    auto added = store.context().add_fuel(ctx.engine_ptr->initial_fuel_amount());
    if (!added) {
        co_await coroutine::return_exception(wasm::exception(added.err().message()));
    }
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
            co_await coroutine::return_exception(wasm::exception(format("Function {} cannot be called on null values", ctx.function_name)));
        }
    }
    uint64_t fuel_before = *store.context().fuel_consumed();

    auto result = func.call(store, argv);

    uint64_t consumed = *store.context().fuel_consumed() - fuel_before;
    wasm_logger.debug("Consumed {} fuel units", consumed);

    if (!result) {
        co_await coroutine::return_exception(wasm::instance_corrupting_exception("Calling wasm function failed: " + result.err().message()));
    }
    std::vector<wasmtime::Val> result_vec = std::move(result).unwrap();
    if (result_vec.size() != 1) {
      co_await coroutine::return_exception(wasm::exception(format("Unexpected number of returned values: {} (expected: 1)", result_vec.size())));
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

seastar::future<bytes_opt> run_script(context& ctx, const std::vector<data_type>& arg_types, const std::vector<bytes_opt>& params, data_type return_type, bool allow_null_input) {
    auto store = wasmtime::Store(ctx.engine_ptr->get());
    auto [instance, func] = create_instance_and_func(ctx, store);
    return run_script(ctx, store, instance, func, arg_types, params, return_type, allow_null_input);
}

seastar::future<bytes_opt> run_script(const db::functions::function_name& name, context& ctx, const std::vector<data_type>& arg_types, const std::vector<bytes_opt>& params, data_type return_type, bool allow_null_input) {
    wasm::instance_cache::value_type func_inst;
    std::exception_ptr ex;
    bytes_opt ret;
    try {
        func_inst = ctx.cache->get(name, arg_types, ctx).get0();
        ret = wasm::run_script(ctx, func_inst->instance->store, func_inst->instance->instance, func_inst->instance->func, arg_types, params, return_type, allow_null_input).get0();
    } catch (const wasm::instance_corrupting_exception& e) {
        func_inst->instance = std::nullopt;
        ex = std::current_exception();
    } catch (...) {
        ex = std::current_exception();
    }
    ctx.cache->recycle(func_inst);
    if (ex) {
        std::rethrow_exception(std::move(ex));
    }
    return make_ready_future<bytes_opt>(ret);
}
}

#endif
