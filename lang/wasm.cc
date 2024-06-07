/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "wasm.hh"
#include "wasm_instance_cache.hh"
#include "concrete_types.hh"
#include "db/config.hh"
#include <seastar/core/byteorder.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/util/defer.hh>
#include "seastarx.hh"
#include "rust/cxx.h"
#include "rust/wasmtime_bindings.hh"
#include <seastar/coroutine/exception.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include "lang/wasm_alien_thread_runner.hh"

logging::logger wasm_logger("wasm");

namespace wasm {

context::context(wasmtime::Engine& engine_ptr, std::string name, instance_cache& cache, uint64_t yield_fuel, uint64_t total_fuel)
    : engine_ptr(engine_ptr)
    , function_name(name)
    , cache(cache)
    , yield_fuel(yield_fuel)
    , total_fuel(total_fuel) {
}

static constexpr size_t WASM_PAGE_SIZE = 64 * 1024;

static void init_abstract_arg(const abstract_type& t, const bytes_opt& param, wasmtime::ValVec& argv, wasmtime::Store& store, wasmtime::Instance& instance) {
        // set up exported memory's underlying buffer,
        // `memory` is required to be exported in the WebAssembly module
        auto memory = wasmtime::get_memory(instance, store);
        size_t mem_size = memory->size(store) * WASM_PAGE_SIZE;
        if (param && param->size() > std::numeric_limits<int32_t>::max()) {
            throw wasm::exception(format("Serialized parameter is too large: {} > {}", param->size(), std::numeric_limits<int32_t>::max()));
        }
        int32_t serialized_size = param ? param->size() : 0;
        if (param) {
            switch (uint32_t abi_ver = wasmtime::get_abi(instance, store, *memory)) {
                case 1: {
                    auto pre_grow = memory->grow(store, 1 + (serialized_size - 1) / WASM_PAGE_SIZE);
                    mem_size = pre_grow * WASM_PAGE_SIZE;
                    break;
                }
                case 2: {
                    auto malloc_func = wasmtime::create_func(instance, store, "_scylla_malloc");
                    wasmtime::create_func(instance, store, "_scylla_free");
                    auto argv = wasmtime::get_val_vec();
                    argv->push_i32(serialized_size);
                    auto rets = wasmtime::get_val_vec();
                    rets->push_i32(0);

                    auto fut = wasmtime::get_func_future(store, *malloc_func, *argv, *rets);
                    // The future only calls malloc, which should complete quickly enough to not need yielding.
                    while (!fut->resume());
                    auto val = rets->pop_val();
                    mem_size = val->i32();
                    break;
                }
                default:
                    throw wasm::exception(format("ABI version {} not recognized", abi_ver));
            }
            // put the argument in wasm module's memory
            std::memcpy(memory->data(store) + mem_size, param->data(), serialized_size);
        } else {
            // size of -1 means that the value is null
            serialized_size = -1;
        }

        // the size of the struct in top 32 bits and the place inside wasm memory where the struct is placed in the bottom 32 bits
        int64_t arg_combined = ((int64_t)serialized_size << 32) | mem_size;
        argv.push_i64(arg_combined);
}

struct init_arg_visitor {
    const bytes_opt& param;
    wasmtime::ValVec& argv;
    wasmtime::Store& store;
    wasmtime::Instance& instance;

    void operator()(const boolean_type_impl&) {
        auto dv = boolean_type->deserialize(*param);
        argv.push_i32(int32_t(value_cast<bool>(dv)));
    }
    void operator()(const byte_type_impl&) {
        auto dv = byte_type->deserialize(*param);
        argv.push_i32(int32_t(value_cast<int8_t>(dv)));
    }
    void operator()(const short_type_impl&) {
        auto dv = short_type->deserialize(*param);
        argv.push_i32(int32_t(value_cast<int16_t>(dv)));
    }
    void operator()(const double_type_impl&) {
        auto dv = double_type->deserialize(*param);
        argv.push_f64(value_cast<double>(dv));
    }
    void operator()(const float_type_impl&) {
        auto dv = float_type->deserialize(*param);
        argv.push_f32(value_cast<float>(dv));
    }
    void operator()(const int32_type_impl&) {
        auto dv = int32_type->deserialize(*param);
        argv.push_i32(value_cast<int32_t>(dv));
    }
    void operator()(const long_type_impl&) {
        auto dv = long_type->deserialize(*param);
        argv.push_i64(value_cast<int64_t>(dv));
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
    wasmtime::ValVec& argv;
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
        auto memory = wasmtime::get_memory(instance, store);
        uint8_t* mem_base = memory->data(store);
        uint8_t* data = mem_base + (val.i64() & 0xffffffff);
        int32_t ret_size = val.i64() >> 32;
        if (ret_size == -1) {
            return bytes_opt{};
        }
        bytes_opt ret;
        const counter_type_impl* counter = dynamic_cast<const counter_type_impl*>(&t);
        if (counter) {
            ret = long_type->decompose(t.deserialize(bytes_view(reinterpret_cast<int8_t*>(data), ret_size)));
        } else {
            ret = t.decompose(t.deserialize(bytes_view(reinterpret_cast<int8_t*>(data), ret_size)));
        }

        if (wasmtime::get_abi(instance, store, *memory) == 2) {
            auto free_func = wasmtime::create_func(instance, store, "_scylla_free");
            auto argv = wasmtime::get_val_vec();
            argv->push_i32((int32_t)val.i64());
            auto rets = wasmtime::get_val_vec();
            auto free_fut = wasmtime::get_func_future(store, *free_func, *argv, *rets);
            // The future only calls free, which should complete quickly enough to not need yielding.
            while (!free_fut->resume());
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
            "funcref",
            "externref",
        };
        if (val.kind() != expected) {
            throw wasm::exception(format("Incorrect wasm value kind returned. Expected {}, got {}", kind_str[size_t(expected)], kind_str[size_t(val.kind())]));
        }
    }
};

seastar::future<> precompile(alien_thread_runner& alien_runner, context& ctx, const std::vector<sstring>& arg_names, std::string script) {
    seastar::promise<rust::Box<wasmtime::Module>> done;
    alien_runner.submit(done, [&engine_ptr = ctx.engine_ptr, script = std::move(script)] {
        return wasmtime::create_module(engine_ptr, rust::Str(script.data(), script.size()));
    });

    ctx.module = co_await done.get_future();
    std::exception_ptr ex;
    try {
        // After precompiling the module, we try creating a store, an instance and a function with it to make sure it's valid.
        // If we succeed, we drop them and keep the module, knowing that we will be able to create them again for UDF execution.
        ctx.module.value()->compile(ctx.engine_ptr);
        auto store = wasmtime::create_store(ctx.engine_ptr, ctx.total_fuel, ctx.yield_fuel);
        auto inst = create_instance(ctx.engine_ptr, **ctx.module, *store);
        create_func(*inst, *store, ctx.function_name);
        ctx.module.value()->release();
    } catch (const rust::Error& e) {
        ex = std::make_exception_ptr(wasm::exception(format("Compilation failed: {}", e.what())));
    }
    if (ex) {
        co_await coroutine::return_exception_ptr(std::move(ex));
    }
}
seastar::future<bytes_opt> run_script(context& ctx, wasmtime::Store& store, wasmtime::Instance& instance, wasmtime::Func& func, const std::vector<data_type>& arg_types, std::span<const bytes_opt> params, data_type return_type, bool allow_null_input) {
    wasm_logger.debug("Running function {}", ctx.function_name);

    rust::Box<wasmtime::ValVec> argv = wasmtime::get_val_vec();
    for (size_t i = 0; i < arg_types.size(); ++i) {
        const abstract_type& type = *arg_types[i];
        const bytes_opt& param = params[i];
        // If nulls are allowed, each type will be passed indirectly
        // as a struct {bool is_null; int32_t serialized_size, char[] serialized_buf}
        if (allow_null_input) {
            visit(type, init_nullable_arg_visitor{param, *argv, store, instance});
        } else if (param) {
            visit(type, init_arg_visitor{param, *argv, store, instance});
        } else {
            co_await coroutine::return_exception(wasm::exception(format("Function {} cannot be called on null values", ctx.function_name)));
        }
    }
    auto rets = wasmtime::get_val_vec();
    rets->push_i32(0);

    auto fut = wasmtime::get_func_future(store, func, *argv, *rets);
    bool stop = false;
    while (!stop) {
        std::exception_ptr eptr;
        try {
            stop = fut->resume();
        } catch (const rust::Error& e) {
            eptr = std::make_exception_ptr(wasm::instance_corrupting_exception(format("Calling wasm function failed: {}", e.what())));
        }
        if (eptr) {
            co_await coroutine::return_exception_ptr(eptr);
        }
        co_await coroutine::maybe_yield();
    }
    auto result = rets->pop_val();

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
        co_return from_val_visitor{*result, store, instance}(static_cast<const abstract_type&>(*return_type));
    } else {
        co_return visit(*return_type, from_val_visitor{*result, store, instance});
    }
}

seastar::future<bytes_opt> run_script(const db::functions::function_name& name, context& ctx, const std::vector<data_type>& arg_types, std::span<const bytes_opt> params, data_type return_type, bool allow_null_input) {
    wasm::instance_cache::value_type func_inst;
    std::exception_ptr ex;
    bytes_opt ret;
    try {
        func_inst = ctx.cache.get(name, arg_types, ctx).get();
        ret = wasm::run_script(ctx, *func_inst->instance->store, *func_inst->instance->instance, *func_inst->instance->func, arg_types, params, return_type, allow_null_input).get();
    } catch (const wasm::instance_corrupting_exception& e) {
        func_inst->instance = std::nullopt;
        ex = std::current_exception();
    } catch (...) {
        ex = std::current_exception();
    }
    if (func_inst) {
        // The construction of func_inst may have failed due to a insufficient free memory for compiled modules.
        ctx.cache.recycle(func_inst);
    }
    if (ex) {
        std::rethrow_exception(std::move(ex));
    }
    return make_ready_future<bytes_opt>(ret);
}
}
