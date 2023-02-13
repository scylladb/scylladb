/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

use anyhow::Context;
use anyhow::{anyhow, Result};
use core::task::Poll;
use futures::future::{BoxFuture, Future};
mod memory_creator;
use memory_creator::ScyllaMemoryCreator;
mod test_memory_creator;
use test_memory_creator::TestScyllaMemoryCreator;

#[cxx::bridge(namespace = "wasmtime")]
mod ffi {
    enum ValKind {
        I32,
        I64,
        F32,
        F64,
        V128,
        FuncRef,
        ExternRef,
    }
    extern "Rust" {
        // We export opaque types directly correlated to wasmtime types with the same names.
        // Each of these types is a wrapper that for each of its methods calls the corresponding
        // wasmtime method on the underlying struct.
        type Instance;
        fn create_instance(
            engine: &Engine,
            module: &Module,
            store: &mut Store,
        ) -> Result<Box<Instance>>;

        type Module;
        fn create_module(engine: &mut Engine, script: &str) -> Result<Box<Module>>;
        fn raw_size(self: &Module) -> usize;
        fn is_compiled(self: &Module) -> bool;
        fn compile(self: &mut Module, engine: &mut Engine) -> Result<()>;
        fn release(self: &mut Module);
        fn add_user(self: &mut Module);
        fn remove_user(self: &mut Module);
        fn user_count(self: &Module) -> usize;

        type Store;
        fn create_store(
            engine: &mut Engine,
            total_fuel: u64,
            yield_fuel: u64,
        ) -> Result<Box<Store>>;

        type Memory;
        fn data(self: &Memory, store: &Store) -> *mut u8;
        fn size(self: &Memory, store: &Store) -> u64;
        fn grow(self: &mut Memory, store: &mut Store, delta: u64) -> Result<u64>;
        fn get_memory(instance: &Instance, store: &mut Store) -> Result<Box<Memory>>;
        fn get_abi(instance: &Instance, store: &mut Store, memory: &Memory) -> Result<u32>;

        type Engine;
        fn create_engine(max_size: u32) -> Result<Box<Engine>>;
        fn create_test_engine(max_size: u32, fail_after: usize) -> Result<Box<Engine>>;

        type Func;
        fn create_func(
            instance: &Instance,
            store: &mut Store,
            function_name: &str,
        ) -> Result<Box<Func>>;

        type Val;
        fn kind(self: &Val) -> Result<ValKind>;
        fn i32(self: &Val) -> Result<i32>;
        fn i64(self: &Val) -> Result<i64>;
        fn f32(self: &Val) -> Result<f32>;
        fn f64(self: &Val) -> Result<f64>;

        // vector of wasmtime values, used for passing arguments to a function and returning results from a function
        type ValVec;
        fn push_i32(self: &mut ValVec, val: i32) -> Result<()>;
        fn push_i64(self: &mut ValVec, val: i64) -> Result<()>;
        fn push_f32(self: &mut ValVec, val: f32) -> Result<()>;
        fn push_f64(self: &mut ValVec, val: f64) -> Result<()>;
        fn pop_val(self: &mut ValVec) -> Result<Box<Val>>;
        fn get_val_vec() -> Result<Box<ValVec>>;

        // type and methods for returning and executing a WebAssembly function asynchronously
        type Fut<'a>;
        unsafe fn resume(self: &mut Fut<'_>) -> Result<bool>;
        unsafe fn get_func_future<'a>(
            store: &'a mut Store,
            func: &'a Func,
            args: &'a ValVec,
            rets: &'a mut ValVec,
        ) -> Result<Box<Fut<'a>>>;
    }
}

pub struct Instance {
    wasmtime_instance: wasmtime::Instance,
}

fn create_instance(engine: &Engine, module: &Module, store: &mut Store) -> Result<Box<Instance>> {
    let mut linker = wasmtime::Linker::new(&engine.wasmtime_engine);
    wasmtime_wasi::add_to_linker(&mut linker, |s| s).context("Failed to add wasi to linker")?;
    let wasmtime_module = module
        .wasmtime_module
        .as_ref()
        .ok_or_else(|| anyhow!("Module is not compiled"))?;

    let mut inst_fut =
        Box::pin(linker.instantiate_async(&mut store.wasmtime_store, wasmtime_module));
    let mut ctx = core::task::Context::from_waker(futures::task::noop_waker_ref());

    loop {
        // If the instantiation uses async imports in the future, we should return the future here.
        // For now, we just poll it to completion.
        match inst_fut.as_mut().poll(&mut ctx) {
            Poll::Pending => {}
            Poll::Ready(Ok(inst)) => {
                return Ok(Box::new(Instance {
                    wasmtime_instance: inst,
                }));
            }
            Poll::Ready(Err(e)) => {
                return Err(anyhow!("Failed to instantiate module: {:?}", e));
            }
        }
    }
}

pub struct Module {
    serialized_module: Vec<u8>,
    wasmtime_module: Option<wasmtime::Module>,
    references: usize,
}

fn create_module(engine: &mut Engine, script: &str) -> Result<Box<Module>> {
    let module_bytes = engine
        .wasmtime_engine
        .precompile_module((&script).as_bytes())
        .map_err(|e| anyhow!("Compilation failed: {:?}", e))?;
    let module = Box::new(Module {
        serialized_module: module_bytes,
        wasmtime_module: None,
        references: 0,
    });
    Ok(module)
}

impl Module {
    fn raw_size(&self) -> usize {
        self.serialized_module.len()
    }
    fn is_compiled(&self) -> bool {
        self.wasmtime_module.is_some()
    }
    fn compile(&mut self, engine: &mut Engine) -> Result<()> {
        if self.is_compiled() {
            return Ok(());
        }
        // `deserialize` is safe because we put the result of `precompile_module` as input.
        let module = unsafe {
            wasmtime::Module::deserialize(&engine.wasmtime_engine, &self.serialized_module)
                .map_err(|e| anyhow!("Deserialization failed: {:?}", e))?
        };
        self.wasmtime_module = Some(module);
        Ok(())
    }
    fn release(&mut self) {
        self.wasmtime_module = None;
    }
    fn add_user(&mut self) {
        self.references += 1;
    }
    fn remove_user(&mut self) {
        self.references -= 1;
    }
    fn user_count(&self) -> usize {
        self.references
    }
}

pub struct Store {
    wasmtime_store: wasmtime::Store<wasmtime_wasi::WasiCtx>,
}

fn create_store(engine: &mut Engine, total_fuel: u64, yield_fuel: u64) -> Result<Box<Store>> {
    let wasi = wasmtime_wasi::WasiCtxBuilder::new().build();
    let mut store = wasmtime::Store::new(&engine.wasmtime_engine, wasi);
    store.out_of_fuel_async_yield(
        ((total_fuel + yield_fuel - 1) / yield_fuel) as u64,
        yield_fuel,
    );
    Ok(Box::new(Store {
        wasmtime_store: store,
    }))
}

pub struct Memory {
    wasmtime_memory: wasmtime::Memory,
}

impl Memory {
    fn data(&self, store: &Store) -> *mut u8 {
        self.wasmtime_memory.data_ptr(&store.wasmtime_store)
    }
    fn size(&self, store: &Store) -> u64 {
        self.wasmtime_memory.size(&store.wasmtime_store)
    }
    fn grow(&self, store: &mut Store, delta: u64) -> Result<u64> {
        self.wasmtime_memory.grow(&mut store.wasmtime_store, delta)
    }
}

fn get_memory(instance: &Instance, store: &mut Store) -> Result<Box<Memory>> {
    let export = instance
        .wasmtime_instance
        .get_export(&mut store.wasmtime_store, "memory")
        .ok_or_else(|| {
            anyhow!("Memory export not found - please export `memory` in the wasm module")
        })?;
    let memory = export
        .into_memory()
        .ok_or_else(|| anyhow!("Exported object memory is not a WebAssembly memory"))?;
    Ok(Box::new(Memory {
        wasmtime_memory: memory,
    }))
}

fn get_abi(instance: &Instance, store: &mut Store, memory: &Memory) -> Result<u32> {
    let export = instance
        .wasmtime_instance
        .get_export(&mut store.wasmtime_store, "_scylla_abi")
        .ok_or_else(|| {
            anyhow!("ABI export not found - please export `_scylla_abi` in the wasm module")
        })?;
    let global = export
        .into_global()
        .ok_or_else(|| anyhow!("Exported object _scylla_abi is not a WebAssembly global"))?;
    if let wasmtime::Val::I32(x) = global.get(&mut store.wasmtime_store) {
        let mut bytes = [0u8; 4];
        memory
            .wasmtime_memory
            .read(&store.wasmtime_store, x as usize, &mut bytes)?;
        Ok(u32::from_le_bytes(bytes))
    } else {
        Err(anyhow!("Exported global _scylla_abi is not an i32"))
    }
}

pub struct Engine {
    wasmtime_engine: wasmtime::Engine,
}

fn create_engine(max_size: u32) -> Result<Box<Engine>> {
    let mut config = wasmtime::Config::new();
    config.async_support(true);
    config.consume_fuel(true);
    // ScyllaMemoryCreator uses malloc (from seastar) to allocate linear memory
    config.with_host_memory(std::sync::Arc::new(ScyllaMemoryCreator::new(
        max_size as usize,
    )));
    // The following configuration settings make wasmtime allocate only as much memory as it needs
    config.static_memory_maximum_size(0);
    config.dynamic_memory_reserved_for_growth(0);
    config.dynamic_memory_guard_size(0);
    config.max_wasm_stack(128 * 1024);
    config.async_stack_size(256 * 1024);

    let engine =
        wasmtime::Engine::new(&config).map_err(|e| anyhow!("Failed to create engine: {:?}", e))?;
    Ok(Box::new(Engine {
        wasmtime_engine: engine,
    }))
}

fn create_test_engine(max_size: u32, fail_after: usize) -> Result<Box<Engine>> {
    let mut config = wasmtime::Config::new();
    config.async_support(true);
    config.consume_fuel(true);
    // ScyllaMemoryCreator uses malloc (from seastar) to allocate linear memory
    config.with_host_memory(std::sync::Arc::new(TestScyllaMemoryCreator::new(
        max_size as usize,
        fail_after,
    )));
    // The following configuration settings make wasmtime allocate only as much memory as it needs
    config.static_memory_maximum_size(0);
    config.dynamic_memory_reserved_for_growth(0);
    config.dynamic_memory_guard_size(0);
    config.max_wasm_stack(128 * 1024);
    config.async_stack_size(256 * 1024);

    let engine =
        wasmtime::Engine::new(&config).map_err(|e| anyhow!("Failed to create engine: {:?}", e))?;
    Ok(Box::new(Engine {
        wasmtime_engine: engine,
    }))
}

pub struct Func {
    wasmtime_func: wasmtime::Func,
}

fn create_func(instance: &Instance, store: &mut Store, name: &str) -> Result<Box<Func>> {
    let export = instance
        .wasmtime_instance
        .get_export(&mut store.wasmtime_store, name)
        .ok_or_else(|| anyhow!("Function {name} was not found in given wasm source code"))?;
    let func = export
        .into_func()
        .ok_or_else(|| anyhow!("Exported object {name} is not a function"))?;
    Ok(Box::new(Func {
        wasmtime_func: func,
    }))
}

pub struct Val {
    wasmtime_val: wasmtime::Val,
}

impl Val {
    fn kind(&self) -> Result<ffi::ValKind> {
        match self.wasmtime_val {
            wasmtime::Val::I32(_) => Ok(ffi::ValKind::I32),
            wasmtime::Val::I64(_) => Ok(ffi::ValKind::I64),
            wasmtime::Val::F32(_) => Ok(ffi::ValKind::F32),
            wasmtime::Val::F64(_) => Ok(ffi::ValKind::F64),
            wasmtime::Val::V128(_) => Ok(ffi::ValKind::V128),
            wasmtime::Val::FuncRef(_) => Ok(ffi::ValKind::FuncRef),
            wasmtime::Val::ExternRef(_) => Ok(ffi::ValKind::ExternRef),
        }
    }
    fn i32(&self) -> Result<i32> {
        match self.wasmtime_val {
            wasmtime::Val::I32(val) => Ok(val),
            _ => Err(anyhow!("Expected i32")),
        }
    }
    fn i64(&self) -> Result<i64> {
        match self.wasmtime_val {
            wasmtime::Val::I64(val) => Ok(val),
            _ => Err(anyhow!("Expected i64")),
        }
    }
    fn f32(&self) -> Result<f32> {
        match self.wasmtime_val {
            wasmtime::Val::F32(val) => Ok(f32::from_bits(val)),
            _ => Err(anyhow!("Expected f32")),
        }
    }
    fn f64(&self) -> Result<f64> {
        match self.wasmtime_val {
            wasmtime::Val::F64(val) => Ok(f64::from_bits(val)),
            _ => Err(anyhow!("Expected f64")),
        }
    }
}

pub struct ValVec {
    wasmtime_val_vec: Vec<wasmtime::Val>,
}

impl ValVec {
    fn push_i32(&mut self, val: i32) -> Result<()> {
        self.wasmtime_val_vec.push(wasmtime::Val::I32(val));
        Ok(())
    }
    fn push_i64(&mut self, val: i64) -> Result<()> {
        self.wasmtime_val_vec.push(wasmtime::Val::I64(val));
        Ok(())
    }
    fn push_f32(&mut self, val: f32) -> Result<()> {
        self.wasmtime_val_vec
            .push(wasmtime::Val::F32(val.to_bits()));
        Ok(())
    }
    fn push_f64(&mut self, val: f64) -> Result<()> {
        self.wasmtime_val_vec
            .push(wasmtime::Val::F64(val.to_bits()));
        Ok(())
    }
    fn pop_val(&mut self) -> Result<Box<Val>> {
        match self.wasmtime_val_vec.pop() {
            Some(val) => Ok(Box::new(Val { wasmtime_val: val })),
            None => Err(anyhow!("Popping from an empty value vector")),
        }
    }
}

fn get_val_vec() -> Result<Box<ValVec>> {
    Ok(Box::new(ValVec {
        wasmtime_val_vec: Vec::<wasmtime::Val>::new(),
    }))
}

pub struct Fut<'a> {
    fut: BoxFuture<'a, Result<()>>,
}

impl<'a> Fut<'a> {
    fn resume(&mut self) -> Result<bool> {
        match self.fut.as_mut().poll(&mut core::task::Context::from_waker(
            futures::task::noop_waker_ref(),
        )) {
            Poll::Pending => Ok(false),
            Poll::Ready(Ok(())) => Ok(true),
            Poll::Ready(Err(e)) => Err(e
                .downcast_ref::<wasmtime::Trap>()
                .map(|t| anyhow!("Trap: {}", t))
                .unwrap_or(e)),
        }
    }
}

fn get_func_future<'a>(
    store: &'a mut Store,
    func: &'a Func,
    args: &'a ValVec,
    rets: &'a mut ValVec,
) -> Result<Box<Fut<'a>>> {
    Ok(Box::new(Fut {
        fut: Box::pin(func.wasmtime_func.call_async(
            &mut store.wasmtime_store,
            args.wasmtime_val_vec.as_slice(),
            rets.wasmtime_val_vec.as_mut_slice(),
        )),
    }))
}
