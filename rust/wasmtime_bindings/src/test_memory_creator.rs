/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

use crate::memory_creator::ScyllaMemoryCreator;
use anyhow::{anyhow, Result};
use std::ops::Range;
use std::ptr;
use wasmtime::LinearMemory;

// The implementation below adds a conditional failure of an allocation
// to the original grow_to().
// It is used in testing instead of an alloc_failure_injector
// because we only assert that the big allocations of WASM pages
// can fail without aborting (as the allocations more likely to fail)
// TODO: remove this code when avoiding crashes on allocation failures
//       becomes possible in Rust
pub struct TestScyllaLinearMemory {
    memory: Box<dyn wasmtime::LinearMemory>,
    allocs: usize,
    fail_after: usize,
}

unsafe impl LinearMemory for TestScyllaLinearMemory {
    fn byte_size(&self) -> usize {
        self.memory.byte_size()
    }
    fn maximum_byte_size(&self) -> Option<usize> {
        self.memory.maximum_byte_size()
    }
    fn grow_to(&mut self, new_size: usize) -> Result<()> {
        let old_ptr = self.memory.as_ptr();
        // This may change the value of self.memory.as_ptr() even if we decide
        // that it should have failed, but we didn't actually guarantee that
        // it would be unchanged in that case, and we're only using this
        // in a test case where we finish the test immediately after the failure
        self.memory.grow_to(new_size)?;
        let new_ptr = self.memory.as_ptr();
        if old_ptr != new_ptr && new_ptr != ptr::null_mut() {
            // We needed to actually perform an allocation, so we could have failed
            let failed = self.allocs == self.fail_after;
            self.allocs += 1;
            if failed {
                return Err(anyhow!("Failed to grow WASM memory: allocation error"));
            }
        }
        Ok(())
    }
    fn as_ptr(&self) -> *mut u8 {
        self.memory.as_ptr()
    }
    fn wasm_accessible(&self) -> Range<usize> {
        self.memory.wasm_accessible()
    }
}

// In order to use the Seastar memory allocator instead of mmap,
// create our own MemoryCreator which directly calls aligned_alloc
// and free, both of which came from Seastar
pub struct TestScyllaMemoryCreator {
    memory_creator: ScyllaMemoryCreator,
    fail_after: usize,
}

impl TestScyllaMemoryCreator {
    pub fn new(max_scylla_size: usize, fail_after: usize) -> Self {
        TestScyllaMemoryCreator {
            memory_creator: ScyllaMemoryCreator::new(max_scylla_size),
            fail_after: fail_after,
        }
    }
}

unsafe impl wasmtime::MemoryCreator for TestScyllaMemoryCreator {
    fn new_memory(
        &self,
        ty: wasmtime::MemoryType,
        minimum: usize,
        maximum: Option<usize>,
        reserved_size_in_bytes: Option<usize>,
        guard_size_in_bytes: usize,
    ) -> Result<Box<dyn wasmtime::LinearMemory>, String> {
        // Create standard linear memory with initial size 0 to avoid the initial allocation
        let memory = self.memory_creator.new_memory(
            ty,
            0,
            maximum,
            reserved_size_in_bytes,
            guard_size_in_bytes,
        )?;
        let mut mem = TestScyllaLinearMemory {
            memory: memory,
            allocs: 0,
            fail_after: self.fail_after,
        };
        // Perform the initial allocation using grow_to of the test memory
        mem.grow_to(minimum).map_err(|e| e.to_string())?;
        Ok(Box::new(mem))
    }
}
