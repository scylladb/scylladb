/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

use anyhow::{anyhow, Result};
use std::ops::Range;
use std::{cmp, ptr, u32};
use wasmtime::LinearMemory;

const WASM_PAGE_SIZE: usize = 64 * 1024;

extern "C" {
    fn aligned_alloc(align: usize, size: usize) -> *mut u8;
    fn free(ptr: *mut u8);
}

pub struct ScyllaLinearMemory {
    ptr: *mut u8,
    size: usize,
    maximum_size: Option<usize>,
}

// The entire ScyllaLinearMemory is used only in a single thread,
// because we're not sharing it between seastar shards
unsafe impl Send for ScyllaLinearMemory {}
unsafe impl Sync for ScyllaLinearMemory {}

impl Drop for ScyllaLinearMemory {
    fn drop(&mut self) {
        // previously allocated or reset to nullptr in grow_to()
        unsafe { free(self.ptr) };
    }
}

unsafe impl LinearMemory for ScyllaLinearMemory {
    fn byte_size(&self) -> usize {
        self.size
    }
    fn maximum_byte_size(&self) -> Option<usize> {
        self.maximum_size
    }
    fn grow_to(&mut self, new_size: usize) -> Result<()> {
        let new_size_maybe_extra_page = new_size.checked_add(WASM_PAGE_SIZE - 1).ok_or(anyhow!(
            "memory grow failed: new size {} is too large",
            new_size
        ))?;
        let new_size_aligned = new_size_maybe_extra_page & !(WASM_PAGE_SIZE - 1);
        if new_size_aligned == self.size {
            return Ok(());
        }
        let max_size = self.maximum_size.unwrap_or(u32::MAX as usize + 1);
        if new_size_aligned > max_size {
            return Err(anyhow!(
                "memory grow failed: new size {} exceeds maximum size {}",
                new_size_aligned,
                max_size
            ));
        }
        let new_ptr: *mut u8;
        if new_size_aligned == 0 {
            new_ptr = ptr::null_mut()
        } else {
            new_ptr = unsafe { aligned_alloc(WASM_PAGE_SIZE, new_size_aligned) };
            if new_ptr.is_null() {
                return Err(anyhow!("Failed to grow WASM memory: allocation error"));
            }
        }
        let copy_size = cmp::min(self.size, new_size_aligned);
        if copy_size > 0 {
            // new_ptr is not null, because new_size_aligned > 0
            // self.ptr is not null, because self.size > 0
            unsafe {
                std::ptr::copy_nonoverlapping(self.ptr, new_ptr, copy_size);
            };
        }
        unsafe { free(self.ptr) };
        self.size = new_size_aligned;
        self.ptr = new_ptr;
        Ok(())
    }
    fn as_ptr(&self) -> *mut u8 {
        self.ptr
    }
    fn wasm_accessible(&self) -> Range<usize> {
        Range {
            start: self.ptr as usize,
            end: self.ptr as usize + self.size,
        }
    }
}

// In order to use the Seastar memory allocator instead of mmap,
// create our own MemoryCreator which directly calls aligned_alloc
// and free, both of which came from Seastar
pub struct ScyllaMemoryCreator {
    max_scylla_size: usize,
}
impl ScyllaMemoryCreator {
    pub fn new(max_scylla_size: usize) -> Self {
        ScyllaMemoryCreator { max_scylla_size }
    }
}
unsafe impl wasmtime::MemoryCreator for ScyllaMemoryCreator {
    fn new_memory(
        &self,
        ty: wasmtime::MemoryType,
        minimum: usize,
        maximum: Option<usize>,
        reserved_size_in_bytes: Option<usize>,
        guard_size_in_bytes: usize,
    ) -> Result<Box<dyn wasmtime::LinearMemory>, String> {
        // assert that this is a memory that only allocates as much as it needs
        assert_eq!(guard_size_in_bytes, 0);
        assert!(reserved_size_in_bytes.is_none());
        assert!(!ty.is_64());
        let max_size = std::cmp::min(
            self.max_scylla_size,
            maximum.unwrap_or(u32::MAX as usize + 1),
        );
        let mut mem = ScyllaLinearMemory {
            ptr: ptr::null_mut(),
            size: 0,
            maximum_size: Some(max_size),
        };
        mem.grow_to(minimum).map_err(|e| e.to_string())?;
        Ok(Box::new(mem))
    }
}
