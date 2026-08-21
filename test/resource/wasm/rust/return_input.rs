extern "C" {
    fn malloc(size: usize) -> usize;
    fn free(ptr: *mut usize);
}

#[no_mangle]
pub unsafe extern "C" fn _scylla_malloc(size: usize) -> u32 {
    malloc(size) as u32
}

#[no_mangle]
pub unsafe extern "C" fn _scylla_free(ptr: *mut usize) {
    free(ptr)
}

#[no_mangle]
pub static _scylla_abi: u32 = 2;

#[no_mangle]
pub extern "C" fn return_input(sizeptr: u64) -> u64 {
    sizeptr
}
