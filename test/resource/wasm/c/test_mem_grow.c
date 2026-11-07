
const int _scylla_abi = 1;

// Grow the memory by val pages, and return the previous memory size in
// pages, or -1 if the memory couldn't grow.
int grow_mem(int val) {
    return __builtin_wasm_memory_grow(0, val);
}
