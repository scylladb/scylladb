
const int _scylla_abi = 1;

int grow_mem(int val) {
    __builtin_wasm_memory_grow(0, val);
    return val;
}
