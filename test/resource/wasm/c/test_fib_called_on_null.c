const int WASM_PAGE_SIZE = 64 * 1024;
const int _scylla_abi = 1;

static long long swap_int64(long long val) {
    val = ((val << 8) & 0xFF00FF00FF00FF00ULL ) | ((val >> 8) & 0x00FF00FF00FF00FFULL );
    val = ((val << 16) & 0xFFFF0000FFFF0000ULL ) | ((val >> 16) & 0x0000FFFF0000FFFFULL );
    return (val << 32) | ((val >> 32) & 0xFFFFFFFFULL);
}

long long fib_aux(long long n) {
    if (n < 2) {
        return n;
    }
    return fib_aux(n-1) + fib_aux(n-2);
}
long long fib(long long p) {
    int size = p >> 32;
    long long* p_val = (long long*)(p & 0xffffffff);
    // Initialize memory for the return value
    long long* ret_val = (long long*)(__builtin_wasm_memory_size(0) * WASM_PAGE_SIZE);
    __builtin_wasm_memory_grow(0, 1); // long long fits in one wasm page
    if (size == -1) {
        *ret_val = swap_int64(42);
    } else {
        *ret_val = swap_int64(fib_aux(swap_int64(*p_val)));
    }
    // 8 is the size of a bigint
    return (long long)(8ll << 32) | (long long)ret_val;
}
