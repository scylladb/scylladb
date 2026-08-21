const int WASM_PAGE_SIZE = 64 * 1024;
const int _scylla_abi = 1;

long long dbl(long long par) {
   int size = par >> 32;
   int position = par & 0xffffffff;
   int orig_size = __builtin_wasm_memory_size(0) * WASM_PAGE_SIZE;
   __builtin_wasm_memory_grow(0, 1 + (2 * size - 1) / WASM_PAGE_SIZE);
   char* p = (char*)0;
   for (int i = 0; i < size; ++i) {
       p[orig_size + i] = p[position + i];
       p[orig_size + size + i] = p[position + i];
   }
   long long ret = ((long long)2 * size << 32) | (long long)orig_size;
   return ret;
}
