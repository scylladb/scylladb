const int _scylla_abi = 1;

static int swap_int32(int val) {
    val = ((val << 8) & 0xFF00FF00 ) | ((val >> 8) & 0x00FF00FF );
    return (val << 16) | ((val >> 16) & 0xFFFF);
}

long long sum(long long acc, long long p) {
    int size = p >> 32;
    int accsize = acc >> 32;
    if (size != 4 || accsize != 16) {
        return acc;
    }
    int p_val = swap_int32(*(int*)(p & 0xffffffff));
    int* acc_val_cnt = (int*)((acc + 4) & 0xffffffff);
    int* acc_val_sum = (int*)((acc + 12) & 0xffffffff);
    *acc_val_cnt = swap_int32(1 + swap_int32(*acc_val_cnt));
    *acc_val_sum = swap_int32(p_val + swap_int32(*acc_val_sum));
    return acc;
}
