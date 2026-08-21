const int _scylla_abi = 1;

static int swap_int32(int val) {
    val = ((val << 8) & 0xFF00FF00 ) | ((val >> 8) & 0x00FF00FF );
    return (val << 16) | ((val >> 16) & 0xFFFF);
}

long long div(long long acc) {
    int accsize = acc >> 32;
    if (accsize != 16) {
        long long ret = -1;
        return ret << 32;
    }
    int* acc_val_cnt = (int*)((acc + 4) & 0xffffffff);
    int* acc_val_sum = (int*)((acc + 12) & 0xffffffff);
    int cnt = swap_int32(*acc_val_cnt);
    int sum = swap_int32(*acc_val_sum);
    float ret_val = (float)sum / cnt;
    *acc_val_cnt = swap_int32(*((unsigned int*)&ret_val));
    acc = 4ll << 32 | (int)acc_val_cnt;
    return acc;
}
