const int _scylla_abi = 1;

int power(int base, int pow) {
    int result = 1;
    for (; 0 < pow; pow >>= 1) {
        result = (pow & 1) ? result * base : result;
        base *= base;
    }
    return result;
}
