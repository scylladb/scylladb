#ifdef SEASTAR_DEFAULT_ALLOCATOR

#include "utils/logalloc.hh"
#include "test/lib/scylla_test_case.hh"

using namespace logalloc;

SEASTAR_TEST_CASE(test_preinit) {
    return use_standard_allocator_segment_pool_backend(1 << 30);
}

#include "./logalloc_test.cc"

#else

#include <iostream>

int main() {
    std::cout << "this test is for debug mode only" << std::endl;
    return 0;
}

#endif
