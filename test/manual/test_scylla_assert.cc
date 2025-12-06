/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

// Simple manual test for scylla_assert macro

#include "utils/assert.hh"
#include <iostream>
#include <exception>

void test_passing_assertion() {
    scylla_assert(true);
    scylla_assert(1 + 1 == 2);
    scylla_assert(1 + 1 == 2, "basic math should work");
    std::cout << "✓ All passing assertions succeeded\n";
}

void test_failing_assertion_without_message() {
    try {
        scylla_assert(false);
        std::cout << "✗ Expected exception was not thrown\n";
    } catch (const std::exception& e) {
        std::cout << "✓ Caught expected exception: " << e.what() << "\n";
    }
}

void test_failing_assertion_with_message() {
    try {
        scylla_assert(1 + 1 == 3, "this should fail");
        std::cout << "✗ Expected exception was not thrown\n";
    } catch (const std::exception& e) {
        std::cout << "✓ Caught expected exception with message: " << e.what() << "\n";
    }
}

int main() {
    std::cout << "Testing scylla_assert macro...\n\n";
    
    test_passing_assertion();
    test_failing_assertion_without_message();
    test_failing_assertion_with_message();
    
    std::cout << "\n✓ All tests completed successfully\n";
    return 0;
}
