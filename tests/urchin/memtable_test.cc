/*
 * Copyright 2015 Cloudius Systems
 */

#define BOOST_TEST_DYN_LINK

#include <boost/test/unit_test.hpp>
#include "tests/test-utils.hh"

#include "core/thread.hh"
#include "memtable.hh"
#include "mutation_source_test.hh"

SEASTAR_TEST_CASE(test_memtable_conforms_to_mutation_source) {
    return seastar::async([] {
        run_mutation_source_tests([](schema_ptr s, const std::vector<mutation>& partitions) {
            auto mt = make_lw_shared<memtable>(s);

            for (auto&& m : partitions) {
                mt->apply(m);
            }

            return mt->as_data_source();
        });
    });
}
